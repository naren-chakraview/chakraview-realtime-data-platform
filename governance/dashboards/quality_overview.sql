-- ─────────────────────────────────────────────────────────────────────────────
-- Quality Overview Dashboard Queries
-- Source: s3://chakra-lakehouse/governance/quality_results/
--
-- Run these against DuckDB (serving/duckdb/catalog.py provides the connection).
-- The quality waterfall (Query 1) is the primary instability indicator:
-- it shows WHERE in the pipeline quality degrades and by how much.
-- ─────────────────────────────────────────────────────────────────────────────

-- ── 1. Quality Waterfall — pipeline instability at a glance ──────────────────
--
-- Shows quality scores at each stage in order. A drop between stages
-- immediately identifies WHERE the pipeline is degrading.
--
-- Example healthy output:
--   bronze_ingestion    100,000 records   99.98%   [✓ above threshold]
--   silver_validation    99,800 records   99.80%   [✓ above threshold]
--   silver_dedup         99,650 records   99.65%   [✓ above threshold]
--   gold_aggregation     99,650 records   99.65%   [✓ above threshold]
--
-- Example degraded output (Silver instability):
--   bronze_ingestion    100,000 records  100.00%   [✓]
--   silver_validation    94,000 records   94.00%   [✗ below 99.9% threshold]  ← problem here
--   silver_dedup         93,900 records   93.90%   [✗]
--   gold_aggregation     93,900 records   93.90%   [✗]

WITH quality_24h AS (
    SELECT *
    FROM iceberg_scan('s3://chakra-lakehouse/governance/quality_results/')
    WHERE evaluated_at >= NOW() - INTERVAL 24 HOURS
),
stage_totals AS (
    SELECT
        pipeline_stage,
        SUM(total_record_count)                          AS total_records,
        SUM(passed_record_count)                         AS passed_records,
        SUM(failed_record_count)                         AS failed_records,
        ROUND(SUM(passed_record_count) * 100.0
              / NULLIF(SUM(total_record_count), 0), 3)   AS quality_pct,
        COUNT(*) FILTER (WHERE NOT threshold_met)        AS checks_below_threshold,
        MODE() WITHIN GROUP (ORDER BY check_name)
            FILTER (WHERE failed_record_count > 0)       AS top_failing_check
    FROM quality_24h
    GROUP BY pipeline_stage
)
SELECT
    CASE pipeline_stage
        WHEN 'bronze_ingestion'  THEN '1. Bronze Ingestion'
        WHEN 'silver_validation' THEN '2. Silver Validation'
        WHEN 'silver_dedup'      THEN '3. Silver Dedup'
        WHEN 'gold_aggregation'  THEN '4. Gold Aggregation'
        ELSE pipeline_stage
    END                                                  AS stage,
    total_records,
    passed_records,
    failed_records,
    quality_pct                                          AS "quality_%",
    checks_below_threshold,
    COALESCE(top_failing_check, '—')                     AS top_failing_check,
    CASE
        WHEN quality_pct >= 99.9 THEN '✓'
        WHEN quality_pct >= 99.0 THEN '⚠'
        ELSE '✗'
    END                                                  AS status
FROM stage_totals
ORDER BY pipeline_stage;


-- ── 2. Quality Score Trend — 30-day time-series per stage ────────────────────
--
-- Reveals degradation patterns: a gradual decline vs. a sudden cliff.
-- A sudden cliff usually indicates a producer change or schema evolution.
-- A gradual decline usually indicates data drift (upstream business change).

SELECT
    DATE_TRUNC('hour', evaluated_at)                     AS hour,
    pipeline_stage,
    ROUND(AVG(quality_score) * 100, 3)                   AS avg_quality_pct,
    MIN(quality_score * 100)                             AS min_quality_pct,
    COUNT(*) FILTER (WHERE NOT threshold_met)            AS threshold_breaches
FROM iceberg_scan('s3://chakra-lakehouse/governance/quality_results/')
WHERE evaluated_at >= NOW() - INTERVAL 30 DAYS
GROUP BY 1, 2
ORDER BY 1 DESC, 2;


-- ── 3. Check-Level Failure Breakdown — what exactly is failing ───────────────
--
-- Drills into which specific checks drive quality degradation.
-- Use this after the waterfall identifies a problematic stage.

SELECT
    pipeline_stage,
    check_name,
    dimension,
    SUM(failed_record_count)                             AS total_failures,
    SUM(total_record_count)                              AS total_records,
    ROUND(SUM(failed_record_count) * 100.0
          / NULLIF(SUM(total_record_count), 0), 4)       AS failure_rate_pct,
    MIN(quality_score * 100)                             AS worst_quality_pct,
    COUNT(*) FILTER (WHERE NOT threshold_met)            AS windows_below_threshold,
    MAX(evaluated_at)                                    AS last_failure_at
FROM iceberg_scan('s3://chakra-lakehouse/governance/quality_results/')
WHERE evaluated_at >= NOW() - INTERVAL 7 DAYS
  AND failed_record_count > 0
GROUP BY 1, 2, 3
ORDER BY total_failures DESC;


-- ── 4. Sample Failure IDs — which specific records failed ────────────────────
--
-- The quality_checker.py stores up to 10 failing event_ids per window.
-- Use these to join against the DLQ table for full context.

SELECT
    qr.evaluated_at,
    qr.pipeline_stage,
    qr.check_name,
    qr.failed_record_count,
    UNNEST(FROM_JSON(qr.sample_failure_ids, '["VARCHAR"]')) AS failed_event_id
FROM iceberg_scan('s3://chakra-lakehouse/governance/quality_results/') qr
WHERE qr.evaluated_at >= NOW() - INTERVAL 1 HOUR
  AND qr.failed_record_count > 0
ORDER BY qr.evaluated_at DESC, qr.failed_record_count DESC
LIMIT 100;


-- ── 5. Quality SLA Compliance Summary — matches the SLO definition ───────────
--
-- Computes: what % of 60-second windows met the quality threshold?
-- This is the quality SLO burn rate input.

WITH windows AS (
    SELECT
        pipeline_stage,
        check_name,
        threshold,
        threshold_met,
        DATE_TRUNC('day', evaluated_at) AS day
    FROM iceberg_scan('s3://chakra-lakehouse/governance/quality_results/')
    WHERE evaluated_at >= NOW() - INTERVAL 30 DAYS
)
SELECT
    pipeline_stage,
    check_name,
    threshold                                            AS required_threshold,
    COUNT(*)                                             AS total_windows,
    SUM(CASE WHEN threshold_met THEN 1 ELSE 0 END)       AS passing_windows,
    ROUND(
        SUM(CASE WHEN threshold_met THEN 1 ELSE 0 END) * 100.0
        / NULLIF(COUNT(*), 0), 3
    )                                                    AS compliance_pct
FROM windows
GROUP BY 1, 2, 3
ORDER BY compliance_pct ASC;
