-- ─────────────────────────────────────────────────────────────────────────────
-- Observability Dashboard Queries
-- Source: s3://chakra-lakehouse/governance/observability_metrics/
--
-- Volume anomalies, distribution drift, and schema drift events.
-- These queries surface OPERATIONAL instability — things that can go wrong
-- even when individual record quality checks pass.
-- ─────────────────────────────────────────────────────────────────────────────

-- ── 1. Volume Anomaly Summary — is data flowing at expected rates? ────────────
--
-- Z-score > 3 means the volume is more than 3 standard deviations from
-- the 7-day baseline. This catches:
--   - Upstream Debezium connector failure (volume drops to 0)
--   - Downstream consumer failure (Silver write rate drops)
--   - Traffic spikes (Black Friday, marketing campaign)
--   - Duplicate ingestion bug (volume spikes 2×)

SELECT
    table_name,
    partition_date,
    CAST(metric_value AS BIGINT)                         AS row_count,
    ROUND(baseline_7d_avg)                               AS baseline_avg_rows,
    ROUND(z_score, 2)                                    AS z_score,
    CASE
        WHEN ABS(z_score) > 3.0  THEN '✗ ANOMALY'
        WHEN ABS(z_score) > 2.0  THEN '⚠ WARNING'
        ELSE '✓ NORMAL'
    END                                                  AS status,
    ROUND((metric_value - baseline_7d_avg)
          / NULLIF(baseline_7d_avg, 0) * 100, 1)         AS "deviation_%"
FROM iceberg_scan('s3://chakra-lakehouse/governance/observability_metrics/')
WHERE metric_name = 'row_count'
  AND partition_date >= CURRENT_DATE - INTERVAL 7 DAYS
ORDER BY ABS(COALESCE(z_score, 0)) DESC, partition_date DESC;


-- ── 2. Volume Trend — 30-day row count per table ─────────────────────────────
--
-- Visualize as a line chart in Grafana. Divergence between Bronze and Silver
-- row counts reveals how many records the Silver job is rejecting.

SELECT
    partition_date,
    MAX(metric_value) FILTER (WHERE table_name = 'bronze.orders')            AS bronze_rows,
    MAX(metric_value) FILTER (WHERE table_name = 'silver.orders')            AS silver_rows,
    MAX(metric_value) FILTER (WHERE table_name = 'gold.order_daily_summary') AS gold_rows,
    -- rejection rate: records lost between Bronze and Silver
    ROUND(
        (MAX(metric_value) FILTER (WHERE table_name = 'bronze.orders')
         - MAX(metric_value) FILTER (WHERE table_name = 'silver.orders'))
        * 100.0
        / NULLIF(MAX(metric_value) FILTER (WHERE table_name = 'bronze.orders'), 0),
        3
    )                                                                         AS "silver_rejection_%"
FROM iceberg_scan('s3://chakra-lakehouse/governance/observability_metrics/')
WHERE metric_name  = 'row_count'
  AND partition_date >= CURRENT_DATE - INTERVAL 30 DAYS
GROUP BY partition_date
ORDER BY partition_date DESC;


-- ── 3. Distribution Drift — has the data character changed? ──────────────────
--
-- Tracks mean and stddev of key numeric columns over time.
-- A rising mean of total_amount_cents with stable count suggests
-- a pricing change; a rising stddev suggests a data quality issue.

SELECT
    partition_date,
    table_name,
    SPLIT_PART(metric_name, '_', 2)                       AS column_name,
    SPLIT_PART(metric_name, '_', 1)                       AS stat_type,
    ROUND(metric_value, 2)                                AS value,
    ROUND(baseline_7d_avg, 2)                             AS baseline,
    ROUND(z_score, 2)                                     AS z_score,
    anomaly_detected
FROM iceberg_scan('s3://chakra-lakehouse/governance/observability_metrics/')
WHERE metric_name LIKE 'mean_%'
   OR metric_name LIKE 'stddev_%'
   OR metric_name LIKE 'p95_%'
   AND partition_date >= CURRENT_DATE - INTERVAL 14 DAYS
ORDER BY partition_date DESC, table_name, metric_name;


-- ── 4. Null Rate Trend — completeness over time ───────────────────────────────
--
-- Null rates should be near-zero for required fields (order_id, customer_id).
-- A rising null_rate signals upstream producer bug or schema mismatch.

SELECT
    partition_date,
    table_name,
    REPLACE(metric_name, 'null_rate_', '')                AS column_name,
    ROUND(metric_value * 100, 4)                          AS "null_rate_%",
    CASE
        WHEN metric_value > 0.001 THEN '✗ ABOVE 0.1% THRESHOLD'
        WHEN metric_value > 0    THEN '⚠ SOME NULLS'
        ELSE '✓ CLEAN'
    END                                                   AS status
FROM iceberg_scan('s3://chakra-lakehouse/governance/observability_metrics/')
WHERE metric_name LIKE 'null_rate_%'
  AND partition_date >= CURRENT_DATE - INTERVAL 14 DAYS
ORDER BY metric_value DESC, partition_date DESC;


-- ── 5. All Active Anomalies — operational alert summary ───────────────────────
--
-- One row per anomaly detected in the last 24 hours.
-- Feed this into the on-call dashboard as the alert triage list.

SELECT
    collected_at,
    table_name,
    metric_name,
    ROUND(metric_value, 2)                               AS current_value,
    ROUND(baseline_7d_avg, 2)                            AS expected_value,
    ROUND(z_score, 2)                                    AS z_score,
    ROUND((metric_value - baseline_7d_avg)
          / NULLIF(baseline_7d_avg, 0) * 100, 1)         AS "deviation_%"
FROM iceberg_scan('s3://chakra-lakehouse/governance/observability_metrics/')
WHERE anomaly_detected = TRUE
  AND collected_at >= NOW() - INTERVAL 24 HOURS
ORDER BY ABS(z_score) DESC;


-- ── 6. Schema Drift Log ───────────────────────────────────────────────────────
--
-- History of all schema changes detected by schema_drift_detector.py.
-- Correlate this with quality dips to determine if a schema change
-- caused the downstream quality degradation.

SELECT
    detected_at,
    table_name,
    drift_type,
    severity,
    column_name,
    old_type,
    new_type,
    is_safe,
    details
FROM iceberg_scan('s3://chakra-lakehouse/governance/schema_drift_events/')
ORDER BY detected_at DESC
LIMIT 50;
