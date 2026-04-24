-- ─────────────────────────────────────────────────────────────────────────────
-- Lineage Graph Queries
-- Source: s3://chakra-lakehouse/governance/lineage_events/
--
-- These queries express the column-level lineage graph stored in Iceberg.
-- The graph answers two classes of questions:
--
--   FORWARD IMPACT:  "If I change source field X, which Gold columns are affected?"
--   BACKWARD TRACE:  "This Gold column has bad values — which source field caused it?"
-- ─────────────────────────────────────────────────────────────────────────────

-- ── 1. Full Column Lineage Graph — source to Gold ─────────────────────────────
--
-- Returns the complete column-level lineage as an edge list.
-- Suitable for rendering as a directed graph (Mermaid, Graphviz, or a BI tool).
--
-- Each row is: source_dataset.source_field → output_dataset.output_column
--              with the transformation type

WITH lineage_events AS (
    SELECT
        job_name,
        run_id,
        event_time,
        CAST(column_lineage_json AS JSON) AS lineage_json
    FROM iceberg_scan('s3://chakra-lakehouse/governance/lineage_events/')
    WHERE event_type = 'COMPLETE'
),
edges AS (
    SELECT
        job_name,
        run_id,
        event_time,
        col_edge->>'output_column'                     AS output_column,
        input_field->>'dataset'                        AS source_dataset,
        input_field->>'field'                          AS source_field,
        input_field->>'transform'                      AS transform_type
    FROM lineage_events,
         UNNEST(FROM_JSON(lineage_json::VARCHAR, '[{"output_column":"","input_fields":[]}]'))
             AS col_edge,
         UNNEST(FROM_JSON(col_edge->>'input_fields',
                          '[{"dataset":"","field":"","transform":""}]'))
             AS input_field
)
SELECT DISTINCT
    source_dataset,
    source_field,
    transform_type,
    output_column,
    job_name                                           AS produced_by
FROM edges
ORDER BY source_dataset, source_field, job_name;


-- ── 2. Forward Impact: "which Gold columns are affected by source field X?" ───
--
-- Use this before renaming or dropping a field in the Avro schema.
-- Replace 'total_amount_cents' with any source field name.

WITH all_lineage AS (
    SELECT
        job_name,
        col_edge->>'output_column'   AS output_column,
        input_field->>'dataset'      AS source_dataset,
        input_field->>'field'        AS source_field,
        input_field->>'transform'    AS transform_type
    FROM iceberg_scan('s3://chakra-lakehouse/governance/lineage_events/'),
         UNNEST(FROM_JSON(column_lineage_json, '[{}]'))           AS col_edge,
         UNNEST(FROM_JSON(col_edge->>'input_fields', '[{}]'))     AS input_field
    WHERE event_type = 'COMPLETE'
)
SELECT DISTINCT
    source_field,
    output_column,
    transform_type,
    job_name
FROM all_lineage
WHERE source_field = 'total_amount_cents'   -- ← change to target field
ORDER BY job_name, output_column;


-- ── 3. Backward Trace: "this Gold column has bad values — trace to source" ────
--
-- Replace 'total_revenue_cents' with any suspect Gold column.

WITH all_lineage AS (
    SELECT
        job_name,
        col_edge->>'output_column'   AS output_column,
        input_field->>'dataset'      AS source_dataset,
        input_field->>'field'        AS source_field,
        input_field->>'transform'    AS transform_type
    FROM iceberg_scan('s3://chakra-lakehouse/governance/lineage_events/'),
         UNNEST(FROM_JSON(column_lineage_json, '[{}]'))           AS col_edge,
         UNNEST(FROM_JSON(col_edge->>'input_fields', '[{}]'))     AS input_field
    WHERE event_type = 'COMPLETE'
)
SELECT DISTINCT
    output_column,
    source_dataset,
    source_field,
    transform_type,
    job_name                                          AS transformation_step
FROM all_lineage
WHERE output_column = 'total_revenue_cents'           -- ← change to suspect column
ORDER BY job_name, source_field;


-- ── 4. Pipeline Run History — what ran, when, and how many records ────────────
--
-- Audit trail of every pipeline execution with row counts.
-- Correlate run_id with quality_results to link pipeline runs to quality scores.

SELECT
    job_name,
    run_id,
    MIN(event_time) FILTER (WHERE event_type = 'START')    AS started_at,
    MAX(event_time) FILTER (WHERE event_type = 'COMPLETE') AS completed_at,
    MAX(event_time) FILTER (WHERE event_type = 'FAIL')     AS failed_at,
    BOOL_OR(event_type = 'COMPLETE')                       AS succeeded,
    MAX(row_count_input)                                   AS rows_read,
    MAX(row_count_output)                                  AS rows_written,
    ROUND(
        MAX(row_count_output) * 100.0
        / NULLIF(MAX(row_count_input), 0), 3
    )                                                      AS "pass_through_%"
FROM iceberg_scan('s3://chakra-lakehouse/governance/lineage_events/')
WHERE event_time >= NOW() - INTERVAL 30 DAYS
GROUP BY job_name, run_id
ORDER BY started_at DESC;


-- ── 5. Quality-Lineage Correlation — tie failures to pipeline runs ────────────
--
-- Joins quality results to lineage run IDs so you can see:
-- "Run abc123 produced 200 Silver failures. The source was kafka://chakra.orders.placed."
-- This bridges the gap between "bad data" and "who produced it".

SELECT
    le.run_id,
    le.job_name,
    MIN(le.event_time)                                     AS run_time,
    SUM(qr.failed_record_count)                            AS total_failures,
    ROUND(AVG(qr.quality_score) * 100, 3)                  AS avg_quality_pct,
    MODE() WITHIN GROUP (ORDER BY qr.check_name)
        FILTER (WHERE NOT qr.threshold_met)                AS top_failing_check
FROM iceberg_scan('s3://chakra-lakehouse/governance/lineage_events/') le
JOIN iceberg_scan('s3://chakra-lakehouse/governance/quality_results/') qr
    ON qr.run_id = le.run_id
WHERE le.event_type = 'COMPLETE'
  AND le.event_time >= NOW() - INTERVAL 7 DAYS
GROUP BY le.run_id, le.job_name
HAVING SUM(qr.failed_record_count) > 0
ORDER BY total_failures DESC;
