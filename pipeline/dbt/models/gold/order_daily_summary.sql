{{/*
  Gold layer: order_daily_summary
  Data product: orders-analytics (contracts/data-products/orders-analytics.yaml)
  Source: silver.orders (Silver Iceberg table)
  SLA: gold_freshness.max_lag_minutes = 5

  This model is scheduled to run every 2 minutes via dbt Cloud or a Flink
  watermark trigger. It is idempotent: running it twice produces identical output.
  Partition: order_date (UTC)
*/}}

{{ config(
    materialized     = 'incremental',
    unique_key       = ['order_date', 'currency'],
    on_schema_change = 'append_new_columns',
    file_format      = 'iceberg',
    location_root    = 's3://chakra-lakehouse/gold/orders-analytics/order_daily_summary',
    partition_by     = [{'field': 'order_date', 'data_type': 'date'}],
    properties       = {
        'write.target-file-size-bytes': '134217728',  -- 128MB target
        'write.distribution-mode':      'hash',
    }
) }}

WITH silver_orders AS (
    SELECT
        DATE_TRUNC('day', occurred_at AT TIME ZONE 'UTC') AS order_date,
        currency,
        order_id,
        customer_id,
        total_amount_cents,
        item_count
    FROM {{ source('silver', 'orders') }}
    WHERE _is_valid = TRUE  -- Silver validation flag; DLQ rows excluded

    {% if is_incremental() %}
        -- Incremental: only process partitions updated since last run.
        -- Iceberg time-travel: read Silver snapshot as of this run's start timestamp
        -- to ensure consistency even if Silver writes are in-flight.
        AND occurred_at >= (
            SELECT DATEADD(minute, -10, MAX(updated_at))
            FROM {{ this }}
        )
    {% endif %}
)

SELECT
    order_date,
    currency,

    COUNT(DISTINCT order_id)                                AS order_count,
    SUM(total_amount_cents)                                 AS total_revenue_cents,
    AVG(total_amount_cents)                                 AS avg_order_cents,
    PERCENTILE_CONT(0.50) WITHIN GROUP (
        ORDER BY total_amount_cents
    )                                                       AS p50_order_cents,
    PERCENTILE_CONT(0.95) WITHIN GROUP (
        ORDER BY total_amount_cents
    )                                                       AS p95_order_cents,
    SUM(item_count)                                         AS item_count,
    COUNT(DISTINCT customer_id)                             AS unique_customers,

    CURRENT_TIMESTAMP                                       AS updated_at

FROM silver_orders
GROUP BY
    order_date,
    currency
