-- Reference queries for the orders-analytics data product.
-- Run via: python -c "from serving.duckdb.catalog import get_connection; con=get_connection(); print(con.execute(open('serving/duckdb/queries/order_analytics.sql').read()).fetchdf())"
-- Or in a Jupyter notebook: %run serving/duckdb/catalog.py

-- ── Last 7 days revenue by currency ──────────────────────────────────────────

SELECT
    order_date,
    currency,
    order_count,
    ROUND(total_revenue_cents / 100.0, 2)   AS revenue_usd,
    ROUND(avg_order_cents    / 100.0, 2)    AS avg_order_usd
FROM orders_analytics.order_daily_summary
WHERE order_date >= CURRENT_DATE - INTERVAL 7 DAYS
ORDER BY order_date DESC, currency;


-- ── Time travel: revenue as of 2 days ago (Iceberg snapshot) ─────────────────
-- Useful for: data quality audits, reconciliation reports, debugging pipeline bugs.

SELECT
    order_date,
    SUM(total_revenue_cents) / 100.0 AS revenue_usd
FROM iceberg_scan(
    's3://chakra-lakehouse/gold/orders-analytics/order_daily_summary/',
    version = (CURRENT_TIMESTAMP - INTERVAL 2 DAYS)::VARCHAR
)
GROUP BY order_date
ORDER BY order_date DESC;


-- ── Data freshness check: how old is the latest Gold partition? ───────────────
-- Use this in monitoring scripts. Alert if result > 5 minutes (gold_freshness SLA).

SELECT
    MAX(updated_at)                                              AS latest_write,
    NOW() - MAX(updated_at)                                     AS data_age,
    CASE
        WHEN NOW() - MAX(updated_at) > INTERVAL 5 MINUTES
        THEN 'SLA_BREACH'
        ELSE 'OK'
    END                                                          AS freshness_status
FROM orders_analytics.order_daily_summary;


-- ── DLQ rate check: malformed events in the last hour ────────────────────────
-- Alert threshold: > 1 per hour (from contracts/data-products/orders-analytics.yaml)

SELECT
    DATE_TRUNC('hour', arrived_at)  AS hour,
    COUNT(*)                        AS dlq_count,
    COUNT(*) FILTER (WHERE late_arrival) AS late_arrivals,
    MODE(failure_stage)             AS dominant_failure_stage,
    MODE(failure_reason)            AS dominant_failure_reason
FROM iceberg_scan('s3://chakra-lakehouse/bronze/orders-analytics/dlq/')
WHERE arrived_at >= NOW() - INTERVAL 24 HOURS
GROUP BY 1
ORDER BY 1 DESC;
