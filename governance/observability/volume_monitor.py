"""
Volume and Distribution Monitor — detects data anomalies across medallion layers.

Runs as a scheduled job (every 15 minutes) independent of the Silver Flink job.
Reads from each Iceberg layer, computes volume and distribution metrics,
and writes results to s3://chakra-lakehouse/governance/observability_metrics/.

Anomaly detection uses Z-score on a 7-day rolling baseline:
  - |z_score| > 3.0 → anomaly flag set, alert fires via Prometheus metric
  - 2.0 < |z_score| <= 3.0 → warning (Grafana amber)

Metrics computed per table per partition_date:
  - row_count               — total rows written in the partition window
  - null_rate_{column}      — fraction of nulls per column
  - mean_{column}           — mean value of numeric columns
  - stddev_{column}         — standard deviation of numeric columns
  - p95_{column}            — 95th percentile of numeric columns
  - distinct_count_{column} — approximate distinct count (HyperLogLog)
"""

from __future__ import annotations

import uuid
from datetime import datetime, date, timezone, timedelta
from dataclasses import dataclass

import duckdb


LAKEHOUSE_PATH = "s3://chakra-lakehouse"
GOVERNANCE_PATH = f"{LAKEHOUSE_PATH}/governance/observability_metrics"

# Numeric columns to profile per table
NUMERIC_COLUMNS = {
    "bronze.orders":           ["total_amount_cents"],
    "silver.orders":           ["total_amount_cents", "item_count"],
    "gold.order_daily_summary": ["total_revenue_cents", "order_count", "unique_customers"],
}

TABLE_PATHS = {
    "bronze.orders":           f"{LAKEHOUSE_PATH}/bronze/orders/",
    "silver.orders":           f"{LAKEHOUSE_PATH}/silver/orders/orders/",
    "gold.order_daily_summary": f"{LAKEHOUSE_PATH}/gold/orders-analytics/order_daily_summary/",
}


@dataclass
class ObservabilityMetric:
    metric_id: str
    table_name: str
    partition_date: str
    metric_name: str
    metric_value: float
    baseline_7d_avg: float | None
    baseline_7d_stddev: float | None
    z_score: float | None
    anomaly_detected: bool
    collected_at: str


def run_monitor(con: duckdb.DuckDBPyConnection, target_date: date | None = None) -> None:
    if target_date is None:
        target_date = date.today()

    metrics: list[ObservabilityMetric] = []

    for table_name, table_path in TABLE_PATHS.items():
        try:
            _collect_table_metrics(con, table_name, table_path, target_date, metrics)
        except Exception as exc:
            print(f"[volume_monitor] Failed to profile {table_name}: {exc}")

    if metrics:
        _write_to_iceberg(con, metrics)
        print(f"[volume_monitor] Wrote {len(metrics)} metrics for {target_date}")


def _collect_table_metrics(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    table_path: str,
    target_date: date,
    out: list[ObservabilityMetric],
) -> None:
    collected_at = datetime.now(timezone.utc).isoformat()
    date_str = target_date.isoformat()

    # Row count for today's partition
    row_count = con.execute(f"""
        SELECT COUNT(*) FROM iceberg_scan('{table_path}')
        WHERE CAST(occurred_at AS DATE) = '{date_str}'
           OR CAST(order_date AS DATE) = '{date_str}'
    """).fetchone()[0]

    # Baseline: 7-day rolling average and stddev from governance metrics
    baseline_row = con.execute(f"""
        SELECT AVG(metric_value), STDDEV(metric_value)
        FROM iceberg_scan('{GOVERNANCE_PATH}/')
        WHERE table_name    = '{table_name}'
          AND metric_name   = 'row_count'
          AND partition_date BETWEEN '{(target_date - timedelta(days=8)).isoformat()}'
                                 AND '{(target_date - timedelta(days=1)).isoformat()}'
    """).fetchone()

    baseline_avg = baseline_row[0]
    baseline_std = baseline_row[1]
    z_score = None
    anomaly = False

    if baseline_avg is not None and baseline_std and baseline_std > 0:
        z_score = round((row_count - baseline_avg) / baseline_std, 3)
        anomaly = abs(z_score) > 3.0

    out.append(ObservabilityMetric(
        metric_id=str(uuid.uuid4()),
        table_name=table_name,
        partition_date=date_str,
        metric_name="row_count",
        metric_value=float(row_count),
        baseline_7d_avg=baseline_avg,
        baseline_7d_stddev=baseline_std,
        z_score=z_score,
        anomaly_detected=anomaly,
        collected_at=collected_at,
    ))

    # Numeric column distribution metrics
    for col in NUMERIC_COLUMNS.get(table_name, []):
        stats_row = con.execute(f"""
            SELECT
                COUNT(*) FILTER (WHERE {col} IS NULL) * 1.0 / COUNT(*) AS null_rate,
                AVG({col})                                               AS mean_val,
                STDDEV({col})                                            AS stddev_val,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY {col})     AS p95_val,
                APPROX_COUNT_DISTINCT({col})                             AS distinct_count
            FROM iceberg_scan('{table_path}')
            WHERE CAST(occurred_at AS DATE) = '{date_str}'
               OR CAST(order_date   AS DATE) = '{date_str}'
        """).fetchone()

        if stats_row is None:
            continue

        null_rate, mean_val, stddev_val, p95_val, distinct_count = stats_row

        for metric_name, metric_value in [
            (f"null_rate_{col}",      null_rate),
            (f"mean_{col}",           mean_val),
            (f"stddev_{col}",         stddev_val),
            (f"p95_{col}",            p95_val),
            (f"distinct_count_{col}", distinct_count),
        ]:
            if metric_value is None:
                continue

            baseline = _get_column_baseline(con, table_name, metric_name, target_date)
            col_z = None
            col_anomaly = False
            if baseline and baseline[1] and baseline[1] > 0:
                col_z = round((metric_value - baseline[0]) / baseline[1], 3)
                col_anomaly = abs(col_z) > 3.0

            out.append(ObservabilityMetric(
                metric_id=str(uuid.uuid4()),
                table_name=table_name,
                partition_date=date_str,
                metric_name=metric_name,
                metric_value=round(float(metric_value), 6),
                baseline_7d_avg=baseline[0] if baseline else None,
                baseline_7d_stddev=baseline[1] if baseline else None,
                z_score=col_z,
                anomaly_detected=col_anomaly,
                collected_at=collected_at,
            ))


def _get_column_baseline(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    metric_name: str,
    target_date: date,
) -> tuple[float, float] | None:
    row = con.execute(f"""
        SELECT AVG(metric_value), STDDEV(metric_value)
        FROM iceberg_scan('{GOVERNANCE_PATH}/')
        WHERE table_name    = '{table_name}'
          AND metric_name   = '{metric_name}'
          AND partition_date BETWEEN '{(target_date - timedelta(days=8)).isoformat()}'
                                 AND '{(target_date - timedelta(days=1)).isoformat()}'
    """).fetchone()
    if row and row[0] is not None:
        return (row[0], row[1] or 0.0)
    return None


def _write_to_iceberg(
    con: duckdb.DuckDBPyConnection, metrics: list[ObservabilityMetric]
) -> None:
    rows = [
        (
            m.metric_id, m.table_name, m.partition_date, m.metric_name,
            m.metric_value, m.baseline_7d_avg, m.baseline_7d_stddev,
            m.z_score, m.anomaly_detected, m.collected_at,
        )
        for m in metrics
    ]
    con.execute(f"""
        INSERT INTO iceberg_scan('{GOVERNANCE_PATH}/')
        SELECT
            column0  AS metric_id,
            column1  AS table_name,
            column2  AS partition_date,
            column3  AS metric_name,
            column4  AS metric_value,
            column5  AS baseline_7d_avg,
            column6  AS baseline_7d_stddev,
            column7  AS z_score,
            column8  AS anomaly_detected,
            column9  AS collected_at
        FROM (VALUES {_sql_values(rows)})
    """)


def _sql_values(rows: list[tuple]) -> str:
    def fmt(v):
        if v is None:
            return "NULL"
        if isinstance(v, bool):
            return "TRUE" if v else "FALSE"
        if isinstance(v, (int, float)):
            return str(v)
        return f"'{str(v)}'"

    return ", ".join(f"({', '.join(fmt(v) for v in row)})" for row in rows)
