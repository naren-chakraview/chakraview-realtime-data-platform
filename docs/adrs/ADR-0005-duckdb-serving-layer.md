# ADR-0005: DuckDB as the Lakehouse Serving Layer

**Status**: Accepted
**Date**: 2024-04-01
**Deciders**: Data platform team
**Supersedes**: —

---

## Context

The Gold-layer Iceberg tables on S3 need a SQL query interface for:

1. **Data analysts** running ad-hoc queries in notebooks
2. **Data validation** scripts run by the pipeline on freshness and quality checks
3. **Agent tasks** (Persona 5) that query the Gold layer to verify Flink job output
4. **Local development** so engineers can work against real data without cloud dependencies

The query engine choice is decoupled from the lakehouse storage choice: Iceberg tables on S3 can be queried by any engine that supports the Iceberg spec. Switching engines requires zero changes to the pipeline.

### Options considered

| Engine | Architecture | Iceberg support | Infrastructure |
|---|---|---|---|
| **DuckDB** | In-process, single-node | Native (read; write via PyIceberg) | None — library |
| **AWS Athena** | Serverless Trino | Native (read + write) | AWS account; no cluster mgmt |
| **Trino (self-hosted)** | Distributed cluster | Native (read + write) | Coordinator + workers + metastore |
| **Apache Spark** | Distributed cluster | Native (read + write) | Cluster or EMR |

---

## Decision

Use **DuckDB** as the serving layer for this reference implementation. Document **AWS Athena** as the production-scale path with explicit threshold criteria for when to migrate.

### Why DuckDB

**Zero infrastructure.** DuckDB runs as a Python library. There is no server to provision, no cluster to size, no metastore to operate. An analyst can query Gold-layer Iceberg tables on S3 with:

```python
import duckdb

con = duckdb.connect()
con.execute("INSTALL iceberg; LOAD iceberg;")
con.execute("INSTALL httpfs;  LOAD httpfs;")

con.execute("""
    CREATE SECRET s3_secret (
        TYPE S3,
        REGION 'us-east-1',
        PROVIDER CREDENTIAL_CHAIN
    )
""")

result = con.execute("""
    SELECT
        order_date,
        SUM(revenue_cents) / 100.0  AS revenue_usd,
        COUNT(DISTINCT order_id)     AS order_count
    FROM iceberg_scan('s3://chakra-lakehouse/gold/order_daily_summary/')
    WHERE order_date >= current_date - INTERVAL 7 DAYS
    GROUP BY order_date
    ORDER BY order_date DESC
""").fetchdf()
```

**Time travel is first-class.** Iceberg snapshot IDs are surfaced directly:

```python
# Query state as of a specific timestamp — useful for data quality audits
con.execute("""
    SELECT * FROM iceberg_scan(
        's3://chakra-lakehouse/gold/order_daily_summary/',
        version = '2024-03-14T00:00:00+00:00'
    )
""")
```

**Reproducibility for readers.** The reference implementation must be runnable without AWS credentials for local validation. DuckDB can query local Iceberg tables (parquet files on disk) with identical SQL. No other option in the list supports this.

**Performance is sufficient for this use case.** DuckDB's vectorized columnar execution is faster than Trino for queries that fit in single-node memory. Benchmark reference: TPC-H SF100 (100GB) completes in seconds on a developer laptop. Gold-layer summary tables are typically small — the raw volume is in Bronze/Silver.

---

## Production-Scale Path: AWS Athena

DuckDB's constraints make it unsuitable in two scenarios:

**Scenario 1 — Concurrent multi-user workloads.** DuckDB is in-process: each Python process has its own DuckDB instance. There is no shared server to route concurrent queries through. MotherDuck (the commercial serverless DuckDB offering) addresses this partially, but it introduces a vendor dependency not present in the reference implementation.

**Scenario 2 — Gold-layer volume exceeds single-node capacity.** DuckDB can spill to disk but is not a distributed engine. Queries that scan hundreds of gigabytes of Gold-layer data will be slower than Athena, which distributes the scan across workers automatically.

### When to migrate to Athena

Migrate from DuckDB to Athena when **any** of the following is true:

| Threshold | Indicator |
|---|---|
| Concurrent analyst count > 10 simultaneous queries | DuckDB per-process isolation causes resource contention |
| Gold-layer table scan volume > 50GB per query regularly | Single-node execution time exceeds analyst tolerance |
| BI tool integration required (Tableau, Superset, Grafana datasource) | Athena has native JDBC/ODBC drivers; DuckDB does not have a server-side JDBC driver |
| Audit-grade query logging required | Athena logs every query to CloudTrail; DuckDB does not |

### Athena migration cost

Because the serving layer is decoupled from the lakehouse, the migration cost is low:

1. Register the Iceberg tables in AWS Glue Data Catalog (Athena's metastore). PyIceberg can do this in one call per table.
2. Replace DuckDB SQL with Athena SQL (syntax is compatible; `iceberg_scan()` calls become standard `SELECT FROM table_name`).
3. Update IAM roles to grant Athena query permissions on the S3 bucket.

**No changes to the Flink pipeline, Debezium connectors, or Iceberg table structure.**

### Athena is not in scope for this reference implementation

Athena requires an AWS account and imposes per-query charges. Including it as the default serving layer would make the reference implementation non-reproducible for readers without AWS credentials. DuckDB with a local Iceberg test dataset is fully reproducible. The Athena path is documented here and in `serving/athena/README.md` for teams that need it.

---

## Consequences

**Positive:**
- No serving-layer infrastructure to operate or pay for
- Local development works without AWS credentials (local Iceberg test fixtures)
- Time travel queries work identically in local and S3 configurations
- Analysts install one package (`pip install duckdb`) and query immediately

**Negative:**
- Not suitable for concurrent multi-user BI workloads out of the box
- No server-side JDBC/ODBC — BI tools cannot connect directly
- In-process state: two Python processes cannot share a single DuckDB instance

---

## Related

- [ADR-0004](ADR-0004-apache-iceberg.md) — Why Iceberg; the serving-layer/storage decoupling
- `serving/duckdb/catalog.py` — DuckDB + PyIceberg catalog initialization
- `serving/duckdb/queries/` — Reference SQL queries against Gold-layer tables
- `serving/athena/README.md` — Athena migration guide and query examples
