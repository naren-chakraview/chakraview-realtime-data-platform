# ADR-0004: Apache Iceberg for Lakehouse Storage

**Status**: Accepted
**Date**: 2024-04-01
**Deciders**: Data platform team

---

## Context

The streaming pipeline needs a storage format for the Bronze, Silver, and Gold layers (ADR-0002) on S3 that supports:
- ACID writes from Flink (concurrent job tasks writing to the same table)
- Schema evolution without rewriting existing data files
- Time travel queries (analysts can query the table as it was at any point)
- Partition evolution (change partitioning strategy without rewriting data)
- Multiple query engines reading the same table (DuckDB today, Athena tomorrow — ADR-0005)

Options considered: Apache Iceberg, Apache Hudi, Delta Lake.

---

## Decision

Use **Apache Iceberg** (v2 spec) for all three lakehouse layers.

**Against Hudi**: Hudi has excellent upsert performance for CDC workloads but its query engine support is narrower (strong Spark coupling; Flink support is less mature; DuckDB cannot read Hudi natively). Hudi's Copy-on-Write vs Merge-on-Read choice adds operational complexity that Iceberg avoids.

**Against Delta Lake**: Delta Lake has first-class Spark and Databricks support but its ecosystem outside that stack is weaker. DuckDB has no native Delta reader; DeltaRS exists but is an additional dependency. Delta's log format is proprietary to Databricks in some features.

**For Iceberg**: The Iceberg spec is engine-agnostic by design. DuckDB, Athena, Trino, Spark, Flink, PyIceberg — all read and write the same table. The REST catalog spec means any catalog (AWS Glue, Nessie, Polaris) works with any engine. Schema evolution uses field IDs, not field names, so renaming a column does not break existing readers.

---

## Key Configuration Decisions

**Catalog**: AWS Glue Data Catalog as the Iceberg REST catalog. Glue is available in every AWS region, requires no additional infrastructure, and integrates with Athena natively. Nessie (open-source) is documented as an alternative for non-AWS deployments.

**Write distribution mode**: `hash` on the primary key for Silver and Gold (enables compaction to merge small files by key range). Bronze uses `none` (append-only; compaction is by time partition).

**Compaction**: An hourly Flink job runs `RewriteDataFilesAction` to merge small files produced by streaming writes. Without compaction, each Flink checkpoint creates a new file, and scan performance degrades within hours.

**Snapshot expiry**: Bronze: 90 days. Silver: 365 days. Gold: 730 days. Expired snapshots are removed by a daily maintenance job; the underlying Parquet files are deleted from S3.

**Partitioning**:
- Bronze: `days(event_timestamp)` — time-based, matches log retention
- Silver: `days(event_timestamp), identity(entity_id)` — enables key-range compaction
- Gold: Model-specific, defined in `contracts/data-products/`

---

## Consequences

**Positive:**
- Engine portability: switching from DuckDB to Athena requires no data migration, only catalog registration.
- Time travel is a first-class feature: Gold-layer analysts can query any historical snapshot by timestamp or snapshot ID.
- Schema evolution with field IDs: adding a column to the source database propagates through Bronze and Silver without rewriting existing Parquet files.
- Concurrent Flink writers use optimistic concurrency control (OCC) — no global write lock.

**Negative:**
- Small-file problem requires active compaction management. Without the hourly compaction job, scan performance degrades significantly within a day of streaming writes.
- Iceberg v2 row-level deletes (used for CDC deletes) produce delete files that must be compacted into data files on a schedule. Until compaction, readers must merge data files and delete files at query time.
- AWS Glue catalog has eventual consistency on some metadata operations. Concurrent writes from many Flink task managers require retry logic in the Iceberg commit protocol.

---

## Related

- [ADR-0002](ADR-0002-medallion-architecture.md) — Medallion layers that use Iceberg
- [ADR-0005](ADR-0005-duckdb-serving-layer.md) — DuckDB reads Iceberg; Athena as the production-scale alternative
- `pipeline/flink/silver_layer_job.py` — Iceberg sink configuration (catalog, write mode, compaction trigger)
- `infrastructure/terraform/modules/iceberg-s3/` — S3 bucket, Glue catalog, IAM roles
