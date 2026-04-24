# ADR-0002: Medallion Architecture (Bronze / Silver / Gold)

**Status**: Accepted
**Date**: 2024-04-01
**Deciders**: Data platform team

---

## Context

Within the single Kappa streaming path (ADR-0001), data passes through successive transformation stages. Each stage has different quality guarantees, different consumers, and different retention requirements. Without an explicit layering model, transformation logic accumulates in a single Flink job and becomes impossible to reprocess selectively.

---

## Decision

Adopt the **Medallion Architecture** with three Iceberg table layers:

### Bronze — Raw CDC events

- **Content**: Exactly what Debezium captured from the WAL, converted to Avro and landed verbatim. No transformations, no filtering, no deduplication.
- **SLA**: Written within 30 seconds of source commit. This is the ingestion freshness SLA.
- **Schema**: Matches `contracts/schemas/*.avsc` exactly. Schema evolution is handled by the Schema Registry; Bronze tables absorb it transparently.
- **Consumers**: Silver Flink job only. No analysts or BI tools query Bronze directly.
- **Retention**: 90 days of Iceberg snapshots (supports full Silver reprocessing).
- **Location**: `s3://chakra-lakehouse/bronze/{source_table}/`

### Silver — Validated and deduplicated

- **Content**: Bronze events after: (1) schema validation, (2) deduplication by primary key + CDC sequence, (3) rejection of malformed records to the DLQ, (4) type coercions and null normalisation.
- **SLA**: Written within 2 minutes of Bronze landing. Silver is the layer that data quality SLAs are measured against.
- **Schema**: Domain-canonical types. Debezium's `before`/`after` envelope is unwrapped; only the `after` state is stored (with `op` type for deletes).
- **Consumers**: Gold Flink job, dbt models, and data quality validation scripts.
- **Retention**: 365 days of Iceberg snapshots.
- **Location**: `s3://chakra-lakehouse/silver/{domain}/{entity}/`

### Gold — Aggregated and serving-ready

- **Content**: Business-level aggregations and joined views built from Silver by dbt models. Pre-computed to serve analyst query latency SLAs.
- **SLA**: Written within 5 minutes of a Silver update. This is the data freshness SLA visible to consumers of the data product.
- **Schema**: Aligned to `contracts/data-products/` — the Gold schema is the public interface of a data product.
- **Consumers**: DuckDB (analysts, notebooks), AWS Athena (BI tools at scale), downstream ML feature stores.
- **Retention**: 730 days of Iceberg snapshots (2 years for analytics and compliance).
- **Location**: `s3://chakra-lakehouse/gold/{data_product}/{model}/`

---

## Layer Isolation Rule

A consumer of the Gold layer must never need to know that Bronze or Silver exist. A consumer of Silver must never need to know the Debezium CDC format. Each layer is a stable interface. If the Debezium schema changes, only the Bronze→Silver Flink job is affected; Gold consumers are unchanged.

---

## Consequences

**Positive:**
- Selective reprocessing: a Silver bug only requires replaying Bronze → Silver, not Bronze → Gold.
- Clear data quality boundary: Silver is where validation happens; Bronze is a complete audit record.
- Gold is the only layer with a consumer SLA. Bronze and Silver are internal pipeline concerns.

**Negative:**
- Storage costs are roughly 3× a single-layer approach. Mitigated by Iceberg's compaction and expiry (Bronze compacts hourly; snapshots older than 90 days are expired).
- Three layers means three sets of Iceberg tables to monitor for write latency, schema drift, and compaction health.

---

## Related

- [ADR-0001](ADR-0001-kappa-architecture.md) — Kappa: single streaming path that feeds all three layers
- [ADR-0004](ADR-0004-apache-iceberg.md) — Iceberg: the storage format for all three layers
- `contracts/data-products/` — Gold schema definitions and freshness SLAs
- `pipeline/flink/silver_layer_job.py` — Bronze → Silver Flink job
- `pipeline/dbt/` — Silver → Gold dbt models
