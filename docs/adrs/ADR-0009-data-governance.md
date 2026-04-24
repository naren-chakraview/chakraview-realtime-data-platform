---
title: ADR-0009 Data Governance — Quality, Observability, Lineage
description: Architecture decision for data quality checks, operational observability, and column-level lineage in the streaming lakehouse.
tags: [adrs]
---

# ADR-0009: Data Governance — Quality, Observability, and Lineage

| | |
|---|---|
| **Status** | Accepted |
| **Date** | 2024-03-20 |
| **Deciders** | Platform Team, Domain Teams |
| **Supersedes** | — |

---

## Context

The platform has pipeline-level observability (freshness SLOs, DLQ rate alerts) but no systematic answer to three governance questions:

1. **Quality**: Is the data in each layer *correct* by business rules — not just present and timely?
2. **Observability**: Are volumes and distributions behaving *as expected* compared to historical baselines?
3. **Lineage**: If a Gold column has bad values, *which source field caused it*, and what transformations were applied?

These questions become critical at scale. A freshness SLA can be green while 6% of Silver records silently fail the `total_mismatch` business rule. Volume can be on-time while carrying an upstream price calculation bug. Without lineage, fixing a Gold column defect requires manually tracing through Flink code, dbt SQL, and the Avro schema.

### What was considered

| Approach | Pros | Cons | Verdict |
|---|---|---|---|
| **Great Expectations / Soda Core** | Rich rule catalog, HTML reports | External service, data leaves lakehouse for profiling, separate data contract | Rejected — adds a third contract format |
| **Monte Carlo / Acyl (commercial)** | Full suite, auto anomaly detection | Cost, vendor lock-in, no column-level lineage in open plans | Rejected |
| **dbt tests only** | Already in stack, easy to add | Gold-only (no Bronze/Silver), batch (not streaming), no lineage | Rejected as sole solution |
| **Governance Iceberg tables + DuckDB** | Single storage layer, queryable by existing tooling, no new services | Requires custom quality profiler, lineage emitter | **Accepted** |
| **OpenLineage + Marquez** | Open standard, Flink/dbt native | Requires running Marquez server | **Accepted as optional layer** on top of Iceberg storage |

---

## Decision

Store all governance data **in the lakehouse as Iceberg tables** at `s3://chakra-lakehouse/governance/`. Use existing DuckDB to query them. This keeps governance data co-located with the pipeline data it describes, queryable with the same tools analysts already use, and auditable with the same time-travel capabilities as the medallion layers.

Three governance layers are added:

### 1. Data Quality — `governance/quality_results/`

Quality rules are declared in `governance/quality/quality_rules.yaml` (human-authored, same discipline as `contracts/`). The `QualityProfiler` runs as a 60-second tumbling window side output in the Silver Flink job, evaluating every rule from the YAML against the batch. Results include:

- `total_record_count` / `passed_record_count` / `failed_record_count` per check per window
- `quality_score` (0.0–1.0): the fraction of records passing the check
- `threshold_met`: whether the score meets the contract-defined threshold
- `sample_failure_ids`: up to 10 `event_id` values for DLQ join

Quality checks are **non-gating** — they do not delay or block the Silver write path. They run in a separate window branch. A failing quality check fires a Prometheus alert but does not stop the pipeline; stopping the pipeline would violate the availability SLA.

Quality dimensions follow ISO/IEC 25012:
- **Completeness**: required fields present and non-null
- **Validity**: values conform to business rules (ranges, enums, cross-field consistency)
- **Consistency**: related fields agree with each other (item sum = total)
- **Timeliness**: data arrives within expected windows

### 2. Data Observability — `governance/observability_metrics/`

The `VolumeMonitor` runs every 15 minutes as a standalone job. It reads each medallion layer and computes per-partition metrics:

- `row_count` — total records in the partition
- `null_rate_{column}` — fraction of nulls per column
- `mean_{column}`, `stddev_{column}`, `p95_{column}` — distribution stats for numeric columns
- `distinct_count_{column}` — approximate cardinality (HyperLogLog)

Anomaly detection uses Z-score against a 7-day rolling baseline:
- `|z_score| > 3.0` → `anomaly_detected = TRUE` → Prometheus alert fires
- `2.0 < |z_score| ≤ 3.0` → Grafana amber warning, no page

The `SchemaDriftDetector` runs hourly. It compares the current Iceberg schema fingerprint to the last known fingerprint and emits structured drift events to `governance/schema_drift_events/`. Drift types follow Iceberg v2 safety rules: `COLUMN_ADDED` and `RENAME` are safe; `TYPE_CHANGED` is potentially unsafe (only widening promotions are safe in Iceberg v2); `COLUMN_DROPPED` is safe for Iceberg files but breaks downstream SQL.

### 3. Data Lineage — `governance/lineage_events/`

The `OpenLineageEmitter` is integrated into the Flink Silver job and (via `dbt-openlineage`) into dbt Gold model runs. It emits `OpenLineageDatasetFacet` events at `START` and `COMPLETE` with column-level lineage via `ColumnLineageDatasetFacet`.

Column-level lineage maps are declared in `governance/lineage/openlineage_emitter.py`:
- `KAFKA_TO_SILVER_LINEAGE`: maps each Kafka Avro field to its Silver column with the transformation type (`direct_copy`, `expression:len(items)`, etc.)
- `SILVER_TO_GOLD_LINEAGE`: maps Silver columns to Gold columns with aggregation types (`aggregate_sum`, `aggregate_count_distinct`, etc.)

Events are stored in Iceberg (queryable by DuckDB) **and optionally forwarded to a Marquez backend** (via `MARQUEZ_URL` env var) for visual graph rendering. The Iceberg storage is the source of truth; Marquez is a read view.

---

## Consequences

### Positive

- **Single storage layer**: governance data is in the same Iceberg/S3 store as pipeline data — same backup, same retention, same access controls
- **DuckDB queryable**: the quality waterfall, volume anomaly, and lineage graph queries all run in DuckDB with `iceberg_scan()` — no new query engine
- **Non-gating quality**: quality checks add observability without adding latency or availability risk to the Silver write path
- **OpenLineage standard**: if the team later adopts DataHub or Apache Atlas, lineage events can be replayed from Iceberg into those systems

### Negative / Tradeoffs

- **No UI out of the box**: Marquez provides a graph UI, but only if deployed. Without it, lineage is SQL-only (which is sufficient for the reference implementation, but less discoverable)
- **Quality rules are code-co-located**: `quality_rules.yaml` is in the platform repo, not in each domain team's repo. A domain team must open a PR to add quality rules. This is intentional (platform team reviews the threshold) but adds friction
- **Z-score baseline needs warmup**: the first 7 days of data produce no anomaly signals. The baseline is empty; the monitor logs but does not alert

### Invariants

- Quality checks must never gate the Silver write path
- Every quality rule must have a `threshold` field — no threshold means the rule is never evaluated for SLO compliance
- Column lineage maps must be updated whenever `_to_silver()` or a dbt model changes a column mapping
- `governance/` tables follow the same 90-day snapshot retention as Bronze (they are audit data)
