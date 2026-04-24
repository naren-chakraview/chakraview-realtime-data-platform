# ADR-0007: Apache Flink for Stream Processing

**Status**: Accepted
**Date**: 2024-04-01
**Deciders**: Data platform team

---

## Context

The pipeline needs a stream processing engine to transform Bronze → Silver (validation, deduplication, DLQ routing) and to run stateful operations (event-time joins, windowed aggregations) that dbt cannot express as SQL transformations on a live stream.

---

## Decision

Use **Apache Flink** (PyFlink API) with **exactly-once end-to-end semantics** via Flink checkpointing + Kafka transactions + Iceberg transactional commits.

**Against Kafka Streams**: Kafka Streams is tightly coupled to Kafka and runs within the application JVM — no separate cluster. It is appropriate for lightweight stateless transformations (routing, filtering, enrichment from a local lookup table). This pipeline requires stateful joins across streams and event-time windowing with late-event handling; these are Flink's core strengths. Kafka Streams is used in this architecture for the lightweight Bronze routing step before Flink picks up the data.

**Against Spark Structured Streaming**: Spark's micro-batch model introduces inherent latency (minimum ~100ms per batch). Flink's continuous streaming model can achieve sub-second Silver layer latency. Flink's state backend (RocksDB) is better suited to long-running stateful jobs than Spark's in-memory state.

---

## Exactly-Once Semantics

The end-to-end exactly-once guarantee requires three coordinated components:

1. **Kafka source**: Flink reads with `COMMITTED` isolation — only reads offsets from transactions that have been committed by Debezium's Kafka producer.
2. **Flink checkpoint**: Distributed snapshot of all operator state, committed at configurable intervals (default: 60 seconds). On failure, Flink restores to the last checkpoint and replays unprocessed records.
3. **Iceberg sink**: Flink uses the Iceberg `FlinkSink` with `exactly-once` mode. Uncommitted Iceberg files are tracked in Flink's checkpoint state; they are committed atomically when the checkpoint completes. If the job fails before commit, the incomplete files are abandoned (Iceberg's orphan file cleanup handles them).

This means: a Flink job failure followed by a restart replays records since the last checkpoint but does not produce duplicate rows in Iceberg. The Kafka offset is only advanced when the checkpoint that includes those records is committed.

---

## Watermarking and Late Events

Silver and Gold jobs use **event-time processing** with watermarks derived from the `event_timestamp` field in each Avro event (not Kafka ingestion time).

Watermark strategy: `BoundedOutOfOrdernessWatermarkStrategy` with a configurable `max_out_of_orderness` (default: 5 minutes). Events that arrive more than 5 minutes after the watermark are routed to a side output stream and written to the DLQ table in Bronze with a `late_arrival` flag. They are not silently dropped.

The 5-minute tolerance is derived from the p99 Kafka consumer lag SLA in `contracts/data-products/`. If the lag SLA tightens, the late-event tolerance must be re-evaluated.

---

## State Backend

RocksDB state backend with incremental checkpointing to S3. RocksDB spills state to disk, enabling stateful joins across millions of keys without heap pressure. Incremental checkpoints (only changed state blocks are uploaded) keep checkpoint duration proportional to the change rate, not the total state size.

---

## Consequences

**Positive:**
- Sub-second Bronze → Silver latency achievable with 60-second checkpoint intervals.
- RocksDB state backend handles joins across the full order history without heap pressure.
- Late events are captured in the DLQ, not silently dropped — data quality is auditable.

**Negative:**
- Flink requires a Job Manager and Task Managers (EKS Deployment + StatefulSet). Operational complexity is higher than Kafka Streams.
- RocksDB checkpoint size on S3 grows with state size. A deduplication job that tracks all order IDs ever seen will have large state. State TTL must be configured per job.
- PyFlink's type system is less expressive than the Java/Scala API. Complex CEP (Complex Event Processing) patterns are better expressed in Java.

---

## Related

- [ADR-0001](ADR-0001-kappa-architecture.md) — Kappa: Flink is the single processing path
- [ADR-0002](ADR-0002-medallion-architecture.md) — Flink implements Bronze→Silver; dbt implements Silver→Gold
- `pipeline/flink/silver_layer_job.py` — Reference PyFlink job: Bronze → Silver with deduplication and DLQ
- `infrastructure/helm/charts/flink/` — Flink Job Manager + Task Manager Helm chart
