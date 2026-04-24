# ADR-0001: Kappa Architecture over Lambda

**Status**: Accepted (updated 2024-09-12 — Kafka references updated to Redpanda)
**Date**: 2024-04-01
**Deciders**: Data platform team

---

## Context

Two reference architectures exist for real-time data platforms:

**Lambda Architecture** — Two parallel paths: a real-time streaming path (low latency, approximate) and a batch reprocessing path (high latency, accurate). Results are merged at query time. The batch layer periodically overwrites streaming output with a "correct" version.

**Kappa Architecture** — A single streaming path. The stream is the system of record. Reprocessing is achieved by replaying the Kafka log from the beginning with a new or updated job. There is no separate batch layer.

Lambda was the dominant pattern circa 2015 when streaming engines were immature and could not produce accurate results at low latency. Apache Flink changed this: it provides exactly-once semantics, event-time processing, and watermarking that make the batch correctness argument obsolete.

The Kappa reprocessing guarantee depends critically on the message log being **replayable from an arbitrary offset** with **configurable retention**. This requirement informed the message bus choice (ADR-0003): Redpanda provides both, with S3 tiered storage keeping 30 days of topic history without proportionally large broker disk.

---

## Decision

Adopt **Kappa Architecture**. A single Apache Flink pipeline processes all events from source to Iceberg. There is no batch layer and no merge step.

Reprocessing is handled by:
1. Resetting the Debezium connector offset to the beginning of the Redpanda topic (for CDC re-reads)
2. Submitting a new Flink job that reads from the earliest Redpanda offset and writes to a new Iceberg table version
3. Swapping the Gold-layer table pointer in the catalog after validation

---

## Consequences

**Positive:**
- One codebase, one mental model. There are no two implementations of the same business logic to keep in sync.
- Flink's exactly-once checkpointing eliminates the accuracy argument for a separate batch layer.
- Iceberg's time travel means historical snapshots are always available without a separate batch store.
- Operational surface is half of Lambda: one pipeline to monitor, one set of SLAs to define.

**Negative:**
- Reprocessing requires Redpanda topic retention sufficient to cover the reprocessing window. This repo uses 30-day retention on all source topics (configured in `contracts/data-products/`). Redpanda's S3 tiered storage keeps this retention window affordable — older segments are offloaded to object storage while remaining accessible to consumers.
- A Flink job bug that corrupts the Silver or Gold layer requires a full reprocessing run from Redpanda. With Lambda, the batch layer would catch this at the next scheduled run.
- Stateful Flink jobs (joins, aggregations) must manage state store size. Lambda's batch path has no state management concern.

---

## Related

- [ADR-0003](ADR-0003-debezium-cdc.md) — Redpanda as message bus; why offset replay and tiered storage were required by Kappa
- [ADR-0007](ADR-0007-flink-stream-processing.md) — Why Flink; exactly-once semantics
- [ADR-0002](ADR-0002-medallion-architecture.md) — Bronze/Silver/Gold layers within the single stream
- `contracts/data-products/` — Redpanda topic retention requirements per data product
