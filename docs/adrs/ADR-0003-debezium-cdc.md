# ADR-0003: Debezium for Change Data Capture + Redpanda as Message Bus

**Status**: Accepted (updated 2024-09-12 to adopt Redpanda)
**Date**: 2024-04-01
**Deciders**: Data platform team

---

## Context

The Chakra Commerce transactional databases (PostgreSQL for Orders, Inventory, Customers; Oracle for the legacy WMS) must stream changes into a message bus without polling the application tables. Two questions were answered independently:

1. **How do we capture changes from the source database?** (ingestion pattern)
2. **What message bus receives and holds those changes?** (broker selection)

---

## Decision 1: Debezium for Change Data Capture

Three ingestion approaches were considered:

1. **Application-level dual-write**: Application writes to both its database and the broker.
2. **Polling (JDBC source connector)**: A connector queries the database on a schedule using an `updated_at` timestamp column.
3. **WAL-based CDC (Debezium)**: A connector reads the database's write-ahead log and emits every committed transaction as an event.

**Against dual-write**: Application teams are not responsible for the analytics pipeline. Coupling every microservice to the data platform creates deployment dependencies and cannot capture deletes or schema changes reliably.

**Against polling**: Polling misses hard deletes entirely (unless a soft-delete pattern is used), has inherent latency bounded by the poll interval, imposes read load on the production database, and cannot observe the `before` state of an update — only the final state.

**For Debezium**: WAL reading has near-zero production database impact (replication slot reads are sequential I/O). It captures inserts, updates, and deletes with full before/after state. Initial snapshot mode bootstraps the Bronze layer with complete current table state before switching to log-based streaming. Schema changes in the source database are reflected automatically.

### When WAL-based CDC is not available

Not every source database supports WAL-based CDC. The decision tree for non-CDC sources is documented in [ADR-0010](ADR-0010-non-cdc-ingestion-patterns.md).

---

## Decision 2: Redpanda as Message Bus

### Alternatives Considered

| Broker | Protocol | Schema Registry | Ops complexity | Verdict |
|---|---|---|---|---|
| **Apache Kafka (self-managed)** | Kafka native | External (Confluent/Apicurio) | High — ZooKeeper (pre-3.x) or KRaft quorum + broker fleet + Connect cluster | Rejected |
| **AWS MSK (managed Kafka)** | Kafka native | External (Glue Schema Registry or MSK Serverless SR) | Medium — AWS manages brokers, still need Connect | Considered |
| **Redpanda** | Kafka-compatible | **Built-in** (Confluent-compatible API) | Low — single binary, no ZooKeeper, no KRaft quorum, no separate SR service | **Selected** |
| **Apache Pulsar** | Pulsar + KoP | Built-in | High — stateless brokers + Apache BookKeeper cluster + ZooKeeper | Rejected |
| **AWS Kinesis** | Kinesis | None | Low (managed) | Rejected — see below |
| **RabbitMQ** | AMQP | None | Low | Rejected — see below |

### Why Redpanda over Kafka

Redpanda implements the full Kafka wire protocol (Kafka API v0–v13). Every Kafka client — Debezium Kafka Connect, Flink's `KafkaSource`, `KafkaSink`, the Confluent Avro serializer — connects to Redpanda without code changes. The migration path is a bootstrap server address change.

The operational advantage is significant:

- **No ZooKeeper or KRaft quorum to operate**: Redpanda uses the Raft consensus protocol internally; there is no separate coordination layer.
- **Built-in Schema Registry**: Redpanda ships with a Confluent-compatible Schema Registry on port 8081. There is no separate SR service to deploy, monitor, or patch. The `SCHEMA_REGISTRY_URL` environment variable simply points at the Redpanda broker.
- **Tiered storage to S3 built-in**: Kafka requires Confluent Tiered Storage or a third-party plugin for S3 offload. Redpanda's tiered storage is a first-class feature — this enables the 30-day Kappa reprocessing window without proportionally large local disk.
- **Single binary**: Redpanda runs as a single `rpk` binary per node. The operator surface area is substantially smaller than a Kafka + ZooKeeper + Schema Registry + Kafka Connect stack.

Performance characteristics:
- Tail latency (p99): Redpanda's C++ implementation avoids JVM GC pauses that cause Kafka's p99 latency spikes under backpressure. At the throughput volumes this platform operates, the difference is measurable but not the primary reason for selection.

### Why not Pulsar

Pulsar's compute/storage separation (stateless brokers + Apache BookKeeper) is architecturally appealing for independent scaling. However:

- BookKeeper is a non-trivial distributed system to operate alongside Redpanda or Kafka
- Debezium's Pulsar connector is less mature than its Kafka connector; the Kafka-on-Pulsar (KoP) compatibility layer introduces a translation layer with subtle behavioral differences
- Flink's Pulsar connector has fewer production examples for the exactly-once + Iceberg commit pattern

Pulsar is worth re-evaluating if multi-datacenter active-active geo-replication becomes a requirement (Pulsar's replication is first-class; Kafka/Redpanda require MirrorMaker 2).

### Why not Kinesis

AWS Kinesis has a maximum retention of 7 days (extendable to 365 days at significant cost). The data product contract specifies `retention_days: 30` for Kappa reprocessing. More critically, Kinesis shards are fixed at creation and cannot be re-partitioned without re-creating the stream. The Kappa reprocessing requirement (reset consumer group to arbitrary offset, replay) is supported but the operational model differs significantly from the Kafka API.

### Why not RabbitMQ

RabbitMQ is a traditional message queue: once a message is consumed it is deleted. There is no concept of an offset log or consumer group reset. The Kappa architecture's reprocessing guarantee — replay from the start of the affected window with a fixed job — is architecturally impossible with RabbitMQ.

---

## Connector Configuration Decisions

**Replication slot per connector**: Each Debezium connector uses a dedicated PostgreSQL replication slot. A shared slot would block the WAL from being discarded if one connector falls behind.

**Heartbeat events**: Debezium emits a heartbeat event every 30 seconds on idle tables. Without heartbeats, a replication slot on a low-traffic table blocks WAL recycling indefinitely, growing the WAL unboundedly.

**SMTs (Single Message Transforms)**: Applied at the connector level to:
- Extract the `after` image for inserts/updates (Bronze stores the full envelope, but downstream consumers rarely need `before`)
- Add a `source_db` field for multi-source fans
- Route to topic names matching `contracts/schemas/` naming conventions

**Offset storage**: Connector offsets are stored in a dedicated Redpanda topic (`_debezium_offsets`), not in Kafka Connect's default topic, so they survive connector restarts and cross-cluster migrations.

---

## Consequences

**Positive:**
- Sub-second latency from source commit to Redpanda topic.
- No application changes required. Data platform team owns the connector; application teams own the schema.
- Built-in Schema Registry eliminates the Confluent BSL licence concern from ADR-0006.
- Tiered storage to S3 reduces broker disk costs for the 30-day Kappa reprocessing window.
- Zero code changes in Flink jobs or Debezium connectors — the Kafka API is identical.

**Negative:**
- Requires a PostgreSQL replication slot (and `wal_level = logical`). This is a production database configuration change — requires DBA approval.
- The Oracle connector requires the LogMiner API, licenced separately in some Oracle editions.
- Debezium runs in Kafka Connect (distributed mode). Kafka Connect is a JVM service that must be operated separately from Redpanda itself. Redpanda does not replace Kafka Connect.
- Redpanda's ecosystem tooling (e.g., Redpanda Console) is newer than Kafka's (Confluent Control Center, Kafka UI). Operational runbook depth is smaller.

---

## Related

- `pipeline/debezium/orders-source-connector.json` — PostgreSQL connector, targets Redpanda
- `infrastructure/redpanda/redpanda-cluster.yaml` — Redpanda Kubernetes StatefulSet
- `contracts/schemas/` — Avro schemas registered in Redpanda's built-in Schema Registry
- [ADR-0006](ADR-0006-avro-schema-registry.md) — Avro + Schema Registry (updated for Redpanda SR)
- [ADR-0010](ADR-0010-non-cdc-ingestion-patterns.md) — Ingestion patterns when WAL CDC is not available
