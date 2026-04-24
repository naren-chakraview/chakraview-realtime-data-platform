# ADR-0003: Debezium for Change Data Capture

**Status**: Accepted
**Date**: 2024-04-01
**Deciders**: Data platform team

---

## Context

The Chakra Commerce transactional databases (PostgreSQL for Orders, Inventory, Customers; Oracle for the legacy WMS) must stream changes into Kafka without polling the application tables. Three approaches were considered:

1. **Application-level dual-write**: The application writes to both its database and Kafka.
2. **Polling (JDBC source connector)**: A Kafka connector queries the database on a schedule using a `updated_at` timestamp column.
3. **WAL-based CDC (Debezium)**: A connector reads the database's write-ahead log and emits every committed transaction as an event.

---

## Decision

Use **Debezium** with WAL-based CDC for all source databases.

**Against dual-write**: Application teams are not responsible for the analytics pipeline. Requiring every microservice to emit Kafka events for the data platform couples the application deployment to the analytics deployment. It also cannot capture deletes or schema changes reliably.

**Against polling**: Polling misses deletes (unless a soft-delete pattern is used), has inherent latency bounded by the poll interval, and imposes read load on the production database. It also cannot observe the before/after state of an update — only the final state.

**For Debezium**: WAL reading has near-zero production database impact (replication slot reads are sequential I/O). It captures inserts, updates, and deletes with full before/after state. Initial snapshot mode bootstraps the Bronze layer with the full current table state before switching to log-based streaming. Schema changes in the source database are reflected in Debezium events automatically.

---

## Connector Configuration Decisions

**Replication slot per connector**: Each Debezium connector uses a dedicated PostgreSQL replication slot. A shared slot would block the WAL from being discarded if one connector falls behind.

**Heartbeat events**: Debezium emits a heartbeat event every 30 seconds on idle tables. Without heartbeats, a replication slot on a low-traffic table blocks WAL recycling indefinitely, growing the WAL unboundedly.

**SMTs (Single Message Transforms)**: Applied at the connector level to:
- Extract the `after` image for inserts/updates (the Bronze layer stores the full envelope, but downstream consumers rarely need `before`)
- Add a `source_db` field for multi-source fans
- Route to topic names matching `contracts/schemas/` naming conventions

**Offset storage**: Connector offsets are stored in a dedicated Kafka topic (`_debezium_offsets`), not in Kafka Connect's default topic, so they survive connector restarts and cross-cluster migrations.

---

## Consequences

**Positive:**
- Sub-second latency from source commit to Kafka message.
- No application changes required. Data platform team owns the connector; application teams own the schema.
- Captures DDL changes: if a column is added in the source, Debezium reflects it in the next event without connector restart.
- Initial snapshot mode provides a complete historical baseline before log-based streaming begins.

**Negative:**
- Requires a PostgreSQL replication slot (and `wal_level = logical`). This is a production database configuration change — requires DBA approval and has a small WAL volume overhead.
- The Oracle connector requires the LogMiner API, which is licenced separately in some Oracle editions. Verify licensing before enabling.
- Debezium runs in Kafka Connect (distributed mode). Kafka Connect itself is a service that must be operated, monitored, and sized.

---

## Related

- `pipeline/debezium/orders-source-connector.json` — PostgreSQL connector for the Orders database
- `pipeline/debezium/inventory-source-connector.json` — PostgreSQL connector for Inventory
- `contracts/schemas/` — Avro schemas that the Bronze layer must produce
- [ADR-0006](ADR-0006-avro-schema-registry.md) — Schema Registry integration for Debezium output
