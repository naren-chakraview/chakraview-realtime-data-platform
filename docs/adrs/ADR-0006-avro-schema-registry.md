# ADR-0006: Avro with Confluent Schema Registry

**Status**: Accepted
**Date**: 2024-04-01
**Deciders**: Data platform team

---

## Context

Kafka topics carry structured events from multiple producers (Debezium connectors, application services) to multiple consumers (Flink jobs, Bronze writers). Without schema enforcement at the Kafka level, a producer can change its event structure and silently break consumers.

---

## Decision

Use **Avro** as the wire format for all Kafka topics, governed by **Confluent Schema Registry** with **BACKWARD_TRANSITIVE** compatibility mode.

**Why Avro over JSON**: JSON schemas are not enforced at the Kafka level; a producer can send any JSON. Avro binary encoding is smaller (no field names on every message), and the Schema Registry enforces that every message is valid against a registered schema before it reaches the broker.

**Why BACKWARD_TRANSITIVE compatibility**: A consumer running schema version N must be able to read messages written with any version ≤ N. This means:
- Adding a field with a default value is allowed.
- Removing a field is allowed only if it had a default.
- Renaming a field is not allowed (use an alias instead).
- Changing a field type is not allowed.

This is stricter than `BACKWARD` (which only checks N against N-1). `BACKWARD_TRANSITIVE` ensures that any consumer can be upgraded independently of the producer, and can read the full Kafka log history without needing to know which schema version wrote each message.

---

## Schema Naming Convention

Schemas live in `contracts/schemas/` and follow the naming pattern `{event-name}-v{major}.avsc`. The major version increments only on breaking changes (which require a migration plan). Minor additions are handled within the same major version via Avro's field-addition-with-default mechanism.

---

## Consequences

**Positive:**
- Schema evolution is governed: a producer cannot push a breaking change without a Schema Registry version bump that triggers CI validation.
- Consumers use the schema ID embedded in the Avro message header to fetch the exact schema version — no out-of-band schema distribution.
- Iceberg Bronze tables store Avro-decoded Parquet, so the schema lineage from source → Bronze → Silver is fully traceable.

**Negative:**
- Schema Registry is a required service. Its availability is a prerequisite for all producers and consumers. A Schema Registry outage halts all new Kafka writes.
- The Confluent Schema Registry is source-available (BSL licence since 2023). The open-source alternative is Apicurio Registry. This reference implementation uses the Confluent implementation but documents the Apicurio migration path in `infrastructure/helm/charts/schema-registry/`.

---

## Related

- `contracts/schemas/` — All Avro schema files (human-authored)
- [ADR-0003](ADR-0003-debezium-cdc.md) — Debezium serializes events using these schemas
- [ADR-0007](ADR-0007-flink-stream-processing.md) — Flink jobs deserialize using these schemas
