# ADR-0006: Avro with Schema Registry

**Status**: Accepted (updated 2024-09-12 — SR now provided by Redpanda built-in)
**Date**: 2024-04-01
**Deciders**: Data platform team

---

## Context

Redpanda topics carry structured events from multiple producers (Debezium connectors, application services) to multiple consumers (Flink jobs, Bronze writers). Without schema enforcement at the broker level, a producer can change its event structure and silently break consumers.

---

## Decision

Use **Avro** as the wire format for all Redpanda topics, governed by **Redpanda's built-in Schema Registry** with **BACKWARD_TRANSITIVE** compatibility mode.

### Wire format: Avro over JSON

JSON schemas are not enforced at the broker level — a producer can send any JSON and it will be accepted. Avro binary encoding is smaller (field names are not repeated in every message), and the Schema Registry enforces that every message is valid against a registered schema *before* it reaches the broker.

### Schema Registry: Redpanda built-in

Redpanda ships with a Schema Registry that implements the full Confluent Schema Registry REST API on port 8081. This means:

- Every Confluent-compatible client — `io.confluent.kafka.serializers.KafkaAvroSerializer`, `AvroRowDeserializationSchema` in Flink, the `avro-serde` library — connects to Redpanda's SR without code changes. `SCHEMA_REGISTRY_URL` points at the Redpanda broker address.
- There is no separate Schema Registry service to deploy, monitor, patch, or size. The SR lifecycle is tied to the Redpanda cluster, not to a separate JVM service.
- The Confluent BSL (Business Source Licence) concern that applied to self-managed Confluent Schema Registry does not apply — Redpanda's Schema Registry is Apache 2.0 licensed.

The Apicurio Registry remains a viable alternative if the platform is deployed without Redpanda (e.g., on AWS MSK). The API is compatible; only the `SCHEMA_REGISTRY_URL` changes.

### Compatibility mode: BACKWARD_TRANSITIVE

A consumer running schema version N must be able to read messages written with any version ≤ N. This enables:

- Consumers to upgrade independently of producers — a consumer on v3 can still read messages that were written when the schema was at v1
- Full Kafka log replay (Kappa reprocessing) without needing to know which schema version wrote each message
- Safe schema evolution rules: **add a field with a default** (allowed), **remove a field with a default** (allowed), **rename a field** (use an alias, not allowed directly), **change a field type** (not allowed)

`BACKWARD_TRANSITIVE` is stricter than `BACKWARD` (which only checks version N against N−1). The stricter mode is required for the Kappa reprocessing guarantee — replaying from the earliest offset must always succeed regardless of how many schema evolutions have occurred since that message was written.

---

## Schema Naming Convention

Schemas live in `contracts/schemas/` and follow the naming pattern `{event-name}-v{major}.avsc`. The major version increments only on breaking changes (which require a migration plan). Minor additions are handled within the same major version via Avro's field-addition-with-default mechanism.

The `contracts/schemas/` files are the source of truth. Registration into Redpanda's Schema Registry happens at connector deployment time via the `tooling/register-schemas.sh` script.

---

## Consequences

**Positive:**
- Schema evolution is governed: a producer cannot push a breaking change without a Schema Registry version bump that triggers CI validation.
- Consumers use the schema ID embedded in the Avro message header to fetch the exact schema version from Redpanda's SR — no out-of-band schema distribution.
- Iceberg Bronze tables store Avro-decoded Parquet, so the schema lineage from source → Bronze → Silver is fully traceable.
- No separate SR service to operate — Redpanda's built-in SR reduces the service count in the platform.

**Negative:**
- Schema Registry availability is still a prerequisite for all producers and consumers. Redpanda's SR is embedded in the broker, so a broker-level outage takes down both messaging and schema validation simultaneously. This is a different failure mode from a separate SR service (where the SR could be independently unavailable without affecting message flow). In practice, a Redpanda cluster outage stops the pipeline regardless of the SR, so this distinction rarely matters operationally.
- `BACKWARD_TRANSITIVE` forbids field renames without aliases. Teams must be trained on the alias mechanism or will attempt renames that the SR rejects at write time.

---

## Related

- `contracts/schemas/` — All Avro schema files (human-authored source of truth)
- `tooling/register-schemas.sh` — Registers schemas into Redpanda SR at deployment
- [ADR-0003](ADR-0003-debezium-cdc.md) — Debezium serializes events using these schemas; Redpanda selected as broker
- [ADR-0007](ADR-0007-flink-stream-processing.md) — Flink jobs deserialize using these schemas
