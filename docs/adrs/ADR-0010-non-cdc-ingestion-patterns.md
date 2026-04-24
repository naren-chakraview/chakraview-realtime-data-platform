# ADR-0010: Ingestion Patterns for Non-CDC Sources

**Status**: Accepted
**Date**: 2024-09-12
**Deciders**: Data platform team

---

## Context

ADR-0003 adopted Debezium WAL-based CDC for all PostgreSQL sources. Not every source database the platform may need to ingest from supports WAL-based CDC. This ADR documents the decision tree and pattern implementations for those cases.

### When WAL CDC is unavailable

| Scenario | Why CDC is unavailable |
|---|---|
| Database has no replication protocol | SQLite, embedded DBs, some proprietary legacy stores |
| Replication privilege not granted | DBA policy prevents `REPLICATION` role on multi-tenant shared databases |
| Oracle without LogMiner licence | Some Oracle editions require a separate LogMiner licence |
| SaaS application database | You have API access but no database access (Salesforce, Workday, etc.) |
| NoSQL store without oplog access | Some managed MongoDB Atlas configurations restrict oplog reads |
| Third-party vendor database | Read-only JDBC credentials only; no WAL access |

---

## Patterns

### Pattern 1: Outbox Pattern (recommended for greenfield services)

The application writes to an **outbox table** in the **same transaction** as the business operation. Debezium captures the outbox table — which is a normal PostgreSQL table with WAL access — rather than the business tables.

```sql
-- Application code: single transaction, no dual-write risk
BEGIN;
  UPDATE orders SET status = 'CONFIRMED', confirmed_at = NOW() WHERE id = :id;
  INSERT INTO outbox (
    id, aggregate_type, aggregate_id, event_type, payload, created_at
  ) VALUES (
    gen_random_uuid(), 'Order', :id, 'OrderConfirmed',
    '{"order_id": "...", "confirmed_at": "..."}',
    NOW()
  );
COMMIT;
-- If the COMMIT fails, both the business update and the outbox row are rolled back.
-- The event is never published without the business operation succeeding.
```

Debezium captures the `outbox` table changes via WAL. The `outbox SMT` (Single Message Transform) unwraps the outbox row into a first-class Redpanda event, routing it to the topic defined in `aggregate_type`.

The outbox table schema is defined in `contracts/schemas/outbox-event-v1.avsc`. The Debezium outbox connector config is in `pipeline/debezium/outbox-connector.json`.

**Characteristics:**
- Exactly-once delivery (database transaction + Debezium at-least-once + deduplication in Silver)
- Captures deletes if the application inserts a `{event_type: "OrderDeleted"}` row
- Requires application code changes — cannot be retrofitted to systems you don't own
- The outbox table schema is the event contract; domain teams author it in `contracts/schemas/`

**When to use**: New services being built, existing services where the team controls the codebase and can add a transaction.

---

### Pattern 2: Timestamp Polling (JDBC connector)

A Kafka Connect JDBC Source Connector queries the source database on a schedule:

```sql
-- Connector executes this every poll.interval.ms (e.g., 30s)
SELECT * FROM orders WHERE updated_at > :last_max_updated_at
ORDER BY updated_at ASC
```

The connector tracks `MAX(updated_at)` as its watermark between polls.

```json
{
  "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
  "connection.url":  "${env:SOURCE_JDBC_URL}",
  "query":           "SELECT * FROM orders WHERE updated_at > ? ORDER BY updated_at ASC",
  "mode":            "timestamp",
  "timestamp.column.name": "updated_at",
  "poll.interval.ms": "30000",
  "topic.prefix":    "chakra.polling."
}
```

**Characteristics:**
- Misses hard deletes entirely — only captures rows that still exist with a changed `updated_at`
- Requires an indexed `updated_at` column on every table being polled
- Maximum freshness is bounded by `poll.interval.ms` — a 30s interval violates the Bronze SLA (≤ 30s from WAL commit)
- Read load on source database scales with poll frequency
- Cannot observe the `before` state of an update

**Fatal limitations for this platform**: The missing-deletes problem means order cancellations and inventory adjustments would not propagate to Bronze. The polling interval also makes the Bronze SLA unachievable.

**When to use**: Read-only analytics sync where deletes either don't occur or are modelled as status changes (`status = 'CANCELLED'` rather than `DELETE`). Acceptable freshness floor of 30+ seconds. Source database cannot be modified (no replication slot, no triggers).

---

### Pattern 3: Database Triggers + Changelog Table

Triggers on `INSERT`, `UPDATE`, and `DELETE` write to a shadow changelog table. A JDBC connector (or custom poller) reads the changelog.

```sql
CREATE TABLE change_log (
  id          BIGSERIAL PRIMARY KEY,
  table_name  TEXT NOT NULL,
  operation   TEXT NOT NULL CHECK (operation IN ('INSERT','UPDATE','DELETE')),
  row_id      TEXT NOT NULL,
  before_json JSONB,
  after_json  JSONB,
  changed_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE OR REPLACE FUNCTION capture_order_change() RETURNS TRIGGER AS $$
BEGIN
  INSERT INTO change_log (table_name, operation, row_id, before_json, after_json)
  VALUES (
    TG_TABLE_NAME,
    TG_OP,
    COALESCE(NEW.id, OLD.id)::TEXT,
    CASE WHEN TG_OP != 'INSERT' THEN row_to_json(OLD) END,
    CASE WHEN TG_OP != 'DELETE' THEN row_to_json(NEW) END
  );
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER orders_change_capture
AFTER INSERT OR UPDATE OR DELETE ON orders
FOR EACH ROW EXECUTE FUNCTION capture_order_change();
```

**Characteristics:**
- Captures inserts, updates, and deletes — does not miss hard deletes
- Portable — works with any database that has trigger support
- Write amplification: every business write triggers an additional changelog write
- Trigger execution is synchronous with the originating transaction — a slow trigger increases transaction latency
- Trigger failure rolls back the originating transaction — must be handled with `EXCEPTION` blocks
- The changelog table grows unboundedly without a purge job

**When to use**: Source database does not support WAL replication but supports triggers (SQL Server without CDC feature enabled, MySQL without binary logging, older PostgreSQL versions). Acceptable write amplification. Cannot change application code.

---

### Pattern 4: Application-Level Event Publishing

Application code publishes events directly to Redpanda in addition to writing to the database.

```typescript
// In the order service — runs after successful DB commit
async function confirmOrder(orderId: string): Promise<void> {
  await db.transaction(async (tx) => {
    await tx.query('UPDATE orders SET status = $1 WHERE id = $2', ['CONFIRMED', orderId]);
    // Outbox row in same transaction — see Pattern 1 for the safer version
  });
  // This publish is outside the transaction — at-least-once, not exactly-once
  await producer.send({
    topic: 'chakra.orders.placed',
    messages: [{ value: JSON.stringify({ order_id: orderId, event_type: 'OrderConfirmed' }) }],
  });
}
```

Without the outbox table, the publish is outside the database transaction. If the process crashes between the `COMMIT` and the `send()`, the event is never published. This is the **dual-write problem** — it can be mitigated but not eliminated without the outbox pattern.

**Characteristics:**
- Event schema is application-defined — domain team has full control
- Dual-write gap: event can be lost if the process crashes between DB commit and broker publish
- Capture completeness depends on all code paths being instrumented — easy to miss a bulk update or admin script
- Cannot capture schema changes, only events the application explicitly publishes

**When to use**: New microservices where exactly-once delivery is not required (or where the Silver deduplication layer can tolerate occasional missing events). Accept in combination with reconciliation to catch gaps.

---

### Pattern 5: Query-Time Federation (No Streaming)

Trino, DuckDB, or Spark reads directly from the source database at query time via a JDBC connector. No events, no streaming.

```sql
-- DuckDB federated query
SELECT * FROM postgres_scan(
  'host=orders-db port=5432 dbname=orders user=reader password=...',
  'public',
  'orders'
)
WHERE order_date >= current_date - INTERVAL 1 DAY;
```

**Characteristics:**
- Zero infrastructure — no broker, no connector
- Not streaming — freshness is bounded by query frequency
- Cannot support event-driven architectures, saga patterns, or real-time downstream consumers
- Read load on source database at query time

**When to use**: Analytics-only use case with no event-driven consumers. Freshness requirement of 15+ minutes. Source database is too sensitive to instrument (e.g., third-party SaaS database read via API). Interim solution while WAL CDC or outbox is being implemented.

---

## Decision Matrix

| Source type | Deletes captured? | Freshness | App changes? | Recommended pattern |
|---|---|---|---|---|
| PostgreSQL with replication role | ✓ | < 1s | No | Debezium WAL CDC (ADR-0003) |
| Any DB — new service, own codebase | ✓ (via event type) | < 1s | Yes | Outbox pattern |
| Any DB — legacy, no code changes possible, deletes matter | ✓ | ~30s | No | Triggers + changelog |
| Any DB — legacy, no code changes, deletes do not matter | ✗ | ≥ 30s | No | Timestamp polling |
| SaaS / API-only source | Depends on API | Varies | No | Application event or scheduled API poll |
| Analytics-only, freshness > 15 min acceptable | N/A | 15+ min | No | Query-time federation |

---

## Outbox Pattern Avro Schema

`contracts/schemas/outbox-event-v1.avsc` defines the canonical outbox envelope. Domain event payloads are embedded as a JSON string in the `payload` field — the outbox table schema is stable even as domain event schemas evolve.

```json
{
  "type": "record",
  "name": "OutboxEvent",
  "namespace": "com.chakraview.outbox.v1",
  "fields": [
    { "name": "id",             "type": { "type": "string", "logicalType": "uuid" } },
    { "name": "aggregate_type", "type": "string" },
    { "name": "aggregate_id",   "type": { "type": "string", "logicalType": "uuid" } },
    { "name": "event_type",     "type": "string" },
    { "name": "payload",        "type": "string",  "doc": "JSON-encoded domain event" },
    { "name": "created_at",     "type": { "type": "long", "logicalType": "timestamp-millis" } },
    { "name": "metadata",       "type": { "type": "record", "name": "OutboxMetadata",
        "fields": [{ "name": "trace_id", "type": "string" }] } }
  ]
}
```

---

## Related

- [ADR-0003](ADR-0003-debezium-cdc.md) — WAL-based CDC for databases that support it
- `pipeline/debezium/orders-source-connector.json` — Reference CDC connector (PostgreSQL)
- `contracts/schemas/outbox-event-v1.avsc` — Outbox envelope schema
