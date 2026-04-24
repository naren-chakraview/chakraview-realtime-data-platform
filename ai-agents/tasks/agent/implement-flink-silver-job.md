# Agent Task: Implement Flink Silver Layer Job

**Task type**: Agent (LLM reasoning required — Persona 5)
**Spec version**: 1.0

---

## Goal

Complete the Bronze → Silver Flink job for the Orders data product. The skeleton in `pipeline/flink/silver_layer_job.py` defines the job structure, operator topology, and deduplication logic. The agent must wire the Iceberg FlinkSink, the Kafka DLQ sink, and the PyIceberg catalog integration.

The job must achieve exactly-once end-to-end semantics per ADR-0007.

---

## Inputs

| File | Why |
|---|---|
| `pipeline/flink/silver_layer_job.py` | Skeleton to complete; contains TODO markers |
| `contracts/data-products/orders-analytics.yaml` | SLAs, DLQ schema, Silver freshness target |
| `contracts/schemas/order-placed-v1.avsc` | Avro schema the Kafka source produces |
| `docs/adrs/ADR-0007-flink-stream-processing.md` | Exactly-once requirements, checkpoint config |
| `docs/adrs/ADR-0004-apache-iceberg.md` | Iceberg sink configuration (catalog, write mode) |
| `ai-agents/context/pipeline-conventions.md` | Naming conventions, metric registration |

---

## Outputs

| File | What to produce |
|---|---|
| `pipeline/flink/silver_layer_job.py` | Complete, runnable PyFlink job (no TODOs remaining) |
| `pipeline/flink/silver_layer_job_test.py` | Unit tests for `ValidateAndDeduplicate` using Flink's `MiniClusterWithClientResource` |
| `pipeline/flink/requirements.txt` | Pinned dependencies (pyflink, pyiceberg, confluent-kafka) |

---

## Implementation Constraints

1. **Iceberg sink**: Use `pyiceberg-flink`'s `FlinkSink` with `write.distribution-mode=hash` on `order_id`. The sink must commit on Flink checkpoint boundaries only — not on a timer.

2. **DLQ sink**: Use `KafkaSink` writing JSON to `chakra.orders.dlq`. Every DLQ record must include all fields from `contracts/data-products/orders-analytics.yaml#dead_letter`. The `raw_payload` field must contain the original Avro bytes, not the decoded dict.

3. **Watermark tolerance**: Read the `max_out_of_orderness` value from the environment variable `FLINK_WATERMARK_TOLERANCE_MINUTES` (default: `5`). Do not hardcode.

4. **State TTL**: The deduplication state TTL must be configurable via `FLINK_DEDUP_STATE_TTL_HOURS` (default: `24`). Use `StateTtlConfig` with `UPDATE_TIME` semantics.

5. **Metrics**: Register these counters via Flink's metrics system (not OTEL — Flink exposes its own Prometheus endpoint):
   - `orders.silver.records_processed`
   - `orders.silver.dlq_routed`
   - `orders.silver.dedup_dropped`

---

## Acceptance Criteria

- [ ] `python pipeline/flink/silver_layer_job.py` starts without error in local mode with `LAKEHOUSE_ENV=local`
- [ ] Unit tests pass: `pytest pipeline/flink/silver_layer_job_test.py`
- [ ] A duplicate event (same `event_id`) routes to DLQ with `failure_reason=duplicate_event_id`
- [ ] An event with mismatched total (`total_amount_cents != sum(items)`) routes to DLQ with `failure_reason=total_mismatch`
- [ ] A valid event appears in the Silver Iceberg table after checkpoint commit
- [ ] No `any` types; all functions have type annotations
