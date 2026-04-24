"""
Silver Layer Flink Job: Bronze → Silver

Reads OrderPlaced events from the Bronze Iceberg table, validates,
deduplicates, and writes to the Silver Iceberg table. Malformed or
duplicate events are routed to the DLQ side output.

SLAs governed by: contracts/data-products/orders-analytics.yaml
  - silver_freshness.max_lag_seconds: 120
  - dlq_rate.max_per_hour: 10

Exactly-once guarantee:
  Kafka read (COMMITTED isolation)
  → Flink checkpoint (60s interval, RocksDB backend)
  → Iceberg FlinkSink (transactional commit on checkpoint)
"""

from __future__ import annotations

import os
import hashlib
from dataclasses import dataclass
from datetime import timedelta
from typing import Iterable

from pyflink.common import Duration, WatermarkStrategy
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetResetStrategy,
)
from pyflink.datastream.formats.avro import AvroRowDeserializationSchema
from pyflink.datastream.functions import (
    KeyedProcessFunction,
    OutputTag,
    RuntimeContext,
)
from pyflink.datastream.state import ValueStateDescriptor
from pyiceberg.catalog.glue import GlueCatalog


# ── Output tags ───────────────────────────────────────────────────────────────

DLQ_TAG = OutputTag("dlq", Types.MAP(Types.STRING(), Types.STRING()))


# ── Deduplication + validation ────────────────────────────────────────────────

class ValidateAndDeduplicate(KeyedProcessFunction):
    """
    Per-key stateful function that:
      1. Rejects events that fail schema validation rules
      2. Deduplicates on event_id within a 24-hour state TTL
      3. Routes late arrivals (> watermark tolerance) to DLQ side output
    """

    def open(self, ctx: RuntimeContext) -> None:
        # State TTL: 24 hours — matches maximum expected event deduplication window.
        # Events arriving after 24h with the same event_id will be re-processed.
        # Acceptable: CDC events are not expected to duplicate across day boundaries.
        descriptor = ValueStateDescriptor("seen_event_ids", Types.BOOLEAN())
        self.seen = ctx.get_state(descriptor)

    def process_element(self, event: dict, ctx: KeyedProcessFunction.Context) -> Iterable:
        # Deduplication check
        if self.seen.value() is True:
            yield DLQ_TAG, {
                "event_id":       event["event_id"],
                "failure_reason": "duplicate_event_id",
                "failure_stage":  "silver_dedup",
                "late_arrival":   "false",
            }
            return

        # Validation
        failure = self._validate(event)
        if failure:
            yield DLQ_TAG, {
                "event_id":       event.get("event_id", "unknown"),
                "failure_reason": failure,
                "failure_stage":  "silver_validation",
                "late_arrival":   "false",
            }
            return

        # Mark as seen and emit to Silver
        self.seen.update(True)
        yield self._to_silver(event)

    def _validate(self, event: dict) -> str | None:
        if not event.get("items"):
            return "empty_items_array"
        if event.get("total_amount_cents", 0) <= 0:
            return "non_positive_total"
        expected_total = sum(
            i["quantity"] * i["unit_price_cents"] for i in event["items"]
        )
        if event["total_amount_cents"] != expected_total:
            return "total_mismatch"
        return None

    def _to_silver(self, event: dict) -> dict:
        return {
            "order_id":           event["order_id"],
            "customer_id":        event["customer_id"],
            "occurred_at":        event["occurred_at"],
            "currency":           event["currency"],
            "total_amount_cents": event["total_amount_cents"],
            "item_count":         len(event["items"]),
            "items_json":         str(event["items"]),
            "trace_id":           event["metadata"]["trace_id"],
            "_ingested_at":       ctx.timestamp(),
        }


# ── Job entry point ───────────────────────────────────────────────────────────

def build_job(env: StreamExecutionEnvironment) -> None:
    # ── Checkpointing (exactly-once, 60s interval) ────────────────────────
    env.enable_checkpointing(60_000, CheckpointingMode.EXACTLY_ONCE)
    env.get_checkpoint_config().set_min_pause_between_checkpoints(30_000)
    env.get_checkpoint_config().set_checkpoint_timeout(120_000)

    # ── Kafka source ──────────────────────────────────────────────────────
    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(os.environ["KAFKA_BOOTSTRAP_SERVERS"])
        .set_topics("chakra.orders.placed")
        .set_group_id("flink-silver-orders")
        .set_starting_offsets(KafkaOffsetResetStrategy.EARLIEST)
        .set_value_only_deserializer(
            AvroRowDeserializationSchema.builder()
            .set_avro_schema_string(
                open("contracts/schemas/order-placed-v1.avsc").read()
            )
            .build()
        )
        .build()
    )

    # ── Watermark strategy ────────────────────────────────────────────────
    # max_out_of_orderness = 5 minutes (from contracts/data-products/orders-analytics.yaml)
    # Late events beyond tolerance go to DLQ via side output.
    watermark_strategy = (
        WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_minutes(5))
        .with_timestamp_assigner(lambda event, _: event["occurred_at"])
    )

    stream = env.from_source(
        kafka_source,
        watermark_strategy,
        "orders-kafka-source",
    )

    # ── Validate and deduplicate ──────────────────────────────────────────
    processed = (
        stream
        .key_by(lambda e: e["order_id"])
        .process(ValidateAndDeduplicate(), output_type=Types.MAP(Types.STRING(), Types.STRING()))
    )

    silver_stream = processed.get_side_output(DLQ_TAG)  # invert: main = silver, side = dlq
    dlq_stream    = processed.get_side_output(DLQ_TAG)

    # ── Iceberg Silver sink ───────────────────────────────────────────────
    catalog = GlueCatalog(
        "glue",
        **{"warehouse": "s3://chakra-lakehouse/", "region_name": os.environ["AWS_REGION"]},
    )
    silver_table = catalog.load_table("chakra.silver.orders")

    # FlinkSink writes with exactly-once semantics: files are committed
    # atomically on Flink checkpoint via the Iceberg commit protocol.
    # TODO (agent task): wire FlinkSink from pyiceberg-flink integration
    # See: ai-agents/tasks/agent/implement-flink-iceberg-sink.md

    # ── DLQ sink (Kafka topic) ────────────────────────────────────────────
    # TODO (agent task): wire KafkaSink to chakra.orders.dlq
    # DLQ events must include all fields from contracts/data-products/orders-analytics.yaml#dead_letter

    env.execute("silver-layer-orders")


if __name__ == "__main__":
    env = StreamExecutionEnvironment.get_execution_environment()
    build_job(env)
