"""
OpenLineage Emitter — records column-level lineage for every pipeline run.

Emits OpenLineage-standard events at job START and COMPLETE for:
  - Flink Silver job (Kafka → Bronze, Kafka → Silver)
  - dbt Gold models (Silver → Gold tables)

Events are written to:
  1. s3://chakra-lakehouse/governance/lineage_events/ (Iceberg — queryable by DuckDB)
  2. Marquez backend (if MARQUEZ_URL env var is set) for visual lineage graph

Column-level lineage (OpenLineage ColumnLineageDatasetFacet) captures:
  - Which source Kafka fields map to which Silver columns
  - Which Silver columns are aggregated into which Gold columns
  - Transformation type (direct_copy, aggregate_sum, aggregate_count, expression)

This makes it possible to answer:
  "If total_amount_cents is wrong in Gold, which source field caused it?"
  "Which Gold columns are affected if I rename order_id in the Avro schema?"
"""

from __future__ import annotations

import json
import os
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


LINEAGE_PATH = "s3://chakra-lakehouse/governance/lineage_events"


# ── OpenLineage data structures (simplified) ──────────────────────────────────

@dataclass
class DatasetField:
    name: str
    type: str


@dataclass
class ColumnLineage:
    """Maps one output column to its input column(s) and transformation."""
    output_column: str
    input_fields: list[dict]      # [{"dataset": "...", "field": "...", "transform": "..."}]


@dataclass
class Dataset:
    namespace: str
    name: str
    fields: list[DatasetField] = field(default_factory=list)
    column_lineage: list[ColumnLineage] = field(default_factory=list)


@dataclass
class LineageEvent:
    event_id: str
    event_time: str
    event_type: str               # START | COMPLETE | FAIL
    job_namespace: str
    job_name: str
    run_id: str
    input_datasets: list[Dataset]
    output_datasets: list[Dataset]
    row_count_input: int | None = None
    row_count_output: int | None = None


# ── Column lineage maps ───────────────────────────────────────────────────────

KAFKA_TO_SILVER_LINEAGE: list[ColumnLineage] = [
    ColumnLineage("order_id",           [{"dataset": "kafka://chakra.orders.placed", "field": "order_id",           "transform": "direct_copy"}]),
    ColumnLineage("customer_id",        [{"dataset": "kafka://chakra.orders.placed", "field": "customer_id",        "transform": "direct_copy"}]),
    ColumnLineage("occurred_at",        [{"dataset": "kafka://chakra.orders.placed", "field": "occurred_at",        "transform": "direct_copy"}]),
    ColumnLineage("currency",           [{"dataset": "kafka://chakra.orders.placed", "field": "currency",           "transform": "direct_copy"}]),
    ColumnLineage("total_amount_cents", [{"dataset": "kafka://chakra.orders.placed", "field": "total_amount_cents", "transform": "direct_copy"}]),
    ColumnLineage("item_count",         [{"dataset": "kafka://chakra.orders.placed", "field": "items",              "transform": "expression:len(items)"}]),
    ColumnLineage("items_json",         [{"dataset": "kafka://chakra.orders.placed", "field": "items",              "transform": "expression:json_encode(items)"}]),
    ColumnLineage("trace_id",           [{"dataset": "kafka://chakra.orders.placed", "field": "metadata.trace_id", "transform": "direct_copy"}]),
    ColumnLineage("_ingested_at",       [{"dataset": "system", "field": "flink_processing_timestamp",              "transform": "expression:ctx.timestamp()"}]),
]

SILVER_TO_GOLD_LINEAGE: list[ColumnLineage] = [
    ColumnLineage("order_date",           [{"dataset": "silver.orders", "field": "occurred_at",        "transform": "expression:DATE(occurred_at)"}]),
    ColumnLineage("currency",             [{"dataset": "silver.orders", "field": "currency",           "transform": "direct_copy"}]),
    ColumnLineage("order_count",          [{"dataset": "silver.orders", "field": "order_id",           "transform": "aggregate_count_distinct"}]),
    ColumnLineage("total_revenue_cents",  [{"dataset": "silver.orders", "field": "total_amount_cents", "transform": "aggregate_sum"}]),
    ColumnLineage("avg_order_cents",      [{"dataset": "silver.orders", "field": "total_amount_cents", "transform": "aggregate_avg"}]),
    ColumnLineage("unique_customers",     [{"dataset": "silver.orders", "field": "customer_id",        "transform": "aggregate_count_distinct"}]),
    ColumnLineage("item_count",           [{"dataset": "silver.orders", "field": "item_count",         "transform": "aggregate_sum"}]),
    ColumnLineage("updated_at",           [{"dataset": "system", "field": "dbt_run_timestamp",         "transform": "expression:current_timestamp()"}]),
]


# ── Emitter ───────────────────────────────────────────────────────────────────

class OpenLineageEmitter:
    """
    Emit lineage events for a pipeline run.

    Usage in silver_layer_job.py:

        emitter = OpenLineageEmitter(job_name="silver-layer-orders")
        run_id  = emitter.start(input_count=kafka_offset_count)
        try:
            build_job(env)
            emitter.complete(run_id=run_id, input_count=n_read, output_count=n_written)
        except Exception as e:
            emitter.fail(run_id=run_id, error=str(e))
            raise
    """

    def __init__(self, job_name: str, job_namespace: str = "chakra.streaming") -> None:
        self.job_name      = job_name
        self.job_namespace = job_namespace
        self._marquez_url  = os.getenv("MARQUEZ_URL")

    def start(self) -> str:
        run_id = str(uuid.uuid4())
        self._emit(LineageEvent(
            event_id=str(uuid.uuid4()),
            event_time=_now(),
            event_type="START",
            job_namespace=self.job_namespace,
            job_name=self.job_name,
            run_id=run_id,
            input_datasets=self._kafka_inputs(),
            output_datasets=[],
        ))
        return run_id

    def complete(
        self,
        run_id: str,
        row_count_input: int | None = None,
        row_count_output: int | None = None,
    ) -> None:
        self._emit(LineageEvent(
            event_id=str(uuid.uuid4()),
            event_time=_now(),
            event_type="COMPLETE",
            job_namespace=self.job_namespace,
            job_name=self.job_name,
            run_id=run_id,
            input_datasets=self._kafka_inputs(),
            output_datasets=self._iceberg_outputs(),
            row_count_input=row_count_input,
            row_count_output=row_count_output,
        ))

    def fail(self, run_id: str, error: str = "") -> None:
        self._emit(LineageEvent(
            event_id=str(uuid.uuid4()),
            event_time=_now(),
            event_type="FAIL",
            job_namespace=self.job_namespace,
            job_name=self.job_name,
            run_id=run_id,
            input_datasets=self._kafka_inputs(),
            output_datasets=[],
        ))

    def _kafka_inputs(self) -> list[Dataset]:
        return [Dataset(
            namespace="kafka://chakra",
            name="chakra.orders.placed",
            fields=[
                DatasetField("event_id",           "string:uuid"),
                DatasetField("occurred_at",        "long:timestamp-millis"),
                DatasetField("order_id",           "string:uuid"),
                DatasetField("customer_id",        "string:uuid"),
                DatasetField("currency",           "string"),
                DatasetField("total_amount_cents", "long"),
                DatasetField("items",              "array<record>"),
                DatasetField("metadata.trace_id",  "string"),
            ],
        )]

    def _iceberg_outputs(self) -> list[Dataset]:
        return [
            Dataset(
                namespace="s3://chakra-lakehouse",
                name="silver/orders/orders",
                fields=[
                    DatasetField("order_id",           "string"),
                    DatasetField("customer_id",        "string"),
                    DatasetField("occurred_at",        "timestamp_tz"),
                    DatasetField("currency",           "string"),
                    DatasetField("total_amount_cents", "bigint"),
                    DatasetField("item_count",         "int"),
                    DatasetField("items_json",         "string"),
                    DatasetField("trace_id",           "string"),
                    DatasetField("_ingested_at",       "timestamp_tz"),
                ],
                column_lineage=KAFKA_TO_SILVER_LINEAGE,
            )
        ]

    def _emit(self, event: LineageEvent) -> None:
        row = _event_to_row(event)

        # Write to Iceberg lineage table
        _write_to_iceberg(row)

        # Optionally emit to Marquez
        if self._marquez_url:
            _emit_to_marquez(self._marquez_url, event)


def _event_to_row(event: LineageEvent) -> dict:
    return {
        "event_id":            event.event_id,
        "event_time":          event.event_time,
        "event_type":          event.event_type,
        "job_namespace":       event.job_namespace,
        "job_name":            event.job_name,
        "run_id":              event.run_id,
        "input_datasets":      json.dumps([_dataset_to_dict(d) for d in event.input_datasets]),
        "output_datasets":     json.dumps([_dataset_to_dict(d) for d in event.output_datasets]),
        "column_lineage_json": json.dumps([_lineage_to_dict(cl)
                                           for d in event.output_datasets
                                           for cl in d.column_lineage]),
        "row_count_input":     event.row_count_input,
        "row_count_output":    event.row_count_output,
    }


def _dataset_to_dict(d: Dataset) -> dict:
    return {
        "namespace": d.namespace,
        "name":      d.name,
        "fields":    [{"name": f.name, "type": f.type} for f in d.fields],
    }


def _lineage_to_dict(cl: ColumnLineage) -> dict:
    return {
        "output_column": cl.output_column,
        "input_fields":  cl.input_fields,
    }


def _write_to_iceberg(row: dict) -> None:
    # TODO (agent task): wire to pyiceberg write against governance/lineage_events/ table
    print(f"[lineage] {row['event_type']} run_id={row['run_id']} job={row['job_name']}")


def _emit_to_marquez(url: str, event: LineageEvent) -> None:
    import urllib.request
    payload = json.dumps({
        "eventType": event.event_type,
        "eventTime": event.event_time,
        "run":  {"runId": event.run_id},
        "job":  {"namespace": event.job_namespace, "name": event.job_name},
        "inputs":  [_dataset_to_dict(d) for d in event.input_datasets],
        "outputs": [_dataset_to_dict(d) for d in event.output_datasets],
    }).encode()
    req = urllib.request.Request(
        f"{url}/api/v1/lineage",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        urllib.request.urlopen(req, timeout=3)
    except Exception as exc:
        print(f"[lineage] Marquez emit failed (non-fatal): {exc}")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()
