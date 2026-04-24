"""
Quality Checker — inline Flink quality profiling for the Silver job.

Runs quality rules from governance/quality/quality_rules.yaml against
every 60-second tumbling window of processed events. Results are written
to s3://chakra-lakehouse/governance/quality_results/ as Iceberg.

Integration point in silver_layer_job.py:

    from governance.quality.quality_checker import QualityProfiler, QUALITY_TAG

    processed = stream.key_by(...).process(ValidateAndDeduplicate(...))

    # Side-branch quality profiling (does not affect Silver writes)
    quality_stream = (
        processed
        .window_all(TumblingProcessingTimeWindows.of(Time.seconds(60)))
        .apply(QualityProfiler(rules=load_rules("governance/quality/quality_rules.yaml")))
    )
    quality_stream.sink_to(iceberg_quality_sink)
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Iterable, Iterator

import yaml
from pyflink.datastream.functions import ProcessWindowFunction, RuntimeContext
from pyflink.datastream.window import TimeWindow


@dataclass
class QualityRule:
    name: str
    dimension: str
    rule: str
    column: str | None = None
    threshold: float = 1.0
    value: float | None = None
    values: list | None = None
    column_a: str | None = None
    column_b: str | None = None
    tolerance_minutes: int | None = None


@dataclass
class QualityResult:
    run_id: str
    pipeline_stage: str
    table_name: str
    check_name: str
    dimension: str
    total_record_count: int
    passed_record_count: int
    failed_record_count: int
    quality_score: float
    threshold: float
    threshold_met: bool
    sample_failure_ids: list[str]
    window_start: str
    window_end: str
    evaluated_at: str


def load_rules(path: str) -> dict[str, list[QualityRule]]:
    """Load quality rules from YAML, keyed by pipeline stage."""
    with open(path) as f:
        spec = yaml.safe_load(f)

    rules_by_stage: dict[str, list[QualityRule]] = {}
    for stage_spec in spec["stages"]:
        stage = stage_spec["stage"]
        rules_by_stage[stage] = [
            QualityRule(**{k: v for k, v in check.items() if k != "description"})
            for check in stage_spec["checks"]
        ]
    return rules_by_stage


class QualityProfiler(ProcessWindowFunction):
    """
    60-second tumbling window function that evaluates all quality rules
    for the current pipeline stage and emits one QualityResult per rule.

    Design notes:
    - Runs in a separate window branch — never delays the Silver write path
    - Uses the same 60s window as Flink checkpoints so quality results
      align with Iceberg snapshot boundaries
    - sample_failure_ids retains up to 10 event_ids for DLQ correlation
    """

    def __init__(self, stage: str, rules: list[QualityRule]) -> None:
        self.stage = stage
        self.rules = rules

    def process(
        self,
        key: str,
        context: ProcessWindowFunction.Context,
        elements: Iterable[dict],
        out: Iterator[dict],
    ) -> None:
        records = list(elements)
        if not records:
            return

        window: TimeWindow = context.window()
        run_id = str(uuid.uuid4())
        window_start = _ms_to_iso(window.start)
        window_end   = _ms_to_iso(window.end)
        evaluated_at = datetime.now(timezone.utc).isoformat()

        for rule in self.rules:
            total, passed, failures = self._evaluate(rule, records)
            score = passed / total if total > 0 else 1.0

            result = QualityResult(
                run_id=run_id,
                pipeline_stage=self.stage,
                table_name=_stage_to_table(self.stage),
                check_name=rule.name,
                dimension=rule.dimension,
                total_record_count=total,
                passed_record_count=passed,
                failed_record_count=total - passed,
                quality_score=round(score, 6),
                threshold=rule.threshold,
                threshold_met=score >= rule.threshold,
                sample_failure_ids=failures[:10],
                window_start=window_start,
                window_end=window_end,
                evaluated_at=evaluated_at,
            )
            out.collect(_to_row(result))

    def _evaluate(
        self, rule: QualityRule, records: list[dict]
    ) -> tuple[int, int, list[str]]:
        total = len(records)
        failures: list[str] = []

        for rec in records:
            if not _passes(rule, rec):
                failures.append(rec.get("event_id", rec.get("order_id", "unknown")))

        passed = total - len(failures)
        return total, passed, failures


def _passes(rule: QualityRule, rec: dict) -> bool:
    col_val = rec.get(rule.column) if rule.column else None

    if rule.rule == "not_null":
        return col_val is not None

    if rule.rule == "greater_than":
        return col_val is not None and col_val > rule.value

    if rule.rule == "less_than":
        return col_val is not None and col_val < rule.value

    if rule.rule == "in_set":
        return col_val in (rule.values or [])

    if rule.rule == "not_future":
        if col_val is None:
            return False
        tolerance_ms = (rule.tolerance_minutes or 5) * 60 * 1000
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        return col_val <= now_ms + tolerance_ms

    if rule.rule == "within_days":
        if col_val is None:
            return False
        cutoff_ms = int(datetime.now(timezone.utc).timestamp() * 1000) - rule.value * 86_400_000
        return col_val >= cutoff_ms

    if rule.rule == "within_minutes":
        if col_val is None:
            return False
        cutoff_ms = int(datetime.now(timezone.utc).timestamp() * 1000) - rule.value * 60_000
        return col_val >= cutoff_ms

    if rule.rule == "column_a_lte_column_b":
        a = rec.get(rule.column_a)
        b = rec.get(rule.column_b)
        return a is not None and b is not None and a <= b

    if rule.rule == "column_sum_equals":
        # Simplified: trust that ValidateAndDeduplicate already checked this
        # Here we recompute to get a quality score, not to gate the record
        items = rec.get("items") or []
        if not items:
            return False
        computed = sum(i.get("quantity", 0) * i.get("unit_price_cents", 0) for i in items)
        return computed == rec.get("total_amount_cents", -1)

    return True  # unknown rule — don't penalise


def _stage_to_table(stage: str) -> str:
    return {
        "bronze_ingestion":  "bronze.orders",
        "silver_validation": "silver.orders",
        "gold_aggregation":  "gold.order_daily_summary",
    }.get(stage, stage)


def _ms_to_iso(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat()


def _to_row(r: QualityResult) -> dict:
    return {
        "run_id":               r.run_id,
        "pipeline_stage":       r.pipeline_stage,
        "table_name":           r.table_name,
        "check_name":           r.check_name,
        "dimension":            r.dimension,
        "total_record_count":   r.total_record_count,
        "passed_record_count":  r.passed_record_count,
        "failed_record_count":  r.failed_record_count,
        "quality_score":        r.quality_score,
        "threshold":            r.threshold,
        "threshold_met":        r.threshold_met,
        "sample_failure_ids":   json.dumps(r.sample_failure_ids),
        "window_start":         r.window_start,
        "window_end":           r.window_end,
        "evaluated_at":         r.evaluated_at,
    }
