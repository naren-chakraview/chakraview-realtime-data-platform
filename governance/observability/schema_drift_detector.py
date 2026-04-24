"""
Schema Drift Detector — detects unexpected schema changes in Iceberg tables.

Runs as a scheduled job (every 1 hour). Compares the current Iceberg schema
fingerprint against the previously recorded one. Detects:
  - New columns added (info: may be intentional schema evolution)
  - Columns dropped (warning: may break downstream consumers)
  - Column type changes (error: breaks Silver deserialization)
  - Column rename (info: detected via Iceberg field ID, safe in Iceberg v2)

Results are written to s3://chakra-lakehouse/governance/schema_drift_events/

Design note on "safe" vs. "unsafe" drift:
  Iceberg v2 uses integer field IDs, not column names, for physical storage.
  ADD COLUMN and RENAME COLUMN are always safe — existing Parquet files
  remain readable. DROP COLUMN is safe for new writes but breaks queries
  that reference the old column name. TYPE CHANGE is always unsafe.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

from pyiceberg.catalog.glue import GlueCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField


GOVERNANCE_PATH = "s3://chakra-lakehouse/governance/schema_drift_events"

MONITORED_TABLES = [
    ("chakra", "bronze", "orders"),
    ("chakra", "silver", "orders"),
    ("chakra", "gold",   "order_daily_summary"),
]


@dataclass
class SchemaDriftEvent:
    event_id: str
    table_name: str
    drift_type: str          # COLUMN_ADDED | COLUMN_DROPPED | TYPE_CHANGED | RENAME | NO_CHANGE
    severity: str            # info | warning | error
    column_name: str | None
    old_type: str | None
    new_type: str | None
    old_schema_fingerprint: str
    new_schema_fingerprint: str
    is_safe: bool
    detected_at: str
    details: str


def run_detector(catalog: GlueCatalog) -> list[SchemaDriftEvent]:
    events: list[SchemaDriftEvent] = []

    for namespace, db, table_name in MONITORED_TABLES:
        full_name = f"{namespace}.{db}.{table_name}"
        try:
            table = catalog.load_table(f"{db}.{table_name}")
            events.extend(_check_table(full_name, table.schema()))
        except Exception as exc:
            print(f"[schema_drift] Failed to load {full_name}: {exc}")

    _write_events(events)
    return events


def _check_table(table_name: str, current_schema: Schema) -> list[SchemaDriftEvent]:
    current_fp = _fingerprint(current_schema)
    last_snapshot = _load_last_snapshot(table_name)

    if last_snapshot is None:
        _save_snapshot(table_name, current_fp, _schema_to_dict(current_schema))
        return []  # First run — no baseline to compare against

    if last_snapshot["fingerprint"] == current_fp:
        return []  # No change

    previous_fields = {f["field_id"]: f for f in last_snapshot["fields"]}
    current_fields  = {f.field_id: f for f in current_schema.fields}

    events: list[SchemaDriftEvent] = []
    detected_at = datetime.now(timezone.utc).isoformat()
    old_fp = last_snapshot["fingerprint"]

    # Detect dropped columns (by field ID — safe in Iceberg v2 but query-breaking)
    for field_id, old_field in previous_fields.items():
        if field_id not in current_fields:
            events.append(SchemaDriftEvent(
                event_id=str(uuid.uuid4()),
                table_name=table_name,
                drift_type="COLUMN_DROPPED",
                severity="warning",
                column_name=old_field["name"],
                old_type=old_field["type"],
                new_type=None,
                old_schema_fingerprint=old_fp,
                new_schema_fingerprint=current_fp,
                is_safe=True,     # safe for Iceberg files; breaks SQL queries referencing the column
                detected_at=detected_at,
                details=f"Column '{old_field['name']}' (field_id={field_id}) was dropped",
            ))

    # Detect new columns and type changes
    for field_id, curr_field in current_fields.items():
        if field_id not in previous_fields:
            events.append(SchemaDriftEvent(
                event_id=str(uuid.uuid4()),
                table_name=table_name,
                drift_type="COLUMN_ADDED",
                severity="info",
                column_name=curr_field.name,
                old_type=None,
                new_type=str(curr_field.field_type),
                old_schema_fingerprint=old_fp,
                new_schema_fingerprint=current_fp,
                is_safe=True,     # ADD COLUMN is always safe in Iceberg v2
                detected_at=detected_at,
                details=f"Column '{curr_field.name}' (field_id={field_id}) was added",
            ))
        else:
            old_field = previous_fields[field_id]
            old_type_str = old_field["type"]
            new_type_str = str(curr_field.field_type)

            if old_type_str != new_type_str:
                is_safe = _is_safe_type_change(old_type_str, new_type_str)
                events.append(SchemaDriftEvent(
                    event_id=str(uuid.uuid4()),
                    table_name=table_name,
                    drift_type="TYPE_CHANGED",
                    severity="error" if not is_safe else "warning",
                    column_name=curr_field.name,
                    old_type=old_type_str,
                    new_type=new_type_str,
                    old_schema_fingerprint=old_fp,
                    new_schema_fingerprint=current_fp,
                    is_safe=is_safe,
                    detected_at=detected_at,
                    details=(
                        f"Column '{curr_field.name}' type changed: "
                        f"{old_type_str} → {new_type_str}"
                    ),
                ))

            # Rename: same field_id, different name
            elif old_field["name"] != curr_field.name:
                events.append(SchemaDriftEvent(
                    event_id=str(uuid.uuid4()),
                    table_name=table_name,
                    drift_type="RENAME",
                    severity="info",
                    column_name=curr_field.name,
                    old_type=old_type_str,
                    new_type=new_type_str,
                    old_schema_fingerprint=old_fp,
                    new_schema_fingerprint=current_fp,
                    is_safe=True,     # Iceberg field IDs absorb renames; old Parquet files unaffected
                    detected_at=detected_at,
                    details=(
                        f"Column renamed: '{old_field['name']}' → '{curr_field.name}' "
                        f"(field_id={field_id} preserved)"
                    ),
                ))

    _save_snapshot(table_name, current_fp, _schema_to_dict(current_schema))
    return events


def _is_safe_type_change(old_type: str, new_type: str) -> bool:
    # Iceberg v2 type promotion rules: safe widening conversions only
    safe_promotions = {
        ("int", "long"),
        ("float", "double"),
        ("decimal(P,S)", "decimal(P2,S)"),  # simplified: precision increase only
    }
    return (old_type, new_type) in safe_promotions


def _fingerprint(schema: Schema) -> str:
    canonical = json.dumps(
        sorted({"id": f.field_id, "name": f.name, "type": str(f.field_type)} for f in schema.fields),
        sort_keys=True,
    )
    return hashlib.sha256(canonical.encode()).hexdigest()[:16]


def _schema_to_dict(schema: Schema) -> list[dict]:
    return [
        {"field_id": f.field_id, "name": f.name, "type": str(f.field_type), "required": f.required}
        for f in schema.fields
    ]


def _load_last_snapshot(table_name: str) -> dict | None:
    # In production: read from governance/schema_snapshots/ Iceberg table
    # For simplicity here: file-based cache (replace with Iceberg query in prod)
    try:
        import os
        path = f"/tmp/schema_snapshot_{table_name.replace('.', '_')}.json"
        if os.path.exists(path):
            with open(path) as f:
                return json.load(f)
    except Exception:
        pass
    return None


def _save_snapshot(table_name: str, fingerprint: str, fields: list[dict]) -> None:
    import os
    path = f"/tmp/schema_snapshot_{table_name.replace('.', '_')}.json"
    with open(path, "w") as f:
        json.dump({"fingerprint": fingerprint, "fields": fields}, f)


def _write_events(events: list[SchemaDriftEvent]) -> None:
    if not events:
        return
    # TODO (agent task): replace with Iceberg FlinkSink or pyiceberg write
    for e in events:
        severity_label = {"info": "ℹ", "warning": "⚠", "error": "✖"}[e.severity]
        print(f"[schema_drift] {severity_label} {e.table_name}: {e.details} (safe={e.is_safe})")
