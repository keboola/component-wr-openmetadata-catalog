"""Run report + snapshot output tables (spec 2.6 / 6.4, T17).

``catalog_run_report`` records one row per entity action this run (the coverage
and drift signal). It carries an authoritative ``schema`` manifest when the
``KBC_DATA_TYPE_SUPPORT`` gate is on, and falls back to the legacy
``columns`` + ``column_metadata`` format when the var is absent/None (before the
portal ``dataTypeSupport=authoritative`` switch is flipped).
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, datetime

REPORT_TABLE = "catalog_run_report"
SNAPSHOT_TABLE = "last_written_snapshot"

REPORT_COLUMNS = (
    "run_id",
    "config_row_id",
    "project_id",
    "entity_type",
    "entity_fqn",
    "action",
    "detail",
    "om_status_code",
    "timestamp",
)
REPORT_PRIMARY_KEY = ("run_id", "entity_fqn", "entity_type")

SNAPSHOT_COLUMNS = ("entity_fqn", "entity_type", "written_fields_json", "content_hash", "updated_at")
SNAPSHOT_PRIMARY_KEY = ("entity_fqn",)

# Explicit, in-repo output destinations (spec 2.3 / 6.4). Set on the manifests so
# the component does NOT depend on the portal defaultBucket, and so the snapshot
# table id is deterministic: it is recorded into state after each write and read
# back on the next run as the three-way-merge base.
OUTPUT_BUCKET = "in.c-wr-openmetadata-catalog"
REPORT_DESTINATION = f"{OUTPUT_BUCKET}.{REPORT_TABLE}"
SNAPSHOT_DESTINATION = f"{OUTPUT_BUCKET}.{SNAPSHOT_TABLE}"

# Action enum (spec 6.4).
ACTION_CREATED = "created"
ACTION_UPDATED = "updated"
ACTION_SKIPPED_UNCHANGED = "skipped_unchanged"
ACTION_SKIPPED_DIVERGED = "skipped_diverged"
ACTION_TOMBSTONED = "tombstoned"
ACTION_UNRESOLVED = "unresolved"
ACTION_DEGRADED = "degraded"
ACTION_FAILED = "failed"


@dataclass
class RunReport:
    """Accumulates ``catalog_run_report`` rows for a run."""

    run_id: str
    config_row_id: str | None = None
    _rows: list[dict] = field(default_factory=list)

    def record(
        self,
        *,
        project_id: str | None,
        entity_type: str,
        entity_fqn: str,
        action: str,
        detail: str = "",
        om_status_code: int | None = None,
        timestamp: str | None = None,
    ) -> None:
        self._rows.append(
            {
                "run_id": self.run_id,
                # config_row_id is absent (None) on a non-row run -> empty string in CSV.
                "config_row_id": self.config_row_id or "",
                "project_id": project_id or "",
                "entity_type": entity_type,
                "entity_fqn": entity_fqn,
                "action": action,
                "detail": detail,
                "om_status_code": "" if om_status_code is None else str(om_status_code),
                "timestamp": timestamp or datetime.now(tz=UTC).isoformat(),
            }
        )

    def rows(self) -> list[dict]:
        return list(self._rows)

    def counts(self) -> Counter:
        return Counter(row["action"] for row in self._rows)

    def has_failures(self) -> bool:
        return any(row["action"] == ACTION_FAILED for row in self._rows)


def uses_authoritative_manifest(kbc_data_type_support: str | None) -> bool:
    """Authoritative ``schema`` only when the gate var is present (not None)."""
    return kbc_data_type_support is not None


def report_schema() -> list[dict]:
    """Authoritative native-type schema for ``catalog_run_report``."""
    schema: list[dict] = []
    for name in REPORT_COLUMNS:
        if name == "timestamp":
            base_type = "TIMESTAMP"
        elif name == "om_status_code":
            base_type = "INTEGER"
        else:
            base_type = "STRING"
        schema.append(
            {
                "name": name,
                "base_type": base_type,
                "primary_key": name in REPORT_PRIMARY_KEY,
            }
        )
    return schema


def snapshot_rows(entries: list) -> list[dict]:
    """Build ``last_written_snapshot`` rows from SnapshotStore entries."""
    now = datetime.now(tz=UTC).isoformat()
    return [
        {
            "entity_fqn": e.entity_fqn,
            "entity_type": e.entity_type,
            "written_fields_json": e.written_fields_json,
            "content_hash": e.content_hash,
            "updated_at": now,
        }
        for e in entries
    ]
