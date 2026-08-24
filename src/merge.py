"""Three-way merge engine + snapshot store (spec 2.3 / 6.2, T15).

Base = the value we last wrote (kept in a component-managed snapshot table). Per
owned field:

    OM empty/absent           -> write
    OM == base                -> update to the new desired value
    OM already == desired      -> no-op
    OM diverged from base      -> leave + record ``skipped_diverged``
                                  (unless ``merge_mode=keboola_always_wins``)

Writes are emitted as an RFC-6902 JSON Patch so only owned fields are touched.
For lineage graphs the same discipline applies structurally: only our own edge
sources are ever dropped, so a ``Manual`` edge is never removed.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field

from configuration import MergeMode

# Owned fields per entity kind (fields this component authors).
OWNED_TABLE_FIELDS = (
    "displayName",
    "description",
    "tableType",
    "columns",
    "tableConstraints",
    "sourceUrl",
    "schemaDefinition",
)
OWNED_PIPELINE_FIELDS = ("displayName", "description", "tasks", "sourceUrl")

# Lineage edge sources this component owns; a Manual edge is never in this set.
OUR_LINEAGE_SOURCES = ("PipelineLineage", "QueryLineage", "ViewLineage")

ACTION_CREATED = "created"
ACTION_UPDATED = "updated"
ACTION_SKIPPED_UNCHANGED = "skipped_unchanged"
ACTION_SKIPPED_DIVERGED = "skipped_diverged"


@dataclass
class MergeDecision:
    action: str
    patch: list[dict] = field(default_factory=list)
    snapshot_fields: dict = field(default_factory=dict)
    is_create: bool = False
    diverged_fields: list[str] = field(default_factory=list)


def _is_empty(value: object) -> bool:
    return value is None or value == "" or value == [] or value == {}


def _patch_op(current: dict, field_name: str, value: object) -> dict:
    op = "replace" if field_name in current and current.get(field_name) is not None else "add"
    return {"op": op, "path": f"/{field_name}", "value": value}


class ThreeWayMerger:
    """Field-level three-way merge over a set of owned fields."""

    def __init__(self, merge_mode: MergeMode) -> None:
        self.keboola_always_wins = merge_mode == MergeMode.KEBOOLA_ALWAYS_WINS

    def merge(
        self,
        *,
        desired: dict,
        current: dict | None,
        base: dict | None,
        owned_fields: tuple[str, ...],
    ) -> MergeDecision:
        if current is None:
            snapshot = {f: desired[f] for f in owned_fields if f in desired}
            return MergeDecision(action=ACTION_CREATED, snapshot_fields=snapshot, is_create=True)

        patch: list[dict] = []
        snapshot: dict = {}
        diverged: list[str] = []
        changed = False

        for name in owned_fields:
            if name not in desired:
                continue
            desired_val = desired[name]
            om_val = current.get(name)
            base_val = (base or {}).get(name)

            if _is_empty(om_val):
                patch.append(_patch_op(current, name, desired_val))
                snapshot[name] = desired_val
                changed = True
            elif om_val == desired_val:
                snapshot[name] = desired_val
            elif (base is not None and om_val == base_val) or self.keboola_always_wins:
                patch.append(_patch_op(current, name, desired_val))
                snapshot[name] = desired_val
                changed = True
            else:
                diverged.append(name)
                snapshot[name] = base_val if base is not None else om_val

        if changed:
            action = ACTION_UPDATED
        elif diverged:
            action = ACTION_SKIPPED_DIVERGED
        else:
            action = ACTION_SKIPPED_UNCHANGED
        return MergeDecision(
            action=action,
            patch=patch,
            snapshot_fields=snapshot,
            diverged_fields=diverged,
        )


def content_hash(fields: dict) -> str:
    canonical = json.dumps(fields, sort_keys=True, default=str)
    return hashlib.sha256(canonical.encode()).hexdigest()


@dataclass
class SnapshotEntry:
    entity_fqn: str
    entity_type: str
    written_fields_json: str
    content_hash: str


class SnapshotStore:
    """The three-way-merge base store (spec 2.3): entity_fqn -> last-written fields."""

    def __init__(self) -> None:
        self._data: dict[str, dict] = {}

    def load_rows(self, rows: list[dict]) -> None:
        for row in rows:
            fqn = row.get("entity_fqn")
            if not fqn:
                continue
            try:
                fields = json.loads(row.get("written_fields_json") or "{}")
            except ValueError, TypeError:
                fields = {}
            self._data[fqn] = {"entity_type": row.get("entity_type"), "fields": fields}

    def base_fields(self, entity_fqn: str) -> dict | None:
        entry = self._data.get(entity_fqn)
        return entry["fields"] if entry else None

    def record(self, entity_fqn: str, entity_type: str, fields: dict) -> None:
        self._data[entity_fqn] = {"entity_type": entity_type, "fields": fields}

    def entries(self) -> list[SnapshotEntry]:
        result: list[SnapshotEntry] = []
        for fqn, entry in sorted(self._data.items()):
            fields_json = json.dumps(entry["fields"], sort_keys=True, default=str)
            result.append(
                SnapshotEntry(
                    entity_fqn=fqn,
                    entity_type=entry.get("entity_type") or "",
                    written_fields_json=fields_json,
                    content_hash=content_hash(entry["fields"]),
                )
            )
        return result
