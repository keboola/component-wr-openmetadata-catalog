"""Incremental per-bucket digest + tombstoning (spec 2.3 / 6.2, T16).

Incremental skip: a per-bucket content digest lives in ``state.json`` (Tier-2
keyed ``project_id -> {bucket -> digest}``). A bucket whose digest is unchanged
is skipped unless ``full_refresh``, a version change, or the auto-cadence fires.
Digests advance only *after* that bucket's OM writes succeed (advance-after-
success), so a mid-run failure re-processes the affected buckets next run.

Tombstoning is fail-closed: on 2.0+ via ``deleteStale`` (dryRun then apply); on
1.13.4 via a self-diff soft-delete; skipped entirely if the scope listing errors
or looks implausibly short.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass

DEFAULT_FULL_REFRESH_EVERY = 20


def bucket_digest(bucket_fields: dict, table_fields: list[dict]) -> str:
    """Content digest of a bucket's catalog-relevant structure."""
    canonical = json.dumps(
        {"bucket": bucket_fields, "tables": table_fields},
        sort_keys=True,
        default=str,
    )
    return hashlib.sha256(canonical.encode()).hexdigest()


class StateManager:
    """Reads/writes the per-row ``state.json`` (pointers + per-bucket digests)."""

    def __init__(self, state: dict | None = None) -> None:
        state = state or {}
        self._projects: dict[str, dict] = dict(state.get("projects") or {})
        # Back-compat: a flat single-project shape gets folded under a default key.
        if "bucket_digests" in state and not self._projects:
            self._projects["_default"] = {"bucket_digests": dict(state["bucket_digests"])}
        self.snapshot_table: str | None = state.get("snapshot_table")
        self.last_full_refresh: str | None = state.get("last_full_refresh")
        self.run_count: int = int(state.get("run_count") or 0)
        self.om_version_seen: str | None = state.get("om_version_seen")

    def bucket_digest(self, project_id: str, bucket_id: str) -> str | None:
        return (self._projects.get(project_id) or {}).get("bucket_digests", {}).get(bucket_id)

    def set_bucket_digest(self, project_id: str, bucket_id: str, digest: str) -> None:
        project = self._projects.setdefault(project_id, {"bucket_digests": {}})
        project.setdefault("bucket_digests", {})[bucket_id] = digest

    def full_refresh_due(self, *, every: int = DEFAULT_FULL_REFRESH_EVERY) -> bool:
        return self.run_count % every == 0

    def to_dict(self) -> dict:
        return {
            "projects": self._projects,
            "snapshot_table": self.snapshot_table,
            "last_full_refresh": self.last_full_refresh,
            "run_count": self.run_count,
            "om_version_seen": self.om_version_seen,
        }


def should_process_bucket(
    *,
    previous_digest: str | None,
    current_digest: str,
    full_refresh: bool,
    version_changed: bool,
    full_refresh_due: bool,
) -> bool:
    """Decide whether a bucket needs (re)processing this run."""
    if full_refresh or version_changed or full_refresh_due:
        return True
    return previous_digest != current_digest


@dataclass
class TombstonePlan:
    to_delete: list[str]
    fail_closed_reason: str | None = None

    @property
    def blocked(self) -> bool:
        return self.fail_closed_reason is not None


class TombstonePlanner:
    """Plans stale-entity reconciliation, fail-closed by construction."""

    @staticmethod
    def deletestale_body(
        scope_fqn: str,
        scope_entity_type: str,
        seen_fqns: list[str],
        *,
        dry_run: bool = True,
        hard_delete: bool = False,
        recursive: bool = True,
    ) -> dict:
        return {
            "scopeFqn": scope_fqn,
            "scopeEntityType": scope_entity_type,
            "seenFqns": sorted(set(seen_fqns)),
            "dryRun": dry_run,
            "hardDelete": hard_delete,
            "recursive": recursive,
        }

    @staticmethod
    def self_diff(
        listed_fqns: list[str] | None,
        seen_fqns: list[str],
        *,
        min_scope: int = 1,
    ) -> TombstonePlan:
        """Compute stale entities (listed in OM scope but not seen this run).

        Fail closed when the scope listing is missing or implausibly short — do
        not delete on partial information.
        """
        if listed_fqns is None:
            return TombstonePlan(to_delete=[], fail_closed_reason="scope listing failed")
        if len(listed_fqns) < min_scope:
            return TombstonePlan(
                to_delete=[],
                fail_closed_reason=f"scope listing implausibly short ({len(listed_fqns)} < {min_scope})",
            )
        seen = set(seen_fqns)
        stale = sorted(fqn for fqn in listed_fqns if fqn not in seen)
        return TombstonePlan(to_delete=stale)
