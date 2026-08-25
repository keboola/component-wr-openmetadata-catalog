"""Keboola bucket/table -> OpenMetadata ``tableType`` detection (clean-room).

Rules (research 3 / spec 4.1):
    - ``hasExternalSchema`` and no sharing        -> External
    - stage in {out, shared, linked}, sharing in  -> Regular
      {specific-projects, none, null}
    - stage == in and a sourceBucket present       -> View
    - a table alias                                -> View
    - otherwise                                    -> Regular
"""

from __future__ import annotations

REGULAR = "Regular"
VIEW = "View"
EXTERNAL = "External"

_REGULAR_STAGES = frozenset({"out", "shared", "linked"})
_REGULAR_SHARING = frozenset({"specific-projects", "none"})


def detect_table_type(
    *,
    stage: str | None,
    sharing: str | None,
    has_external_schema: bool,
    has_source_bucket: bool,
    is_alias: bool,
) -> str:
    """Return one of ``Regular`` / ``View`` / ``External``."""
    if has_external_schema and sharing in (None, "none"):
        return EXTERNAL
    if stage in _REGULAR_STAGES and (sharing is None or sharing in _REGULAR_SHARING):
        return REGULAR
    if stage == "in" and has_source_bucket:
        return VIEW
    if is_alias:
        return VIEW
    return REGULAR
