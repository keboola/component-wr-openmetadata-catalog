"""Keboola bucket/table -> OpenMetadata ``tableType`` detection (clean-room).

Rules (research 3 / spec 4.1), first match wins:
    - ``hasExternalSchema`` and no sharing        -> External
    - a linked bucket (``sourceBucket`` present)   -> View, in any stage
    - a table alias                                -> View, in any stage
    - otherwise                                    -> Regular

A linked bucket can live in the ``out`` stage as well as ``in``, so the link is
checked before the stage: its tables are views of the source project's tables.
"""

from __future__ import annotations

REGULAR = "Regular"
VIEW = "View"
EXTERNAL = "External"


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
    if has_source_bucket or is_alias:
        return VIEW
    return REGULAR
