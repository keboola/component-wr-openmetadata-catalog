"""Fully-qualified-name builder and name sanitisation (clean-room).

OpenMetadata addresses every entity by a dot-separated FQN
(``service.database.schema.table``). Keboola object names can contain spaces,
dots and other characters that would break an FQN, so names are sanitised into
FQN-safe segments while the original is preserved as the entity ``displayName``.

The functions here are pure and deterministic: the same input always yields the
same FQN, so N independent project rows converge on one coherent catalog.
"""

from __future__ import annotations

import re

_UNSAFE = re.compile(r"[^0-9A-Za-z_-]+")
_MULTI_UNDERSCORE = re.compile(r"_+")


def sanitize_name(name: str | None) -> str:
    """Return an FQN-safe single segment for ``name``.

    Dots (the FQN separator) and whitespace collapse to underscores; other
    unsafe characters are replaced with underscores; dashes are preserved.
    """
    if name is None:
        return "unnamed"
    text = str(name).strip().replace(".", "_")
    text = _UNSAFE.sub("_", text)
    text = _MULTI_UNDERSCORE.sub("_", text).strip("_")
    return text or "unnamed"


def sanitize_display_name(name: str | None) -> str | None:
    """Return the original human-readable name (trimmed), preserved as ``displayName``."""
    if name is None:
        return None
    text = str(name).strip()
    return text or None


def database_service_fqn(service_name: str) -> str:
    """FQN of the DatabaseService (E1) — a single sanitised segment."""
    return sanitize_name(service_name)


def database_fqn(service_name: str, project: str) -> str:
    """FQN of a Database (E2) ← Keboola project."""
    return f"{sanitize_name(service_name)}.{sanitize_name(project)}"


def schema_fqn(service_name: str, project: str, bucket_path: str) -> str:
    """FQN of a DatabaseSchema (E3) ← Keboola bucket."""
    return f"{sanitize_name(service_name)}.{sanitize_name(project)}.{sanitize_name(bucket_path)}"


def table_fqn(service_name: str, project: str, bucket_path: str, table_name: str) -> str:
    """FQN of a Table (E4) ← Keboola table.

    ``project`` is passed explicitly so a source table in another project (a
    linked/shared bucket) resolves onto the node its owning project created,
    rather than the project currently being catalogued.
    """
    return (
        f"{sanitize_name(service_name)}.{sanitize_name(project)}."
        f"{sanitize_name(bucket_path)}.{sanitize_name(table_name)}"
    )


def pipeline_name(project: str, config_or_flow_id: str) -> str:
    """Entity ``name`` for a Pipeline (E13/E14): ``<project>.<config-or-flow-id>``."""
    return f"{sanitize_name(project)}.{sanitize_name(str(config_or_flow_id))}"


def pipeline_fqn(service_name: str, project: str, config_or_flow_id: str) -> str:
    """FQN of a Pipeline ← component config or flow (spec 2.1)."""
    return f"{sanitize_name(service_name)}.{pipeline_name(project, config_or_flow_id)}"
