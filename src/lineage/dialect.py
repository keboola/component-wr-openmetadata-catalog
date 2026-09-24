"""Transformation backend / component id -> SQLGlot dialect + casing (spec 5.2, T13).

Snowflake-first, with a per-dialect plan for BigQuery / Redshift / Synapse. The
dialect is auto-detected (never a user field). An unknown backend yields a
non-SQL / generic marker so the caller records ``unresolved`` rather than
guessing.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class DialectInfo:
    """Resolved SQL dialect facets for a transformation."""

    name: str
    sqlglot_dialect: str | None
    is_sql: bool
    # How the backend folds unquoted identifiers: probe this case first.
    unquoted_case: str  # "upper" | "lower" | "preserve"


_GENERIC = DialectInfo(name="generic", sqlglot_dialect=None, is_sql=True, unquoted_case="preserve")
_NON_SQL = DialectInfo(name="non_sql", sqlglot_dialect=None, is_sql=False, unquoted_case="preserve")

# Backend keyword -> DialectInfo.
_BACKENDS = {
    "snowflake": DialectInfo("snowflake", "snowflake", True, "upper"),
    "bigquery": DialectInfo("bigquery", "bigquery", True, "preserve"),
    "redshift": DialectInfo("redshift", "redshift", True, "lower"),
    "synapse": DialectInfo("synapse", "tsql", True, "preserve"),
    "exasol": DialectInfo("exasol", None, True, "upper"),
}

# Component id -> backend keyword. Python/other transformations are not SQL.
_COMPONENT_BACKENDS = {
    "keboola.snowflake-transformation": "snowflake",
    "keboola.snowflake-transformation-v2": "snowflake",
    "keboola.google-bigquery-transformation": "bigquery",
    "keboola.google-bigquery-transformation-v2": "bigquery",
    "keboola.redshift-transformation": "redshift",
    "keboola.synapse-transformation": "synapse",
    "keboola.exasol-transformation": "exasol",
}

_NON_SQL_COMPONENTS = frozenset(
    {
        "keboola.python-transformation-v2",
        "keboola.python-transformation",
        "keboola.r-transformation-v2",
        "keboola.julia-transformation",
    }
)


def dialect_for(*, backend: str | None = None, component_id: str | None = None) -> DialectInfo:
    """Resolve a dialect from a table backend and/or transformation component id."""
    if component_id:
        if component_id in _NON_SQL_COMPONENTS:
            return _NON_SQL
        mapped = _COMPONENT_BACKENDS.get(component_id)
        if mapped:
            return _BACKENDS[mapped]
    if backend:
        info = _BACKENDS.get(backend.strip().lower())
        if info:
            return info
    return _GENERIC
