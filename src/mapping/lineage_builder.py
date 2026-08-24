"""Lineage edge builder (E16 declared + E17 column + ViewLineage, spec 4, T12).

Produces version-neutral :class:`LineageEdge` records; the orchestrator turns
each into an OM ``AddLineageRequest`` via :func:`to_add_lineage_request`,
resolving entity FQNs to ids just before ``PUT /lineage`` (entities exist first).
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass, field

from lineage.column_lineage import LineageResult
from mapping import fqn

SOURCE_PIPELINE = "PipelineLineage"
SOURCE_QUERY = "QueryLineage"
SOURCE_VIEW = "ViewLineage"


@dataclass
class LineageEdge:
    from_fqn: str
    to_fqn: str
    source: str
    from_type: str = "table"
    to_type: str = "table"
    columns_lineage: list[dict] = field(default_factory=list)
    temp_lineage_tables: list[str] = field(default_factory=list)
    pipeline_fqn: str | None = None
    sql_query: str | None = None


def declared_edges(
    storage: dict,
    *,
    service_name: str,
    project: str,
    pipeline_fqn: str | None = None,
) -> list[LineageEdge]:
    """E16: coarse all-inputs -> all-outputs edges from a config's storage mapping."""
    inputs = (storage.get("input") or {}).get("tables") or []
    outputs = (storage.get("output") or {}).get("tables") or []
    in_fqns = [f for t in inputs if (f := fqn.table_fqn_from_storage_id(service_name, project, t.get("source", "")))]
    out_fqns = [
        f for t in outputs if (f := fqn.table_fqn_from_storage_id(service_name, project, t.get("destination", "")))
    ]
    edges: list[LineageEdge] = []
    for source_fqn in in_fqns:
        for dest_fqn in out_fqns:
            if source_fqn == dest_fqn:
                continue
            edges.append(
                LineageEdge(
                    from_fqn=source_fqn,
                    to_fqn=dest_fqn,
                    source=SOURCE_PIPELINE,
                    pipeline_fqn=pipeline_fqn,
                )
            )
    return edges


def view_edge(view_fqn: str, source_fqn: str) -> LineageEdge:
    """ViewLineage edge for a linked/shared bucket (base table -> view)."""
    return LineageEdge(from_fqn=source_fqn, to_fqn=view_fqn, source=SOURCE_VIEW)


def column_edges(
    result: LineageResult,
    *,
    service_name: str,
    project: str,
    pipeline_fqn: str | None = None,
) -> list[LineageEdge]:
    """E17: group resolved column edges by (from_table, to_table) into QueryLineage edges."""
    grouped: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for edge in result.column_edges:
        from_tbl_fqn = fqn.table_fqn_from_storage_id(service_name, project, edge.from_table)
        to_tbl_fqn = fqn.table_fqn_from_storage_id(service_name, project, edge.to_table)
        if not from_tbl_fqn or not to_tbl_fqn:
            continue
        grouped[(from_tbl_fqn, to_tbl_fqn)].append(
            {
                "fromColumns": [fqn.column_fqn(from_tbl_fqn, edge.from_column)],
                "toColumn": fqn.column_fqn(to_tbl_fqn, edge.to_column),
            }
        )

    temp = sorted(result.temp_lineage_tables)
    return [
        LineageEdge(
            from_fqn=from_tbl_fqn,
            to_fqn=to_tbl_fqn,
            source=SOURCE_QUERY,
            columns_lineage=cols,
            temp_lineage_tables=temp,
            pipeline_fqn=pipeline_fqn,
        )
        for (from_tbl_fqn, to_tbl_fqn), cols in grouped.items()
    ]


def to_add_lineage_request(
    edge: LineageEdge,
    resolve_id: Callable[[str, str], str | None],
) -> dict | None:
    """Build an OM ``AddLineageRequest`` dict, resolving FQNs -> entity ids.

    ``resolve_id(fqn, type)`` returns the entity id or ``None``; if either
    endpoint is missing the edge is skipped (referenced entity absent -> OM 404).
    """
    from_id = resolve_id(edge.from_fqn, edge.from_type)
    to_id = resolve_id(edge.to_fqn, edge.to_type)
    if not from_id or not to_id:
        return None

    details: dict = {"source": edge.source}
    if edge.columns_lineage:
        details["columnsLineage"] = edge.columns_lineage
    if edge.temp_lineage_tables:
        details["tempLineageTables"] = edge.temp_lineage_tables
    if edge.sql_query:
        details["sqlQuery"] = edge.sql_query
    if edge.pipeline_fqn:
        pipeline_id = resolve_id(edge.pipeline_fqn, "pipeline")
        if pipeline_id:
            details["pipeline"] = {"id": pipeline_id, "type": "pipeline"}

    return {
        "edge": {
            "fromEntity": {"id": from_id, "type": edge.from_type},
            "toEntity": {"id": to_id, "type": edge.to_type},
            "lineageDetails": details,
        }
    }
