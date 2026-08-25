"""Column-level SQL lineage via SQLGlot (E17, spec 1/4.2, T13).

Seeded by our own ``lineage-spike/`` PoC (clean-room; copies nothing from the
Collate-headed reference lineage files). Parses a transformation's SQL, traces
each output column to a physical upstream column, resolves workspace names to
Keboola storage table ids via the config's storage mapping, and chains through
workspace intermediates (recorded as ``tempLineageTables``).

A parse/lineage miss is *recorded* in the resolution taxonomy, never raised
(spec 6.3) — the run always continues.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass, field

import sqlglot
from sqlglot import exp
from sqlglot.lineage import lineage as sqlglot_lineage

from lineage.dialect import DialectInfo
from lineage.resolution import CoverageMetrics, Resolution

logger = logging.getLogger(__name__)


@dataclass(frozen=True, order=True)
class ColumnEdge:
    # ``order=True`` makes edges deterministically sortable by
    # (from_table, from_column, to_table, to_column) — the stable key used
    # wherever the (unordered) ``column_edges`` set feeds emitted output/logs,
    # so PUT /lineage payloads and warning order no longer depend on Python's
    # per-process string-hash randomization.
    from_table: str
    from_column: str
    to_table: str
    to_column: str


@dataclass
class LineageResult:
    column_edges: set[ColumnEdge] = field(default_factory=set)
    table_edges: set[tuple[str, str]] = field(default_factory=set)
    temp_lineage_tables: set[str] = field(default_factory=set)
    metrics: CoverageMetrics = field(default_factory=CoverageMetrics)
    unresolved_notes: list[str] = field(default_factory=list)


def _has_column_ref(node: exp.Expression) -> bool:
    """A projection with no column reference is a literal / clock / sequence."""
    return any(True for _ in node.find_all(exp.Column))


def _probe_order(name: str, dialect: DialectInfo) -> tuple[str, ...]:
    quoted = f'"{name}"'
    if dialect.unquoted_case == "upper":
        return (quoted, name)
    if dialect.unquoted_case == "lower":
        return (name, quoted)
    return (name, quoted)


class _WorkspaceGraph:
    """Accumulates raw workspace-level table/column edges before storage resolution."""

    def __init__(self) -> None:
        self.ws_schema: dict[str, list[str]] = {}
        self.table_edges: set[tuple[str, str]] = set()
        self.col_edges: set[tuple[str, str, str, str]] = set()
        self.star_tables: set[str] = set()


def _select_and_target(
    tree: exp.Expression, graph: _WorkspaceGraph
) -> tuple[exp.Expression | None, str | None, list[str]]:
    """Return (select_expr, target_table_name, explicit_target_cols) for CREATE-AS / INSERT."""
    if isinstance(tree, exp.Create):
        created = tree.this.find(exp.Table) if tree.this else None
        cols = [c.name for c in tree.find_all(exp.ColumnDef)]
        if created is not None and cols:
            graph.ws_schema[created.name] = cols
        if isinstance(tree.expression, exp.Select | exp.Union):
            return tree.expression, (created.name if created is not None else None), cols
        return None, None, []

    if isinstance(tree, exp.Insert):
        target = tree.this
        table = target.find(exp.Table) if target else None
        tgt_tbl = table.name if table is not None else None
        explicit = [i.name for i in target.find_all(exp.Identifier)][1:] if target is not None else []
        if isinstance(tree.expression, exp.Select):
            return tree.expression, tgt_tbl, explicit
    return None, None, []


def _probe_lineage(name: str, select: exp.Expression, dialect: DialectInfo):
    """Try each identifier-casing probe; return the first lineage node or None."""
    last_error: Exception | None = None
    for probe in _probe_order(name, dialect):
        try:
            return sqlglot_lineage(probe, select, dialect=dialect.sqlglot_dialect)
        except Exception as exc:  # noqa: BLE001 - a lineage miss is recorded, never raised
            last_error = exc
    logger.debug("lineage probe failed for %s: %s", name, last_error)
    return None


def _trace_projection(
    proj: exp.Expression,
    name: str,
    select: exp.Expression,
    tgt_tbl: str | None,
    dialect: DialectInfo,
    graph: _WorkspaceGraph,
    result: LineageResult,
    note_prefix: str,
) -> None:
    if not _has_column_ref(proj):
        result.metrics.add(Resolution.NO_UPSTREAM)
        return

    node = _probe_lineage(name, select, dialect)
    if node is None:
        result.metrics.add(Resolution.UNRESOLVED)
        result.unresolved_notes.append(f"{note_prefix}.{name}: lineage error")
        return

    found = star = False
    for leaf in node.walk():
        src = leaf.source
        if isinstance(src, exp.Table):
            src_tbl = src.name
            src_col = leaf.name.split(".")[-1].strip('"')
            graph.table_edges.add((src_tbl, tgt_tbl or ""))
            if src_col == "*":
                star = True
                graph.star_tables.add(src_tbl)
                continue
            graph.col_edges.add((src_tbl, src_col, tgt_tbl or "", name))
            found = True

    if found:
        result.metrics.add(Resolution.RESOLVED)
    elif star:
        result.metrics.add(Resolution.NEEDS_SCHEMA)
        result.unresolved_notes.append(f"{note_prefix}.{name}: SELECT * needs source schema")
    else:
        result.metrics.add(Resolution.UNRESOLVED)
        result.unresolved_notes.append(f"{note_prefix}.{name}: no physical source table")


def extract_column_lineage(
    statements: list[tuple[str, str]],
    *,
    in_map: dict[str, str],
    out_map: dict[str, str],
    dialect: DialectInfo,
) -> LineageResult:
    """Extract resolved column/table lineage for one transformation config.

    Args:
        statements: ordered ``(code_name, sql)`` pairs.
        in_map: workspace input alias -> storage source table id.
        out_map: workspace output alias -> storage destination table id.
        dialect: resolved :class:`DialectInfo`.
    """
    result = LineageResult()
    if not dialect.is_sql:
        return result

    graph = _WorkspaceGraph()
    for code_name, sql in statements:
        try:
            trees = sqlglot.parse(sql, dialect=dialect.sqlglot_dialect)
        except Exception:  # noqa: BLE001 - parse miss recorded per statement, never raised
            result.metrics.add(Resolution.UNRESOLVED)
            result.unresolved_notes.append(f"{code_name}: parse error")
            continue

        for tree in trees:
            if tree is None:
                continue
            select, tgt_tbl, explicit = _select_and_target(tree, graph)
            if select is None:
                continue
            first = select.find(exp.Select) if isinstance(select, exp.Union) else select
            projections = first.expressions if first is not None else []
            tgt_cols = explicit or graph.ws_schema.get(tgt_tbl or "", [])
            for i, proj in enumerate(projections):
                name = proj.alias_or_name or (tgt_cols[i] if i < len(tgt_cols) else None)
                if not name:
                    continue
                _trace_projection(proj, name, select, tgt_tbl, dialect, graph, result, code_name)

    _resolve_to_storage(graph, in_map, out_map, result)
    return result


def _add_column_edge(result: LineageResult, from_table: str, from_col: str, to_table: str, to_col: str) -> None:
    """Record a resolved column edge, dropping same-storage-table self-references.

    A column has no lineage to itself; an SCD / self-snapshot config that reads and
    rewrites the same storage table would otherwise yield a ``from_table ==
    to_table`` edge that becomes an OM self-loop (rejected 400 downstream), so it is
    never emitted (spec 4.2).
    """
    if from_table == to_table:
        return
    result.column_edges.add(ColumnEdge(from_table, from_col, to_table, to_col))


def _classify(name: str, in_map: dict[str, str], out_map: dict[str, str]) -> tuple[str, str]:
    """Return (storage_id_or_name, kind) with kind in {input, output, intermediate}."""
    if name in in_map:
        return in_map[name], "input"
    if name in out_map:
        return out_map[name], "output"
    return name, "intermediate"


def _resolve_to_storage(
    graph: _WorkspaceGraph,
    in_map: dict[str, str],
    out_map: dict[str, str],
    result: LineageResult,
) -> None:
    """Resolve workspace edges to storage-to-storage edges, chaining intermediates."""
    # --- table edges: transitive reachability input -> ... -> output ---------
    # Every iteration below is over a sorted view of an otherwise-unordered set,
    # so the whole resolution pass (and thus emitted edge/temp-table membership
    # and order) is stable given the same input, independent of hash seeding.
    succ: dict[str, set[str]] = defaultdict(set)
    for s, t in sorted(graph.table_edges):
        if t:
            succ[s].add(t)

    starts = sorted(set(succ.keys()) | {t for targets in succ.values() for t in targets})
    for start in starts:
        sid, skind = _classify(start, in_map, out_map)
        if skind != "input":
            continue
        for target, mids in _reachable_outputs(start, succ, in_map, out_map):
            tid, _ = _classify(target, in_map, out_map)
            result.table_edges.add((sid, tid))
            result.temp_lineage_tables.update(mids)

    # --- column edges: chain (table,col) nodes from output back to input ------
    col_succ: dict[tuple[str, str], set[tuple[str, str]]] = defaultdict(set)
    for s_tbl, s_col, t_tbl, t_col in sorted(graph.col_edges):
        if t_tbl:
            col_succ[(s_tbl, s_col)].add((t_tbl, t_col))

    for (s_tbl, s_col), targets in col_succ.items():
        s_id, s_kind = _classify(s_tbl, in_map, out_map)
        for t_tbl, t_col in sorted(targets):
            t_id, t_kind = _classify(t_tbl, in_map, out_map)
            if s_kind == "input" and t_kind == "output":
                _add_column_edge(result, s_id, s_col, t_id, t_col)

    # one-hop chaining: input.col -> intermediate.col -> output.col
    for (s_tbl, s_col), targets in col_succ.items():
        s_id, s_kind = _classify(s_tbl, in_map, out_map)
        if s_kind != "input":
            continue
        for mid_tbl, mid_col in sorted(targets):
            _, mid_kind = _classify(mid_tbl, in_map, out_map)
            if mid_kind != "intermediate":
                continue
            for out_tbl, out_col in sorted(col_succ.get((mid_tbl, mid_col), set())):
                out_id, out_kind = _classify(out_tbl, in_map, out_map)
                if out_kind == "output" and s_id != out_id:
                    _add_column_edge(result, s_id, s_col, out_id, out_col)
                    result.temp_lineage_tables.add(mid_tbl)


def _reachable_outputs(
    start: str,
    succ: dict[str, set[str]],
    in_map: dict[str, str],
    out_map: dict[str, str],
) -> list[tuple[str, list[str]]]:
    """BFS downstream from ``start``; yield (output_name, intermediate_ids_on_path)."""
    results: list[tuple[str, list[str]]] = []
    stack: list[tuple[str, list[str]]] = [(start, [])]
    visited: set[str] = set()
    while stack:
        node, mids = stack.pop()
        if node in visited:
            continue
        visited.add(node)
        for nxt in sorted(succ.get(node, ())):
            _, kind = _classify(nxt, in_map, out_map)
            if kind == "output":
                results.append((nxt, mids))
            elif kind == "intermediate":
                stack.append((nxt, [*mids, nxt]))
    return results
