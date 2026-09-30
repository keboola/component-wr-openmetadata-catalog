"""Phase B: SQL-inferred input lineage (E17 cont'd, lineage-phaseB-contract.md).

A transformation increasingly reads an upstream table via a DIRECT
fully-qualified SQL ref (``"PROJDB"."in.c-bucket"."table"``) instead of
Keboola's declared input mapping, so ``storage.input.tables`` is empty and the
column engine previously dropped that source as "intermediate" (its bare
``exp.Table.name`` never matched the empty declared ``in_map``). These tests
lock in the fix: the workspace graph keys a table ref on its QUALIFIED name
(``db.name``) when the parser reports one, and an ``inferred_in_map`` /
``inferred_out_map`` promotes a qualified ref to a real source/target only
when its derived Table FQN is one the run actually cataloged
(``known_table_fqns``) — filtering out CTEs, scratch tables and
non-cataloged/cross-project refs. Declared ``in_map``/``out_map`` always win.
"""

import sqlglot
from sqlglot import exp

from lineage.column_lineage import ColumnEdge, extract_column_lineage
from lineage.dialect import dialect_for
from lineage.resolution import Resolution
from mapping import fqn as fqn_mod

SNOW = dialect_for(component_id="keboola.snowflake-transformation")
SVC = "keboola-stack"
PROJ = "Acme_Project"


def _known_fqns(*storage_ids: str) -> set[str]:
    """Build a ``known_table_fqns`` set for these fixtures, narrowed to ``set[str]``.

    ``table_fqn_from_storage_id`` returns ``str | None`` for an unparseable id; every
    storage id in this file is well-formed, so the walrus-filtered comprehension both
    narrows the type and would silently drop a typo'd fixture id rather than leaking
    ``None`` into the set.
    """
    return {f for sid in storage_ids if (f := fqn_mod.table_fqn_from_storage_id(SVC, PROJ, sid))}


def test_sqlglot_splits_quoted_dotted_schema_into_db_and_name_snowflake():
    """De-risking spike (run FIRST): verify the contract's core SQLGlot
    assumption empirically before relying on it anywhere else. A Snowflake
    quoted identifier that itself contains a dot (``"in.c-bucket"`` — the
    Keboola bucket id) is parsed as a SINGLE ``.db`` segment, dot included —
    it is NOT split into two separate identifiers. If this ever changes
    upstream, every other test in this file (and the qualified-key
    implementation) would need to change with it.
    """
    sql = 'INSERT INTO "out"."t" SELECT "a" FROM "PROJDB"."in.c-bucket"."src"'
    (tree,) = sqlglot.parse(sql, dialect="snowflake")
    assert tree is not None
    tables = list(tree.find_all(exp.Table))
    (source,) = [t for t in tables if t.name == "src"]
    assert source.db == "in.c-bucket"
    assert source.name == "src"
    # storage_id = f"{db}.{name}" reproduces the Keboola storage id exactly.
    assert f"{source.db}.{source.name}" == "in.c-bucket.src"


def test_direct_ref_resolves_via_inferred_in_map_5922_pattern():
    """The verified real-world pattern (project 5922): OUTPUT mapping is
    declared, input mapping is empty, the source is read via a direct
    cross-schema ref. ``known_table_fqns`` cataloging both endpoints is what
    lets the qualified ref be trusted as a real input."""
    stmts = [
        (
            "code1",
            'INSERT INTO "result" SELECT "a", "b" FROM "PROJDB"."in.c-bucket"."src"',
        )
    ]
    known_fqns = _known_fqns("in.c-bucket.src", "out.c-x.t")
    res = extract_column_lineage(
        stmts,
        in_map={},
        out_map={"result": "out.c-x.t"},
        dialect=SNOW,
        service_name=SVC,
        project=PROJ,
        known_table_fqns=known_fqns,
    )
    assert ("in.c-bucket.src", "out.c-x.t") in res.table_edges
    assert ColumnEdge("in.c-bucket.src", "a", "out.c-x.t", "a") in res.column_edges
    assert ColumnEdge("in.c-bucket.src", "b", "out.c-x.t", "b") in res.column_edges
    assert res.metrics.counts[Resolution.RESOLVED] == 2


def test_direct_ref_target_also_qualified_uses_inferred_out_map():
    """Outputs stay declared-primary, but a qualified CREATE/INSERT target is
    optionally inferred the same way when it is cataloged too — both sides of
    a fully direct-SQL transform (no declared mapping at all) resolve.

    Uses lowercase-quoted identifiers matching Keboola's canonical bucket-id
    casing: SQLGlot preserves a quoted identifier's case exactly (verified in
    the spike test above), so a mixed-case fixture would only be testing
    incidental case-folding, not the inference logic itself.
    """
    stmts = [
        (
            "code1",
            'INSERT INTO "out.c-x"."t" SELECT "a", "b" FROM "PROJDB"."in.c-bucket"."src"',
        )
    ]
    known_fqns = _known_fqns("in.c-bucket.src", "out.c-x.t")
    res = extract_column_lineage(
        stmts,
        in_map={},
        out_map={},
        dialect=SNOW,
        service_name=SVC,
        project=PROJ,
        known_table_fqns=known_fqns,
    )
    assert ("in.c-bucket.src", "out.c-x.t") in res.table_edges
    assert ColumnEdge("in.c-bucket.src", "a", "out.c-x.t", "a") in res.column_edges


def test_non_cataloged_qualified_ref_is_not_inferred_as_input():
    """A qualified ref whose derived FQN is NOT in ``known_table_fqns`` (a CTE
    alias's backing scratch table, a workspace temp, a cross-project ref the
    run never cataloged) must never be promoted to an input — it stays
    unresolved, dropped, and is never raised."""
    stmts = [
        (
            "code1",
            'INSERT INTO "result" SELECT "a" FROM "WORKDB"."scratch"."tmp"',
        )
    ]
    # known_table_fqns deliberately omits scratch.tmp's FQN.
    known_fqns = _known_fqns("out.c-x.t")
    res = extract_column_lineage(
        stmts,
        in_map={},
        out_map={"result": "out.c-x.t"},
        dialect=SNOW,
        service_name=SVC,
        project=PROJ,
        known_table_fqns=known_fqns,
    )
    assert res.table_edges == set()
    assert res.column_edges == set()


def test_bare_workspace_alias_with_declared_in_map_unaffected_by_phase_b_params():
    """Regression: a bare workspace-alias statement with a DECLARED in_map
    resolves exactly as before, even when the new Phase-B params are supplied
    (Phase-B is additive — it must never change the declared path)."""
    stmts = [("code1", 'INSERT INTO "result" SELECT "id", "amount" FROM "src"')]
    known_fqns = _known_fqns("in.c-main.orders")
    res = extract_column_lineage(
        stmts,
        in_map={"src": "in.c-main.orders"},
        out_map={"result": "out.c-sales.result"},
        dialect=SNOW,
        service_name=SVC,
        project=PROJ,
        known_table_fqns=known_fqns,
    )
    assert ColumnEdge("in.c-main.orders", "id", "out.c-sales.result", "id") in res.column_edges
    assert ("in.c-main.orders", "out.c-sales.result") in res.table_edges
    assert res.metrics.counts[Resolution.RESOLVED] == 2


def test_declared_in_map_wins_over_inferred_on_same_qualified_key():
    """Merge precedence: if a qualified key happens to collide with an
    explicit declared ``in_map`` entry, the declared mapping wins."""
    stmts = [("code1", 'INSERT INTO "result" SELECT "a" FROM "PROJDB"."in.c-bucket"."src"')]
    known_fqns = _known_fqns("in.c-bucket.src")
    res = extract_column_lineage(
        stmts,
        in_map={"in.c-bucket.src": "in.c-bucket.OVERRIDDEN"},
        out_map={"result": "out.c-x.t"},
        dialect=SNOW,
        service_name=SVC,
        project=PROJ,
        known_table_fqns=known_fqns,
    )
    assert ("in.c-bucket.OVERRIDDEN", "out.c-x.t") in res.table_edges
    assert ("in.c-bucket.src", "out.c-x.t") not in res.table_edges
    assert ColumnEdge("in.c-bucket.OVERRIDDEN", "a", "out.c-x.t", "a") in res.column_edges


def test_known_table_fqns_none_disables_inference_entirely():
    """Omitting ``known_table_fqns`` (the default) disables Phase B outright —
    a qualified ref with no known-FQN gate never becomes an input, matching
    every pre-Phase-B call site (this is what keeps the existing
    ``tests/unit/test_lineage.py`` suite green unmodified)."""
    stmts = [("code1", 'INSERT INTO "result" SELECT "a" FROM "PROJDB"."in.c-bucket"."src"')]
    res = extract_column_lineage(
        stmts,
        in_map={},
        out_map={"result": "out.c-x.t"},
        dialect=SNOW,
    )
    assert res.table_edges == set()
    assert res.column_edges == set()
