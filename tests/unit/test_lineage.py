from lineage.column_lineage import ColumnEdge, extract_column_lineage
from lineage.dialect import dialect_for
from lineage.resolution import CoverageMetrics, Resolution

SNOW = dialect_for(component_id="keboola.snowflake-transformation")


def test_dialect_resolution():
    assert dialect_for(component_id="keboola.snowflake-transformation").sqlglot_dialect == "snowflake"
    assert dialect_for(backend="bigquery").sqlglot_dialect == "bigquery"
    assert dialect_for(backend="redshift").unquoted_case == "lower"
    assert dialect_for(component_id="keboola.python-transformation-v2").is_sql is False
    assert dialect_for(backend="something-unknown").name == "generic"


def test_coverage_metrics_fraction():
    m = CoverageMetrics()
    m.add(Resolution.RESOLVED)
    m.add(Resolution.RESOLVED)
    m.add(Resolution.UNRESOLVED)
    m.add(Resolution.NO_UPSTREAM)  # excluded from denominator
    assert m.resolved == 2
    assert m.traceable == 3
    assert round(m.resolved_fraction, 3) == round(2 / 3, 3)


def test_snowflake_direct_resolved_edges():
    stmts = [("code1", 'INSERT INTO "result" SELECT "id", "amount" FROM "src"')]
    res = extract_column_lineage(
        stmts,
        in_map={"src": "in.c-main.orders"},
        out_map={"result": "out.c-sales.result"},
        dialect=SNOW,
    )
    assert ColumnEdge("in.c-main.orders", "id", "out.c-sales.result", "id") in res.column_edges
    assert ("in.c-main.orders", "out.c-sales.result") in res.table_edges
    assert res.metrics.counts[Resolution.RESOLVED] == 2


def test_multi_step_chain_resolves_via_temp_lineage_tables():
    stmts = [
        ("stg", 'CREATE TABLE "stg" AS SELECT "id", "amount" FROM "src"'),
        ("out", 'CREATE TABLE "result" AS SELECT "id", "amount" AS "total" FROM "stg"'),
    ]
    res = extract_column_lineage(
        stmts,
        in_map={"src": "in.c-main.orders"},
        out_map={"result": "out.c-sales.result"},
        dialect=SNOW,
    )
    assert ColumnEdge("in.c-main.orders", "id", "out.c-sales.result", "id") in res.column_edges
    assert ColumnEdge("in.c-main.orders", "amount", "out.c-sales.result", "total") in res.column_edges
    assert "stg" in res.temp_lineage_tables
    assert ("in.c-main.orders", "out.c-sales.result") in res.table_edges


def test_ctas_does_not_silently_under_report():
    stmts = [("code1", 'CREATE TABLE "result" AS SELECT "id" FROM "src"')]
    res = extract_column_lineage(
        stmts,
        in_map={"src": "in.c-main.orders"},
        out_map={"result": "out.c-sales.result"},
        dialect=SNOW,
    )
    # The CTAS column must be RESOLVED, not silently NO_UPSTREAM / UNRESOLVED.
    assert res.metrics.counts[Resolution.RESOLVED] == 1
    assert res.metrics.counts[Resolution.NO_UPSTREAM] == 0
    assert ColumnEdge("in.c-main.orders", "id", "out.c-sales.result", "id") in res.column_edges


def test_literal_projection_is_no_upstream():
    stmts = [("code1", 'INSERT INTO "result" SELECT CURRENT_TIMESTAMP() AS "ts" FROM "src"')]
    res = extract_column_lineage(
        stmts,
        in_map={"src": "in.c-main.orders"},
        out_map={"result": "out.c-sales.result"},
        dialect=SNOW,
    )
    assert res.metrics.counts[Resolution.NO_UPSTREAM] == 1
    assert res.metrics.counts[Resolution.RESOLVED] == 0


def test_unparseable_sql_records_unresolved_and_continues():
    stmts = [("bad", "SELECT FROM WHERE )))((("), ("ok", 'INSERT INTO "result" SELECT "id" FROM "src"')]
    res = extract_column_lineage(
        stmts,
        in_map={"src": "in.c-main.orders"},
        out_map={"result": "out.c-sales.result"},
        dialect=SNOW,
    )
    # unparseable statement recorded, but the good one still resolves
    assert res.metrics.counts[Resolution.UNRESOLVED] >= 1
    assert res.metrics.counts[Resolution.RESOLVED] == 1


def test_non_sql_dialect_returns_empty():
    stmts = [("py", "print('hello')")]
    res = extract_column_lineage(
        stmts,
        in_map={},
        out_map={},
        dialect=dialect_for(component_id="keboola.python-transformation-v2"),
    )
    assert res.column_edges == set()
    assert res.metrics.total == 0
