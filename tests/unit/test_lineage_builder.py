import logging
import os
import subprocess
import sys
import textwrap
from collections import defaultdict
from pathlib import Path

from lineage.column_lineage import ColumnEdge, LineageResult, extract_column_lineage
from lineage.dialect import dialect_for
from mapping import fqn
from mapping.lineage_builder import (
    SOURCE_PIPELINE,
    SOURCE_QUERY,
    SOURCE_VIEW,
    column_edges,
    declared_edges,
    to_add_lineage_request,
    view_edge,
)

SVC = "keboola-stack"
PROJ = "Acme_Project"
SNOW = dialect_for(component_id="keboola.snowflake-transformation")


def _resolver(known):
    return lambda fqn, _type: known.get(fqn)


def test_declared_edges_are_n_by_m_and_tagged_pipeline_lineage():
    storage = {
        "input": {"tables": [{"source": "in.c-main.a"}, {"source": "in.c-main.b"}]},
        "output": {"tables": [{"destination": "out.c-res.x"}, {"destination": "out.c-res.y"}]},
    }
    edges = declared_edges(storage, service_name=SVC, project=PROJ, pipeline_fqn="keboola-stack.Acme_Project__99")
    assert len(edges) == 4  # 2 inputs x 2 outputs
    assert all(e.source == SOURCE_PIPELINE for e in edges)
    assert all(e.pipeline_fqn == "keboola-stack.Acme_Project__99" for e in edges)
    assert edges[0].from_fqn == "keboola-stack.Acme_Project.in_c-main.a"


def test_to_add_lineage_request_resolves_ids_and_attaches_pipeline():
    edge = declared_edges(
        {"input": {"tables": [{"source": "in.c-main.a"}]}, "output": {"tables": [{"destination": "out.c-res.x"}]}},
        service_name=SVC,
        project=PROJ,
        pipeline_fqn="keboola-stack.Acme_Project__99",
    )[0]
    known = {
        "keboola-stack.Acme_Project.in_c-main.a": "id-a",
        "keboola-stack.Acme_Project.out_c-res.x": "id-x",
        "keboola-stack.Acme_Project__99": "id-pipe",
    }
    req = to_add_lineage_request(edge, _resolver(known))
    assert req is not None
    assert req["edge"]["fromEntity"] == {"id": "id-a", "type": "table"}
    assert req["edge"]["toEntity"] == {"id": "id-x", "type": "table"}
    assert req["edge"]["lineageDetails"]["source"] == SOURCE_PIPELINE
    assert req["edge"]["lineageDetails"]["pipeline"] == {"id": "id-pipe", "type": "pipeline"}


def test_edge_skipped_when_entity_missing():
    edge = view_edge("keboola-stack.P.b.view", "keboola-stack.P.b.base")
    assert edge.source == SOURCE_VIEW
    # target entity not in OM yet -> resolver returns None -> edge skipped, never a dangling ref
    assert to_add_lineage_request(edge, _resolver({"keboola-stack.P.b.base": "id-base"})) is None


def test_column_edges_group_by_table_pair_and_carry_temp_tables():
    result = LineageResult(
        column_edges={
            ColumnEdge("in.c-main.orders", "id", "out.c-res.result", "id"),
            ColumnEdge("in.c-main.orders", "amount", "out.c-res.result", "total"),
        },
        temp_lineage_tables={"stg"},
    )
    edges = column_edges(result, service_name=SVC, project=PROJ, pipeline_fqn="keboola-stack.Acme_Project__7")
    assert len(edges) == 1  # one edge for the single table pair
    edge = edges[0]
    assert edge.source == SOURCE_QUERY
    assert edge.from_fqn == "keboola-stack.Acme_Project.in_c-main.orders"
    assert edge.to_fqn == "keboola-stack.Acme_Project.out_c-res.result"
    assert len(edge.columns_lineage) == 2
    assert edge.temp_lineage_tables == ["stg"]
    to_cols = {c["toColumn"] for c in edge.columns_lineage}
    assert "keboola-stack.Acme_Project.out_c-res.result.total" in to_cols


def test_declared_edges_skip_self_reference():
    storage = {
        "input": {"tables": [{"source": "out.c-res.x"}]},
        "output": {"tables": [{"destination": "out.c-res.x"}]},
    }
    assert declared_edges(storage, service_name=SVC, project=PROJ) == []


def test_column_edges_drop_self_loop_not_emitted_and_warn(caplog):
    # An SCD / self-snapshot config resolves the SAME storage table on both ends
    # (in.c-scd.snapshot read AND rewritten); the same-table pair would become an
    # OM self-loop (400). It must be dropped (not emitted) with a warning.
    result = LineageResult(
        column_edges={
            ColumnEdge("in.c-scd.snapshot", "id", "in.c-scd.snapshot", "id"),
            ColumnEdge("in.c-main.orders", "id", "out.c-res.result", "id"),  # normal edge, kept
        },
    )
    with caplog.at_level(logging.WARNING):
        edges = column_edges(result, service_name=SVC, project=PROJ)

    # the self-loop pair is gone; the normal edge survives unchanged
    assert len(edges) == 1
    assert edges[0].from_fqn == "keboola-stack.Acme_Project.in_c-main.orders"
    assert edges[0].to_fqn == "keboola-stack.Acme_Project.out_c-res.result"
    assert edges[0].from_fqn != edges[0].to_fqn
    assert any("self-loop" in rec.getMessage() for rec in caplog.records)


def test_to_add_lineage_request_drops_self_loop_view_edge(caplog):
    # A ViewLineage edge whose source FQN equals its target FQN (e.g. a linked
    # bucket pointing at itself) is caught by the central pre-PUT gate.
    edge = view_edge("keboola-stack.P.b.t", "keboola-stack.P.b.t")
    assert edge.source == SOURCE_VIEW
    with caplog.at_level(logging.WARNING):
        req = to_add_lineage_request(edge, _resolver({"keboola-stack.P.b.t": "id-t"}))
    assert req is None
    assert any("self-loop" in rec.getMessage() for rec in caplog.records)


def test_column_edges_degrade_to_table_level_when_target_is_empty_stub(caplog):
    # (a) An SCD / self-snapshot output ("out.c-scd.snapshot") is declared but never
    # materialised in Storage, so it is cataloged as an empty stub (columns=[]).
    # A columnsLineage entry to it would name columns the table lacks -> OM 400 on
    # the WHOLE edge. It must degrade to a table-level edge (no columnsLineage) with
    # a warning, since both endpoint tables exist in the catalog.
    result = LineageResult(
        column_edges={ColumnEdge("in.c-main.orders", "id", "out.c-scd.snapshot", "snapshot_pk")},
    )
    catalog = {
        "keboola-stack.Acme_Project.in_c-main.orders": {"id"},
        "keboola-stack.Acme_Project.out_c-scd.snapshot": set(),  # empty stub
    }
    with caplog.at_level(logging.WARNING):
        edges = column_edges(result, service_name=SVC, project=PROJ, column_catalog=catalog)

    assert len(edges) == 1
    edge = edges[0]
    assert edge.source == SOURCE_QUERY
    assert edge.from_fqn == "keboola-stack.Acme_Project.in_c-main.orders"
    assert edge.to_fqn == "keboola-stack.Acme_Project.out_c-scd.snapshot"
    assert edge.columns_lineage == []  # no columnsLineage -> OM cannot 400 on a missing column
    assert any("snapshot" in rec.getMessage() for rec in caplog.records)


def test_column_edges_drop_mapping_referencing_unknown_column(caplog):
    # (b) One mapping references a column the target does not have ("missing"); it is
    # dropped, while the sibling mapping with a real column survives.
    result = LineageResult(
        column_edges={
            ColumnEdge("in.c-main.orders", "amount", "out.c-res.result", "total"),  # valid
            ColumnEdge("in.c-main.orders", "id", "out.c-res.result", "missing"),  # target lacks 'missing'
        },
    )
    catalog = {
        "keboola-stack.Acme_Project.in_c-main.orders": {"id", "amount"},
        "keboola-stack.Acme_Project.out_c-res.result": {"total"},
    }
    with caplog.at_level(logging.WARNING):
        edges = column_edges(result, service_name=SVC, project=PROJ, column_catalog=catalog)

    assert len(edges) == 1
    edge = edges[0]
    assert len(edge.columns_lineage) == 1  # only the valid mapping remains
    assert edge.columns_lineage[0]["toColumn"] == "keboola-stack.Acme_Project.out_c-res.result.total"
    assert any("missing" in rec.getMessage() for rec in caplog.records)


def test_column_edges_keep_fully_valid_edge_unchanged():
    # (c) Every referenced column exists in the catalog -> the edge is emitted with
    # its columnsLineage intact, identical to the no-catalog behaviour.
    result = LineageResult(
        column_edges={
            ColumnEdge("in.c-main.orders", "id", "out.c-res.result", "id"),
            ColumnEdge("in.c-main.orders", "amount", "out.c-res.result", "total"),
        },
        temp_lineage_tables={"stg"},
    )
    catalog = {
        "keboola-stack.Acme_Project.in_c-main.orders": {"id", "amount"},
        "keboola-stack.Acme_Project.out_c-res.result": {"id", "total"},
    }
    edges = column_edges(result, service_name=SVC, project=PROJ, column_catalog=catalog)

    assert len(edges) == 1
    edge = edges[0]
    assert len(edge.columns_lineage) == 2
    assert edge.temp_lineage_tables == ["stg"]
    to_cols = {c["toColumn"] for c in edge.columns_lineage}
    assert to_cols == {
        "keboola-stack.Acme_Project.out_c-res.result.id",
        "keboola-stack.Acme_Project.out_c-res.result.total",
    }


def test_e17_end_to_end_sql_to_add_lineage_request():
    """E17 seam — real transformation SQL all the way to the OM AddLineageRequest.

    Compensates the DROPPED functional case ``10_run_column_lineage_only``: the two
    halves (SQL text -> ``extract_column_lineage`` in test_lineage.py, and
    ``LineageResult`` -> edges -> request here) are each covered, but nothing else
    chains them. This drives a multi-step transformation (with an intermediate temp
    table) through the parser, groups + validates the parsed column edges against
    the cataloged column sets, and asserts the resulting ``QueryLineage``
    ``AddLineageRequest`` carries the parser-derived ``columnsLineage`` and the
    pipeline attachment.
    """
    statements = [
        ("load_stg", 'CREATE TABLE "stg" AS SELECT "id", "amount" FROM "src"'),
        ("build_result", 'INSERT INTO "result" SELECT "id", "amount" AS "total" FROM "stg"'),
    ]
    result = extract_column_lineage(
        statements,
        in_map={"src": "in.c-main.orders"},
        out_map={"result": "out.c-sales.result"},
        dialect=SNOW,
    )
    # (1) the parser resolved both columns through the intermediate temp table
    assert ColumnEdge("in.c-main.orders", "id", "out.c-sales.result", "id") in result.column_edges
    assert ColumnEdge("in.c-main.orders", "amount", "out.c-sales.result", "total") in result.column_edges
    assert "stg" in result.temp_lineage_tables

    # (2) group + validate against the endpoint tables' cataloged column sets
    catalog = {
        "keboola-stack.Acme_Project.in_c-main.orders": {"id", "amount"},
        "keboola-stack.Acme_Project.out_c-sales.result": {"id", "total"},
    }
    edges = column_edges(
        result,
        service_name=SVC,
        project=PROJ,
        pipeline_fqn="keboola-stack.Acme_Project__42",
        column_catalog=catalog,
    )
    assert len(edges) == 1
    edge = edges[0]
    assert edge.source == SOURCE_QUERY
    assert edge.from_fqn == "keboola-stack.Acme_Project.in_c-main.orders"
    assert edge.to_fqn == "keboola-stack.Acme_Project.out_c-sales.result"
    assert edge.temp_lineage_tables == ["stg"]

    # (3) resolve FQNs -> OM ids and assert the final PUT /lineage payload
    known = {
        edge.from_fqn: "id-src",
        edge.to_fqn: "id-result",
        "keboola-stack.Acme_Project__42": "id-pipe",
    }
    req = to_add_lineage_request(edge, _resolver(known))
    assert req is not None
    details = req["edge"]["lineageDetails"]
    assert req["edge"]["fromEntity"] == {"id": "id-src", "type": "table"}
    assert req["edge"]["toEntity"] == {"id": "id-result", "type": "table"}
    assert details["source"] == SOURCE_QUERY
    assert details["tempLineageTables"] == ["stg"]
    assert details["pipeline"] == {"id": "id-pipe", "type": "pipeline"}
    to_cols = {c["toColumn"] for c in details["columnsLineage"]}
    assert to_cols == {
        "keboola-stack.Acme_Project.out_c-sales.result.id",
        "keboola-stack.Acme_Project.out_c-sales.result.total",
    }


# --------------------------------------------------------------------------
# Determinism: the column-lineage set feeds emitted output (PUT /lineage
# payloads + warning logs); its iteration order must be a stable function of
# the edge set, not Python's per-process string-hash randomization.
# --------------------------------------------------------------------------


def _emitted_pairs(edges):
    return [(e.from_fqn, e.to_fqn) for e in edges]


def test_column_edge_is_stably_sortable():
    # ColumnEdge sorts by (from_table, from_column, to_table, to_column).
    edges = [
        ColumnEdge("b", "1", "z", "9"),
        ColumnEdge("a", "2", "z", "9"),
        ColumnEdge("a", "1", "z", "9"),
        ColumnEdge("a", "1", "y", "9"),
    ]
    assert sorted(edges) == [
        ColumnEdge("a", "1", "y", "9"),
        ColumnEdge("a", "1", "z", "9"),
        ColumnEdge("a", "2", "z", "9"),
        ColumnEdge("b", "1", "z", "9"),
    ]


def test_column_edges_emission_order_equals_stable_sort():
    # Several edges across several table-pairs, with several columns per pair.
    result = LineageResult(
        column_edges={
            ColumnEdge("in.c-main.orders", "id", "out.c-res.result", "id"),
            ColumnEdge("in.c-main.orders", "amount", "out.c-res.result", "total"),
            ColumnEdge("in.c-main.orders", "qty", "out.c-res.summary", "q"),
            ColumnEdge("in.c-ext.people", "name", "out.c-res.result", "who"),
            ColumnEdge("in.c-ext.people", "age", "out.c-res.summary", "a"),
        },
    )
    edges = column_edges(result, service_name=SVC, project=PROJ)

    # Expected emission derived directly from the ColumnEdge stable-sort key:
    # table-pairs in first-seen order, columnsLineage in the same order.
    expected_pairs: list[tuple[str, str]] = []
    expected_cols: dict[tuple[str, str], list[str]] = defaultdict(list)
    for ce in sorted(result.column_edges):
        from_fqn = fqn.table_fqn_from_storage_id(SVC, PROJ, ce.from_table)
        to_fqn = fqn.table_fqn_from_storage_id(SVC, PROJ, ce.to_table)
        assert from_fqn and to_fqn  # all test storage ids resolve
        pair = (from_fqn, to_fqn)
        if pair not in expected_pairs:
            expected_pairs.append(pair)
        expected_cols[pair].append(fqn.column_fqn(to_fqn, ce.to_column))

    assert _emitted_pairs(edges) == expected_pairs
    for e in edges:
        got = [c["toColumn"] for c in e.columns_lineage]
        assert got == expected_cols[(e.from_fqn, e.to_fqn)]


def test_column_edges_drop_warning_order_is_deterministic(caplog):
    # Every mapping references a column absent from the (empty-stub) target set,
    # so all are dropped with a "Dropping column-level mapping" warning. The
    # warning sequence must follow the stable sort key, not hash order.
    result = LineageResult(
        column_edges={
            ColumnEdge("in.c-main.orders", "id", "out.c-res.result", "gone_c"),
            ColumnEdge("in.c-main.orders", "amount", "out.c-res.result", "gone_a"),
            ColumnEdge("in.c-main.orders", "qty", "out.c-res.result", "gone_b"),
        },
    )
    catalog = {
        "keboola-stack.Acme_Project.in_c-main.orders": {"id", "amount", "qty"},
        "keboola-stack.Acme_Project.out_c-res.result": {"real"},  # none of the targets exist
    }
    with caplog.at_level(logging.WARNING):
        column_edges(result, service_name=SVC, project=PROJ, column_catalog=catalog)

    drops = [r.getMessage() for r in caplog.records if "Dropping column-level mapping" in r.getMessage()]
    assert len(drops) == 3
    # The warnings appear in ColumnEdge stable-sort order (from_table constant,
    # so by from_column here: amount < id < qty), which the source-column
    # substring reflects — never in hash order.
    from_cols_in_order = [m.split(".orders.")[1].split(" ")[0] for m in drops]
    assert from_cols_in_order == ["amount", "id", "qty"]


def _emit_across_hash_seed(seed: int) -> str:
    """Run the column-lineage emission in a fresh interpreter under a fixed
    PYTHONHASHSEED and return its serialized emitted order."""
    src_dir = str(Path(__file__).resolve().parents[2] / "src")
    script = textwrap.dedent(
        """
        import json
        from lineage.column_lineage import ColumnEdge, LineageResult
        from mapping.lineage_builder import column_edges

        result = LineageResult(column_edges={
            ColumnEdge("in.c-main.orders", "id", "out.c-res.result", "id"),
            ColumnEdge("in.c-main.orders", "amount", "out.c-res.result", "total"),
            ColumnEdge("in.c-main.orders", "qty", "out.c-res.summary", "q"),
            ColumnEdge("in.c-ext.people", "name", "out.c-res.result", "who"),
            ColumnEdge("in.c-ext.people", "age", "out.c-res.summary", "a"),
            ColumnEdge("in.c-ext.people", "city", "out.c-res.summary", "c"),
        })
        edges = column_edges(result, service_name="keboola-stack", project="Acme_Project")
        out = [(e.from_fqn, e.to_fqn, [c["toColumn"] for c in e.columns_lineage]) for e in edges]
        print(json.dumps(out))
        """
    )
    env = dict(os.environ, PYTHONHASHSEED=str(seed), PYTHONPATH=src_dir)
    proc = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, env=env, check=True)
    return proc.stdout.strip()


def test_column_edges_order_is_hash_seed_independent():
    # The recorder's finding, reproduced across processes: without the stable
    # sort the emitted order differs between hash seeds; with it, it is identical.
    out_seed_1 = _emit_across_hash_seed(1)
    out_seed_2 = _emit_across_hash_seed(2)
    out_seed_3 = _emit_across_hash_seed(42)
    assert out_seed_1  # non-empty guard
    assert out_seed_1 == out_seed_2 == out_seed_3


def test_to_add_lineage_request_drops_id_level_self_loop(caplog):
    # Two distinct FQNs that resolve to the SAME OM entity id are a malformed
    # self-referential payload (entities exist -> not a 404 -> OM 400). Skipped.
    edge = declared_edges(
        {"input": {"tables": [{"source": "in.c-main.a"}]}, "output": {"tables": [{"destination": "out.c-res.x"}]}},
        service_name=SVC,
        project=PROJ,
    )[0]
    known = {edge.from_fqn: "same-id", edge.to_fqn: "same-id"}
    with caplog.at_level(logging.WARNING):
        req = to_add_lineage_request(edge, _resolver(known))
    assert req is None
    assert any("same OM entity" in rec.getMessage() for rec in caplog.records)
