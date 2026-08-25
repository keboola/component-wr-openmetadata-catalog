import logging

from lineage.column_lineage import ColumnEdge, LineageResult
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
