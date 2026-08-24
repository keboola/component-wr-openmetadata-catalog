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
