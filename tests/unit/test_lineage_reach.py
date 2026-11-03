"""Lineage beyond a config's own mapping: row mappings, bucket paths, warehouse names, external buckets."""

import threading
from types import SimpleNamespace

import component as component_mod
import report as report_mod
from client.storage_reader import SourceBucket, SourceColumn, SourceTable
from component import ProjectContext, _ProjectRun
from configuration import CustomPropertiesMode, FailureMode, MergeMode
from lineage.column_lineage import extract_column_lineage
from lineage.dialect import dialect_for
from mapping import fqn as fqn_mod
from mapping import lineage_builder
from mapping.dashboard_builder import DashboardBuilder
from mapping.entity_builder import EntityBuilder
from mapping.pipeline_builder import PipelineBuilder
from merge import SnapshotStore, ThreeWayMerger
from sync import StateManager

SVC, PROJ, PID = "keboola", "Proj", "4214"


def _t(storage_id: str, paths: dict[str, str] | None = None) -> str:
    fqn = fqn_mod.table_fqn_from_storage_id(SVC, PROJ, storage_id, paths)
    assert fqn is not None
    return fqn


def _run(reader) -> _ProjectRun:
    return _ProjectRun(
        ctx=ProjectContext(project_id=PID, project_name=PROJ, storage_token="t", storage_url="https://s"),
        reader=reader,
        entities=EntityBuilder(SVC, PROJ, PID, "https://ui.example"),
        pipelines=PipelineBuilder(SVC, PROJ, PID, "https://ui.example"),
        dashboards=DashboardBuilder(SVC, PROJ, PID, "https://ui.example"),
    )


def _component() -> component_mod.Component:
    comp = component_mod.Component.__new__(component_mod.Component)  # bypass ComponentBase.__init__
    comp._config = SimpleNamespace(failure_mode=FailureMode.COLLECT_AND_FAIL)  # ty: ignore[invalid-assignment]
    return comp


# ------------------------------------------------------------- bucket paths


def test_lineage_names_a_linked_bucket_table_by_the_path_it_is_cataloged_under():
    """A linked bucket's Storage path is its source bucket's, and its tables are
    cataloged under that path, so a mapping that reads the linked table must too."""
    paths = {"in.c-shared": "out.c-source"}
    assert _t("in.c-shared.orders", paths) == f"{SVC}.{PROJ}.out_c-source.orders"
    assert _t("in.c-main.orders", paths) == f"{SVC}.{PROJ}.in_c-main.orders"  # unlisted: path == id


def test_declared_edge_reads_a_linked_bucket_table_by_its_cataloged_path():
    storage = {
        "input": {"tables": [{"source": "in.c-shared.orders", "destination": "orders"}]},
        "output": {"tables": [{"source": "result", "destination": "out.c-main.result"}]},
    }
    paths = {"in.c-shared": "out.c-source"}
    [edge] = lineage_builder.declared_edges(storage, service_name=SVC, project=PROJ, bucket_paths=paths)
    assert (edge.from_fqn, edge.to_fqn) == (_t("in.c-shared.orders", paths), _t("out.c-main.result"))


def test_view_of_a_linked_table_points_at_the_source_bucket_path():
    """The source project catalogs the table under its own bucket's path (a BigQuery
    dataset name, not the bucket id), which the link carries as ``sourceBucket.path``."""
    builder = EntityBuilder(SVC, PROJ, PID, "https://ui.example")
    bucket = SourceBucket(
        id="in.c-shared",
        name="c-shared",
        stage="in",
        path="out_c_sales",
        source_bucket={"id": "out.c-sales", "path": "out_c_sales"},
    )
    table = SourceTable(
        id="in.c-shared.orders",
        name="orders",
        columns=[SourceColumn(name="id")],
        source_table={"id": "out.c-sales.orders", "project": {"name": "Source"}},
    )
    built = builder.table_body(bucket, table)
    assert built.view_source_fqn == fqn_mod.table_fqn(SVC, "Source", "out_c_sales", "orders")


# ------------------------------------------------------- warehouse names in SQL


def _bigquery_lineage(schema_buckets: dict[str, str] | None):
    paths = {"in.c-main": "in_c_main", "out.c-main": "out_c_main"}
    return extract_column_lineage(
        [("main", "CREATE TABLE result AS SELECT o.id FROM `my-project.in_c_main.orders` o")],
        in_map={},
        out_map={"result": "out.c-main.result"},
        dialect=dialect_for(component_id="keboola.google-bigquery-transformation-v2"),
        service_name=SVC,
        project=PROJ,
        known_table_fqns={_t("in.c-main.orders", paths), _t("out.c-main.result", paths)},
        bucket_paths=paths,
        schema_buckets=schema_buckets,
    )


def test_bigquery_ref_by_dataset_name_resolves_to_its_bucket():
    result = _bigquery_lineage({"in_c_main": "in.c-main", "out_c_main": "out.c-main"})
    assert result.table_edges == {("in.c-main.orders", "out.c-main.result")}


def test_bigquery_ref_stays_unresolved_without_the_dataset_lookup():
    assert _bigquery_lineage(None).table_edges == set()


# ------------------------------------------------------------- row mappings


def test_empty_mappings_arrive_as_empty_arrays_and_produce_nothing():
    cfg = {"configuration": {"storage": []}, "rows": [{"configuration": {"storage": {"input": []}}}]}
    assert component_mod.Component._storages_of(cfg) == [{}]
    assert not component_mod.Component._is_producing(cfg)


def test_each_enabled_row_is_its_own_storage():
    cfg = {
        "configuration": {"storage": {"input": {"tables": [{"source": "in.c-a.top"}]}}},
        "rows": [
            {"configuration": {"storage": {"input": {"tables": [{"source": "in.c-a.r1"}]}}}},
            {"isDisabled": True, "configuration": {"storage": {"input": {"tables": [{"source": "in.c-a.off"}]}}}},
            {"configuration": {"parameters": {}}},
        ],
    }
    storages = component_mod.Component._storages_of(cfg)
    assert [[t["source"] for t in s["input"]["tables"]] for s in storages] == [["in.c-a.top"], ["in.c-a.r1"]]
    assert component_mod.Component._is_producing({"rows": cfg["rows"][1:2] + cfg["rows"][:1]})


def test_produced_outputs_leave_out_tables_a_row_maps():
    run = _run(None)
    run.produced_tables[("keboola.ex-db-mysql", "ex1")] = {"in.c-db.mapped", "in.c-db.unmapped"}
    cfg = {
        "id": "ex1",
        "rows": [{"configuration": {"storage": {"output": {"tables": [{"destination": "in.c-db.mapped"}]}}}}],
    }
    produced = component_mod.Component._produced_outputs(run, "keboola.ex-db-mysql", "extractor", cfg)
    assert produced == ["in.c-db.unmapped"]


class _RowsReader:
    """A writer mapping one table per row, and an app whose rows each read and write."""

    def list_buckets(self):
        return []

    def list_component_configs(self):
        def row(storage, **extra):
            return {"configuration": {"storage": storage}, **extra}

        return [
            {
                "id": "keboola.wr-db-snowflake",
                "type": "writer",
                "configurations": [
                    {
                        "id": "wr1",
                        "configuration": {"storage": []},
                        "rows": [
                            row({"input": {"tables": [{"source": "in.c-a.t1"}]}}),
                            row({"input": {"tables": [{"source": "in.c-a.t2"}]}}),
                            row({"input": {"tables": [{"source": "in.c-a.t3"}]}}, isDisabled=True),
                        ],
                    }
                ],
            },
            {
                "id": "kds-team.app-rows",
                "type": "application",
                "configurations": [
                    {
                        "id": "app1",
                        "configuration": {},
                        "rows": [
                            row(
                                {
                                    "input": {"tables": [{"source": "in.c-a.t1"}]},
                                    "output": {"tables": [{"destination": "out.c-b.o1"}]},
                                }
                            ),
                            row(
                                {
                                    "input": {"tables": [{"source": "in.c-a.t2"}]},
                                    "output": {"tables": [{"destination": "out.c-b.o2"}]},
                                }
                            ),
                            row({"input": {"tables": [{"source": "in.c-a.t4"}]}}),  # reads only
                        ],
                    }
                ],
            },
        ]


class _LineageOM:
    """Resolves every table and pipeline FQN; records each PUT /lineage."""

    is_2_0_or_newer = False

    def __init__(self):
        self.put_calls: list[dict] = []

    def get_by_fqn(self, kind, fqn, fields=None):
        return {"id": fqn}

    def put_lineage(self, request):
        self.put_calls.append(request)

    def delete_lineage_by_source(self, entity_type, fqn, source, entity_id=None):
        pass


def test_row_mappings_draw_lineage_without_pairing_rows():
    run = _run(_RowsReader())
    writer_fqn = fqn_mod.pipeline_fqn(SVC, PROJ, "wr1")
    app_fqn = fqn_mod.pipeline_fqn(SVC, PROJ, "app1")
    run.pipeline_fqn_by_config.update({"wr1": writer_fqn, "app1": app_fqn})
    om = _LineageOM()
    config = SimpleNamespace(
        parallel_requests=1,
        pipeline_lineage_as_node=False,
        write_table_lineage=True,
        write_pipeline_lineage=True,
        write_column_lineage=False,
        write_bucket_lineage=False,
        write_data_apps=False,
        write_dashboard_lineage=False,
    )
    _component()._lineage_pass(config, om, run, component_mod.RunReport(run_id="rid"), StateManager())  # ty: ignore[invalid-argument-type]

    pairs = {(r["edge"]["fromEntity"]["id"], r["edge"]["toEntity"]["id"]) for r in om.put_calls}
    # the writer: one table -> writer node edge per enabled row
    assert (_t("in.c-a.t1"), writer_fqn) in pairs
    assert (_t("in.c-a.t2"), writer_fqn) in pairs
    assert all(_t("in.c-a.t3") not in pair for pair in pairs)
    # the app: each row's input feeds only that row's output
    assert (_t("in.c-a.t1"), _t("out.c-b.o1")) in pairs
    assert (_t("in.c-a.t2"), _t("out.c-b.o2")) in pairs
    assert (_t("in.c-a.t1"), _t("out.c-b.o2")) not in pairs
    # its pipeline rides on those edges; the read-only row still shows it as a node
    assert (_t("in.c-a.t1"), app_fqn) not in pairs
    assert (_t("in.c-a.t4"), app_fqn) in pairs


# ---------------------------------------------------------- external buckets


def _hit(fqn: str, *, deleted: bool = False) -> dict:
    service_name, database, schema, table = fqn.split(".")
    return {
        "name": table,
        "fullyQualifiedName": fqn,
        "database": {"name": database},
        "databaseSchema": {"name": schema},
        "service": {"name": service_name},
        "deleted": deleted,
    }


def test_external_source_matches_names_ignoring_case_in_other_services():
    hits = [
        _hit("snowflake_prod.PROD_DB.SALES.ORDERS"),
        _hit("snowflake_prod.OTHER_DB.SALES.ORDERS"),  # other database
        _hit("snowflake_prod.PROD_DB.SALES.ORDERS_OLD"),  # other table
        _hit("keboola.Proj.SALES.ORDERS"),  # the writer's own service
        _hit("snowflake_old.PROD_DB.SALES.ORDERS", deleted=True),
    ]
    matches = lineage_builder.external_source_matches(hits, ("prod_db", "sales"), "orders", "keboola")
    assert matches == ["snowflake_prod.PROD_DB.SALES.ORDERS"]


def test_external_source_with_a_schema_only_path_matches_any_database():
    hits = [_hit("bq.proj-a.sales.orders"), _hit("bq.proj-b.sales.orders")]
    matches = lineage_builder.external_source_matches(hits, ("sales",), "orders", "keboola")
    assert matches == ["bq.proj-a.sales.orders", "bq.proj-b.sales.orders"]


def test_external_edge_maps_columns_of_the_same_name():
    edge = lineage_builder.external_edge(
        "keboola.Proj.in_c-ext.ORDERS", "sf.DB.S.ORDERS", ["ID", "AMOUNT", "NOTE"], ["id", "Amount", "has space"]
    )
    assert edge.source == "CrossDatabaseLineage"
    assert edge.columns_lineage == [
        {"fromColumns": ["sf.DB.S.ORDERS.id"], "toColumn": "keboola.Proj.in_c-ext.ORDERS.ID"},
        {"fromColumns": ["sf.DB.S.ORDERS.Amount"], "toColumn": "keboola.Proj.in_c-ext.ORDERS.AMOUNT"},
    ]


class _ExternalReader:
    def list_buckets(self):
        return [
            SourceBucket(
                id="in.c-ext",
                name="c-ext",
                stage="in",
                path="in.c-ext",
                has_external_schema=True,
                backend_path=("PROD_DB", "SALES"),
            )
        ]

    def iter_tables(self, bucket_id):
        return iter([SourceTable(id="in.c-ext.ORDERS", name="ORDERS", columns=[SourceColumn(name="ID")])])


class _ExternalOM:
    """Catalog OM double whose search finds ``source`` (when set) in another service."""

    is_2_0_or_newer = False
    source = "snowflake_prod.PROD_DB.SALES.ORDERS"

    def __init__(self, hits: list[dict]):
        self.hits = hits
        self.searches: list[tuple[str, str]] = []
        self.lineage: list[dict] = []
        self.created: dict[str, str] = {}
        self.lock = threading.Lock()

    def ensure_custom_properties(self, entity_type, specs, *, create=True):
        return set()

    def put_entity(self, kind, body):
        with self.lock:
            if kind == "tables":
                self.created[f"{body['databaseSchema']}.{body['name']}"] = f"id-{body['name']}"
        return {"id": f"id-{body['name']}"}

    def get_by_fqn(self, kind, fqn, fields=None):
        if kind != "tables":
            return None
        if fqn == self.source:
            return {"id": "id-source", "columns": [{"name": "ID"}]}
        return {"id": self.created[fqn]} if fqn in self.created else None

    def search_tables(self, table_name, schema_name, size=25):
        self.searches.append((table_name, schema_name))
        return self.hits

    def put_lineage(self, request):
        self.lineage.append(request)

    def find_user_id_by_email(self, email):
        return None


def _sync_external(om):
    config = SimpleNamespace(
        parallel_requests=1,
        full_refresh=False,
        buckets=[],
        write_buckets=True,
        write_table_lineage=True,
        custom_properties=CustomPropertiesMode.EXISTING_ONLY,
    )
    report = component_mod.RunReport(run_id="rid")
    _component()._catalog_pass(
        config,  # ty: ignore[invalid-argument-type]  (duck-typed test double)
        om,
        _run(_ExternalReader()),
        StateManager({"run_count": 1}),
        SnapshotStore(),
        report,
        ThreeWayMerger(MergeMode.THREE_WAY_MERGE),
        version_changed=False,
    )
    return report


def test_external_table_gets_an_edge_from_its_warehouse_source():
    om = _ExternalOM([_hit(_ExternalOM.source)])
    _sync_external(om)
    assert om.searches == [("ORDERS", "SALES")]
    [request] = om.lineage
    edge = request["edge"]
    assert (edge["fromEntity"]["id"], edge["toEntity"]["id"]) == ("id-source", "id-ORDERS")
    assert edge["lineageDetails"]["source"] == "CrossDatabaseLineage"
    assert edge["lineageDetails"]["columnsLineage"] == [
        {"fromColumns": [f"{_ExternalOM.source}.ID"], "toColumn": f"{SVC}.{PROJ}.in_c-ext.ORDERS.ID"}
    ]


def test_external_table_without_a_cataloged_source_is_reported_unresolved():
    om = _ExternalOM([])
    report = _sync_external(om)
    assert om.lineage == []
    [row] = [r for r in report.rows() if r["entity_type"] == "Lineage"]
    assert row["action"] == report_mod.ACTION_UNRESOLVED
    assert "PROD_DB.SALES.ORDERS: not in OpenMetadata" in row["detail"]
