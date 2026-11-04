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


def _bigquery_lineage(schema_buckets: dict[str, str] | None, *, ref_project: str = "my-project"):
    paths = {"in.c-main": "in_c_main", "out.c-main": "out_c_main"}
    return extract_column_lineage(
        [("main", f"CREATE TABLE result AS SELECT o.id FROM `{ref_project}.in_c_main.orders` o")],
        in_map={},
        out_map={"result": "out.c-main.result"},
        dialect=dialect_for(component_id="keboola.google-bigquery-transformation-v2"),
        service_name=SVC,
        project=PROJ,
        known_table_fqns={_t("in.c-main.orders", paths), _t("out.c-main.result", paths)},
        bucket_paths=paths,
        schema_buckets=schema_buckets,
        schema_databases={"in_c_main": "my-project", "out_c_main": "my-project"},
    )


def test_bigquery_ref_by_dataset_name_resolves_to_its_bucket():
    result = _bigquery_lineage({"in_c_main": "in.c-main", "out_c_main": "out.c-main"})
    assert result.table_edges == {("in.c-main.orders", "out.c-main.result")}


def test_bigquery_ref_to_another_project_stays_unresolved():
    """Another GCP project's dataset of the same name is not this project's bucket."""
    result = _bigquery_lineage({"in_c_main": "in.c-main"}, ref_project="other-project")
    assert result.table_edges == set()


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


def test_table_metadata_names_outputs_only_when_no_mapping_declares_one():
    """The metadata is a fallback: a table keeps naming a config after the config
    stops writing it, so any output mapping (a disabled row's too) wins."""
    run = _run(None)
    run.produced_tables[("keboola.ex-db-mysql", "ex1")] = {"in.c-db.orders"}

    def produced(rows):
        cfg = {"id": "ex1", "configuration": {"storage": []}, "rows": rows}
        return component_mod.Component._produced_outputs(run, "keboola.ex-db-mysql", "extractor", cfg)

    def row(destination, **extra):
        return {"configuration": {"storage": {"output": {"tables": [{"destination": destination}]}}}, **extra}

    assert produced([{"configuration": {"parameters": {"outputTable": "in.c-db.orders"}}}]) == ["in.c-db.orders"]
    assert produced([row("in.c-db.renamed")]) == []
    assert produced([row("in.c-db.orders", isDisabled=True)]) == []


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

    def __init__(
        self,
        hits: list[dict],
        *,
        search_error: Exception | None = None,
        put_error: Exception | None = None,
        source_readable: bool = True,
    ):
        self.hits = hits
        self.search_error = search_error
        self.put_error = put_error
        self.source_readable = source_readable
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
            return {"id": "id-source", "columns": [{"name": "ID"}]} if self.source_readable else None
        return {"id": self.created[fqn]} if fqn in self.created else None

    def search_tables(self, table_name, schema_name, size=25):
        self.searches.append((table_name, schema_name))
        if self.search_error:
            raise self.search_error
        return self.hits

    def put_lineage(self, request):
        if self.put_error and request["edge"]["lineageDetails"]["source"] == "CrossDatabaseLineage":
            raise self.put_error
        self.lineage.append(request)

    def find_user_id_by_email(self, email):
        return None


def _sync_external(om, state: StateManager | None = None):
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
        state or StateManager({"run_count": 1}),
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


def _lineage_row(report) -> dict:
    [row] = [r for r in report.rows() if r["entity_type"] == "Lineage"]
    return row


def test_external_source_found_twice_or_unreadable_is_unresolved():
    twice = _sync_external(_ExternalOM([_hit(_ExternalOM.source), _hit("snowflake_dr.PROD_DB.SALES.ORDERS")]))
    assert "several OpenMetadata tables match" in _lineage_row(twice)["detail"]

    unreadable = _sync_external(_ExternalOM([_hit(_ExternalOM.source)], source_readable=False))
    row = _lineage_row(unreadable)
    assert row["action"] == report_mod.ACTION_UNRESOLVED
    assert f"search found {_ExternalOM.source}, but it could not be read" in row["detail"]


def test_external_lookup_failure_is_reported_and_retried_next_run():
    state = StateManager({"run_count": 1})
    report = _sync_external(_ExternalOM([], search_error=component_mod.OMClientError("503")), state)
    assert _lineage_row(report)["action"] == report_mod.ACTION_FAILED
    assert state.bucket_digest(PID, "in.c-ext") is None  # the bucket re-syncs next run


def test_permission_error_on_the_source_service_does_not_stop_the_run():
    """Every Keboola entity was just written with the same token: a 403 here is about
    the other service, so it fails this edge only."""
    state = StateManager({"run_count": 1})
    om = _ExternalOM([_hit(_ExternalOM.source)], put_error=component_mod.OMAuthError("403"))
    report = _sync_external(om, state)
    row = _lineage_row(report)
    assert row["action"] == report_mod.ACTION_FAILED
    assert "may lack the right" in row["detail"]
    assert state.bucket_digest(PID, "in.c-ext") is None


def test_a_schema_name_two_buckets_share_names_neither():
    class _Reader:
        def list_buckets(self):
            def bucket(bucket_id, path, *, linked=False):
                return SourceBucket(
                    id=bucket_id,
                    name=bucket_id.split(".")[1],
                    path=bucket_id,
                    backend_path=path,
                    source_bucket={"id": "out.c-x"} if linked else None,
                )

            return [
                bucket("in.c-ext1", ("DB1", "PUBLIC")),
                bucket("in.c-ext2", ("DB2", "PUBLIC")),
                bucket("in.c-main", ("KBC", "in.c-main")),
                bucket("in.c-link", ("OTHER", "in.c-main"), linked=True),
            ]

    run = _run(_Reader())
    component_mod.Component._index_buckets(run)
    assert run.schema_buckets == {"in.c-main": "in.c-main"}  # own bucket wins over the linked one


def test_layout_switch_keeps_a_node_edge_another_row_writes():
    """Row 1 has a table edge, so its node edges are legacy; row 2 only reads the
    same table, so it writes that table -> pipeline edge. It must not be deleted."""

    class _Reader:
        def list_buckets(self):
            return []

        def list_component_configs(self):
            def row(storage):
                return {"configuration": {"storage": storage}}

            return [
                {
                    "id": "kds-team.app-rows",
                    "type": "application",
                    "configurations": [
                        {
                            "id": "app1",
                            "rows": [
                                row(
                                    {
                                        "input": {"tables": [{"source": "in.c-a.t1"}]},
                                        "output": {"tables": [{"destination": "out.c-b.o1"}]},
                                    }
                                ),
                                row({"input": {"tables": [{"source": "in.c-a.t1"}]}}),
                            ],
                        }
                    ],
                }
            ]

    class _OM(_LineageOM):
        def __init__(self):
            super().__init__()
            self.deleted: list[tuple[str, str]] = []

        def delete_lineage_edge(self, from_type, from_id, to_type, to_id):
            self.deleted.append((from_id, to_id))

    run = _run(_Reader())
    app_fqn = fqn_mod.pipeline_fqn(SVC, PROJ, "app1")
    run.pipeline_fqn_by_config["app1"] = app_fqn
    om = _OM()
    config = SimpleNamespace(
        parallel_requests=1,
        pipeline_lineage_as_node=False,
        write_table_lineage=True,
        write_pipeline_lineage=True,
        write_column_lineage=False,
        write_bucket_lineage=False,
    )
    _component()._lineage_pass(
        config,  # ty: ignore[invalid-argument-type]  (duck-typed test double)
        om,  # ty: ignore[invalid-argument-type]  (duck-typed test double)
        run,
        component_mod.RunReport(run_id="rid"),
        StateManager(),
        drop_legacy_node_edges=True,
    )
    assert (_t("in.c-a.t1"), app_fqn) not in om.deleted
    assert (app_fqn, _t("out.c-b.o1")) in om.deleted  # row 1's legacy output edge still goes


def test_alias_of_a_table_here_uses_the_project_name_override():
    builder = EntityBuilder(SVC, "Override", PID, "https://ui.example")
    builder.bucket_paths = {"in.c-src": "in.c-src"}
    table = SourceTable(
        id="in.c-a.alias",
        name="alias",
        is_alias=True,
        columns=[SourceColumn(name="id")],
        source_table={"id": "in.c-src.t", "project": {"id": int(PID), "name": "Real Name"}},
    )
    built = builder.table_body(SourceBucket(id="in.c-a", name="c-a", stage="in", path="in.c-a"), table)
    assert built.view_source_fqn == fqn_mod.table_fqn(SVC, "Override", "in.c-src", "t")


def test_external_edge_prefers_the_exact_column_over_one_differing_in_case():
    """A quoted Snowflake table can hold both ``ID`` and ``id``: each maps to itself."""
    edge = lineage_builder.external_edge("k.P.s.T", "sf.D.S.T", ["ID", "id", "Note"], ["ID", "id", "NOTE", "note"])
    assert edge.columns_lineage == [
        {"fromColumns": ["sf.D.S.T.ID"], "toColumn": "k.P.s.T.ID"},
        {"fromColumns": ["sf.D.S.T.id"], "toColumn": "k.P.s.T.id"},
    ]  # "Note" has two case-only matches and no exact one: left out


# ------------------------------------------------------------- data app code


def test_code_table_refs_finds_qualified_names_in_strings():
    code = '''
    q = """SELECT * FROM "KBC_USE4_5922"."in.c-otlp"."traces" t JOIN "out.c-main"."users" u"""
    other = "SELECT 1 FROM \\"in.c-esc\\".\\"t2\\""
    bq = "SELECT * FROM `my-project.in_c_main.orders`"
    label = ", ".join(["a", "b"])
    '''
    assert lineage_builder.code_table_refs(code) == {
        (("KBC_USE4_5922",), "in.c-otlp", "traces"),
        ((), "out.c-main", "users"),
        ((), "in.c-esc", "t2"),
        (("my-project",), "in_c_main", "orders"),
    }


def _data_app_run(script: str):
    class _Reader:
        def list_buckets(self):
            return [
                SourceBucket(
                    id="in.c-otlp", name="c-otlp", path="in.c-otlp", backend_path=("KBC_USE4_5922", "in.c-otlp")
                )
            ]

    run = _run(_Reader())
    component_mod.Component._index_buckets(run)
    run.seen_table_fqns.add(_t("in.c-otlp.traces"))
    dashboard_fqn = fqn_mod.dashboard_fqn(SVC, PROJ, "app1")
    run.dashboard_fqn_by_config["app1"] = dashboard_fqn
    cfg = {"id": "app1", "configuration": {"storage": {}, "parameters": {"script": [script]}}}
    config = SimpleNamespace(write_data_apps=True, write_dashboard_lineage=True)
    edges = _component()._data_app_edges_for(config, run, cfg, {})
    return {(e.from_fqn, e.to_fqn) for e in edges}, dashboard_fqn


def test_data_app_without_input_mapping_reads_the_tables_its_code_queries():
    pairs, dashboard_fqn = _data_app_run('query_data("""SELECT * FROM "KBC_USE4_5922"."in.c-otlp"."traces" """)')
    assert pairs == {(_t("in.c-otlp.traces"), dashboard_fqn)}


def test_data_app_code_naming_another_project_or_an_uncataloged_table_gets_no_edge():
    pairs, _ = _data_app_run('"KBC_USE4_9999"."in.c-otlp"."traces" and "in.c-otlp"."missing"')
    assert pairs == set()


# ------------------------------------------------------ one Database per project


def _owner_check(source_url):
    existing = None if source_url is None else {"id": "db-1", "sourceUrl": source_url}
    component_mod.Component._check_database_owner(_run(None), f"{SVC}.{PROJ}", existing)


def test_a_database_of_another_project_with_the_same_name_stops_the_run():
    """'[CF] Sales' and '(CF) Sales' both map to CF_Sales: their runs would overwrite
    each other and tombstone each other's tables in buckets of the same name."""
    try:
        _owner_check("https://connection.keboola.com/admin/projects/9382/storage")
    except component_mod.ProjectNameCollision as exc:
        assert "already belongs to Keboola project 9382" in str(exc)
        assert "Project Name Override" in str(exc)
    else:
        raise AssertionError("expected ProjectNameCollision")


def test_the_projects_own_database_or_one_without_a_project_url_passes():
    for url in (
        f"https://connection.us-east4.gcp.keboola.com/admin/projects/{PID}/storage",
        f"https://connection.keboola.com/admin/projects/{PID}",  # host and suffix may differ
        "",
        None,
    ):
        _owner_check(url)


class _ForeignDatabaseOM(_ExternalOM):
    """The project's Database exists, written by another project of the same name."""

    def __init__(self):
        super().__init__([])
        self.puts: list[str] = []

    def get_by_fqn(self, kind, fqn, fields=None):
        if kind == "databases":
            return {"id": "db-1", "sourceUrl": "https://connection.keboola.com/admin/projects/9382/storage"}
        return super().get_by_fqn(kind, fqn, fields)

    def put_entity(self, kind, body):
        self.puts.append(kind)
        return super().put_entity(kind, body)


def test_catalog_pass_writes_nothing_for_a_project_whose_database_is_taken():
    om = _ForeignDatabaseOM()
    try:
        _sync_external(om)
    except component_mod.ProjectNameCollision:
        pass
    else:
        raise AssertionError("expected ProjectNameCollision")
    assert om.puts == ["databaseServices"]  # the shared service only: no database, schema or table


def test_code_refs_skip_a_database_built_at_run_time():
    """Apps take the database from the environment: the name still resolves by bucket and table."""
    code = """
    sql = f'SELECT * FROM "{DB}"."in.c-otlp"."traces"'
    env = f'SELECT * FROM "{os.environ["KBC_DB"]}"."in.c-otlp"."traces"'
    bq = f"SELECT * FROM `{project}.in_c_main.orders`"
    """
    refs = lineage_builder.code_table_refs(code)
    assert ((), "in.c-otlp", "traces") in refs
    assert ((), "in_c_main", "orders") in refs
    assert not any(databases for databases, _, _ in refs)


def test_code_refs_ignore_commented_out_queries():
    code = '# SELECT * FROM "in.c-old"."gone"  (old query)\nq = "SELECT 1 FROM \\"in.c-live\\".\\"t\\""\n'
    assert lineage_builder.code_table_refs(code) == {((), "in.c-live", "t")}


def test_data_app_reads_a_table_through_an_f_string_database():
    pairs, dashboard_fqn = _data_app_run('query_data(f\'SELECT * FROM "{DB}"."in.c-otlp"."traces"\')')
    assert pairs == {(_t("in.c-otlp.traces"), dashboard_fqn)}


def test_data_app_with_no_inline_code_or_odd_shapes_has_no_code_inputs():
    run = _run(None)
    for cfg in (
        {"id": "a", "configuration": {"parameters": {"dataApp": {"git": {"repository": "https://example.com/r"}}}}},
        {"id": "a", "configuration": {"parameters": {"script": None}}},
        {"id": "a", "configuration": {"parameters": {"script": {"main": "x"}}}},
        {"id": "a", "configuration": {"parameters": {"script": [1, None]}}},
        {"id": "a", "configuration": {"parameters": ["x"]}},
        {"id": "a", "configuration": []},
    ):
        assert component_mod.Component._code_inputs(run, cfg) == []


def test_bigquery_data_app_resolves_the_dataset_name_and_checks_the_project():
    class _Reader:
        def list_buckets(self):
            return [
                SourceBucket(id="in.c-main", name="c-main", path="in_c_main", backend_path=("my-project", "in_c_main"))
            ]

    run = _run(_Reader())
    component_mod.Component._index_buckets(run)
    run.seen_table_fqns.add(_t("in.c-main.orders", {"in.c-main": "in_c_main"}))

    def inputs(code):
        return component_mod.Component._code_inputs(run, {"configuration": {"parameters": {"script": code}}})

    assert inputs("SELECT * FROM `my-project.in_c_main.orders`") == ["in.c-main.orders"]
    assert inputs("SELECT * FROM `other-project.in_c_main.orders`") == []
