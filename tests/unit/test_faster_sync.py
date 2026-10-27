"""Run-time savings: one Storage call per bucket, change checks, lineage skip, parallel writes."""

import dataclasses
import threading
import time
from types import SimpleNamespace
from unittest import mock

import pytest

import component as component_mod
import report as report_mod
from client.om_client import OMClient
from client.storage_reader import SourceBucket, SourceColumn, SourceTable, StorageReader, needs_detail
from component import ProjectContext, _ProjectRun
from configuration import Configuration, CustomPropertiesMode, FailureMode, MergeMode
from mapping import entity_builder, lineage_builder
from mapping.dashboard_builder import DashboardBuilder
from mapping.entity_builder import EntityBuilder
from mapping.pipeline_builder import PipelineBuilder
from merge import SnapshotStore, ThreeWayMerger
from sync import StateManager, bucket_digest, digest_fields, lineage_digest

SVC, PROJ, PID = "keboola", "Proj", "4214"


class _Response:
    def __init__(self, body):
        self.status_code = 200
        self._body = body

    def json(self):
        return self._body


def _run(reader=None) -> _ProjectRun:
    return _ProjectRun(
        ctx=ProjectContext(project_id=PID, project_name=PROJ, storage_token="t", storage_url="https://s"),
        reader=reader,  # ty: ignore[invalid-argument-type]
        entities=EntityBuilder(SVC, PROJ, PID, "https://ui.example"),
        pipelines=PipelineBuilder(SVC, PROJ, PID, "https://ui.example"),
        dashboards=DashboardBuilder(SVC, PROJ, PID, "https://ui.example"),
    )


def _component() -> component_mod.Component:
    comp = component_mod.Component.__new__(component_mod.Component)  # bypass ComponentBase.__init__
    comp._config = SimpleNamespace(failure_mode=FailureMode.COLLECT_AND_FAIL)  # ty: ignore[invalid-assignment]
    return comp


# ------------------------------------------------------------- Storage reads


def test_iter_tables_reads_a_bucket_in_one_call_with_columns_and_metadata():
    session = mock.Mock()
    session.get.return_value = _Response(
        [
            {"id": "in.c-a.t1", "name": "t1", "columns": ["id"], "isTyped": True},
            {"id": "in.c-a.t2", "name": "t2", "columns": ["id"], "isAlias": True},
            {"id": "in.c-a.t3", "name": "t3", "columns": ["id"]},
        ]
    )
    reader = StorageReader("https://connection.keboola.com", "t", session=session)

    tables = list(reader.iter_tables("in.c-a"))

    session.get.assert_called_once()
    assert session.get.call_args.kwargs["params"] == {"include": "columns,metadata,columnMetadata"}
    assert [t.name for t in tables] == ["t1", "t2", "t3"]
    # typed definitions and alias sources are only in the per-table GET
    assert [needs_detail(t) for t in tables] == [True, True, False]


class _CatalogReader:
    """Two buckets with one plain and one typed table each; records detail reads."""

    def __init__(self):
        self.detail_reads: list[str] = []
        self.buckets = [
            SourceBucket(id="in.c-a", name="c-a", stage="in", path="in.c-a"),
            SourceBucket(id="in.c-b", name="c-b", stage="in", path="in.c-b"),
        ]

    def list_buckets(self):
        return list(self.buckets)

    def iter_tables(self, bucket_id):
        return iter(
            [
                SourceTable(id=f"{bucket_id}.plain", name="plain", columns=[SourceColumn(name="id")], row_count=5),
                SourceTable(id=f"{bucket_id}.typed", name="typed", columns=[SourceColumn(name="id")], is_typed=True),
            ]
        )

    def get_table(self, table_id):
        self.detail_reads.append(table_id)
        return SourceTable(
            id=table_id,
            name=table_id.rsplit(".", 1)[-1],
            columns=[SourceColumn(name="id", definition={"type": "NUMBER"})],
            is_typed=True,
        )


class _CatalogOM:
    """Keeps the service and database it created, so a later run is a real re-run."""

    def __init__(self, custom_properties: set[str] | None = None):
        self.custom_properties = custom_properties or set()
        self.puts: list[tuple[str, str]] = []
        self.kept: dict[str, dict] = {}
        self.lock = threading.Lock()

    def get_by_fqn(self, kind, fqn, fields=None):
        return self.kept.get(kind)

    def put_entity(self, kind, body):
        with self.lock:
            self.puts.append((kind, body["name"]))
            if kind in ("databaseServices", "databases"):
                self.kept[kind] = body
        return {"id": f"id-{body['name']}"}

    def ensure_custom_properties(self, entity_type, specs, *, create=True):
        return set(self.custom_properties)

    def put_lineage(self, request):
        return {}

    def list_entities(self, kind, params=None, page_size=200):
        return iter([])

    def find_user_id_by_email(self, email):
        return None

    @property
    def is_2_0_or_newer(self):
        return False


def _catalog_config(**overrides):
    base = {
        "parallel_requests": 4,
        "full_refresh": False,
        "buckets": [],
        "write_buckets": True,
        "custom_properties": CustomPropertiesMode.EXISTING_ONLY,
    }
    return SimpleNamespace(**{**base, **overrides})


def _sync_catalog(reader, om, state, config):
    run = _run(reader)
    report = component_mod.RunReport(run_id="rid")
    _component()._catalog_pass(
        config,
        om,
        run,
        state,
        SnapshotStore(),
        report,
        ThreeWayMerger(MergeMode.THREE_WAY_MERGE),
        version_changed=False,
    )
    return run, report


def test_unchanged_bucket_costs_no_detail_read_and_no_tombstone_scope():
    reader = _CatalogReader()
    state = StateManager({"run_count": 1})
    om = _CatalogOM()
    _sync_catalog(reader, om, state, _catalog_config())
    assert sorted(reader.detail_reads) == ["in.c-a.typed", "in.c-b.typed"]  # first run: only typed tables

    reader.detail_reads.clear()
    om.puts.clear()
    run, report = _sync_catalog(reader, om, state, _catalog_config())

    assert reader.detail_reads == []
    assert [p for p in om.puts if p[0] in ("tables", "databaseSchemas")] == []
    assert run.seen_schema_fqns == set()  # nothing to tombstone in an unchanged bucket
    assert len(run.seen_table_fqns) == 4  # still seen: lineage and tombstoning rely on it
    schema_rows = [r for r in report.rows() if r["entity_type"] == "Schema"]
    assert [r["entity_fqn"] for r in schema_rows] == ["in.c-a", "in.c-b"]  # input order kept
    assert {r["action"] for r in schema_rows} == {report_mod.ACTION_SKIPPED_UNCHANGED}


def test_table_statistics_change_the_digest_only_when_written():
    table = SourceTable(id="in.c-a.t", name="t", row_count=1, data_size_bytes=10, last_import_date="2026-10-01")
    loaded = SourceTable(id="in.c-a.t", name="t", row_count=2, data_size_bytes=20, last_import_date="2026-10-02")
    volatile = frozenset(entity_builder.TABLE_STAT_PROPERTIES)
    bucket = {"id": "in.c-a"}

    assert bucket_digest(bucket, [digest_fields(table, exclude=volatile)]) == bucket_digest(
        bucket, [digest_fields(loaded, exclude=volatile)]
    )
    assert bucket_digest(bucket, [digest_fields(table)]) != bucket_digest(bucket, [digest_fields(loaded)])


def test_bucket_resyncs_on_new_data_only_when_row_count_property_is_written():
    class LoadingReader(_CatalogReader):
        rows = 5

        def iter_tables(self, bucket_id):
            LoadingReader.rows += 1  # every run loads new rows
            return iter([SourceTable(id=f"{bucket_id}.t", name="t", row_count=LoadingReader.rows)])

    for written, expected_puts in ((set(), 0), (set(entity_builder.TABLE_STAT_PROPERTIES.values()), 2)):
        state = StateManager({"run_count": 1})
        om = _CatalogOM(written)
        _sync_catalog(LoadingReader(), om, state, _catalog_config())
        om.puts.clear()
        _sync_catalog(LoadingReader(), om, state, _catalog_config())
        assert len([p for p in om.puts if p[0] == "tables"]) == expected_puts


# ----------------------------------------------------------------- lineage


class _LineageOM:
    def __init__(self, known_ids: dict[str, str], *, fail_puts_to: str | None = None):
        self.known_ids = known_ids
        self.fail_puts_to = fail_puts_to
        self.calls: list[tuple] = []
        self.lock = threading.Lock()
        self.is_2_0_or_newer = False

    def get_by_fqn(self, kind, fqn, fields=None):
        entity_id = self.known_ids.get(fqn)
        return {"id": entity_id} if entity_id else None

    def put_lineage(self, request):
        to_id = request["edge"]["toEntity"]["id"]
        with self.lock:
            self.calls.append(("put", request["edge"]["fromEntity"]["id"], to_id))
        if self.fail_puts_to and to_id == self.known_ids[self.fail_puts_to]:
            raise component_mod.OMClientError("boom")
        return {}

    def delete_lineage_by_source(self, entity_type, fqn, source, entity_id=None):
        assert entity_id == self.known_ids[fqn]  # the id is resolved once, by the caller
        with self.lock:
            self.calls.append(("delete", entity_type, fqn, source))


def _t(name: str) -> str:
    return f"{SVC}.{PROJ}.in_c-a.{name}"


PIPE = f"{SVC}-{PROJ}.cfg1"


def _edges(pipeline: str | None = PIPE):
    return [
        lineage_builder.LineageEdge(from_fqn=_t("a"), to_fqn=_t("b"), source="PipelineLineage", pipeline_fqn=pipeline),
        lineage_builder.LineageEdge(from_fqn=_t("b"), to_fqn=_t("c"), source="PipelineLineage"),
    ]


def _known():
    return {_t("a"): "id-a", _t("b"): "id-b", _t("c"): "id-c", PIPE: "id-pipe"}


def _refresh(om, state, edges, *, force=False, created=(), parallel=4):
    run = _run()
    run.created_fqns.update(created)
    report = component_mod.RunReport(run_id="rid")
    _component()._refresh_lineage(
        SimpleNamespace(parallel_requests=parallel), om, run, report, state, edges, force=force
    )
    return report


def _puts(om):
    return [c for c in om.calls if c[0] == "put"]


def test_unchanged_lineage_target_is_neither_deleted_nor_rewritten():
    state = StateManager()
    first = _LineageOM(_known())
    _refresh(first, state, _edges())
    assert len(_puts(first)) == 2

    second = _LineageOM(_known())
    report = _refresh(second, state, _edges())

    assert second.calls == []
    assert {r["action"] for r in report.rows()} == {report_mod.ACTION_SKIPPED_UNCHANGED}
    assert len(report.rows()) == 2


def test_only_the_changed_target_is_rewritten():
    state = StateManager()
    _refresh(_LineageOM(_known()), state, _edges())

    om = _LineageOM(_known())
    changed = [_edges()[0], lineage_builder.LineageEdge(from_fqn=_t("a"), to_fqn=_t("c"), source="PipelineLineage")]
    _refresh(om, state, changed)

    assert {c[2] for c in om.calls} == {"id-c", _t("c")}  # target c only: its deletes and its put


@pytest.mark.parametrize(
    ("kwargs", "label"),
    [({"force": True}, "full refresh"), ({"created": {_t("a")}}, "endpoint created this run")],
)
def test_forced_or_recreated_lineage_is_rewritten(kwargs, label):
    state = StateManager()
    _refresh(_LineageOM(_known()), state, _edges())
    om = _LineageOM(_known())
    _refresh(om, state, _edges(), **kwargs)
    assert _puts(om), label


def test_failed_edge_is_retried_next_run():
    state = StateManager()
    _refresh(_LineageOM(_known(), fail_puts_to=_t("c")), state, _edges())
    om = _LineageOM(_known())
    _refresh(om, state, _edges())
    assert [c[2] for c in _puts(om)] == ["id-c"]


def test_rewritten_pipeline_target_rewrites_every_edge_that_carries_it():
    """OpenMetadata's delete-by-source on a pipeline drops every edge carrying it.

    A flow -> pipeline edge changes; the a -> b edge carries that pipeline, so it
    must be written again even though its own target b did not change.
    """
    flow = f"{SVC}-{PROJ}.flow"
    known = {**_known(), flow: "id-flow"}
    flow_edge = lineage_builder.LineageEdge(
        from_fqn=flow, to_fqn=PIPE, source="PipelineLineage", from_type="pipeline", to_type="pipeline"
    )
    state = StateManager()
    _refresh(_LineageOM(known), state, [*_edges(), flow_edge])

    om = _LineageOM(known)
    _refresh(om, state, [*_edges(), dataclasses.replace(flow_edge, sql_query="changed")])

    assert ("put", "id-a", "id-b") in om.calls
    assert ("put", "id-b", "id-c") not in om.calls
    # every delete happens before the first write
    kinds = [c[0] for c in om.calls]
    assert kinds.index("put") > max(i for i, k in enumerate(kinds) if k == "delete")


def test_lineage_digest_ignores_edge_order():
    a, b = _edges()
    assert lineage_digest([a, b]) == lineage_digest([b, a])
    assert lineage_digest([a]) != lineage_digest([dataclasses.replace(a, pipeline_fqn=None)])


def test_om_client_delete_by_source_uses_a_known_id_without_lookup():
    om = OMClient("https://om.example.com", "tok")
    om.server_version = "1.13.4"
    with mock.patch.object(om, "_request") as request:
        om.delete_lineage_by_source("table", _t("a"), "PipelineLineage", entity_id="id-a")
    request.assert_called_once_with("DELETE", "/lineage/table/id-a/type/PipelineLineage")


# ---------------------------------------------------------------- parallel


def test_parallel_keeps_report_order_and_runs_concurrently():
    active = 0
    peak = 0
    lock = threading.Lock()

    def task(item, item_report):
        nonlocal active, peak
        with lock:
            active += 1
            peak = max(peak, active)
        time.sleep(0.02 * (5 - item))  # later items finish first
        item_report.record(project_id=PID, entity_type="Table", entity_fqn=str(item), action="created")
        with lock:
            active -= 1

    report = component_mod.RunReport(run_id="rid")
    config = SimpleNamespace(parallel_requests=4)
    component_mod.Component._parallel(config, list(range(5)), task, report)  # ty: ignore[invalid-argument-type]

    assert [r["entity_fqn"] for r in report.rows()] == ["0", "1", "2", "3", "4"]
    assert peak > 1


def test_parallel_reraises_the_first_error_and_keeps_finished_rows():
    def task(item, item_report):
        if item == 1:
            raise component_mod.UserException("fail fast")
        item_report.record(project_id=PID, entity_type="Table", entity_fqn=str(item), action="created")

    report = component_mod.RunReport(run_id="rid")
    with pytest.raises(component_mod.UserException, match="fail fast"):
        config = SimpleNamespace(parallel_requests=2)
        component_mod.Component._parallel(config, [0, 1], task, report)  # ty: ignore[invalid-argument-type]
    assert [r["entity_fqn"] for r in report.rows()] == ["0"]


def test_parallel_requests_setting_is_bounded():
    base = {"om_host": "https://om", "#bot_token": "x"}
    assert Configuration(**base).parallel_requests == 8
    assert Configuration(**base, advanced={"parallel_requests": 1}).parallel_requests == 1
    with pytest.raises(component_mod.UserException):
        Configuration(**base, parallel_requests=11)


# ------------------------------------------------------- review follow-ups


def test_nested_flow_rewrite_reaches_edges_carrying_the_inner_pipeline():
    """G gains nested flow F; F -> C pulls in C, whose delete drops A -> X (carries C)."""
    g, f, c = (f"{SVC}-{PROJ}.{n}" for n in ("g", "f", "c"))
    known = {**_known(), g: "id-g", f: "id-f", c: "id-c-pipe"}

    def pipe_edge(src, dst):
        return lineage_builder.LineageEdge(
            from_fqn=src, to_fqn=dst, source="PipelineLineage", from_type="pipeline", to_type="pipeline"
        )

    table_edge = lineage_builder.LineageEdge(from_fqn=_t("a"), to_fqn=_t("b"), source="PipelineLineage", pipeline_fqn=c)
    state = StateManager()
    _refresh(_LineageOM(known), state, [pipe_edge(f, c), table_edge])

    om = _LineageOM(known)
    _refresh(om, state, [pipe_edge(g, f), pipe_edge(f, c), table_edge])

    assert ("put", "id-a", "id-b") in om.calls


def test_failed_cleanup_is_retried_next_run():
    class FailingDeleteOM(_LineageOM):
        def delete_lineage_by_source(self, entity_type, fqn, source, entity_id=None):
            raise component_mod.OMClientError("500")

    state = StateManager()
    _refresh(FailingDeleteOM(_known()), state, _edges())
    om = _LineageOM(_known())
    _refresh(om, state, _edges())
    assert len(_puts(om)) == 2


def test_om_2_0_cleanup_deletes_by_name_without_id_lookup():
    class Om20(_LineageOM):
        is_2_0_or_newer = True

        def __init__(self, known_ids):
            super().__init__(known_ids)
            self.lookups = 0

        def get_by_fqn(self, kind, fqn, fields=None):
            self.lookups += 1
            return super().get_by_fqn(kind, fqn, fields)

        def delete_lineage_by_source(self, entity_type, fqn, source, entity_id=None):
            assert entity_id is None
            self.calls.append(("delete", entity_type, fqn, source))

    om = Om20(_known())
    om.is_2_0_or_newer = True
    _refresh(om, StateManager(), _edges(pipeline=None))
    assert om.lookups == 3  # the three edge endpoints for the PUTs, none for the cleanup


def test_failed_tombstone_makes_the_bucket_resync_next_run():
    class ListingFailsOM(_CatalogOM):
        def list_entities(self, kind, params=None, page_size=200):
            raise component_mod.OMClientError("500")

    reader = _CatalogReader()
    reader.buckets = reader.buckets[:1]
    state = StateManager({"run_count": 1})
    om = ListingFailsOM()
    run, report = _sync_catalog(reader, om, state, _catalog_config())
    assert state.bucket_digest(PID, "in.c-a") is not None

    _component()._tombstone_pass(_catalog_config(), om, run, report, state)  # ty: ignore[invalid-argument-type]

    assert state.bucket_digest(PID, "in.c-a") is None


def test_linked_bucket_tables_are_read_in_full():
    reader = _CatalogReader()
    reader.buckets = [SourceBucket(id="in.c-a", name="c-a", stage="in", path="in.c-a", source_bucket={"id": "x"})]
    _sync_catalog(reader, _CatalogOM(), StateManager({"run_count": 1}), _catalog_config())
    assert sorted(reader.detail_reads) == ["in.c-a.plain", "in.c-a.typed"]


def test_parallel_fail_fast_does_not_wait_for_earlier_slow_items():
    started: list[int] = []

    def task(item, item_report):
        started.append(item)
        if item == 0:
            time.sleep(0.5)
        if item == 1:
            raise component_mod.UserException("fail fast")

    config = SimpleNamespace(parallel_requests=2)
    t0 = time.perf_counter()
    with pytest.raises(component_mod.UserException):
        component_mod.Component._parallel(config, list(range(50)), task, component_mod.RunReport(run_id="r"))  # ty: ignore[invalid-argument-type]
    assert len(started) < 50  # the rest were cancelled, not run
    assert time.perf_counter() - t0 < 1.0


def test_om_client_retries_429():
    session = mock.Mock()
    busy = mock.Mock(status_code=429, text="slow down")
    ok = mock.Mock(status_code=200, text="{}")
    ok.json.return_value = {}
    session.request.side_effect = [busy, ok]
    om = OMClient("https://om.example.com", "tok", session=session, backoff_base=0)
    om.put_lineage({"edge": {}})
    assert session.request.call_count == 2


# ------------------------------------------------------- review round 2 (PR #6)


def test_lookup_error_fails_only_its_target_and_the_run_goes_on():
    class LookupFailsOM(_LineageOM):
        def get_by_fqn(self, kind, fqn, fields=None):
            if fqn == _t("a"):
                raise component_mod.OMClientError("500 after retries")
            return super().get_by_fqn(kind, fqn, fields)

    state = StateManager()
    om = LookupFailsOM(_known())
    report = _refresh(om, state, _edges(pipeline=None))

    assert ("put", "id-b", "id-c") in om.calls  # the sibling target is written
    digests = state.lineage_digests(PID)
    assert f"table:{_t('c')}" in digests
    assert f"table:{_t('b')}" not in digests  # retried next run
    failed = [r for r in report.rows() if r["action"] == report_mod.ACTION_FAILED]
    assert len(failed) == 1 and "entity lookup" in failed[0]["detail"]


def test_absent_endpoint_takes_a_digest_and_is_rewritten_once_it_appears():
    known = _known()
    del known[_t("a")]  # the input table is not in OpenMetadata
    state = StateManager()
    report = _refresh(_LineageOM(known), state, _edges(pipeline=None))
    assert {r["action"] for r in report.rows() if r["entity_fqn"].startswith(_t("a"))} == {report_mod.ACTION_UNRESOLVED}
    assert f"table:{_t('b')}" in state.lineage_digests(PID)
    assert state.lineage_unresolved(PID) == {f"table:{_t('b')}": [f"table:{_t('a')}"]}

    still_absent = _LineageOM(known)
    _refresh(still_absent, state, _edges(pipeline=None))
    assert _puts(still_absent) == []  # nothing changed: no delete, no write

    appeared = _LineageOM(_known())
    _refresh(appeared, state, _edges(pipeline=None))
    assert _puts(appeared) == [("put", "id-a", "id-b")]
    assert state.lineage_unresolved(PID) == {}


def test_alias_view_edge_is_written_after_its_source_bucket_is_created():
    class AliasReader(_CatalogReader):
        def __init__(self):
            super().__init__()
            # the alias bucket is listed (and synced) before its source bucket
            self.buckets = [
                SourceBucket(id="in.c-b2", name="c-b2", stage="in", path="in.c-b2"),
                SourceBucket(id="in.c-b1", name="c-b1", stage="in", path="in.c-b1"),
            ]

        def iter_tables(self, bucket_id):
            if bucket_id == "in.c-b2":
                return iter([self.get_table("in.c-b2.t")])
            return iter([SourceTable(id="in.c-b1.t", name="t", columns=[SourceColumn(name="id")])])

        def get_table(self, table_id):
            return SourceTable(
                id=table_id,
                name="t",
                columns=[SourceColumn(name="id")],
                is_alias=True,
                source_table={"id": "in.c-b1.t"},
            )

    class CreatingOM(_CatalogOM):
        def __init__(self):
            super().__init__()
            self.created: dict[str, str] = {}
            self.lineage: list[dict] = []

        def get_by_fqn(self, kind, fqn, fields=None):
            if kind == "tables":
                return {"id": self.created[fqn]} if fqn in self.created else None
            return super().get_by_fqn(kind, fqn, fields)

        def put_entity(self, kind, body):
            created = super().put_entity(kind, body)
            if kind == "tables":
                fqn = f"{body['databaseSchema']}.{body['name']}"
                created = {"id": f"id-{fqn}"}  # both tables are named "t": keep their ids apart
                self.created[fqn] = created["id"]
            return created

        def put_lineage(self, request):
            self.lineage.append(request)
            return {}

    om = CreatingOM()
    _sync_catalog(AliasReader(), om, StateManager({"run_count": 1}), _catalog_config(parallel_requests=1))
    assert len(om.lineage) == 1
    assert om.lineage[0]["edge"]["lineageDetails"]["source"] == "ViewLineage"
