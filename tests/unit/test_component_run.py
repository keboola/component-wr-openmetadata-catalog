"""Run-level unit coverage for the OpenMetadata catalog writer.

Kept under ``tests/unit`` (the done-bar runs ``pytest tests/unit``) and fully
self-contained (network clients faked, a temporary ``KBC_DATADIR``):

* Issue B — the tombstone pass is scope-safe: with a ``buckets`` selector it
  reconciles ONLY within the enumerated buckets, never deleting entities of
  selector-excluded buckets, while still removing an in-scope table that no
  longer exists in Keboola.
* Failure modes — ``collect_and_fail`` / ``fail_fast`` (both exit 1) and
  ``log_only`` (exit 0) each have unit coverage.
"""

import csv
import io
import json
from types import SimpleNamespace

import pytest
from keboola.component.exceptions import UserException

import component as component_mod
import report as report_mod
from client.om_client import OMClient, OMNotFound
from client.storage_reader import SourceBucket, SourceColumn, SourceTable
from component import ProjectContext, _ProjectRun
from configuration import FailureMode, MergeMode
from mapping import fqn as fqn_mod
from mapping import lineage_builder
from mapping.entity_builder import EntityBuilder
from mapping.pipeline_builder import PipelineBuilder
from merge import SnapshotStore, ThreeWayMerger
from sync import StateManager, bucket_digest

BASE_PARAMS = {
    "om_host": "https://om.example.com",
    "#bot_token": "jwt",
    "write_pipelines": False,
    "write_lineage": False,
    "write_column_lineage": False,
}


def _make_datadir(tmp_path, params, state=None):
    data = tmp_path / "data"
    (data / "in" / "tables").mkdir(parents=True)
    (data / "out" / "tables").mkdir(parents=True)
    (data / "config.json").write_text(json.dumps({"parameters": params}))
    if state is not None:
        (data / "in" / "state.json").write_text(json.dumps(state))
    return str(data)


@pytest.fixture
def _env(monkeypatch):
    monkeypatch.setenv("KBC_TOKEN", "storage-tok")
    monkeypatch.setenv("KBC_URL", "https://connection.keboola.com")
    monkeypatch.setenv("KBC_STACKID", "connection.keboola.com")
    monkeypatch.setenv("KBC_PROJECTNAME", "P")
    monkeypatch.delenv("KBC_CONFIGROWID", raising=False)
    monkeypatch.delenv("KBC_DATA_TYPE_SUPPORT", raising=False)


def _report_rows(tmp_path):
    text = (tmp_path / "data" / "out" / "tables" / "catalog_run_report.csv").read_text()
    return list(csv.DictReader(io.StringIO(text)))


# --------------------------------------------------------------------- fakes


class FakeOM:
    """1.13.4 OM whose table writes optionally fail (for the failure-mode tests)."""

    def __init__(self, *a, fail_tables=False, **k):
        self.is_2_0_or_newer = False
        self.fail_tables = fail_tables
        # Mirror the real client's contract: probe_version() records server_version,
        # which run() reads back to gate the >=2.0 write paths.
        self.server_version = "1.13.4"

    def probe_version(self):
        self.server_version = "1.13.4"
        return {"version": "1.13.4", "revision": "r", "timestamp": 1}

    def verify_auth(self):
        return {"name": "ingestion-bot"}

    def get_by_fqn(self, kind, fqn, fields=None):
        return None

    def put_entity(self, kind, body):
        if kind == "tables" and self.fail_tables:
            raise RuntimeError("boom")
        return {"id": f"id-{body.get('name')}"}

    def patch_entity(self, kind, fqn, patch):
        return {"id": "x"}

    def list_entities(self, kind, params=None, page_size=200):
        return iter([])

    def put_lineage(self, edge):
        return {}

    def delete_lineage_by_source(self, *a):
        return None

    def soft_delete(self, *a, **k):
        return None

    def put_pipeline_status(self, *a):
        return {}


class OneBucketStorage:
    def __init__(self, *a, **k):
        pass

    def verify_token(self):
        return {"owner": {"id": "777", "name": "P"}}

    def list_buckets(self):
        return [SourceBucket(id="out.c-sales", name="c-sales", stage="out", path="out.c-sales")]

    def iter_tables(self, bucket_id):
        return iter([SourceTable(id="out.c-sales.orders", name="orders", columns=[SourceColumn(name="id")])])

    def list_component_configs(self):
        return []

    def read_snapshot_rows(self, table_id, limit=1000000):
        return []


# --------------------------------------------------- Issue B: tombstone scope


SERVICE = "svc"
PROJECT = "P"


def _schema_fqn(bucket_path):
    return fqn_mod.schema_fqn(SERVICE, PROJECT, bucket_path)


def _table_fqn(bucket_path, table_name):
    return fqn_mod.table_fqn(SERVICE, PROJECT, bucket_path, table_name)


SCHEMA_KEEP = _schema_fqn("out.c-keep")
SCHEMA_OTHER = _schema_fqn("out.c-other")
KEEP_FQN = _table_fqn("out.c-keep", "orders")  # in Keboola AND OM -> untouched
REMOVED_FQN = _table_fqn("out.c-keep", "removed")  # in OM, gone from Keboola -> tombstoned
OTHER_FQN = _table_fqn("out.c-other", "b1")  # OM entity in an allowlist-EXCLUDED bucket -> must survive


class TwoBucketStorage(OneBucketStorage):
    def list_buckets(self):
        return [
            SourceBucket(id="out.c-keep", name="c-keep", stage="out", path="out.c-keep"),
            SourceBucket(id="out.c-other", name="c-other", stage="out", path="out.c-other"),
        ]

    def iter_tables(self, bucket_id):
        # "removed" is deliberately absent from out.c-keep this run; out.c-other is
        # never enumerated because the allowlist excludes it.
        if bucket_id == "out.c-keep":
            return iter([SourceTable(id="out.c-keep.orders", name="orders", columns=[SourceColumn(name="id")])])
        return iter([SourceTable(id="out.c-other.b1", name="b1", columns=[SourceColumn(name="id")])])


class TombstoneOM(FakeOM):
    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        self.listed_by_schema = {SCHEMA_KEEP: [KEEP_FQN, REMOVED_FQN], SCHEMA_OTHER: [OTHER_FQN]}
        self.om_entity_ids = {REMOVED_FQN: "id-removed", OTHER_FQN: "id-other"}
        self.deleted: list[str] = []
        self.schema_list_calls: list[str] = []

    def get_by_fqn(self, kind, fqn, fields=None):
        # The catalog pass reads tables WITH fields (create path -> None); the
        # tombstone pass resolves a stale fqn WITHOUT fields and needs its id.
        if fields is None and fqn in self.om_entity_ids:
            return {"id": self.om_entity_ids[fqn], "fullyQualifiedName": fqn}
        return None

    def list_entities(self, kind, params=None, page_size=200):
        params = params or {}
        if "databaseSchema" in params:
            schema = params["databaseSchema"]
            self.schema_list_calls.append(schema)
            return iter([{"fullyQualifiedName": f} for f in self.listed_by_schema.get(schema, [])])
        if "database" in params:
            # The pre-fix whole-database scope — asserted never to be taken.
            self.schema_list_calls.append("__database__")
            everything = [f for fqns in self.listed_by_schema.values() for f in fqns]
            return iter([{"fullyQualifiedName": f} for f in everything])
        return iter([])

    def soft_delete(self, kind, entity_id, **k):
        self.deleted.append(entity_id)


def test_tombstone_is_scope_safe_under_allowlist(tmp_path, monkeypatch, _env):
    params = {**BASE_PARAMS, "service_name": SERVICE, "buckets": ["out.c-keep"]}
    monkeypatch.setenv("KBC_DATADIR", _make_datadir(tmp_path, params))
    om = TombstoneOM()
    monkeypatch.setattr(component_mod, "OMClient", lambda *a, **k: om)
    monkeypatch.setattr(component_mod, "StorageReader", TwoBucketStorage)

    component_mod.Component().run()

    # Only the in-scope (allowlisted) schema was ever a deletion scope — never the
    # whole database, never the excluded bucket's schema.
    assert om.schema_list_calls == [SCHEMA_KEEP]
    assert "__database__" not in om.schema_list_calls
    # The stale in-scope table is removed; the excluded bucket's entity survives.
    assert om.deleted == ["id-removed"]
    assert "id-other" not in om.deleted

    tombstoned = [r for r in _report_rows(tmp_path) if r["action"] == "tombstoned"]
    assert [r["entity_fqn"] for r in tombstoned] == [REMOVED_FQN]


# --------------------------------------------------------- failure modes (D)


def test_collect_and_fail_raises_at_end(tmp_path, monkeypatch, _env):
    monkeypatch.setenv("KBC_DATADIR", _make_datadir(tmp_path, BASE_PARAMS))  # collect_and_fail is default
    monkeypatch.setattr(component_mod, "OMClient", lambda *a, **k: FakeOM(fail_tables=True))
    monkeypatch.setattr(component_mod, "StorageReader", OneBucketStorage)

    with pytest.raises(UserException) as exc:
        component_mod.Component().run()
    assert "failed" in str(exc.value).lower()
    # write_always -> the report survives the raise, with a failed row.
    assert any(r["action"] == "failed" for r in _report_rows(tmp_path))


def test_fail_fast_raises_immediately(tmp_path, monkeypatch, _env):
    params = {**BASE_PARAMS, "failure_mode": "fail_fast"}
    monkeypatch.setenv("KBC_DATADIR", _make_datadir(tmp_path, params))
    monkeypatch.setattr(component_mod, "OMClient", lambda *a, **k: FakeOM(fail_tables=True))
    monkeypatch.setattr(component_mod, "StorageReader", OneBucketStorage)

    with pytest.raises(UserException) as exc:
        component_mod.Component().run()
    # fail_fast raises at the failing entity, not the end-of-run tally.
    assert "Failed writing" in str(exc.value)


def test_log_only_does_not_raise(tmp_path, monkeypatch, _env):
    params = {**BASE_PARAMS, "failure_mode": "log_only"}
    monkeypatch.setenv("KBC_DATADIR", _make_datadir(tmp_path, params))
    monkeypatch.setattr(component_mod, "OMClient", lambda *a, **k: FakeOM(fail_tables=True))
    monkeypatch.setattr(component_mod, "StorageReader", OneBucketStorage)

    component_mod.Component().run()  # log_only -> exit 0
    assert any(r["action"] == "failed" for r in _report_rows(tmp_path))


# --------------------------------------- incremental digest-skip (tombstone-safe)


class _RecordingOM:
    """OM double that records every write; get returns None (create path)."""

    def __init__(self):
        self.put_calls: list[tuple[str, str | None]] = []
        self.patch_calls: list[tuple[str, str]] = []
        self.is_2_0_or_newer = False

    def get_by_fqn(self, kind, fqn, fields=None):
        return None

    def put_entity(self, kind, body):
        self.put_calls.append((kind, body.get("name")))
        return {"id": f"id-{body.get('name')}"}

    def patch_entity(self, kind, fqn, patch):
        self.patch_calls.append((kind, fqn))
        return {"id": "x"}


class _OneBucketReader:
    def __init__(self, bucket, tables):
        self._bucket = bucket
        self._tables = tables

    def list_buckets(self):
        return [self._bucket]

    def iter_tables(self, bucket_id):
        return iter(list(self._tables))


def test_incremental_skip_is_tombstone_safe_and_writes_nothing():
    """A bucket whose stored digest matches is skipped, but its tables must STILL be
    marked seen (so the per-schema tombstone pass never deletes an unchanged bucket's
    catalog), no OM schema/table write is issued, and the digest is left untouched
    (advance-after-success only persists after a real write). VCR case 06 exercises
    this end-to-end but does not assert the tombstone-safety wiring."""
    svc, proj, pid = "keboola-stack", "Proj", "4214"
    bucket = SourceBucket(id="in.c-x", name="c-x", stage="in", path="in.c-x")
    tables = [
        SourceTable(
            id="in.c-x.orders",
            name="orders",
            columns=[SourceColumn(name="id"), SourceColumn(name="amount")],
        )
    ]
    # State pre-seeded with the MATCHING digest -> should_process_bucket() is False.
    # run_count=1 so the periodic full-refresh cadence (run_count % 20 == 0) does not
    # force a reprocess this run.
    digest = bucket_digest(vars(bucket), [vars(t) for t in tables])
    state = StateManager({"projects": {pid: {"bucket_digests": {bucket.id: digest}}}, "run_count": 1})

    om = _RecordingOM()
    run = _ProjectRun(
        ctx=ProjectContext(project_id=pid, project_name=proj, storage_token="t", storage_url="https://s"),
        reader=_OneBucketReader(bucket, tables),  # ty: ignore[invalid-argument-type]  (duck-typed test double)
        entities=EntityBuilder(svc, proj, pid, "https://ui.example"),
        pipelines=PipelineBuilder(svc, proj, pid, "https://ui.example"),
    )
    report = component_mod.RunReport(run_id="rid")

    comp = component_mod.Component.__new__(component_mod.Component)  # bypass ComponentBase.__init__
    comp._config = SimpleNamespace(failure_mode=FailureMode.COLLECT_AND_FAIL)  # ty: ignore[invalid-assignment]
    config = SimpleNamespace(full_refresh=False, buckets=[])

    # config/om are duck-typed test doubles (SimpleNamespace / _RecordingOM).
    comp._catalog_pass(
        config,  # ty: ignore[invalid-argument-type]
        om,  # ty: ignore[invalid-argument-type]
        run,
        state,
        SnapshotStore(),
        report,
        ThreeWayMerger(MergeMode.THREE_WAY_MERGE),
        version_changed=False,
    )

    # 1) No OM write for the unchanged bucket's schema/tables (only the always-on
    #    DatabaseService + Database upserts precede the bucket loop).
    written_kinds = {kind for kind, _ in om.put_calls}
    assert written_kinds == {"databaseServices", "databases"}
    assert om.patch_calls == []

    # 2) The bucket is reported skipped_unchanged.
    schema_rows = [r for r in report.rows() if r["entity_type"] == "Schema"]
    assert schema_rows and all(r["action"] == report_mod.ACTION_SKIPPED_UNCHANGED for r in schema_rows)

    # 3) Tombstone-safety: the skipped bucket's tables are STILL seen + cataloged.
    assert len(run.seen_table_fqns) == 1
    (seen_fqn,) = run.seen_table_fqns
    assert run.column_catalog.get(seen_fqn) == {"id", "amount"}

    # 4) advance-after-success: the stored digest is untouched (no write happened).
    assert state.bucket_digest(pid, bucket.id) == digest


# ------------------------------------------ IMPORTANT 1: pipeline-status is best-effort


class _StatusJobReader:
    """Fake JobQueueReader that yields a summarizable run for the status push."""

    def __init__(self, *a, **k):
        pass

    def get_lineage_events(self, job_id):
        return [{"id": job_id}]

    @staticmethod
    def summarize_run(events):
        return SimpleNamespace(to_status_body=lambda: {"pipelineStatus": "Successful"})


def test_pipeline_status_404_is_recorded_not_fatal(monkeypatch):
    """put_pipeline_status on a non-existent FQN raises OMNotFound (an OMClientError,
    NOT a UserException). _push_pipeline_status must swallow it — record a failed row
    and continue — instead of letting it propagate to sys.exit(2)."""

    class _NotFoundOM(FakeOM):
        def put_pipeline_status(self, *a):
            raise OMNotFound("Not found: PUT /pipelines/svc.P.pipeline.cfg1/status")

    monkeypatch.setattr(component_mod, "JobQueueReader", _StatusJobReader)
    om = _NotFoundOM()
    run = _ProjectRun(
        ctx=ProjectContext(project_id="777", project_name="P", storage_token="t", storage_url="https://s"),
        reader=OneBucketStorage(),  # ty: ignore[invalid-argument-type]  (duck-typed test double)
        entities=EntityBuilder("svc", "P", "777", "https://ui"),
        pipelines=PipelineBuilder("svc", "P", "777", "https://ui"),
    )
    report = component_mod.RunReport(run_id="rid")
    cfg = {"id": "cfg1", "configuration": {"_lastJobId": "999"}}
    env = {"url": "https://connection.keboola.com"}

    comp = component_mod.Component.__new__(component_mod.Component)  # bypass ComponentBase.__init__

    # Pre-fix this call re-raises OMNotFound (only UserException was caught) -> exit 2.
    comp._push_pipeline_status(run, "svc.P.pipeline.cfg1", cfg, env, om, report)

    failed = [r for r in report.rows() if r["action"] == report_mod.ACTION_FAILED]
    assert len(failed) == 1
    assert failed[0]["entity_type"] == "Pipeline"
    assert failed[0]["entity_fqn"] == "svc.P.pipeline.cfg1"
    assert "pipeline status" in failed[0]["detail"]


# ----------------------------------- IMPORTANT 3: version is always the probed value


def test_server_version_always_follows_the_probe():
    """There is no override any more: ``server_version`` (and the >=2.0 gate) is
    always whatever ``probe_version()`` recorded from ``/system/version``."""
    om = OMClient("https://om.example.com", "tok")
    om.server_version = "1.13.4"  # as if just probed
    assert om.is_2_0_or_newer is False

    om.server_version = "2.1.0"  # as if a later probe recorded a newer version
    assert om.is_2_0_or_newer is True


# ------------------------------------- Phase A: pipeline-node lineage wiring


class _LineageRecordingOM:
    """OM double for ``_lineage_pass``/``_refresh_lineage`` coverage: resolves
    FQNs from a fixed id table and records every put/delete call."""

    def __init__(self, known_ids: dict[str, str]):
        self.known_ids = known_ids
        self.put_calls: list[dict] = []
        self.delete_calls: list[tuple[str, str, str]] = []

    def get_by_fqn(self, kind, fqn, fields=None):
        entity_id = self.known_ids.get(fqn)
        return {"id": entity_id} if entity_id else None

    def put_lineage(self, request):
        self.put_calls.append(request)

    def delete_lineage_by_source(self, entity_type, fqn, source):
        self.delete_calls.append((entity_type, fqn, source))


def test_lineage_pass_uses_pipeline_edges_when_pipeline_exists_else_declared_fallback():
    """Per the Phase A contract: a producing config WITH a pipeline_fqn (the
    normal path, write_pipelines on) gets pipeline-node edges (input->pipeline,
    pipeline->output); one WITHOUT a pipeline_fqn (write_pipelines off) falls
    back to the old table->table ``declared_edges``."""
    svc, proj, pid = "keboola-stack", "Acme_Project", "4214"

    class _TwoConfigReader:
        def list_component_configs(self):
            return [
                {
                    "id": "keboola.snowflake-transformation",
                    "configurations": [
                        {
                            "id": "cfg-with-pipeline",
                            "configuration": {
                                "storage": {
                                    "input": {"tables": [{"source": "in.c-main.a"}]},
                                    "output": {"tables": [{"destination": "out.c-res.x"}]},
                                }
                            },
                        }
                    ],
                },
                {
                    "id": "keboola.ex-generic",
                    "configurations": [
                        {
                            "id": "cfg-without-pipeline",
                            "configuration": {
                                "storage": {
                                    "input": {"tables": [{"source": "in.c-main.b"}]},
                                    "output": {"tables": [{"destination": "out.c-res.y"}]},
                                }
                            },
                        }
                    ],
                },
            ]

    run = _ProjectRun(
        ctx=ProjectContext(project_id=pid, project_name=proj, storage_token="t", storage_url="https://s"),
        reader=_TwoConfigReader(),  # ty: ignore[invalid-argument-type]  (duck-typed test double)
        entities=EntityBuilder(svc, proj, pid, "https://ui.example"),
        pipelines=PipelineBuilder(svc, proj, pid, "https://ui.example"),
    )
    pipeline_fqn_value = "keboola-stack.Acme_Project__cfg-with-pipeline"
    run.pipeline_fqn_by_config["cfg-with-pipeline"] = pipeline_fqn_value
    # "cfg-without-pipeline" is deliberately absent -> as if write_pipelines was off.

    known_ids = {
        fqn_mod.table_fqn_from_storage_id(svc, proj, "in.c-main.a"): "id-in-a",
        fqn_mod.table_fqn_from_storage_id(svc, proj, "out.c-res.x"): "id-out-x",
        fqn_mod.table_fqn_from_storage_id(svc, proj, "in.c-main.b"): "id-in-b",
        fqn_mod.table_fqn_from_storage_id(svc, proj, "out.c-res.y"): "id-out-y",
        pipeline_fqn_value: "id-pipeline",
    }
    om = _LineageRecordingOM(known_ids)
    report = component_mod.RunReport(run_id="rid")
    config = SimpleNamespace(write_lineage=True, write_column_lineage=False)

    comp = component_mod.Component.__new__(component_mod.Component)  # bypass ComponentBase.__init__
    comp._lineage_pass(config, om, run, report)  # ty: ignore[invalid-argument-type]

    edges_by_ids = {(r["edge"]["fromEntity"]["id"], r["edge"]["toEntity"]["id"]): r["edge"] for r in om.put_calls}
    assert len(om.put_calls) == 3  # 2 pipeline-node edges + 1 declared fallback edge

    # config WITH a pipeline -> input->pipeline and pipeline->output edges
    in_edge = edges_by_ids[("id-in-a", "id-pipeline")]
    assert in_edge["fromEntity"]["type"] == "table"
    assert in_edge["toEntity"]["type"] == "pipeline"
    out_edge = edges_by_ids[("id-pipeline", "id-out-x")]
    assert out_edge["fromEntity"]["type"] == "pipeline"
    assert out_edge["toEntity"]["type"] == "table"

    # config WITHOUT a pipeline -> the old table->table declared_edges fallback
    fallback_edge = edges_by_ids[("id-in-b", "id-out-y")]
    assert fallback_edge["fromEntity"]["type"] == "table"
    assert fallback_edge["toEntity"]["type"] == "table"


# --------------------------- Phase B: SQL-inferred input -> pipeline wiring


def _direct_sql_config(cfg_id: str, out_declared: dict, script: list[str], in_declared: list | None = None) -> dict:
    """A component-config fixture: declared storage + inline transformation SQL."""
    return {
        "id": cfg_id,
        "configuration": {
            "storage": {
                "input": {"tables": in_declared or []},
                "output": out_declared,
            },
            "parameters": {"blocks": [{"codes": [{"name": "code1", "script": script}]}]},
        },
    }


def test_lineage_pass_infers_input_pipeline_edge_from_direct_sql_when_declared_input_empty():
    """The tr-fact_pull_request upstream-fix scenario (Phase B): a config
    declares an OUTPUT mapping but an EMPTY ``storage.input.tables`` because
    its SQL reads the upstream table via a direct fully-qualified ref instead
    of a declared input. The column-lineage engine's ``known_table_fqns``
    gate recovers that ref as a real input, and ``_lineage_pass`` wires it to
    the pipeline the same way a declared input would be
    (``input_table -> pipeline``, reusing the Phase-A edge shape)."""
    svc, proj, pid = "keboola-stack", "Acme_Project", "4214"

    class _DirectSqlReader:
        def list_component_configs(self):
            return [
                {
                    "id": "keboola.snowflake-transformation",
                    "configurations": [
                        _direct_sql_config(
                            "cfg-direct-sql",
                            out_declared={"tables": [{"source": "result", "destination": "out.c-sales.result"}]},
                            script=['INSERT INTO "result" SELECT "id", "amount" FROM "PROJDB"."in.c-main"."orders"'],
                        )
                    ],
                }
            ]

    run = _ProjectRun(
        ctx=ProjectContext(project_id=pid, project_name=proj, storage_token="t", storage_url="https://s"),
        reader=_DirectSqlReader(),  # ty: ignore[invalid-argument-type]  (duck-typed test double)
        entities=EntityBuilder(svc, proj, pid, "https://ui.example"),
        pipelines=PipelineBuilder(svc, proj, pid, "https://ui.example"),
    )
    pipeline_fqn_value = "keboola-stack.Acme_Project__cfg-direct-sql"
    run.pipeline_fqn_by_config["cfg-direct-sql"] = pipeline_fqn_value

    in_fqn = fqn_mod.table_fqn_from_storage_id(svc, proj, "in.c-main.orders")
    out_fqn = fqn_mod.table_fqn_from_storage_id(svc, proj, "out.c-sales.result")
    run.column_catalog[in_fqn] = {"id", "amount"}
    run.column_catalog[out_fqn] = {"id", "amount"}

    known_ids = {in_fqn: "id-in-orders", out_fqn: "id-out-result", pipeline_fqn_value: "id-pipeline"}
    om = _LineageRecordingOM(known_ids)
    report = component_mod.RunReport(run_id="rid")
    config = SimpleNamespace(write_lineage=True, write_column_lineage=True)

    comp = component_mod.Component.__new__(component_mod.Component)  # bypass ComponentBase.__init__
    comp._lineage_pass(config, om, run, report)  # ty: ignore[invalid-argument-type]

    edges_by_ids = {(r["edge"]["fromEntity"]["id"], r["edge"]["toEntity"]["id"]): r["edge"] for r in om.put_calls}

    # the SQL-inferred input -> pipeline edge (storage.input.tables was declared empty)
    in_edge = edges_by_ids[("id-in-orders", "id-pipeline")]
    assert in_edge["fromEntity"]["type"] == "table"
    assert in_edge["toEntity"]["type"] == "pipeline"
    assert in_edge["lineageDetails"]["source"] == lineage_builder.SOURCE_PIPELINE

    # the declared output -> pipeline edge (Phase A, unaffected by Phase B)
    out_edge = edges_by_ids[("id-pipeline", "id-out-result")]
    assert out_edge["fromEntity"]["type"] == "pipeline"
    assert out_edge["toEntity"]["type"] == "table"


def test_lineage_pass_skips_sql_inference_when_declared_input_present():
    """Inference only fills the gap: when ``storage.input.tables`` is NOT
    empty, the declared mapping stays authoritative and no
    ``inferred_input -> pipeline`` edge is added — even for a table the SQL
    ALSO reads via a separate, otherwise-cataloged direct qualified ref."""
    svc, proj, pid = "keboola-stack", "Acme_Project", "4214"

    class _MixedReader:
        def list_component_configs(self):
            return [
                {
                    "id": "keboola.snowflake-transformation",
                    "configurations": [
                        _direct_sql_config(
                            "cfg-mixed",
                            out_declared={"tables": [{"source": "result", "destination": "out.c-sales.result"}]},
                            in_declared=[{"source": "in.c-main.declared", "destination": "declared"}],
                            script=[
                                'INSERT INTO "result" SELECT "id" FROM "declared"',
                                'INSERT INTO "result" SELECT "id" FROM "PROJDB"."in.c-main"."extra"',
                            ],
                        )
                    ],
                }
            ]

    run = _ProjectRun(
        ctx=ProjectContext(project_id=pid, project_name=proj, storage_token="t", storage_url="https://s"),
        reader=_MixedReader(),  # ty: ignore[invalid-argument-type]  (duck-typed test double)
        entities=EntityBuilder(svc, proj, pid, "https://ui.example"),
        pipelines=PipelineBuilder(svc, proj, pid, "https://ui.example"),
    )
    pipeline_fqn_value = "keboola-stack.Acme_Project__cfg-mixed"
    run.pipeline_fqn_by_config["cfg-mixed"] = pipeline_fqn_value

    declared_fqn = fqn_mod.table_fqn_from_storage_id(svc, proj, "in.c-main.declared")
    extra_fqn = fqn_mod.table_fqn_from_storage_id(svc, proj, "in.c-main.extra")
    out_fqn = fqn_mod.table_fqn_from_storage_id(svc, proj, "out.c-sales.result")
    run.column_catalog[declared_fqn] = {"id"}
    run.column_catalog[extra_fqn] = {"id"}
    run.column_catalog[out_fqn] = {"id"}

    known_ids = {
        declared_fqn: "id-in-declared",
        extra_fqn: "id-in-extra",
        out_fqn: "id-out-result",
        pipeline_fqn_value: "id-pipeline",
    }
    om = _LineageRecordingOM(known_ids)
    report = component_mod.RunReport(run_id="rid")
    config = SimpleNamespace(write_lineage=True, write_column_lineage=True)

    comp = component_mod.Component.__new__(component_mod.Component)  # bypass ComponentBase.__init__
    comp._lineage_pass(config, om, run, report)  # ty: ignore[invalid-argument-type]

    put_pairs = {(r["edge"]["fromEntity"]["id"], r["edge"]["toEntity"]["id"]) for r in om.put_calls}
    assert ("id-in-declared", "id-pipeline") in put_pairs  # Phase A: declared input, unaffected
    assert ("id-in-extra", "id-pipeline") not in put_pairs  # Phase B inference stays off: declared list non-empty


def test_refresh_lineage_deletes_stale_edges_keyed_on_target_type():
    """``_refresh_lineage``'s stale-edge cleanup must delete by each edge's
    TARGET type, not a hardcoded "table": an input->pipeline edge's target is
    the pipeline, a pipeline->output edge's target is the table."""
    svc, proj, pid = "keboola-stack", "Acme_Project", "4214"
    storage = {
        "input": {"tables": [{"source": "in.c-main.a"}]},
        "output": {"tables": [{"destination": "out.c-res.x"}]},
    }
    pipeline_fqn_value = "keboola-stack.Acme_Project__cfg1"
    edges = lineage_builder.pipeline_edges(storage, service_name=svc, project=proj, pipeline_fqn=pipeline_fqn_value)
    assert len(edges) == 2  # sanity: one input->pipeline, one pipeline->output edge

    in_table_fqn = fqn_mod.table_fqn_from_storage_id(svc, proj, "in.c-main.a")
    out_table_fqn = fqn_mod.table_fqn_from_storage_id(svc, proj, "out.c-res.x")
    known_ids = {in_table_fqn: "id-in-a", out_table_fqn: "id-out-x", pipeline_fqn_value: "id-pipeline"}
    om = _LineageRecordingOM(known_ids)
    run = _ProjectRun(
        ctx=ProjectContext(project_id=pid, project_name=proj, storage_token="t", storage_url="https://s"),
        reader=OneBucketStorage(),  # ty: ignore[invalid-argument-type]  (unused by _refresh_lineage)
        entities=EntityBuilder(svc, proj, pid, "https://ui.example"),
        pipelines=PipelineBuilder(svc, proj, pid, "https://ui.example"),
    )
    report = component_mod.RunReport(run_id="rid")
    comp = component_mod.Component.__new__(component_mod.Component)  # bypass ComponentBase.__init__

    comp._refresh_lineage(om, run, report, edges)

    delete_targets = {(entity_type, fqn) for entity_type, fqn, _source in om.delete_calls}
    assert ("pipeline", pipeline_fqn_value) in delete_targets
    assert ("table", out_table_fqn) in delete_targets
    # never the pre-fix hardcoded "table" for the pipeline target
    assert ("table", pipeline_fqn_value) not in delete_targets
