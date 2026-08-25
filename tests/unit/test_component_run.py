"""Run-level unit coverage for the OpenMetadata catalog writer.

Kept under ``tests/unit`` (the done-bar runs ``pytest tests/unit``) and fully
self-contained (network clients faked, a temporary ``KBC_DATADIR``):

* Issue B — the tombstone pass is scope-safe: with a ``bucket_allowlist`` it
  reconciles ONLY within the enumerated buckets, never deleting entities of
  allowlist-excluded buckets, while still removing an in-scope table that no
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
from configuration import Configuration, FailureMode, MergeMode
from mapping import fqn as fqn_mod
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
        # which run() reads back (so om_version_override can override it).
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
    params = {**BASE_PARAMS, "service_name": SERVICE, "bucket_allowlist": ["out.c-keep"]}
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
    config = SimpleNamespace(full_refresh=False, stages=[], bucket_allowlist=[], bucket_denylist=[])

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


# ----------------------------------------- IMPORTANT 3: om_version_override drives the gate


def _config_with(**extra) -> Configuration:
    return Configuration(**{"om_host": "https://om.example.com", "#bot_token": "t", **extra})


def test_om_version_override_drives_2_0_gate():
    """With om_version_override set, the is_2_0_or_newer gate follows the override,
    not the probed /system/version value."""
    om = OMClient("https://om.example.com", "tok")
    om.server_version = "1.13.4"  # as if just probed
    assert om.is_2_0_or_newer is False

    component_mod.Component._apply_version_override(_config_with(om_version_override="2.1.0"), om)

    assert om.server_version == "2.1.0"
    assert om.is_2_0_or_newer is True  # the gate now follows the override


def test_om_version_override_absent_keeps_probe():
    om = OMClient("https://om.example.com", "tok")
    om.server_version = "1.13.4"  # as if just probed

    component_mod.Component._apply_version_override(_config_with(), om)

    assert om.server_version == "1.13.4"  # untouched
    assert om.is_2_0_or_newer is False
