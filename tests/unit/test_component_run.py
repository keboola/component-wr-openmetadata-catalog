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
from configuration import FailureMode, MergeMode, ProjectScope
from mapping import entity_builder, lineage_builder
from mapping import fqn as fqn_mod
from mapping import pipeline_builder as pipeline_builder_mod
from mapping.dashboard_builder import DashboardBuilder
from mapping.entity_builder import EntityBuilder
from mapping.pipeline_builder import PipelineBuilder
from merge import SnapshotStore, ThreeWayMerger
from sync import StateManager, bucket_digest, digest_fields

BASE_PARAMS = {
    "om_host": "https://om.example.com",
    "#bot_token": "jwt",
    "write_transformations": False,
    "write_components": False,
    "write_flows": False,
    "write_data_apps": False,
    "write_table_lineage": False,
    "write_pipeline_lineage": False,
    "write_bucket_lineage": False,
    "write_dashboard_lineage": False,
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


def _storage_fqn(service_name: str, project: str, storage_id: str) -> str:
    """``table_fqn_from_storage_id`` narrowed to ``str`` for well-formed test fixture ids.

    The real function returns ``str | None`` for an unparseable storage id; every id
    used in this file is a valid ``stage.c-bucket.table`` fixture, so this asserts
    that instead of leaking ``None`` into a ``set[str]``/``dict[str, str]``.
    """
    fqn = fqn_mod.table_fqn_from_storage_id(service_name, project, storage_id)
    assert fqn is not None
    return fqn


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
    # force a reprocess this run. Seeded via digest_fields() (not raw vars()) so it
    # matches what production computes — vars() would also carry the provenance-only
    # created_by_metadata field, which digest_fields() deliberately excludes.
    digest = bucket_digest(digest_fields(bucket), [digest_fields(t) for t in tables])
    state = StateManager({"projects": {pid: {"bucket_digests": {bucket.id: digest}}}, "run_count": 1})

    om = _RecordingOM()
    run = _ProjectRun(
        ctx=ProjectContext(project_id=pid, project_name=proj, storage_token="t", storage_url="https://s"),
        reader=_OneBucketReader(bucket, tables),  # ty: ignore[invalid-argument-type]  (duck-typed test double)
        entities=EntityBuilder(svc, proj, pid, "https://ui.example"),
        pipelines=PipelineBuilder(svc, proj, pid, "https://ui.example"),
        dashboards=DashboardBuilder(svc, proj, pid, "https://ui.example"),
    )
    report = component_mod.RunReport(run_id="rid")

    comp = component_mod.Component.__new__(component_mod.Component)  # bypass ComponentBase.__init__
    comp._config = SimpleNamespace(failure_mode=FailureMode.COLLECT_AND_FAIL)  # ty: ignore[invalid-assignment]
    config = SimpleNamespace(full_refresh=False, buckets=[], write_buckets=True)

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


# ------------------------- custom properties (extension/owners) wiring, generalized
# from the Dashboard-only treatment to Tables, Pipelines, DatabaseSchemas, Databases.
#
# ``_upsert`` issues up to TWO ``get_by_fqn`` reads per entity (see
# ``component.py``'s ``_PRIMARY_FIELDS_BY_KIND`` / ``_with_extension_owners``):
# a PRIMARY existence-check whose ``fields=`` must stay byte-for-byte identical to
# what it was before this feature (an already-recorded VCR cassette's request
# matches on ``query`` too, so appending ``,extension,owners`` there would
# invalidate every prior table/schema/database interaction), plus a SEPARATE,
# best-effort second call for ``fields="extension,owners"`` -- but only when (a)
# the entity already exists (nothing to compare against on a create), and (b) the
# primary call didn't already carry "extension" (pipelines/dashboards do, so they
# never get a second call). The two test functions below cover each path: create
# (no second call, for any kind) and update (second call, for the three kinds
# whose primary fields lack "extension").


class _CustomPropsRecordingOM:
    """OM double recording ``ensure_custom_properties()`` calls and every
    ``get_by_fqn`` call's ``(kind, fields)`` -- everything the wiring tests below
    need to assert on, on top of the plain create-path recording ``_RecordingOM``
    already provides.

    ``existing=True`` simulates every entity already existing in OM (a placeholder
    ``{"id": "existing-<kind>"}`` returned from every ``get_by_fqn`` call), which
    flips ``_upsert``'s create/update decision to PATCH -- the precondition for
    ``_with_extension_owners``'s second call to fire at all. ``existing=False``
    (the default) simulates a from-scratch catalog: ``current is None`` on the
    primary call short-circuits that second call for every kind.
    """

    def __init__(self, *, fail_entity_type: str | None = None, existing: bool = False):
        self.put_calls: list[tuple[str, str | None]] = []
        self.patch_calls: list[tuple[str, str]] = []
        self.ensure_calls: list[tuple[str, list]] = []
        self.fields_calls: list[tuple[str, str | None]] = []
        self.is_2_0_or_newer = False
        self._fail_entity_type = fail_entity_type
        self._existing = existing

    def ensure_custom_properties(self, entity_type, specs):
        self.ensure_calls.append((entity_type, specs))
        if entity_type == self._fail_entity_type:
            raise RuntimeError("boom")
        return {name for name, *_ in specs}

    def get_by_fqn(self, kind, fqn, fields=None):
        self.fields_calls.append((kind, fields))
        return {"id": f"existing-{kind}"} if self._existing else None

    def put_entity(self, kind, body):
        self.put_calls.append((kind, body.get("name")))
        return {"id": f"id-{body.get('name')}"}

    def patch_entity(self, kind, fqn, patch):
        self.patch_calls.append((kind, fqn))
        return {"id": "x"}

    def find_user_id_by_email(self, email):
        return None  # owner resolution itself is covered elsewhere; not this test's concern

    def calls_by_kind(self) -> dict[str, list]:
        """Groups ``fields_calls`` by kind, preserving each kind's call order (so a
        test can assert e.g. tables' primary call then its second extension/owners
        call, in sequence)."""
        grouped: dict[str, list] = {}
        for kind, fields in self.fields_calls:
            grouped.setdefault(kind, []).append(fields)
        return grouped


class _CustomPropsReader:
    """Reader double feeding one bucket/table (catalog pass) and one producing
    component config (pipeline pass) — mirrors ``_OneBucketReader``/``OneBucketStorage``
    but also serves ``list_component_configs()``."""

    def __init__(self, bucket, tables, components):
        self._bucket = bucket
        self._tables = tables
        self._components = components

    def list_buckets(self):
        return [self._bucket]

    def iter_tables(self, bucket_id):
        return iter(list(self._tables))

    def list_component_configs(self):
        return list(self._components)


def _run_custom_props_passes(om, *, fail_entity_type: str | None = None):
    """Builds one bucket/table/component fixture and runs ``_catalog_pass`` +
    ``_pipeline_pass`` against it with ``om`` -- shared by both the create-path and
    update-path wiring tests below."""
    svc, proj, pid = "keboola-stack", "Proj", "4214"
    bucket = SourceBucket(id="in.c-x", name="c-x", stage="in", path="in.c-x")
    tables = [SourceTable(id="in.c-x.orders", name="orders", columns=[SourceColumn(name="id")])]
    component = {
        "id": "keboola.snowflake-transformation",
        "configurations": [
            {
                "id": "123",
                "name": "T",
                "currentVersion": {"creatorToken": {"description": "owner@keboola.com"}},
                "configuration": {
                    "storage": {"output": {"tables": [{"source": "out", "destination": "in.c-x.orders"}]}},
                    "parameters": {"blocks": []},
                },
            }
        ],
    }

    reader = _CustomPropsReader(bucket, tables, [component])
    run = _ProjectRun(
        ctx=ProjectContext(project_id=pid, project_name=proj, storage_token="t", storage_url="https://s"),
        reader=reader,  # ty: ignore[invalid-argument-type]  (duck-typed test double)
        entities=EntityBuilder(svc, proj, pid, "https://ui.example"),
        pipelines=PipelineBuilder(svc, proj, pid, "https://ui.example"),
        dashboards=DashboardBuilder(svc, proj, pid, "https://ui.example"),
    )
    report = component_mod.RunReport(run_id="rid")
    state = StateManager({"run_count": 1})

    comp = component_mod.Component.__new__(component_mod.Component)  # bypass ComponentBase.__init__
    comp._config = SimpleNamespace(failure_mode=FailureMode.COLLECT_AND_FAIL)  # ty: ignore[invalid-assignment]
    catalog_config = SimpleNamespace(full_refresh=True, buckets=[], write_buckets=True)
    pipeline_config = SimpleNamespace(
        scope=ProjectScope.THIS_PROJECT,
        write_flows=True,
        flows=[],
        write_transformations=True,
        transformations=[],
        write_components=True,
        components=[],
        write_pipeline_status=False,
    )

    # config/om are duck-typed test doubles (SimpleNamespace / _CustomPropsRecordingOM).
    comp._catalog_pass(
        catalog_config,  # ty: ignore[invalid-argument-type]
        om,
        run,
        state,
        SnapshotStore(),
        report,
        ThreeWayMerger(MergeMode.THREE_WAY_MERGE),
        version_changed=False,
    )
    comp._pipeline_pass(
        pipeline_config,  # ty: ignore[invalid-argument-type]
        om,
        run,
        SnapshotStore(),
        report,
        ThreeWayMerger(MergeMode.THREE_WAY_MERGE),
        env={"url": None},
    )
    return run, report


def test_ensure_custom_properties_and_fields_wiring_create():
    """CREATE path (nothing exists in OM yet): ``_catalog_pass`` ensures
    database/databaseSchema/table custom properties and ``_pipeline_pass`` ensures
    pipeline custom properties, each with its builder's own CUSTOM_PROPERTIES list.
    Every kind's PRIMARY ``get_by_fqn`` requests its own fields shape (tables:
    "columns,tableConstraints"; databaseSchemas/databases: none; pipelines already
    "extension,owners") and, because ``current`` comes back ``None`` (nothing to
    compare a create against), the second ``_with_extension_owners`` call never
    fires for ANY kind -- exactly one ``get_by_fqn`` call per kind. A failing
    ``ensure_custom_properties`` for one entity type (best-effort, like the
    existing Dashboard precedent) must not raise out of either pass — the run
    keeps writing."""
    # "database" deliberately fails -> proves the ensure-custom-properties call is
    # best-effort, same precedent as the existing Dashboard pass.
    om = _CustomPropsRecordingOM(fail_entity_type="database")
    _, report = _run_custom_props_passes(om)

    ensure_by_type = dict(om.ensure_calls)
    assert set(ensure_by_type) == {"database", "databaseSchema", "table", "pipeline"}
    assert {n for n, *_ in ensure_by_type["database"]} == {n for n, *_ in entity_builder.DATABASE_CUSTOM_PROPERTIES}
    assert {n for n, *_ in ensure_by_type["databaseSchema"]} == {n for n, *_ in entity_builder.SCHEMA_CUSTOM_PROPERTIES}
    assert {n for n, *_ in ensure_by_type["table"]} == {n for n, *_ in entity_builder.TABLE_CUSTOM_PROPERTIES}
    assert {n for n, *_ in ensure_by_type["pipeline"]} == {n for n, *_ in pipeline_builder_mod.CUSTOM_PROPERTIES}

    calls = om.calls_by_kind()
    assert calls["tables"] == ["columns,tableConstraints"]
    assert calls["databaseSchemas"] == [None]
    assert calls["databases"] == [None]
    assert calls["pipelines"] == ["extension,owners"]

    # every entity was a fresh create (never a patch) against this bare OM double.
    assert om.patch_calls == []
    created_kinds = {kind for kind, _ in om.put_calls}
    assert {"databases", "databaseSchemas", "tables", "pipelines"} <= created_kinds

    # best-effort: the "database" ensure_custom_properties raised, but neither pass
    # raised out — the run kept going and recorded rows for the table and pipeline.
    assert any(r["entity_type"] == "Table" for r in report.rows())
    assert any(r["entity_type"] == "Pipeline" for r in report.rows())


def test_upsert_second_call_fetches_extension_owners_on_update():
    """UPDATE path (every entity already exists in OM): for kinds whose PRIMARY
    fields shape does NOT already include "extension"/"owners" (tables,
    databaseSchemas, databases), ``_upsert`` issues a SEPARATE, best-effort
    ``get_by_fqn(kind, fqn, fields="extension,owners")`` call right after the
    primary existence check -- so the merge can compare against OM's current
    extension/owners without ever changing the primary call's (already-recorded-
    by-VCR) query string. Pipelines already carry "extension,owners" on their
    PRIMARY call, so no second call fires for them."""
    om = _CustomPropsRecordingOM(existing=True)
    _run_custom_props_passes(om)

    calls = om.calls_by_kind()
    assert calls["tables"] == ["columns,tableConstraints", "extension,owners"]
    assert calls["databaseSchemas"] == [None, "extension,owners"]
    assert calls["databases"] == [None, "extension,owners"]
    assert calls["pipelines"] == ["extension,owners"]

    # current is not None for every kind above -> every write went through the
    # merge/PATCH path, never treated as a fresh create.
    assert om.put_calls == []
    patched_kinds = {kind for kind, _ in om.patch_calls}
    assert {"tables", "databaseSchemas", "databases", "pipelines"} <= patched_kinds


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
        dashboards=DashboardBuilder("svc", "P", "777", "https://ui"),
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


# ----------------------------------- data-app (Dashboard) upstream table lineage


class _DataAppReader:
    """Reader that returns one data-app config with a table input mapping."""

    def __init__(self, config_id, source_table):
        self._config_id = config_id
        self._source = source_table

    def list_component_configs(self):
        return [
            {
                "id": "keboola.data-apps",
                "configurations": [
                    {
                        "id": self._config_id,
                        "configuration": {"storage": {"input": {"tables": [{"source": self._source}]}}},
                    }
                ],
            }
        ]


class _LineageOM:
    """OM double recording lineage puts + source-scoped deletes; resolves known FQNs."""

    def __init__(self, ids):
        self._ids = ids
        self.is_2_0_or_newer = False
        self.put_edges: list[dict] = []
        self.deletes: list[tuple[str, str, str]] = []

    def get_by_fqn(self, kind, fqn, fields=None):
        return {"id": self._ids[fqn]} if fqn in self._ids else None

    def put_lineage(self, edge):
        self.put_edges.append(edge)
        return {}

    def delete_lineage_by_source(self, entity_type, fqn, source):
        self.deletes.append((entity_type, fqn, source))


def test_data_app_lineage_pass_emits_table_to_dashboard_edge():
    """The lineage pass turns a data app's input mapping into an upstream
    table -> Dashboard DashboardLineage edge, and its stale-edge cleanup targets
    the Dashboard with the DashboardLineage source (not a table source)."""
    svc, proj, pid = "keboola-stack", "P", "4214"
    config_id = "01app"
    source_table = "in.c-main.a"
    source_fqn = fqn_mod.table_fqn_from_storage_id(svc, proj, source_table)
    dashboard_fqn = fqn_mod.dashboard_fqn(svc, proj, config_id)

    om = _LineageOM({source_fqn: "id-src", dashboard_fqn: "id-dash"})
    run = _ProjectRun(
        ctx=ProjectContext(project_id=pid, project_name=proj, storage_token="t", storage_url="https://s"),
        reader=_DataAppReader(config_id, source_table),  # ty: ignore[invalid-argument-type]  (duck-typed double)
        entities=EntityBuilder(svc, proj, pid, "https://ui"),
        pipelines=PipelineBuilder(svc, proj, pid, "https://ui"),
        dashboards=DashboardBuilder(svc, proj, pid, "https://ui", "connection.keboola.com"),
    )
    run.dashboard_fqn_by_config[config_id] = dashboard_fqn  # the dashboard pass ran first
    report = component_mod.RunReport(run_id="rid")
    config = SimpleNamespace(
        write_dashboard_lineage=True,
        write_data_apps=True,
        write_column_lineage=False,
        write_bucket_lineage=False,
    )

    comp = component_mod.Component.__new__(component_mod.Component)  # bypass ComponentBase.__init__
    comp._lineage_pass(config, om, run, report)  # ty: ignore[invalid-argument-type]

    assert len(om.put_edges) == 1
    edge = om.put_edges[0]["edge"]
    assert edge["fromEntity"] == {"id": "id-src", "type": "table"}
    assert edge["toEntity"] == {"id": "id-dash", "type": "dashboard"}
    assert edge["lineageDetails"]["source"] == "DashboardLineage"
    # cleanup dropped our DashboardLineage on the dashboard target before re-adding
    assert ("dashboard", dashboard_fqn, "DashboardLineage") in om.deletes

    lineage_rows = [r for r in report.rows() if r["entity_type"] == "Lineage"]
    assert lineage_rows and lineage_rows[0]["action"] == report_mod.ACTION_UPDATED


def test_data_app_lineage_skipped_when_write_dashboard_lineage_off():
    """With write_dashboard_lineage off the data-app branch emits nothing, even
    though the pass is entered for column lineage."""
    svc, proj, pid = "keboola-stack", "P", "4214"
    om = _LineageOM({})
    run = _ProjectRun(
        ctx=ProjectContext(project_id=pid, project_name=proj, storage_token="t", storage_url="https://s"),
        reader=_DataAppReader("01app", "in.c-main.a"),  # ty: ignore[invalid-argument-type]  (duck-typed double)
        entities=EntityBuilder(svc, proj, pid, "https://ui"),
        pipelines=PipelineBuilder(svc, proj, pid, "https://ui"),
        dashboards=DashboardBuilder(svc, proj, pid, "https://ui", "connection.keboola.com"),
    )
    run.dashboard_fqn_by_config["01app"] = fqn_mod.dashboard_fqn(svc, proj, "01app")
    report = component_mod.RunReport(run_id="rid")
    config = SimpleNamespace(
        write_dashboard_lineage=False,
        write_data_apps=True,
        write_column_lineage=True,
        write_bucket_lineage=False,
    )

    comp = component_mod.Component.__new__(component_mod.Component)
    comp._lineage_pass(config, om, run, report)  # ty: ignore[invalid-argument-type]

    assert om.put_edges == []


# ------------------------------------------ native owner resolution (best-effort, cached)


def test_resolve_owner_caches_lookup_per_email():
    class _UserOM:
        def __init__(self):
            self.calls = 0

        def find_user_id_by_email(self, email):
            self.calls += 1
            return "om-user" if email == "known@keboola.com" else None

    om = _UserOM()
    run = _ProjectRun(
        ctx=ProjectContext(project_id="1", project_name="P", storage_token="t", storage_url="https://s"),
        reader=OneBucketStorage(),  # ty: ignore[invalid-argument-type]  (duck-typed double)
        entities=EntityBuilder("svc", "P", "1", "https://ui"),
        pipelines=PipelineBuilder("svc", "P", "1", "https://ui"),
        dashboards=DashboardBuilder("svc", "P", "1", "https://ui"),
    )
    comp = component_mod.Component.__new__(component_mod.Component)

    assert comp._resolve_owner(om, run, "known@keboola.com") == "om-user"  # ty: ignore[invalid-argument-type]
    assert comp._resolve_owner(om, run, "known@keboola.com") == "om-user"  # ty: ignore[invalid-argument-type]  (cached)
    assert comp._resolve_owner(om, run, None) is None  # ty: ignore[invalid-argument-type]  (empty e-mail short-circuits)
    assert om.calls == 1  # one lookup for the one distinct e-mail; the empty one never hit OM


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
        dashboards=DashboardBuilder(svc, proj, pid, "https://ui.example"),
    )
    pipeline_fqn_value = "keboola-stack.Acme_Project__cfg-with-pipeline"
    run.pipeline_fqn_by_config["cfg-with-pipeline"] = pipeline_fqn_value
    # "cfg-without-pipeline" is deliberately absent -> as if write_pipelines was off.

    known_ids = {
        _storage_fqn(svc, proj, "in.c-main.a"): "id-in-a",
        _storage_fqn(svc, proj, "out.c-res.x"): "id-out-x",
        _storage_fqn(svc, proj, "in.c-main.b"): "id-in-b",
        _storage_fqn(svc, proj, "out.c-res.y"): "id-out-y",
        pipeline_fqn_value: "id-pipeline",
    }
    om = _LineageRecordingOM(known_ids)
    report = component_mod.RunReport(run_id="rid")
    config = SimpleNamespace(
        write_table_lineage=True,
        write_pipeline_lineage=True,
        write_column_lineage=False,
        write_bucket_lineage=False,  # bucket-lineage aggregation is covered by its own dedicated tests
    )

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
        dashboards=DashboardBuilder(svc, proj, pid, "https://ui.example"),
    )
    pipeline_fqn_value = "keboola-stack.Acme_Project__cfg-direct-sql"
    run.pipeline_fqn_by_config["cfg-direct-sql"] = pipeline_fqn_value

    in_fqn = _storage_fqn(svc, proj, "in.c-main.orders")
    out_fqn = _storage_fqn(svc, proj, "out.c-sales.result")
    run.column_catalog[in_fqn] = {"id", "amount"}
    run.column_catalog[out_fqn] = {"id", "amount"}

    known_ids = {in_fqn: "id-in-orders", out_fqn: "id-out-result", pipeline_fqn_value: "id-pipeline"}
    om = _LineageRecordingOM(known_ids)
    report = component_mod.RunReport(run_id="rid")
    config = SimpleNamespace(
        write_table_lineage=True,
        write_pipeline_lineage=True,
        write_column_lineage=True,
        write_bucket_lineage=False,  # bucket-lineage aggregation is covered by its own dedicated tests
    )

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
        dashboards=DashboardBuilder(svc, proj, pid, "https://ui.example"),
    )
    pipeline_fqn_value = "keboola-stack.Acme_Project__cfg-mixed"
    run.pipeline_fqn_by_config["cfg-mixed"] = pipeline_fqn_value

    declared_fqn = _storage_fqn(svc, proj, "in.c-main.declared")
    extra_fqn = _storage_fqn(svc, proj, "in.c-main.extra")
    out_fqn = _storage_fqn(svc, proj, "out.c-sales.result")
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
    config = SimpleNamespace(
        write_table_lineage=True,
        write_pipeline_lineage=True,
        write_column_lineage=True,
        write_bucket_lineage=False,  # bucket-lineage aggregation is covered by its own dedicated tests
    )

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

    in_table_fqn = _storage_fqn(svc, proj, "in.c-main.a")
    out_table_fqn = _storage_fqn(svc, proj, "out.c-res.x")
    known_ids = {in_table_fqn: "id-in-a", out_table_fqn: "id-out-x", pipeline_fqn_value: "id-pipeline"}
    om = _LineageRecordingOM(known_ids)
    run = _ProjectRun(
        ctx=ProjectContext(project_id=pid, project_name=proj, storage_token="t", storage_url="https://s"),
        reader=OneBucketStorage(),  # ty: ignore[invalid-argument-type]  (unused by _refresh_lineage)
        entities=EntityBuilder(svc, proj, pid, "https://ui.example"),
        pipelines=PipelineBuilder(svc, proj, pid, "https://ui.example"),
        dashboards=DashboardBuilder(svc, proj, pid, "https://ui.example"),
    )
    report = component_mod.RunReport(run_id="rid")
    comp = component_mod.Component.__new__(component_mod.Component)  # bypass ComponentBase.__init__

    comp._refresh_lineage(om, run, report, edges)

    delete_targets = {(entity_type, fqn) for entity_type, fqn, _source in om.delete_calls}
    assert ("pipeline", pipeline_fqn_value) in delete_targets
    assert ("table", out_table_fqn) in delete_targets
    # never the pre-fix hardcoded "table" for the pipeline target
    assert ("table", pipeline_fqn_value) not in delete_targets


# ------------------------------- object-family split: sync actions + pass gating


class _FamilyReader:
    """One producing transformation, one producing extractor, one flow, one data app."""

    def list_component_configs(self):
        return [
            {
                "id": "keboola.snowflake-transformation",
                "type": "transformation",
                "configurations": [
                    {
                        "id": "t1",
                        "name": "Transform One",
                        "configuration": {
                            "storage": {"output": {"tables": [{"source": "o", "destination": "out.c-x.t"}]}}
                        },
                    }
                ],
            },
            {
                "id": "keboola.ex-db-mysql",
                "type": "extractor",
                "configurations": [
                    {
                        "id": "e1",
                        "name": "Extract One",
                        "configuration": {
                            "storage": {"output": {"tables": [{"source": "o", "destination": "out.c-x.e"}]}}
                        },
                    }
                ],
            },
            {
                "id": "keboola.orchestrator",
                "configurations": [{"id": "f1", "name": "Flow One", "configuration": {"phases": [], "tasks": []}}],
            },
            {
                "id": "keboola.data-apps",
                "configurations": [{"id": "a1", "name": "App One", "configuration": {}}],
            },
        ]


def test_sync_actions_partition_transformations_components_data_apps(tmp_path, monkeypatch, _env):
    """listTransformations/listComponents/listDataApps each return exactly their
    own family — a flow and the "other" family's config never leak across."""
    monkeypatch.setenv("KBC_DATADIR", _make_datadir(tmp_path, BASE_PARAMS))
    monkeypatch.setattr(component_mod, "StorageReader", lambda *a, **k: _FamilyReader())
    comp = component_mod.Component()

    transformations = comp.list_transformations()
    components = comp.list_components()
    data_apps = comp.list_data_apps()

    assert [e.value for e in transformations] == ["t1"]
    assert transformations[0].label == "keboola.snowflake-transformation / Transform One"
    assert [e.value for e in components] == ["e1"]
    assert components[0].label == "keboola.ex-db-mysql / Extract One"
    assert [e.value for e in data_apps] == ["a1"]
    assert data_apps[0].label == "App One"  # listDataApps: label=name, no component-id prefix


# --------------------------------------------------- pipeline-pass family gating + kbcType


class _PipelineRecordingOM(FakeOM):
    """Records every upserted Pipeline body (not just kind/name), for asserting
    kbcType and for counting how many pipelines a gated/filtered pass wrote."""

    def __init__(self):
        super().__init__()
        self.pipeline_bodies: dict[str, dict] = {}

    def ensure_custom_properties(self, entity_type, specs):
        return {name for name, *_ in specs}

    def put_entity(self, kind, body):
        if kind == "pipelines":
            self.pipeline_bodies[body["name"]] = body
        return super().put_entity(kind, body)


def _family_gating_run():
    svc, proj, pid = "keboola-stack", "Proj", "4214"
    run = _ProjectRun(
        ctx=ProjectContext(project_id=pid, project_name=proj, storage_token="t", storage_url="https://s"),
        reader=_FamilyReader(),  # ty: ignore[invalid-argument-type]  (duck-typed test double)
        entities=EntityBuilder(svc, proj, pid, "https://ui.example"),
        pipelines=PipelineBuilder(svc, proj, pid, "https://ui.example"),
        dashboards=DashboardBuilder(svc, proj, pid, "https://ui.example"),
    )
    return run


def _run_pipeline_pass(config_overrides: dict) -> tuple[_PipelineRecordingOM, component_mod.RunReport]:
    om = _PipelineRecordingOM()
    run = _family_gating_run()
    report = component_mod.RunReport(run_id="rid")
    base = {
        "scope": ProjectScope.THIS_PROJECT,
        "write_transformations": True,
        "transformations": [],
        "write_components": True,
        "components": [],
        "write_flows": True,
        "flows": [],
        "write_pipeline_status": False,
    }
    config = SimpleNamespace(**{**base, **config_overrides})
    comp = component_mod.Component.__new__(component_mod.Component)
    comp._pipeline_pass(
        config,  # ty: ignore[invalid-argument-type]  (duck-typed test double)
        om,  # ty: ignore[invalid-argument-type]  (duck-typed test double)
        run,
        SnapshotStore(),
        report,
        ThreeWayMerger(MergeMode.THREE_WAY_MERGE),
        env={"url": None},
    )
    return om, report


def test_pipeline_pass_family_off_excludes_that_kind():
    # Transformations off -> "t1" never written; the extractor and the flow still are.
    om, _ = _run_pipeline_pass({"write_transformations": False})
    written_config_ids = {body["extension"]["kbcConfigId"] for body in om.pipeline_bodies.values()}
    assert "t1" not in written_config_ids
    assert {"e1", "f1"} <= written_config_ids


def test_pipeline_pass_components_off_excludes_that_kind():
    om, _ = _run_pipeline_pass({"write_components": False})
    written_config_ids = {body["extension"]["kbcConfigId"] for body in om.pipeline_bodies.values()}
    assert "e1" not in written_config_ids
    assert {"t1", "f1"} <= written_config_ids


def test_pipeline_pass_flows_off_excludes_that_kind():
    om, _ = _run_pipeline_pass({"write_flows": False})
    written_config_ids = {body["extension"]["kbcConfigId"] for body in om.pipeline_bodies.values()}
    assert "f1" not in written_config_ids
    assert {"t1", "e1"} <= written_config_ids


def test_pipeline_pass_selector_narrows_to_subset():
    # A non-empty `transformations` selector is an allowlist within the family.
    om, _ = _run_pipeline_pass({"transformations": ["t1"]})
    written_config_ids = {body["extension"]["kbcConfigId"] for body in om.pipeline_bodies.values()}
    assert "t1" in written_config_ids

    om_excluded, _ = _run_pipeline_pass({"transformations": ["some-other-id"]})
    written_excluded = {body["extension"]["kbcConfigId"] for body in om_excluded.pipeline_bodies.values()}
    assert "t1" not in written_excluded
    assert {"e1", "f1"} <= written_excluded  # the other families are unaffected by the transformations selector


def test_pipeline_pass_kbc_type_reflects_component_kind():
    om, _ = _run_pipeline_pass({})
    by_config_id = {body["extension"]["kbcConfigId"]: body for body in om.pipeline_bodies.values()}
    assert by_config_id["t1"]["extension"]["kbcType"] == "transformation"
    assert by_config_id["e1"]["extension"]["kbcType"] == "extractor"
    assert by_config_id["f1"]["extension"]["kbcType"] == "orchestration"


# --------------------------------------------------- dashboard-pass data_apps selector


def test_dashboard_pass_data_apps_selector_narrows_to_subset():
    svc, proj, pid = "keboola-stack", "Proj", "4214"
    om = FakeOM()
    run = _ProjectRun(
        ctx=ProjectContext(project_id=pid, project_name=proj, storage_token="t", storage_url="https://s"),
        reader=_DataAppReader("01app", "in.c-main.a"),  # ty: ignore[invalid-argument-type]  (duck-typed double)
        entities=EntityBuilder(svc, proj, pid, "https://ui"),
        pipelines=PipelineBuilder(svc, proj, pid, "https://ui"),
        dashboards=DashboardBuilder(svc, proj, pid, "https://ui", "connection.keboola.com"),
    )
    report = component_mod.RunReport(run_id="rid")
    config = SimpleNamespace(scope=ProjectScope.THIS_PROJECT, data_apps=["some-other-app"])

    comp = component_mod.Component.__new__(component_mod.Component)
    comp._dashboard_pass(config, om, run, SnapshotStore(), report, ThreeWayMerger(MergeMode.THREE_WAY_MERGE))  # ty: ignore[invalid-argument-type]

    dashboard_rows = [r for r in report.rows() if r["entity_type"] == "Dashboard"]
    assert dashboard_rows == []  # "01app" excluded by the selector -> never written


# --------------------------------------------------- flow -> child-pipeline lineage


class _FlowChildReader:
    """A flow orchestrating one child config (whose own Pipeline was built this
    run, per `run.pipeline_fqn_by_config`) plus one task referencing a config
    that never got a pipeline (selector-excluded / non-producing)."""

    def list_component_configs(self):
        return [
            {
                "id": "keboola.orchestrator",
                "configurations": [
                    {
                        "id": "flow1",
                        "configuration": {
                            "phases": [],
                            "tasks": [
                                {
                                    "id": "task1",
                                    "name": "run child",
                                    "phase": 1,
                                    "task": {"componentId": "keboola.snowflake-transformation", "configId": "child1"},
                                },
                                {
                                    "id": "task2",
                                    "name": "run missing child",
                                    "phase": 1,
                                    "task": {"componentId": "keboola.ex-db-mysql", "configId": "missing-child"},
                                },
                            ],
                        },
                    }
                ],
            }
        ]


def test_flow_child_lineage_emits_pipeline_to_pipeline_edge_when_gated_on():
    svc, proj, pid = "keboola-stack", "Acme_Project", "4214"
    run = _ProjectRun(
        ctx=ProjectContext(project_id=pid, project_name=proj, storage_token="t", storage_url="https://s"),
        reader=_FlowChildReader(),  # ty: ignore[invalid-argument-type]  (duck-typed test double)
        entities=EntityBuilder(svc, proj, pid, "https://ui.example"),
        pipelines=PipelineBuilder(svc, proj, pid, "https://ui.example"),
        dashboards=DashboardBuilder(svc, proj, pid, "https://ui.example"),
    )
    flow_fqn = "keboola-stack.Acme_Project__flow1"
    child_fqn = "keboola-stack.Acme_Project__child1"
    run.pipeline_fqn_by_config["flow1"] = flow_fqn
    run.pipeline_fqn_by_config["child1"] = child_fqn
    # "missing-child" deliberately absent -> its task must be skipped, not raise.

    known_ids = {flow_fqn: "id-flow", child_fqn: "id-child"}
    om = _LineageRecordingOM(known_ids)
    report = component_mod.RunReport(run_id="rid")
    config = SimpleNamespace(
        write_pipeline_lineage=True,
        write_table_lineage=False,
        write_column_lineage=False,
        write_bucket_lineage=False,
    )

    comp = component_mod.Component.__new__(component_mod.Component)
    comp._lineage_pass(config, om, run, report)  # ty: ignore[invalid-argument-type]

    assert len(om.put_calls) == 1
    edge = om.put_calls[0]["edge"]
    assert edge["fromEntity"] == {"id": "id-flow", "type": "pipeline"}
    assert edge["toEntity"] == {"id": "id-child", "type": "pipeline"}
    assert edge["lineageDetails"]["source"] == lineage_builder.SOURCE_PIPELINE


def test_flow_child_lineage_absent_when_pipeline_lineage_off():
    svc, proj, pid = "keboola-stack", "Acme_Project", "4214"
    run = _ProjectRun(
        ctx=ProjectContext(project_id=pid, project_name=proj, storage_token="t", storage_url="https://s"),
        reader=_FlowChildReader(),  # ty: ignore[invalid-argument-type]  (duck-typed test double)
        entities=EntityBuilder(svc, proj, pid, "https://ui.example"),
        pipelines=PipelineBuilder(svc, proj, pid, "https://ui.example"),
        dashboards=DashboardBuilder(svc, proj, pid, "https://ui.example"),
    )
    run.pipeline_fqn_by_config["flow1"] = "keboola-stack.Acme_Project__flow1"
    run.pipeline_fqn_by_config["child1"] = "keboola-stack.Acme_Project__child1"

    om = _LineageRecordingOM({})
    report = component_mod.RunReport(run_id="rid")
    config = SimpleNamespace(
        write_pipeline_lineage=False,
        write_table_lineage=False,
        write_column_lineage=False,
        write_bucket_lineage=False,
    )

    comp = component_mod.Component.__new__(component_mod.Component)
    comp._lineage_pass(config, om, run, report)  # ty: ignore[invalid-argument-type]

    assert om.put_calls == []


# --------------------------------------------------- bucket-lineage aggregation (integration)


def test_lineage_pass_aggregates_bucket_edges_when_gated_on():
    """Two producing configs (no pipeline built for either -> plain table->table
    fallback under write_table_lineage) whose endpoints live in different
    buckets: with write_bucket_lineage on, a databaseSchema->databaseSchema
    edge is ALSO emitted, deduped across both table pairs sharing the same
    bucket pair."""
    svc, proj, pid = "keboola-stack", "Acme_Project", "4214"

    class _TwoConfigReader:
        def list_component_configs(self):
            return [
                {
                    "id": "keboola.ex-generic",
                    "configurations": [
                        {
                            "id": "cfg1",
                            "configuration": {
                                "storage": {
                                    "input": {"tables": [{"source": "in.c-main.a"}]},
                                    "output": {"tables": [{"destination": "out.c-res.x"}]},
                                }
                            },
                        }
                    ],
                }
            ]

    run = _ProjectRun(
        ctx=ProjectContext(project_id=pid, project_name=proj, storage_token="t", storage_url="https://s"),
        reader=_TwoConfigReader(),  # ty: ignore[invalid-argument-type]  (duck-typed test double)
        entities=EntityBuilder(svc, proj, pid, "https://ui.example"),
        pipelines=PipelineBuilder(svc, proj, pid, "https://ui.example"),
        dashboards=DashboardBuilder(svc, proj, pid, "https://ui.example"),
    )
    # No pipeline_fqn_by_config entry for "cfg1" -> declared_edges table->table fallback.
    in_fqn = _storage_fqn(svc, proj, "in.c-main.a")
    out_fqn = _storage_fqn(svc, proj, "out.c-res.x")
    schema_in = fqn_mod.schema_fqn(svc, proj, "in.c-main")
    schema_out = fqn_mod.schema_fqn(svc, proj, "out.c-res")
    known_ids = {in_fqn: "id-in-a", out_fqn: "id-out-x", schema_in: "id-schema-in", schema_out: "id-schema-out"}
    om = _LineageRecordingOM(known_ids)
    report = component_mod.RunReport(run_id="rid")
    config = SimpleNamespace(
        write_table_lineage=True,
        write_pipeline_lineage=False,
        write_column_lineage=False,
        write_bucket_lineage=True,
    )

    comp = component_mod.Component.__new__(component_mod.Component)
    comp._lineage_pass(config, om, run, report)  # ty: ignore[invalid-argument-type]

    edges_by_ids = {(r["edge"]["fromEntity"]["id"], r["edge"]["toEntity"]["id"]): r["edge"] for r in om.put_calls}
    assert ("id-in-a", "id-out-x") in edges_by_ids  # the table->table fallback edge
    schema_edge = edges_by_ids[("id-schema-in", "id-schema-out")]
    assert schema_edge["fromEntity"]["type"] == "databaseSchema"
    assert schema_edge["toEntity"]["type"] == "databaseSchema"
    assert schema_edge["lineageDetails"]["source"] == lineage_builder.SOURCE_SCHEMA


def test_lineage_pass_no_bucket_edge_when_gated_off():
    svc, proj, pid = "keboola-stack", "Acme_Project", "4214"

    class _OneConfigReader:
        def list_component_configs(self):
            return [
                {
                    "id": "keboola.ex-generic",
                    "configurations": [
                        {
                            "id": "cfg1",
                            "configuration": {
                                "storage": {
                                    "input": {"tables": [{"source": "in.c-main.a"}]},
                                    "output": {"tables": [{"destination": "out.c-res.x"}]},
                                }
                            },
                        }
                    ],
                }
            ]

    run = _ProjectRun(
        ctx=ProjectContext(project_id=pid, project_name=proj, storage_token="t", storage_url="https://s"),
        reader=_OneConfigReader(),  # ty: ignore[invalid-argument-type]  (duck-typed test double)
        entities=EntityBuilder(svc, proj, pid, "https://ui.example"),
        pipelines=PipelineBuilder(svc, proj, pid, "https://ui.example"),
        dashboards=DashboardBuilder(svc, proj, pid, "https://ui.example"),
    )
    in_fqn = _storage_fqn(svc, proj, "in.c-main.a")
    out_fqn = _storage_fqn(svc, proj, "out.c-res.x")
    om = _LineageRecordingOM({in_fqn: "id-in-a", out_fqn: "id-out-x"})
    report = component_mod.RunReport(run_id="rid")
    config = SimpleNamespace(
        write_table_lineage=True,
        write_pipeline_lineage=False,
        write_column_lineage=False,
        write_bucket_lineage=False,
    )

    comp = component_mod.Component.__new__(component_mod.Component)
    comp._lineage_pass(config, om, run, report)  # ty: ignore[invalid-argument-type]

    assert len(om.put_calls) == 1  # only the table->table edge; no schema->schema edge
