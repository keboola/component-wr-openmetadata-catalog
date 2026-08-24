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

import pytest
from keboola.component.exceptions import UserException

import component as component_mod
from client.storage_reader import SourceBucket, SourceColumn, SourceTable
from mapping import fqn as fqn_mod

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

    def probe_version(self):
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
