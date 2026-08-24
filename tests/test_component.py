"""Orchestrator tests for the OpenMetadata catalog writer (T18).

Runs Component against a temporary KBC_DATADIR with the network clients faked.
"""

import json

import pytest
from keboola.component.exceptions import UserException

import component as component_mod
from client.manage_client import ManageScopeError
from client.om_client import OMAuthError
from client.storage_reader import SourceBucket, SourceColumn, SourceTable

BASE_PARAMS = {
    "om_host": "https://om.example.com",
    "#bot_token": "jwt",
    "write_pipelines": False,
    "write_lineage": False,
    "write_column_lineage": False,
}


def _make_datadir(tmp_path, params, state=None, action=None):
    data = tmp_path / "data"
    (data / "in" / "tables").mkdir(parents=True)
    (data / "out" / "tables").mkdir(parents=True)
    cfg = {"parameters": params}
    if action:
        cfg["action"] = action
    (data / "config.json").write_text(json.dumps(cfg))
    if state is not None:
        (data / "in" / "state.json").write_text(json.dumps(state))
    return str(data)


class FakeOM:
    def __init__(self, *a, fail_tables=False, **k):
        self.server_version = None
        self.is_2_0_or_newer = False
        self.fail_tables = fail_tables
        self.put_calls = []

    def probe_version(self):
        self.server_version = "1.13.4"
        return {"version": "1.13.4", "revision": "r", "timestamp": 1}

    def get_by_fqn(self, kind, fqn, fields=None):
        return None

    def put_entity(self, kind, body):
        if kind == "tables" and self.fail_tables:
            raise RuntimeError("boom")
        self.put_calls.append((kind, body.get("name")))
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


class FakeStorage:
    def __init__(self, *a, **k):
        pass

    def verify_token(self):
        return {"owner": {"id": "777", "name": "Acme Project"}}

    def list_buckets(self):
        return [SourceBucket(id="out.c-sales", name="c-sales", stage="out", path="out.c-sales")]

    def iter_tables(self, bucket_id):
        return iter([SourceTable(id="out.c-sales.orders", name="orders", columns=[SourceColumn(name="id")])])

    def list_component_configs(self):
        return []

    def read_snapshot_rows(self, table_id, limit=1000000):
        return []


@pytest.fixture
def _env(monkeypatch):
    monkeypatch.setenv("KBC_TOKEN", "storage-tok")
    monkeypatch.setenv("KBC_URL", "https://connection.keboola.com")
    monkeypatch.setenv("KBC_STACKID", "connection.keboola.com")
    monkeypatch.setenv("KBC_PROJECTNAME", "Acme Project")
    monkeypatch.delenv("KBC_CONFIGROWID", raising=False)
    monkeypatch.delenv("KBC_DATA_TYPE_SUPPORT", raising=False)


def test_run_catalog_happy_path(tmp_path, monkeypatch, _env):
    monkeypatch.setenv("KBC_DATADIR", _make_datadir(tmp_path, BASE_PARAMS))
    monkeypatch.setattr(component_mod, "OMClient", FakeOM)
    monkeypatch.setattr(component_mod, "StorageReader", FakeStorage)

    comp = component_mod.Component()
    comp.run()

    report_csv = tmp_path / "data" / "out" / "tables" / "catalog_run_report.csv"
    snapshot_csv = tmp_path / "data" / "out" / "tables" / "last_written_snapshot.csv"
    assert report_csv.exists()
    assert snapshot_csv.exists()
    content = report_csv.read_text()
    assert "created" in content
    assert "out_c-sales.orders" in content
    # state persisted with a bucket digest for the project
    state = json.loads((tmp_path / "data" / "out" / "state.json").read_text())
    assert state["run_count"] == 1
    assert "out.c-sales" in state["projects"]["777"]["bucket_digests"]


def test_all_projects_without_manage_token_raises(tmp_path, monkeypatch, _env):
    params = {**BASE_PARAMS, "project_scope": "all_projects"}
    monkeypatch.setenv("KBC_DATADIR", _make_datadir(tmp_path, params))
    monkeypatch.setattr(component_mod, "OMClient", FakeOM)
    monkeypatch.setattr(component_mod, "StorageReader", FakeStorage)
    with pytest.raises(UserException):
        component_mod.Component().run()


def test_tier2_scope_failure_degrades(tmp_path, monkeypatch, _env):
    params = {**BASE_PARAMS, "project_scope": "all_projects", "#manage_token": "mng"}
    monkeypatch.setenv("KBC_DATADIR", _make_datadir(tmp_path, params))
    monkeypatch.setattr(component_mod, "OMClient", FakeOM)
    monkeypatch.setattr(component_mod, "StorageReader", FakeStorage)

    class FailingManage:
        def __init__(self, *a, **k):
            pass

        def enumerate_and_mint(self, org):
            raise ManageScopeError("insufficient scope")

    monkeypatch.setattr(component_mod, "ManageClient", FailingManage)

    comp = component_mod.Component()
    comp.run()  # must NOT raise — degrade to host project

    content = (tmp_path / "data" / "out" / "tables" / "catalog_run_report.csv").read_text()
    assert "degraded" in content


def test_collect_and_fail_raises_at_end(tmp_path, monkeypatch, _env):
    monkeypatch.setenv("KBC_DATADIR", _make_datadir(tmp_path, BASE_PARAMS))
    monkeypatch.setattr(component_mod, "OMClient", lambda *a, **k: FakeOM(fail_tables=True))
    monkeypatch.setattr(component_mod, "StorageReader", FakeStorage)

    comp = component_mod.Component()
    with pytest.raises(UserException):
        comp.run()
    # the report is still written (write_always) before the raise
    content = (tmp_path / "data" / "out" / "tables" / "catalog_run_report.csv").read_text()
    assert "failed" in content


def test_log_only_does_not_raise(tmp_path, monkeypatch, _env):
    params = {**BASE_PARAMS, "failure_mode": "log_only"}
    monkeypatch.setenv("KBC_DATADIR", _make_datadir(tmp_path, params))
    monkeypatch.setattr(component_mod, "OMClient", lambda *a, **k: FakeOM(fail_tables=True))
    monkeypatch.setattr(component_mod, "StorageReader", FakeStorage)
    component_mod.Component().run()  # log_only -> exit 0


def test_test_connection_ok(tmp_path, monkeypatch, _env):
    monkeypatch.setenv("KBC_DATADIR", _make_datadir(tmp_path, BASE_PARAMS, action="testConnection"))
    monkeypatch.setattr(component_mod, "OMClient", FakeOM)
    monkeypatch.setattr(component_mod, "StorageReader", FakeStorage)
    result = component_mod.Component().test_connection()
    assert "1.13.4" in result.message


def test_test_connection_bad_bot_token(tmp_path, monkeypatch, _env):
    class BadOM(FakeOM):
        def probe_version(self):
            raise OMAuthError("401 unauthorized")

    monkeypatch.setenv("KBC_DATADIR", _make_datadir(tmp_path, BASE_PARAMS, action="testConnection"))
    monkeypatch.setattr(component_mod, "OMClient", BadOM)
    monkeypatch.setattr(component_mod, "StorageReader", FakeStorage)
    # the sync-action wrapper converts a UserException into exit(1)
    with pytest.raises(SystemExit) as exc:
        component_mod.Component().test_connection()
    assert exc.value.code == 1


def test_list_buckets_sync_action(tmp_path, monkeypatch, _env):
    monkeypatch.setenv("KBC_DATADIR", _make_datadir(tmp_path, BASE_PARAMS, action="listBuckets"))
    monkeypatch.setattr(component_mod, "StorageReader", FakeStorage)
    elements = component_mod.Component().list_buckets()
    assert elements[0].value == "out.c-sales"


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main([__file__, "-v"]))
