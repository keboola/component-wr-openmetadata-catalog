"""Orchestrator tests for the OpenMetadata catalog writer (T18).

Runs Component against a temporary KBC_DATADIR with the network clients faked.
"""

import csv
import io
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

    def verify_auth(self):
        return {"name": "ingestion-bot"}

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


def test_second_run_loads_base_and_updates_changed_owned_field(tmp_path, monkeypatch, _env):
    """BLOCKING #1 regression: the three-way-merge base must be populated on the
    second run from the prior run's snapshot, round-tripped through
    ``state.snapshot_table``. A Keboola-side change to an owned field then merges
    (``updated``) instead of being classified ``skipped_diverged``.

    Under the original defect ``state.snapshot_table`` was never written, so the
    base was always empty and this test would report ``skipped_diverged``.
    """
    om_store: dict = {}  # (kind, fqn) -> body, shared across both runs (like OM server)
    snapshot_holder: dict = {"rows": []}  # rows the prior run wrote to Storage
    desc_holder: dict = {"description": "v1"}  # the desired table description per run

    class StatefulOM:
        def __init__(self, *a, **k):
            self.is_2_0_or_newer = False
            self._last: tuple[str, str] | None = None

        def probe_version(self):
            return {"version": "1.13.4", "revision": "r", "timestamp": 1}

        def get_by_fqn(self, kind, fqn, fields=None):
            self._last = (kind, fqn)
            return om_store.get((kind, fqn))

        def put_entity(self, kind, body):
            assert self._last is not None  # always set by a preceding get_by_fqn
            om_store[self._last] = dict(body)
            return {"id": f"id-{self._last[1]}"}

        def patch_entity(self, kind, fqn, patch):
            entity = dict(om_store.get((kind, fqn)) or {})
            for op in patch:
                entity[op["path"].lstrip("/")] = op["value"]
            om_store[(kind, fqn)] = entity
            return {"id": "x"}

        def list_entities(self, kind, params=None, page_size=200):
            return iter([])

        def delete_lineage_by_source(self, *a):
            return None

        def soft_delete(self, *a, **k):
            return None

        def put_lineage(self, *a):
            return {}

        def put_pipeline_status(self, *a):
            return {}

    class RecordingStorage(FakeStorage):
        def iter_tables(self, bucket_id):
            return iter(
                [
                    SourceTable(
                        id="out.c-sales.orders",
                        name="orders",
                        description=desc_holder["description"],
                        columns=[SourceColumn(name="id")],
                    )
                ]
            )

        def read_snapshot_rows(self, table_id, limit=1000000):
            return list(snapshot_holder["rows"])

    monkeypatch.setattr(component_mod, "OMClient", StatefulOM)
    monkeypatch.setattr(component_mod, "StorageReader", RecordingStorage)

    # --- Run 1: first sight of the table, created at description "v1". ---
    monkeypatch.setenv("KBC_DATADIR", _make_datadir(tmp_path / "r1", BASE_PARAMS))
    component_mod.Component().run()

    state_after_1 = json.loads((tmp_path / "r1" / "data" / "out" / "state.json").read_text())
    assert state_after_1["snapshot_table"] == "in.c-wr-openmetadata-catalog.last_written_snapshot"

    # The platform would load this CSV into Storage; feed it to run 2 as the base.
    snap_csv = (tmp_path / "r1" / "data" / "out" / "tables" / "last_written_snapshot.csv").read_text()
    snapshot_holder["rows"] = list(csv.DictReader(io.StringIO(snap_csv)))
    assert snapshot_holder["rows"], "run 1 must produce a non-empty snapshot base"

    # --- Run 2: same table, owned field changed to "v2" (state carried over). ---
    desc_holder["description"] = "v2"
    monkeypatch.setenv("KBC_DATADIR", _make_datadir(tmp_path / "r2", BASE_PARAMS, state=state_after_1))
    component_mod.Component().run()

    report_rows = list(
        csv.DictReader(
            io.StringIO((tmp_path / "r2" / "data" / "out" / "tables" / "catalog_run_report.csv").read_text())
        )
    )
    table_rows = [r for r in report_rows if r["entity_type"] == "Table"]
    assert table_rows, "the table must appear in run 2's report"
    assert all(r["action"] != "skipped_diverged" for r in table_rows)
    assert any(r["action"] == "updated" for r in table_rows)


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
    # /system/version is unauthenticated, so the version probe succeeds even with
    # a bad token; the bad #bot_token must be caught by the authenticated
    # verify_auth() call (GET /users/loggedInUser -> 401).
    class BadOM(FakeOM):
        def verify_auth(self):
            raise OMAuthError("OpenMetadata rejected the bot token (401) on GET /users/loggedInUser.")

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
