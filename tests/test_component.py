"""Orchestrator tests for the OpenMetadata catalog writer (T18).

Runs Component against a temporary KBC_DATADIR with the network clients faked.
"""

import csv
import io
import json

import pytest
from keboola.component.exceptions import UserException

import component as component_mod
from client.om_client import OMAuthError, OMConnectionError
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


class PersistentOM(FakeOM):
    """A FakeOM that remembers what was written, like a real OM across runs."""

    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        self.store: dict[tuple[str, str], dict] = {}

    @staticmethod
    def _fqn(kind, body):
        parent = {"databases": "service", "databaseSchemas": "database", "tables": "databaseSchema"}.get(kind)
        return f"{body[parent]}.{body['name']}" if parent and body.get(parent) else body["name"]

    def get_by_fqn(self, kind, fqn, fields=None):
        return self.store.get((kind, fqn))

    def put_entity(self, kind, body):
        created = super().put_entity(kind, body)
        self.store[(kind, self._fqn(kind, body))] = {**body, **created}
        return created


def _run_twice(tmp_path, monkeypatch, second_params, *, same_om=True):
    """Run 1 with BASE_PARAMS, then run 2 on its state; return run 2's table writes."""
    monkeypatch.setattr(component_mod, "StorageReader", FakeStorage)
    om1 = PersistentOM()
    monkeypatch.setattr(component_mod, "OMClient", lambda *a, **k: om1)
    monkeypatch.setenv("KBC_DATADIR", _make_datadir(tmp_path / "r1", BASE_PARAMS))
    component_mod.Component().run()
    state = json.loads((tmp_path / "r1" / "data" / "out" / "state.json").read_text())

    om2 = om1 if same_om else PersistentOM()
    om2.put_calls = []
    monkeypatch.setattr(component_mod, "OMClient", lambda *a, **k: om2)
    monkeypatch.setenv("KBC_DATADIR", _make_datadir(tmp_path / "r2", second_params, state=state))
    component_mod.Component().run()
    return [name for kind, name in om2.put_calls if kind == "tables"]


def test_unchanged_bucket_is_skipped_on_the_same_target(tmp_path, monkeypatch, _env):
    assert _run_twice(tmp_path, monkeypatch, BASE_PARAMS) == []


def test_service_name_change_resyncs_unchanged_buckets(tmp_path, monkeypatch, _env):
    """After a Service Name change the new service tree is empty, so an unchanged
    bucket must be written again (else it goes missing there and lineage into it is dropped)."""
    assert _run_twice(tmp_path, monkeypatch, {**BASE_PARAMS, "service_name": "keboola-renamed"}) == ["orders"]


def _report_actions(run_dir):
    rows = csv.DictReader(io.StringIO((run_dir / "data" / "out" / "tables" / "catalog_run_report.csv").read_text()))
    return {(r["entity_type"], r["action"]) for r in rows}


def test_catalog_format_change_resyncs_unchanged_buckets(tmp_path, monkeypatch, _env):
    """When a release changes what the writer emits (e.g. public deep links), unchanged
    buckets must be processed once more, or the fix never reaches existing entities."""
    monkeypatch.setattr(component_mod, "StorageReader", FakeStorage)
    om = PersistentOM()
    monkeypatch.setattr(component_mod, "OMClient", lambda *a, **k: om)
    monkeypatch.setenv("KBC_DATADIR", _make_datadir(tmp_path / "r1", BASE_PARAMS))
    component_mod.Component().run()
    state = json.loads((tmp_path / "r1" / "data" / "out" / "state.json").read_text())

    monkeypatch.setenv("KBC_DATADIR", _make_datadir(tmp_path / "same", BASE_PARAMS, state=state))
    component_mod.Component().run()
    # A digest-skipped bucket reports only its Schema; a processed one also reports its tables.
    assert not any(entity_type == "Table" for entity_type, _ in _report_actions(tmp_path / "same"))

    monkeypatch.setattr(component_mod, "_CATALOG_FORMAT_VERSION", component_mod._CATALOG_FORMAT_VERSION + 1)
    monkeypatch.setenv("KBC_DATADIR", _make_datadir(tmp_path / "bumped", BASE_PARAMS, state=state))
    component_mod.Component().run()
    assert any(entity_type == "Table" for entity_type, _ in _report_actions(tmp_path / "bumped"))


def test_wiped_om_resyncs_unchanged_buckets(tmp_path, monkeypatch, _env):
    """Same target, but the OM service was deleted between runs: the saved digests
    say "already in OM" while OM holds nothing, so the run must write the bucket again."""
    assert _run_twice(tmp_path, monkeypatch, BASE_PARAMS, same_om=False) == ["orders"]


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
            self.server_version: str | None = None
            self._last: tuple[str, str] | None = None

        def probe_version(self):
            self.server_version = "1.13.4"
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


def test_legacy_org_scope_keys_are_ignored_and_host_project_cataloged(tmp_path, monkeypatch, _env):
    """The org-wide mode is gone: a row saved with the old ``scope`` / ``#manage_token`` /
    ``organization_id`` keys is not rejected, and still catalogs its own host project."""
    params = {**BASE_PARAMS, "scope": "all_projects", "#manage_token": "mng", "organization_id": "123"}
    monkeypatch.setenv("KBC_DATADIR", _make_datadir(tmp_path, params))
    monkeypatch.setattr(component_mod, "OMClient", FakeOM)
    monkeypatch.setattr(component_mod, "StorageReader", FakeStorage)

    component_mod.Component().run()

    report_rows = list(
        csv.DictReader(io.StringIO((tmp_path / "data" / "out" / "tables" / "catalog_run_report.csv").read_text()))
    )
    assert {r["entity_type"] for r in report_rows} >= {"Table"}
    assert all(r["action"] != "degraded" for r in report_rows)


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


def test_test_connection_unreachable_host(tmp_path, monkeypatch, _env):
    # An unreachable OM host is user-fixable -> UserException -> exit 1 (spec 6.3),
    # not an exit-2 internal error.
    class UnreachableOM(FakeOM):
        def probe_version(self):
            raise OMConnectionError("Could not reach OpenMetadata (GET /system/version) after 5 attempts.")

    monkeypatch.setenv("KBC_DATADIR", _make_datadir(tmp_path, BASE_PARAMS, action="testConnection"))
    monkeypatch.setattr(component_mod, "OMClient", UnreachableOM)
    monkeypatch.setattr(component_mod, "StorageReader", FakeStorage)
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
