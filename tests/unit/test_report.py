from report import (
    ACTION_CREATED,
    ACTION_DEGRADED,
    ACTION_FAILED,
    ACTION_UNRESOLVED,
    REPORT_PRIMARY_KEY,
    RunReport,
    report_schema,
    snapshot_rows,
)


def test_report_row_shape_and_null_safe_config_row_id():
    report = RunReport(run_id="run-1", config_row_id=None)
    report.record(
        project_id="1234",
        entity_type="Table",
        entity_fqn="svc.p.b.t",
        action=ACTION_CREATED,
        om_status_code=200,
        timestamp="2026-08-24T10:00:00+00:00",
    )
    row = report.rows()[0]
    assert row["config_row_id"] == ""  # None -> empty, never a crash
    assert row["om_status_code"] == "200"
    assert row["action"] == "created"
    assert row["timestamp"] == "2026-08-24T10:00:00+00:00"


def test_new_action_values_recorded():
    report = RunReport(run_id="r", config_row_id="row-9")
    report.record(project_id="p", entity_type="Column", entity_fqn="a", action=ACTION_UNRESOLVED)
    report.record(project_id="p", entity_type="Project", entity_fqn="b", action=ACTION_DEGRADED)
    report.record(project_id="p", entity_type="Table", entity_fqn="c", action=ACTION_FAILED)
    counts = report.counts()
    assert counts["unresolved"] == 1
    assert counts["degraded"] == 1
    assert counts["failed"] == 1
    assert report.has_failures() is True


def test_no_failures_when_none_recorded():
    report = RunReport(run_id="r")
    report.record(project_id="p", entity_type="Table", entity_fqn="c", action=ACTION_CREATED)
    assert report.has_failures() is False


def test_report_schema_native_types():
    schema = {s["name"]: s for s in report_schema()}
    assert schema["timestamp"]["base_type"] == "TIMESTAMP"
    assert schema["om_status_code"]["base_type"] == "INTEGER"
    assert schema["entity_fqn"]["base_type"] == "STRING"
    pk = {s["name"] for s in report_schema() if s["primary_key"]}
    assert pk == set(REPORT_PRIMARY_KEY)


def test_snapshot_rows_from_entries():
    class E:
        entity_fqn = "svc.p.b.t"
        entity_type = "Table"
        written_fields_json = "{}"
        content_hash = "abc"

    rows = snapshot_rows([E()])
    assert rows[0]["entity_fqn"] == "svc.p.b.t"
    assert rows[0]["content_hash"] == "abc"
    assert "updated_at" in rows[0]
