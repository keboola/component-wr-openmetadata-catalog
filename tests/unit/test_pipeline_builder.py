from mapping.pipeline_builder import PipelineBuilder

UI = "https://connection.keboola.com"


def _builder():
    return PipelineBuilder("keboola-stack", "Acme_Project", "1234", UI)


def test_pipeline_service_body():
    svc = _builder().pipeline_service_body()
    assert svc["serviceType"] == "CustomPipeline"
    assert svc["name"] == "keboola-stack"


def test_config_to_pipeline_with_ordered_tasks_and_sql():
    config = {
        "id": "999",
        "name": "My Transform",
        "configuration": {
            "parameters": {
                "blocks": [
                    {"name": "block 1", "codes": [{"name": "c1", "script": ["SELECT 1", "SELECT 2"]}]},
                    {"name": "block 2", "codes": [{"name": "c2", "script": ["SELECT 3"]}]},
                ]
            }
        },
    }
    built = _builder().build_pipeline("keboola.snowflake-transformation", config)
    assert built.fqn == "keboola-stack.Acme_Project__999"
    body = built.body
    assert body["name"] == "Acme_Project__999"
    tasks = body["tasks"]
    assert [t["name"] for t in tasks] == ["block_1", "block_2"]
    assert tasks[0]["taskType"] == "QUERY"
    assert tasks[0]["taskSQL"] == "SELECT 1;\nSELECT 2"
    assert tasks[0]["downstreamTasks"] == ["block_2"]
    assert tasks[1]["downstreamTasks"] == []
    assert body["sourceUrl"].endswith("/components/keboola.snowflake-transformation/999")


def test_flow_to_pipeline_with_phase_ordering():
    config = {
        "id": "flow-1",
        "name": "Daily Flow",
        "configuration": {
            "phases": [{"id": 1, "name": "extract"}, {"id": 2, "name": "load"}],
            "tasks": [
                {"id": "t1", "name": "extract task", "phase": 1, "enabled": True},
                {"id": "t2", "name": "load task", "phase": 2, "enabled": True},
                {"id": "t3", "name": "disabled", "phase": 2, "enabled": False},
            ],
        },
    }
    b = _builder()
    assert b.is_flow("keboola.orchestrator") is True
    built = b.build_pipeline("keboola.orchestrator", config)
    tasks = built.body["tasks"]
    names = [t["name"] for t in tasks]
    assert "extract_task" in names
    assert "load_task" in names
    assert "disabled" not in names  # disabled task skipped
    extract = next(t for t in tasks if t["name"] == "extract_task")
    assert extract["downstreamTasks"] == ["load_task"]
    assert built.body["sourceUrl"].endswith("/flows/flow-1")


def test_writer_rows_fallback_to_tasks():
    config = {
        "id": "500",
        "name": "Writer",
        "configuration": {"parameters": {}},
        "rows": [
            {"id": "r1", "name": "row one", "configuration": {"parameters": {"query": "SELECT 9"}}},
            {"id": "r2", "name": "row two", "isDisabled": True, "configuration": {}},
        ],
    }
    built = _builder().build_pipeline("keboola.wr-db-snowflake", config)
    tasks = built.body["tasks"]
    assert [t["name"] for t in tasks] == ["row_one"]
    assert tasks[0]["taskSQL"] == "SELECT 9"


def test_python_transformation_gets_python_task_type():
    # E13, PYTHON branch of _task_type: a python transformation's block tasks are
    # tagged PYTHON (not QUERY); the script is still carried as taskSQL (code text).
    config = {
        "id": "777",
        "name": "Score",
        "configuration": {"parameters": {"blocks": [{"name": "b", "codes": [{"name": "c", "script": ["print(1)"]}]}]}},
    }
    built = _builder().build_pipeline("keboola.python-transformation-v2", config)
    task = built.body["tasks"][0]
    assert task["taskType"] == "PYTHON"
    assert task["taskSQL"] == "print(1)"


def test_flow_multi_task_phase_fan_out():
    # E14 downstream fan-out: a phase whose successor holds MULTIPLE tasks lists all
    # of them as downstreamTasks (the single-task-per-phase test can't show this).
    config = {
        "id": "flow-2",
        "configuration": {
            "phases": [{"id": 1, "name": "extract"}, {"id": 2, "name": "load"}],
            "tasks": [
                {"id": "e1", "name": "extract a", "phase": 1, "enabled": True},
                {"id": "l1", "name": "load a", "phase": 2, "enabled": True},
                {"id": "l2", "name": "load b", "phase": 2, "enabled": True},
            ],
        },
    }
    built = _builder().build_pipeline("keboola.orchestrator", config)
    extract = next(t for t in built.body["tasks"] if t["name"] == "extract_a")
    assert extract["downstreamTasks"] == ["load_a", "load_b"]


def test_writer_rows_dict_query_shape():
    # E13 rows fallback: some writers store the row query as a nested dict
    # ({"query": "..."}) rather than a bare string; both must yield taskSQL.
    config = {
        "id": "600",
        "name": "Writer",
        "configuration": {"parameters": {}},
        "rows": [{"id": "r1", "name": "load", "configuration": {"parameters": {"query": {"query": "SELECT 42"}}}}],
    }
    built = _builder().build_pipeline("keboola.wr-db-snowflake", config)
    assert built.body["tasks"][0]["taskSQL"] == "SELECT 42"
