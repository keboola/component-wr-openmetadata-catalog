from mapping.pipeline_builder import CUSTOM_PROPERTIES, PipelineBuilder, component_kind

UI = "https://connection.keboola.com"
STACK = "connection.us-east4.gcp.keboola.com"


def _builder(stack_id=None):
    return PipelineBuilder("keboola-stack", "Acme_Project", "1234", UI, stack_id)


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


# --------------------------------------------------------------------- custom property constants


def test_custom_properties_names_and_types():
    names = {n for n, *_ in CUSTOM_PROPERTIES}
    assert names == {
        "kbcComponentId",
        "kbcConfigId",
        "kbcConfigUrl",
        "kbcLastChange",
        "kbcOwner",
        "kbcSyncedAt",
        "kbcType",
    }
    types = {n: t for n, t, *_ in CUSTOM_PROPERTIES}
    assert types["kbcConfigUrl"] == "hyperlink-cp"
    assert types["kbcOwner"] == "email"
    assert types["kbcType"] == "string"


# --------------------------------------------------------------------- component_kind categorization


def test_component_kind_flow_takes_priority():
    assert component_kind("keboola.orchestrator", None) == "orchestration"
    assert component_kind("keboola.flow", "application") == "orchestration"


def test_component_kind_data_app():
    assert component_kind("keboola.data-apps", None) == "data_app"


def test_component_kind_transformation_from_type_field():
    assert component_kind("keboola.some-future-transform", "transformation") == "transformation"


def test_component_kind_transformation_from_id_set_fallback():
    # No `type` supplied -> falls back to the SQL/PYTHON id sets already used to
    # pick a Pipeline task's taskType.
    assert component_kind("keboola.snowflake-transformation", None) == "transformation"
    assert component_kind("keboola.python-transformation-v2", None) == "transformation"


def test_component_kind_extractor_writer_application_from_type_field():
    assert component_kind("keboola.some-extractor", "extractor") == "extractor"
    assert component_kind("keboola.some-writer", "writer") == "writer"
    assert component_kind("keboola.some-app", "application") == "application"


def test_component_kind_extractor_from_id_pattern_when_type_missing():
    # Verified: list_component_configs() DOES return `type`, but the id-pattern
    # fallback must still hold when `type` is absent/unrecognised.
    assert component_kind("keboola.ex-db-mysql", None) == "extractor"


def test_component_kind_writer_from_id_pattern_when_type_missing():
    assert component_kind("keboola.wr-db-snowflake", None) == "writer"


def test_component_kind_other_when_nothing_matches():
    assert component_kind("keboola.processor-unzip", None) == "other"
    assert component_kind("keboola.processor-unzip", "processor") == "other"


# --------------------------------------------------------------------- kbcType extension wiring


def test_config_pipeline_extension_carries_kbc_type():
    config = {"id": "999", "configuration": {"parameters": {}}}
    built = _builder().build_pipeline("keboola.snowflake-transformation", config, kind="transformation")
    assert built.body["extension"]["kbcType"] == "transformation"


def test_flow_pipeline_extension_carries_kbc_type():
    config = {"id": "flow-1", "configuration": {"phases": [], "tasks": []}}
    built = _builder().build_pipeline("keboola.orchestrator", config, kind="orchestration")
    assert built.body["extension"]["kbcType"] == "orchestration"


def test_pipeline_extension_omits_kbc_type_when_kind_not_given():
    # Backward-compatible default: every pre-existing call site that predates the
    # object-family split keeps working, simply without a kbcType value.
    config = {"id": "999", "configuration": {"parameters": {}}}
    built = _builder().build_pipeline("keboola.snowflake-transformation", config)
    assert "kbcType" not in built.body["extension"]


# --------------------------------------------------------------------- config-pipeline extension/owner


def test_config_pipeline_extension_full_population_plain_email():
    config = {
        "id": "999",
        "name": "My Transform",
        "currentVersion": {
            "created": "2026-04-14T20:56:16+0200",
            "creatorToken": {"description": "jakub.smagin@keboola.com"},
        },
        "configuration": {"parameters": {"blocks": []}},
    }
    built = _builder(STACK).build_pipeline("keboola.snowflake-transformation", config, synced_at="2026-09-22 10:00 UTC")
    ext = built.body["extension"]
    assert ext["kbcComponentId"] == "keboola.snowflake-transformation"
    assert ext["kbcConfigId"] == "999"
    assert ext["kbcOwner"] == "jakub.smagin@keboola.com"
    assert ext["kbcLastChange"] == "2026-04-14 20:56"
    assert ext["kbcSyncedAt"] == "2026-09-22 10:00 UTC"
    assert ext["kbcConfigUrl"] == {
        "url": (
            "https://connection.us-east4.gcp.keboola.com/admin/projects/1234/components/"
            "keboola.snowflake-transformation/999"
        ),
        "displayText": "Open configuration",
    }


def test_config_pipeline_owner_extracted_from_wrapped_email():
    # Same extractor dashboard_builder uses (enrichment.creator_token_email) -> no duplicate regex.
    config = {
        "id": "999",
        "currentVersion": {"creatorToken": {"description": "kbagent-cli [x@keboola.com]"}},
        "configuration": {"parameters": {}},
    }
    built = _builder().build_pipeline("keboola.snowflake-transformation", config)
    assert built.body["extension"]["kbcOwner"] == "x@keboola.com"


def test_config_pipeline_extension_filtered_by_available_properties():
    config = {"id": "999", "configuration": {"parameters": {}}}
    built = _builder().build_pipeline(
        "keboola.snowflake-transformation", config, available_properties={"kbcComponentId"}
    )
    assert set(built.body["extension"].keys()) == {"kbcComponentId"}


def test_config_pipeline_owners_set_via_resolver():
    config = {
        "id": "999",
        "currentVersion": {"creatorToken": {"description": "jakub.smagin@keboola.com"}},
        "configuration": {"parameters": {}},
    }
    built = _builder().build_pipeline(
        "keboola.snowflake-transformation", config, owner_resolver=lambda email: "om-user-3"
    )
    assert built.body["owners"] == [{"id": "om-user-3", "type": "user"}]


def test_config_pipeline_owners_absent_without_resolver():
    config = {
        "id": "999",
        "currentVersion": {"creatorToken": {"description": "jakub.smagin@keboola.com"}},
        "configuration": {"parameters": {}},
    }
    built = _builder().build_pipeline("keboola.snowflake-transformation", config)
    assert "owners" not in built.body


def test_config_pipeline_owners_absent_when_no_creator_email_resolver_never_called():
    calls = []

    def resolver(email):
        calls.append(email)
        return "x"

    config = {"id": "999", "configuration": {"parameters": {}}}
    built = _builder().build_pipeline("keboola.snowflake-transformation", config, owner_resolver=resolver)
    assert "owners" not in built.body
    assert calls == []


# --------------------------------------------------------------------- flow-pipeline shares the logic


def test_flow_pipeline_extension_and_owner_share_config_pipeline_logic():
    config = {
        "id": "flow-1",
        "currentVersion": {
            "created": "2026-04-14T20:56:16+0200",
            "creatorToken": {"description": "kbagent-cli [flow-owner@keboola.com]"},
        },
        "configuration": {"phases": [], "tasks": []},
    }
    built = _builder(STACK).build_pipeline("keboola.orchestrator", config, synced_at="2026-09-22 10:00 UTC")
    ext = built.body["extension"]
    assert ext["kbcComponentId"] == "keboola.orchestrator"
    assert ext["kbcConfigId"] == "flow-1"
    assert ext["kbcOwner"] == "flow-owner@keboola.com"
    assert ext["kbcLastChange"] == "2026-04-14 20:56"
    assert ext["kbcSyncedAt"] == "2026-09-22 10:00 UTC"
    assert ext["kbcConfigUrl"] == {
        "url": "https://connection.us-east4.gcp.keboola.com/admin/projects/1234/flows/flow-1",
        "displayText": "Open configuration",
    }


def test_flow_pipeline_owners_set_via_resolver():
    config = {
        "id": "flow-1",
        "currentVersion": {"creatorToken": {"description": "flow-owner@keboola.com"}},
        "configuration": {"phases": [], "tasks": []},
    }
    built = _builder().build_pipeline("keboola.orchestrator", config, owner_resolver=lambda email: "om-user-5")
    assert built.body["owners"] == [{"id": "om-user-5", "type": "user"}]


def test_flow_pipeline_owners_absent_when_no_creator_email_resolver_never_called():
    calls = []

    def resolver(email):
        calls.append(email)
        return "x"

    config = {"id": "flow-1", "configuration": {"phases": [], "tasks": []}}
    built = _builder().build_pipeline("keboola.orchestrator", config, owner_resolver=resolver)
    assert "owners" not in built.body
    assert calls == []
