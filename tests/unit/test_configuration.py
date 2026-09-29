import pytest
from keboola.component.exceptions import UserException

from configuration import (
    Configuration,
    FailureMode,
    MergeMode,
)


def _base_params(**overrides):
    params = {
        "om_host": "https://om.example.com",
        "#bot_token": "jwt-abc",
    }
    params.update(overrides)
    return params


def test_valid_config_parses():
    cfg = Configuration(**_base_params())
    assert cfg.om_host == "https://om.example.com"
    assert cfg.bot_token == "jwt-abc"
    assert cfg.merge_mode == MergeMode.THREE_WAY_MERGE
    assert cfg.failure_mode == FailureMode.COLLECT_AND_FAIL
    assert cfg.write_table_lineage is True
    assert cfg.write_column_lineage is True
    assert cfg.write_bucket_lineage is True
    assert cfg.write_pipeline_lineage is True
    assert cfg.write_dashboard_lineage is True
    assert cfg.write_buckets is True
    assert cfg.write_transformations is True
    assert cfg.write_components is True
    assert cfg.write_flows is True
    assert cfg.write_data_apps is True
    assert cfg.buckets == []
    assert cfg.transformations == []
    assert cfg.components == []
    assert cfg.flows == []
    assert cfg.data_apps == []
    assert not hasattr(cfg, "write_pipelines")
    assert not hasattr(cfg, "configurations")
    assert not hasattr(cfg, "write_lineage")


def test_removed_org_scope_keys_are_ignored():
    """The org-wide mode was removed: a row still carrying its old keys parses, and
    none of them survive on the model."""
    cfg = Configuration(
        **_base_params(
            scope="all_projects",
            **{"#manage_token": "manage-xyz"},
            organization_id="123",
            projects=["111"],
        )
    )
    for removed in ("scope", "manage_token", "organization_id", "projects"):
        assert not hasattr(cfg, removed), removed


def test_missing_om_host_raises_userexception():
    with pytest.raises(UserException) as exc:
        Configuration(**{"#bot_token": "jwt"})
    assert "om_host" in str(exc.value)


def test_missing_bot_token_raises_userexception():
    with pytest.raises(UserException) as exc:
        Configuration(om_host="https://om.example.com")
    assert "bot_token" in str(exc.value)


def test_bad_enum_value_rejected():
    with pytest.raises(UserException):
        Configuration(**_base_params(merge_mode="nonsense"))


def test_ssh_enabled_without_block_raises():
    with pytest.raises(UserException):
        Configuration(**_base_params(use_ssh_tunnel=True))


def test_ssh_block_parses_with_alias():
    cfg = Configuration(
        **_base_params(
            use_ssh_tunnel=True,
            ssh={"host": "bastion", "user": "kbc", "port": 2222, "#private_key": "KEY"},
        )
    )
    assert cfg.ssh is not None
    assert cfg.ssh.host == "bastion"
    assert cfg.ssh.port == 2222
    assert cfg.ssh.private_key == "KEY"


def test_service_name_default_resolves_from_stackid():
    cfg = Configuration(**_base_params())
    assert cfg.resolve_service_name("connection.keboola.com") == "keboola-connection-keboola-com"
    assert cfg.resolve_service_name(None) == "keboola"


def test_service_name_override_wins():
    cfg = Configuration(**_base_params(service_name="my-service"))
    assert cfg.resolve_service_name("connection.keboola.com") == "my-service"


def test_storage_token_row_field_alias():
    cfg = Configuration(**_base_params(**{"#storage_token": "row-token"}))
    assert cfg.storage_token == "row-token"


def test_selector_fields_parse_non_empty():
    cfg = Configuration(
        **_base_params(
            buckets=["in.c-main"],
            transformations=["123"],
            components=["456"],
            flows=["789"],
            data_apps=["01app"],
        )
    )
    assert cfg.buckets == ["in.c-main"]
    assert cfg.transformations == ["123"]
    assert cfg.components == ["456"]
    assert cfg.flows == ["789"]
    assert cfg.data_apps == ["01app"]


def test_family_enable_bools_can_be_disabled():
    cfg = Configuration(
        **_base_params(
            write_buckets=False,
            write_transformations=False,
            write_components=False,
            write_flows=False,
            write_data_apps=False,
            write_bucket_lineage=False,
            write_table_lineage=False,
            write_column_lineage=False,
            write_pipeline_lineage=False,
            write_dashboard_lineage=False,
        )
    )
    assert cfg.write_buckets is False
    assert cfg.write_transformations is False
    assert cfg.write_components is False
    assert cfg.write_flows is False
    assert cfg.write_data_apps is False
    assert cfg.write_bucket_lineage is False
    assert cfg.write_table_lineage is False
    assert cfg.write_column_lineage is False
    assert cfg.write_pipeline_lineage is False
    assert cfg.write_dashboard_lineage is False


def test_nested_ui_groups_are_flattened():
    """The row schema nests families under ``objects`` and behaviour under
    ``advanced`` / ``advanced.lineage``; the UI saves that nested shape. The
    model must read the nested values, not silently drop them to defaults."""
    cfg = Configuration(
        **_base_params(
            objects={
                "write_buckets": True,
                "buckets": ["in.c-main"],
                "write_transformations": False,
                "transformations": [],
                "write_components": True,
                "components": ["1234"],
                "write_flows": False,
                "flows": [],
                "write_data_apps": False,
                "data_apps": [],
            },
            advanced={
                "lineage": {
                    "write_bucket_lineage": False,
                    "write_table_lineage": True,
                    "write_column_lineage": False,
                    "write_pipeline_lineage": True,
                    "write_dashboard_lineage": False,
                },
                "write_pipeline_status": False,
                "full_refresh": True,
                "merge_mode": "keboola_always_wins",
                "failure_mode": "log_only",
            },
        )
    )
    # objects group
    assert cfg.write_buckets is True
    assert cfg.buckets == ["in.c-main"]
    assert cfg.write_transformations is False
    assert cfg.write_components is True
    assert cfg.components == ["1234"]
    assert cfg.write_flows is False
    assert cfg.write_data_apps is False
    # advanced.lineage group
    assert cfg.write_bucket_lineage is False
    assert cfg.write_table_lineage is True
    assert cfg.write_column_lineage is False
    assert cfg.write_pipeline_lineage is True
    assert cfg.write_dashboard_lineage is False
    # advanced scalars
    assert cfg.write_pipeline_status is False
    assert cfg.full_refresh is True
    assert cfg.merge_mode == MergeMode.KEBOOLA_ALWAYS_WINS
    assert cfg.failure_mode == FailureMode.LOG_ONLY
    # the nested containers themselves are not retained as stray attributes
    assert not hasattr(cfg, "objects")
    assert not hasattr(cfg, "advanced")


def test_flat_config_still_parses_without_nested_groups():
    """A flat config (older configs / functional fixtures, no ``objects`` /
    ``advanced``) must keep working unchanged."""
    cfg = Configuration(**_base_params(write_buckets=False, merge_mode="keboola_always_wins"))
    assert cfg.write_buckets is False
    assert cfg.merge_mode == MergeMode.KEBOOLA_ALWAYS_WINS
    assert cfg.write_table_lineage is True
