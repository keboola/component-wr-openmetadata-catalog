import pytest
from keboola.component.exceptions import UserException

from configuration import (
    BranchFilter,
    Configuration,
    FailureMode,
    MergeMode,
    ProjectScope,
    Stage,
)


def _tier1_params(**overrides):
    params = {
        "om_host": "https://om.example.com",
        "#bot_token": "jwt-abc",
    }
    params.update(overrides)
    return params


def test_valid_tier1_config_parses():
    cfg = Configuration(**_tier1_params())
    assert cfg.om_host == "https://om.example.com"
    assert cfg.bot_token == "jwt-abc"
    assert cfg.project_scope == ProjectScope.ROWS
    assert cfg.merge_mode == MergeMode.THREE_WAY_MERGE
    assert cfg.failure_mode == FailureMode.COLLECT_AND_FAIL
    assert cfg.branch_filter == BranchFilter.PRODUCTION_ONLY
    assert cfg.write_lineage is True
    assert cfg.write_column_lineage is True
    assert cfg.stages == [Stage.IN, Stage.OUT]
    assert cfg.bucket_allowlist == []


def test_valid_tier2_config_parses():
    cfg = Configuration(
        **_tier1_params(
            project_scope="all_projects",
            **{"#manage_token": "manage-xyz"},
            organization_id="123",
        )
    )
    assert cfg.project_scope == ProjectScope.ALL_PROJECTS
    assert cfg.manage_token == "manage-xyz"
    assert cfg.organization_id == "123"


def test_missing_om_host_raises_userexception():
    with pytest.raises(UserException) as exc:
        Configuration(**{"#bot_token": "jwt"})
    assert "om_host" in str(exc.value)


def test_missing_bot_token_raises_userexception():
    with pytest.raises(UserException) as exc:
        Configuration(om_host="https://om.example.com")
    assert "bot_token" in str(exc.value)


def test_all_projects_without_manage_token_raises_userexception():
    with pytest.raises(UserException) as exc:
        Configuration(**_tier1_params(project_scope="all_projects", organization_id="123"))
    assert "manage_token" in str(exc.value) or "#manage_token" in str(exc.value)


def test_all_projects_without_organization_id_raises_userexception():
    with pytest.raises(UserException) as exc:
        Configuration(**_tier1_params(project_scope="all_projects", **{"#manage_token": "manage-xyz"}))
    assert "organization_id" in str(exc.value)


def test_all_projects_missing_both_names_both_in_message():
    with pytest.raises(UserException) as exc:
        Configuration(**_tier1_params(project_scope="all_projects"))
    message = str(exc.value)
    assert "#manage_token" in message and "organization_id" in message


def test_bad_enum_value_rejected():
    with pytest.raises(UserException):
        Configuration(**_tier1_params(merge_mode="nonsense"))


def test_ssh_enabled_without_block_raises():
    with pytest.raises(UserException):
        Configuration(**_tier1_params(use_ssh_tunnel=True))


def test_ssh_block_parses_with_alias():
    cfg = Configuration(
        **_tier1_params(
            use_ssh_tunnel=True,
            ssh={"host": "bastion", "user": "kbc", "port": 2222, "#private_key": "KEY"},
        )
    )
    assert cfg.ssh is not None
    assert cfg.ssh.host == "bastion"
    assert cfg.ssh.port == 2222
    assert cfg.ssh.private_key == "KEY"


def test_service_name_default_resolves_from_stackid():
    cfg = Configuration(**_tier1_params())
    assert cfg.resolve_service_name("connection.keboola.com") == "keboola-connection-keboola-com"
    assert cfg.resolve_service_name(None) == "keboola"


def test_service_name_override_wins():
    cfg = Configuration(**_tier1_params(service_name="my-service"))
    assert cfg.resolve_service_name("connection.keboola.com") == "my-service"


def test_storage_token_row_field_alias():
    cfg = Configuration(**_tier1_params(**{"#storage_token": "row-token"}))
    assert cfg.storage_token == "row-token"
