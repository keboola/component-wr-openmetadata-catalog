"""Sync actions and run-level helpers: testConnection, custom properties, service renames."""

from types import SimpleNamespace
from unittest import mock

import pytest
from keboola.component.exceptions import UserException

import component as component_mod
from client.om_client import OMConnectionError
from configuration import Configuration, CustomPropertiesMode


def _component(parameters: dict) -> component_mod.Component:
    # ``configuration`` is a read-only property on ComponentBase; shadow it on a subclass.
    # action="run" makes the sync-action wrapper return / raise instead of writing to
    # stdout and exiting, so the action is testable in-process.
    configuration = SimpleNamespace(parameters=parameters, action="run")
    comp_cls = type("_Component", (component_mod.Component,), {"configuration": configuration})
    return comp_cls.__new__(comp_cls)  # bypass ComponentBase.__init__


_SSH_PARAMS = {
    "om_host": "https://om.internal",
    "#bot_token": "jwt",
    "use_ssh_tunnel": True,
    "ssh": {"host": "bastion.example", "user": "kbc", "#private_key": "KEY"},
}


def test_test_connection_goes_through_the_ssh_tunnel_when_enabled():
    proxy = mock.Mock(local_om_host="https://127.0.0.1:40000")
    om = mock.Mock(server_version="1.13.4")
    with (
        mock.patch.object(component_mod.ssh_proxy, "maybe_open_tunnel", return_value=proxy) as open_tunnel,
        mock.patch.object(component_mod, "OMClient", return_value=om) as om_cls,
        mock.patch.object(component_mod, "StorageReader"),
        mock.patch.object(component_mod, "resolve_storage_credentials", return_value=("t", "https://s")),
    ):
        _component(_SSH_PARAMS).test_connection()
    open_tunnel.assert_called_once()
    assert om_cls.call_args.args[0] == "https://127.0.0.1:40000"
    proxy.close.assert_called_once()


def test_test_connection_unreachable_host_says_the_tunnel_is_off():
    om = mock.Mock()
    om.probe_version.side_effect = OMConnectionError("Could not reach OpenMetadata at https://om.internal/api/v1")
    with (
        mock.patch.object(component_mod, "OMClient", return_value=om),
        pytest.raises(UserException) as exc,
    ):
        _component({"om_host": "https://om.internal", "#bot_token": "jwt"}).test_connection()
    assert "SSH tunnel is OFF" in str(exc.value)
    assert "static egress IP" in str(exc.value)


def test_test_connection_unreachable_through_tunnel_names_the_bastion():
    om = mock.Mock()
    om.probe_version.side_effect = OMConnectionError("Could not reach OpenMetadata")
    with (
        mock.patch.object(component_mod.ssh_proxy, "maybe_open_tunnel", return_value=mock.Mock(local_om_host=None)),
        mock.patch.object(component_mod, "OMClient", return_value=om),
        pytest.raises(UserException) as exc,
    ):
        _component(_SSH_PARAMS).test_connection()
    assert "SSH tunnel is ON" in str(exc.value)
    assert "bastion.example:22" in str(exc.value)


@pytest.mark.parametrize(
    ("mode", "expected_create"),
    [(CustomPropertiesMode.EXISTING_ONLY, False), (CustomPropertiesMode.CREATE, True)],
)
def test_custom_properties_mode_controls_type_definition(mode, expected_create):
    om = mock.Mock()
    om.ensure_custom_properties.return_value = {"kbcProjectId"}
    out = component_mod.Component._ensure_custom_properties(om, "table", [], "Table", mode)
    assert out == {"kbcProjectId"}
    assert om.ensure_custom_properties.call_args.kwargs == {"create": expected_create}


def test_custom_properties_off_writes_no_extension_and_calls_nothing():
    om = mock.Mock()
    assert (
        component_mod.Component._ensure_custom_properties(om, "table", [], "Table", CustomPropertiesMode.OFF) == set()
    )
    om.ensure_custom_properties.assert_not_called()


def test_configuration_from_ssh_params_is_valid():
    assert Configuration(**_SSH_PARAMS).use_ssh_tunnel is True


# ------------------------------------------------ renamed per-project services


def _run(project_name="L0 Events", project_id="592"):
    from mapping.dashboard_builder import DashboardBuilder
    from mapping.entity_builder import EntityBuilder
    from mapping.pipeline_builder import PipelineBuilder

    return component_mod._ProjectRun(
        ctx=component_mod.ProjectContext(
            project_id=project_id, project_name=project_name, storage_token="t", storage_url="https://s"
        ),
        reader=None,  # ty: ignore[invalid-argument-type]  (unused here)
        entities=EntityBuilder("keboola", project_name, project_id, "https://ui"),
        pipelines=PipelineBuilder("keboola", project_name, project_id, "https://ui"),
        dashboards=DashboardBuilder("keboola", project_name, project_id, "https://ui"),
    )


def test_legacy_project_id_service_is_soft_deleted_once_renamed():
    om = mock.Mock()
    om.get_by_fqn.return_value = {"id": "svc-old", "description": "Pipelines of Keboola project 592, catalogued by x"}
    report = component_mod.RunReport(run_id="rid")
    component_mod.Component._retire_legacy_service(
        om, _run(), report, "pipelineServices", "PipelineService", "keboola-L0_Events"
    )
    om.get_by_fqn.assert_called_once_with("pipelineServices", "keboola-592")
    om.soft_delete.assert_called_once_with("pipelineServices", "svc-old", recursive=True)
    assert [r["action"] for r in report.rows()] == ["tombstoned"]


def test_absent_legacy_service_is_left_alone():
    om = mock.Mock()
    om.get_by_fqn.return_value = None
    report = component_mod.RunReport(run_id="rid")
    component_mod.Component._retire_legacy_service(
        om, _run(), report, "dashboardServices", "DashboardService", "keboola-L0_Events"
    )
    om.soft_delete.assert_not_called()
    assert report.rows() == []


@pytest.mark.parametrize(
    ("as_node", "table_lineage", "expected"),
    [(False, True, False), (True, True, True), (False, False, True)],
)
def test_pipelines_stay_nodes_when_no_table_edge_can_carry_them(as_node, table_lineage, expected):
    config = SimpleNamespace(pipeline_lineage_as_node=as_node, write_table_lineage=table_lineage)
    assert component_mod._pipelines_as_lineage_nodes(config) is expected  # ty: ignore[invalid-argument-type]


def test_same_named_service_of_another_project_is_not_retired():
    om = mock.Mock()
    om.get_by_fqn.return_value = {"id": "svc-other", "description": "Pipelines of Keboola project 77, catalogued by x"}
    component_mod.Component._retire_legacy_service(
        om, _run(), component_mod.RunReport(run_id="rid"), "pipelineServices", "PipelineService", "keboola-L0_Events"
    )
    om.soft_delete.assert_not_called()
