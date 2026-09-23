"""Object-family gating/scoping tests, mirroring ``test_bucket_scope.py``.

Covers ``Component._pipeline_config_in_scope`` (the flow/transformation/component
family checkbox + selector) and ``Component._data_app_in_scope`` (the data-apps
selector), against a real ``Configuration`` model rather than a duck-typed double.
"""

from component import Component
from configuration import Configuration, ProjectScope

_TIER1 = {"om_host": "https://om.example.com", "#bot_token": "jwt-abc"}


# --------------------------------------------------------------------- transformations


def test_transformation_in_scope_by_default():
    cfg = Configuration(**_TIER1)
    assert Component._pipeline_config_in_scope(cfg, "transformation", "t1") is True


def test_transformation_family_off_excludes_every_transformation():
    cfg = Configuration(**_TIER1, write_transformations=False)
    assert Component._pipeline_config_in_scope(cfg, "transformation", "t1") is False


def test_transformation_selector_restricts_to_subset():
    cfg = Configuration(**_TIER1, transformations=["t1"])
    assert Component._pipeline_config_in_scope(cfg, "transformation", "t1") is True
    assert Component._pipeline_config_in_scope(cfg, "transformation", "t2") is False


def test_transformation_selector_does_not_affect_other_families():
    # A `transformations` selector never narrows extractor/writer/flow eligibility.
    cfg = Configuration(**_TIER1, transformations=["t1"])
    assert Component._pipeline_config_in_scope(cfg, "extractor", "e1") is True
    assert Component._pipeline_config_in_scope(cfg, "orchestration", "f1") is True


# --------------------------------------------------------------------- components (extractor/writer/...)


def test_component_family_off_excludes_every_component_kind():
    cfg = Configuration(**_TIER1, write_components=False)
    assert Component._pipeline_config_in_scope(cfg, "extractor", "e1") is False
    assert Component._pipeline_config_in_scope(cfg, "writer", "w1") is False
    assert Component._pipeline_config_in_scope(cfg, "application", "a1") is False
    assert Component._pipeline_config_in_scope(cfg, "other", "o1") is False


def test_component_selector_restricts_to_subset():
    cfg = Configuration(**_TIER1, components=["e1"])
    assert Component._pipeline_config_in_scope(cfg, "extractor", "e1") is True
    assert Component._pipeline_config_in_scope(cfg, "writer", "w1") is False


# --------------------------------------------------------------------- flows


def test_flow_family_off_excludes_every_flow():
    cfg = Configuration(**_TIER1, write_flows=False)
    assert Component._pipeline_config_in_scope(cfg, "orchestration", "f1") is False


def test_flow_selector_restricts_to_subset():
    cfg = Configuration(**_TIER1, flows=["f1"])
    assert Component._pipeline_config_in_scope(cfg, "orchestration", "f1") is True
    assert Component._pipeline_config_in_scope(cfg, "orchestration", "f2") is False


# --------------------------------------------------------------------- all_projects: selectors hidden


def test_all_projects_scope_ignores_every_selector():
    # An org-wide row's selector ids can't map across projects, so all_projects
    # writes every family-enabled config regardless of the (hidden) selector.
    cfg = Configuration(
        **_TIER1,
        scope="all_projects",
        **{"#manage_token": "manage-xyz"},
        organization_id="123",
        transformations=["only-this-one"],
    )
    assert cfg.scope == ProjectScope.ALL_PROJECTS
    assert Component._pipeline_config_in_scope(cfg, "transformation", "some-other-id") is True
    # ...but the family checkbox still gates, even under all_projects.
    cfg_off = Configuration(
        **_TIER1,
        scope="all_projects",
        **{"#manage_token": "manage-xyz"},
        organization_id="123",
        write_transformations=False,
    )
    assert Component._pipeline_config_in_scope(cfg_off, "transformation", "t1") is False


# --------------------------------------------------------------------- data apps


def test_data_app_in_scope_by_default():
    cfg = Configuration(**_TIER1)
    assert Component._data_app_in_scope(cfg, "01app") is True


def test_data_app_selector_restricts_to_subset():
    cfg = Configuration(**_TIER1, data_apps=["01app"])
    assert Component._data_app_in_scope(cfg, "01app") is True
    assert Component._data_app_in_scope(cfg, "02app") is False


def test_data_app_all_projects_scope_ignores_selector():
    cfg = Configuration(
        **_TIER1,
        scope="all_projects",
        **{"#manage_token": "manage-xyz"},
        organization_id="123",
        data_apps=["only-this-one"],
    )
    assert Component._data_app_in_scope(cfg, "some-other-app") is True
