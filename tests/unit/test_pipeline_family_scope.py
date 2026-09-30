"""Object-family gating/scoping tests, mirroring ``test_bucket_scope.py``.

Covers ``Component._pipeline_config_in_scope`` (the flow/transformation/component
family checkbox + selector) and ``Component._data_app_in_scope`` (the data-apps
selector), against a real ``Configuration`` model rather than a duck-typed double.
"""

from component import Component
from configuration import Configuration

_BASE = {"om_host": "https://om.example.com", "#bot_token": "jwt-abc"}


# --------------------------------------------------------------------- transformations


def test_transformation_in_scope_by_default():
    cfg = Configuration(**_BASE)
    assert Component._pipeline_config_in_scope(cfg, "transformation", "t1") is True


def test_transformation_family_off_excludes_every_transformation():
    cfg = Configuration(**_BASE, write_transformations=False)
    assert Component._pipeline_config_in_scope(cfg, "transformation", "t1") is False


def test_transformation_selector_restricts_to_subset():
    cfg = Configuration(**_BASE, transformations=["t1"])
    assert Component._pipeline_config_in_scope(cfg, "transformation", "t1") is True
    assert Component._pipeline_config_in_scope(cfg, "transformation", "t2") is False


def test_transformation_selector_does_not_affect_other_families():
    # A `transformations` selector never narrows extractor/writer/flow eligibility.
    cfg = Configuration(**_BASE, transformations=["t1"])
    assert Component._pipeline_config_in_scope(cfg, "extractor", "e1") is True
    assert Component._pipeline_config_in_scope(cfg, "orchestration", "f1") is True


# --------------------------------------------------------------------- components (extractor/writer/...)


def test_component_family_off_excludes_every_component_kind():
    cfg = Configuration(**_BASE, write_components=False)
    assert Component._pipeline_config_in_scope(cfg, "extractor", "e1") is False
    assert Component._pipeline_config_in_scope(cfg, "writer", "w1") is False
    assert Component._pipeline_config_in_scope(cfg, "application", "a1") is False
    assert Component._pipeline_config_in_scope(cfg, "other", "o1") is False


def test_component_selector_restricts_to_subset():
    cfg = Configuration(**_BASE, components=["e1"])
    assert Component._pipeline_config_in_scope(cfg, "extractor", "e1") is True
    assert Component._pipeline_config_in_scope(cfg, "writer", "w1") is False


# --------------------------------------------------------------------- flows


def test_flow_family_off_excludes_every_flow():
    cfg = Configuration(**_BASE, write_flows=False)
    assert Component._pipeline_config_in_scope(cfg, "orchestration", "f1") is False


def test_flow_selector_restricts_to_subset():
    cfg = Configuration(**_BASE, flows=["f1"])
    assert Component._pipeline_config_in_scope(cfg, "orchestration", "f1") is True
    assert Component._pipeline_config_in_scope(cfg, "orchestration", "f2") is False


# --------------------------------------------------------------------- data apps


def test_data_app_in_scope_by_default():
    cfg = Configuration(**_BASE)
    assert Component._data_app_in_scope(cfg, "01app") is True


def test_data_app_selector_restricts_to_subset():
    cfg = Configuration(**_BASE, data_apps=["01app"])
    assert Component._data_app_in_scope(cfg, "01app") is True
    assert Component._data_app_in_scope(cfg, "02app") is False
