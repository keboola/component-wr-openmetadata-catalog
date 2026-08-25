"""Bucket-scoping tests for the catalog pass (MINOR fix: never catalog our own bucket)."""

import report as report_mod
from client.storage_reader import SourceBucket
from component import Component
from configuration import Configuration

_TIER1 = {"om_host": "https://om.example.com", "#bot_token": "jwt-abc"}


def _bucket(bucket_id: str, stage: str = "in") -> SourceBucket:
    return SourceBucket(id=bucket_id, name=bucket_id, stage=stage)


def test_own_output_bucket_is_excluded_by_default():
    """The component's own bookkeeping bucket is never in scope, even with the
    default (all-in/out, no allow/denylist) config -- so the writer never
    catalogs its own report/snapshot tables."""
    cfg = Configuration(**_TIER1)
    assert cfg.bucket_denylist == []  # excluded by rule, not by user config
    own = _bucket(report_mod.OUTPUT_BUCKET)
    assert Component._bucket_in_scope(cfg, own) is False


def test_regular_in_bucket_is_in_scope():
    """A normal in-stage bucket remains in scope under the default config."""
    cfg = Configuration(**_TIER1)
    assert Component._bucket_in_scope(cfg, _bucket("in.c-sales")) is True
