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
    default (empty ``buckets`` selector = all) config -- so the writer never
    catalogs its own report/snapshot tables."""
    cfg = Configuration(**_TIER1)
    assert cfg.buckets == []  # excluded by rule, not by user config
    own = _bucket(report_mod.OUTPUT_BUCKET)
    assert Component._bucket_in_scope(cfg, own) is False


def test_regular_in_bucket_is_in_scope():
    """A normal in-stage bucket remains in scope under the default config."""
    cfg = Configuration(**_TIER1)
    assert Component._bucket_in_scope(cfg, _bucket("in.c-sales")) is True


def test_empty_buckets_selector_includes_all_buckets():
    """An empty ``buckets`` selector means 'all buckets' (both in/out stage)."""
    cfg = Configuration(**_TIER1)
    assert Component._bucket_in_scope(cfg, _bucket("in.c-sales")) is True
    assert Component._bucket_in_scope(cfg, _bucket("out.c-sales", stage="out")) is True


def test_non_empty_buckets_selector_restricts_to_subset():
    """A non-empty ``buckets`` selector is an explicit allowlist."""
    cfg = Configuration(**_TIER1, buckets=["in.c-sales"])
    assert Component._bucket_in_scope(cfg, _bucket("in.c-sales")) is True
    assert Component._bucket_in_scope(cfg, _bucket("in.c-other")) is False


def test_own_output_bucket_excluded_even_when_selected():
    """The bookkeeping bucket is excluded even if a user explicitly lists it."""
    cfg = Configuration(**_TIER1, buckets=[report_mod.OUTPUT_BUCKET])
    own = _bucket(report_mod.OUTPUT_BUCKET)
    assert Component._bucket_in_scope(cfg, own) is False
