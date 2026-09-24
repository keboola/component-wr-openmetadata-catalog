from sync import (
    StateManager,
    TombstonePlanner,
    bucket_digest,
    should_process_bucket,
)


def test_bucket_digest_stable_and_sensitive():
    a = bucket_digest({"id": "b"}, [{"name": "t", "cols": ["x"]}])
    b = bucket_digest({"id": "b"}, [{"name": "t", "cols": ["x"]}])
    c = bucket_digest({"id": "b"}, [{"name": "t", "cols": ["x", "y"]}])
    assert a == b
    assert a != c


def test_unchanged_digest_skips():
    assert (
        should_process_bucket(
            previous_digest="d1",
            current_digest="d1",
            full_refresh=False,
            version_changed=False,
            full_refresh_due=False,
        )
        is False
    )


def test_changed_digest_reprocesses():
    assert (
        should_process_bucket(
            previous_digest="d1",
            current_digest="d2",
            full_refresh=False,
            version_changed=False,
            full_refresh_due=False,
        )
        is True
    )


def test_full_refresh_ignores_digests():
    assert (
        should_process_bucket(
            previous_digest="d1",
            current_digest="d1",
            full_refresh=True,
            version_changed=False,
            full_refresh_due=False,
        )
        is True
    )


def test_version_change_forces_reprocess():
    assert (
        should_process_bucket(
            previous_digest="d1",
            current_digest="d1",
            full_refresh=False,
            version_changed=True,
            full_refresh_due=False,
        )
        is True
    )


def test_state_manager_tier2_per_project_keying():
    sm = StateManager({"projects": {"p1": {"bucket_digests": {"in.c-a": "d1"}}}, "run_count": 5})
    assert sm.bucket_digest("p1", "in.c-a") == "d1"
    assert sm.bucket_digest("p2", "in.c-a") is None
    sm.set_bucket_digest("p2", "out.c-b", "d9")
    assert sm.to_dict()["projects"]["p2"]["bucket_digests"]["out.c-b"] == "d9"
    assert sm.run_count == 5


def test_state_manager_flat_backcompat():
    sm = StateManager({"bucket_digests": {"in.c-a": "d1"}})
    assert sm.bucket_digest("_default", "in.c-a") == "d1"


def test_full_refresh_due_cadence():
    assert StateManager({"run_count": 20}).full_refresh_due(every=20) is True
    assert StateManager({"run_count": 21}).full_refresh_due(every=20) is False


def test_advance_after_success_semantics_preserve_prior_on_failure():
    # A digest is only set after success; if we never call set, the prior stays.
    sm = StateManager({"projects": {"p1": {"bucket_digests": {"in.c-a": "old"}}}})
    # simulate a failed bucket: do not set new digest
    assert sm.bucket_digest("p1", "in.c-a") == "old"


def test_tombstone_deletestale_body():
    body = TombstonePlanner.deletestale_body("svc.p", "database", ["svc.p.b.t1", "svc.p.b.t2"], dry_run=True)
    assert body["scopeFqn"] == "svc.p"
    assert body["dryRun"] is True
    assert body["seenFqns"] == ["svc.p.b.t1", "svc.p.b.t2"]


def test_tombstone_self_diff_finds_stale():
    plan = TombstonePlanner.self_diff(
        listed_fqns=["svc.p.b.t1", "svc.p.b.t2", "svc.p.b.stale"],
        seen_fqns=["svc.p.b.t1", "svc.p.b.t2"],
    )
    assert plan.blocked is False
    assert plan.to_delete == ["svc.p.b.stale"]


def test_tombstone_fails_closed_on_missing_listing():
    plan = TombstonePlanner.self_diff(listed_fqns=None, seen_fqns=["svc.p.b.t1"])
    assert plan.blocked is True
    assert plan.to_delete == []


def test_tombstone_fails_closed_on_short_scope():
    plan = TombstonePlanner.self_diff(listed_fqns=[], seen_fqns=["svc.p.b.t1"], min_scope=1)
    assert plan.blocked is True
