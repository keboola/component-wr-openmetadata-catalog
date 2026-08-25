"""VCR functional tests for keboola.wr-openmetadata-catalog.

Discovers the recorded cassettes under ``tests/functional/`` and replays each one
through the datadirtest VCR harness. Until cassettes are recorded (the Phase-5
recording step) ``tests/functional/`` is absent, so ``get_test_cases`` returns
``[]``, this module collects zero cases, and the unit/orchestrator suite stays
green on its own.

Recording is done with the ``keboola.datadirtest`` scaffolder (never by hand) —
see ``tests/setup/README.md`` for the exact command, the ``secrets.json`` shape,
and the per-case record notes.
"""

import json
import unittest
from pathlib import Path

import pytest
from keboola.datadirtest.vcr import VCRDataDirTester, VCRTestDataDir, get_test_cases

# Incremental-skip case whose cassette was recorded with chained state (05's
# out/state.json seeds 06's in/state.json). ``keboola.datadirtest`` wipes a
# non-chained test's in/state.json to ``{}`` at replay setUp, which would turn
# this skip run into a full reprocess (nothing matches the recorded skip
# traffic). We re-seed the committed incremental state via the library's own
# ``last_state_override`` so the harness seeds instead of wiping. See
# tests/setup/README.md ("Incremental-skip state override").
_STATE_OVERRIDE_CASE = "06_run_catalog_incremental_second_run"

FUNCTIONAL_DIR = str(Path(__file__).parent / "functional")
COMPONENT_SCRIPT = str(Path(__file__).parent.parent / "src" / "component.py")

# Frozen clock so the ``timestamp`` (catalog_run_report) and ``updated_at``
# (last_written_snapshot) columns are identical between recording and replay.
# MUST equal the ``--freeze-time`` value passed to the scaffolder at record time.
FREEZE_TIME = "2026-08-24T12:00:00"

# Deterministic, NON-SECRET Keboola platform env the component reads in
# ``Component._read_environment()``. These MUST be identical at record time
# (exported in the shell before ``scaffold``) and at replay time (set here), or
# the recorded request URIs / entity FQNs / report run_id will not match on
# replay. Real secrets never live here: they come from ``secrets.json`` at record
# time and the scaffolder masks them back out of the committed ``config.json``.
#
# KBC_URL is both the Storage API base and the VCR match host — set it to the
# SAME stack the scratch recording project lives on (default: the US multi-tenant
# stack). KBC_STACKID feeds the OM DatabaseService FQN root, and KBC_RUNID feeds
# the report primary key, so both must stay fixed.
KBC_ENV = {
    "KBC_URL": "https://connection.us-east4.gcp.keboola.com",
    "KBC_STACKID": "connection.us-east4.gcp.keboola.com",
    "KBC_PROJECTNAME": "cf-openmetadata-catalog-test",
    "KBC_RUNID": "keboola-om-catalog-vcr",
    "KBC_COMPONENTID": "keboola.wr-openmetadata-catalog",
    "KBC_CONFIGID": "vcr-test",
    # Replay-only dummy (the auth header is stripped from every cassette). At
    # record time export a REAL read-only Storage token for the forward_token
    # cases (16, and the 20 degrade fallback).
    "KBC_TOKEN": "dummy-storage-token-replay",
}


@pytest.mark.parametrize("test_name", get_test_cases(FUNCTIONAL_DIR))
def test_functional(test_name, monkeypatch):
    """Replay a single recorded VCR functional case."""
    for key, value in KBC_ENV.items():
        monkeypatch.setenv(key, value)
    # A non-row run: config_row_id is recorded as null; the data-type gate is off.
    monkeypatch.delenv("KBC_CONFIGROWID", raising=False)
    monkeypatch.delenv("KBC_DATA_TYPE_SUPPORT", raising=False)

    if test_name == _STATE_OVERRIDE_CASE:
        # Seed the committed incremental state (the 64 project-4214 bucket
        # digests) instead of letting the harness wipe it, so the recorded
        # skipped_unchanged run replays as recorded.
        test_dir = str(Path(FUNCTIONAL_DIR) / test_name)
        seed_state = json.loads((Path(test_dir) / "source" / "data" / "in" / "state.json").read_text())
        test = VCRTestDataDir(
            data_dir=test_dir,
            component_script=COMPONENT_SCRIPT,
            last_state_override=seed_state,
            vcr_freeze_time=FREEZE_TIME,
        )
        result = unittest.TextTestRunner(verbosity=2).run(unittest.TestSuite([test]))
        assert result.wasSuccessful(), f"{test_name} VCR replay failed: {result.errors + result.failures}"
        return

    tester = VCRDataDirTester(
        data_dir=FUNCTIONAL_DIR,
        component_script=COMPONENT_SCRIPT,
        selected_tests=[test_name],
        vcr_freeze_time=FREEZE_TIME,
    )
    tester.run()
