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

from pathlib import Path

import pytest
from keboola.datadirtest.vcr import VCRDataDirTester, get_test_cases

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
    "KBC_URL": "https://connection.keboola.com",
    "KBC_STACKID": "connection.keboola.com",
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

    tester = VCRDataDirTester(
        data_dir=FUNCTIONAL_DIR,
        component_script=COMPONENT_SCRIPT,
        selected_tests=[test_name],
        vcr_freeze_time=FREEZE_TIME,
    )
    tester.run()
