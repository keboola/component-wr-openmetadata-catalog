"""Column-lineage coverage CI gate (E17, spec 9, plan T14).

Runs the resolution taxonomy over a fixed, identity-scrubbed SQL corpus and
fails if the resolved fraction regresses below the pinned threshold or any case
silently drops resolved -> no_upstream/unresolved.
"""

import json
import pathlib
from collections import Counter

import pytest

from lineage.column_lineage import extract_column_lineage
from lineage.dialect import dialect_for

_CORPUS_PATH = pathlib.Path(__file__).parent.parent / "lineage_corpus" / "corpus.json"
_CORPUS = json.loads(_CORPUS_PATH.read_text())


def _run_case(case: dict):
    dialect = dialect_for(component_id=case["component_id"])
    result = extract_column_lineage(
        [tuple(s) for s in case["statements"]],
        in_map=case["in_map"],
        out_map=case["out_map"],
        dialect=dialect,
    )
    return {str(k): v for k, v in result.metrics.counts.items()}


@pytest.mark.coverage_gate
def test_column_lineage_coverage_gate():
    aggregate: Counter = Counter()
    for case in _CORPUS["cases"]:
        actual = _run_case(case)
        expected_resolved = case["expected"].get("resolved", 0)
        assert actual.get("resolved", 0) >= expected_resolved, (
            f"{case['name']}: resolved regressed {actual} < baseline {case['expected']}"
        )
        aggregate.update(actual)

    resolved = aggregate["resolved"]
    traceable = resolved + aggregate["needs_schema"] + aggregate["unresolved"]
    fraction = resolved / traceable if traceable else 1.0
    assert fraction >= _CORPUS["threshold"], (
        f"column-lineage coverage {fraction:.3f} regressed below threshold {_CORPUS['threshold']}"
    )


def test_gate_detects_a_deliberate_regression():
    """A broken case (expected resolved higher than achievable) is caught."""
    broken = {
        "component_id": "keboola.snowflake-transformation",
        "in_map": {"src": "in.c-main.orders"},
        "out_map": {"result": "out.c-out.result"},
        # a literal-only projection resolves nothing, but we claim 5 resolved
        "statements": [["c1", 'INSERT INTO "result" SELECT 1 AS "x" FROM "src"']],
        "expected": {"resolved": 5},
    }
    actual = _run_case(broken)
    assert actual.get("resolved", 0) < broken["expected"]["resolved"]
