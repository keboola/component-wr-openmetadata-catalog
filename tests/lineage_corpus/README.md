# Column-lineage coverage corpus (E17, spec 9 / plan T14)

`corpus.json` is a fixed set of **synthetic, identity-scrubbed** SQL
transformations (no customer identifiers) seeded from the shape of the
`lineage-spike/corpus_run.py` PoC. Each case declares its dialect, storage
input/output mapping, ordered SQL statements, and the **baseline** resolution
taxonomy (`expected`) produced by `src/lineage/column_lineage.py`.

The CI gate `tests/unit/test_coverage_gate.py::test_column_lineage_coverage_gate`
runs the extractor over the corpus and fails if:

- the aggregate `resolved` fraction regresses below the pinned `threshold`, or
- any case's `resolved` count silently drops below its baseline (the CTAS
  under-report landmine, research §7 Blocker 5).

To extend coverage, add a case (keep names generic — no customer identity) and
refresh its `expected` block from a green run.
