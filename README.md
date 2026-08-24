keboola.wr-openmetadata-catalog
===============================

A push-based **writer** that reads a Keboola project's metadata (buckets, tables,
columns, native datatypes, primary keys, descriptions, sharing/linkage, the
producing configs' storage mapping and transformation SQL, flows/orchestrations,
and job run history) and pushes a catalog, pipelines, and lineage into an
[OpenMetadata](https://open-metadata.org/) tenant via the OpenMetadata REST API.

It is an atypical writer: its "input" is the whole project's metadata pulled over
the Storage / Configurations / Job Queue APIs (not Keboola input-table mapping),
and its "output" is OpenMetadata entities plus two small Keboola report tables.

What it writes to OpenMetadata
==============================

- **Catalog** — DatabaseService (stack) → Database (project) → DatabaseSchema
  (bucket) → Table (Regular / View / External) with columns, native datatypes,
  primary keys, descriptions, and deep links back into the Keboola UI.
- **Pipelines** — component configs and flows/orchestrations → Pipeline + Tasks
  (`taskSQL`, `downstreamTasks`), plus run history → pipeline status.
- **Lineage** — declared table-level edges (`source=PipelineLineage`) and
  column-level edges computed with SQLGlot (`source=QueryLineage`), chained
  through workspace intermediates.
- **Three-way merge** — the component only touches fields it authored; a field a
  human edited in OpenMetadata is left alone (unless `merge_mode=keboola_always_wins`).

**OM target:** stable **1.13.4** is the guaranteed surface; three 2.0-only
features (`deleteStale`, bulk `?overrideMetadata=`, delete-lineage-by-source-name)
are probe-gated via `GET /api/v1/system/version` with 1.13.4 fallbacks.

Authentication
==============

- **OpenMetadata:** a JWT bot token (`#bot_token`), `Authorization: Bearer`.
- **Keboola Storage:** the host project uses the forwarded token; additional
  projects use a per-row read-only `#storage_token` (Tier-1) or a single
  `#manage_token` that enumerates the org and mints short-lived read-only tokens
  (Tier-2, degrades to the host project when the token lacks scope).

Configuration
=============

Root config holds the OpenMetadata connection and global behaviour; each config
row is one Keboola project. See `component_config/configSchema.json` and
`component_config/configRowSchema.json`. Sync actions: **testConnection** and
**listBuckets**.

Output tables
=============

- `catalog_run_report` — one row per entity action (`created` / `updated` /
  `skipped_unchanged` / `skipped_diverged` / `tombstoned` / `unresolved` /
  `degraded` / `failed`); `write_always`, so it survives a failing job.
- `last_written_snapshot` — the three-way-merge base (last-written fields per FQN).

Development
===========

Python 3.14, `uv`, `ruff`. Run the suite and lint:

```
uv sync --all-groups
uv run ruff check src/ tests/
uv run pytest tests/
```

The **column-lineage coverage gate** (`tests/unit/test_coverage_gate.py`, marker
`coverage_gate`) runs the SQLGlot resolution taxonomy over
`tests/lineage_corpus/corpus.json` and fails if the resolved fraction regresses
below the pinned threshold or any case silently drops `resolved`. It runs as part
of the standard `pytest tests/` suite, so CI enforces it — run it in isolation with
`uv run pytest -m coverage_gate`.

Integration
===========

For deployment and integration with Keboola, see the
[developer documentation](https://developers.keboola.com/extend/component/deployment/).
