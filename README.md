keboola.wr-openmetadata-catalog
===============================

A push-based **writer** that reads a Keboola project's metadata (buckets, tables,
columns, native datatypes, primary keys, descriptions, sharing/linkage, the
producing configs' storage mapping and transformation SQL, flows/orchestrations,
data apps, and job run history) and pushes a catalog, pipelines, dashboards, and
lineage into an [OpenMetadata](https://open-metadata.org/) instance via the
OpenMetadata REST API.

It is an atypical writer: its "input" is the whole project's metadata pulled over
the Storage / Configurations / Job Queue APIs (not Keboola input-table mapping),
and its "output" is OpenMetadata entities plus two small Keboola report tables.

Setup
=====

1. **OpenMetadata bot token.** In OpenMetadata open *Settings → Bots*, pick (or
   create) a bot and generate a token. A dedicated bot with a long expiry is
   better than the shared `ingestion-bot`.
2. **Network path.** The OpenMetadata host must be reachable from the Keboola
   stack. Either allow-list Keboola's static egress IP addresses on the
   OpenMetadata side, or turn on **Use SSH Tunnel** and give a bastion host that
   can reach OpenMetadata.
3. **Configuration.** Create a configuration and fill in **OpenMetadata Host**,
   **OpenMetadata Token** and, if needed, the SSH tunnel. Click **Test Connection**.
   The test uses the same network path as the job (through the bastion when the
   tunnel is on) and says which path failed.
4. **One row per project.** Add a configuration row for each Keboola project to
   catalog:
   - leave **Storage Token** empty to catalog the project the configuration lives in, or
   - paste a read-only **Storage** token of another project on the same stack
     (*Project Settings → API Tokens* in that project).

   No organization-level or master token is needed. Management API tokens do not
   work here — the writer reads Storage.

What it writes to OpenMetadata
==============================

- **Catalog** — DatabaseService (`keboola`, shown as "Keboola"; override with Service
  Name) → Database (project) → DatabaseSchema (bucket) → Table with columns, native
  datatypes, primary keys, descriptions, and deep links back into the Keboola UI.
  Table type: **Regular**; **View** for a table of a linked (shared-in) bucket or a
  table alias; **External** for an external bucket.
- **Pipelines** — transformations, components (extractors/writers/apps) and
  flows/orchestrations → Pipeline + Tasks (`taskSQL`, `downstreamTasks`), plus run
  history → pipeline status. One PipelineService per project, named
  `<service>-<project name>` (for example `keboola-L0_Events`), so a pipeline's FQN
  and URL show which project it belongs to.
- **Dashboards** — Keboola data apps (`keboola.data-apps`) → Dashboard entities under
  one `CustomDashboard` service per project (named like the pipeline service). The
  dashboard's **Project** field is the Keboola project name.
- **Lineage** — see the coverage table below.
- **Owners** — native OpenMetadata `owners` when the creator's e-mail matches an
  OpenMetadata user.
- **Custom properties** (optional) — typed `kbc*` properties (ids, deep links, last
  change, owner e-mail) on Tables, Schemas, Databases, Pipelines and Dashboards.

Lineage coverage
----------------

Each table lineage edge connects two tables directly. The pipeline that produces the
output (a transformation, an application, …) is shown **on the edge**, so the
OpenMetadata lineage view keeps column-level lineage visible and still says which
pipeline made the change. An extractor (output tables only) or a writer (input
tables only) has no table → table edge, so it is shown as a node: extractor →
table, or table → writer.

| Lineage | Captured | Source of truth |
|---|---|---|
| Table → table inside a project | Yes | Input/output mapping; for SQL transformations the parsed SQL narrows each output to the inputs it really reads |
| Column → column | Yes, SQL transformations (Snowflake, BigQuery, …) | SQL parsed with SQLGlot; unresolved columns are listed in `catalog_run_report` |
| SQL that reads a table without an input mapping | Yes, for Snowflake-style `"stage.c-bucket"."table"` references to tables cataloged in the same run | Parsed SQL |
| Shared / linked bucket → source project | Yes, as a View with 1:1 column lineage, when the source project is cataloged into the same service (a row for it, with no Project Name Override) | Bucket link metadata |
| Bucket → bucket | Yes, aggregated from table lineage | — |
| Flow → its tasks | Yes, pipeline → pipeline | Flow definition |
| Table → data app | Yes | Data app input mapping |
| Data app code / Git-hosted apps | No — the writer cannot read app code | — |
| External bucket → BigQuery / Snowflake source table | Not yet | — |
| Source system → extractor output (e.g. MySQL table → Keboola table) | Not yet | — |
| BigQuery fully-qualified names in SQL (`project.dataset.table`) without an input mapping | Not yet | — |

Settings reference
==================

Configuration (shared by all rows)
----------------------------------

| Setting | What it does |
|---|---|
| OpenMetadata Host | Base URL of the OpenMetadata instance; the writer appends `/api/v1`. |
| OpenMetadata Token | Bot JWT token (see Setup). |
| Use SSH Tunnel / SSH Tunnel | Reach OpenMetadata through a bastion: host, user, port, private key. |
| Service Name | Root of every FQN. Empty = `keboola` (shown as "Keboola"). Set it only when several Keboola stacks write into one OpenMetadata instance. Changing it starts a new tree. |
| Custom Properties | **Fill only properties already defined** (default): values are written only for `kbc*` properties an OpenMetadata admin has defined. **Define missing properties**: the bot creates them (needs permission to edit entity types). **Do not write**: no custom properties. An OpenMetadata custom property is defined per entity type, so it shows on *every* table, schema, pipeline or dashboard in the instance — including BigQuery, MySQL and other services. That is why the writer does not create them unless asked. |

Row (one Keboola project)
-------------------------

| Setting | What it does |
|---|---|
| Storage Token | Empty = the configuration's own project; otherwise a read-only Storage token of another project on the same stack. |
| Project Name Override | Name of the project's Database (and of its pipeline/dashboard services) in OpenMetadata. Default: the Keboola project name. |
| Objects | Which families to catalog — buckets, transformations, components, flows, data apps — each with an optional "only these" selector (empty = all). |
| Advanced → Lineage | One switch per lineage type: bucket, table, column, pipeline (pipeline on the edge + flow → task lineage), dashboard. **Pipeline as lineage node (legacy)** also draws each pipeline as its own node between the tables; off by default. |
| Write Pipeline Status | Push each pipeline's last job result as OpenMetadata pipeline status. |
| Full Refresh | Ignore the change checks and re-push every bucket and every lineage edge this run. |
| Merge Mode | **Three-way merge** (default): the writer remembers what it wrote last time (the `last_written_snapshot` table). If a person changed a field in OpenMetadata since then — for example a description — the writer keeps the person's value and marks the entity `skipped_diverged` in the run report. **Keboola overwrites**: Keboola is the source of truth; every field the writer owns is overwritten on every run. Fields the writer does not own (tags, glossary terms, …) are never touched in either mode. |
| Failure Mode | **Collect and fail at end** (default), **Fail fast**, or **Log only**. |
| Parallel Requests | How many OpenMetadata and Storage requests run at the same time (1–10, default 8). Lower it if the OpenMetadata server is slow or overloaded. |

Incremental runs
----------------

A run re-writes only what changed since the last successful run:

- **Buckets** — a bucket whose tables, columns and descriptions did not change is
  skipped. Row count, data size and last import date count as a change only when
  their `kbcRowsCount` / `kbcDataSizeBytes` / `kbcLastImport` custom properties are
  written, so a daily data load alone does not re-sync a bucket. A change of only a
  typed column's native type (for example `VARCHAR(10)` to `VARCHAR(100)`) is not
  seen until the next full refresh.
- **Lineage** — a table, pipeline or bucket whose incoming lineage edges did not
  change is skipped. Lineage that someone deleted by hand in OpenMetadata comes back
  on the next full refresh.
- **Tombstoning** (soft-deleting tables removed from Storage) runs only in buckets
  that changed.

Every 20th run, and every run with **Full Refresh** on, re-writes everything.

Sync actions: **testConnection**, **listBuckets**, **listTransformations**,
**listComponents**, **listFlows**, **listDataApps**.

**OM target:** stable **1.13.4** is the guaranteed surface; three 2.0-only
features (`deleteStale`, bulk `?overrideMetadata=`, delete-lineage-by-source-name)
are probe-gated via `GET /api/v1/system/version` with 1.13.4 fallbacks.

Output tables
=============

- `catalog_run_report` — one row per entity action (`created` / `updated` /
  `skipped_unchanged` / `skipped_diverged` / `tombstoned` / `unresolved` /
  `failed`); `write_always`, so it survives a failing job.
- `last_written_snapshot` — the three-way-merge base (last-written fields per FQN).

Upgrading
=========

Version 0.2 changes what an existing configuration writes on its next run:

- **Pipeline and data-app services** are named after the project
  (`keboola-<project name>`) instead of the project id (`keboola-<project id>`).
  The first run creates the new services and **soft-deletes** the old
  `keboola-<project id>` services with their pipelines, dashboards and lineage
  edges. A soft-deleted entity can be restored in OpenMetadata; tags or owners a
  person set on the old pipelines are not copied to the new ones.
- **Lineage layout** moved from table → pipeline → table to a direct table → table
  edge that carries the pipeline. To keep the old layout, turn on *Pipeline as
  lineage node (legacy)*. With *Table lineage* off, pipelines stay nodes, because
  there is no table edge to carry them.
- **Table type**: a table of a linked bucket, or a table alias, is now a *View*
  in every bucket stage (before, only in the `in` stage).
- **Custom properties** are no longer defined automatically. Properties that
  already exist keep being filled; choose *Define missing properties* to restore
  the old behaviour.
- The first run re-writes every bucket and every lineage edge once (the catalog
  format version changed, and lineage change tracking starts with that run).

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
