# keboola.wr-openmetadata-catalog — Design Spec

> Type: writer
> Component ID: `keboola.wr-openmetadata-catalog`
> Status: draft
> Date: 2026-08-24
> Baseline: `docs/superpowers/research/2026-08-24-openmetadata-catalog-research.md` (capability
> inventory E1–E19, verified OM REST §8 + Keboola platform §9 contracts, 7 blocker verdicts).

## 0. Scope of this build (read first)

This spec covers a **maximal-scope build** (approved 2026-08-24, overriding every "defer"
recommendation): a push-based writer that reads Keboola project metadata and pushes catalog,
pipelines, and both lineage tiers to an OpenMetadata (OM) tenant via the OM REST API. Concretely this
build delivers:

- **Catalog** — OM entity families E1–E12 as applicable (services, databases, schemas, tables
  Regular/View/External, columns + datatypes, primary keys, descriptions, deep links, tags).
- **Pipeline family (E13/E14/E15)** — component configs → Pipeline (E13), flows/orchestrations →
  Pipeline (E14), and job run history → Pipeline status (E15) via the Job Queue
  `GET /jobs/{id}/open-api-lineage` events.
- **Declared table-level lineage (E16)** — coarse `storage.input → storage.output` edges derived from
  the producing components' config storage mapping, tagged `source=PipelineLineage`.
- **Column-level SQL lineage (E17)** — SQLGlot over transformation SQL, resolved through the config's
  storage mapping and workspace intermediates, tagged `source=QueryLineage`. **Snowflake-first**, with
  a per-dialect casing/expansion plan (BigQuery / Redshift / Synapse) and a resolved / no-upstream /
  unresolved taxonomy wired to a **CI coverage gate**. Seeded by `lineage-spike/`.
- **Three-way merge** — base = the value we last wrote; update if OM still matches, leave + log if a
  human diverged, write if empty. Same diff logic for lineage graphs; never touch `Manual` edges.
- **Incremental** — per-bucket content digest in `state.json`; periodic full refresh.
- **Multi-project, both tiers (E-side seam):** **Tier-1** config rows (one per project, encrypted
  read-only Storage token) **and Tier-2** manage-token "all org projects" (enumerate + mint), with
  **graceful degradation to Tier-1 when the `manage:storage-tokens` super token isn't available** (see
  §5.4). One config seam supports both.
- **Run report / drift table** — a Keboola output table recording every create/update/skip/tombstone/
  failure, doubling as the coverage-and-drift signal.
- **OM target:** the **latest _stable_ OM release, 1.13.4** (verified — see §3.5), which is what real
  tenants and the public sandbox run today. **2.0.0 is a pre-release / RC, not GA** ("changes until the
  final release"), so its 2.0-only niceties (`deleteStale`, bulk `?overrideMetadata=`, the lineage
  delete-by-source-name convenience) are supported **opportunistically and version-probe-gated**, never
  hard-depended on. A `GET /api/v1/system/version` probe selects the 2.0 path when present and the
  1.13.4 path otherwise.

Deferred to a later build, tracked in §4 and §10 (NOT silently dropped): **Glossary / Metric
(E18/E19)** — not requested and needing a separate Keboola semantic-layer source. **Not building at
all:** cross-warehouse lineage (needs warehouse creds the component must not hold).

Every E1–E19 capability and every considered-but-excluded OM entity appears with an explicit verdict
in §4. The full-scope discipline is: deferrals are user-approved roadmap rows, never author cuts.

---

## 1. Overview & source system

The component runs as a Keboola job, reads a project's metadata (buckets, tables, columns, native
datatypes, primary keys, descriptions, sharing/linkage, the producing configs' storage mapping and
transformation SQL, flows/orchestrations, and job run history) through the **Keboola Storage +
Configurations + Job Queue APIs**, maps it to OpenMetadata's entity model, and pushes it to a
customer-hosted OpenMetadata tenant through the **OpenMetadata REST API** (`PUT /api/v1/tables`,
`PUT /api/v1/pipelines`, `PUT /api/v1/lineage`, and siblings — all FQN-keyed `createOrUpdate`
upserts).

- **Target system:** OpenMetadata — open-source data catalog / metadata platform. REST API base
  `<host>/api/v1`; docs https://docs.open-metadata.org/ and the versioned JSON schemas / JAX-RS
  resources at the `1.13.4-release` and `2.0.0-release` tags (verified in research §8).
- **Source system:** the Keboola Connection platform itself, via the Storage API
  (`https://connection.<stack>.keboola.cloud/v2/storage`) and the Job Queue lineage endpoint
  (research §9.2). Docs https://keboola.docs.apiary.io/ and `help.keboola.com`.
- **Primary use case:** keep a customer's OpenMetadata catalog continuously in sync with what lives
  in their Keboola project(s) — every bucket, table, column, primary key, the transformation/flow
  pipelines and their run history, and both the declared table-to-table and computed column-to-column
  data flow — so data consumers browse Keboola assets, their processing pipelines, and end-to-end
  lineage alongside the rest of their estate, with deep links back into the Keboola UI.

This is an **atypical writer**: its "input" is the whole project's metadata pulled over the API (not
Keboola input-table mapping), and its "output" is OM entities plus one small Keboola report table. It
is **not** a port of any OM-hosted ingestion connector — it pushes *into* OM from the outside.

---

## 2. Keboola mapping

How OM concepts map onto Keboola, and how the component fits the platform runtime.

### 2.1 Entity mapping (source → target)

| Keboola object | OM entity | OM endpoint (createOrUpdate) |
|---|---|---|
| Stack (`KBC_STACKID`) | DatabaseService | `PUT /api/v1/services/databaseServices` |
| Project | Database | `PUT /api/v1/databases` |
| Bucket | DatabaseSchema | `PUT /api/v1/databaseSchemas` |
| Table (out/regular) | Table (`tableType=Regular`) | `PUT /api/v1/tables` |
| Linked/shared bucket (stage=in + `sourceBucket`) | Table (`tableType=View`) + lineage edge | `PUT /api/v1/tables` + `PUT /api/v1/lineage` |
| Table alias | Table (`tableType=View`) + lineage edge | same (improvement over reference) |
| External-schema bucket (`hasExternalSchema=true`, no sharing) | Table (`tableType=External`) | `PUT /api/v1/tables` |
| Column + native datatype | `Column` in the Table body | part of Table body |
| `primaryKey` / `definition.primaryKeysNames` | `tableConstraints[] PRIMARY_KEY` | part of Table body |
| `KBC.description` (bucket/table/column) | `description` fields | part of entity body |
| Keboola UI deep link | `sourceUrl` | field on Database/Schema/Table/Pipeline |
| Producer config `storage.input/output.tables` + `KBC.createdBy.*` | table-level lineage edge | `PUT /api/v1/lineage` (`source=PipelineLineage`) |
| Transformation SQL (blocks/codes), resolved via storage mapping | column-level lineage edge | `PUT /api/v1/lineage` w/ `columnsLineage` + `tempLineageTables` (`source=QueryLineage`) |
| The stack (once) | PipelineService | `PUT /api/v1/services/pipelineServices` |
| Component configuration (rows/blocks/codes) | Pipeline + Tasks (`taskSQL`, `downstreamTasks`) | `PUT /api/v1/pipelines` |
| Flow / orchestration (phases) | Pipeline + Tasks (phase ordering) | `PUT /api/v1/pipelines` |
| Job Queue run history (`GET /jobs/{id}/open-api-lineage`) | Pipeline status / run history | `PUT /api/v1/pipelines/{fqn}/status` |

**FQN scheme** (deterministic, so N project-rows form one coherent catalog):
`service.database.schema.table` = `<service_name>.<project>.<bucket.path>.<table.name>`. The service
is **one DatabaseService per stack** (recommended over per-project — see §5.1). Names are sanitised
(spaces → underscores, dots avoided in table names); the original is preserved as `displayName`.

### 2.2 Config vs config rows

**Config rows, one row per Keboola project** (per the multiple-independent-objects convention — each
project is enabled/disabled/retried independently and gets its own per-row `state.json`). OM
connection + global behaviour live at **root** config; per-project settings (token, name override,
bucket/stage filters) live at **row** level. The platform merges root + row into one `config.json`
before each row runs — the component only ever sees the merged shape (research §9.4).

- **Execution model:** rows run **sequentially** in `rowsSortOrder` by default. We do **not** assume
  parallel rows. If an operator wants concurrency they set the root `parallelism` property (integer or
  `"infinity"`); the design is safe either way because each row targets a disjoint set of OM FQNs
  (different project ⇒ different `Database` node), so parallel rows do not collide on OM writes. The
  shared DatabaseService (E1) is a create-or-update upsert and is idempotent under concurrency.
- **No Keboola input mapping.** The component reads metadata via the Storage API, so it uses neither
  root- nor row-level `storage.input.tables`. The "input mapping belongs on the row" tainting concern
  therefore does not apply. (Stated explicitly so the grounding gate can confirm it.)

### 2.3 Incremental strategy → state + snapshot store

Two related problems share one mechanism (research §4):

1. **Incremental skip** — per-bucket **content digest** kept in `state.json`. A bucket whose digest is
   unchanged since the last successful run is skipped. State stays far below the ~1 MB guidance (one
   short digest per bucket; hundreds of buckets at most) — deliberately **not** per-table (a project
   can hold 5k+ tables).
2. **Three-way-merge base** — the precise "what we last wrote, per field" record is too large for
   `state.json` at 5k tables, so it lives in a **component-managed snapshot table** in the project's
   Storage (default bucket, incremental upsert on entity FQN, JSON blob + content hash per entity).
   The component reads it back at run start **via the Storage API** (it already holds the token), so
   no user-facing input mapping is required. `state.json` holds only pointers: per-bucket digests, the
   snapshot table id, `last_full_refresh`, and a run counter.

`state.json` shape (per row):

```json
{
  "bucket_digests": { "in.c-main": "9f2a…", "out.c-sales": "1b77…" },
  "snapshot_table": "out.c-keboola-om-catalog.last_written_snapshot",
  "last_full_refresh": "2026-08-24T10:00:00+00:00",
  "run_count": 42,
  "om_version_seen": "1.13.4"
}
```

**Advance-after-success (non-negotiable):** a bucket's digest and the row's `state.json` are persisted
only *after* that bucket's OM entity writes have succeeded. A run that fails partway keeps the old
digests, so the affected buckets are re-processed next run rather than silently skipped. State is
written once at the end of a successful pass (and `catalog_run_report` is `write_always` so partial
progress is still visible on failure).

**Periodic full refresh** ignores digests and re-reads OM ground truth (rebuilding the snapshot). It
fires on `full_refresh=true`, on a version change, or automatically every N runs (default 20;
internal constant). Lineage is dirty-checked off the producing config's version.

### 2.4 Secrets → `#`-prefixed keys

- `#bot_token` (root) — OM JWT bot token.
- `#storage_token` (row) — read-only Storage token for a project other than the host project (Tier-1).
- `#manage_token` (root, optional) — Management API super/application token with `manage:storage-tokens`
  for Tier-2 "all org projects" enumerate + mint (§5.4). Absent ⇒ Tier-1.
- `#ssh.private_key` (root, optional) — SSH bastion private key.

All `#`-prefixed ⇒ encrypted at rest as `KBC::ProjectSecure` (the default scope), decrypted into the
container at runtime (research §9.5; encryption reference). No plaintext secret keys anywhere.

### 2.5 Sync actions

- **`testConnection`** — validate the OM host + bot token (`GET /api/v1/system/version` with the
  bearer token: reachability + auth + captured server version) **and** the Storage credential for the
  row (verify the token). Surfaces reachability / bad-token failures in the UI instead of only at
  runtime. (Implemented via `component-build-ui`; sync-action detail lives there.)
- **`listBuckets`** — enumerate the row's project buckets (via the resolved Storage token) to populate
  the `bucket_allowlist` / `bucket_denylist` dropdowns. Buckets are enumerable, so the convention is a
  sync-action dropdown over free-text; free-text creatable entry is the fallback when the token cannot
  list yet (e.g. before it is entered).

### 2.6 Output bucket / table naming

The component writes two Keboola output tables to its **default output bucket**
(`out.c-keboola-om-catalog…` derived by the platform from component + config id — not hard-coded):

- `catalog_run_report` — one row per entity action this run (see §6.4). `write_always: true` so it
  survives a failing job (essential in `collect_and_fail` mode). Incremental + PK so re-runs upsert.
- `last_written_snapshot` — the three-way-merge base store (§2.3). Incremental upsert on entity FQN.

Scratch files go to `/tmp`, never `/data/out/tables/` (everything under the latter is uploaded).

---

## 3. Authentication & connection

### 3.1 OpenMetadata auth

**JWT bot token** (`Authorization: Bearer <jwt>`) — verified in research §8.1. One encrypted
`#bot_token`. Chosen over the local basic-auth `POST /users/login` + `/refresh` flow (not intended
for machine-to-machine) and over per-run token minting (unnecessary lifecycle). The token carries
full API privileges, so a **dedicated custom bot** with a long / `Unlimited` expiry (rotated by the
customer) is recommended over the shared `ingestion-bot`.

**Provisioning (one-time, OM admin):** OM UI → Settings → Bots → add a bot (or reuse `ingestion-bot`)
→ *Generate new token* → copy the JWT into `#bot_token`. No per-run UI click-through.

### 3.2 Keboola Storage auth

- **Host project (single-project / the row for the project the config lives in):** free and headless
  via **`forward_token`**, which injects `KBC_TOKEN` (Storage token) + `KBC_URL` (Storage API URL);
  **`forward_token_details`** adds `KBC_PROJECTNAME`. No credential in that row.
  - **Provisioning:** `forward_token: true` and `forward_token_details: true` are app-record booleans
    set only via the admin Dev-Portal endpoint (`PATCH /admin/apps/{app}`) — a **vendor cannot enable
    them from self-serve**; it needs Keboola staff action (research §9.1, env-vars reference Tier 2/3).
    **Request this early** — it is a build prerequisite for the single-project story.
- **Additional projects — Tier-1 (multi-project rows):** a **read-only Storage token minted in that
  project** by its master token (`help.keboola.com/management/project/tokens`), pasted into the row's
  `#storage_token`. Read-only is sufficient — the component only reads metadata.
- **All org projects — Tier-2 (manage token):** a single `#manage_token` (Management API
  super/application token carrying `manage:storage-tokens`) enumerates org projects
  (`GET /manage/organizations/{organizationId}/projects`) and mints a short-lived read-only Storage
  token per project (`POST /manage/projects/{projectId}/tokens`) at run time — no per-project row.
  **Provisioning:** this token class is **likely internal/partner-provisioned** (research Blocker 3,
  INFERRED it needs a support/partner request) and it is an **org-wide blast-radius** credential →
  host the config in a locked-down admin project, log the exact project list per run, and mint only
  short-lived read-only tokens. Because obtainability is uncertain, Tier-2 **degrades gracefully to
  Tier-1** (§5.4): it is a Phase-7 provisioning dependency, not a build blocker.

### 3.3 Connection method

Hand-rolled thin **REST client over `requests`** for both OM and Keboola Storage. No OM SDK: the
`openmetadata-ingestion` package version-locks to the OM server and constrains the Python floor
(research §8.9), so one image could target only one OM version. The REST client **targets the latest
stable OM 1.13.4 as the guaranteed surface** and treats the 2.0-only additions as opportunistic (§3.5):
a `GET /api/v1/system/version` probe selects the 2.0 bulk-`overrideMetadata` / `deleteStale` /
delete-lineage-by-source-name path when the tenant is 2.0+, and falls back to the 1.13.4 path (per-id
soft-delete / delete-lineage-by-source-by-id) otherwise (§6.2). Because 2.0 is a pre-release whose
surface may still change before GA, no code path hard-depends on it.

### 3.4 Provisioning blockers / access

- **Network reachability (the one true gating unknown, research Blocker 1):** the customer's OM API
  must be reachable from Keboola's static egress IPs. This is a **per-customer deployment concern, not
  a build blocker**. v1 ships two mitigations: (a) document allowlisting Keboola's published static
  outbound IPs (`help.keboola.com/components/ip-addresses/`); (b) an **optional SSH-bastion proxy**
  config block (precedent `keboola.wr-openlineage`). A Python writer implements the tunnel itself
  (e.g. `sshtunnel`); we do not assume a platform-provided helper (research §9.6 caveat).
- **Sandbox for contract tests:** OM ships a public sandbox (`sandbox.open-metadata.org`) and a
  trivial local `docker compose` / `metadata docker` quickstart → VCR cassettes can be recorded with
  **no customer creds**. Customer-tenant creds are needed only for the Phase-7 live smoke test.

### 3.5 Verified OM version + endpoint surface (primary-source, 2026-08-24)

Verified against the GitHub Releases API, the raw JSON schemas, and the raw JAX-RS resource Java at the
`1.13.4-release` and `2.0.0-release` tags (and confirmed against the live public sandbox). This
**supersedes** the research doc's "2.0.0 GA on 2026-08-24" note, which was inaccurate.

- **Latest _stable / GA_ release = `1.13.4-release`** (2026-08-21, `prerelease=false`, marked "Latest").
  The public sandbox `sandbox.open-metadata.org` runs it (`GET /api/v1/system/version` →
  `{"version":"1.13.4","revision":"…","timestamp":…}`). Sources:
  `https://api.github.com/repos/open-metadata/OpenMetadata/releases`,
  `https://sandbox.open-metadata.org/api/v1/system/version`.
- **`2.0.0-release` is a PRE-RELEASE / RC** (2026-08-24, `prerelease=true`, "changes until the final
  release is published") — **not GA**. No `2.0.1`/`2.1.0` exists yet; `2.0.0-release` is the only 2.x
  tag. Source: `https://github.com/open-metadata/OpenMetadata/releases/tag/2.0.0-release`. **⇒ We
  target 1.13.4 as the guaranteed surface and gate the 2.0-only surface behind the version probe.**
- **`GET /api/v1/system/version`** — `VersionResource.java` `@Path("/v1/system/version")` `@GET
  getCatalogVersion()` → `OpenMetadataServerVersion` with fields **`version`, `revision`, `timestamp`**.
  Not auth-required (it is in `JwtFilter.EXCLUDED_ENDPOINTS`) but a Bearer token is accepted harmlessly
  — so `testConnection` still uses it to check reachability + auth. Sources: `VersionResource.java` and
  `openMetadataServerVersion.json` at `2.0.0-release`; live sandbox confirms the three fields.
- **`DELETE /api/v1/tables/deleteStale` — 2.0-only.** `TableResource.java` `@DELETE @Path("/deleteStale")`
  taking `BulkDeleteStaleRequest{scopeFqn (req), scopeEntityType (req), seenFqns[] (req), dryRun
  (default false), hardDelete (default false), recursive (default true)}`. Absent in `1.13.4-release`
  (dir listing has only `bulkOperationResult.json`). Sources: `TableResource.java` +
  `type/bulkDeleteStaleRequest.json` at `2.0.0-release`; contents listing at `1.13.4-release`.
- **`PUT /api/v1/tables/bulk` (`bulkCreateOrUpdateTables`)** exists in **both** 1.13.4 and 2.0. The
  **`?overrideMetadata=`** query param (bool, default false; overwrites curated fields, disables the
  sourceHash fast-path) is **2.0-only** — absent in `1.13.4-release`. Source: `TableResource.java` at
  both tags.
- **Entity + pipeline + lineage endpoints are unchanged 1.13.4 → 2.0 (2.0 is purely additive):**
  `PUT /services/databaseServices`, `PUT /databases`, `PUT /databaseSchemas`, `PUT /tables`,
  `PUT /services/pipelineServices`, `PUT /pipelines`, `PUT /pipelines/{fqn}/status`, `PUT /lineage`
  (`AddLineage.edge → entitiesEdge{fromEntity,toEntity,description,lineageDetails}`; `lineageDetails`
  = `sqlQuery, columnsLineage, pipeline, source, tempLineageTables, assetEdges, …`; `source` enum
  incl. `Manual/ViewLineage/QueryLineage/PipelineLineage/…`), and `PATCH …/name/{fqn}` (json-patch).
  Sources: the respective `*Resource.java` + `entityLineage.json` / `pipeline.json` at `2.0.0-release`.
- **Lineage delete-by-source path differs by version (matters for the three-way lineage diff):**
  - 2.0: `DELETE /api/v1/lineage/source/name/{entityType}/{entityFQN}/type/{lineageSource}` (by-FQN,
    convenient) — **NEW in 2.0**.
  - 1.13.4: only the by-id form `DELETE /api/v1/lineage/{entityType}/{entityId}/type/{lineageSource}`
    (resolve the entity id first). The by-id form **also exists in 2.0**.
  ⇒ the merge engine drops our stale `PipelineLineage`/`QueryLineage`/`ViewLineage` edges via the
  by-name path on 2.0+ and the by-id path on 1.13.4 — the capability exists on both; only the path
  differs. Source: `LineageResource.java` at both tags.
- **Auth:** JWT Bearer via `JwtFilter` (`Authorization: Bearer <jwt>`, `TOKEN_PREFIX="Bearer"`);
  unchanged in 2.0. Source: `JwtFilter.java` at `2.0.0-release`.

**Net for the build:** everything this component needs works on **1.13.4** except three 2.0-only
niceties (`deleteStale`, bulk `?overrideMetadata=`, lineage delete-by-source-**name**), each of which
has a 1.13.4 fallback and is selected by the version probe. When 2.0 GAs, the component already uses
its niceties with no code change. No endpoint this component depends on is renamed/removed in 2.0.

---

## 4. Capability inventory & scope

Every E1–E19 capability from the research baseline, plus the OM entities explicitly considered and
excluded. **Verdict legend:** *In scope* = built in this (maximal-scope) build; *In scope (deferred)*
= user-approved roadmap, built a later phase; *Excluded* = user-approved cut with reason.

### 4.1 OM entity families (E1–E19)

| # | Capability | Verdict | Rationale |
|---|---|---|---|
| E1 | DatabaseService ← stack | **In scope** | FQN root; one per stack; idempotent upsert. |
| E2 | Database ← project | **In scope** | one per project (per row); `sourceUrl` → project deep link. |
| E3 | DatabaseSchema ← bucket | **In scope** | `sourceUrl` → bucket. |
| E4 | Table (Regular) ← table | **In scope** | columns, PK, rowCount/size/lastImport, description, sourceUrl. |
| E5 | Table (View) + lineage ← linked/shared bucket | **In scope** | `schemaDefinition` = `CREATE VIEW … SELECT * FROM <source fqn>` + a ViewLineage edge. |
| E6 | Table (View) ← table alias | **In scope** | improvement over the reference (which omitted it). Small delta on E5. |
| E7 | Table (External) ← external-schema bucket | **In scope** | the External *table entity*; cross-warehouse *lineage off it* is Excluded (see CW row). |
| E8 | Column + datatype | **In scope** | map order: typed `definition.type`/`basetype` → legacy `KBC.datatype.basetype` → `UNKNOWN`; never write `dataLength:1`. |
| E9 | tableConstraints PRIMARY_KEY | **In scope** | from `primaryKey` / `definition.primaryKeysNames` (reference parsed but never mapped it). |
| E10 | Descriptions (schema/table/column) | **In scope** | from `KBC.description`; semantically thin in practice — set expectations, don't fabricate. |
| E11 | `sourceUrl` deep links | **In scope** | derived from stack + project/bucket/table id (reference hard-coded the UI base). |
| E12 | Tags / custom properties | **In scope (tags optional)** | entity-type + `KBC.createdBy.*` tags; classification creation guarded (skip if the bot lacks policy). |
| E13 | Pipeline ← component config | **In scope** | config → Pipeline (+PipelineService); rows/blocks/codes → Tasks; block SQL → `taskSQL`; `downstreamTasks` ordering; `sourceUrl` deep link. |
| E14 | Pipeline ← flow/orchestration | **In scope** | flow/orchestration phases → Pipeline + Tasks with phase ordering. Same PipelineService as E13. |
| E15 | Pipeline status / run history ← Job Queue | **In scope** | `GET /jobs/{id}/open-api-lineage` START/COMPLETE events → `PUT /api/v1/pipelines/{fqn}/status` (success/fail + timestamps). |
| E16 | **Lineage — table-level (declared)** | **In scope** | free, all component types, no SQL. Coarse (all-inputs → all-outputs). `source=PipelineLineage`. |
| E17 | Lineage — column-level (SQL / SQLGlot) | **In scope** | SQLGlot over transformation SQL, resolved via storage mapping + `tempLineageTables` chaining; `source=QueryLineage`. Snowflake-first; per-dialect casing (BigQuery/Redshift/Synapse); resolved/no-upstream/unresolved taxonomy + CI coverage gate. Seeded by `lineage-spike/`. |
| E18 | Glossary / GlossaryTerm | **In scope (deferred)** | needs a Keboola semantic-layer source; not requested. Group with E19 as a later semantic-layer phase. |
| E19 | Metric | **In scope (deferred)** | first-class OM `createOrUpdate`; needs semantic-layer metrics as source; not requested. Grouped with E18. |

### 4.2 Lineage tiers

| Tier | Verdict | Rationale |
|---|---|---|
| Declared table-level (from `storage.input/output`) | **In scope** | E16; `source=PipelineLineage`. Honest coarseness: N inputs × M outputs. |
| Column-level (SQLGlot over transformation SQL) | **In scope** | E17; `source=QueryLineage`; Snowflake-first, per-dialect casing plan, CI coverage gate. |
| Cross-warehouse (Keboola table ↔ customer's own BigQuery/Snowflake) | **Excluded** | needs warehouse credentials the component must not hold; the one thing the reference does that we deliberately do not match. User-approved cut. |

### 4.3 OM entities considered & excluded (beyond E1–E19)

| OM entity | Verdict | Rationale |
|---|---|---|
| **StoredProcedure** (`PUT /api/v1/storedProcedures`) | **Excluded** | no Keboola source concept — transformations map to lineage/Pipeline, not a stored-procedure object. Revisit only if a Keboola construct maps cleanly. |
| **Query entity** (`PUT /api/v1/queries` — SQL query catalog) | **Excluded** | redundant with lineage `sqlQuery`/`taskSQL`; adds no standalone catalog value. Revisit alongside E17 column lineage. |
| **Domain / DataProduct** (governance grouping) | **Excluded** | no deterministic Keboola source; organisational grouping needs a semantic mapping. Candidate for a future semantic-layer phase with E18/E19. |
| **DataQuality testCases** (`PUT /api/v1/dataQuality/testCases`) | **Excluded** | Keboola exposes no data-quality signal to push; revisit if/when it does. |

### 4.4 Mechanics for the in-scope surface

- **OM writes:** FQN-keyed `PUT` upserts for entities; `PUT /api/v1/lineage` for edges (both endpoints
  create-or-update). Field-level three-way-merge writes use `PATCH …/name/{fqn}` with
  `Content-Type: application/json-patch+json` (RFC-6902) so we touch only fields we own.
- **Bulk throughput:** `PUT /api/v1/tables/bulk` (`bulkCreateOrUpdateTables`) works on **1.13.4+**; the
  **2.0-only** `?overrideMetadata=` is probe-gated and off by default (the three-way merge preserves
  curator fields anyway) — §3.5.
- **Ordering:** both lineage endpoints require the referenced entities to already exist and be
  NON_DELETED (else 404) → the **catalog pass runs before the lineage pass**, always.
- **Pagination (reads):** OM list is cursor-based (`after`/`before`, `limit ≤ 1000000`) — page modestly
  (100–500) to avoid server timeouts on `fields=columns`. Keboola Storage buckets/tables are listed
  per bucket with `?include=…`.
- **Rate limits:** OM documents none and has no `429`/`Retry-After` contract → self-impose bounded
  concurrency (a handful) + own exponential backoff on 5xx / connection errors.
- **Nested shapes:** OM `Column.children[]` (struct/map/union) handled recursively; Keboola typed
  `definition.columns[].definition` is the authoritative native type, distinct from legacy
  `KBC.datatype.*` column-metadata.

---

## 5. Configuration & schema

The actual `configSchema.json` / `configRowSchema.json` are built by **`component-build-ui`** — this
section specifies the fields, shape, widgets, and persistence rules; it does not write the JSON.

### 5.1 Fields

**Root config** (OM connection + global behaviour — entered once, shared across rows):

| Field | Required | Secret | Purpose / default |
|---|---|---|---|
| `om_host` | yes | — | OM API base URL, e.g. `https://openmetadata.example.com` (component appends `/api/v1`). |
| `#bot_token` | yes | ✅ | OM JWT bot token. |
| `service_name` | no | — | OM DatabaseService name (FQN root). Default derived from `KBC_STACKID` at runtime (§5.2). **One service per stack** (recommended over per-project: keeps N project-rows one coherent catalog and the service name is near-permanent). |
| `project_scope` | no | — | enum `rows` (Tier-1, default) / `all_projects` (Tier-2). Selects the multi-project seam (§5.4). Stores value. |
| `#manage_token` | conditional | ✅ | Management API super/application token (`manage:storage-tokens`). Required only when `project_scope=all_projects`; shown via `dependencies`. |
| `organization_id` | conditional | — | org id for Tier-2 project enumeration; required (or auto-resolved from the manage token) only when `project_scope=all_projects`. |
| `merge_mode` | no | — | enum `three_way_merge` (default) / `keboola_always_wins`. Stores value. |
| `failure_mode` | no | — | enum `collect_and_fail` (default) / `fail_fast` / `log_only`. Stores value. |
| `branch_filter` | no | — | enum `production_only` (default) / `all_branches` — which `KBC.createdBy.branch.id` to include (reference hard-coded production-only). Stores value. |
| `write_lineage` | no | — | bool, default `true` — emit E16 declared table-level lineage. |
| `write_column_lineage` | no | — | bool, default `true` — emit E17 column-level SQL lineage (SQLGlot). |
| `write_pipelines` | no | — | bool, default `true` — emit E13/E14 Pipeline entities (config + flow). |
| `write_pipeline_status` | no | — | bool, default `true` — emit E15 run history; gated on `write_pipelines` via `dependencies`. |
| `full_refresh` | no | — | bool, default `false` — force a full re-push this run, ignoring digests. |
| `use_ssh_tunnel` | no | — | bool, default `false` — gate for the SSH block. |
| `ssh` | no | (`#private_key`) | object `{host, user, port, #private_key}`, shown only when `use_ssh_tunnel=true`. |
| `om_version_override` | no | — | text; normally auto-probed via `GET /api/v1/system/version`. Advanced. |
| `debug` | no | — | bool, default `false` — verbose logging. |

**Row config** (per Keboola project):

| Field | Required | Secret | Purpose / default |
|---|---|---|---|
| `#storage_token` | conditional | ✅ | read-only Storage token for this project. **Omit for the host project** (uses `forward_token`); **required for any other project.** |
| `project_name_override` | no | — | human-readable OM `Database` name; default = `KBC_PROJECTNAME` (host) or derived from the token (remote). |
| `stages` | no | — | multi-select `{in, out}`, default both — which bucket stages to catalog. |
| `bucket_allowlist` | no | — | multi-select of bucket ids (from `listBuckets`); default empty = all buckets. |
| `bucket_denylist` | no | — | multi-select of bucket ids (from `listBuckets`); default empty. |

**Row semantics per tier.** Under `project_scope=rows` (Tier-1) a row **is** one project (its
`#storage_token`, name, and filters). Under `project_scope=all_projects` (Tier-2) a single "org row"
drives the run: at runtime it enumerates org projects and loops over them, so the row's
`#storage_token`/`project_name_override` are unused (the manage token mints per-project tokens and
names come from enumeration), while `stages`/`bucket_allowlist`/`bucket_denylist` apply org-wide.
Either way the component stays row-based (`rows` ≥ 1) and keeps per-row state; Tier-2 keys its state
by `project_id → {bucket → digest}`.

### 5.2 UI scope & config shape (explicit decision)

| Field | Level | Required | User-facing or internal | Default | In a NEW config's saved params? |
|---|---|---|---|---|---|
| `om_host` | config | yes | user-facing (text) | — | yes — user sets it |
| `#bot_token` | config | yes | user-facing (password) | — | yes — user sets it |
| `service_name` | config | no | user-facing (text, Advanced) | derived from `KBC_STACKID` at runtime | no — omit until the user overrides; default computed in the Pydantic model |
| `project_scope` | config | no | user-facing (enum) | `rows` | only if changed to `all_projects` |
| `#manage_token` | config | conditional | user-facing (password), gated on `project_scope=all_projects` | — | only when `all_projects` |
| `organization_id` | config | conditional | user-facing (text), gated on `project_scope=all_projects` | auto-resolved from the manage token when omitted | only when `all_projects` and set |
| `merge_mode` | config | no | user-facing (enum, Advanced) | `three_way_merge` | only if changed — gate under Advanced, don't seed a fresh config |
| `failure_mode` | config | no | user-facing (enum, Advanced) | `collect_and_fail` | only if changed |
| `branch_filter` | config | no | user-facing (enum, Advanced) | `production_only` | only if changed |
| `write_lineage` | config | no | user-facing (bool, Advanced) | `true` | only if changed |
| `write_column_lineage` | config | no | user-facing (bool, Advanced) | `true` | only if changed |
| `write_pipelines` | config | no | user-facing (bool, Advanced) | `true` | only if changed |
| `write_pipeline_status` | config | no | user-facing (bool, Advanced), gated on `write_pipelines` | `true` | only if changed |
| `full_refresh` | config | no | user-facing (bool, Advanced) | `false` | only if changed |
| `use_ssh_tunnel` | config | no | user-facing (bool) | `false` | only if changed |
| `ssh.*` | config | no | user-facing, gated on `use_ssh_tunnel` | — | only when `use_ssh_tunnel=true` |
| `om_version_override` | config | no | user-facing (text, Advanced) | — (auto-probe) | no — omit until set |
| `debug` | config | no | user-facing (bool) | `false` | only if changed |
| `#storage_token` | row | conditional | user-facing (password) | — | only for remote-project rows |
| `project_name_override` | row | no | user-facing (text) | derived at runtime | no — omit until set |
| `stages` | row | no | user-facing (multi-select) | `[in, out]` | only if narrowed |
| `bucket_allowlist` / `bucket_denylist` | row | no | user-facing (async multi-select, `listBuckets`) | `[]` | only if set |
| *service FQN root, sanitised names, snapshot table id, per-bucket digests, auto-full-refresh cadence, SQL dialect (auto-detected per transformation backend)* | — | — | **internal** (never in schema) | computed | no |

**Rules baked in (the recurring runtime-gate catches):**
- **No silent default persists in a fresh config.** Every optional default is either computed in the
  Pydantic model at runtime (not serialised) or gated behind Advanced / a `dependencies` toggle, so a
  newly-created config's saved parameters contain only what the user actually chose plus visible,
  self-explanatory values. The user never sees a diff full of values they didn't set.
- **Enums / multi-selects store the machine value, not the label** (`enum` + `enum_titles`).
- **Row-based config needs `rows` ≥ 1** — an empty config catalogs nothing; `testConnection` and the
  run both require at least one row.
- **Sectioning:** the root form groups into named `type: object` sections — *Connection*
  (`om_host`, `#bot_token`, SSH), *Scope* (`project_scope` + the gated `#manage_token`/
  `organization_id`), and *Advanced* (merge/failure/branch, the `write_*` lineage+pipeline toggles,
  full-refresh, version) — since it exceeds ~6 mixed fields.
- **Column-lineage SQL dialect is auto-detected**, not a user field: the component reads each
  transformation's backend/component id and picks the SQLGlot dialect (Snowflake, BigQuery, Redshift,
  Synapse). Unknown dialects fall back to a generic parse and are recorded as `unresolved` in the
  coverage taxonomy rather than guessed.
- **No Full/Incremental load dropdown and no watermark field.** Incremental here is the per-bucket
  digest skip (§2.3), an internal optimisation — it is not a Keboola output-mapping load mode the user
  toggles. `full_refresh` is the only user-visible incremental control.

### 5.3 UI presentation (widget per user-facing field)

| Field | Widget | Notes |
|---|---|---|
| `om_host` | text | URL; validated by `testConnection`. |
| `#bot_token` | password | encrypted `#`. |
| `service_name` | text | placeholder shows the derived default. |
| `project_scope` / `merge_mode` / `failure_mode` / `branch_filter` | `enum` + `enum_titles` | humanised labels; stores value. |
| `#manage_token` | password | shown via `dependencies` on `project_scope=all_projects`; encrypted `#`. |
| `organization_id` | text | shown via `dependencies` on `project_scope=all_projects`; auto-resolved placeholder. |
| `write_lineage` / `write_column_lineage` / `write_pipelines` / `write_pipeline_status` / `full_refresh` / `use_ssh_tunnel` / `debug` | checkbox (bool) | `write_pipeline_status` gated on `write_pipelines`. |
| `ssh.host/user/port` | text; `ssh.#private_key` password | shown via `options.dependencies` on `use_ssh_tunnel`. |
| `#storage_token` | password | row-level; encrypted `#`. |
| `project_name_override` | text | placeholder shows derived name. |
| `stages` | multi-select `enum` | `{in, out}`. |
| `bucket_allowlist` / `bucket_denylist` | async `select` + `autoload` (`listBuckets`) | buckets are enumerable via the row token; dropdown over free-text per convention. Creatable free-text is the fallback when the token can't list yet. |

### 5.4 Multi-project tiers

Both tiers are built; `project_scope` selects between them and Tier-2 degrades to Tier-1 when the
manage credential isn't usable.

- **Tier-1 (`project_scope=rows`):** config **rows**, one per project, each with its own encrypted
  read-only `#storage_token` (the host-project row uses `forward_token` and omits it). Per-row state;
  enable/disable per row = the enterprise allowlist. FQN scheme keeps N rows → one coherent catalog.
- **Tier-2 (`project_scope=all_projects`):** a single org row + `#manage_token` (super/application
  manage token with `manage:storage-tokens`). At run time it enumerates org projects
  (`GET /manage/organizations/{organizationId}/projects`) and mints a **short-lived read-only** Storage
  token per project (`POST /manage/projects/{projectId}/tokens`) — "one credential → all projects"
  (capability verified, research Blocker 3). **Security shape:** org-wide blast-radius credential →
  host the config in a locked-down admin project; mint only short-lived read-only tokens; **log the
  exact project list per run**; never use the manage token to read data.
- **Graceful degradation (the key Tier-2 contract):**
  - `project_scope=all_projects` with **no `#manage_token`** → **config error** (`UserException`, exit
    1): the user chose the tier but gave no credential.
  - `#manage_token` present but **enumeration/mint fails on scope/permission** (the likely real-world
    case — the super token is internal/partner-provisioned) → **degrade to the host project only**
    (via `forward_token`), emit a **prominent warning** in logs and a `catalog_run_report` drift row
    stating that org enumeration was unavailable and what scope is missing, and continue (exit 0 unless
    other failures). Tier-2 is thus a **Phase-7 provisioning dependency, not a build blocker.**
- The `catalog_run_report` doubles as the **coverage/drift signal** for both tiers (which projects were
  catalogued, what diverged, what went stale, whether Tier-2 degraded).

---

## 6. Code architecture

`run()` is a thin orchestrator; all logic lives in well-named private methods and dedicated modules.
One Pydantic config model, validated at construction (raising `UserException` on validation error).

### 6.1 Module layout

```
src/
  component.py            # Component(ComponentBase): run() orchestrator + sync actions (test_connection, list_buckets)
  configuration.py        # Pydantic Configuration (+ nested Ssh, enums); validates merged root+row
  client/
    om_client.py          # thin OM REST client (auth, PUT/PATCH/GET/DELETE, pagination, backoff, version probe, bulk, pipelines, status)
    storage_reader.py     # Keboola Storage API reader (buckets, tables, columns, native definition, metadata, producer configs, transformation SQL, flows)
    manage_client.py      # Tier-2: enumerate org projects + mint short-lived read-only per-project tokens
    job_queue_reader.py   # E15: GET /jobs/{id}/open-api-lineage run history → pipeline status
    ssh_proxy.py          # optional SSH bastion tunnel (sshtunnel) around the OM host
  mapping/
    fqn.py                # FQN builder + name sanitisation (clean-room)
    table_type.py         # Regular / View / External detection (clean-room)
    entity_builder.py     # bucket→DatabaseSchema, table→Table (+columns, PK, datatype, description, sourceUrl)
    datatype.py           # Keboola typed definition / legacy KBC.datatype → OM DataType enum
    lineage_builder.py    # E16 declared table-level edges from producer storage.input/output
    pipeline_builder.py   # E13/E14 config+flow → Pipeline + Tasks (taskSQL, downstreamTasks); E15 status bodies
  lineage/                # E17 column-level SQL lineage (seeded from lineage-spike/)
    column_lineage.py     # SQLGlot parse → column edges, resolved via storage mapping + tempLineageTables chaining
    dialect.py            # transformation backend → SQLGlot dialect (Snowflake/BigQuery/Redshift/Synapse); casing rules
    resolution.py         # resolved / no-upstream / unresolved taxonomy + coverage metrics (feeds the CI gate)
  merge.py                # three-way merge engine + snapshot-store read/write (base = last-written)
  sync.py                 # incremental per-bucket digest; tombstoning (self-diff 1.13.4 / deleteStale 2.0+, fail-closed)
  report.py               # catalog_run_report writer (write_always) + run summary + Tier-2/coverage drift rows
```

### 6.2 `run()` orchestration (per row)

1. Build `Configuration` from merged params → validate (fail early, exit 1 on bad config; e.g.
   `project_scope=all_projects` with no `#manage_token`).
2. **Resolve the project set:**
   - Tier-1 → the row's project (via `#storage_token` or `forward_token`).
   - Tier-2 → enumerate org projects via `manage_client`; mint a short-lived read-only token per
     project; **log the project list**. On enumeration/mint permission failure → **degrade to the host
     project** (forward_token) + a drift warning (§5.4). Open the optional SSH tunnel; construct
     `om_client`.
3. **Version probe** `GET /api/v1/system/version`; record it; if the tenant is 2.0+ use the 2.0-only
   niceties (`deleteStale`, bulk `overrideMetadata`, delete-lineage-by-source-**name**), else use the
   1.13.4 fallbacks (per-id soft-delete, delete-lineage-by-source-**by-id**) — §3.5.
4. For each project (loop; per-project state under Tier-2): load prior `state.json` + read back the
   snapshot table (merge base).
5. **Catalog pass:** ensure DatabaseService (E1) → Database (E2) → for each in-scope bucket (stage +
   allow/deny filters, digest skip unless full refresh): DatabaseSchema (E3) → Tables (E4–E9) with the
   three-way merge; collect per-entity failures per `failure_mode`.
6. **Pipeline pass** (if `write_pipelines`): ensure PipelineService → build Pipeline entities from
   component configs (E13) and flows/orchestrations (E14) with Tasks (`taskSQL`, `downstreamTasks`);
   three-way merge. If `write_pipeline_status`, push run history (E15) from `job_queue_reader`.
7. **Lineage pass:** declared table-level edges (E16, if `write_lineage`, `source=PipelineLineage`) and
   column-level edges (E17, if `write_column_lineage`, SQLGlot via the `lineage/` package,
   `source=QueryLineage`, chained through `tempLineageTables`). Attach the `pipeline` ref to edges when
   the producing Pipeline exists. Diff against our prior edges (drop our stale
   `PipelineLineage`/`QueryLineage`/`ViewLineage` edges via delete-all-by-source, add new, never touch
   `Manual`). Record the E17 resolution taxonomy for the coverage gate.
8. **Tombstone pass** (fail-closed): reconcile stale entities — `deleteStale` (dryRun then apply) on
   2.0+, else self-diff soft-delete; skip entirely if the scope listing errors or looks
   implausibly short.
9. Write the updated snapshot + `state.json` (advance-after-success, §2.3); write `catalog_run_report`
   (incl. Tier-2/coverage drift rows); in `collect_and_fail`, raise `UserException` at the end if any
   entity failed.

**Ordering invariants:** entities before edges (OM 404s on edges to absent entities); Pipelines before
edges that reference them; the catalog/pipeline/lineage passes are per project inside the Tier-2 loop.

### 6.3 Error handling (exit codes)

| Condition | Handling |
|---|---|
| Bad config / validation error | `UserException` → **exit 1** |
| OM host unreachable / SSH tunnel fails | `UserException` → **exit 1** |
| OM `401` (missing/expired bot token) / `403` (bot lacks policy) | `UserException` → **exit 1** (user-fixable) |
| Storage token invalid / unauthorised | `UserException` → **exit 1** |
| `project_scope=all_projects` with **no** `#manage_token` | `UserException` → **exit 1** (config error) |
| Tier-2 manage-token enumeration/mint **permission** failure | **degrade** to host project (forward_token) + prominent warning + drift report row; **exit 0** (§5.4) |
| Column-lineage parse miss on one transformation | recorded `unresolved` in the taxonomy + report; **never fails the run** (the coverage gate is a dev/CI gate, not runtime) |
| Per-entity write failure (`4xx`/`5xx` on one entity) | collected per `failure_mode`; written to `catalog_run_report`; `collect_and_fail` raises `UserException` at end |
| `412` precondition on a PATCH | refetch + retry once, then treat as a per-entity failure |
| Tombstone list error / implausibly short scope | **fail closed** — skip tombstoning, log it, do not delete |
| Unexpected (bug, unhandled) | bubble up → **exit 2** (message hidden from user) |

Setup / config / auth failures (the first five rows) **always** fail regardless of `failure_mode`.
The Tier-2 degrade and the column-lineage parse miss are deliberately **non-fatal** (they surface in
the report). The delete path **always** fails closed.

### 6.4 Output tables (native types)

- `catalog_run_report` — columns: `run_id, config_row_id, project_id, entity_type` (Database/Schema/
  Table/Pipeline/lineage-edge), `entity_fqn, action` (`created`/`updated`/`skipped_unchanged`/
  `skipped_diverged`/`tombstoned`/`unresolved`/`degraded`/`failed`), `detail, om_status_code,
  timestamp`. `unresolved` = a column-lineage parse miss (E17); `degraded` = a Tier-2 enumeration
  fallback (§5.4). Written with an **authoritative `schema` manifest** (native types): `timestamp` →
  `TIMESTAMP`, `om_status_code` → `INTEGER`, everything else → `STRING`. Values are produced by the
  component (not an untrusted source), so native typing is safe here. `PK = [run_id, entity_fqn,
  entity_type]`, `incremental=true`, `write_always=true`, `has_header=True` (with a header row
  written).
- `last_written_snapshot` — `entity_fqn (PK), entity_type, written_fields_json, content_hash,
  updated_at`; incremental upsert; the merge base (§2.3).
- The Developer Portal `dataTypeSupport` property is set to `authoritative` in Phase 6 so the `schema`
  manifest is honoured (else it is silently downgraded to legacy hints). The write path honours
  `KBC_DATA_TYPE_SUPPORT` for *this component's own output manifest* — when the var is **absent/`None`**
  (gate off, before the portal switch is flipped) it falls back to the legacy `columns` +
  `column_metadata` format so the report table still loads correctly. `config_row_id` is taken from
  `KBC_CONFIGROWID`, which is **absent (`None`), not empty**, on a non-row run — handled gracefully
  (recorded as null in the report).

### 6.5 Reading Keboola native types (into OM columns)

Map order per source column, driven **purely off what the Storage API returns for that table**: typed
`definition.columns[].definition.type` (or its `basetype`) when the table carries a native
`definition` → legacy `KBC.datatype.basetype` column-metadata → `UNKNOWN`. This mapping is
**independent of `KBC_DATA_TYPE_SUPPORT`**: that env var governs the *format of the component's own
output manifest* (§6.4) and reflects the *host* project's feature gate — for a remote project
catalogued through a row `#storage_token` it says nothing about that source at all, and it is
**absent/`None`** (not one of three literal values) when the gate is off. Never emit the `dataLength:1`
placeholder for unknown-length varchars (OM does not enforce it). This is a **read** mapping into an
OM payload — it does not load into Snowflake, so the "declared numeric type can lie" hard-fail risk
does not apply here; we still map conservatively and preserve `dataTypeDisplay`.

### 6.6 Clean-room & licensing

The reference connector is **design reference only** — reimplement, copy nothing. Four files carry
**Collate Community License** headers (`connection.py`, `storage/source.py`,
`storage/database_lineage.py`, `pipeline/lineage/transformation_lineage.py`); the repo LICENSE is
**MIT (Keboola)**. Our `mapping/*` and `client/*` modules are written from the *understanding* of the
FQN / table-type / mapping logic, so **no Collate header and no customer identifier ever lands in
`src/`**. This matters most for the now-in-scope lineage work: two of the four Collate-headed files
(`storage/database_lineage.py`, `pipeline/lineage/transformation_lineage.py`) are lineage code — our
`src/lineage/` package is built **clean-room from `lineage-spike/` (our own PoC, no licence issue)**
and the OM `entityLineage` schema understanding, copying nothing from them; the reference's
per-component-type lineage extractor tree is explicitly NOT reused (research §6). The customer's name
(present in the local reference materials) must not appear in any committed artifact.

### 6.7 Key dependencies

- `keboola.component` (Common Interface: config, state, manifests, sync actions).
- `requests` (OM + Storage REST).
- `pydantic` (config model).
- `sqlglot` (E17 column-level lineage — parse + dialect handling; seeded by `lineage-spike/`).
- `sshtunnel` (only when `use_ssh_tunnel`).
- **No** `openmetadata-ingestion` / OM SDK (§3.3).

---

## 7. Testing — enumerate the cases up front

VCR against **both** the OM REST API and the Keboola Storage API (record from the OM public sandbox /
local quickstart + a scratch Keboola project; no customer creds). Full rules:
`component-test/references/vcr-configs-format.md`.

| Case | Kind | Covers |
|---|---|---|
| `01_testConnection_ok` | sync-action ok | OM version probe + bot token + Storage token valid |
| `02_testConnection_bad_bot_token` | sync-action fail | OM `401` surfaced in UI |
| `03_testConnection_unreachable` | sync-action fail | connection error surfaced in UI |
| `04_run_catalog_full_first` | run | E1–E4/E8/E9 first run, empty snapshot → all created |
| `05_run_catalog_incremental_skip` | run | unchanged bucket digest → skipped; changed → re-pushed |
| `06_run_view_and_external` | run | E5/E6 View + ViewLineage edge; E7 External table |
| `07_run_lineage_declared` | run | E16 edges from producer `storage.input/output`; `Manual` untouched |
| `08_run_merge_diverged` | run edge | OM value ≠ base → `skipped_diverged` + report row |
| `09_run_merge_keboola_wins` | run | `merge_mode=keboola_always_wins` overwrites divergence |
| `10_run_tombstone_self_diff_1_13` | run | ≤1.13.x soft-delete of missing entity; fail-closed on short scope |
| `11_run_tombstone_deletestale_om2_0` | run | 2.0+ `deleteStale` dryRun then apply, version-gated |
| `12_run_missing_bot_token` | run fail (exit 1) | auth error path |
| `13_run_unreachable_host` | run fail (exit 1) | reachability error path |
| `14_run_empty_project` | run edge | zero buckets → still emits `catalog_run_report`, no crash |
| `15_run_native_types` | run | typed `definition` → OM DataType; legacy fallback; `UNKNOWN`; no `dataLength:1` |
| `16_run_multi_project_rows` | run | host-project row (forward_token) + remote row (`#storage_token`) → one catalog |
| `17_run_failure_mode_collect_and_fail` | run fail (exit 1) | one entity `500` → collected, report row, exit 1 at end |
| `18_run_failure_mode_log_only` | run (exit 0) | one entity `500` → logged, report row, exit 0 |
| `19_run_ssh_tunnel` | run | `use_ssh_tunnel=true` path (tunnel mocked/loopback) |
| `20_run_pipeline_from_config` | run | E13 config → Pipeline + Tasks (`taskSQL`, `downstreamTasks`) + PipelineService |
| `21_run_pipeline_from_flow` | run | E14 flow/orchestration phases → Pipeline + Tasks (phase ordering) |
| `22_run_pipeline_status` | run | E15 `GET /jobs/{id}/open-api-lineage` → `PUT /pipelines/{fqn}/status` |
| `23_run_column_lineage_snowflake` | run | E17 Snowflake SQL → `columnsLineage`; `source=QueryLineage`; `resolved` taxonomy |
| `24_run_column_lineage_unresolved` | run edge | parse miss → `unresolved` in taxonomy + report; run continues (exit 0) |
| `25_run_column_lineage_dialect` | run | non-Snowflake dialect (BigQuery/Redshift/Synapse) casing; `tempLineageTables` chaining |
| `26_run_tier2_all_projects` | run | Tier-2: `#manage_token` enumerate org projects + mint per-project tokens → one catalog |
| `27_run_tier2_degrade` | run edge | manage token insufficient scope → degrade to host project + `degraded` drift row, exit 0 |
| `28_run_tier2_missing_manage_token` | run fail (exit 1) | `project_scope=all_projects` + no `#manage_token` → config error |
| `29_run_om2_0_bulk_override` | run | 2.0+ `PUT /tables/bulk` + version-gated `?overrideMetadata=`; `deleteStale` |

**VCR sanitizers (`VCR_SANITIZERS` wired in `component.py`):** `#bot_token`, `#storage_token`,
`#manage_token`, `KBC_TOKEN`, minted per-project tokens, the OM host, SSH `#private_key`, and any
project id / name / user identifier (incl. in transformation SQL) in OM, Storage, Management, or Job
Queue responses. The Phase-2 research captured OM `PUT /tables` and `PUT /lineage` payload shapes that
can seed cassettes; the OM public sandbox (1.13.4) / local quickstart records the pipeline surface, and
a local 2.0 RC records the 2.0-only surface (`deleteStale`, `overrideMetadata`).

---

## 8. Deployment & validation (CF test / cf-dev project)

- Register / update the app via **`kbagent dev-portal`** (Phase 6): configSchema + configRowSchema,
  the `testConnection` sync action, `dataTypeSupport=authoritative`, and portal-owned descriptions /
  URLs. Request `forward_token` + `forward_token_details` enablement (Keboola-staff action) early.
- **Phase-7 smoke** (needs a reachable test OM instance — OM sandbox or a `docker compose` tenant
  reachable from cf-dev egress): create a cf-dev config via `kbagent` with `runtime.tag` overridden to
  the `initial-implementation` branch build, one row for the cf-dev project (forward_token), run it.
- **A successful run** creates in OM: one DatabaseService + PipelineService, one Database (the cf-dev
  project), a DatabaseSchema per bucket, a Table per table (with columns + PK), Pipeline entities for
  the project's configs/flows with run-history status, and both declared `PipelineLineage` and computed
  `QueryLineage` edges; and in Keboola: a `catalog_run_report` table with `created`/`updated` rows and
  no `failed` rows (some `unresolved` column-lineage rows are expected on non-SQL/unknown-dialect
  transforms). Re-running with no metadata change → mostly `skipped_unchanged`.
- **Tier-2 smoke** (if a `manage:storage-tokens` token is obtainable in cf-dev): a single org-row run
  enumerates + catalogs multiple projects; otherwise confirm the `degraded` fallback path (host project
  only + drift row).

---

## 9. Testing/runtime gates pre-decided (for the downstream gates)

- **Phase-5 testing gate:** every in-scope §4 capability (catalog, pipelines E13/E14/E15, both lineage
  tiers, both multi-project tiers) and every §5 sync action/run mode has a §7 case; cassettes carry
  real payloads and are sanitised.
- **Column-lineage coverage gate (E17):** a CI check runs the `lineage/resolution.py` taxonomy over a
  fixed SQL corpus (seeded from `lineage-spike/corpus_run.py`) and fails if the `resolved` fraction
  regresses or if any case silently drops from `resolved` to `no-upstream`/`unresolved` (the CTAS
  silent-under-report landmine, research §7 Blocker 5). Precision is spot-checked by a sampled human
  review pass, per dialect.
- **Phase-7 runtime gate:** no silent default persists in a fresh config (§5.2); every enum/
  multi-select stores the value not the label; row-based config requires `rows` ≥ 1.
- **Static gates (Phase 4/6):** `run()` is a thin orchestrator (§6.2); one Pydantic config model
  (§6, `configuration.py`); incremental state storage + restore is spelled out (§2.3); native-type
  manifest + `dataTypeSupport` handling is explicit (§6.4).

---

## 10. Open risks & blockers

| # | Risk / blocker | Severity | Owner / mitigation |
|---|---|---|---|
| 1 | **OM reachability from Keboola egress** (customer runs OM in-cluster) | High (deployment, not build) | ship the optional SSH-bastion field + document static-egress-IP allowlisting; confirm with the customer before Phase-7 smoke. Not a build gate. |
| 2 | **`forward_token` / `forward_token_details` enablement** needs Keboola staff (admin Dev-Portal endpoint) | High (build prerequisite) | request early in Phase 4/6; without it the single-project story has no headless Storage token. |
| 3 | **Three-way-merge snapshot at 5k-table scale** — snapshot table read-back + digest must stay within state ≤1 MB and Storage read latency | Medium | per-bucket digest in state; full field snapshot in a Storage table read via API (§2.3); bulk endpoints for throughput; periodic full refresh. |
| 4 | **Tombstoning correctness** — no native bulk reconcile on 1.13.4; risk of over-deletion | Medium | self-diff + **fail closed** on list error/short scope; `deleteStale` dryRun on 2.0+ only; report every tombstone. |
| 5 | **No GA 2.x yet** — verification showed 2.0.0 is a **pre-release/RC** (surface may change before GA); latest stable is 1.13.4 | Medium (resolved by design) | §3.5 (verified, cited): target 1.13.4 as guaranteed; the 3 2.0-only niceties are probe-gated with 1.13.4 fallbacks and never hard-depended on. Re-confirm the 2.0 surface at GA before enabling it by default. |
| 6 | **Tier-2 super-token provisioning + org blast radius** — `manage:storage-tokens` is likely internal/partner-provisioned and org-wide-scoped | High (provisioning, not build) | build both tiers with **graceful degradation to Tier-1** (§5.4); host in a locked-down admin project; mint only short-lived read-only tokens; log the project list per run. Phase-7 provisioning dependency. |
| 7 | **Column-lineage precision + non-Snowflake dialects** — recall proven on Snowflake; precision spot-checked; other dialects unmeasured | Medium | resolved/no-upstream/unresolved taxonomy + CI coverage gate (§9); per-dialect casing in `lineage/dialect.py`; sampled human review; CTAS silent-under-report guarded by the gate. |
| 8 | **Clean-room / Collate headers** (esp. the two lineage files) | Low (handled) | reimplement from understanding + our own `lineage-spike/`; no Collate header or customer identifier in `src/`; repo LICENSE stays MIT. |
| 9 | **Descriptions are semantically thin** (few Keboola assets carry prose) | Low (expectation) | set expectations; don't fabricate; still map `KBC.description` when present. |
| 10 | **Scope growth** — maximal scope materially enlarges Phase 4/5/7 (pipelines, column lineage, Tier-2) | Medium (expected) | plan re-decomposed into more granular tasks (see the plan); each capability independently gated. |
