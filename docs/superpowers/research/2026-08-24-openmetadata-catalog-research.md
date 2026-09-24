# Phase 2 Research — Keboola → OpenMetadata Catalog Writer

**Component:** `keboola.wr-openmetadata-catalog` (vendor/app-id TBD — see Blocker 7)
**Type:** push-based writer. Runs as a Keboola job, reads project metadata via Storage/Job-Queue APIs,
pushes to a customer's OpenMetadata (OM) REST API. Clean-room reimplementation — **no** OM SDK, **no**
copied reference-connector code (Collate-licensed / customer-owned; see Blocker 6).
**Author of this note:** Phase 2 research teammate. This is DATA for the plan lead, not a human report.

> Sourcing: the reference connector was read only to understand mapping / FQN / table-type logic.
> Its customer identity (name appears in file headers and hardcoded URLs) is deliberately omitted from
> every artifact per the handoff rules. All contract facts below are verified against OM/Keboola
> primary docs or the reference code; live-tenant values remain assumptions until creds exist.

---

## 0. TL;DR verdict

**FEASIBLE and worth building as a general catalog-integration story — conditional on one blocker
(network reachability of the customer's OM API from Keboola egress).** Everything else is either
settled by public docs or degrades gracefully. The push direction is fully supported by OM
(`PUT /api/v1/tables`, `PUT /api/v1/lineage` are FQN-keyed upserts). Keboola-side auth for the
single-project case is free via `forward_token`. Column-level SQL lineage is proven (not a research
risk) on Snowflake; other dialects are measured-work, not unknowns.

The catalog-only Phase 1 will **not** displace the requesting customer's own connector (they already
have column + cross-warehouse lineage), so the business justification is "catalog integration for
everyone", not "unblock this customer". That is a product call for the lead, recorded here.

---

## 1. Feasibility & provisioning verdict

| Dimension | Verdict |
|---|---|
| Direction (push) | **Supported.** OM entity + lineage endpoints are `createOrUpdate` (PUT) upserts keyed on FQN. Re-running updates rather than duplicates. |
| Keboola auth (single project) | **Free / headless.** `forward_token` injects the running project's Storage token + URL. No credential in config. |
| OM auth | **Headless JWT bot token.** One encrypted `#bot_token` secret. No admin-UI click-through per run once a bot exists (bot creation is a one-time OM-admin step). |
| Sandbox | OM ships a public sandbox (sandbox.open-metadata.org) + trivial local `docker compose` / `metadata docker` quickstart → contract verification needs **no** customer creds. Customer-tenant creds needed only for live smoke test. |
| Column lineage feasibility | **Proven**, not a research risk (corpus run: every traceable column recovered on 42 Snowflake transforms, 0 parse failures). Remaining risk is *precision* + *per-dialect casing*, not feasibility. |
| **Blocker** | **Network reachability** of the customer's in-cluster OM from Keboola's static egress IPs. Resolve with the customer before build. SSH-proxy precedent exists (`keboola.wr-openlineage`). |

Provisioning steps a customer performs once (documentable, not per-run):
1. Create an OM bot + JWT token (OM UI: Settings → Bots, or ingestion-bot), copy the token.
2. Ensure the OM API base URL is reachable from Keboola egress (or provide SSH bastion).
3. (Multi-project) mint a read-only Storage token per additional project → paste into that config row.

---

## 2. COMPLETE capability inventory (the full menu)

This is the independent baseline for the Phase-3 scope gate. Default posture: **everything here is
in-scope** unless the user opts a row out against this menu. Phase (P1/P2/P3) is a *sequencing*
recommendation, not a scope cut.

### 2a. OM entity families we can WRITE (target surface)

| # | OM entity | Endpoint (createOrUpdate) | Fed from (Keboola) | Phase | Notes |
|---|---|---|---|---|---|
| E1 | **DatabaseService** (type `Database`/custom) | `PUT /api/v1/services/databaseServices` | Stack (`KBC_STACKID`) | P1 | One per stack. FQN root — near-permanent, decide naming before v1. |
| E2 | **Database** | `PUT /api/v1/databases` | Project (name sanitised) | P1 | `sourceUrl` → project deep link. |
| E3 | **DatabaseSchema** | `PUT /api/v1/databaseSchemas` | Bucket (`bucket.path` name, `displayName`, description) | P1 | `sourceUrl` → bucket. |
| E4 | **Table (Regular)** | `PUT /api/v1/tables` | Table in OUT/IN bucket | P1 | columns, PK, rowCount/size/lastImport, description, sourceUrl. |
| E5 | **Table (View)** | `PUT /api/v1/tables` (tableType=View) + lineage | Linked/shared bucket (stage=in + `sourceBucket`) | P1 | `schemaDefinition` = `CREATE VIEW … SELECT * FROM <source fqn>` + a lineage edge. The reference's best generic idea. |
| E6 | **Table (View) from alias** | same | Table alias | P1 (small) | Reference connector did **not** handle this — an improvement we can add. |
| E7 | **Table (External)** | `PUT /api/v1/tables` (tableType=External) | Bucket with `hasExternalSchema=true` & no sharing | P1 | External-schema buckets. (Cross-warehouse *lineage* off them is out of scope; the External *table* entity is in.) |
| E8 | **Column + dataType** | part of Table body | Table columns | P1 | Typed `definition` datatype first (fix vs reference which only read legacy `KBC.datatype.type`), `basetype` fallback, UNKNOWN last. arrayDataType, dataLength (only when known — never `1` placeholder). |
| E9 | **tableConstraints (PRIMARY_KEY)** | part of Table body | table `primaryKey` | P1 | Reference parsed PK then never mapped it — fix. |
| E10 | **Descriptions** (schema/table/column) | part of entity body | `KBC.description` metadata | P1 | Semantically thin in practice (0/14 sampled tables had prose) — set expectations. |
| E11 | **sourceUrl deep links** | field on E2–E6 + pipelines | Keboola UI URL derived from stack + project/bucket/table id | P1 | Reference hardcoded the UI base URL — we derive it. |
| E12 | **Custom properties / tags** | tag on entities; classification | entity type, `KBC.createdBy.*` | P1 (tags optional) | Reference tagged pipelines with entity-type classification. |
| E13 | **Pipeline (from component config)** | `PUT /api/v1/pipelines` (+ `PUT /api/v1/services/pipelineServices`) | Component configuration | P1/P2 | config → Pipeline; rows/blocks/codes → Tasks; block SQL → `taskSQL`; `downstreamTasks` ordering. |
| E14 | **Pipeline (from flow/orchestration)** | same | Flow / conditional flow | P1/P2 | phases → Tasks with downstream ordering. |
| E15 | **Pipeline status / run history** | `PUT /api/v1/pipelines/{fqn}/status` | Job Queue job runs | P2 | success/fail + timestamps. |
| E16 | **Lineage — table-level (declared)** | `PUT /api/v1/lineage` | `storage.input.tables`/`storage.output.tables` + `KBC.createdBy.*` | **P1** | Free, every component, no SQL. Coarse (all-inputs→all-outputs). Tag `source=PipelineLineage`. |
| E17 | **Lineage — column-level (SQL)** | `PUT /api/v1/lineage` w/ columnsLineage | SQLGlot over transformation SQL, resolved via storage mapping | **P2** | Snowflake proven; BigQuery/Redshift/Synapse ≈1 wk each. Tag `source=QueryLineage`. |
| E18 | **Glossary / GlossaryTerm** | `PUT /api/v1/glossaries`, `PUT /api/v1/glossaryTerms` | Keboola semantic layer (glossary) | P3 | Untouched by reference connector — where we beat it. |
| E19 | **Metric** | `PUT /api/v1/metrics` (VERIFIED first-class) | Keboola semantic-layer metrics | P3 | Same. `PUT /api/v1/dataQuality/testCases` also available if we ever surface data-quality. |

### 2b. Keboola objects we MAP FROM (source surface)

Stack · Project · Bucket (in/out, stage, sharing, path, description, sourceBucket, hasExternalSchema)
· Table · Linked/shared bucket · Table alias · Column · Column datatype (typed `definition` + legacy
`KBC.datatype.*`) · Primary key · Descriptions (bucket/table/column via `KBC.description`) ·
`KBC.createdBy.*` / `KBC.lastUpdatedBy.*` metadata (producing config + branch) · Component
configuration · Config rows · Transformation blocks / codes / SQL · Flow / orchestration + phases ·
Job / run history · Semantic-layer glossary · Semantic-layer metrics · Keboola UI deep links ·
Stages · Branches (production-only filter, made configurable — reference hardcoded it).

### 2c. Lineage tiers

1. **Declared table-level** — from each config's `storage.input/output.tables` (+ `KBC.createdBy.*`
   corroboration, + `sourceBucket` for source→linked-bucket view edges). Free, all component types,
   no SQL parsing, no allowlist. **P1.** Honest coarseness: N inputs × M outputs (does not say which
   input feeds which output).
2. **Column-level (SQL)** — SQLGlot over transformation SQL, workspace names resolved to storage IDs
   by the config's storage mapping, chained through workspace intermediate tables. **P2.** Proven on
   Snowflake; per-dialect casing + measurement for BigQuery/Redshift/Synapse. Needs a
   resolved/no-upstream-exists/genuinely-unresolved taxonomy wired to a CI coverage gate (the CTAS
   silent-under-report bug is the reason).
3. **Cross-warehouse** (Keboola table ↔ customer's own BigQuery/Snowflake tables) — **OUT OF SCOPE.**
   Needs warehouse creds the component must not hold; this is the one thing the reference connector
   does that we deliberately do not match (it uses a GCP service account + Resource Manager).

Non-SQL transforms (Python/R), extractors, writers → table-level only (E16). Tag edges with
`lineageDetails.source` so the UI distinguishes computed (QueryLineage) vs declared (PipelineLineage);
never touch `Manual` edges.

---

## 3. Mapping & FQN scheme (from reference, verified against OM entity model)

- FQN scheme: `service.database.schema.table` → `<stack-service>.<project>.<bucket.path>.<table.name>`.
  Deterministic from stack + project so N project-rows form one coherent catalog (linked bucket in
  project B lands on the node project A created). **Service naming is near-permanent** — decide
  service-per-stack vs service-per-project before v1 (recommend service-per-stack).
- Names sanitised (spaces→underscores; avoid dots in table names) for clean FQNs; original kept as
  `displayName`.
- Table-type detection (from reference `storage/source.py`, to reimplement clean-room):
  - `hasExternalSchema=true` AND sharing∈{none,null} → **External**
  - stage∈{out,shared,linked} AND sharing∈{specific-projects,none,null} → **Regular**
  - stage=in AND `sourceBucket` present → **View** (+ view DDL + lineage edge)
  - else → **Regular**
- Column datatype: request the typed `definition` datatype (reference bug: only fetched
  `?include=metadata,columnMetadata`, so native types were missed); fall back to
  `KBC.datatype.basetype`, then UNKNOWN. Do **not** write `dataLength:1` for unknown-length varchars.
- PK → `tableConstraints[].constraintType=PRIMARY_KEY, columns=[…]`.

---

## 4. Merge / incremental / errors (design facts that shape the writer, confirmed by assessment)

- **Three-way merge**, base = the value we last wrote (NOT push-if-empty, which goes stale forever).
  Update when OM still matches what we wrote; leave + log divergence when a human edited it; write
  when empty. Same diff logic for lineage graphs (remove our stale edges, add new, never touch Manual).
  Optional "Keboola always wins" override. Requires a per-field record of what we last wrote — same
  store the incremental design needs (one mechanism, two problems).
- **Incremental**: per-bucket digest in `state.json` (state ≤ ~1MB; a project can hold 5k+ tables so
  per-table hashes won't fit) OR full manifest in a component Storage table + pointer in state.
  Periodic full refresh regardless. Lineage dirty-check off config version.
- **Deletion / tombstoning**: no framework `markDeletedTables` on the push side. For OM ≤1.13.x
  (realistic target) we diff ourselves (list what the service holds, soft-delete what Keboola no longer
  has) and **fail closed** (skip tombstoning if the list errors/looks implausibly short). OM 2.0.0+
  offers native `DELETE /api/v1/tables/deleteStale` (reconcile-by-`seenFqns`, has `dryRun`) — use it
  when the server supports it (version-probe gated). For lineage, the delete-all-by-`source` endpoint
  (§8.4) is the clean diff primitive: drop our `QueryLineage`/`PipelineLineage` edges, keep `Manual`.
- **Failure mode toggle**: `fail_fast` / `collect_and_fail` (default) / `log_only`. Setup failures
  (unreachable host, bad token, unresolvable service) ALWAYS fail (UserException, exit 1). Delete path
  ALWAYS fails closed. Write per-entity failures to an output table, not just the log.

---

## 5. Multi-project / org scope (Keboola side)

- **Tier 1 (buildable today):** config **rows**, one per project, each with its own encrypted
  read-only `#storage_token`; per-row state; enable/disable per row = the enterprise allowlist.
  Single-project row needs no token (forward_token). FQN scheme keeps N rows → one catalog.
- **Tier 2 ("all projects" from one credential):** optional `#org_credential`. Depends on platform
  answers — see Blockers 2 & 3. Degrades gracefully to a discovery/drift report + per-project rows if
  token delegation isn't free.
- **Security shape:** org credential (if used) only to *enumerate + delegate*, never to read data;
  host it in a locked-down admin project (that project becomes the org blast radius); log the exact
  project list per run.

---

## 6. Reuse from the handed-over assets (clean-room)

- **Reusable as design reference only (reimplement, do not copy):** storage→OM mapping shape, FQN
  building, table-type detection, linked-bucket-as-view idea, pipeline/flow→Pipeline/Task mapping.
- **`lineage-spike/` (OURS):** `spike.py` + `corpus_run.py` are our own SQLGlot PoC — reusable as the
  literal seed of the P2 lineage module (no license issue).
- **Do NOT reuse:** anything importing `metadata.ingestion.*` / OM SDK generated schema; the
  per-component-type lineage extractor tree (`pipeline/lineage/*` — an allowlist treadmill); the
  BigQuery cross-warehouse flow (`storage/database_lineage.py`); the 4 Collate-licensed files.

---

## 7. Blockers (from SPEC §"Must discover / verify") — verdicts

| # | Blocker (SPEC §"Must discover / verify") | Verdict |
|---|---|---|
| **1** | **Reachability** of customer-internal OM from Keboola egress; SSH-proxy precedent. | **NOT-BLOCKING technically — one customer confirmation needed.** Both mitigations verified (§9.6): allowlist Keboola's published static outbound IPs (`help.keboola.com/components/ip-addresses/`), or ship an SSH-bastion proxy config field (precedent: `keboola.wr-openlineage`, `keboola/ssh-tunnel`). **Action:** ask the customer "is the OM API reachable from our egress IPs, or must we tunnel?" before build. Build v1 with an optional SSH-proxy field so either answer works. This is the only true gating unknown, and it's a yes/no question, not a design risk. |
| **2** | **OAuth vs org-token** for cross-project (can a component get a user-scoped OAuth session vs Keboola's own API?). | **OPEN — platform-team question, but NO LONGER a prerequisite.** No public evidence a component obtains a user-scoped OAuth session against Keboola's own API via the component OAuth broker (broker targets third-party services). BUT the token-exchange auth-bridge (`resolveStorageToken`, §9.7) shows a subject-token→storage-token path exists server-side. Verdict: OAuth would give the cleanest trust boundary, but Blocker 3's positive result means Tier-2 ships **without** it. Keep as a "nice-to-have, confirm with platform team", not a build gate. |
| **3** | **Manage-token delegation** — can a Management API token enumerate org projects AND get Storage access to each? | **CAPABILITY VERIFIED (revises the assessment); credential-obtainability is the remaining OPEN question.** Both operations are real manage-API endpoints (§9.7): `GET /manage/organizations/{id}/projects` enumerates, and `POST /manage/projects/{id}/tokens` (`createProjectStorageToken`) mints a Storage token in a member project **without a pre-existing token there**. So the assessment's pessimism ("minting needs canManageTokens *inside* each project; access ≠ enumeration") was wrong *for the manage-side path* — access DOES come with enumeration. **BUT** `createProjectStorageToken` requires a **super/application** manage token carrying the `manage:storage-tokens` scope (per the manage-API client's documented test scopes) — a higher-privilege, Keboola-internal/partner token class, **not** something an ordinary org admin self-provisions from the UI. **Verdict:** Tier-2 "one credential → all projects" is technically buildable (not degraded to a discovery/drift report), *conditional on the customer/account team being able to obtain a `manage:storage-tokens` super token* — confirm with the platform/account team (INFERRED that it needs a support/partner request). Security caveats hold: it's a manage credential → org blast radius → host in a locked-down admin project, use only to enumerate+mint short-lived read-only per-project tokens, log the project list per run. Fallback if the credential can't be obtained: ship Tier-1 rows + the discovery/drift report. |
| **4** | **OM versions to support** → REST surface + smoke matrix. | **SETTLED (customer input needed for the exact pin).** Contract stable across 1.9 → 2.0 (1.13.4 on 2026-08-21; 2.0.0 GA'd 2026-08-24). Floor on **1.13.x** compatibility (mature, widely deployed); probe `GET /api/v1/system/version` and gate any 2.0-only surface (`deleteStale`, bulk `overrideMetadata`) on it. Hand-rolled REST client targets the range — no SDK version-lock / py-floor cap. **Action:** ask the customer their OM server version; smoke-test that version + current latest. |
| **5** | **Lineage precision + non-Snowflake dialects.** | **NOT a feasibility risk — scoped work.** Recall proven (corpus: every traceable column, 0 parse failures, Snowflake). Open items are (a) **precision** — spot-checked, not measured → add a sampled human-review pass + a resolved/no-upstream/unresolved taxonomy wired to a CI coverage gate; (b) **dialects** — only Snowflake measured; BigQuery/Redshift/Synapse each ≈1 wk (SQLGlot supports them; casing rules differ). P2, per-dialect, behind the coverage gate. Known landmines to design in: identifier casing, CTAS-shape silent under-reporting, multi-step chaining via `tempLineageTables`, stale/dangling configs, UPDATE…SET…(SELECT). |
| **6** | **Licensing** (clean-room requirement). | **BLOCKING for code reuse — must get written relicense before any handed-over code lands.** 4 files carry Collate Community License headers (incl. `storage/source.py`); the rest is customer-owned. Our plan is already **clean-room**: reimplement from the mapping/FQN/table-type *understanding*, copy nothing. `lineage-spike/` is ours (no issue). **Action:** (a) proceed clean-room regardless; (b) obtain written relicense permission from the customer for the connector, and note the Collate files derive from OM's own templates (Apache-2.0 upstream) — legal to confirm. Nothing copied = licensing does not gate the build, only would gate any copy-paste (which we forbid). |
| **7** | **Vendor / app-id** (`keboola.*` vs `kds-team.*`); does an OM component already exist? | **DECISION NEEDED (no live OM component today).** Assessment verified **no OpenMetadata component under the `keboola` vendor**; related precedents exist (`keboola.wr-openlineage`, `keboola.wr-google-data-catalog-openlineage-writer`). Recommend **`keboola.wr-openmetadata-catalog`** (first-party writer, aligns with sibling `keboola.wr-openlineage`). If it's to be a KDS-team/community build, `kds-team.wr-openmetadata-catalog`. **Action:** lead confirms vendor; then reserve the app-id in the Dev Portal. Not a blocker to research/spec; needed before scaffold/Phase-1 release.

---

## 8. OpenMetadata REST API — verified contract

Base path `<host>/api/v1`. Facts VERIFIED against OM JSON schemas + JAX-RS resource Java source at
release tags `1.13.4-release` (2026-08-21) and `2.0.0-release` (2026-08-24) unless marked INFERRED.

**8.1 Auth (VERIFIED).** JWT **bot token**. In OM UI → Settings → Bots, use `ingestion-bot` (system
bot, non-deletable) or add a custom bot; copy its JWT ("Generate new token"). Call the API with
`Authorization: Bearer <jwt>`; the token carries **full API privileges** → the single `#bot_token`
secret. Programmatic token mgmt: `PUT /api/v1/users/generateToken/{id}` (body
`GenerateTokenRequest{id, JWTTokenExpiry}`), `GET /api/v1/users/token/{id}`,
`PUT /api/v1/users/revokeToken`. `JWTTokenExpiry` enum: `OneHour, 1, 7, 30, 60, 90, Unlimited` (days).
Recommend a dedicated custom bot with a long/`Unlimited` expiry (rotated) → no refresh lifecycle.
Token types: `BOT, OM_USER, PERSONAL_ACCESS`. (A local basic-auth `POST /api/v1/users/login` +
`/refresh` exists but is not for M2M.) JWKS validation endpoint: `GET /api/v1/system/config/jwks`.

**8.2 Entity upsert = PUT (VERIFIED, FQN-keyed createOrUpdate — create if absent, update if present).**
- `PUT /api/v1/services/databaseServices` (**note the `/services/` segment** — not
  `/api/v1/databaseServices`), `PUT /api/v1/databases`, `PUT /api/v1/databaseSchemas`,
  `PUT /api/v1/tables`, `PUT /api/v1/services/pipelineServices`, `PUT /api/v1/pipelines`.
- `PUT /api/v1/lineage` (`addLineageEdge`).
- `PUT /api/v1/glossaries`, `PUT /api/v1/glossaryTerms`, `PUT /api/v1/metrics` (P3 — Metric IS a
  first-class createOrUpdate), `PUT /api/v1/dataQuality/testCases`.
- **Bulk upsert:** `PUT /api/v1/tables/bulk` (`bulkCreateOrUpdateTables`, exists ≥1.13.4) — throughput
  path for 5k-table projects; 2.0.0 adds `?overrideMetadata=` to force-overwrite curator fields.
- Field-level curation-safe update (VERIFIED): `PATCH /api/v1/tables/{id}` AND
  `PATCH /api/v1/tables/name/{fqn}`, `Content-Type: application/json-patch+json` (RFC-6902). Used for
  three-way-merge writes so we touch only fields we own. PATCH also exists on lineage edges.

**8.3 `createTable` body (VERIFIED).** Required: `name`, `columns`, `databaseSchema` (parent **FQN**).
Also set: `displayName, description, tableType, tableConstraints, schemaDefinition` (the DDL field —
`viewDefinition` was removed long ago, ≤1.9.0; use `schemaDefinition`), `sourceUrl, retentionPeriod`
(ISO-8601 duration), `tags, owners`. Profile written separately, not in create body.
- `tableType` enum (full): `Regular, External, Dynamic, View, SecureView, MaterializedView, Iceberg,
  Local, Partitioned, Foreign, Transient, Stream, Stage, SemanticView`. We use **Regular / View /
  External**.
- `Column`: `name, displayName, dataType, arrayDataType, dataLength, precision, scale, dataTypeDisplay,
  description, ordinalPosition, constraint, children[]` (struct/map/union). `constraint` enum: `NULL,
  NOT_NULL, UNIQUE, PRIMARY_KEY`. Required per-column: `name, dataType`.
- `DataType` enum is ~85 values incl. `NUMBER, INT, BIGINT, DECIMAL, NUMERIC, FLOAT, DOUBLE, VARCHAR,
  CHAR, STRING, TEXT, DATE, TIMESTAMP, TIMESTAMPZ, TIME, ARRAY, MAP, STRUCT, JSON, UUID, BOOLEAN,
  BYTES, UNKNOWN`. **`dataLength` is optional and NOT server-enforced for VARCHAR/CHAR** (contrary to
  the reference's belief) → safely **omit** when unknown; never write the `1` placeholder.
- `tableConstraints[]`: `{constraintType, columns[], referredColumns[], relationshipType}`;
  `constraintType` enum `UNIQUE, PRIMARY_KEY, FOREIGN_KEY, SORT_KEY, DIST_KEY, CLUSTER_KEY`. PK →
  `PRIMARY_KEY`.
- Profile (`TableProfile`): `rowCount, columnCount, sizeInByte` (upstream doc-string wrongly says "GB";
  it is **bytes** — flag for any UI label), `timestamp` (required).

**8.4 Lineage body (VERIFIED, `type/entityLineage.json` + `LineageResource.java`).**
`PUT /api/v1/lineage` with `AddLineageRequest{edge: EntitiesEdge}`; `EntitiesEdge = {fromEntity,
toEntity, description?, lineageDetails?}`. Both entities **must already exist and be NON_DELETED**
(else 404) → catalog job runs before lineage. `LineageDetails`:
- `sqlQuery`, `columnsLineage[]` = `{fromColumns:[<fqn>...], toColumn:<fqn>, function?}`,
  `pipeline` (ref), `source`,
- `tempLineageTables[]` = `{fromEntity, toEntity}[]` — **hops through workspace/intermediate tables**
  (exactly the P2 chaining need).
- `source` enum (full): `Manual, ViewLineage, QueryLineage, PipelineLineage, DashboardLineage,
  DbtLineage, SparkLineage, OpenLineage, ExternalTableLineage, CrossDatabaseLineage, ChildAssets`
  (default `Manual`). We use **QueryLineage / PipelineLineage / ViewLineage**; never overwrite `Manual`.
- Add-by-FQN convenience (skip ID resolution):
  `PUT /api/v1/lineage/{fromEntity}/name/{fromFQN}/{toEntity}/name/{toFQN}`.
- Delete an edge (VERIFIED): `DELETE /api/v1/lineage/{fromEntity}/{fromId}/{toEntity}/{toId}` (+ FQN
  variant). **Delete-all-by-source:**
  `DELETE /api/v1/lineage/source/name/{entityType}/{entityFQN}/type/{lineageSource}` — removes every
  edge of one `source` value touching an entity. **This is the clean primitive for the lineage
  three-way diff**: replace all our `QueryLineage`/`PipelineLineage` edges on a table without touching
  `Manual` ones.

**8.5 Delete / tombstoning (VERIFIED — version-dependent).** Soft-delete a table:
`DELETE /api/v1/tables/{id}?hardDelete=false&recursive=false` (+ FQN variant), same shape on
schema/db. **Native bulk reconcile `DELETE /api/v1/tables/deleteStale` is 2.0.0-ONLY** (released
2026-08-24): body `BulkDeleteStaleRequest{scopeFqn, scopeEntityType, seenFqns[], dryRun, hardDelete,
recursive}` — report all FQNs seen this run; anything in-scope not in `seenFqns` is deleted; `dryRun`
previews. **For 1.13.x and earlier (the realistic target today) there is NO bulk equivalent → we
self-diff (list scope, soft-delete missing) and fail closed.** Gate any use of `deleteStale` behind a
version probe.

**8.6 List / pagination / fetch-by-FQN (VERIFIED).**
`GET /api/v1/tables?databaseSchema=<fqn>&fields=<csv>&limit=<n>&after=<cursor>&before=<cursor>&include=non-deleted`
— cursor pagination (`before`/`after` opaque, mutually exclusive; `limit` `@Min(0) @Max(1000000)
@DefaultValue(10)`). Fetch by FQN: `GET /api/v1/tables/name/{fqn}?fields=...`. `fields` = comma-sep
selection (e.g. `columns,tags,owners,tableConstraints,profile`). Practical caveat (INFERRED): huge
`limit` + `fields=columns` can time the server out — page modestly (100–500).

**8.7 Rate limits (VERIFIED-negative).** No documented server-side rate limiting; no `429`/`Retry-After`
contract (open unimplemented feature request confirms none). Self-impose bounded concurrency (a
handful) + own backoff on 5xx/connection errors.

**8.8 Error model (VERIFIED — Dropwizard `{code, message}`).** `400` bad request/malformed JSON
(fatal); `401` auth (missing/expired JWT, sets `WWW-Authenticate: om-auth`); `403` bot lacks
role/policy (config issue, fatal); `404` referenced entity/FQN absent (common on lineage before
catalog — actionable: create entity first); `409` `ENTITY_ALREADY_EXISTS` (mostly moot for PUT
upserts); **`412` precondition/version mismatch on PATCH → retry after refetch** (matters for the
merge PATCH flow); `5xx` retryable with backoff.

**8.9 Versions (VERIFIED via GitHub Releases).** `1.9.0` (2025-08) → `1.10` → `1.11` → `1.13.4`
(2026-08-21) → **`2.0.0` (2026-08-24, hours old at research time)**; ~one minor / 6–8 wks. The tables
+ table-lineage + `columnsLineage` + `tempLineageTables` + `schemaDefinition` surface is **stable
1.9 → 2.0 with no breaking renames**; only additions are 2.0-only (`deleteStale`, bulk
`overrideMetadata`). `openmetadata-ingestion` (Python SDK) is version-locked to the server (py floor
`>=3.9` on 1.13.x, `>=3.10` on 2.0) — the reason we **hand-roll a thin REST client over dicts** and
never import it. **Recommend: floor on 1.13.x compatibility (mature); probe `GET /api/v1/system/version`
and gate any 2.0-only surface on it.** Smoke-test the customer's exact version + latest (Blocker 4).

---

## 9. Keboola platform APIs — verified contract

**9.1 Injected env vars (VERIFIED, `common-interface/environment`).** Always present: `KBC_DATADIR`
(`/data/`), `KBC_PROJECTID`, `KBC_STACKID`, `KBC_CONFIGID`, `KBC_CONFIGVERSION`, `KBC_COMPONENTID`,
`KBC_CONFIGROWID`, `KBC_BRANCHID`, `KBC_RUNID`, `KBC_STAGING_FILE_PROVIDER`, `KBC_COMPONENT_RUN_MODE`,
`KBC_DATA_TYPE_SUPPORT` (`authoritative`/`hints`/`none` — tells us how far to trust native types).
- **`forward_token`** → injects `KBC_TOKEN` (the project Storage token) + `KBC_URL` (Storage API URL).
- **`forward_token_details`** → adds `KBC_PROJECTNAME`, `KBC_TOKENID`, `KBC_TOKENDESC`, `KBC_REALUSER`.
- **Caveat (VERIFIED):** `forwardToken`/`forwardTokenDetails` are app-record booleans set **only via
  the admin Dev-Portal endpoint (`PATCH /admin/apps/{app}`), not the vendor self-serve
  `PATCH /vendors/{vendor}/apps/{app}`** → a vendor cannot enable them themselves; it needs Keboola
  staff action. Provisioning step for the build — request it early. Gives the single-project story
  with zero credential in config, and the OM `Database` name free from `KBC_PROJECTNAME`.
  (`KBC_PROJECTID`/`KBC_STACKID` are always injected regardless — for FQN derivation — but the
  project *name* comes only from `forwardTokenDetails`.)

**9.2 Storage API reads (VERIFIED / assessment-live-verified).** Base = `KBC_URL` (e.g.
`https://connection.<stack>.keboola.cloud`), header `X-StorageApi-Token: KBC_TOKEN`.
- Buckets: `GET /v2/storage/buckets` (+ `?include=metadata` for the `metadata[]` KV list). Bucket
  fields used: `id, name, stage (in|out), displayName, description, path, sharing, sourceBucket,
  hasExternalSchema, backend`.
- Tables: `GET /v2/storage/buckets/{id}/tables` and detail `GET /v2/storage/tables/{id}` with
  `?include=columns,metadata,columnMetadata,buckets`. **Fix vs reference:** the reference fetched only
  `metadata,columnMetadata`, so it missed native types — we also request the typed definition.
- **Native/typed datatypes (VERIFIED, exact path from `storage-api-php-client/apiary.apib`):** a
  native-typed table (`isTyped=true`) carries a top-level `definition` object; per column the path is
  `table.definition.columns[].definition.{type, nullable, length}` (backend-native, always a base type
  — never alias/virtual) plus `table.definition.columns[].basetype`
  (STRING/INTEGER/NUMERIC/FLOAT/BOOLEAN/DATE/TIMESTAMP), and `table.definition.primaryKeysNames[]`.
  The spec states *"definition is available only for tables with native datatypes."* This is the modern
  source of truth, distinct from legacy `KBC.datatype.type`/`.basetype`/`.length` **column-metadata**
  entries (present on untyped/legacy tables). **Map order:** typed `definition.type` (or its `basetype`)
  → legacy `KBC.datatype.basetype` → `UNKNOWN`. `KBC_DATA_TYPE_SUPPORT` (`authoritative`/`hints`/`none`)
  says how far to trust it.
- **primaryKey (VERIFIED; assessment live 9/14 tables declared one):** table detail exposes a top-level
  `primaryKey: [colName,...]` (always present; also `definition.primaryKeysNames` on typed tables) →
  OM `tableConstraints` PRIMARY_KEY.
- System metadata keys present: `KBC.description`, `KBC.createdBy.component.id`,
  `KBC.createdBy.configuration.id`, `KBC.lastUpdatedBy.*`, `KBC.createdBy.branch.id` (the last is the
  production-vs-dev-branch filter the reference hardcoded — we make it configurable). Table stats:
  `rowsCount`, `dataSizeBytes`, `lastImportDate`.
- Linked/shared bucket: `sourceBucket` names the source project + bucket id + its tables → basis for
  the View entity + ViewLineage edge.
- Run-level lineage (VERIFIED, Job Queue swagger): `GET /jobs/{jobId}/open-api-lineage` returns the
  job as an OpenLineage START/COMPLETE event list (child jobs included for orchestration/phase jobs);
  same `X-StorageApi-Token` auth. Per-job — call once per job id. Feeds E15 pipeline-status / run
  history. This is exactly what `keboola.wr-openlineage` consumes.

**9.3 State (VERIFIED, `common-interface/config-file`).** `/data/in/state.json` in,
`/data/out/state.json` out; JSON; encrypted `KBC::ProjectSecure`; per-configuration and **per config
row**. Documented guidance: **"should not generally exceed 1 MB."** Behaves like a cookie, not a DB;
concurrent runs of one config race (last write wins). → drives per-bucket digest (not per-table) or
manifest-in-Storage-table + pointer-in-state.

**9.4 Config rows (VERIFIED, Configurations API + config-file spec).** One row per project; per-row
`parameters` (merged over root, row wins), **per-row isolated state** (root `state` unused when rows
exist; nothing shared between rows besides static config); `isDisabled` skips a row; order via
`rowsSortOrder`. **Rows run sequentially by default**; a root `parallelism` property (integer = max
concurrent rows, or `"infinity"` = all at once) opts into parallel — the plan must not assume parallel
rows unless it sets this. (Per-row *processor* mapping is not clearly documented — treat as unverified.)

**9.5 Encryption (VERIFIED).** Config/state fields whose key starts with `#` are encrypted at rest and
decrypted into the container at runtime. Scopes: `KBC::ProjectSecure` (default, project-bound),
`KBC::ComponentSecure`, `KBC::ConfigSecure`. → `#bot_token`, `#storage_token`, `#org_credential` are
`#`-fields.

**9.6 SSH proxy + outbound IPs (Blocker-1 mitigation).** VERIFIED: Keboola publishes static outbound
IP ranges per stack for firewall allowlisting (`help.keboola.com/components/ip-addresses/`; AWS stacks
carry reverse-DNS; machine-readable feed `/components/ip-addresses/kbc-public-ip.json`). VERIFIED:
config-based SSH-proxy pattern documented for Generic Extractor/DB extractors (block: `host, user,
port, #privateKey`; PHP lib `keboola/ssh-tunnel`). **CAVEAT (INFERRED, needs source check):** whether
a *custom Python* component gets a ready-made SSH-tunnel helper (and whether `keboola.wr-openlineage`'s
proxy is such a helper) is **not confirmed in public docs** — a Python writer would likely implement
the tunnel itself (e.g. `sshtunnel` PyPI) if needed. Net: internal-only OM is reachable via (a)
allowlisting Keboola egress IPs, or (b) an SSH-bastion proxy config field we implement. Confirm the
`wr-openlineage` proxy mechanism against its repo before citing it as a drop-in.

**9.7 Management API — org/project + token delegation (VERIFIED via `kbc-manage-api-php-client`
`src/Client.php`).** Base `https://management.<stack>` (e.g. `management.keboola.com`,
`management.eu-central-1.keboola.com`); manage token / org-member / maintainer / super-admin auth
(client also supports static JWT + k8s SA token strategies).
- **Enumerate projects:** `GET /manage/organizations/{organizationId}/projects`
  (`listOrganizationProjects`) — plus `listOrganizations`, `listMaintainerOrganizations`,
  `listOrganizationProjectsUsers`.
- **Mint per-project Storage access (the crux — REVISES the assessment):**
  `POST /manage/projects/{projectId}/tokens` (`createProjectStorageToken`) — a **manage** token creates
  a Storage token **inside a member project without already holding a token there.** So enumeration and
  access are NOT separate problems (the assessment assumed they were). **BUT the required credential is
  a super/application manage token carrying the `manage:storage-tokens` scope** (per the client's
  documented test scopes) — a higher-privilege, likely Keboola-internal/partner token class, **not**
  self-provisioned by an ordinary org admin from the UI (INFERRED it needs a support/partner request).
  → capability verified; customer-obtainability of the credential is the open item (Blocker 3).
- **Token exchange:** `POST /manage/internal/auth-bridge/resolve-storage-token`
  (`resolveStorageToken`, header `X-Subject-Token: Bearer <subjectToken>`, body `{projectId}`) —
  resolves a subject (user) token into a project Storage token. Present and VERIFIED as a client
  method, but under `/manage/internal/…` → treat as an **internal, possibly-unstable** contract;
  confirm with the platform team before depending on it.
- Contrast (still VERIFIED): a **Storage** token can only be created by a **master** Storage token
  inside its own project (`help.keboola.com/management/project/tokens`). The manage-API route above is
  the *only* way to get cross-project access from one credential — and it requires a **manage** token,
  which is the org blast-radius concern (§5 security shape).
