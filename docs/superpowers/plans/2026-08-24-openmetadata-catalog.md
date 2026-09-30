# keboola.wr-openmetadata-catalog — Implementation Plan

> Executes the design spec `docs/superpowers/specs/2026-08-24-openmetadata-catalog-design.md`.
> Built on branch `initial-implementation`. Run via `superpowers:subagent-driven-development`: one
> fresh subagent per task, reviewed between tasks. Each task names its **owner skill** so the subagent
> stays Keboola-aware, and its **gate** (the lifecycle-tracker phase whose exit-gate scope judges it).
> **Scope = maximal** (approved 2026-08-24): catalog (E1–E12), pipeline family (E13/E14/E15), both
> lineage tiers (E16 declared + E17 column-level SQLGlot), three-way merge, incremental, and BOTH
> multi-project tiers (Tier-1 rows + Tier-2 manage-token with graceful degradation). Only
> **E18/E19 (Glossary/Metric)** and **cross-warehouse lineage** are out (roadmap at the end). This
> materially enlarges Phase 4/5/7 — expected.
>
> **OM target (verified, spec §3.5):** the guaranteed surface is the latest **stable** release
> **1.13.4**; **2.0.0 is a pre-release/RC**, so its three 2.0-only niceties (`deleteStale`, bulk
> `?overrideMetadata=`, delete-lineage-by-source-name) are **version-probe-gated** with 1.13.4
> fallbacks and never hard-depended on.

## Conventions for every task

- **TDD where it fits** (`superpowers:test-driven-development`): mapping/merge/datatype/lineage logic
  is pure and unit-testable — write the test first.
- No secret value, no customer identifier, and no Collate licence header ever lands in `src/`
  (spec §6.6). Clean-room: reimplement from understanding, copy nothing from `reference-connector/`.
  The `src/lineage/` package is clean-room from `lineage-spike/` (ours) + the OM `entityLineage` schema.
- `ruff check` + `ruff format` clean; Python 3.14; typed; `run()` stays a thin orchestrator.
- After each task the reviewer checks the acceptance criterion before the next task starts.

---

## Phase 4 — Implement (owner: `component-develop`; gate: Phase-4 static)

### T1 — Config model + clean scaffold
- **Files:** `src/configuration.py`, remove the cookiecutter example from `src/component.py`.
- **Do:** one Pydantic `Configuration` over the merged root+row params (spec §5.1). Root: `om_host`,
  `#bot_token` (alias), `service_name?`, `project_scope` enum (`rows`/`all_projects`),
  `#manage_token?`, `organization_id?`, `merge_mode`/`failure_mode`/`branch_filter` enums,
  `write_lineage`, `write_column_lineage`, `write_pipelines`, `write_pipeline_status`, `full_refresh`,
  `use_ssh_tunnel`, nested `Ssh{host,user,port,#private_key}`, `om_version_override?`, `debug`. Row:
  `#storage_token?`, `project_name_override?`, `stages`, `bucket_allowlist`, `bucket_denylist`. Compute
  `service_name` default from `KBC_STACKID` at runtime (not persisted). **Cross-field validation:**
  `project_scope=all_projects` requires `#manage_token` (else `UserException`). Validate at
  construction; raise `UserException` on `ValidationError`.
- **Verify:** unit tests — valid Tier-1 + Tier-2 configs parse; missing `om_host`/`#bot_token` →
  `UserException`; `all_projects` without `#manage_token` → `UserException`; enums reject bad values;
  `service_name` default resolves from a stubbed `KBC_STACKID`.

### T2 — OpenMetadata REST client
- **Files:** `src/client/om_client.py`.
- **Do:** thin client over `requests` (spec §3.3, §3.5, §4.4, research §8). `Authorization: Bearer
  <#bot_token>`; base `<om_host>/api/v1`. Methods: `probe_version()` (`GET /system/version`),
  `put_entity(kind, body)` for databaseServices/databases/databaseSchemas/tables **and**
  pipelineServices/pipelines (note the `/services/` segment), `put_pipeline_status(fqn, body)`,
  `bulk_put_tables(bodies, override_metadata=False)`, `patch_entity(kind, fqn, json_patch)`
  (`application/json-patch+json`), `get_by_fqn`, `list(kind, params)` cursor pagination (page 100–500),
  `put_lineage(edge)`, `delete_lineage_by_source(entityType, fqn, source)`, `soft_delete(kind, id)`,
  `delete_stale(body)` (2.0-only, §3.5). Bounded concurrency + exponential
  backoff on 5xx/connection errors; map `401/403` → `UserException`, `412` → refetch-once. No OM SDK.
  `delete_stale`, bulk `override_metadata`, and delete-lineage-by-source-**name** are 2.0-only (spec
  §3.5) — gate them on the probed version (≥2.0) with 1.13.4 fallbacks (per-id soft-delete,
  delete-lineage-by-source-**by-id**); the guaranteed target is stable 1.13.4.
- **Verify:** unit tests with mocked `requests` — version probe parses the §3.5 shape (`version`,
  `revision`, `timestamp`); PUT hits the right path (services vs non-services; pipelines); backoff
  retries on 500; `401` raises `UserException`; on a probed 1.13.4 the client uses the fallbacks and
  never calls `deleteStale`/`overrideMetadata`; on 2.0+ it uses them.

### T3 — Keboola Storage + Configurations reader
- **Files:** `src/client/storage_reader.py`.
- **Do:** read metadata via the Storage + Configurations APIs (research §9.2). Resolve credential: row
  `#storage_token` if present, else `forward_token` (`KBC_TOKEN`/`KBC_URL`), else a token injected by
  the manage client (Tier-2); if none, `UserException`. List buckets (`?include=metadata`), tables per
  bucket, table detail with `?include=columns,metadata,columnMetadata,buckets` (request the typed
  `definition`). Expose bucket fields, per-column typed `definition` + legacy `KBC.datatype.*` +
  `basetype`, `primaryKey`, stats, and system metadata keys (`KBC.description`, `KBC.createdBy.*`,
  `KBC.createdBy.branch.id`). **Also read (for pipelines + lineage):** producing-component configs and
  their `storage.input/output`, **transformation blocks/codes/SQL**, and **flows/orchestrations +
  phases**. Read the component's own prior snapshot table.
- **Verify:** unit tests with recorded/stub JSON — buckets & tables parse; typed vs legacy datatype
  both surface; transformation SQL + flow phases parse; missing token → `UserException`;
  `forward_token` fallback used when row token absent.

### T4 — Management API client (Tier-2) [owner: `component-develop`]
- **Files:** `src/client/manage_client.py`.
- **Do:** with `#manage_token` (super/application token) call
  `GET /manage/organizations/{organizationId}/projects` to enumerate, and
  `POST /manage/projects/{projectId}/tokens` to mint a **short-lived read-only** Storage token per
  project (research §9.7). Resolve `organization_id` from the token when omitted. **Log the exact
  project list per run.** Raise a structured, catchable error on scope/permission failure (so the
  orchestrator can degrade — spec §5.4), distinct from a missing-token config error.
- **Verify:** unit tests with mocked `requests` — enumeration parses; mint returns a per-project token;
  a `403`/scope failure raises the degrade-signal error, not a hard `UserException`.

### T5 — Job Queue reader (E15 run history) [owner: `component-develop`]
- **Files:** `src/client/job_queue_reader.py`.
- **Do:** `GET /jobs/{jobId}/open-api-lineage` (Job Queue, `X-StorageApi-Token`) → OpenLineage
  START/COMPLETE events (incl. child jobs for orchestrations); map to a pipeline-status record
  (success/fail + start/end timestamps) for the config's/flow's Pipeline FQN.
- **Verify:** unit tests with recorded JSON — a job's events map to a status body; child jobs of an
  orchestration are included.

### T6 — Optional SSH bastion tunnel
- **Files:** `src/client/ssh_proxy.py`.
- **Do:** when `use_ssh_tunnel`, open an `sshtunnel` forward to the OM host before building `om_client`;
  a tunnel failure → `UserException` (exit 1). No-op when disabled.
- **Verify:** unit test — disabled path is a no-op; a failing tunnel raises `UserException`.

### T7 — FQN builder + name sanitisation (clean-room)
- **Files:** `src/mapping/fqn.py`.
- **Do:** `service.database.schema.table` from `<service_name>.<project>.<bucket.path>.<table.name>`;
  Pipeline FQN `<service_name>.<project>.<config-or-flow-id>`; sanitise (spaces→underscores, avoid dots
  in table names); keep the original as `displayName`.
- **Verify:** unit tests — deterministic table + pipeline FQNs; a linked bucket in project B resolves
  onto the node project A created; sanitisation stable and reversible-to-displayName.

### T8 — Table-type detection (clean-room)
- **Files:** `src/mapping/table_type.py`.
- **Do:** rules from research §3 — `hasExternalSchema && sharing∈{none,null}` → External;
  `stage∈{out,shared,linked} && sharing∈{specific,none,null}` → Regular; `stage=in && sourceBucket`
  → View; table alias → View; else Regular.
- **Verify:** unit tests over each branch.

### T9 — Datatype mapping
- **Files:** `src/mapping/datatype.py`.
- **Do:** map order typed `definition.type`/`basetype` → legacy `KBC.datatype.basetype` → `UNKNOWN`
  onto the OM `DataType` enum, driven **purely off the Storage API response per table** (independent of
  `KBC_DATA_TYPE_SUPPORT` — spec §6.5); carry `dataTypeDisplay`; **never** emit `dataLength:1`.
- **Verify:** unit tests — typed wins over legacy; legacy fallback; `UNKNOWN` last; unknown-length
  varchar omits `dataLength`; mapping unaffected by `KBC_DATA_TYPE_SUPPORT` value/absence.

### T10 — Catalog entity builder (E1–E12)
- **Files:** `src/mapping/entity_builder.py`.
- **Do:** build OM bodies — DatabaseService (E1), Database (E2, `sourceUrl`), DatabaseSchema (E3),
  Table (E4–E9: columns via T9, `tableConstraints PRIMARY_KEY`, `tableType` from T8, View
  `schemaDefinition` + edge intent, External, `description` from `KBC.description`, `sourceUrl` deep
  link, optional entity-type/`KBC.createdBy.*` tags). Respect `branch_filter`.
- **Verify:** unit tests — a Regular table body has columns+PK+sourceUrl; a linked bucket yields a View
  body + a pending ViewLineage edge; External detected; deep link matches the stack UI pattern.

### T11 — Pipeline builder (E13/E14/E15)
- **Files:** `src/mapping/pipeline_builder.py`.
- **Do:** PipelineService (once). E13: component config → Pipeline; rows/blocks/codes → Tasks with
  `taskSQL` and `downstreamTasks` ordering; `sourceUrl` deep link. E14: flow/orchestration phases →
  Pipeline + Tasks with phase ordering. E15: shape a `PUT /pipelines/{fqn}/status` body from T5's
  run-history record. FQNs via T7.
- **Verify:** unit tests — config → Pipeline with ordered Tasks + `taskSQL`; flow phases → ordered
  Tasks; a run-history record → a valid status body.

### T12 — Declared table-level lineage builder (E16)
- **Files:** `src/mapping/lineage_builder.py`.
- **Do:** from each producing config's `storage.input/output.tables` (+ `KBC.createdBy.*`), emit coarse
  all-inputs→all-outputs `AddLineageRequest` edges tagged `source=PipelineLineage`; attach the
  `pipeline` ref when the producing Pipeline (T11) exists; plus ViewLineage edges from T10. FQNs via T7.
- **Verify:** unit tests — N×M edges; `source=PipelineLineage`; `pipeline` ref attached; no edge
  references a non-existent entity in the batch.

### T13 — Column-level SQL lineage package (E17) [clean-room, seeded by `lineage-spike/`]
- **Files:** `src/lineage/column_lineage.py`, `src/lineage/dialect.py`, `src/lineage/resolution.py`.
- **Do:** port the `lineage-spike/` PoC into `column_lineage.py`: SQLGlot parse of transformation SQL →
  column edges, resolving workspace names to storage table ids via the config's storage mapping and
  chaining through workspace intermediates (`tempLineageTables`). Emit `columnsLineage`
  (`fromColumns[]`, `toColumn`, `function?`) tagged `source=QueryLineage`. `dialect.py`: map the
  transformation backend/component id → SQLGlot dialect (Snowflake/BigQuery/Redshift/Synapse) + casing
  rules; unknown → generic parse + record `unresolved`. `resolution.py`: the
  resolved/no-upstream/unresolved taxonomy + coverage metrics. Guard the CTAS silent-under-report
  shape. A parse miss is recorded, never raised (spec §6.3).
- **Verify:** unit tests — Snowflake transform → expected column edges (`resolved`); a multi-step chain
  resolves via `tempLineageTables`; a CTAS shape does not silently under-report; an unparseable/unknown
  dialect records `unresolved` and the run continues.

### T14 — Column-lineage coverage CI gate
- **Files:** `tests/lineage_corpus/` (corpus + expected taxonomy), a `pytest`-marked coverage test,
  CI wiring in `.github/workflows/push.yml`.
- **Do:** run `resolution.py` over a fixed SQL corpus (seeded from `lineage-spike/corpus_run.py`, with
  customer identifiers scrubbed) and fail CI if the `resolved` fraction regresses below a pinned
  threshold or any case silently drops `resolved → no-upstream/unresolved` (spec §9).
- **Verify:** the gate passes on the seeded corpus at the pinned threshold; a deliberately-broken
  fixture makes it fail.

### T15 — Three-way merge + snapshot store
- **Files:** `src/merge.py`.
- **Do:** base = last-written snapshot (read via `storage_reader`). Per field: OM==base → update;
  OM!=base → leave + record `skipped_diverged` (unless `merge_mode=keboola_always_wins` → overwrite);
  OM empty/absent → write. Emit `PATCH json-patch` touching only owned fields. For lineage, drop our
  stale `PipelineLineage`/`QueryLineage`/`ViewLineage` edges (delete-all-by-source) then add current;
  never touch `Manual`. Applies to Table, Pipeline, and edges. Write the updated snapshot (entity_fqn +
  entity_type PK, fields JSON, content hash).
- **Verify:** unit tests over the full truth table (empty/match/diverge × both merge modes) for tables
  AND pipelines; lineage diff never removes a `Manual` edge.

### T16 — Incremental digest + tombstoning
- **Files:** `src/sync.py`.
- **Do:** per-bucket content digest into `state.json` (Tier-2: keyed `project_id → {bucket → digest}`);
  skip an unchanged bucket unless `full_refresh`, the auto-cadence (every N runs, default 20), or a
  version change fires. **Advance-after-success** (spec §2.3): persist digests + state only after that
  bucket's OM writes succeed. Tombstoning: 2.0+ `deleteStale` (dryRun then apply, `seenFqns`);
  1.13.4 self-diff (list scope, soft-delete missing); **fail closed** on list error/short scope. Every
  tombstone reported.
- **Verify:** unit tests — unchanged digest skips; changed digest re-processes; full_refresh ignores
  digests; a mid-run failure leaves the prior digest intact; Tier-2 per-project keying; tombstone
  fail-closed; version gate selects the right delete path.

### T17 — Report + snapshot output tables
- **Files:** `src/report.py`.
- **Do:** write `catalog_run_report` (columns + action enum per spec §6.4, incl. `unresolved`,
  `degraded`) with an **authoritative `schema`** manifest (`timestamp`→TIMESTAMP,
  `om_status_code`→INTEGER, rest STRING), `PK=[run_id, entity_fqn, entity_type]`, `incremental=true`,
  `write_always=true`, `has_header=True` with a header row. The output-manifest format honours
  `KBC_DATA_TYPE_SUPPORT` (**absent/`None` → legacy `columns`+`column_metadata`**). `config_row_id`
  from `KBC_CONFIGROWID` is `None`-safe. Write `last_written_snapshot` (incremental upsert). Scratch →
  `/tmp` only.
- **Verify:** unit test — authoritative `schema` with `has_header:true` when the gate is on; legacy
  manifest when `KBC_DATA_TYPE_SUPPORT` absent; `write_always` set; `config_row_id` null-safe; new
  action values recorded; no stray file under `/data/out/tables/`.

### T18 — Component orchestrator + sync actions + error handling
- **Files:** `src/component.py`.
- **Do:** `run()` per spec §6.2 — validate → **resolve project set** (Tier-1 row, or Tier-2 enumerate +
  mint via `manage_client` with **graceful degradation** to the host project on scope failure) → open
  SSH tunnel → version probe → per project: load state+snapshot → catalog pass → pipeline pass (E13/E14
  + E15 status if enabled) → lineage pass (E16 + E17 if enabled) → tombstone pass → write
  snapshot/state/report → collect_and_fail raise. `run_sync_action` handlers `test_connection` and
  `list_buckets`. Read env vars `None`-safe (`KBC_CONFIGROWID`, `KBC_DATA_TYPE_SUPPORT`). Exit codes per
  §6.3 (config/auth/setup always exit 1; `all_projects` w/o `#manage_token` exit 1; Tier-2 scope
  failure degrades exit 0; column-lineage miss non-fatal; unexpected exit 2). Register `VCR_SANITIZERS`
  incl. `#manage_token` + minted tokens.
- **Verify:** orchestrator thin (logic in modules); `test_connection`/`list_buckets` return ok/fail;
  missing bot token → exit 1; `all_projects` w/o manage token → exit 1; Tier-2 scope failure → degrade
  + `degraded` report row + exit 0; unexpected → exit 2.

### T19 — configSchema + row schema + sync actions UI (owner: `component-build-ui`; gate: schema-ui)
- **Files:** `component_config/configSchema.json`, `component_config/configRowSchema.json`,
  `component_config/*` descriptions.
- **Do:** root schema sections *Connection* (`om_host`, `#bot_token`, SSH), *Scope* (`project_scope`
  with `options.dependencies` revealing `#manage_token`/`organization_id` on `all_projects`),
  *Advanced* (merge/failure/branch enums, `write_lineage`/`write_column_lineage`/`write_pipelines`/
  `write_pipeline_status` [status gated on pipelines], `full_refresh`, `om_version_override`); row
  schema; `testConnection` + `listBuckets` sync actions; `enum`+`enum_titles` for all enums (store
  value); async `select`+`autoload` (`listBuckets`) for bucket allow/deny (free-text fallback), per
  spec §5. No default seeded into a fresh config beyond visible/self-explanatory ones.
- **Verify:** schema-tester renders; enums store values; `#manage_token`/`organization_id` appear only
  on `all_projects`; SSH fields only on `use_ssh_tunnel`; `write_pipeline_status` only on
  `write_pipelines`; `listBuckets` populates the dropdowns; a fresh config's saved params contain only
  user-set values.

---

## Phase 5 — Local VCR + pytest (owner: `component-test` / `generate-vcr-tests`; gate: Phase-5 testing)

### T20 — VCR test preparation (owner: `vcr-test-preparer`)
- **Do:** build `tests/functional/*/` for every §7 case (**01–29**): sync-action ok+fail; catalog
  full/incremental; view/external; declared + column lineage (Snowflake, other dialect, unresolved);
  pipelines (config/flow/status); Tier-1 rows; Tier-2 all-projects + degrade + missing-manage-token;
  2.0-only bulk-override/deleteStale (recorded against a local 2.0 RC); failure modes; SSH. Wire `VCR_SANITIZERS` (incl. `#manage_token`
  + minted tokens + SQL identifiers). `secrets.json` skeleton; row-scoped `state.json` fixtures;
  expected output tables. Single merged `config.json` per case (no root/row split). Record from the OM
  public sandbox / local quickstart + a scratch Keboola project — no customer creds.
- **Verify:** every §7 row (01–29) has a case dir; configs cover ok+fail for both sync actions and
  each run mode incl. both multi-project tiers.

### T21 — Record + validate cassettes (owners: `vcr-recorder` → `vcr-cassette-validator`)
- **Do:** record via the datadirtest scaffolder (only sanctioned path); the validator gates both axes —
  no secret/PII/customer identifier baked in (incl. transformation SQL + minted tokens + Management/Job
  Queue responses), and each recording matches its declared intent.
- **Verify:** validator returns PASS; sanitizers confirmed on all three token types, the OM host, SSH
  key, org/project ids, and SQL identifiers.

### T22 — datadir/unit suite + coverage gate green
- **Do:** finish unit tests (T1–T18) + datadir functional tests; wire the E17 coverage gate (T14) into
  CI; full `pytest` green.
- **Verify:** paste `N passed`; coverage gate passes at the pinned threshold; `ruff check` clean.

---

## Downstream phases (tracked in the lifecycle file, not tasks here)

- **Phase 6 — `component-dev-portal`:** configSchema/row schema + `testConnection`/`listBuckets` sync
  actions live in portal; set `dataTypeSupport=authoritative`; request
  `forward_token`+`forward_token_details` enablement (Keboola staff); portal-owned descriptions/URLs;
  verify via fresh `kbagent dev-portal` GET. (After the 0.0.1 release so CI-sync doesn't overwrite.)
- **Phase 7 — `component-test` (tier 4):** deploy the `initial-implementation` image; cf-dev config via
  `kbagent` with `runtime.tag` overridden to the branch build; real job succeeds end-to-end against a
  reachable test OM instance — **latest stable 1.13.4** as the primary smoke, plus a **2.0 RC** if
  available to exercise the 2.0-only paths (catalog + pipelines + both lineage tiers). Tier-2 smoke if
  a `manage:storage-tokens` token is obtainable, else confirm the `degraded` fallback. Fresh-config UI
  acceptance.
- **Phase 8 — `component-checklist-review` + `babysit-pr`:** open the one PR, full completeness audit,
  PR loop to convergence, hand to maintainer for the merge decision.

---

## Roadmap — deferred capabilities (user-approved; NOT built in this plan)

| Item | Spec ref | When / seed |
|---|---|---|
| E18/E19 Glossary + Metric | §4.1 | later phase; not requested; needs a Keboola semantic-layer source (glossary/metrics). |
| Cross-warehouse lineage | §4.2 | **Not building** — needs warehouse creds the component must not hold. |
