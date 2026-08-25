# VCR functional tests — recording guide

These tests replay recorded HTTP interactions against **two** APIs — OpenMetadata
(OM) and the Keboola Storage / Configurations / Management / Job-Queue APIs — so
the suite runs in CI with no live credentials. Cassettes are **recorded from real
interactions** with the `keboola.datadirtest` scaffolder; **never hand-author or
hand-patch a cassette** (regenerate instead).

- Test matrix: `tests/setup/configs.json` (one entry per case, intent in each `description`).
- Runner: `tests/test_functional.py` (replays each recorded dir; frozen clock + fixed non-secret KBC env).
- Sanitizers: `VCR_SANITIZERS` in `src/component.py` (loaded by the scaffolder).

## What to record against

- **OM:** the public sandbox `https://sandbox.open-metadata.org` (runs OM **1.13.4**,
  the guaranteed stable surface — spec §3.5). No customer creds needed.
- **Keboola:** a dedicated **CF scratch project** that has buckets/tables (and, for
  the pipeline/lineage cases, at least one transformation + one flow + a completed
  job). Use a non-customer project — its project id/name are baked into cassettes
  and output tables, so they must be safe to commit.

## secrets.json (git-ignored)

Copy `secrets.json.dist` (repo root) to `secrets.json` and fill real values. It is
deep-merged into every test's config at record time and masked back out of the
committed `config.json`:

```json
{ "parameters": { "#bot_token": "<OM sandbox JWT bot token>",
                  "#storage_token": "<real read-only Storage token>" } }
```

## Record

Export the deterministic **non-secret** KBC env first (must match
`KBC_ENV` in `tests/test_functional.py` exactly, or replay URIs/FQNs won't match):

```bash
export KBC_URL=https://connection.keboola.com          # the scratch project's stack
export KBC_STACKID=connection.keboola.com
export KBC_PROJECTNAME=cf-openmetadata-catalog-test
export KBC_RUNID=keboola-om-catalog-vcr
export KBC_COMPONENTID=keboola.wr-openmetadata-catalog
export KBC_CONFIGID=vcr-test
uv run python -m keboola.datadirtest scaffold --secrets secrets.json \
    --freeze-time 2026-08-24T12:00:00
```

Then verify replay: `uv run pytest tests/test_functional.py -v`, and run the
`vcr-cassette-validator` gate before committing.

## Bucket subset (run-mode cassettes)

The run-mode catalog/merge/Tier-2 cases —
`05, 06, 07, 09, 11, 12, 16, 19` — record against a **64-bucket subset**
of project **4214** (`[CF] New Features Testing`), pinned in their config as
`parameters.bucket_allowlist` and mirrored in `tests/setup/subset_buckets_4214.json`.

**Why:** the full project has 713 buckets / ~5 449 tables; a full-catalog run
records to a **~159 MB** cassette, over **GitHub's 100 MB per-file limit**. The
64-bucket subset keeps each cassette well under the limit (every committed subset
cassette is ≤ ~6 MB) while staying representative — diverse connectors, typed + PK
columns, previously length-less string columns, and the duplicate-sanitised-column
bucket `in.c-keboola-ex-airbyte-wrapper-1221443127` (exercises the column de-dup fix).

Keep `configs.json`'s `bucket_allowlist` and `subset_buckets_4214.json` in sync;
re-scaffolding without the allowlist would silently regenerate 159 MB cassettes.

## Dropped cases: 08 (pipelines + lineage) and 10 (column-lineage only)

Cases **08** (`08_run_pipelines_and_lineage`) and **10** (`10_run_column_lineage_only`)
are **deliberately NOT committed as VCR cassettes** and have been removed from the
matrix (`configs.json`). Their run path records
`GET /v2/storage/branch/.../components?include=configuration`, which returns a dump
of **every other component's configuration** in the recording project. That dump
contains real credentials (RSA/PKCS#8 private keys, cloud access-key ids) and
third-party identifiers belonging to unrelated configs. The component's original
field-name sanitizer only knew *its own* secret field names, so it could not
anticipate those foreign secret field names and the recording baked them in.
Committing that to a public repo is unacceptable, so 08/10 are dropped.

The behaviour they exercised is covered by **unit tests** instead:

- **E13** (config → `Pipeline` + Tasks from storage input/output + SQL) and **E14**
  (flow → `Pipeline`): `tests/unit/test_pipeline_builder.py`.
- **E17** (column-level SQL lineage via SQLGlot → column edges): covered by
  `tests/unit/test_lineage.py` and `tests/unit/test_lineage_builder.py`.

The `VCR_SANITIZERS` scrubber in `src/component.py` was also broadened to redact,
recursively, any credential-shaped field **name** or **value** in any recorded
body (see the Sanitizers section below) so a future recording cannot leak foreign
secrets. Even so, 08/10 stay out of the committed VCR set: `?include=configuration`
is inherently a whole-project secret surface and must not be recorded against a
real project.

**Tier-2 case (19):** `organization_id` is `3697` (not a secret — kept literal in
`configs.json`); the org has 4 projects but only 4214 holds data, so 19 catalogs
4214's subset and creates empty databases for the other three.

## Incremental-skip state override (case 06)

`06_run_catalog_incremental_second_run` is recorded **after 05 with `--chain-state`**
so 05's `out/state.json` (the 64 project-4214 bucket digests, `run_count=1`) seeds
06's `in/state.json` and the run records as `skipped_unchanged`.

At **replay**, `keboola.datadirtest` wipes a non-chained test's `in/state.json` to
`{}` in its temp copy (`_override_input_state` in `datadirtest.py`), because 05/06
are sibling leaf dirs, not one nested chain container. With empty state every
bucket looks changed, so the component would reprocess instead of skip and nothing
would match the recorded skip cassette. `tests/test_functional.py` therefore
special-cases 06: it builds the `VCRTestDataDir` directly with
`last_state_override` = 06's committed `in/state.json`, i.e. the library's own
chaining parameter, so the harness **seeds** the digests instead of wiping them.
The component itself is correct (it skips when the digest is present); this is a
replay-time harness wiring, not a component change.

## Per-case record notes

Most success cases record straight from the sandbox + scratch project with the
`#storage_token` (row) path. The exceptions:

| Case | Special record-time handling |
|------|------------------------------|
| `02_testConnection_bad_bot_token` | Record in a pass whose `--secrets` file **omits `#bot_token`**, so the intentionally-bad token reaches the authenticated OM call (`GET /users/loggedInUser`) and 401s (the global merge would otherwise restore the real token). The version probe is unauthenticated, so the auth call is what fails. |
| `04_listBuckets_bad_storage_token` | Record in a pass whose `--secrets` file **omits `#storage_token`**, so the intentionally-bad token reaches Storage and 401s (the global merge would otherwise restore the real token). |
| `06_run_catalog_incremental_second_run` | Record **after** `05` with `--chain-state` so `05`'s `out/state.json` seeds `06`'s `in/state.json` → unchanged buckets `skipped_unchanged`. |
| `11`/`12` merge (`skipped_diverged` / overwrite) | Pre-seed OM: create an entity, then diverge one curated field, and provide a chained snapshot base so the merge sees `base == last-written`. |
| `13`/`14`/`15` failure_mode | Need **one** OM entity write to fail deterministically (a seeded conflict / a payload OM rejects) so `catalog_run_report` gets an `action=failed` row. |
| `16_run_host_project_forward_token` | Omit `#storage_token` from `--secrets` for this pass and export a **real read-only `KBC_TOKEN`** (forward_token path). |
| `17`/`18` config-validation failures | No HTTP — empty cassette, `expected_status.json` exit 1. Record straight through. `#manage_token` must stay absent from `secrets.json` for `18`. |
| `19_run_tier2_all_projects` | Needs a real `manage:storage-tokens` **manage token** + `organization_id`; add `#manage_token` to `--secrets` for this pass only. If unavailable, record later / against a mock — do not block the phase. |
| `20_run_tier2_degrade` | Needs a **scope-limited** manage token (or a mocked 401/403) + a real `KBC_TOKEN` for the host-project fallback. Conditional, like `19`. |
| `21_testConnection_unreachable_host` | Records a real connection error (unreachable `om_host`); the retry/backoff makes this the slowest recording (~15 s). Maps to `OMConnectionError` → `UserException` → **exit 1** (spec §6.3). |

## Sanitizers (secrets scrubbed from cassettes)

`VCR_SANITIZERS` in `src/component.py` has two layers:

1. **`DefaultSanitizer`** (1) whitelists request/response headers to
   `{content-type, content-length, accept}` — so `Authorization: Bearer <#bot_token>`
   (OM), `X-StorageApi-Token` (Storage token / `KBC_TOKEN` / Tier-2 minted token) and
   `X-KBC-ManageApiToken` (`#manage_token`) are stripped from every interaction — and
   (2) redacts sensitive body fields by exact name, extended with `token` (the Tier-2
   minted Storage token returned in the mint response body) and the `#`-prefixed
   config keys.

2. **`CredentialScrubber`** (defense-in-depth for *foreign* secrets that the exact
   field-name list can never enumerate ahead of time). It walks every recorded
   request/response body recursively and redacts:
   - any value whose **field name** matches (case-insensitive) a credential pattern
     — `private_key`, `snowflake_private_key`, `accesskeyid`, `aws_access_key_id`,
     `access_key`, `api_key`, `api_key_id`, `aws_key_id`, `secret`, `password`,
     `token`, and any name ending `_key` / `_secret` / `_password` / `_token` or
     containing `access_key`;
   - any string whose **shape** is a credential — a PEM/PKCS#8 private-key block
     (`-----BEGIN … PRIVATE KEY-----`) or an AWS access-key id (`AKIA[0-9A-Z]{16}`) —
     regardless of the field name it sits under.

The OM host and Keboola stack host are intentionally **not** sanitized (both public;
the request URI is the VCR match key). The SSH `#private_key` never crosses HTTP.
This second layer is why a future recording of a `?include=configuration` response
could not leak a foreign RSA key or AWS id — but 08/10 remain dropped regardless
(see "Dropped cases" above).
