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
| `21_testConnection_unreachable_host` | Records a real connection error (unreachable `om_host`); the retry/backoff makes this the slowest recording (~15 s). Currently exits 2 (reachability → OMClientError), not the exit 1 spec §6.3 describes — a separate minor gap, not fixed here. |

## Sanitizers (secrets scrubbed from cassettes)

`DefaultSanitizer` in `src/component.py` (1) whitelists request/response headers to
`{content-type, content-length, accept}` — so `Authorization: Bearer <#bot_token>`
(OM), `X-StorageApi-Token` (Storage token / `KBC_TOKEN` / Tier-2 minted token) and
`X-KBC-ManageApiToken` (`#manage_token`) are stripped from every interaction — and
(2) redacts sensitive body fields by name, extended with `token` (the Tier-2 minted
Storage token returned in the mint response body) and the `#`-prefixed config keys.
The OM host and Keboola stack host are intentionally **not** sanitized (both public;
the request URI is the VCR match key). The SSH `#private_key` never crosses HTTP.
