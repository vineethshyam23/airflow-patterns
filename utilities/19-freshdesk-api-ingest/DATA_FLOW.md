# Data flow: Freshdesk API ingest

Schedule: `@hourly`. Catchup off. `max_active_runs=1`.

## Branch decision

| Condition | Branch | Resources |
|-----------|--------|-----------|
| `day == 1` and `hour == 1` | `monthly_task_start` | contacts, agents, roles, groups, companies |
| otherwise | `hourly_task_start` | tickets |

Wall clock is `datetime.now()` inside the branch callable (same as
source). If you need logical-date branching for catchup, switch to
`context["data_interval_start"]` — we did not, because catchup is off.

## Stage A — Extract

| Resource | Window / scope | Notes |
|----------|----------------|-------|
| `tickets` | `updated_since` = 1st of current month 00:00 UTC | Hourly rewrite of NDJSON |
| `contacts` | full list pages | `custom_fields` → string |
| `agents` | full | |
| `roles` | full | |
| `groups` | full | |
| `companies` | full | `custom_fields` → string |

Output: `{TMP_LOC}{resource}.json` as newline-delimited JSON.
Default `TMP_LOC` is `/home/airflow/gcs/data/freshdesk/` on Composer.

Pagination: `per_page=100`, increment `page` until empty array or
non-200. No cursor — retries restart at page 1 and overwrite the file.

## Stage B — Raw zone copy

`GCSToGCSOperator` copies Composer object
`data/freshdesk/{resource}.json` to
`{rawzone}/freshdesk/{YYYY-MM-DD}/{resource}.json`.
`LOAD_DATE` is `date.today()` at parse/run — acceptable with catchup
off; prefer `{{ ds }}` if you ever enable historical re-runs.

## Stage C — Staging load

`GCSToBigQueryOperator` loads NDJSON into
`staging.freshdesk_{resource}` with WRITE_TRUNCATE and schema from
`schema_json/freshdesk_{resource}.json`. Every resource truncates —
dbt owns history / SCD if needed.

## Stage D — dbt

- Hourly: `freshdesk_tickets_dbt_run` after tickets load (`ALL_DONE`).
- Monthly: all dim loads → `pause` (`ALL_SUCCESS`) →
  `freshdesk_dims_dbt_run` → `end`.

## Idempotency and re-runs

- Re-run hourly: re-fetches month-to-date tickets, overwrites today's
  dated raw object, truncates staging, re-triggers tickets dbt.
- Re-run monthly branch: only fires again if you hit the 1st @ 01:00
  window or manually clear and trigger with a patched branch callable.
  For ad-hoc dim refresh, temporarily force the branch or run the
  monthly task chain from the UI.
- Partial monthly failure: `pause` waits on ALL_SUCCESS, so dims dbt
  does not run on a half-loaded set. Fix the failed resource and clear
  downstream.

## Failure modes worth knowing

- 429 / rate limit: task retries (3 × 10 min). Client does not implement
  exponential backoff beyond a short sleep on exception — Airflow is
  the backoff.
- Empty tickets file mid-month: possible if no updates yet; staging
  truncate still runs. Downstream dbt should tolerate empty ticket
  staging for that hour.
- Schema JSON missing in the raw bucket: BQ load fails before dbt.
  Keep `schema_json/freshdesk_*.json` versioned with the landing path.
- Branch skip confusion: skipped branch tasks show pink/skipped in the
  UI — that is expected every hour that is not the monthly window.
