# Data flow: dbt Cloud run status Slack digest

## Schedule

| Item | Value |
|------|-------|
| DAG id | `dbt_jobs_status_dag` |
| Cron | `1 5 * * *` (05:01 UTC) |
| Catchup | False |
| Retries | 2 (10 min delay) |
| Max active | default |

## Path

1. Upstream trigger DAGs (not this pattern) submit dbt Cloud jobs and
   write run ids into Airflow Variables whose keys contain
   `dbt_runids` (JSON lists).
2. At 05:01 UTC, `get_dbt_runids` scans Variables via `create_session`
   and XComs the flattened id list.
3. `get_dbt_job_status` calls dbt Cloud (`account_id` =
   `DBT_ACCOUNT_ID`) for each id: run details + job name.
4. Rows with status Failed are XComed; Succeeded rows are dropped.
5. `slack_notification` posts one webhook message per Failed row to
   `#dwh-dbt-ops`, including environment label, job name/id,
   finished_at, and a Slack-linkified job URL.

## Failure modes

| Failure | Effect |
|---------|--------|
| No matching Variables / empty lists | Empty XCom → Slack no-op; DAG succeeds |
| Malformed Variable JSON | That Variable skipped; others still polled |
| dbt Cloud API error for one run id | Logged; that id skipped; others continue |
| `get_job` permission / name lookup fails | Falls back to numeric `job_id` as name |
| Null `finished_at` on a Failed row | Message shows `None` (guarded parse) |
| Slack webhook down | Task fails after some messages may already have posted |
| Variable overwritten before poll | Prior failed run id never reported |

## Contracts

- Discovery key substring: `dbt_runids` (not a prefix-only match)
- Status rule: Succeeded iff `is_success` and not `is_error`; else Failed
- Slack only for Failed — quiet mornings are a feature
- Environment label from `env` Variable/env var (`DEV`/`PROD` →
  sanitized stream labels)
- Does not clear or rotate Variables after posting
