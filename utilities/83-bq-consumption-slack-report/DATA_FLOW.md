# Data flow: BigQuery consumption Slack digest

## Schedule

| Item | Value |
|------|-------|
| DAG id | `bq_user_consumption_statistics` |
| Cron | `30 6 * * *` (06:30 UTC) |
| Catchup | False |
| Retries | 0 |
| Max active | default (single-task DAG) |

## Path

1. Upstream job (not this DAG) writes yesterday's rows into
   `monitoring.bq_usage` (`username`, `query_date`,
   `total_billed_gibibytes`, `n_queries`, `total_costs_in_eur`).
2. At 06:30 UTC, `get_status` opens BigQueryHook (location EU).
3. Query 1 — top 3 human users for `CURRENT_DATE() - 1 day`, ordered
   by GiB, with ratio and Slack emoji CASE expressions.
4. Query 2 — all-time heaviest human user (no date filter). Message
   text still says "30 day record".
5. Query 3 — sum GiB and EUR for the current calendar month.
6. Lines are concatenated; SlackWebhookOperator posts to `#dwh-ops`.

## Failure modes

| Failure | Effect |
|---------|--------|
| Missing yesterday partition | Header-only / thin message; task still succeeds if Slack posts |
| BQ permission / query error | Task fails; email_on_failure fires |
| Slack webhook down | Task fails (post is inside the same callable) |
| Service-account-only traffic yesterday | Human top-3 empty; heaviest/month lines may still populate |

## Contracts

- Human filter: `username NOT LIKE '%gserviceaccount%'`
- Currency label in Slack is "Euro" — matches the column
  `total_costs_in_eur` from the monitoring table
- No XCom: message never leaves the worker except via Slack
