# Data flow: Odoo / CRM assets + leads lifecycle export

Schedule: `0 6,13 * * *` (06:00 and 13:00 UTC). Catchup off — a missed
run should be an explicit re-trigger with a widened `_valid_from`
window if you need to recover skipped SCD versions.

## Stage A — dbt refined refresh

`DbtCloudRunJobOperator` runs the Cloud job behind Airflow Variable
`dbt_job_odoo_leads_assets_lifecycle` (timeout 600s, poll every 10s).
That job materializes (or refreshes) the three refined tables the
ingest tasks read.

If dbt fails, all three ingest tasks stay blocked (`all_success`). That
is correct: shipping yesterday's refined snapshot as "today's delta"
is worse than a delayed partner feed.

## Stage B — Parallel Avro ingest (FR)

| Task | Source | Delta filter |
|------|--------|--------------|
| `ingest_odoo_lead_lifecycle_data` | `refined.odoo_leads_lifecycle` | `_valid_flag`, `_valid_from >= today`, country FR |
| `ingest_odoo_asset_lifecycle_data` | `refined.odoo_assets_lifecycle` | `_valid_flag`, `_valid_from >= today`, country FR |
| `ingest_odoo_voucher_code_data` | `refined.odoo_voucher_code` | `asset_created_date >= yesterday`, country FR |

Each task: SELECT → Avro encode → POST chunks of 500 to
`/ingestbulk/{country}/{schema_id}`.

## Idempotency and re-runs

- Re-run after dbt success: re-posts today's SCD versions / voucher
  window. Safe if the bus upserts on natural keys; coordinate otherwise.
- Re-run only one ingest task: fine — siblings are independent after
  dbt.
- Missed calendar day: `_valid_from >= CURRENT_DATE()` will not
  backfill yesterday's versions. Widen the filter or run a one-off
  query for the gap, then restore the daily predicate.
- Never put ingest before dbt. You will ship stale refined rows and
  label them as the morning refresh.

## Failure modes worth knowing

- OAuth 401 mid-chunk: client clears token and retries the POST once.
- Empty lead/asset result mid-day: often "no SCD versions opened today"
  rather than a broken pipeline — check dbt run logs before paging.
- Empty voucher window: possible on quiet days; check
  `asset_created_date` freshness if it stays empty across both slots.
- dbt timeout (600s): warehouse slot contention / Cloud job queue —
  bump only after checking the job itself.
- PII classification: Avro contracts historically marked `table_PII: no`
  while carrying email / phone / name. Do not treat that flag as
  governance truth.
- Memory: full result set buffered before chunking. One-country FR is
  fine; multi-country would change the shape.
