# Data flow: Payment wallet API ingest

## Steps

1. **Auth** — POST client id/secret to the token URL; store
   `Bearer {accessToken}`. On 401/403, clear token and refresh once
   per retry attempt.
2. **Fetch (per feed)** — `get_payment_wallet_data` writes
   `data/payment_wallet/{file_name}.json` as NDJSON on the Composer
   data volume.
   - **KYC** — page until empty; no count endpoint.
   - **Transactions** — call `/transaction/count`, then page with
     `size=1000` until retrieved >= count.
   - **VOP** — for each `report_date` in the list, page with
     `filters.reportDate` until empty (or HTTP 400 stop).
3. **Upload** — `GCSToGCSOperator` copies Composer → rawzone under
   `payment-wallet/{kyc|transactions|vop_performance}/{YYYY-MM-DD}/`.
4. **Empty check** — `BranchPythonOperator` downloads the blob; zero
   bytes → `no_data_*` → `end`. Non-empty → `process_data_*`.
5. **Load staging** — NDJSON treated as CSV/TSV with one `JSON`
   column into `trusted_staging.{file_name}`.
   - KYC: `WRITE_TRUNCATE`
   - Transactions / VOP: `WRITE_APPEND`
6. **Join** — successful loads converge on `stage_1`.
7. **dbt** — two Cloud jobs in parallel (KYC job also builds VOP
   models via shared tag). Missing job id / provider → EmptyOperator
   stub so the graph still imports in a reference checkout.
8. **Notify** — `check_all_tasks` collects states; transactions path
   also pulls today's loaded count from the trusted intermediate;
   three Slack (or print) summaries fire `ALL_DONE`.

## Date / path coupling

`load_date` / API window are computed at DAG parse time from
`datetime.now()`. Paths and filters therefore follow wall clock for
the scheduled morning run. Manual backfills need Variable overrides
(VOP start/end) or a rewrite onto `{{ ds }}`.

HTTP 400 on a page is treated as "skip this page" for list feeds and
"stop" for VOP. That matched production API quirks; do not treat it
as success without checking ops Slack for under-counts.

## Failure modes

| Failure | Behaviour |
|---------|-----------|
| Bad / empty OAuth creds | Token fetch raises; feed task fails; retries ×3 |
| Transient 5xx / network | Sleep 120s, retry up to 3; then raise |
| Empty API window | Empty NDJSON → branch to `no_data_*` → end |
| GCS copy / BQ load fail | Task fail; notification lists the related task ids |
| dbt fail | Notification marks the dbt task failed; other feed may still succeed |
| Slack provider missing | Message printed to task log instead |

## Downstream

Staging feeds dbt models for trusted KYC, transactions, and VOP
performance tables. Pattern 11 (partner event-bus KYC export) reads
a *refined* KYC table after those models — separate DAG, separate
SLA.
