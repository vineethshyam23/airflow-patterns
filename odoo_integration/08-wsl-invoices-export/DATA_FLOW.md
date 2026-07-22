# Data flow: Odoo WSL invoices dual export

Schedule: `55 5 * * *` (daily 05:55 UTC). Catchup off — a missed run
should be an explicit re-trigger, not an automatic APPEND storm into the
recommender table.

## Stage A — dbt trusted refresh

`DbtCloudRunJobOperator` runs the Cloud job behind Airflow Variable
`dbt_job_odoo_wsl_invoices` (timeout 600s, poll every 10s). That job
materializes `trusted.int_odoo_wsl_invoices` from the warehouse extract of
Odoo WSL billing.

If dbt fails, the chain stops. Neither sink moves. That is the correct
failure: stale recommender + stale events beat divergent sinks.

## Stage B — Recommender APPEND

`BigQueryInsertJobOperator` runs the shared SELECT into
`ml_recommender.odoo_wsl_invoices` with `WRITE_APPEND`.

This is a growing history for the recommender stack, not a SCD Type 2
table. There is no expire step. Dedup / partition strategy belongs to
the ML side (or a follow-up MERGE) — do not pretend APPEND is idempotent.

## Stage C — Event ingest

1. Same SELECT via `OdooWslInvoices.get_send_query()`
2. `PythonOperator` → `send_wsl_invoices_data(country='de', query=...)`
3. Encode Avro → POST chunks of 500 to `/ingestbulk/{country}/{schema_id}`

Full current snapshot. No hash compare against hist. The event consumer
owns upsert / dedup semantics on `uni_key`.

## Idempotency and re-runs

- Re-run after dbt success, before APPEND: APPEND may duplicate the day's
  snapshot; ingest may re-post. Clear or partition before a deliberate
  replay.
- Re-run only the ingest task: safe if the bus tolerates duplicate
  `uni_key` events; coordinate with the consumer otherwise.
- Never flip order (ingest before dbt). You will ship yesterday's trusted
  table and call it today's run.
- Scheduler gap with `catchup=True` (production original): N duplicate
  APPENDs into recommender. Sanitized DAG keeps catchup off for that
  reason.

## Failure modes worth knowing

- OAuth 401 mid-chunk: client clears token and retries the POST once.
- Empty result after `uni_key IS NOT NULL`: usually a broken trusted
  build — fail loudly upstream, do not invent rows.
- dbt timeout (600s): check Cloud job queue / warehouse slot contention
  before bumping timeout again.
- Column mapping: production mapped `theo_total_rec_revenue` from the
  onetime column. Fixed in the sanitized encoder — verify consumers if
  you backport.
- Full-table cost creep: monitor BQ bytes + API chunk counts; add a
  booking_date window when nightly full send stops being honest.
