# Data flow: Odoo list-price / commission monthly delta export

Schedule: `55 2 1 * *` (01st of month, 02:55 UTC). Catchup off — we want
the current month-end close snapshot, not a replay of every missed calendar
month (backfills need an explicit manual run).

## Stage A — Build today's snapshot

`OdooListPrice.get_odoo_list_price_insert_query()` reads
`refined.odoo_wsl_invoice_lines` for the pilot market, joins
`discovery.ic_price_list` and customer id cleanup, aggregates the outbound
commission measures, and emits `_keyhash`, `_rowhash`, `_valid_from`,
`_valid_until`, `_valid_flag`.

Result lands in `trusted_staging.odoo_list_price_today` with
`WRITE_TRUNCATE`.

## Stage B — Delta ingest

1. Build the send SELECT: today rows whose `_keyhash` is new **or** whose
   `_rowhash` changed vs hist
2. `PythonOperator` → `send_odoo_list_price_data(query, country='fr')`
3. Encode Avro (dates as logicalType date) → POST chunks of 500 to
   `/ingestbulk/{country}/{schema_id}`

A `pause` boundary sits between truncate and ingest so the today table is
fully visible before the Python task starts reading.

## Stage C — History append + expire

Only after ingest succeeds:

1. `WRITE_APPEND` the same delta into `odoo_list_price_hist`
2. `UPDATE` active hist rows whose key is in today but key+rowhash is not —
   set `_valid_until` to end of yesterday and `_valid_flag = false`

SCD Type 2 lite. We do not rebuild hist from scratch; we append change and
close the prior version.

## Idempotency and re-runs

- Re-running before hist updates: today truncates again; send may re-post
  the same delta. Downstream ingest should tolerate duplicates (or clear
  the failed task and re-run that step only).
- Re-running after hist updates: delta is empty — safe no-op on the API.
- Never flip the order (hist before ingest). That is the silent-empty-month
  failure mode.
- Mid-month credit note: wait for next scheduled run, or trigger the DAG
  manually once the refined WSL table has the credit.

## Failure modes worth knowing

- OAuth 401 mid-chunk: client clears token and retries the POST once.
- Empty delta: logged, not failed — expected after a quiet billing month.
- Refined lag (Odoo extract late on the 1st): today truncates to a stale
  snapshot; check upstream Odoo → warehouse jobs before paging finance.
- Ingest success with hist still pending: chain waits; a failure before
  hist update means the next run can retry the same delta.
- Price book gap (`ic_price_list` miss): commission measures go null/zero
  for that product — production finance flagged these via separate DQ;
  do not silently invent list prices in the export.
