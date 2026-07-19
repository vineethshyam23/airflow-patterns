# Data flow: Salesforce asset history delta export

Schedule: `40 3 * * *` (daily 03:40). Catchup off — we want today's CRM
snapshot, not a replay of every missed calendar day.

## Stage A — Build today's snapshot

`AssetQuery.get_asset_export_insert_query()` reads
`refined.sfdc_asset_history_ES`, casts columns to the outbound contract, and
emits `_keyhash`, `_rowhash`, `_valid_from`, `_valid_until`, `_valid_flag`.

Result lands in `trusted_staging.sfdc_asset_history_today` with
`WRITE_TRUNCATE`.

## Stage B — Delta ingest

1. Build the send SELECT: today rows whose `_keyhash` is new **or** whose
   `_rowhash` changed vs hist
2. `PythonOperator` → `send_sfdc_asset_history_data(query, country='es')`
3. Encode Avro (dates as logicalType date) → POST chunks of 500 to
   `/ingestbulk/{country}/{schema_id}`

A `pause` boundary sits between truncate and ingest so the today table is
fully visible before the Python task starts reading.

## Stage C — History append + expire

Only after ingest succeeds:

1. `WRITE_APPEND` the same delta into `sfdc_asset_history_hist`
2. `UPDATE` active hist rows whose key is in today but key+rowhash is not —
   set `_valid_until` to end of yesterday and `_valid_flag = false`

SCD Type 2 lite. We do not rebuild hist from scratch; we append change and
close the prior version.

## Idempotency and re-runs

- Re-running before hist updates: today truncates again; send may re-post
  the same delta. Downstream ingest should tolerate duplicates (or clear
  the failed task and re-run that step only).
- Re-running after hist updates: delta is empty — safe no-op on the API.
- Never flip the order (hist before ingest). That is the silent-empty-day
  failure mode.

## Failure modes worth knowing

- OAuth 401 mid-chunk: client clears token and retries the POST once.
- Empty delta: logged, not failed — expected after a quiet CRM night.
- Refined table lag (CRM sync late): today truncates to a stale snapshot;
  check upstream SFDC → warehouse jobs before paging CRM admins.
- Ingest success with hist still pending: chain waits; a failure before
  hist update means the next run can retry the same delta.
