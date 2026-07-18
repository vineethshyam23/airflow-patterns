# Data flow: multi-country FBO/NBO scoring export

Schedule: `55 0 10 * *` (10th of each month, 00:55). Catchup off — we want
this month's scores, not a replay of every missed calendar day.

## Stage A — Build today's snapshot

`ScoringQuery.get_scoring_export_insert_query()` unions one country SELECT
per market in `ScoringQuery.countries`. Each country path:

1. Split establishments into FBO (no CRM establishment/account id) and NBO
   (CRM-linked)
2. Join the matching scoring model (`analytical_scoring_metro_customer` or
   `analytical_scoring_dish_customer`)
3. Inner-join the trusted customer whitelist and primary authorized person
4. Drop rows with null `bundle_recommendation`
5. Emit `_keyhash`, `_rowhash`, `_valid_from`, `_valid_until`, `_valid_flag`

Result lands in `trusted_staging.scoring_data_export_today` with
`WRITE_TRUNCATE`.

## Stage B — Per-country delta ingest

For each country in the send list (includes PL even without a model):

1. Build the send SELECT: today rows for that `iso_code` whose `_keyhash` is
   new **or** whose `_rowhash` changed vs hist
2. `PythonOperator` → `send_scoring_data(country, query)`
3. Encode Avro → POST chunks of 500 to
   `/ingestbulk/{country}/{schema_id}`

Tasks run in parallel after a `pause` boundary so the today truncate is
fully visible before any country starts reading.

## Stage C — History append + expire

Only after **all** ingest tasks succeed:

1. `WRITE_APPEND` the same delta into `scoring_data_export_hist`
2. `UPDATE` active hist rows whose key is in today but key+rowhash is not —
   set `_valid_until` to end of yesterday and `_valid_flag = false`

This is SCD Type 2 lite. We do not rebuild hist from scratch; we append
change and close the prior version.

## Idempotency and re-runs

- Re-running before hist updates: today truncates again; send may re-post the
  same delta. Downstream ingest should tolerate duplicates (or you clear the
  failed country and re-run that task only).
- Re-running after hist updates: delta is empty — safe no-op on the API.
- Never flip the order (hist before ingest). That is the silent-empty-month
  failure mode.

## Failure modes worth knowing

- OAuth 401 mid-country: client clears token and retries the POST once.
- Empty country result: logged, not failed — expected for PL and quiet markets.
- Whitelist / auth-person join removing "too many" rows: usually a trusted_mcc
  refresh lag, not a scoring model outage. Check those tables before paging
  the model owners.
- Partial country success with hist still pending: the chain waits for all
  ingest tasks; a single country failure blocks hist update so the next run
  can retry the same delta.
