# Data flow: Multi-country platform-customer footprint export

## Daily chain

1. Upstream wholesale account, CRM clean IDs, match_result, analytical
   customer, product-spot asset, and establishment base loads land for
   the export markets.
2. Daily schedule — `etl_platform_customer_export` starts at 05:05 UTC
   (`catchup=False`, `max_active_runs=1`).
3. **insert_table_*** — 14 parallel `BigQueryInsertJobOperator` tasks.
   First country (`PL` in the list) WRITE_TRUNCATEs
   `staging.platform_customer_staging`; the rest WRITE_APPEND.
4. **pause** — barrier so all inserts finish before dbt.
5. **dbt_platform_customer_table_refresh** — dbt Cloud job (id from
   Variable `dbt_job_platform_customer_export`) materializes
   `refined.platform_customer_export` with `_valid_flag` / `_valid_from`.
6. **ingest_*** — 14 parallel PythonOperators. Each runs
   `get_send_query(country)` then
   `send_platform_customer_data(country, query)`: Avro encode, POST
   chunks of 500. 401 → clear token, retry same payload.

Idempotency: re-running ingest without refreshing staging/dbt re-sends
today's valid rows. Partner ingest should be upsert-friendly, or
coordinate a full resend with them.

## Matching rules (do not silently change)

Insert SQL prefers CRM-cleaned wholesale↔establishment links, then
adds fuzzy `match_result` pairs that are not already covered, then
full-outer-joins POS matches from a secondary establishment source.
When a wholesale_id appears in both subscription and POS paths, product
flags take `max` / `greatest`. HR additionally requires `status_cd = 1`.

Changing "prefer CRM clean" to "always fuzzy" or dropping the POS
union will shift customer counts in partner reports without an obvious
Composer failure.

## Staging contract quirk

All countries share one staging table. A mid-run failure after truncate
can leave the table with a subset of countries. Production relied on
retries + ALL_SUCCESS before dbt. Prefer a row-count / country-count
check before dbt if you harden this pattern.

## Failure modes

- One insert fails → ALL_SUCCESS blocks pause/dbt; staging may be
  partially rewritten. Fix upstream data or SQL, clear/restart.
- dbt fails after inserts → staging is fresh; refined unchanged; no
  POST. Fix the dbt job and resume from dbt (or restart the DAG).
- One ingest fails after dbt → other countries may already have POSTed.
  Re-run failed ingest tasks only; expect duplicate events for markets
  you re-send unless partner ingest is exactly-once.
- OAuth 401 storm → token refresh retries per chunk; raise on other
  HTTP errors (sanitized sample). Production mostly logged responses.

## Field notes

- Wide Avro contract: identity + bundle + per-product Y/N and
  create/delete timestamps + referrers + acquisition/deletion dates.
- Send filter is `_valid_flag = True AND date(_valid_from) >= current_date()`.
  That is today's open version, not a hash-delta.
- `has_Menukit` / obsolete `has_POS` columns travel as zeros/nulls in
  the insert SELECT for schema stability with older partner consumers.
- BE is in the ISO map but not in `country_list`. Adding it is a
  product decision, not a typo fix.
