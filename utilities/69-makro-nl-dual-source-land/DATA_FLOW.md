# Data flow: Wholesale NL dual-source land

## Branch 1 — MCC customer base

1. **Auth** — Password-grant OAuth2; Basic client id/secret, form username
   / password. Token cached on the client; 401 clears token and retries.
2. **Paginate** — `GET …?last_mutday_from={{ ds }}&offset=&rows=10000`.
   Write each `result` row as JSONL to Composer
   `data/wholesale/customer_NL/customer_base_{ds}.json`.
3. **Promote** — Composer bucket → rawzone
   `wholesale/customer_base_NL/{ds}/customer_base.json`.
4. **Load** — Treat JSONL as CSV with tab delimiter into
   `trusted_staging.wholesale_customer_NL(value JSON)`, WRITE_TRUNCATE.
5. **dbt** — Deferrable Cloud job; run id stored in
   `etl_wholesale_nl_customer_dbt_runids` (ALL_DONE).

## Branch 1b — Merge requests (parallel)

1. **GET** merge-request endpoint (no date filter in production).
2. Project winning/losing ids + status to JSONL.
3. Promote to rawzone; load as NDJSON with schema object from rawzone
   `schema_json/wholesale_customer_merge_requests.json`.
4. No dbt step — staging only for later enrichment / reverse sync.

## Branch 2 — CHD market CSV

1. **ShortCircuit `check_file`** — List landing-zone blobs matching
   `WHOLESALE_NL_*.csv` outside `processed/`. Return False → skip rest.
2. **Clean in place** — For each file: strip whitespace from numeric
   columns; invalid integer strings (PHONE, review counts, etc.) →
   empty string (NULL on load); overwrite blob.
3. **Load** — CSV → `trusted_staging.chd_market_data_NL` with
   `schema_fields`, `max_bad_records=0`, jagged rows allowed.
4. **Archive** — Move matched objects to `processed/WHOLESALE_NL_…`.
5. **dbt** — Second Cloud job; run id in `etl_chd_runids` (ALL_DONE).

## Failure modes

| Failure | Behaviour |
|---------|-----------|
| OAuth / API down | Customer or merge branch fails; other chains still run |
| Empty mutation window | Empty JSONL → truncate staging to empty; dbt still runs |
| Merge branch fails | Customer load still runs (`all_done`) |
| No CHD files | ShortCircuit skips CHD branch (not a failure) |
| Dirty integer after clean | Load fails (`max_bad_records=0`) — investigate file |
| dbt timeout | Was worse when DAG timeout was 20m; reference uses 60m |
| CHD replay | Move objects from `processed/` back to landing prefix |

## Downstream

Staging + dbt feed discovery enrichment tables consumed by the HubSpot
/ MCC reverse-export DAG (outbound pattern, not this folder). CRM and
field-sales tools read those enrichment tables; they are out of scope
here.
