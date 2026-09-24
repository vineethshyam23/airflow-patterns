# Data flow: Reservation Tool incremental Cloud SQL export

## Per-run sequence

1. **start** — empty marker.
2. **get_max_ids**
   - If `execution_date.weekday() == 6` (Sunday): set every
     incremental `max_id` to 0, TRUNCATE matching `rt_*` staging
     tables (ignore missing), push `is_weekly=True`.
   - Else: for each incremental table, `SELECT COALESCE(MAX(id), 0)`
     from staging; default to 0 on missing table / query error;
     push `{table}_max_id` to XCom.
3. For each table in `ALL_TABLES` (sorted union of both catalogs):
   1. **export_<table>** — Cloud SQL Admin CSV to
      `gs://<raw_bucket>/reservationtool/<table>/<ds>/000000/<table>.csv`.
      Incremental queries append `WHERE id > <xcom max_id>`.
   2. **load_<table>** — GCS→BQ into
      `{project}.dwh_trusted_staging.rt_<table>`, schema from
      `schema_json/rt_<table>.json`.
      Incremental: WRITE_APPEND. Full-load: WRITE_TRUNCATE.
4. **all_loaded** — `ALL_DONE` gate after every load.
5. **dbt_rt_run** — dbt Cloud job (Variable), or EmptyOperator stub.
6. **end** — `ALL_DONE`.

Logical date `{{ ds }}` is baked into the GCS path. Re-running the
same `ds` overwrites the same objects; incremental APPENDs are not
idempotent on re-run unless you truncate first — prefer clearing
the load + re-export from a corrected watermark, or wait for Sunday.

## CSV contract

Export SQL applies, per column:

| MySQL case | Export expression |
|------------|-------------------|
| `bit` / `tinyint(1)` in cast set | `IFNULL(CAST(col AS UNSIGNED), '')` |
| Free-text / general columns | `IFNULL(REPLACE… newlines/quotes, '')` |
| Credential columns | Omitted (`EXCLUDE_COLUMNS`) |

Empty CSV fields load as NULL for non-STRING BigQuery types. dbt
staging restores semantic NULLs with `NULLIF(col, '')` and applies
PII masks before restricted views.

## Failure modes

| Symptom | Likely cause | What to do |
|---------|--------------|------------|
| 409 `operationInProgress` | Backup or prior export still running | Operator retries; if exhausted, wait for instance idle and clear the export task |
| Staging grows on re-run | Incremental WRITE_APPEND replayed | Truncate that `rt_*` table or wait for Sunday baseline |
| BQ CSV parse errors | Schema drift or unsanitised quotes | Regenerate schema JSON; confirm SELECT still matches columns |
| `max_id` stuck at 0 | Staging table missing / query failed | First run is a full dump by design; fix IAM / table create |
| dbt job missing | Variable unset | Reference DAG uses EmptyOperator; set `rt_dbt_job_id` in Composer |
| Passwords in staging | Column added without exclude | Keep credentials in `EXCLUDE_COLUMNS`; assert they never enter `_TABLE_COLUMNS` |

## Coupling to dbt

Staging tables are sources for `stg_rt_*` models. dbt owns:

- PII masking and surrogate keys (`_keyhash`, `_rowhash`)
- SCD Type 2 / snapshots for delete detection after Sunday reload
- Restricted / confidential variants of customers and establishments
  (no second Cloud SQL export)

## What is intentionally out of band

- Parallel Cloud SQL Admin exports
- In-DAG SCD merge (see Offer Tool pattern 27)
- Always-full CMS dumps (see Hydra pattern 39)
- BigQuery slot-reservation helpers (`bq_reservation.py` is a
  naming collision — night-ETL slots, not this product)
