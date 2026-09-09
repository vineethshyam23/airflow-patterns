# Data flow: Hydra Cloud SQL weekly full export

## Per-run sequence

1. **start** — empty marker.
2. For each table in `HYDRA_RAW_TABLES` (order matters):
   1. **export_<table>** — Cloud SQL Admin `exportContext` with
      `selectQuery` from `RAW_TABLE_EXPORT_QUERIES[table]`, CSV to
      `gs://<raw_bucket>/hydra_raw/<table>/<ds>/000000/<table>.csv`.
   2. **load_hyd_v2_<table>** — GCS→BQ into
      `{project}.dwh_trusted_staging.hyd_v2_<table>`, WRITE_TRUNCATE,
      schema from `schema_json/hyd_v2_<table>.json`.
3. **hydra_v2_dbt_job** — dbt Cloud job (Variable), after last load.
4. **end** — `ALL_DONE` so partial failures still close the run UI.

Logical date `{{ ds }}` is baked into the GCS path. Re-running the
same `ds` overwrites the same objects and truncates staging again —
idempotent at the raw/staging boundary.

## CSV contract

Export SQL applies, per column:

| MySQL case | Export expression |
|------------|-------------------|
| `bit` / `bit(n)` | `IFNULL(CAST(col AS UNSIGNED), '')` |
| STRING-mapped types | `IFNULL` + doubled `"` + CR/LF → `<br />` |
| Other numerics / dates | `IFNULL(col, '')` |

Empty CSV fields load as NULL for non-STRING BigQuery types. String
columns keep the newline placeholder through staging; restore in dbt
only if a consumer needs real line breaks.

## Failure modes

| Symptom | Likely cause | What to do |
|---------|--------------|------------|
| 409 `operationInProgress` | Backup or prior export still running | Operator retries; if exhausted, clear task and wait for instance idle |
| BQ CSV parse errors | Schema drift or unescaped quotes | Regenerate schema JSON; confirm SELECT still matches columns |
| Load skipped / upstream failed | Export never wrote GCS object | Fix export first; do not clear only the load |
| dbt job missing | Variable unset | Reference DAG uses EmptyOperator; set `hydra_v2_dbt_job_id` in real Composer |
| Sensitive data in staging | Table added without review | Keep sensitive / OAuth keys out of `HYDRA_RAW_TABLES`; assert guards fail import |

## Coupling to dbt

Staging tables are sources for `stg_hyd_v2_*` views. Password /
reset-token columns on `users` are dropped in generated staging SQL
via `HYDRA_STG_EXCLUDED_COLUMNS`. Snapshots use check-all strategy
with `hard_deletes=invalidate` so weekly TRUNCATE+reload still
detects removals.

## What is intentionally out of band

- Incremental `WHERE id > …` watermarks (rejected for CMS volatility)
- Restricted / confidential establishment streams
- OAuth token tables
- Legacy `etl_hydra` INSERT/UPDATE trusted-layer path (derived-events
  allowlist only)
