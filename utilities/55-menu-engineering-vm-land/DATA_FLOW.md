# Data flow: Menu Engineering VM Postgres land

## Per-run sequence

1. **start** — empty marker.
2. **prepare_vm** — ensure CSV directory exists and is writable;
   enable gcloud parallel composite upload; wipe stale tracker
   files under `~/.config/gcloud/surface_data/storage/tracker_files/`.
3. **export_\*** (parallel, one task per table) — SSH:
   `docker exec … psql -c "COPY schema.table TO STDOUT WITH CSV HEADER"`
   redirected to `{csv_dir}/{table}.csv`. Fail if the file is missing.
4. **upload_product_\*** (parallel) — SSH:
   `gcloud storage cp` into
   `gs://{product_bucket}/{product_prefix}/{table}.csv`.
5. **cleanup_vm_after_upload** — journald vacuum (3d), delete product
   `*.log` older than 2 days, remove rotated `/var/log` archives,
   delete `*.csv` in the CSV directory.
6. **copy_rawzone_\*** (parallel) — Composer GCS→GCS into
   `{raw_prefix}/menu_engineering/{ds}/{table}.csv`.
7. **load_staging_\*** (parallel) — GCS→BQ
   `{dwh_project}.{staging}.me_{table}_tbl`, WRITE_TRUNCATE,
   schema from `schema_json/{table}.json`.
8. **dbt_me_run** — dbt Cloud job (Variable), or EmptyOperator stub.
9. **end** — `ALL_DONE`.

Logical date `{{ ds }}` is only on the **rawzone** path. The product
bucket path is a rolling overwrite (`…/{table}.csv`). Re-running the
same `ds` overwrites the dated rawzone objects and TRUNCATEs staging
again — safe for full-dump semantics.

## Object layout

| Hop | Path |
|-----|------|
| VM disk | `/home/postgres_csv_files/{table}.csv` |
| Product GCS | `gs://{me_product_bucket}/menu_engineering/{table}.csv` |
| DWH rawzone | `gs://{me_raw_bucket}/menu_engineering/menu_engineering/{ds}/{table}.csv` |
| Schema | `gs://{me_raw_bucket}/schema_json/{table}.json` |
| Staging | `{me_dwh_project}.dwh_trusted_staging.me_{table}_tbl` |

## Failure modes

| Symptom | Likely cause | What to do |
|---------|--------------|------------|
| SSH timeout / auth failure | Conn broken or key rotated | Fix `me_vm_ssh_conn_id`; re-run bootstrap runbook |
| `Export failed: file not created` | docker / psql / schema.table missing | Check container name, DB, schema; confirm table exists |
| gcloud storage JSON decode on upload | Stale composite tracker files | `prepare_vm` already wipes them; clear manually if an old image skipped prepare |
| Product upload OK, rawzone copy 403 | Composer SA missing read on product bucket or write on rawzone | Fix GCS IAM; do not grant VM SA rawzone write as a shortcut |
| BQ CSV parse errors | Schema drift / quoted newlines | Refresh `schema_json/{table}.json`; keep `allow_quoted_newlines=True` |
| VM disk full | Cleanup skipped or failed mid-run | Re-run from cleanup, or SSH and delete `*.csv` / old logs |
| dbt job missing | Variable unset | Reference DAG uses EmptyOperator; set `me_dbt_job_id` |
| Two runs collide on CSV names | Overlap | Keep `max_active_runs=1` |

## Coupling to dbt

Staging `me_*_tbl` tables are sources for menu-engineering trusted
models. dbt owns type cleanup, SCD / snapshots if needed, and any
PII masking. The DAG does not invent key/row hashes in SQL.

## What is intentionally out of band

- Cloud SQL Admin export + 409 retry (see #39 / #54)
- Partner Avro enrichment exports (see #20–#22)
- Incremental watermarks — tables here are full-dump
- Shipping the one-time SSH keygen / VM metadata tasks in the daily
  graph (runbook only)
- Pulling Postgres through Composer as a direct client
