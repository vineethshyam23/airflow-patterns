# Data flow: Jira Service Desk ingest

## Incremental (default)

1. Schedule fires at 04:00 and 12:00 UTC (`0 4,12 * * *`).
2. For each project key in `jira_project_keys`:
   - Build JQL: `project=KEY AND updated >= interval_start AND updated <= interval_end`
   - Approximate-count preflight; write an empty JSONL if zero
   - Page `/rest/api/3/search/jql` with `nextPageToken`, expand changelog,
     flatten ADF description/comments, append issue dicts in memory
   - Checkpoint to `*_temp_N.jsonl` every 10k rows; write final JSONL under
     `/home/airflow/gcs/data/jira_{project}/`
3. GCSToGCS copies Composer data object → rawzone
4. GCSToBigQuery appends into `trusted_staging.jira_{project}` (JSON column)
5. After all project loads: dbt Cloud job normalizes + dedupes
6. Best-effort persist of the dbt run id into an Airflow Variable

## Full load (`FULL_LOAD_MODE = True`)

1. At DAG parse, call `get_jira_project_date_range` per project
2. `generate_monthly_ranges` → one task per calendar month in span
3. Each monthly task writes
   `{KEY}_full_load_{YYYY_MM}_{execution}.jsonl`
4. `merge_monthly_files` concatenates shards, deletes them, writes the
   single `{KEY}_full_load_{execution}.jsonl`
5. Same upload → BQ append → dbt tail as incremental

## Failure modes

| Failure | What happens | Operator move |
|---------|--------------|---------------|
| 401 on extract | Task fails; Variable token likely rotated | Refresh `jira_service_desk_creds` |
| 429 mid-page | Client sleeps 60s and retries same page | Usually self-heals; widen delay if chronic |
| Empty interval | Empty JSONL + append of zero rows | Fine; dbt no-ops on new keys |
| One monthly task fails | Other months can succeed; merge waits | Clear/re-run the failed month task |
| dbt job missing Variable | EmptyOperator stub runs | Set `jira_dbt_job_id` before prod |
| Parse-time probe fails (full mode) | DAG import error | Fix auth/network before toggling full load |

## Grain and keys

- Extract grain: one Jira issue per JSONL line (key + fields + flattened
  description/comments + changelog histories)
- Warehouse grain after dbt: one row per issue key (latest updated wins)
- Cross-project: separate staging tables per project key; dbt unions
