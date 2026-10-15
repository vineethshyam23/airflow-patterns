# Data flow: AppFigures weekly ingest

## Run order

1. Scheduler fires Monday ~03:59 UTC (`59 3 * * 1`).
2. Parse-time (or worker) computes previous Mon–Sun as
   `start_date` / `end_date`.
3. For each report in parallel:
   - `data_fetch_*` GETs AppFigures CSV →
     `data/appfigures/appfigures_{name}.csv` on Composer
   - `upload_storage_*` copies to
     `gs://{rawzone}/appfigures/{name}/{end_date}/{name}.csv`
   - `load_staging_*` TRUNCATEs
     `{project}.trusted_staging.appfigures_{name}`
   - `copy_table_trusted_*` APPENDs into
     `{project}.trusted.appfigures_{name}`
4. `stage` waits for all four chains (`TriggerRule.ALL_DONE`).
5. `appfigures_dbt` runs the Cloud job (or EmptyOperator in sample).
6. `end`.

## Report grains

| file_name | API report | group_by | Tables |
|-----------|------------|----------|--------|
| sales | sales | products,countries,dates | appfigures_sales |
| ratings | ratings | product,date | appfigures_ratings |
| ratings_product | ratings | product | appfigures_ratings_product |
| ratings_country | ratings | country | appfigures_ratings_country |

## Date window

```python
start = today - timedelta(days=today.weekday() + 7)  # last Monday
end   = start + timedelta(days=6)                    # last Sunday
```

Production evaluated this at **module import**. That means:

- Steady Monday runs usually get the intended prior week
- Backfills / long-lived DAG processors can pin a stale window
- Prefer `{{ data_interval_start }}` macros in a rewrite

## Idempotency

- Staging: truncate per run — safe to re-load the same CSV
- Trusted: append — re-runs duplicate the week
- Recovery: delete trusted rows for `{end_date}` week, then clear
  from fetch or upload and re-run

## Failure modes

| Failure | Effect | What to do |
|---------|--------|------------|
| Bad / missing auth token | Fetch raises (sanitized) | Fix Variable; re-run chain |
| API 5xx / timeout | Task retries (3 × 10 min) | Wait vendor; then clear |
| Schema drift vs schema_json | BQ load fails; trusted untouched | Align schema object |
| One chain fails, others OK | Stage may still fire (`ALL_DONE`) | Check dbt inputs; prefer `ALL_SUCCESS` if you rewrite |
| dbt timeout (300s) | Job fails after loads | Raise timeout or slim models |
| Re-run without prune | Duplicate trusted rows | Delete week partition / filter |

## Scale notes

AppFigures weekly CSVs are small. Wall-clock is API latency + four
BQ loads, not compute. Keep `max_active_runs=1` so Monday overlaps
cannot double-append.
