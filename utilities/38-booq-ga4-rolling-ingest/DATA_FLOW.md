# Data flow: POS vendor GA4 rolling event ingest

## Steps

1. **Fan-out day loads** — For each `day_offset` in 1..7, a
   `BigQueryInsertJobOperator` runs a scripted DELETE+INSERT against
   staging for `DATE_SUB(CURRENT_DATE(), INTERVAL day_offset DAY)`.
2. **Source filter** — Read from
   `{project}.analytics_{property}.events_*` where `_TABLE_SUFFIX`
   equals the `YYYYMMDD` of that offset day.
3. **Staging write** — Target
   `{project}.analytics_{property}_temp.pos_vendor_ga_events`. Delete
   that date first so re-runs replace, not append.
4. **Data Transfer** — After all seven loads succeed,
   `run_Data_transfers` starts a manual BQ Data Transfer config
   (resource name from Variable) and sleeps ~6 minutes.
5. **dbt** — One Cloud job transforms staging / transfer outputs into
   trusted GA4 models. Timeout 1000s; poll every 10s.
6. **Run ids** — `get_runids_task` runs with `ALL_DONE`, pulls XCom
   return value or job URL, writes Variable
   `etl_booq_google_analytics_runids`.

## Date coupling

SQL uses `CURRENT_DATE()` at BigQuery runtime, not the Airflow logical
date. The morning schedule is stable; a manual re-run always reloads
"today's" rolling window, not the DAG run's `ds`. For historical
backfill, extend the offset loop or rewrite with
`{{ macros.ds_add(ds, -n) }}`.

## Failure modes

| Failure | Behaviour |
|---------|-----------|
| Missing GA4 shard for a day | INSERT inserts zero rows; task still succeeds |
| BQ job fail on one day | That task retries once (10 min); siblings unaffected until fan-in |
| Transfer config Variable missing | Transfer task fails before sleep |
| Transfer slow / failed | Sleep still completes; dbt may see stale companion tables |
| dbt fail | `get_runids_task` still runs (`ALL_DONE`); Variable may be partial |
| `dagrun_timeout` (20 min) | Whole DAG marked failed if chain overruns |

## Downstream

Staging + transfer outputs feed the dbt Cloud job (product funnels,
session metrics). BI dashboards read trusted models — out of scope for
this folder. A sibling brand DAG loads a different GA4 property into
trusted staging without the transfer step.
