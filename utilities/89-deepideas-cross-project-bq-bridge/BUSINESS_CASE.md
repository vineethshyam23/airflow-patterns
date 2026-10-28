# Business case: menu-engineering cross-project BQ bridge

Menu engineering runs in its own GCP project. Refined menu tables land
there (VM Postgres → GCS → BigQuery → dbt — pattern 55). Analytics and
product pipelines, though, expect those menus under the main DWH
project's trusted dataset and IAM.

I kept that hand-off as its own one-task DAG instead of folding the
copy into the land DAG or granting every DWH consumer cross-project
read on the menu-engineering project.

## What this unlocked

- DWH consumers query `trusted.co_menus` with ordinary DWH IAM — no
  menu-engineering project grants on every BI / dbt / export SA
- Project boundary stays honest: land owns freshness inside
  `menu_eng_project`; bridge owns the DWH-facing snapshot
- Cheap daily full replace via stock `BigQueryToBigQueryOperator` —
  no GCS hop, no custom operator, no dbt step on the DWH side
- Re-runs are idempotent: truncate + copy again from the authoritative
  refined source

## Constraints

- Schedule (`0 22 * * *`) is the only upstream gate. Production never
  added an `ExternalTaskSensor` on the land DAG. If land slips past
  22:00 UTC, this bridge happily truncates yesterday's good copy with
  a stale (or empty) source.
- `WRITE_TRUNCATE` leaves a brief empty window on the destination.
  Fine for overnight analytics; wrong if a near-real-time API reads
  `trusted.co_menus` during the copy.
- Retries stay at 1 with a 3-minute delay — production values. Bump
  only if BQ slot contention makes the copy flaky.
- No row-count check, no schema assert. Green task means the BQ copy
  job finished, not that cardinality matched yesterday.
- Source project is fixed (authoritative menu zone). Destination
  flips with ENV. DEV mirrors therefore read the shared menu-eng
  project — plan IAM and cost accordingly.

## What this is not

Not Menu Engineering VM Postgres land (pattern 55). Not the intra-DWH
sales mart two-phase copy (pattern 68). Not the weekly recommender /
insights `copy_jobs` registry (pattern 81). This is a daily
single-table external-project → trusted bridge.
