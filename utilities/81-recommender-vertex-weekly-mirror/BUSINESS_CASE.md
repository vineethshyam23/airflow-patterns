# Business case: recommender Vertex weekly mirror

Article recommender and Insights share consumers needed a stable,
queryable copy of two different source shapes on one weekly clock:

1. Market potential tables that live in a separate REX Vertex
   project (cross-project `SELECT *`).
2. A curated set of DWH tables / light joins published into a
   dedicated `insights_share` dataset for downstream product and
   analytics teams.

I kept both groups in one DAG behind a single `copy_jobs` registry
instead of two schedules. Same write disposition (`WRITE_TRUNCATE`),
same priority (`BATCH`), same Monday window. Adding a mirror is a
dict (or a `SHARE_QUERIES` entry), not a new Composer DAG.

## What this unlocked

- Declarative onboard path: one entry = one SELECT + destination
  dataset + table id
- Parallel fan-out after `start` — Poland market copies and the
  insights share copies run together under `max_active_runs=1`
- Cross-project REX → DWH refined without pulling REX credentials
  into application code (Composer SA IAM only)
- Intra-DWH share dataset that can reshape keys (`establishment_id`)
  without inventing a second orchestration style
- Cheap weekly freshness via BATCH priority; slot spikes stay off
  the weekday interactive path

## Constraints

- Schedule is `0 11 * * 1`. Between Mondays the destinations are
  stale by design — fine for potential rankings and share tables,
  wrong if anyone treats them as near-real-time.
- Full WRITE_TRUNCATE each run. No watermark, no SCD. Re-runs
  overwrite the destination tables completely.
- Source REX project is fixed to prod even when destination is DEV.
  That is intentional for market potential (one authoritative zone)
  but means DEV mirrors read prod REX — IAM and cost planning must
  account for it.
- No post-copy row-count gate. A green task means the job finished,
  not that source and destination cardinality matched.
- Insights share queries are SELECT-only by contract. Do not add
  DDL/DML into `SHARE_QUERIES` — keep the registry load path boring.

## What this is not

Not Food Graph ML propagation (pattern 40). Not Vertex PipelineJob
submit (pattern 45). Not POS Intelligence recommendations → partner
event bus (pattern 53). Not HD export → trusted MCC parallel
materialization (pattern 80) — that is a daily country-list + Slack
aggregate contract. This is a weekly declarative multi-destination
BigQuery mirror registry.
