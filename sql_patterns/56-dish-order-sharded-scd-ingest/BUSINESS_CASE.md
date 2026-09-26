# Business case: multi-shard food-ordering land with SCD Type 2

## Problem

The ordering product splits tenant databases across many Cloud SQL
MySQL shards so no single instance holds every restaurant. Analytics
still needs one trusted table per entity (`order_orders`,
`order_customers`, …) with history — which tenant moved to which
shard, which menu row changed, which payment status flipped — without
replaying full nightly dumps into BigQuery as if the world were a
single database.

A naïve “export every shard into its own dataset” collapses downstream
joins. A naïve “one giant federated query” does not survive Cloud SQL
export quotas or Composer task runtimes when you have dozens of
instances.

## Approach that stuck

1. **Discover** the tenant→shard map from the master registry once per
   run (`getdbs.sh`).
2. **Filter** that map per shard IP so each export task only touches
   databases that live there.
3. **Export in parallel** across shards (and once for master dims),
   computing `_keyhash` / `_rowhash` in MySQL so BigQuery only compares
   pairs.
4. **Merge** shard CSV fragments into one raw-zone object per table —
   the trusted layer never sees shard identity.
5. **SCD Type 2** per table: snapshot → truncate-load staging → insert
   new hash pairs → expire missing ones → promote. Night-ETL
   reservation pins the heavy query jobs so on-demand slots stay for
   humans.

`max_active_runs=1` is non-negotiable: overlapping merges corrupt the
raw-zone objects the load chains read.

## Tradeoffs I accepted

- **Bash + `gcloud sql export`** instead of
  `CloudSQLExportInstanceOperator`. The house scripts already knew
  how to walk tenant DB lists and sleep between tables; rewriting
  forty-plus shards onto the Python operator was not worth the cutover
  risk when this pipeline was already the overnight critical path.
- **Parse-time `date.today()`** in the staging load path. It matches
  production and fails loudly when a task requeues across midnight —
  ugly, but preferable to silently loading yesterday’s merge under a
  templated path that no longer exists. A `{{ ds }}` rewrite is the
  right cleanup when someone next touches the DAG.
- **Insert → update → copy** instead of a single MERGE. Same house
  SCD as Offer Tool (#27). Easier to clear one failed step in the UI
  than to re-debug a 2k-line MERGE when a hash column drifts.
- **Elevated `max_bad_records` on `payment_logs`**. That CSV is noisy;
  failing the whole shard merge over a handful of jagged rows costs
  more than tolerating them and spotting volume drops in monitoring.

## What this is not

Not a watermark / incremental id strategy (see #54). Not a weekly
full-dump operator library (see #39). Not the partner Avro export of
order lifetime metrics for a single market (see #26). Those answer
different questions. This pattern answers: “how do you land a
sharded multi-tenant OLTP product into one historized trusted schema
every night without melting the source?”
