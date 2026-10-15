# Business case: BigQuery API refined zone + AlloyDB dual-store

## Problem

Product dashboards and partner APIs need establishment-scoped facts —
website visits, reservations, orders, POS KPIs, CRM activity — with
sub-second reads and a stable contract. BigQuery is the system of
record for those transforms, but it is the wrong primary store for a
latency-sensitive product API. Dumping every refined table into a
serving database every night is also wrong: most feeds are read from
BigQuery directly; only the CRM activity dashboard needed a Postgres
replica.

I needed one Composer DAG that:

1. Rebuilds the API-facing refined tables daily (full replace is fine —
   the SQL already collapses SCD sources).
2. Keeps a single market dashboard table warm in AlloyDB with
   incremental inserts and conflict-safe re-runs.
3. Does not invent a second orchestration path for "API platform"
   deploys (Gateway / Apigee stay elsewhere).

## Approach that stuck

Parallel `BigQueryInsertJobOperator` tasks WRITE_TRUNCATE six
`refined.api_*` tables plus a wholesale→establishment map. After the
fan-in, a Python task reads AlloyDB `max(created_date)`, pulls the
overlapping BQ window, and inserts with `ON CONFLICT (unique_key) DO
NOTHING`.

Unique keys are MD5 hashes of activity identity fields so re-runs on
the boundary day are cheap no-ops rather than duplicate CRM events in
the serving DB.

## Tradeoffs I accepted

- **Row-by-row Python inserts** instead of `COPY` / `execute_values`.
  The first market feed was small enough that operability beat
  throughput. If the window grows past a few hundred thousand rows per
  day, batch this — the logging every 100 rows is already the smell.
- **Full truncate on BQ API tables** instead of incremental MERGE.
  Downstream API consumers treat these as daily snapshots clustered on
  establishment id. Truncate-reload is simpler to reason about when a
  join upstream changes shape.
- **Hardcoded `bigquery_default` on BQ tasks** even though DEV defines
  another conn. Left as-is in the sample so the footgun stays visible;
  wire `gcp_conn_id` when you promote this pattern.
- **Demo establishment remaps in SQL.** Partner UAT needed synthetic
  SFIDs overlaid on real product data. Ugly, but cheaper than a second
  seed pipeline. Placeholders only in this portfolio copy.

## What this is not

Not an API Gateway, Apigee, or Cloud Function pattern. Those live in
the `api-integrations` repo. This DAG stops at warehouse dual-store
prep for consumers that already exist.
