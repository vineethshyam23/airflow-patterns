# Business Case: Value Creation Zone bi-monthly refresh

The Value Creation Dashboard answers a blunt question twice a month:
are wholesale-carded hospitality customers growing, churning, or
re-activating — and what is the PSM (propensity / uplift) signal per
market? Finance and commercial ops do not want that answer on a daily
recompute. They want a stable mid-month and early-month snapshot that
lines up with acquisition and MAG reporting calendars.

I kept the refresh as one Composer DAG with a hard sync barrier rather
than 16 country DAGs or a pure dbt project. The reason is operational:
the PSM stored procedure must not run until every country transaction
shard and the global MAG / mapping tables are fresh in staging. An
EmptyOperator (`start_storeproc`) is the cheapest, most visible way to
encode that. Calling `get_psm_uplift_values_v2(iso, env)` for prod and
dev from Airflow also keeps the dual-env contract next to the data
land — not buried in a notebook.

## What this unlocks

- Bi-monthly (3rd + 8th) CREATE OR REPLACE of ~16 markets into
  `trusted_staging.vcd_*` without touching the live dashboard project
  until the barrier clears.
- Country-specific SQL for awkward markets (Austria synthetic IDs,
  null assortment fields, no hospitality filter) in one place.
- A short-TTL discovery union over country transaction shards so the
  stored proc has a single source table and failed runs do not leave
  permanent orphans.
- Run-level status aggregation after ALL_DONE so operators see which
  of the ~100 staging tasks failed without opening the Graph view.

## Tradeoffs I accepted

- One large DAG (~100+ BQ tasks) is harder to unit-test than dbt
  models. Versioned SQL builders help; they do not replace warehouse
  tests on the stored procedure itself.
- CREATE OR REPLACE everywhere is simple and correct for full
  bi-monthly rebuilds. It is also slot-heavy on DE / FR / PL — the
  schedule at 05:15 UTC assumes night-ETL traffic has cooled.
- Prod and dev stored-proc chains share the same barrier. A slow
  country blocks both envs. Parallelizing SP calls further is an
  obvious next lever once the barrier pattern is trusted.
- Production left `max_active_runs` unset. I add `max_active_runs=1`
  here — overlapping 3rd/8th runs when a prior run overruns is how you
  get half-written staging under the dashboard.

## Not this pattern

- Pattern 46: Food Graph refined multi-country zone (daily analytics
  fan-out / fan-in inside DWH).
- Pattern 48 / 52: Offer Tool product-project zone publish.
- Pattern 24: MAG acquisition + penetration monthly *export* (partner
  bus). This pattern *consumes* MAG hist tables as VCD inputs.
- Sibling PSM CSV land DAG: GCS → staging of uplift CSVs after the
  stored proc writes objects — shipped as pattern 66.
