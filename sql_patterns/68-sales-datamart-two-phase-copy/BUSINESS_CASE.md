# Business Case: Sales data mart two-phase copy

Sales and a handful of Composer siblings need the same dozen analytical
objects every morning. Giving them IAM on the full trusted / refined
layer is the lazy answer and the wrong one — it couples BI users to
internal SCD views, leaks objects they should never touch, and makes
every schema change a permissions ticket. The better contract is a
dedicated mart dataset (`refined_sales`) that is a deliberate, named
subset of the warehouse, refreshed daily, with WRITE_TRUNCATE so
consumers always see a coherent snapshot.

This is not the same problem as Salesforce refined aggregates (#59).
That pattern *builds* CRM-shaped tables with SQL transforms. This
pattern *relocates* already-built analytical views and trusted tables
into a team dataset. No business logic in the DAG — the engineering
value is the two-operator copy strategy, the phase barrier, and the
permission boundary.

## What this unlocks

- A Sales-facing dataset with a short, reviewable catalog instead of
  "everything in trusted".
- Views become queryable tables in the mart (BigQuery-to-BigQuery does
  not copy views; phase 1 materializes them).
- Tables copy natively in phase 2 with EU location pinning — cheaper
  and cleaner than another SELECT * when the source is already a table.
- CRM decommission without rewriting the mart contract: freeze the old
  SFDC copies in place, drop them from the live catalog, keep subscription /
  Hydra / catalog / Odoo / analytical-actual refresh running.

## Tradeoffs I accepted

- Full daily WRITE_TRUNCATE on every object. Incremental MERGE would
  save slots on large tables and complicate "is the mart current?"
  answers. For a sales mart that BI treats as a daily cut, truncate
  wins until cost complains.
- Soft trigger rule (`none_failed_min_one_success`). One failed view
  should not discard the whole phase when siblings are independent —
  but operators must still investigate partial mart freshness.
- Pause Dummy/Empty between phases instead of `max_active_tasks`. The
  pause is an intentional slot-pressure valve after N parallel insert
  jobs before launching native copies. Cap concurrency too if the
  project is slot-constrained; the barrier alone is not a throttle.
- No upstream sensors. The 08:05 schedule assumes overnight land is
  done. A late trusted load means the mart copies yesterday's truth —
  acceptable for Sales BI, not for finance close.

## Not this pattern

- Pattern 59: Salesforce refined daily aggregates (SQL fan-out into
  CRM-facing snapshots).
- Pattern 05: SFDC asset-history hash-delta → Avro event bus.
- Pattern 64 / 65: MAG monthly historization (calendar APPEND, not a
  daily mart copy).
- Pattern 67: Daily refined-zone SCD spine (builds the analytical
  actuals this mart may later copy).
- Thin acquisition-ID monthly snapshot (`etl_auto_history_acquisition_IDs_*`)
  — same MAG family as #65, not a mart isolation pattern.
