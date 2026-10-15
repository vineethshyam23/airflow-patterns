# Business Case: Daily refined-zone SCD spine

The overnight refined zone is the contract between trusted land and
everything that sells numbers — partner Avro exports, MAG hist siblings,
Salesforce refined aggregates, and analyst dashboards. Production packs
that into one ~4.7k-line DAG. I do not ship the monolith; I ship the
spine that actually carries the risk: materialize analytical actuals,
apply hash-based SCD Type 2, quarantine test establishments, and pin
the jobs to a night BigQuery reservation so interactive BI does not
fight ETL for slots.

Hash SCD2 here is deliberate. Matching-engine SCD (#01) owns match-pair
history with richer SQL builders. Derived-events (#61) appends change
rows into an event store via LAG. MAG month-grain hist (#64 / #65) is a
calendar append, not a daily expire. This spine is the daily
`_keyhash|_rowhash` insert-then-expire loop that keeps Hydra and
Reservation Tool analytical hist tables point-in-time queryable without
rewriting full snapshots every night.

## What this unlocks

- Current actuals tables (`analytical_*_actual`) rebuilt every night
  via WRITE_TRUNCATE from refined views — cheap, idempotent, readable.
- Hist tables that grow only when a hash pair is new, and close
  yesterday when a pair disappears from staging.
- Dual conservative / progressive test-establishment lists so Order and
  mapping consumers can pick a strict or loose anti-join without
  re-deriving regex in every DAG.
- Night-ETL reservation pinning so a 03:15 fan-out does not blow the
  on-demand budget shared with Tableau / console users.

## Tradeoffs I accepted

- Sequential actuals before hist branches. Parallelizing every
  WRITE_TRUNCATE would finish faster on a quiet night and saturate the
  reservation on a busy one. Predictable slot pressure wins.
- Insert-then-expire order (not expire-then-insert). A failed expire
  leaves stale `_valid_flag=TRUE` rows alongside the new version — ugly
  but recoverable. Expire-first risks a window with no current row if
  insert fails. Prefer over-current and fix with a clear+rerun.
- Deferrable expire UPDATEs. Workers free up while BigQuery runs; the
  triggerer owns the wait. Requires a Composer image that supports
  deferrable BQ operators — worth it when you have a dozen hist tables.
- Focused subset only. Country MCC fan-out, Food Graph mappings, Order
  refined tables stay out of this folder. Shipping them as one "pattern"
  would be a code dump, not a teachable contract.

## Not this pattern

- Pattern 01: Matching Engine SCD Type 2 (match-pair domain + builders).
- Pattern 46: Food Graph refined multi-country zone.
- Pattern 61: Derived events SCD LAG → append-only event store.
- Pattern 64 / 65: MAG monthly WRITE_APPEND historization (calendar grain).
- Pattern 59: Salesforce refined daily aggregates (CRM-facing fan-out).
- The remaining arms of production `etl_refined_zone` (MCC, mappings,
  Order, bundle-over-time) — evaluate separately if depth warrants.
