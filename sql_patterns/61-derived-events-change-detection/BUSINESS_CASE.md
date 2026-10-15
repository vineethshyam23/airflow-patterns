# Business case: Derived events change-detection

Product, CRM, and analytics teams needed a single append-only store of
**business events** — not raw SCD rows — so engagement scoring and
lifecycle reporting could ask "what changed, for whom, when?" without
re-deriving LAG logic in every consumer.

Three source systems feed the store:

- **CMS** (website/establishment content) — config and content edits
- **Adobe Analytics** — custom hit events already instrumented on the
  product surface
- **Reservation Tool** — capacity, channels, menus, notification settings

The warehouse already historized those tables. The missing piece was a
stable contract: entity key, event name, timestamp, optional payload,
source label, and a hash that makes daily re-runs idempotent.

## Why this lived in Composer

In 2018 the cheapest path was one DAG of BigQueryInsertJobOperators with
inline SQL. That choice aged poorly — ~58 sequential tasks, long wall
clock, one failure blocks the rest — but the **detection contracts** are
still the useful engineering:

1. SCD `LAG()` over `_valid_from` for attribute changes
2. Hit unnest + event dictionary for analytics custom events
3. MD5 `_rowhash` anti-join so APPEND is safe to re-run
4. Occasional one-record-per-entity-per-day ranking when SCD churn is noisy

I would not greenfield a 3k-line monolith today. I would keep the SQL
contracts, split by source system, and parallelize independent groups.
The pattern worth shipping is the change-detection / event-store shape,
not the linear chain.

## Downstream

Activity scores and engagement views read `trusted.derived_events`.
They care about the schema and hash semantics more than which Airflow
task wrote the row. Getting those wrong is more expensive than a slow
DAG — especially Adobe's hit-scoped hash, which differs from the SCD
formula.
