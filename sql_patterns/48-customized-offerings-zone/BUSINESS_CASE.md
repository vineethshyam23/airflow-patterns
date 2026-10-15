# Business Case: Offer Tool multi-project zone fan-out

The Offer Tool (field-sales product) needs wholesale assortment, visit /
article analytics, Food Graph gaps, benchmarking, and FBO/NBO scores
in *its own* GCP projects — not as live queries against the DWH. Product
teams deploy against a stable dataset (`offer_tool_zone`) with WRITE
access isolated from warehouse IAM.

I kept the publish step in Composer rather than a dbt project-to-project
copy because the hard problem is operational, not SQL:

1. **Weekday stage fan-out** — rebuilding acc + stg + prod every night
   was burning slots for little benefit. Wednesday widens to all three
   product stages; other days refresh prod only.
2. **DEV Monday gate** — ShortCircuitOperator so the shared DEV Composer
   does not thrash offer-tool-dev five nights a week.
3. **Nested gaps reconciliation** — the zone nests `fg_gaps` for the
   product UI; warehouse trusted keeps unnested. A truncate into
   `monitoring` plus an alert task catches count drift before sales
   notices empty ingredients.

## What this unlocks

- Per-stage product projects that stay schema-compatible with the
  Offer Tool release trains (acc → stg → prod).
- Country × stage nested loops for assortment / masterdata / visit /
  article without hand-written DAG copies.
- Soft-delete filtering at publish time (`exclude_deleted_statement`)
  so deleted wholesale cards never land in the product zone.
- Branch-level article ranking materialized once per stage, then
  joined per country for recommendation tables.

## Tradeoffs I accepted

- `schedule_interval=None` in the portfolio DAG mirrors production's
  trigger-aligned run (often kicked after refined Food Graph / scoring
  freshness). Cron ownership lives with the ops runbook, not this file.
- WRITE_TRUNCATE everywhere is correct for a product snapshot and
  expensive on Wednesday multi-stage days. Incremental merge is the
  next cost lever once stage parity checks are automated.
- Slack / webhook alerting is intentionally stubbed to `print` in the
  portfolio — production wired a Connection-backed webhook. Do not
  reintroduce channel names here.

## Not this pattern

- Pattern 27: Offer Tool Cloud SQL → SCD Type 2 ingest (OLTP source).
- Pattern 46: Food Graph refined zone *inside* the DWH.
- Pattern 16 / 04: benchmarking gaps / FBO scoring *exports* to partner
  buses — different consumers; this DAG publishes into product BQ.
