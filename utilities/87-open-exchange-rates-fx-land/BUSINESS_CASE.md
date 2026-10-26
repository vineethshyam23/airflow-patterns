# Business case: Open Exchange Rates FX land

Finance and platform reporting needed a daily FX table with both
USD-base rates (what the vendor returns) and EUR-relative factors
(what European P&L views actually join on). Buying a second paid FX
feed, or pushing every currency conversion into BI, was the wrong
trade for ~170 currencies and one snapshot a day.

I kept the land in Composer: one API call, nested JSON staging so we
do not explode the rates map in Python, and a short BigQuery INSERT
that UNNESTs, cross-joins EUR, and appends only unseen API timestamps.
That gave finance a durable `exchange_rates` table without a dbt job
slot on the critical path and without rewriting rates as flat CSVs.

## What this unlocked

- One trusted table serves both USD and EUR consumers — `to_usd` /
  `to_eur` / inverse columns are computed once at land time
- Timestamp dedupe makes the DAG safe to re-run after a partial
  failure; staging is truncate, trusted only grows on new snapshots
- Nested JSON preserves the vendor shape (`rates` map) until SQL —
  fewer Python transforms to drift when OEX adds currencies
- Small payload (~one request, ~170 rows) stays in the 05:00 UTC
  finance window next to other lightweight lands

## Constraints

- OEX free/standard plans are USD-base. EUR factors assume EUR is
  present in the response (it always has been for this feed). If EUR
  is missing, the CROSS JOIN yields zero rows with no explicit alert.
- Production extract only wrote the Composer file on HTTP 200 and did
  not raise — a bad key looked like a green DAG with stale trusted
  data. The sanitized extract fails the task on non-200.
- Production named the rawzone object with `date.today()` at DAG parse
  time. That breaks catchup/backfill; this folder uses `{{ ds }}`.
- Append-only trusted has no SCD close. A corrected re-publish for the
  same timestamp is skipped — delete the timestamp row first if you
  need a reload.
- No DEV/PROD branching in the original DAG (hardcoded prod project /
  bucket). Variables cover that here; keep conn ids env-specific.

## What this is not

Not AppFigures weekly CSV API ingest (pattern 28) — that fans out
report grains and hands off to dbt. Not finance-kit multi-entity CSV
hash-delta (pattern 86) — that lands vendor drops with pre-hashed
rows. Not Invoice Radar / LPV reporting (patterns 32 / 82) — those
reconcile invoices, they do not maintain FX. This folder is the
*daily FX API nested land with SQL-side EUR/USD factors*.
