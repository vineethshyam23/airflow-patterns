# Business case: Odoo invoices partner export

Partner analytics needed the *general* Odoo invoice-line fact for
FR and ES — booking date, payment status, product code, EUR net and
local gross — not the wholesale-only WSL intermediate we already
ship for a DE pilot. Upstream dbt lands `refined.odoo_invoices`
(with validity columns). This DAG is the export path onto the
partner event bus.

I kept countries sequential and batches parallel inside each market.
Invoice volume in FR alone is large enough that a single Python
task streaming the whole country slice risks worker OOM and
multi-hour wall time. Five `FARM_FINGERPRINT` partitions on
`(odoo_id, odoo_invoice_id, product_code)` give stable shards that
re-runs can retry independently.

## What this unlocked

- Daily full-country Avro feed without loading an entire FR/ES
  slice into one worker
- Shared OAuth path with throttle-aware retries and a per-batch
  token stagger so five parallel tasks do not 403 the IdP
- Explicit Avro type mapping (DATE → logicalType date, timestamps
  as formatted strings, longs/doubles/booleans) so the registered
  schema stays honest when BQ types drift
- Deferrable dbt Cloud job with a 60-minute hard timeout so a stuck
  refresh does not hold the Composer slot overnight

## Constraints

- Schedule is `30 5 * * *`. The refined dbt job must finish before
  export tasks query. Prefer a Dataset sensor over hoping cron
  order holds if upstream slips.
- `max_active_tasks=3` with `TOTAL_BATCHES=5` is intentional
  production tuning: at most three of the five country batches run
  at once. Raising the pool without staggering tokens will trip
  IdP rate limits; raising batches without raising the pool just
  lengthens the country window.
- Full load, no date filter. The partner treats the subject as a
  daily snapshot. Re-runs re-post the same rows — coordinate before
  a historical replay.
- `end` / `end_{cc}` use `ALL_DONE`. A failed batch does not freeze
  sibling country completion — good for partial delivery, bad if
  "DAG green" is treated as full coverage.
- Sandbox and prod ingest hosts differ in path prefix. Externalize
  the whole ingest base URL so that difference stays out of code.

## What this is not

Not WSL wholesale dual export (pattern 08) — that path owns a
trusted intermediate, a recommender APPEND, and a single DE market.
Not Invoice Radar LPV reconciliation (pattern 32) or the monthly
LPV Adjustment pack (pattern 82). Not the Odoo DWH inbound spine
(pattern 76) — only the partner-facing refined invoice feed.
