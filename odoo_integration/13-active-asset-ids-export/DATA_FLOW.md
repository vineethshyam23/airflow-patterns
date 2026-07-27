# Data flow: weekly active asset ID snapshot

Schedule: `0 8 * * 0` (Sundays 08:00 UTC). Catchup off — a missed week
is an explicit re-trigger of the full snapshot, not a catchup flood.

## Stage A — Upstream cleanup (external)

Weekly `sale_order_line` cleanup invalidates / removes deleted lines
from `refined_sales.odoo_sale_order_line` before this DAG reads it.
Source coordinated by schedule (cleanup earlier Sunday / Saturday);
no ExternalTaskSensor in the original DAG. If cleanup slips, this
export ships stale "still active" IDs and the partner will not drop
them.

## Stage B — Parallel Avro ingest (13 countries)

| Task pattern | Source | Filter |
|--------------|--------|--------|
| `ingest_active_asset_ids_{CC}` | `refined_sales.odoo_sale_order_line` (+ order, country, partner) | `UPPER(rc.code) = {CC}` |

Markets: CZ, DE, ES, FR, HR, HU, IT, NL, PL, PT, RO, SK, TR.

Each task: SELECT DISTINCT → Avro encode → POST chunks of 500 to
`/ingestbulk/{country}/{schema_id}`.

Columns shipped:

| Column | Role |
|--------|------|
| `sale_order_line_id` | Join key with asset lifecycle |
| `sale_order_id` | Parent order |
| `establishment_id` | Partner UUID (nullable) |
| `_ldts` | Snapshot date (`CURRENT_DATE()`) |

## Idempotency and re-runs

- Re-run: re-posts the full active set for every country (or the
  failed ones if you clear + re-trigger selectively). Coordinate with
  the consumer — bus ingest is additive.
- Partial country failure: siblings keep running; `end` still fires
  (`ALL_DONE`). Re-run only the failed country tasks.
- Never run this before cleanup has finished for the week. You will
  certify deleted lines as active.

## Failure modes worth knowing

- OAuth 401 mid-chunk: client clears token and retries the POST once.
- Empty country result: unusual for established markets — check
  refined sales freshness and country code joins before assuming
  "zero active lines."
- Execution timeout 60 min/country: large markets + cold BQ slots.
- `ALL_DONE` on end: DAG can show success-ish while one market is
  red. Alert on task failures, not only DAG success.
- No post-export row-count assert against BQ. Log chunk totals and
  trend them if deletion detection starts drifting.
- Memory: full country result buffered before chunking.
