# Data flow: Ranked menu-gaps export to partner event bus

Schedule: `15 6 1 * *` (monthly, 1st of month 06:15 UTC). Catchup
off — a missed month is an explicit re-trigger, not a storm of
additive posts.

## Stage A — dbt refresh

`dbt_menu_gaps_ranked_refresh` runs the Cloud job that materializes
`refined.menu_gaps_ranked_{cc}` for each active market. Timeout 1200s
— these models are heavier than the KYC/list-price siblings.

If dbt fails, no country export starts. Shipping yesterday's refined
table as "this month's gaps" is worse than a delayed partner feed.

## Stage B — sequential countries, parallel batches

| Scope | Behaviour |
|-------|-----------|
| Countries | `de → fr → nl → es → pl → hr → it → pt` (sequential) |
| Batches / country | 5 parallel tasks (`export_{cc}_batch_0..4`) |
| Partition SQL | `MOD(ABS(FARM_FINGERPRINT(CONCAT(establishment_id,'-',article_no))), 5) = batch` |
| Default filter | D-1: `_update_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)` |
| Chunk size | 2000 Avro records per POST |

Each batch task: stream BQ → Avro encode → POST chunks.

Exported fields (sanitized names): wholesale_id, iso_code,
establishment_id, ingredient, type, menu_type, menu_item_name,
relevance, branch_desc, article_no, variant_tu_key, department_flag,
product_key, article_name, one_year_revenue, rank_, account_id,
person_id, cardholder_key, customer_key, unique_wholesale_id,
_update_ts.

## Idempotency and re-runs

- Re-run after dbt success: re-posts the D-1 slice for each partition.
  Safe if the bus upserts on natural keys; coordinate otherwise.
- Clear / re-run one `export_{cc}_batch_N` task: only that hash slice
  re-posts. Other batches and countries are untouched.
- Full historical backfill: call `send_ranged_batch(..., full_load=True)`
  (or expose a DAG Param). Default path will not ship a full month of
  mid-cycle updates under a D-1 filter.
- Never put ingest before dbt. You will ship a stale refined snapshot.

## Failure modes worth knowing

- OAuth 401 mid-chunk: client clears token and retries the POST with
  the same body.
- Empty batch: often a quiet market day under D-1, not a broken OAuth
  path — check refined `_update_ts` distribution before paging the
  event API.
- dbt timeout (1200s): usually model contention or a bad foodgraph
  upstream, not Avro encoding. Do not bump timeout without reading the
  Cloud job graph.
- HTTP 4xx/5xx on ingest: raises (`raise_for_status`) after retries.
- `ALL_DONE` on country/end markers: later markets still run after a
  failed batch. Treat failed-task count as the delivery signal.
- Hash key change: if someone alters the CONCAT inputs, rows move
  between batches and re-runs no longer map 1:1 to prior partitions.
