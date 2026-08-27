# Data flow: Single-market Order + Reservation export

## Schedule

| Setting | Value |
|---------|-------|
| Cron | `0 6 1 * *` (1st of month, 06:00 UTC) |
| catchup | False |
| max_active_runs | 1 |
| max_active_tasks | 1 |
| Export execution_timeout | 4 hours per product task |
| dbt timeout | 600s |
| Chunk size | 500 Avro records |

## End-to-end path

1. Composer triggers on the 1st.
2. `DbtCloudRunJobOperator` runs the tagged job that rebuilds
   `refined.market_dish_orders` and `refined.market_dish_reservations`
   (Order/Reservation facts + latest subscription asset attrs).
3. For each country in `MarketDishOrders.countries` (today: `pl`):
   - Orders task: SELECT → Avro (`market_dish_orders`) → POST chunks
   - Reservations task: SELECT → Avro (`market_dish_reservations`) →
     POST chunks
4. Both tasks feed `end` with `ALL_DONE`.

## Grain

| Feed | Grain | Notable fields |
|------|-------|----------------|
| Orders | One row per order | order_id, order_number, order_date, order_status, order_price + establishment / wholesale / owner / latest asset |
| Reservations | One row per reservation | reservation_id, reservation_created_date + same establishment / wholesale / owner / asset envelope |

Timestamps are formatted as strings (`%Y-%m-%d %H:%M:%S`) at SELECT
time so Avro transport stays string-typed for both feeds.

## Failure modes

| Failure | Effect | Ops note |
|---------|--------|----------|
| dbt job timeout / fail | Neither export runs | Fix refine first; do not manually POST stale tables |
| Orders export fails | Reservations may still complete (`ALL_DONE`) | Re-run Orders task only after checking partner partial ingest |
| Reservations export fails | Symmetric to above | Same |
| 401 mid-chunk | Token refresh, same payload retried | Expected on long full-loads |
| Transient HTTP / JSON decode | Backoff up to 10 attempts, then raise | Partner rate limits show up here |
| Empty refined table | Zero chunks, DONE with 0 rows | Still a successful ship — confirm dbt actually ran |

## Country list

Single market today. Extending `countries` requires matching refined
partitions / filters and registered schema ids per market — do not
blindly loop a second ISO code against PL-only tables.
