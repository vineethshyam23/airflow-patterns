# Data flow: Independent-establishment menu-gaps export

Schedule: `30 6 1 * *` (monthly, 1st of month 06:30 UTC). Catchup
off — a missed month is an explicit re-trigger, not a storm of
additive posts. Offset 15 minutes after the ranked sibling so the
two feeds do not contend for Composer slots and ingest quota at once.

## Stage A — assume refined is ready

No dbt task in this DAG. Upstream materializes
`refined.menu_gaps_independent_{cc}` for each active market. If that
table is stale, this export will still run — treat upstream freshness
as a separate SLA, not something this DAG can paper over.

## Stage B — sequential countries, parallel batches

| Scope | Behaviour |
|-------|-----------|
| Countries | Active ISO list (starts with `es`; extend via query module) |
| Batches / country | 5 parallel tasks (`export_{cc}_batch_0..4`) |
| Partition SQL | `MOD(ABS(FARM_FINGERPRINT(CONCAT(establishment_id,'-',IFNULL(menu_item_name,''),'-',IFNULL(ingredient,'')))), 5) = batch` |
| Default filter | D-1: `_update_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)` |
| Chunk size | 1000 Avro records per POST |

Each batch task: stream BQ → Avro encode → POST chunks.

Exported fields: establishment_id, iso_code, establishment_name,
postal_code, city, street_name, street_number, address, geo_lat,
geo_long, google_places_id, phone, email, website,
establishment_type, cuisine_type, menu_type, menu_item_name,
ingredient, created_at, _update_ts.

## Idempotency and re-runs

- Re-run: re-posts the D-1 slice for each partition. Safe if the bus
  upserts on natural keys; coordinate otherwise — and treat contact
  fields as PII during any historical replay.
- Clear / re-run one `export_{cc}_batch_N` task: only that hash slice
  re-posts. Other batches and countries are untouched.
- Full historical backfill: call `send_ranged_batch(..., full_load=True)`
  (or expose a DAG Param). Default path will not ship a full month of
  mid-cycle updates under a D-1 filter.
- Adding a country: extend `ACTIVE_ISO_CODES`, confirm refined table
  exists, confirm schema id is registered for that ISO on the bus.

## Failure modes worth knowing

- OAuth 401 mid-chunk: client clears token and retries the POST with
  the same body.
- Empty batch: often a quiet market day under D-1, not a broken OAuth
  path — check refined `_update_ts` distribution first.
- HTTP 4xx/5xx on ingest: raises (`raise_for_status`) after retries.
- `ALL_DONE` on country/end markers: later markets still run after a
  failed batch. Treat failed-task count as the delivery signal.
- Hash key change: if someone alters the CONCAT inputs, rows move
  between batches and re-runs no longer map 1:1 to prior partitions.
- Stale upstream refined: this DAG has no guard. Pair with a freshness
  sensor if the partner cannot tolerate last-month gaps.
