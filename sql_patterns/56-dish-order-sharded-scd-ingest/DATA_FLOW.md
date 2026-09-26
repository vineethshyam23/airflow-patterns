# Data flow: food-ordering sharded SCD ingest

## Schedule

- Cron: `30 0 * * *` (00:30 UTC)
- `max_active_runs=1`, `catchup=False`
- One retry / 3-minute delay on tasks
- Cleanup runs with `ALL_DONE` so scratch CSVs do not accumulate
  after a partial failure

## Run order

1. **getdbs** — export tenant→shard map from master →
   `{DATA_ROOT}/foodorder_dbs.csv`
2. **getdbs_{instance}** (×N, parallel) — grep map by shard IP →
   `foodorder_dbs_{instance}.csv`
3. **export_foodorder_shard_{instance}** (×N, parallel) — for each
   tenant DB on that shard, `gcloud sql export` every sharded table
   into the export bucket under `_shards/{ds}/{instance}/…`
4. **export_foodorder_master** — wait for all shard exports, then
   export the four master tables into `{export_bucket}/{ds}/`
5. **mergefiles** — concat shard fragments →
   `{raw_bucket}/foodorder/{table}/{ds}/{table}.csv`
6. **Per table (parallel after merge)**
   - Master: `GCSToGCS` export bucket → raw zone
   - Sharded: `EmptyOperator` (merge already wrote the object)
   - Snapshot `trusted.order_{t}` → `trusted_staging.tmp_order_{t}`
   - WRITE_TRUNCATE load CSV → `trusted_staging.order_{t}`
   - INSERT staging rows whose `concat(_keyhash,_rowhash)` is new
     among currently valid `FoodOrder` rows
   - UPDATE tmp: expire valid rows missing from today’s staging
   - WRITE_TRUNCATE promote tmp → `trusted.order_{t}`
7. **wait_for_files** (`ALL_DONE` barrier) → **clean_files**

## Object layout

```
gs://db-export-food-order-prod/
  {ds}/countries.csv                    # master
  _shards/{ds}/{instance}/{db}/orders.csv

gs://dwh-rawzone/
  foodorder/orders/{ds}/orders.csv      # merged
  foodorder/countries/{ds}/countries.csv
  schema_json/order_orders.json
```

## Hash + SCD contract

Export SELECTs (in the bash scripts) add:

| Column | Meaning |
|--------|---------|
| `_keyhash` | MD5 of primary key |
| `_rowhash` | MD5 of business columns |
| `_create_ts` | export timestamp |
| `_job_name` | DAG id |
| `_sourcesystem` | `FoodOrder` |

Trusted SCD columns maintained in BigQuery:

| Column | Meaning |
|--------|---------|
| `_valid_from` | hour-floored load timestamp |
| `_valid_until` | `2099-12-31` while current; else prior hour − 1s |
| `_valid_flag` | True while current |
| `_update_ts` | set on expire |

## Failure modes

| Failure | Effect | Recovery |
|---------|--------|----------|
| One shard export 409 / timeout | Blocks master export (ALL_SUCCESS) | Clear + retry that shard task; merge waits |
| Merge missing a fragment | Partial CSV in raw zone | Re-run merge after shard retry; do not start loads mid-merge |
| Staging load jagged rows | Fails at `max_bad_records` (300 for `payment_logs`) | Fix export SELECT / schema JSON; clear load chain |
| SCD insert/update slot pressure | Query job queues / fails | Reservation wrapper should absorb night wave; check reservation capacity |
| Task requeue across midnight | `date.today()` load path may miss merge object | Prefer `{{ ds }}` rewrite; or clear from merge onward on the original logical date |

## What was deliberately left out

- Downstream triggers into refined Salesforce DAGs (commented out in
  production source)
- Freshness `BigQueryCheckOperator` on `order_orders` (also commented;
  worth re-enabling when monitoring is wired)
- Full 44-shard instance map and full 38-table list — see
  `shard_config.py` for the trimmed sample
