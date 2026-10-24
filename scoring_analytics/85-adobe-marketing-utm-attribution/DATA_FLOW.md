# Data flow: Adobe marketing UTM attribution

## Run order

1. Hourly `etl_aa_adobe_rawfeed_hourly` (pattern 49) lands hit data
   into `refined.adobe_datafeed` throughout the day.
2. At 06:00 UTC, Composer starts `adobe_marketing_utm`.
3. One `BigQueryInsertJobOperator` runs the INSERT…SELECT for
   `visit_start_time_gmt_dt = CURRENT_DATE()` and the configured
   website product type.
4. Rows append into `refined.adobe_marketing_utm`.

No sensor, no staging file, no dbt step in this DAG.

## Projection path

```
refined.adobe_datafeed (hit grain)
  → filter: visit date + product type
  → derive: visitor_id, funnel flags, referral pivots
  → pass-through: UTM eVars, page fields, visit_page_num
  → INSERT INTO refined.adobe_marketing_utm
```

## Funnel and channel rules

| Signal | Condition |
|--------|-----------|
| Pricing page | `page_url` contains `"pricing"` |
| Request demo | `var_event_name = button_click` AND value `Request Demo` |
| Form submit | traffic page contains `"pricing"` AND `form_submit_success` / `Submit` |
| Internal | `referrer_type_id = 1` |
| External sites | `referrer_type_id = 2` |
| Search | `referrer_type_id = 3` |
| Direct / bookmark | `referrer_type_id = 6` |
| Social | `referrer_type_id = 9` |

Flags store the `visit_id` on match and NULL otherwise so
`COUNT(DISTINCT col)` yields visit counts without a second grain.

## Idempotency

- Bare append. Same calendar day re-run duplicates every selected hit.
- Recovery: delete the affected `visit_start_time_gmt_dt` from the
  target, then re-run (or change the date expression and run once).
- No watermark / MERGE / partition replace in production source.

## Failure modes

| Failure | Effect | What to do |
|---------|--------|------------|
| Hourly land late at 06:00 | Empty insert; green DAG | Re-run after partition exists; add sensor |
| Duplicate re-run | Inflated visit counts | Delete date partition; re-run once |
| BQ flake | Task fails immediately | Manual clear / re-run; raise `retries` |
| Wrong product Variable | Empty or wrong suite | Check `adobe_utm_product_type` |
| CURRENT_DATE vs logical date | Backfill misses intended day | Override date expression or edit SQL |

## Scale notes

Single partitioned scan of today's hits for one product type —
seconds, not minutes. Parallelism is unnecessary. If marketing later
asks for multi-suite attribution, fan out product types as TaskGroup
members with shared SQL builder; do not widen the WHERE to all
products in one scan without checking slot cost on the hit table.
