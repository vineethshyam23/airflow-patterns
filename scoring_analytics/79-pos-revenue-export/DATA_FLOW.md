# Data flow: Platform POS revenue potential export

## Run order

1. Upstream `pos_potential` / SAM dbt job finishes around 06:00 UTC
   on the 1st (not in this folder).
2. At 06:30 UTC, Composer starts `etl_pos_revenue_export`.
3. Countries run in series:
   - `export_pos_revenue_DE`
   - `export_pos_revenue_FR`
   - `export_pos_revenue_ES`
   - `export_pos_revenue_IT`
   (4h timeout each)
4. Final `end` (`ALL_DONE`).

## Per-country path

```
BQ SELECT (@country_code)
  → row iterator
  → Avro binary encode (schema parsed once)
  → base64 value records
  → POST chunks of 500 to /ingestbulk/{cc}/{schema_id}
```

Dates ship as strings. Numeric fields keep Avro long / double —
do not string-cast everything or the partner schema registry
rejects the payload.

## SQL population rules

| Rule | Effect |
|------|--------|
| POS licence product codes only | Non-POS assets ignored |
| Trailing 12 months from last workday | Window is relative per establishment, not calendar month |
| ≥4 Monday-weeks of coverage | Thin histories excluded |
| Negatives → 0 via GREATEST | Refund-heavy sites do not ship negative potential |
| Annualise if live < 365 days and not Suspended | Matches SAM `annual_sale` |
| QUALIFY unique (cc, customer_no, home_store) | Ambiguous matches drop entirely |
| customer_no / home_store_id NOT NULL | Unmatched establishments never ship |

## Idempotency

- Full monthly load, no watermark. Every 1st-of-month run reships
  the current snapshot for that country.
- Mid-month re-runs of the DAG re-post the same rows — the sink is
  append-oriented; coordinate with the partner before a replay.
- Re-running a failed country task re-ships that country only.

## Failure modes

| Failure | Effect | What to do |
|---------|--------|------------|
| OAuth all grants exhausted | Task fails at startup or mid-chunk | Check Variables / realm / grant type |
| Transient HTTP | Linear backoff up to 10 attempts | Usually self-heals; else check body size |
| 401 mid-load | Token refresh + same chunk retry | If loops, secret rotation race — pause and fix Variables |
| One country fails | Later countries may still run; `ALL_DONE` end | Re-run failed task id; do not assume full coverage from green DAG |
| Upstream dbt late | Stale or empty annualisation inputs | Prefer Dataset / ExternalTaskSensor over cron hope |
| QUALIFY drops rows | Ambiguous wholesale matches silently omitted | Investigate match_result duplicates for that country |
| 413 / payload too large | Chunk POST fails | Lower `CHUNK_SIZE` |

## Scale notes

Customer-grain volume is smaller than establishment × ingredient
feeds, so chunk size 500 matches other monthly Avro partners on
this bus and keeps memory bounded while streaming. If one market
grows past the 4-hour timeout, partition that country with
`FARM_FINGERPRINT` — do not raise `max_active_tasks` across all
four markets at once.
