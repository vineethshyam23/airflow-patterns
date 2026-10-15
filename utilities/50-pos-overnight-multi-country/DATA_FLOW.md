# Data flow: Overnight multi-country POS

## Modes

| Mode | How | Date list |
|------|-----|-----------|
| Daily (default) | No backfill Variables | `[yesterday YYYYMMDD]` |
| Backfill | Set `pos_overnight_backfill_start` + `_end` (YYYY-MM-DD) | Inclusive day list |

Optional module constants in `date_range.py` mirror the production
"uncomment two lines" habit for local experiments. Prefer Variables in
Composer so a backfill does not require a deploy.

## Object layout (sanitized)

```
gs://pos-vendor-drop/
├── Vendor-Machine_*YYYYMMDDT*.csv
├── Vendor-Article_*.csv
├── Vendor-Debtor_*.csv
├── Vendor-DebLoc_*.csv
└── transactions_daily/
    ├── netherlands/orders/tickets-netherlands-YYYYMMDD.jsonl
    ├── germany/v2/orders/tickets-germany-YYYYMMDD.jsonl
    ├── france/v2/orders/...
    ├── italy/v2/orders/...
    ├── spain/v2/orders/...
    └── {country}/cm/Tenant_Debtor_YYYYMMDD.jsonl
```

After country dbt succeeds, ticket objects move under
`.../orders/processed/`.

## Step sequence

1. **Master fan-out** — for each core table, resolve today's date-token
   blob and TRUNCATE-load `trusted_staging.<table>_stg`. Missing file =
   soft skip (task still succeeds).
2. **Mapping fan-out** — for each ISO, walk the date list and TRUNCATE
   load mapping JSONL into `trusted.vendor_customer_transaction_mapping_{ISO}`.
   Last successful day in a backfill wins.
3. **Ticket branches (parallel)** — for each ISO: APPEND matching
   JSONL into `trusted_staging.pos_transactions_{ISO}` → country dbt job
   → move objects to `processed/`.
4. **Fan-in** — `end_file_loading` + mapping barrier → customer dbt
   (also fed by master) → matching ids → POS transforms → Tableau →
   materialize `refined.vendor_customer_base`.

## Failure modes

| Symptom | Likely cause | What we do |
|---------|--------------|------------|
| Empty staging, no error | Vendor late drop / wrong date token | Soft skip; Slack only on hard failures |
| Country dbt fails, files still in drop | Load succeeded, transform failed | Do not move; retry leaves objects in place |
| Half countries green | One market missing tickets | Soft skip that day; matching may be thin for that ISO |
| Overlapping runs | Manual trigger during schedule | Blocked by `max_active_runs=1` |
| Backfill archives wrong day | Divergent date helpers | Shared `get_date_range()` for load and move |

## Out of scope in this sample

- Salesforce matching bridge with customer-specific ID remaps (production
  side-branch; not portable without inventing fake IDs)
- OSM geocode enrichment for dashboard countries (inactive / commented
  path in source)
- Full lead/order master schemas (same lander pattern; omitted for length)
