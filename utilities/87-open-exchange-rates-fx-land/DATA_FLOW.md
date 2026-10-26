# Data flow: Open Exchange Rates FX land

## Run order

1. At 05:00 UTC, Composer starts `etl_oex`.
2. `extract_oex` calls OEX `latest.json` with `app_id` from connection
   password, reshapes rates into one nested JSONL row, writes
   `composer_bucket/data/oex/oex.json`.
3. `copy_to_raw` copies that object to `rawzone/oex/oex_{{ ds }}.json`.
4. `load_staging` loads NDJSON → `trusted_staging.oex_data` with
   `WRITE_TRUNCATE` using `schema_json/oex_data.json`.
5. `insert_trusted` UNNESTs `rates`, CROSS JOINs the EUR row, computes
   EUR/USD factors, and APPENDS rows whose `timestamp` is not already
   in `trusted.exchange_rates`.

## Path contract

| Stage | Path / table |
|-------|----------------|
| Composer FUSE | `data/oex/oex.json` |
| Rawzone | `oex/oex_{{ ds }}.json` |
| Schema | `schema_json/oex_data.json` |
| Staging | `{project}.trusted_staging.oex_data` |
| Trusted | `{project}.trusted.exchange_rates` |

## Staging shape

One row per API response:

```json
{"timestamp": 1698307200, "rates": [{"currency": "EUR", "rate": 0.94}, ...]}
```

## Trusted columns

| Column | Meaning |
|--------|---------|
| `timestamp` | API snapshot (`TIMESTAMP_SECONDS`) |
| `currency` | ISO code |
| `to_eur` | `rate / EUR_rate` |
| `to_usd` | USD-base rate from API |
| `eur_to_currency` | `1 / to_eur` |
| `usd_to_currency` | `1 / rate` |

## Idempotency and recovery

| Situation | Recovery |
|-----------|----------|
| Extract failed | Fix conn / API; clear task and re-run |
| Copy/load failed | Composer file may still exist; re-run from failed task |
| Insert failed after staging truncate | Re-run insert only — staging holds latest snapshot |
| Need to reload a timestamp | DELETE matching rows from `exchange_rates`, then re-run |
| Historical date backfill | Use OEX `/api/historical/{date}.json` (not in this DAG); point extract at that endpoint and set `ds` |

## Failure modes

| Failure | Effect | What to do |
|---------|--------|------------|
| Empty app_id / 401 | Extract red | Rotate key in connection password |
| Silent non-200 (prod behaviour) | Green DAG, stale FX | Prefer raise-on-error extract |
| EUR absent | Zero trusted rows | Alert on insert row count ≈ 0 |
| Parse-time `date.today()` object name | Wrong rawzone path on backfill | Use `{{ ds }}` (this folder) |
| Schema drift | Load fails | Update JSON on load bucket |

## Scale notes

One request, ~170 currencies, four sequential tasks — no slot pressure.
The `NOT IN (SELECT DISTINCT timestamp …)` filter is fine while trusted
is one snapshot per day. If you ever land intraday snapshots, switch
the anti-join to a clustered `(timestamp)` lookup or MERGE on
`(timestamp, currency)`.
