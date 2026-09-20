# Pattern 50: Overnight multi-country POS land + backfill

Composer DAG that lands the full POS vendor overnight drop at 07:00
Europe/Amsterdam: master CSVs (machines, articles, debtors, locations),
five-country ticket JSONL into a single JSON staging column, tenant–
debtor mapping dumps, then the shared customer / matching / POS / Tableau
dbt chain and a materialized customer-base table.

Supports a closed date-range backfill via Airflow Variables without a
second DAG — same load/move callables walk each day in one run.

Distinct from pattern 43 (midday debtor + location only) and pattern 35
(HMAC store-details API): this is the overnight spine those jobs sit on.

Source (read-only):
- `dags/etl_dish_pos.py`

## Files

| File | Role |
|------|------|
| `date_range.py` | Daily vs backfill date list (YYYYMMDD) |
| `vendor_master_load.py` | Master CSV schemas, prefixes, staging TRUNCATE, SCD hash SQL |
| `country_transactions.py` | Ticket APPEND, processed move, mapping TRUNCATE |
| `dag_pos_overnight.py` | Timetable, fan-outs, dbt chain, materialize, Slack |
| `BUSINESS_CASE.md` | Why overnight is not just "load everything" |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Modes, object layout, failure modes |

## Quick start

```bash
python -c "import ast; ast.parse(open('date_range.py').read())"
python -c "import ast; ast.parse(open('vendor_master_load.py').read())"
python -c "import ast; ast.parse(open('country_transactions.py').read())"
python -c "import ast; ast.parse(open('dag_pos_overnight.py').read())"
```

Needs Composer Variables for project / drop bucket / datasets, optional
dbt Cloud job ids (including per-country JSON map), and Slack webhook.
This folder is a sanitized reference, not a deploy package.

## Sanitization notes

- GCP project `hd-dwh-stream-1` → Variable `dwh_project`
- Bucket `hd-dwh-toaster-dev-pos-data` → Variable `pos_vendor_drop_bucket`
- Datasets `dwh_trusted*` / `dwh_refined` → `trusted` / `trusted_staging` / `refined`
- Tables `booq_*` / `dish_pos_transactions_*` → `vendor_*` / `pos_transactions_*`
- Object prefixes `Eijsink-*` → `Vendor-*`
- Hardcoded dbt job ids → Variables `pos_overnight_dbt_*` + country JSON map
- Emails / Slack / owners → `dataops@example.com` / `#pos-data-ops` / `data-platform`
- Backfill toggle via NameError constants → Variables (optional module override kept)
- Dropped Salesforce matching side-branch with customer-specific UUID remaps
- Dropped geocode enrichment branch (mostly commented / inactive in source)
- Dropped dbt run-id Variable scrape and emoji-heavy status Slack
- Package import for helpers (was inline in the monolithic source DAG)
- Master schemas trimmed to SCD-relevant cores; lead/order dumps noted in docs

## Distinct from nearby patterns

| | 35 (HMAC store) | 43 (afternoon) | 50 (this) |
|---|---|---|---|
| Cadence | Daily API pull | 13:00 local | 07:00 local |
| Scope | Store-details CSV | Debtor + location only | Full master + 5-country tickets |
| Hard part | HMAC + dual CSV repair | Selective re-land without full POS | Date-range backfill in one run + fan-out |

## Category

`utilities/50-pos-overnight-multi-country/`
