# Pattern 23: Multi-country platform-customer footprint export

Daily build of matched wholesale customers' product footprint (bundle
tier, Reservation, Website, Order, POS, Pay, lifecycle timestamps)
across 14 countries. Parallel BigQuery inserts land in one staging
table; dbt refreshes the refined valid-flag snapshot; parallel Avro
bulk ingest posts today's rows to the partner event bus.

Distinct from pattern 11 (payment KYC, pilot market), pattern 17
(monthly market-listing dump), pattern 04 (FBO/NBO scores), and the
Deepideas hash-delta family (20–22). This feed answers "which products
does this matched wholesale customer run today?" — not KYC status,
not listing enrichment, not gap scoring.

Source (read-only):
- `dags/etl_dana_DISH_customer_data_export.py`
- `dags/horeca_digital/dana_DISH_customer_export.py`
- `dags/horeca_digital/dana_DISH_customer_query.py`

## Files

| File | Role |
|------|------|
| `customer_query.py` | Per-country insert SQL + today's send SELECT |
| `customer_export.py` | OAuth + Avro encode + chunked POST |
| `dag_dish_customer_export.py` | Daily Composer wiring (insert → dbt → ingest) |
| `BUSINESS_CASE.md` | Why footprint ≠ KYC ≠ market listing ≠ scores |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Stages, matching rules, failure modes |

## Quick start

```bash
python -c "import ast; ast.parse(open('customer_query.py').read())"
python -c "import ast; ast.parse(open('customer_export.py').read())"
python -c "import ast; ast.parse(open('dag_dish_customer_export.py').read())"
python customer_query.py
python customer_export.py
```

Needs wholesale / CRM / match / product-spot upstream tables, Composer
BigQuery connection `bigquery_default`, dbt Cloud Variable
`dbt_job_platform_customer_export`, and event-API OAuth Variables plus
a registered schema id. This folder is a sanitized reference, not a
deploy.

## Sanitization notes

- GCP projects `hd-dwh-stream-*` → `dwh_project` / `dwh_project_dev`
- Datasets `dwh_refined` / `dwh_trusted_staging` / `dwh_trusted_mcc` →
  `refined` / `staging` / `trusted_wholesale`
- `metro_id` / `metro_account_id` → `wholesale_id` / `wholesale_account_id`
- Product brand prefixes (`DISH_*`, `MTO_*`) → `platform_*` / `SUB_*`
- Event API host / schema ids / OAuth Variable names generalized
- Real notification emails → `dataops@example.com`
- Owner / author names removed; commented credentials stripped
- Package imports `horeca_digital.*` → local modules
- Avro schema parsed once per send; `query.result()` once
- `max_active_runs` set on the DAG constructor (was in default_args)
- dbt job id → Variable `dbt_job_platform_customer_export`

## Distinct from patterns 04 / 11 / 17 / 20–22

| | 04 | 11 | 17 | 20–22 | 23 (this) |
|---|----|----|----|-------|-----------|
| Question | FBO/NBO scores | Payment KYC status | Market listing attrs | Gap / establishment deltas | Product footprint of matched customers |
| Cadence | Export batches | Daily dbt → ingest | Monthly full load | Weekly hash-delta | Daily staging → dbt → ingest |
| Grain | Score rows | Establishment KYC | Listing per country | Hash-keyed gap rows | wholesale_id × country footprint |
| Markets | Multi | Pilot (PL) | Multi sequential | Per-feed | 14 parallel |

## Category

`scoring_analytics/23-dish-customer-export/`
