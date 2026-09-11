# Pattern 41: Collections partner multi-market case ingest

Composer DAG that pulls collections case files for AT/DE/FR/ES/IT,
lands NDJSON on GCS, loads typed BigQuery staging, and optionally
triggers dbt Cloud. Per-market TaskGroups, Secret Manager keys, and
ShortCircuit gates keep empty markets from blocking the run.

Distinct from pattern 11 (outbound KYC Avro export) and pattern 37
(payment wallet API land). This pattern is partner collections case
files into the DWH.

Source (read-only):
- `dags/etl_pair_finance_cases_daily.py`
- `dags/horeca_digital/pair_finance_api.py`
- `dags/horeca_digital/pair_finance_pipeline.py`

## Files

| File | Role |
|------|------|
| `dag_pair_finance_cases.py` | Multi-market TaskGroups, staging load, dbt ShortCircuit |
| `pair_finance_api.py` | Bearer client, pagination, flatten, NDJSON |
| `pair_finance_pipeline.py` | Secrets, GCS land, extract/load/has_records |
| `BUSINESS_CASE.md` | Why one DAG owns five markets |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Daily vs full-load paths, failure modes |

## Quick start

```bash
python -c "import ast; ast.parse(open('pair_finance_api.py').read())"
python -c "import ast; ast.parse(open('pair_finance_pipeline.py').read())"
python -c "import ast; ast.parse(open('dag_pair_finance_cases.py').read())"
```

To run for real you need Secret Manager keys (or Variable JSON),
Composer bucket Variable, BigQuery connections for DEV/PROD, and an
optional dbt Cloud job id. This folder is a sanitized reference, not
a deploy.

## Sanitization notes

- GCP projects `hd-dwh-stream-*` → `dwh_project` / `dwh_project_dev`
- Hostnames `*.pairfinance.com` → `*.collections.example.com`
- Merchant `Dish_*` → `partner_{market}`
- Secret ids `pair-finance-*-api-key` → `collections-{market}-api-key`
- Staging dataset `dwh_trusted_staging` → `trusted_staging`
- Emails → `dataops@example.com`
- Owner → `data-platform`
- Ticket IDs and personal names removed
- Package imports `horeca_digital.*` → local modules
- `dbt_poll_interval` inlined (no utils import)
- Default dbt job id → empty Variable (ShortCircuit)
- Composer bucket default → `composer-data`
- Cross-file names kept consistent:
  `MARKET_CONFIG`, `list_cases`, `get_case`, `flatten_case`,
  `cases_to_ndjson`, `normalize_market`, `STAGING_SCHEMA_FIELDS`,
  `gcs_object_name`, `extract_cases`, `load_gcs`, `has_records`,
  `raw_bucket_name`, `resolve_env`

## Category

`payment_processing/41-pair-finance-cases-ingest/`
