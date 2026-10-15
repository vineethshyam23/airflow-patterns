# Pattern 69: Wholesale NL dual-source land (MCC API + CHD CSV)

Daily Composer DAG with three independent chains: partner MCC customer
mutations (OAuth2 paginated JSON → JSON staging → dbt), parallel
merge-request extract, and an optional CHD market CSV land that cleans
dirty integer fields before a strict BigQuery load and a second dbt job.

Distinct from pattern 25 (SEO NDJSON GCS ingest) and pattern 35 (POS
HMAC CSV): mutation-window API + optional landing-zone CSV under one
DAG, with opaque JSON staging for the API arm and schema-strict CSV for
the market arm.

Source (read-only):
- `dags/etl_makro_NL.py`
- `dags/horeca_digital/makro_customers_api.py` (inbound helpers only)

## Files

| File | Role |
|------|------|
| `makro_customers_api.py` | OAuth client, pagination, merge extract, CHD clean/schema |
| `dag_makro_nl_dual_source.py` | Three-chain DAG + deferrable dbt stubs |
| `BUSINESS_CASE.md` | Why dual-source under one schedule |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Steps and failure modes |

## Quick start

```bash
python -c "import ast; ast.parse(open('makro_customers_api.py').read())"
python -c "import ast; ast.parse(open('dag_makro_nl_dual_source.py').read())"
```

Needs Airflow Variables for MCC OAuth (`wholesale_nl_mcc_*`,
`mcc_oauth_*`), customer / merge URLs, `composer_bucket`,
`chd_landing_bucket`, dbt job ids, and schema JSON
`schema_json/wholesale_customer_merge_requests.json` in rawzone. This
folder is a sanitized reference, not a deploy package.

## Sanitization notes

- GCP projects `hd-dwh-stream-1` / `-dev` → `dwh_project` / `dwh_project_dev`
- Buckets `hd-digital-dp-rawzone` / landing CHD → `rawzone` /
  `landingzone-chd` (Variables)
- Dataset `dwh_trusted_staging` → `trusted_staging`
- Brand / partner names Makro → wholesale NL; file prefix `MAKRO_NL` →
  `WHOLESALE_NL`
- Variable names `makroNL_hubspot_*` → `wholesale_nl_mcc_*` /
  `wholesale_nl_customer_*`
- Hardcoded dbt job ids → Variables
- Owner / emails → `data-platform` / `dataops@example.com`
- Package import `horeca_digital.makro_customers_api` → local module
- HubSpot reverse-export functions omitted (outbound sibling pattern)
- Removed emoji / checkmark log noise from CHD cleaner
- Raised `dagrun_timeout` 20m → 60m to match dbt execution timeout
- Added `max_active_runs=1`, request timeouts, structured logging
- CHD vendor column names kept (load contract)

## Category

`utilities/69-makro-nl-dual-source-land/`
