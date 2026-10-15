# Pattern 35: POS vendor store-details HMAC CSV ingest

Daily Composer DAG that authenticates to a POS vendor webservice with
a date-bound HMAC-MD5 signature, lands a repaired establishment CSV,
normalizes column counts in GCS, loads BigQuery staging (TRUNCATE),
and triggers one dbt Cloud job.

Distinct from pattern 03 (Adyen terminals) and pattern 11 (DishPay
KYC): this is establishment master data + product activation flags,
not payment events.

Source (read-only):
- `dags/etl_booq_storedetails.py`
- `dags/horeca_digital/booq_storedetails.py`

## Files

| File | Role |
|------|------|
| `storedetails_api.py` | HMAC sign, header validate, CSV repair, GCS normalize |
| `dag_booq_storedetails.py` | Linear fetch → upload → repair → load → dbt DAG |
| `BUSINESS_CASE.md` | Why daily full-reload master data lands this way |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Steps, auth/path coupling, failure modes |

## Quick start

```bash
python -c "import ast; ast.parse(open('storedetails_api.py').read())"
python -c "import ast; ast.parse(open('dag_booq_storedetails.py').read())"
```

Needs Airflow Variables `vendor_storedetails_hmac_key` (JSON with
`hmac_key`), `composer_bucket`, optionally `vendor_storedetails_endpoint`
/ `booq_storedetails_dbt_job_id`, plus schema JSON
`schema_json/booq_storedetails.json` in the rawzone bucket. This folder
is a sanitized reference, not a deploy package.

## Sanitization notes

- GCP projects `hd-dwh-stream-1` / `-dev` → `dwh_project` / `dwh_project_dev`
- Buckets `hd-digital-dp-rawzone` / Composer → `rawzone` /
  `composer_bucket` Variable
- Dataset `dwh_trusted_staging` → `trusted_staging`
- Variable `mijn_eijsink_key` / `HMACKeyHills` →
  `vendor_storedetails_hmac_key` / `hmac_key`
- Hardcoded vendor host → Variable `vendor_storedetails_endpoint`
  (default `vendor.example.com`)
- Hardcoded dbt job ids → Variable `booq_storedetails_dbt_job_id`
- Owner / emails → `data-platform` / `dataops@example.com`
- Package import `horeca_digital.booq_storedetails` → local
  `storedetails_api`
- GCS prefix `mijn_eijsink_storedetails/` → `vendor_storedetails/`
- Added `max_active_runs=1`; dbt stub when provider / job id missing
- Light hardening: request timeout, `makedirs`, structured logging
- Vendor column names kept (API contract); Dutch labels are the source
  schema, not marketing copy

## Category

`utilities/35-booq-storedetails-hmac-ingest/`
