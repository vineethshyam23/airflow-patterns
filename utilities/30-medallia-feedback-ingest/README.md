# Pattern 30: Medallia survey feedback SCD Type 2 ingest

Daily Composer pipeline that pulls NPS / churn / downgrade survey
feedback from the Medallia GraphQL Query API via OAuth2
client-credentials, lands a headerless CSV under rawzone, truncate-
loads BigQuery staging, then applies inline SCD Type 2
(hash insert + close obsolete rows inside a 366-day window) before
promoting a tmp snapshot back to trusted.

Distinct from pattern 19 (Freshdesk REST tickets), pattern 27
(Offer Tool Cloud SQL SCD via dbt-shaped hashes), and pattern 29
(Vonage stats NDJSON + dbt). This is GraphQL pagination + CSV
landing + operator-owned Type 2.

Source (read-only):
- `dags/etl_medallia.py`
- `dags/horeca_digital/medallia.py`

## Files

| File | Role |
|------|------|
| `extract_medallia.py` | OAuth + GraphQL pagination + MD5 hashes + GCS CSV |
| `dag_medallia_pipeline.py` | Six-task sequential SCD2 DAG |
| `BUSINESS_CASE.md` | Why survey history lands this way |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Window, steps, failure modes |

## Quick start

```bash
python -c "import ast; ast.parse(open('extract_medallia.py').read())"
python -c "import ast; ast.parse(open('dag_medallia_pipeline.py').read())"
```

Needs Airflow Variables `medallia_creds` (JSON), optionally
`medallia_rawzone_bucket`, `medallia_gcp_project`,
`medallia_gcp_conn_id`, `medallia_gcs_conn_id`, plus a Composer
schema object at `schema_json/medallia_feedback_record.json`.
This folder is a sanitized reference, not a deploy package.

## Sanitization notes

- GCP project `hd-dwh-stream-1` → Variable `medallia_gcp_project`
  (default `dwh_project`)
- Bucket `hd-digital-dp-rawzone` → Variable `medallia_rawzone_bucket`
  (default `rawzone`)
- Datasets `dwh_trusted` / `dwh_trusted_staging` →
  `trusted` / `trusted_staging`
- Tenant Medallia hosts → `apis.example-medallia.com` /
  `example.medallia.eu` (override via env)
- Hardcoded OAuth client_id + keys JSON path → Variable
  `medallia_creds`
- Vendor field ids (`e_hd_*`, numeric custom ids) anonymized;
  warehouse column names kept
- Owner / emails → `data-platform` / `dataops@example.com`
- Package import `horeca_digital.medallia` → local `extract_medallia`
- Removed dead `initialize_storage_client` / keyfile logging path;
  extract uses ADC `storage.Client`
- Fixed missing comma after
  `english_translation_promoter_reason_comment` in the insert SELECT
- Product brand names in comments generalized to "platform"

## Category

`utilities/30-medallia-feedback-ingest/`
