# Pattern 29: Vonage Contact Center daily stats ingest

Daily Composer pipeline that pulls five Contact Center stats grains
from the Vonage (NewVoiceMedia) API via OAuth2 client-credentials,
lands NDJSON under a date-keyed rawzone path, loads truncate into
BigQuery staging as a single JSON column, runs a dbt Cloud job behind
a fan-in barrier, then posts API-vs-refined row counts to Slack.

Distinct from pattern 19 (Freshdesk REST) and pattern 28 (AppFigures
CSV). This is a multi-endpoint contact-center stats pull with
pagination, token refresh mid-page, and opaque JSON landing so vendor
schema drift does not break the load.

Source (read-only):
- `dags/etl_vonage_dbt.py`
- `dags/horeca_digital/get_vonage_data.py`

## Files

| File | Role |
|------|------|
| `fetch_vonage_data.py` | OAuth + paginated fetch + NDJSON write + refined count |
| `dag_vonage_pipeline.py` | Five parallel chains + dbt barrier + Slack status |
| `BUSINESS_CASE.md` | Why contact-center stats land this way |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Grains, dates, failure modes |

## Quick start

```bash
python -c "import ast; ast.parse(open('fetch_vonage_data.py').read())"
python -c "import ast; ast.parse(open('dag_vonage_pipeline.py').read())"
```

Needs Airflow Variables `env`, `composer_bucket`, `vonage_creds`
(JSON), and optionally `vonage_dbt_job_id`, `vonage_slack_conn_id`,
`vonage_slack_channel`. This folder is a sanitized reference, not a
deploy package.

## Sanitization notes

- GCP projects `hd-dwh-stream-*` → `dwh_project` / `dwh_project_dev`
- Buckets `hd-digital-dp-*-rawzone` → `rawzone` / `rawzone_dev`
- Datasets `dwh_trusted_staging` / `dwh_refined_sales` →
  `trusted_staging` / `refined_sales`
- Hardcoded OAuth client_id / client_secret in `__main__` removed →
  Variable `vonage_creds`
- Hardcoded dbt job id → Variable `vonage_dbt_job_id`
  (EmptyOperator when unset)
- Owner / emails → `data-platform` / `dataops@example.com`
- Slack channel / conn → Variables; message emojis stripped
- Package import `horeca_digital.get_vonage_data` → local module
- Custom `DbtCloudRunJobOperator` import → providers stub with fallback
- Endpoint lookup fails closed on unknown `file_name` (production
  could unbound `endpoint`)

## Category

`utilities/29-vonage-contact-center-ingest/`
