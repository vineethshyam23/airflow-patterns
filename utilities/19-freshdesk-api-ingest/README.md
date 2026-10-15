# Pattern 19: Freshdesk REST API ingest

Hourly ticket landing plus monthly dimension refresh from Freshdesk
REST into GCS / BigQuery staging, then dbt Cloud. One DAG, two
cadences via BranchPythonOperator.

Distinct from pattern 15 (Odoo Postgres helpdesk pull) and pattern 06
(refined ticket → event bus). This is the SaaS helpdesk API → warehouse
path.

Source (read-only):
- `dags/horeca_digital/freshdesk_extract.py`
- `dags/horeca_digital/archived/etl_freshdesk_import.py`

## Files

| File | Role |
|------|------|
| `freshdesk_client.py` | Paginated REST → NDJSON; ticket `updated_since` |
| `dag_freshdesk_api_ingest.py` | Branch hourly/monthly → GCS → BQ → dbt |
| `BUSINESS_CASE.md` | Why SaaS helpdesk landing sits beside Odoo |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Stages, branch rules, failure modes |

## Quick start

```bash
python -c "import ast; ast.parse(open('freshdesk_client.py').read())"
python -c "import ast; ast.parse(open('dag_freshdesk_api_ingest.py').read())"
```

Needs Airflow Variables `freshdesk_apikey`, `freshdesk_domain`,
`composer_bucket`, `dbt_freshdesk_tickets_job_id`,
`dbt_freshdesk_dims_job_id`, plus `schema_json/freshdesk_*.json` in
the raw bucket. This folder is a sanitized reference, not a deploy.

## Sanitization notes

- GCP projects `hd-dwh-stream-*` → `dwh_project` / `dwh_project_dev`
- Buckets `hd-digital-dp-*-rawzone` → `data-platform-*-rawzone`
- Dataset `dwh_trusted_staging` → `staging`
- Freshdesk subdomain hard-code → Variable `freshdesk_domain`
  (default `helpdesk-tenant`)
- Hard-coded dbt job IDs → Variables
  `dbt_freshdesk_tickets_job_id` / `dbt_freshdesk_dims_job_id`
- Notification emails → `dataops@example.com`
- Owner / author names removed
- Commented `__main__` block with a real API key removed entirely
- Module-level `FreshDesk(...)` at import → per-task factory
  (`_make_fetch`) so DAG parse does not read the API key
- Package import `horeca_digital.freshdesk_extract` → local module
- `EmptyOperator` with DummyOperator fallback
- Client re-raises after rate-limit sleep so Airflow retries work

## Distinct from patterns 06 / 15

| | Pattern 06 | Pattern 15 | Pattern 19 |
|---|------------|------------|------------|
| Direction | Warehouse → event bus | Odoo Postgres → warehouse | Freshdesk API → warehouse |
| Source | refined BQ tickets | Odoo Postgres | Freshdesk REST |
| Payload | Avro | NDJSON → BQ | NDJSON → BQ |
| Cadence | Daily | On-demand | Hourly + monthly branch |

## Category

`utilities/19-freshdesk-api-ingest/`
