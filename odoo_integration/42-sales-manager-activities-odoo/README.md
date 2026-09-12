# Pattern 42: Field-sales activities → Odoo CRM

Composer DAG that pulls completed field-sales activities for eight EU
markets, lands NDJSON on GCS, appends BigQuery staging, runs dbt, and
pushes leads to Odoo CRM. Same-day branch skip prevents double API
pulls on accidental re-queue.

Distinct from pattern 02 (Odoo lead-engine class only) and patterns
06–09 (Odoo → event-bus exports). This pattern is the inbound
field-sales API orchestration into CRM.

Source (read-only):
- `dags/etl_sales_manager_activities.py`
- `dags/horeca_digital/sales_manager_activities.py`
- uses `horeca_digital/lead_engine_odoo.py` (covered by pattern 02)

## Files

| File | Role |
|------|------|
| `dag_sales_manager_activities.py` | Same-day branch, country fan-out, dbt, Odoo, Slack |
| `sam_activities_api.py` | OAuth2 client, pagination, NDJSON normalize |
| `odoo_lead_push.py` | Thin adapter; wire pattern 02 lead engine in prod |
| `BUSINESS_CASE.md` | Why one DAG owns API → CRM |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Daily path, idempotency, failure modes |

## Quick start

```bash
python -c "import ast; ast.parse(open('sam_activities_api.py').read())"
python -c "import ast; ast.parse(open('odoo_lead_push.py').read())"
python -c "import ast; ast.parse(open('dag_sales_manager_activities.py').read())"
```

To run for real you need Airflow Variables for OAuth (`sam_*`),
Composer/rawzone buckets, Odoo JSON creds, optional dbt job id, and
the pattern 02 lead engine wired into `odoo_lead_push.py`. This folder
is a sanitized reference, not a deploy.

## Sanitization notes

- GCP projects `hd-dwh-stream-*` → `dwh_project` / `dwh_project_dev`
- Buckets / datasets → `rawzone`, `composer-data`, `trusted_staging`
- MCC / Metronom / Metro SAM naming → field-sales / SAM activities API
- Hardcoded OAuth test credentials removed (Variables only)
- Emails / Slack channels → `dataops@example.com` / `#crm-data-ops`
- Owner → `data-platform`
- Package imports `horeca_digital.*` → local modules
- Hardcoded dbt job id → Variable `sam_leads_dbt_job_id`
- Odoo push deferred to pattern 02 via thin adapter stub
- Fixed missing imports / method-name drift from the source module
- `max_active_runs=1`; dbt stub when provider / job id missing

## Category

`odoo_integration/42-sales-manager-activities-odoo/`
