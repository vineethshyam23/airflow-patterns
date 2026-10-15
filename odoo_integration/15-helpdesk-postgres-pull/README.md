# Pattern 15: Odoo helpdesk Postgres incremental pull

On-demand (or externally scheduled) landing DAG that pulls helpdesk
entities from Odoo Postgres into GCS/BigQuery staging, then refreshes
trusted models via dbt Cloud.

Distinct from pattern 06: that DAG exports refined tickets to an event
bus. This DAG is the warehouse ingest side.

Source (read-only):
- `dags/horeca_digital/helpdesk_odoo_import.py`
- `dags/horeca_digital/helpdesk_odoo.py`
- `dags/horeca_digital/archived/odoo_migration/etl_odoo_helpdesk_import.py`

## Files

| File | Role |
|------|------|
| `helpdesk_postgres_pull.py` | `HelpdeskPostgresPull` — Postgres → NDJSON |
| `helpdesk_row_count_checks.py` | Optional Odoo vs warehouse create/update counts |
| `dag_helpdesk_postgres_pull.py` | Composer DAG: fetch → GCS → BQ → dbt |
| `BUSINESS_CASE.md` | Why Postgres pull sits beside pattern 06 |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Stages, idempotency, failure modes |

## Quick start

```bash
python -c "import ast; ast.parse(open('helpdesk_postgres_pull.py').read())"
python -c "import ast; ast.parse(open('helpdesk_row_count_checks.py').read())"
python -c "import ast; ast.parse(open('dag_helpdesk_postgres_pull.py').read())"
```

To run for real you need Odoo Postgres Variables (`odoo_dm_creds` /
`odoo_prod_creds`), a Composer data volume path, raw-zone schemas under
`schema_json/`, and `dbt_odoo_helpdesk_job_id`. This folder is a
sanitized reference, not a deploy.

## Sanitization notes

- GCP projects `hd-dwh-stream-*` → `dwh_project` / `dwh_project_dev`
- Buckets `hd-digital-dp-*-rawzone` → `data-platform-*-rawzone`
- Dataset `dwh_trusted_staging` → `staging`; views → `trusted_views`
- Ticket custom fields `dish_*` aliased to neutral NDJSON keys
  (`language_id`, `country_id`, `reporter_email`, …)
- Team `dish_oppening_support_hours` → `opening_support_hours` (typo
  kept only on the Postgres source column name)
- Portal `access_token` removed from ticket extract
- Real notification emails → `dataops@example.com`
- Owner / author names removed
- Hard-coded dbt `job_id` → Variable `dbt_odoo_helpdesk_job_id`
- Variable `odoo_DM_creds` → `odoo_dm_creds`
- Module-level `HelpdeskPull(...)` at import → per-task factory
  (`_make_fetch`) so DAG parse does not open Postgres
- Package import `horeca_digital.helpdesk_odoo_import` → local module
- `EmptyOperator` with DummyOperator fallback

## Distinct from pattern 06

| | Pattern 06 | Pattern 15 |
|---|------------|------------|
| Direction | Warehouse → event bus | Odoo → warehouse |
| Source | `refined.odoo_helpdesk_ticket` | Odoo Postgres |
| Payload | Avro bulk ingest | NDJSON → BQ staging |
| Cadence | Daily scheduled | On-demand (`None`) |

## Category

`odoo_integration/15-helpdesk-postgres-pull/`
