# Pattern 09: Odoo / CRM assets + leads lifecycle export

Twice-daily pipeline that refreshes refined Odoo/CRM lead and asset
lifecycle models via dbt Cloud, then fans out three Avro bulk ingest
tasks for a single partner market (FR): lead lifecycle, asset
lifecycle, and voucher codes.

Source (read-only):
- `dags/etl_dana_odoo_assets_leads_export.py`
- `dags/horeca_digital/dana_odoo_assets_leads_lifecycle_export.py`

## Files

| File | Role |
|------|------|
| `lifecycle_queries.py` | SCD / created-date SELECT builders for the three feeds |
| `lifecycle_export.py` | OAuth client, Avro encode, chunked bulk POST |
| `dag_assets_leads_lifecycle_export.py` | Composer DAG: dbt → parallel ingest |
| `BUSINESS_CASE.md` | Why one DAG owns three related schemas |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Run order, idempotency, failure modes |

## Quick start

```bash
python -c "import ast; ast.parse(open('lifecycle_queries.py').read())"
python -c "import ast; ast.parse(open('lifecycle_export.py').read())"
python -c "import ast; ast.parse(open('dag_assets_leads_lifecycle_export.py').read())"
python lifecycle_queries.py   # prints the three SELECTs
```

To run for real you need the refined tables, a dbt Cloud job id in
Airflow Variables, event-API OAuth, and three schema ids. This folder
is a sanitized reference, not a deploy.

## Sanitization notes

- GCP projects `hd-dwh-stream-*` → `dwh_project` / `dwh_project_dev`
- Dataset `dwh_refined` → `refined`
- Tables `dana_odoo_*` → `odoo_*`
- Event API host / schema ids / OAuth Variable names generalized
- Hard-coded dbt job id → Airflow Variable
  `dbt_job_odoo_leads_assets_lifecycle`
- Real notification emails → `dataops@example.com`
- Owner / author names removed
- Package import `horeca_digital.dana_odoo_assets_leads_lifecycle_export`
  → local modules
- `DummyOperator` → `EmptyOperator` (with fallback)
- `max_active_runs` moved to DAG constructor (was only in default_args)
- Avro schema parse moved outside the per-row loop
- HTTP errors now raise (`raise_for_status`) instead of log-only
- Sibling `send_active_asset_ids_data` left out — that belongs to a
  different DAG (`etl_dana_odoo_active_asset_ids_export`)

## Category

`odoo_integration/09-assets-leads-lifecycle-export/`
