# Pattern 13: Weekly active Odoo asset ID snapshot

Weekly multi-country export of active `sale_order_line` IDs to a
partner event bus. The consumer LEFT JOINs this snapshot against
asset lifecycle data and treats missing IDs as deleted.

No dbt step — reads refined sales tables after upstream cleanup.

Source (read-only):
- `dags/etl_dana_odoo_active_asset_ids_export.py`
- `dags/horeca_digital/dana_odoo_assets_leads_lifecycle_export.py`
  (`get_active_asset_ids_query`, `send_active_asset_ids_data`)

## Files

| File | Role |
|------|------|
| `active_asset_ids_query.py` | Country-scoped DISTINCT SELECT over refined sales |
| `active_asset_ids_export.py` | OAuth client, Avro encode, chunked bulk POST |
| `dag_active_asset_ids_export.py` | Composer DAG: 13 parallel country tasks |
| `BUSINESS_CASE.md` | Why presence snapshot sits beside lifecycle deltas |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Run order, idempotency, failure modes |

## Quick start

```bash
python -c "import ast; ast.parse(open('active_asset_ids_query.py').read())"
python -c "import ast; ast.parse(open('active_asset_ids_export.py').read())"
python -c "import ast; ast.parse(open('dag_active_asset_ids_export.py').read())"
python active_asset_ids_query.py   # prints the DE SELECT
```

To run for real you need refined sales tables, event-API OAuth, and
the active-IDs schema id in Airflow Variables. This folder is a
sanitized reference, not a deploy.

## Sanitization notes

- GCP projects `hd-dwh-stream-*` → `dwh_project` / `dwh_project_dev`
- Dataset `dwh_refined_sales` → `refined_sales`
- Column `dish_partner_uuid` → `partner_uuid`
- Schema names `dana_odoo_active_asset_ids*` → `odoo_active_asset_ids*`
- Event API host / schema ids / OAuth Variable names generalized
- Partner product names (DANA / MFR / Metro) → event bus / master-file
- Real notification emails → `dataops@example.com`
- Owner / author names removed
- Package import `horeca_digital.dana_odoo_assets_leads_lifecycle_export`
  → local modules
- `DummyOperator` → `EmptyOperator` (with fallback)
- `max_active_runs` kept on DAG constructor only (dropped from
  default_args duplicate)
- Avro schema parse moved outside the per-row loop
- HTTP errors now raise (`raise_for_status`) instead of log-only
- BigQuery DATE → Avro logical date conversion made explicit

## Distinct from pattern 09

Pattern 09: twice-daily FR SCD deltas (leads, assets, vouchers) after
dbt. This pattern: weekly 13-country presence snapshot with no dbt.
Same export module family in production; different Composer DAG and
different consumer contract.

## Category

`odoo_integration/13-active-asset-ids-export/`
