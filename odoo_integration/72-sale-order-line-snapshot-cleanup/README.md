# Pattern 72: Odoo sale_order_line snapshot soft-close cleanup

Weekly Composer job that reconciles live Odoo `sale_order_line` IDs
with the dbt snapshot table. Rows still open in the warehouse
(`dbt_valid_to IS NULL`) but gone from Odoo get soft-closed — history
stays, the open set shrinks. ID sets travel Odoo → NDJSON → GCS → BQ
temp table so XCom never sees millions of integers.

Distinct from inbound Odoo exports (06–15), weekly active-ID snapshot
(13), and reverse ETL (60): this is **delete detection for a dbt
snapshot**, not a land, partner export, or ERP write-back.

Source (read-only):
- `dags/etl_odoo_sale_order_line_cleanup_optimized.py`

## Files

| File | Role |
|------|------|
| `odoo_postgres.py` | Lazy Postgres client (ID extract only) |
| `dag_sale_order_line_snapshot_cleanup.py` | Extract → GCS → temp BQ → identify → UPDATE → stats → drop temp |
| `BUSINESS_CASE.md` | Why soft-close + GCS instead of XCom |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Grain, sync bands, failure modes |

## Quick start

```bash
python -c "import ast; ast.parse(open('odoo_postgres.py').read())"
python -c "import ast; ast.parse(open('dag_sale_order_line_snapshot_cleanup.py').read())"
```

Needs Airflow Variables (`env`, `dwh_project_id`, `rawzone_bucket`,
`odoo_dm_creds` / `odoo_prod_creds`), write access to
`trusted.odoo_sale_order_line_snapshot`, and an `ops` dataset for the
temp ID table. This folder is a sanitized reference, not a deploy.

### Variables

| Variable | Purpose |
|----------|---------|
| `env` | `DEV` vs prod branch |
| `dwh_project_id` | BigQuery project |
| `rawzone_bucket` | GCS bucket for NDJSON ID file |
| `odoo_dm_creds` / `odoo_prod_creds` | Postgres hostname/database/db_user/db_pwd |

## Sanitization notes

- GCP `hd-dwh-stream-1*` → Variable `dwh_project_id` defaults
  `dwh_project` / `dwh_project_dev`
- Buckets `hd-digital-dp-*-rawzone` → Variable `rawzone_bucket`
- Datasets `dwh_de` / `dwh_trusted` → `ops` / `trusted`
- Creds `odoo_DM_creds` → `odoo_dm_creds` (aligned with pattern 15)
- Owner / emails → `data-platform` / `dataops@example.com`
- Package import `horeca_digital.odoo_data_export.OdooDataPull` → local
  `OdooPostgresClient` (connection + retry only)
- Emoji / brochure status strings in logs → plain IN SYNC / MINOR /
  SIGNIFICANT bands
- Unused `BigQueryCreateEmptyTableOperator` import dropped
- Sync thresholds kept as named constants (`SYNC_OK=100`, `SYNC_WARN=1000`)

## Category

`odoo_integration/72-sale-order-line-snapshot-cleanup/`
