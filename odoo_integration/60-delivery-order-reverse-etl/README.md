# Pattern 60: Delivery order reverse ETL (BQ → Odoo stock.picking)

Composer DAG that reads curated delivery orders from BigQuery and
creates or updates Odoo `stock.picking` records — moves, serial lots,
machine codes, fulfilment validation — via OdooRPC + Postgres lookups.

Distinct from inbound Odoo exports (06–15) and field-sales CRM push
(42): this is warehouse → ERP logistics write-back. The live
`horeca_digital/product_installation_odoo.py` module was missing from
the tree; logic was extracted from the archived odoo_migration module.

Source (read-only):
- `dags/horeca_digital/archived/etl_delivery_order.py`
- `dags/horeca_digital/archived/odoo_migration/product_installation_odoo.py`
  (`OdooProductInstallation` delivery-order path only)

## Files

| File | Role |
|------|------|
| `dag_delivery_order.py` | On-demand DAG; ranked BQ query + PythonOperator |
| `odoo_delivery_order.py` | Insert/update sync class + dual connection |
| `odoo_connection.py` | Thin OdooRPC login; wire pattern 05 in prod |
| `BUSINESS_CASE.md` | Why reverse ETL into stock.picking |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Grain, idempotency, failure modes |

## Quick start

```bash
python -c "import ast; ast.parse(open('odoo_connection.py').read())"
python -c "import ast; ast.parse(open('odoo_delivery_order.py').read())"
python -c "import ast; ast.parse(open('dag_delivery_order.py').read())"
```

To run for real you need Airflow Variables (`env`, `dwh_project_id`,
`odoo_*_creds` with hostname/database/user/password/db_user/db_pwd),
Odoo XMLIDs for warehouse / customer location, the trusted delivery
table with nested `delivery_details`, and the exported-ids control
table. This folder is a sanitized reference, not a deploy.

## Sanitization notes

- GCP project `hd-dwh-stream-1` → `dwh_project` / Variable `dwh_project_id`
- Datasets / tables → `trusted.odoo_stock_picking_delivery_order`,
  `ops.delivery_order_exported_ids`
- Brand prefixes (`dish_*`) on Odoo fields / modules → generic
  `partner_uuid`, `subscription_uuid`, `fulfilment_status`,
  `product_catalog`, `machine.code`, `__external_asset_id__`
- Owner / emails → `data-platform` / `dataops@example.com`
- Package imports `horeca_digital.*` → local modules
- SalesforceProductInstallation land queries omitted (upstream of
  trusted table; not required for the reverse-ETL pattern)
- Replaced `eval()` on POS-server-ready flags with a safe bool parse
- Collapsed repeated timeout blocks onto `retry_on_timeout` where the
  behaviour stayed equivalent; kept sale-order 300s retry
- `DummyOperator` → `EmptyOperator`; added tags + `max_active_runs=1`
- Mutable default `exception_list=[]` fixed to `None`

## Category

`odoo_integration/60-delivery-order-reverse-etl/`
