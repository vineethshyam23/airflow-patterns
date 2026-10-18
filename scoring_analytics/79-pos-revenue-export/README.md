# Pattern 79: Platform POS revenue potential Avro export

Monthly pipeline that streams customer-grain POS revenue potentials
from the warehouse to a partner event API. Trailing-12-month net
sales plus SAM-aligned annualisation, gated on licence lifecycle
and a four-week coverage floor. Countries run sequentially so one
OAuth / ingest path stays operable under full load.

Distinct from pattern 78 (Advanced Replenishment *predicted
quantity*), patterns 12/14 (ranked menu-gap opportunities), and
pattern 53 (POS Intelligence *article* recommendations). This feed
ships *annualised POS revenue potential* at wholesale customer
grain — the SAM `annual_sale` contract on the event bus.

Source (read-only):
- `dags/etl_dana_pos_revenue_export.py`
- `dags/horeca_digital/dana_pos_revenue_export.py`

## Files

| File | Role |
|------|------|
| `pos_revenue_export.py` | Revenue SQL, OAuth, Avro, chunked POST |
| `dag_pos_revenue_export.py` | Composer DAG: sequential countries, 4h timeout |
| `BUSINESS_CASE.md` | Why a separate monthly subject vs AR / POS intel |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Schedule, population rules, failure modes |

## Quick start

```bash
python -c "import ast; ast.parse(open('pos_revenue_export.py').read())"
python -c "import ast; ast.parse(open('dag_pos_revenue_export.py').read())"
python pos_revenue_export.py   # Avro schema parse + SQL smoke
```

To run for real you need product_spot asset/transaction tables, the
refined ERP↔wholesale match, event-API OAuth Variables, and a
registered schema id. This folder is a sanitized reference, not a
deploy.

## Sanitization notes

- GCP projects `hd-dwh-stream-*` → `dwh_project` / `dwh_project_dev`
- Dataset `dish_product_spot` → `product_spot`
- Tables: `aodoo_asset` → `erp_asset`;
  `dish_pos_soldItem_transactions_odoo` →
  `pos_sold_item_transactions_erp`;
  `match_result_odoo_mcc` → `match_result_erp_wholesale`
- Column renames: `metro_id` → `wholesale_id`, `cust_no` →
  `customer_no` (including match join)
- Partner / wholesale brand names and ticket ids removed
- Event API host / schema ids / OAuth Variable names generalized
- Real notification emails → `dataops@example.com`
- Package imports `horeca_digital.*` → local module + local `batched`
- Hard-coded schema hex ids → Airflow Variables with placeholder
  defaults
- `max_active_runs` / `max_active_tasks` on the DAG constructor

## Category

`scoring_analytics/79-pos-revenue-export/`
