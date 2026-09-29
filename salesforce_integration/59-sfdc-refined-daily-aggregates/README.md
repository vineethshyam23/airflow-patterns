# Pattern 59: Salesforce refined daily aggregates

Composer DAG that WRITE_TRUNCATEs eight CRM-facing snapshot tables in
BigQuery — food orders, reservations, subscription billing, vouchers,
Odoo→SFDC establishment revenue (with MD5 `_rowhash`), invoice copy,
app logins, and multi-country POS — then emails reporting.

Distinct from pattern 05 (SFDC asset-history hash-delta → Avro event
bus): this pattern materializes warehouse tables for Salesforce /
reporting consumers and does not call CRM APIs.

Source (read-only):
- `dags/etl_refined_salesforce.py`

## Files

| File | Role |
|------|------|
| `dag_sfdc_refined_aggregates.py` | Staged fan-out of eight BQ truncates + email |
| `sfdc_refined_queries.py` | SQL builders for each snapshot |
| `BUSINESS_CASE.md` | Why CRM-shaped warehouse tables beat live joins |
| `ARCHITECTURE.md` | Components + Mermaid diagram |
| `DATA_FLOW.md` | Schedule, grains, failure modes |

## Quick start

```bash
python -c "import ast; ast.parse(open('sfdc_refined_queries.py').read())"
python -c "import ast; ast.parse(open('dag_sfdc_refined_aggregates.py').read())"
python sfdc_refined_queries.py
```

To run for real you need Airflow Variable `dwh_project_id`, BigQuery
dataset `refined_salesforce`, the upstream refined/trusted tables
referenced in the builders, and SMTP for `EmailOperator`. This folder
is a sanitized reference, not a deploy.

## Sanitization notes

- GCP project `hd-dwh-stream-1` → `dwh_project` / Variable `dwh_project_id`
- Dataset `dwh_refined_salesforce` → `refined_salesforce`
- Datasets `dwh_refined` / `dwh_trusted` / `dwh_trusted_views` /
  `dwh_trusted_staging` → `refined` / `trusted` / `trusted_views` /
  `trusted_staging`
- Product brand table prefixes (`dish_*`, `art_*`, `aac_*`, `asfdc_*`,
  `aodoo_*`) → generic `orders_*` / `rt_*` / `subscription_*` /
  `sfdc_*` / `odoo_*` / `app_datafeed` / `pos_payment_items_*`
- `dish_product_spot` → `product_spot`
- Emails → `dataops@example.com`; owner → `data-platform`
- Dead commented POS-vendor matching-id task removed
- Inline SQL extracted to `sfdc_refined_queries.py`
- Added `max_active_runs=1` and modern `EmailOperator` import path
- Kept `TriggerRule.ALL_DONE` behaviour visible (partial-refresh risk)

## Category

`salesforce_integration/59-sfdc-refined-daily-aggregates/`
