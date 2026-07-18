# Pattern Backlog

Tracking file for the daily Airflow pattern shipping automation.
Source of truth for Done / Next / Skipped is also mirrored in automation Memories.

## Done

| # | Pattern | Category | Source (airflow2) | Notes |
|---|---------|----------|-------------------|-------|
| 01 | Matching Engine SCD Type 2 | `sql_patterns/01-matching-engine-scd-type2/` | (shipped before backlog) | In repo |
| 02 | POS product category prediction | `ml_pipelines/02-product-category-prediction/` | `dags/horeca_digital/posms_predict_product_category.py` (+ archived DAG overview) | Shipped 2026-07-16 |
| 03 | Adyen payment terminal integration | `payment_processing/03-adyen-payment-terminal/` | `dags/etl_adyen_payment_terminal.py` + `dags/horeca_digital/adyen_payment_terminal_integration.py` | Shipped 2026-07-17 |
| 04 | Multi-country FBO/NBO scoring export | `scoring_analytics/04-dana-scoring/` | `dags/horeca_digital/dana_scoring_query.py` + `dana_scoring_export.py` + `dags/etl_dana_scoring_data_export.py` | Shipped 2026-07-18 |

## Also already in repo (not from daily automation priority queue)

| Pattern | Category | Notes |
|---------|----------|-------|
| Accounts / invoice load | `odoo_integration/01-accounts-invoice-load/` | Existing |
| Leads ingestion | `odoo_integration/02-leads-ingestion/` | Existing |
| Opportunities load | `odoo_integration/03-opportunities-load/` | Existing |
| Dynamic TaskGroups | `odoo_integration/04-dynamic-taskgroups/` | Existing |
| Connection management | `odoo_integration/05-connection-management/` | Existing |

## Next (priority order)

1. **Salesforce** — one strong SFDC DAG (e.g. `dana_sfdc_asset_query.py` / export pair) → `salesforce_integration/05-sfdc-asset-export/` (or next free number)
2. **Odoo daily/incremental sync** — not a full migration dump (prefer something not already covered under `odoo_integration/`)
3. Other unique unused DAG under `horeca_digital/` / `archived/` (e.g. matching export, dishpay KYC, helpdesk)

## Skipped

_None yet._

## Blockers

### 2026-07-16 (earlier runs) — Source access failed — RESOLVED

- GitLab clone via `GITLAB_TOKEN` now works (oauth2 HTTPS sparse checkout).
- Pattern 02 shipped after unblock.
