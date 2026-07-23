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
| 05 | Salesforce asset history delta export | `salesforce_integration/05-sfdc-asset-history-export/` | `dags/horeca_digital/dana_sfdc_asset_query.py` + `dana_sfdc_asset_export.py` + `archived/etl_dana_SFDC_asset_history_export.py` | Shipped 2026-07-19 |
| 06 | Odoo helpdesk tickets daily event export | `odoo_integration/06-helpdesk-tickets-export/` | `dags/etl_dana_odoo_helpdesk_tickets.py` + `dags/horeca_digital/dana_odoo_helpdesk_ticket.py` | Shipped 2026-07-20 |
| 07 | Odoo list-price / commission monthly delta export | `odoo_integration/07-list-price-export/` | `dags/horeca_digital/dana_odoo_list_price_query.py` + `dana_odoo_list_price_export.py` + `archived/etl_dana_Odoo_list_price_export.py` | Shipped 2026-07-21 |
| 08 | Odoo WSL invoices dual export (event + recommender) | `odoo_integration/08-wsl-invoices-export/` | `dags/etl_dana_odoo_wsl_invoices_export.py` + `dags/horeca_digital/dana_odoo_wsl_invoices.py` | Shipped 2026-07-22 |
| 09 | Odoo / CRM assets + leads lifecycle export | `odoo_integration/09-assets-leads-lifecycle-export/` | `dags/etl_dana_odoo_assets_leads_export.py` + `dags/horeca_digital/dana_odoo_assets_leads_lifecycle_export.py` | Shipped 2026-07-23 |

## Also already in repo (not from daily automation priority queue)

| Pattern | Category | Notes |
|---------|----------|-------|
| Accounts / invoice load | `odoo_integration/01-accounts-invoice-load/` | Existing |
| Leads ingestion | `odoo_integration/02-leads-ingestion/` | Existing |
| Opportunities load | `odoo_integration/03-opportunities-load/` | Existing |
| Dynamic TaskGroups | `odoo_integration/04-dynamic-taskgroups/` | Existing |
| Connection management | `odoo_integration/05-connection-management/` | Existing |

## Next (priority order)

1. **Matching engine export to event bus** — `dana_matching_engine_export.py` / `matching_export_to_DANA.py` (distinct from SCD Type 2 pattern 01)
2. **DishPay KYC export** — `dana_dishpay_kyc_export.py` + `etl_dana_dishpay_kyc_export.py`
3. **REX menu gaps export** — `dana_rex_menu_gaps_export.py` (+ non-metro variant if distinct enough)
4. **Odoo active asset IDs weekly snapshot** — `etl_dana_odoo_active_asset_ids_export.py` (sibling of pattern 09)
5. Odoo helpdesk *pull* (`helpdesk_odoo_import.py`) only if we want the Postgres incremental extractor as a separate pattern from the event export
6. Additional Salesforce DAG only if clearly distinct from asset history

## Skipped

_None yet._

## Blockers

### 2026-07-16 (earlier runs) — Source access failed — RESOLVED

- GitLab clone via `GITLAB_TOKEN` now works (oauth2 HTTPS sparse checkout).
- Pattern 02 shipped after unblock.
