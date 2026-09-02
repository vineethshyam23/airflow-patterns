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
| 10 | Matching engine export to partner event bus | `sql_patterns/10-matching-engine-event-export/` | `dags/horeca_digital/matching_export_to_DANA.py` + `dana_matching_engine_export.py` + `archived/etl_dana_matching_engine_export.py` | Shipped 2026-07-24 |
| 11 | Payment KYC export to partner event bus | `payment_processing/11-dishpay-kyc-export/` | `dags/etl_dana_dishpay_kyc_export.py` + `dags/horeca_digital/dana_dishpay_kyc_export.py` + `dana_dishpay_kyc_query.py` | Shipped 2026-07-25 |
| 12 | Ranked menu-gaps export (FARM_FINGERPRINT batches) | `scoring_analytics/12-rex-menu-gaps-export/` | `dags/etl_dana_rex_menu_gaps_export.py` + `dags/horeca_digital/dana_rex_menu_gaps_export.py` + `dana_rex_menu_gaps_query.py` | Shipped 2026-07-26 |
| 13 | Weekly active Odoo asset ID snapshot | `odoo_integration/13-active-asset-ids-export/` | `dags/etl_dana_odoo_active_asset_ids_export.py` + `dags/horeca_digital/dana_odoo_assets_leads_lifecycle_export.py` (active-ID query/send) | Shipped 2026-07-27 |
| 14 | Independent-establishment menu-gaps export | `scoring_analytics/14-menu-gaps-independent-export/` | `dags/etl_dana_rex_menu_gaps_non_metro_export.py` + `dags/horeca_digital/dana_rex_menu_gaps_non_metro_export.py` | Shipped 2026-07-29 |
| 15 | Odoo helpdesk Postgres incremental pull | `odoo_integration/15-helpdesk-postgres-pull/` | `dags/horeca_digital/helpdesk_odoo_import.py` + `helpdesk_odoo.py` + `archived/odoo_migration/etl_odoo_helpdesk_import.py` | Shipped 2026-08-12 |
| 16 | Peer benchmarking gaps (multi-country + Avro) | `scoring_analytics/16-benchmarking-gaps/` | `dags/etl_benchmarking_gaps.py` + `dags/horeca_digital/benchmarking_gaps_queries.py` + `dana_deepideas_benchmarking_gaps_export.py` + delta helpers in `dana_deepideas_query.py` | Shipped 2026-08-13 |
| 17 | Establishment market-data monthly Avro export | `scoring_analytics/17-dish-market-data-export/` | `dags/etl_dana_dish_market_data_export.py` + `dags/horeca_digital/dana_dish_market_data_export.py` + `foodgraph_queries.dish_market_data_active_isocode_list` | Shipped 2026-08-16 |
| 18 | SEO business-listing menu URL extraction | `utilities/18-dataforseo-menu-url-extraction/` | `dags/etl_dataforseo_menu_url_extractor.py` + `dataforseo_gbq_menu_url_extractor.py` + `dataforseo_menu_url_discovery.py` + `dataforseo_menu_url_utils.py` | Shipped 2026-08-17 |
| 19 | Freshdesk REST API ingest (hourly + monthly branch) | `utilities/19-freshdesk-api-ingest/` | `dags/horeca_digital/freshdesk_extract.py` + `archived/etl_freshdesk_import.py` | Shipped 2026-08-19 |
| 20 | Deepideas establishment attribute weekly Avro export | `scoring_analytics/20-deepideas-establishment-export/` | `dags/etl_dana_deep_ideas_export.py` + `dags/horeca_digital/dana_deepideas_establishment_export.py` + `dana_deepideas_query.py` (Establishment) | Shipped 2026-08-20 |
| 21 | Deepideas main-category gaps weekly Avro export | `scoring_analytics/21-deepideas-gaps-category-export/` | `dags/etl_dana_deep_ideas_export.py` + `dags/horeca_digital/dana_deepideas_gaps_category_export.py` + `dana_deepideas_query.py` (GapsCategory) | Shipped 2026-08-21 |
| 22 | Deepideas gap-ingredients weekly Avro export | `scoring_analytics/22-deepideas-gaps-ingredients-export/` | `dags/etl_dana_deep_ideas_export.py` + `dags/horeca_digital/dana_deepideas_gaps_ingredients_export.py` + `dana_deepideas_query.py` (GapIngredients) | Shipped 2026-08-22 |
| 23 | Multi-country platform-customer footprint export | `scoring_analytics/23-dish-customer-export/` | `dags/etl_dana_DISH_customer_data_export.py` + `dags/horeca_digital/dana_DISH_customer_export.py` + `dana_DISH_customer_query.py` | Shipped 2026-08-23 |
| 24 | MAG acquisition + penetration monthly export | `scoring_analytics/24-mag-acquisition-penetration/` | `dags/etl_dana_mag_export.py` + `dags/horeca_digital/dana_mag_acquisition.py` + `dana_mag_penetration.py` | Shipped 2026-08-24 |
| 25 | SEO listing GCS ingest (vendor NDJSON → BQ) | `utilities/25-dataforseo-gcs-ingest/` | `dags/etl_dataforseo_ingestion.py` + `dags/horeca_digital/dataforseo_gcs_ingest.py` | Shipped 2026-08-26 |
| 26 | Single-market Order + Reservation monthly export | `scoring_analytics/26-pl-dish-orders-reservations/` | `dags/etl_dana_pl_dish_orders_reservations_export.py` + `dags/horeca_digital/dana_pl_dish_orders_export.py` + `dana_pl_dish_orders_query.py` | Shipped 2026-08-27 |
| 27 | Offer Tool multi-table Cloud SQL SCD Type 2 ingest | `sql_patterns/27-customized-offering-scd-ingest/` | `dags/etl_customized_offering.py` + `dags/horeca_digital/customized_offering_queries.py` (export queries) | Shipped 2026-08-28 |
| 28 | AppFigures weekly mobile analytics ingest | `utilities/28-appfigures-pipeline/` | `dags/etl_appfigures_pipeline.py` + `dags/horeca_digital/get_appfigures_data.py` | Shipped 2026-08-29 |
| 29 | Vonage Contact Center daily stats ingest | `utilities/29-vonage-contact-center-ingest/` | `dags/etl_vonage_dbt.py` + `dags/horeca_digital/get_vonage_data.py` | Shipped 2026-08-30 |
| 30 | Medallia survey feedback SCD Type 2 ingest | `utilities/30-medallia-feedback-ingest/` | `dags/etl_medallia.py` + `dags/horeca_digital/medallia.py` | Shipped 2026-08-31 |
| 31 | Maileon email marketing import (8 reports + metadata + dbt) | `utilities/31-maileon-email-import/` | `dags/etl_maileon_import.py` + `dags/horeca_digital/maileon.py` + `get_maileon_names.py` | Shipped 2026-09-01 |
| 32 | Invoice Radar LPV vs invoice reconciliation + email report | `data_quality/32-invoice-radar/` | `dags/etl_invoice_radar.py` + `invoice_radar/` + `invoice_radar_airflow/` + `email_delivery/` | Shipped 2026-09-02 |

## Also already in repo (not from daily automation priority queue)

| Pattern | Category | Notes |
|---------|----------|-------|
| Accounts / invoice load | `odoo_integration/01-accounts-invoice-load/` | Existing |
| Leads ingestion | `odoo_integration/02-leads-ingestion/` | Existing |
| Opportunities load | `odoo_integration/03-opportunities-load/` | Existing |
| Dynamic TaskGroups | `odoo_integration/04-dynamic-taskgroups/` | Existing |
| Connection management | `odoo_integration/05-connection-management/` | Existing |

## Next (priority order)

1. Exchange rates (`exchangerates.py`) — only if non-trivial engineering value
2. Tourism NRW only if clearly more than a thin dbt trigger (current DAG is mostly `DbtCloudRunJobOperator`; HasData extract removed)
3. Additional Salesforce DAG only if clearly distinct from asset history (Marketing Cloud / archived SFMC only if clearly valuable)
4. Other unique high-value DAG under `horeca_digital/` or `archived/` not already Done
5. Skip `invoice_ai_data_import.py` unless rewritten without embedded secrets (AlloyDB OCR extract; separate from Invoice Radar)

## Skipped

_None yet._

## Blockers

### 2026-07-16 (earlier runs) — Source access failed — RESOLVED

- GitLab clone via `GITLAB_TOKEN` now works (oauth2 HTTPS sparse checkout).
- Pattern 02 shipped after unblock.
