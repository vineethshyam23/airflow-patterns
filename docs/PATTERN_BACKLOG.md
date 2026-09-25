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
| 33 | Jira Service Desk ingest (incremental + monthly full-load) | `utilities/33-jira-service-desk-ingest/` | `dags/etl_jira_HDSD.py` + `dags/horeca_digital/jira_hdsd.py` | Shipped 2026-09-03 |
| 34 | Mailchimp email analytics ingest (6 grains + campaign fan-out) | `utilities/34-mailchimp-email-analytics/` | `dags/etl_mailchimp.py` + `dags/horeca_digital/mailchimp.py` | Shipped 2026-09-04 |
| 35 | POS vendor store-details HMAC CSV ingest | `utilities/35-booq-storedetails-hmac-ingest/` | `dags/etl_booq_storedetails.py` + `dags/horeca_digital/booq_storedetails.py` | Shipped 2026-09-05 |
| 36 | Mach2 Odoo sales Excel email report | `odoo_integration/36-mach2-sales-email-report/` | `dags/etl_mach2_report.py` + `dags/horeca_digital/mach2_report/` + `mach2_report_airflow/` + `email_delivery/` | Shipped 2026-09-06 |
| 37 | Payment wallet API ingest (KYC + txn + VOP) | `payment_processing/37-dishpay-api-ingest/` | `dags/etl_dishpay_dbt.py` + `dags/horeca_digital/get_dish_pay_data.py` | Shipped 2026-09-07 |
| 38 | POS vendor GA4 rolling event ingest | `utilities/38-booq-ga4-rolling-ingest/` | `dags/etl_booq_google_analytics.py` | Shipped 2026-09-08 |
| 39 | Hydra Cloud SQL weekly full export (v2) | `custom_operators/39-hydra-cloudsql-weekly-export/` | `dags/etl_hydra_job_v2.py` + `horeca_digital/hydra_raw_export_queries.py` + `operators/cloudsql_retry_operator.py` | Shipped 2026-09-09 |
| 40 | Food Graph ML multi-project propagation | `ml_pipelines/40-foodgraph-ml-propagation/` | `dags/etl_foodgraph.py` + `dags/horeca_digital/foodgraph_queries.py` | Shipped 2026-09-10 |
| 41 | PAIR Finance multi-market case ingest | `payment_processing/41-pair-finance-cases-ingest/` | `dags/etl_pair_finance_cases_daily.py` + `dags/horeca_digital/pair_finance_api.py` + `pair_finance_pipeline.py` | Shipped 2026-09-11 |
| 42 | Field-sales activities → Odoo CRM | `odoo_integration/42-sales-manager-activities-odoo/` | `dags/etl_sales_manager_activities.py` + `dags/horeca_digital/sales_manager_activities.py` | Shipped 2026-09-12 |
| 43 | Midday POS customer-master refresh | `utilities/43-pos-afternoon-customer-refresh/` | `dags/etl_dish_pos_afternoon.py` | Shipped 2026-09-13 |
| 44 | Lead enrichment + Cloud Run scoring | `ml_pipelines/44-lead-enrichment-cloud-run-scoring/` | `dags/etl_leads_enrichment.py` (+ pattern 02 Odoo push) | Shipped 2026-09-14 |
| 45 | Food Graph Vertex PipelineJob submit | `ml_pipelines/45-foodgraph-vertex-pipeline-job/` | `dags/horeca_digital/archived/etl_food_graph_vertex.py` + `food_graph_vertex.py` + `food_graph_vertex_utils.py` | Shipped 2026-09-15 |
| 46 | Food Graph refined multi-country zone | `sql_patterns/46-refined-foodgraph-zone/` | `dags/etl_refined_foodgraph_zone.py` + `dags/horeca_digital/foodgraph_queries.py` | Shipped 2026-09-16 |
| 47 | Absolute multi-channel activity scores | `scoring_analytics/47-absolute-activity-scores/` | `dags/absolute_activityscores.py` | Shipped 2026-09-17 |
| 48 | Offer Tool weekday-aware multi-project zone | `sql_patterns/48-customized-offerings-zone/` | `dags/etl_customized_offering_zone.py` + `dags/horeca_digital/customized_offering_queries.py` | Shipped 2026-09-18 |
| 49 | Adobe Analytics hourly Data Feed land | `utilities/49-adobe-analytics-hourly-rawfeed/` | `dags/etl_aa_adobe_rawfeed_hourly.py` | Shipped 2026-09-19 |
| 50 | Overnight multi-country POS land + backfill | `utilities/50-pos-overnight-multi-country/` | `dags/etl_dish_pos.py` | Shipped 2026-09-20 |
| 51 | Adobe Analytics app Data Feed land | `utilities/51-adobe-analytics-app-rawfeed/` | `dags/etl_aa_adobe_rawfeed_app.py` | Shipped 2026-09-21 |
| 52 | Offer Tool on-demand multi-project zone | `sql_patterns/52-customized-offerings-zone-on-demand/` | `dags/etl_customized_offerings_zone_on_demand.py` + `dags/horeca_digital/customized_offering_queries.py` | Shipped 2026-09-22 |
| 53 | POS Intelligence recommendations → partner event bus | `scoring_analytics/53-pos-intelligence-recommendations-export/` | `dags/etl_dana_pos_intelligence_recommendations_export.py` + `dags/horeca_digital/dana_pos_intelligence_export.py` | Shipped 2026-09-23 |
| 54 | Reservation Tool incremental Cloud SQL export (id-watermark + Sunday full sync) | `custom_operators/54-reservation-tool-incremental-export/` | `dags/etl_reservationtool_v2.py` + `dags/horeca_digital/rt_table_config.py` + `operators/cloudsql_retry_operator.py` | Shipped 2026-09-24 |
| 55 | Menu Engineering VM Postgres land (SSH COPY + dual-bucket) | `utilities/55-menu-engineering-vm-land/` | `dags/etl_deepideas_to_DWH.py` | Shipped 2026-09-25 |

## Also already in repo (not from daily automation priority queue)

| Pattern | Category | Notes |
|---------|----------|-------|
| Accounts / invoice load | `odoo_integration/01-accounts-invoice-load/` | Existing |
| Leads ingestion | `odoo_integration/02-leads-ingestion/` | Existing |
| Opportunities load | `odoo_integration/03-opportunities-load/` | Existing |
| Dynamic TaskGroups | `odoo_integration/04-dynamic-taskgroups/` | Existing |
| Connection management | `odoo_integration/05-connection-management/` | Existing |

## Next (priority order)

1. DISH Order sharded Cloud SQL land (`etl_dishorder.py`) — 44 shards + master, CSV merge, SCD Type 2 into `order_*`
2. Alternate: Keycloak SSO events (`etl_sso.py`) — tar.gz land + append-only trusted
3. Refined zone monthly / value-creation zone — only if engineering depth is distinct from #46 / #48
4. Exchange rates / Tourism NRW — skip unless engineering depth returns
5. Skip `invoice_ai_data_import.py` unless rewritten without embedded secrets
6. Skip thin dbt wrappers: matching-engine, activity-score, app rawfeed job, master_id, owg_v2
7. Do not confuse `bq_reservation.py` (BigQuery slot reservation helper) with Reservation Tool product (#54)
8. Do not ship Deepideas Avro enrichment (#20–#22) or VM land (#55) again


## Skipped

| Pattern | Source | Reason | Date |
|---------|--------|--------|------|
| Eijsink GA4 rolling ingest | `dags/etl_eijsink_google_analytics.py` | Same 7-day DELETE+INSERT + dbt pattern as #38; only delta is missing Data Transfer + different property/staging | 2026-09-09 |
| Matching Engine dbt Cloud wrapper | `dags/etl_matching_engine.py` / `matching_engine_prod_job` | Thin dbt Cloud job trigger; SCD already #01, partner export #10 | 2026-09-18 |

## Blockers

### 2026-07-16 (earlier runs) — Source access failed — RESOLVED

- GitLab clone via `GITLAB_TOKEN` now works (oauth2 HTTPS sparse checkout).
- Pattern 02 shipped after unblock.
