-- Menu extraction tables. Scratch project is fine.
-- Dataset `de` must already exist. Re-run is safe.

CREATE TABLE IF NOT EXISTS `dwh_project.de.extracted_menu_urls` (
  menu_url_id INT64 NOT NULL,
  restaurant_name STRING,
  places_id STRING,
  country STRING,
  first_seen TIMESTAMP,
  establishment_id INT64,
  website STRING,
  menu_url STRING,
  fetched_menu_url STRING,
  fetched_menu_url_status STRING,
  menus_on_page INT64,
  playwright_used BOOL,
  _extraction_complete BOOL,
  _extracted_ts TIMESTAMP,
  _create_ts TIMESTAMP,
  _update_ts TIMESTAMP
)
CLUSTER BY country, establishment_id
OPTIONS (description = "Phase 1 ledger. One row per establishment menu URL.");

CREATE TABLE IF NOT EXISTS `dwh_project.de.menu_extraction_control_lnd` (
  menu_url_id INT64 NOT NULL,
  establishment_id INT64 NOT NULL,
  restaurant_name STRING,
  places_id STRING NOT NULL,
  country STRING NOT NULL,
  website STRING,
  menu_url STRING,
  fetched_menu_url STRING,
  effective_url STRING,
  effective_url_type STRING,
  gcs_html_uri STRING,
  gcs_md_uri STRING,
  content_hash STRING,
  menu_id INT64,
  menu_fetch_status STRING,
  error_message STRING,
  attempt_count INT64,
  _create_ts TIMESTAMP,
  _update_ts TIMESTAMP,
  _processed_ts TIMESTAMP
)
CLUSTER BY country, establishment_id
OPTIONS (description = "Phase 2 work queue. Pipeline state lives here, not on the ledger.");

CREATE TABLE IF NOT EXISTS `dwh_project.de.menu_md_staging` (
  menu_url_id INT64 NOT NULL,
  menu_id INT64,
  country STRING,
  effective_url STRING,
  gcs_md_uri STRING,
  markdown_text STRING,
  content_hash STRING,
  _load_ts TIMESTAMP
)
CLUSTER BY country, menu_url_id
OPTIONS (description = "BQML input. One markdown body per menu_url_id.");

CREATE TABLE IF NOT EXISTS `dwh_project.de.extracted_menus` (
  menu_id INT64 NOT NULL,
  menu_url_id INT64,
  establishment_id INT64,
  country STRING,
  effective_url STRING,
  restaurant_name STRING,
  places_id STRING,
  website STRING,
  menu_title STRING,
  menu_text STRING,
  menu_content_json STRING,
  gcs_html_uri STRING,
  gcs_md_uri STRING,
  content_hash STRING,
  model_name STRING,
  prompt_version STRING,
  _extracted_ts TIMESTAMP,
  _create_ts TIMESTAMP,
  _update_ts TIMESTAMP
)
CLUSTER BY country, establishment_id
OPTIONS (description = "Page-level menu. Not one row per dish.");
