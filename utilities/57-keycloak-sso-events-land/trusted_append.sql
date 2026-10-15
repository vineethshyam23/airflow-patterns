-- Staging → trusted append for Keycloak SSO events.
-- Placeholders are replaced by the DAG at parse/build time.
-- event_timestamp lands as epoch millis in the CSV; convert here.

SELECT
  id,
  client_id,
  details_json,
  error,
  ip_address,
  realm_id,
  session_id,
  TIMESTAMP_MILLIS(event_timestamp) AS event_timestamp,
  type,
  user_id,
  CURRENT_TIMESTAMP() AS _create_ts,
  TIMESTAMP(NULL) AS _update_ts,
  'etl_sso_events' AS _job_name,
  0 AS _job_id,
  'SSO' AS _sourcesystem,
  TIMESTAMP(CURRENT_DATE()) AS _valid_from,
  TIMESTAMP('2099-12-31 00:00:00') AS _valid_until,
  TRUE AS _valid_flag
FROM `{{ project_id }}.{{ staging_dataset }}.sso_events`
