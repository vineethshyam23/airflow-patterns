-- Refined Adobe Analytics *app* hit enrich (sanitized).
-- Mobile-suite projection: app id, device, carrier, OS, screen, event.
-- Joins only the four lookups the app feed actually ships.
--
-- Production shipped without the one-day hit_id anti-join that pattern 49
-- uses on the web path (predicate left commented in source). Documented
-- here as a known gap — enable the WHERE clause below if redeliveries
-- become noisy before the dbt-owned sibling takes over transforms.
--
-- Placeholders: {{ project_id }}, {{ staging }}, {{ refined }}

SELECT DISTINCT
  a.post_evar9 AS product_name,
  a.post_mobileappid AS app_id,
  a.post_evar8 AS user_id,
  a.post_evar2 AS establishment_id,
  COALESCE(a.post_evar4, a.post_cust_visid) AS business_id,
  a.post_evar15 AS contact_id,
  CONCAT(CAST(a.hitid_high AS INT64), '_', CAST(a.hitid_low AS INT64)) AS hit_id,
  CONCAT(
    CAST(a.post_visid_high AS INT64), '_',
    CAST(a.post_visid_low AS INT64), '_',
    CAST(a.visit_num AS INT64), '_',
    CAST(a.visit_start_time_gmt AS INT64)
  ) AS visit_id,
  CONCAT(
    CAST(a.post_visid_high AS INT64), '_',
    CAST(a.post_visid_low AS INT64)
  ) AS visitor_id,
  CONCAT(
    CAST(a.post_visid_high AS INT64), '_',
    CAST(a.post_visid_low AS INT64), '_',
    CAST(a.visit_num AS INT64), '_',
    CAST(a.visit_start_time_gmt AS INT64), '_',
    CAST(a.hitid_high AS INT64), '_',
    CAST(a.hitid_low AS INT64), '_',
    a.post_evar23
  ) AS unique_page_id,
  a.post_evar10 AS user_type,
  LOWER(a.post_evar3) AS user_language,
  UPPER(a.post_evar12) AS user_country,
  e.value AS country_value,
  a.geo_country,
  a.geo_city,
  a.geo_zip,
  d.value AS connection_type,
  a.post_mobiledevice AS device_type_model,
  a.post_evar11 AS device_type,
  CASE
    WHEN LOWER(SPLIT(a.post_evar11, ' ')[OFFSET(0)]) IN ('iphone', 'ipad', 'ipod')
      THEN SPLIT(a.post_evar11, ' ')[OFFSET(0)]
    ELSE CONCAT(
      UPPER(LEFT(SPLIT(a.post_evar11, ' ')[OFFSET(0)], 1)),
      LOWER(SUBSTR(SPLIT(a.post_evar11, ' ')[OFFSET(0)], 2))
    )
  END AS device_type_brand,
  a.carrier AS mobile_carrier,
  CASE
    WHEN i.value LIKE 'Windows%' AND i.value NOT LIKE '%Phone%' THEN 'Windows'
    WHEN i.value = 'Macintosh (iPhone)' OR i.value LIKE 'Mobile iOS%' THEN 'iOS'
    WHEN i.value = 'Macintosh' THEN 'OS X'
    ELSE RTRIM(REGEXP_REPLACE(i.value, '[0-9.]', ''))
  END AS os_aggr,
  a.post_mobileosversion AS os_version,
  a.post_evar13 AS app_version,
  a.post_evar5 AS environment,
  a.post_evar23 AS pagename,
  a.post_evar7 AS screen,
  a.post_page_event_var2 AS event_name,
  a.post_evar14 AS event_value,
  TIMESTAMP_SECONDS(CAST(a.visit_start_time_gmt AS INT64)) AS visit_start_time_gmt,
  CAST(
    TIMESTAMP_SECONDS(CAST(a.visit_start_time_gmt AS INT64)) AS DATE
  ) AS visit_start_time_gmt_dt,
  MIN(a.date_time) OVER (
    PARTITION BY CONCAT(
      CAST(a.post_visid_high AS INT64), '_',
      CAST(a.post_visid_low AS INT64), '_',
      CAST(a.visit_num AS INT64), '_',
      CAST(a.visit_start_time_gmt AS INT64)
    )
  ) AS first_hit_date_time,
  MAX(a.date_time) OVER (
    PARTITION BY CONCAT(
      CAST(a.post_visid_high AS INT64), '_',
      CAST(a.post_visid_low AS INT64), '_',
      CAST(a.visit_num AS INT64), '_',
      CAST(a.visit_start_time_gmt AS INT64)
    )
  ) AS last_hit_date_time,
  a.date_time AS hit_date_time,
  DATE(a.date_time) AS hit_date_time_dt,
  a.hitid_low,
  a.hitid_high,
  a.post_visid_high,
  a.post_visid_low,
  a.visit_num,
  a.post_evar1 AS free_evar1,
  a.post_evar6 AS free_evar6,
  a.post_evar16 AS free_evar16,
  a.post_evar17 AS free_evar17,
  a.post_evar18 AS free_evar18,
  a.post_evar19 AS free_evar19,
  a.post_evar20 AS free_evar20,
  a.post_evar21 AS free_evar21,
  a.post_evar22 AS free_evar22,
  a.post_evar24 AS free_evar24,
  a.post_evar25 AS free_evar25,
  a.post_evar26 AS free_evar26,
  a.post_evar27 AS free_evar27,
  a.post_evar28 AS free_evar28,
  a.post_evar29 AS free_evar29,
  a.post_evar30 AS free_evar30
FROM `{{ project_id }}.{{ staging }}.aa_appfeed_hit_data` a
LEFT JOIN `{{ project_id }}.{{ staging }}.aa_appfeed_connection_type` d
  ON a.connection_type = CAST(d.key AS STRING)
LEFT JOIN `{{ project_id }}.{{ staging }}.aa_appfeed_country` e
  ON a.country = CAST(e.key AS STRING)
LEFT JOIN `{{ project_id }}.{{ staging }}.aa_appfeed_operating_systems` i
  ON a.os = CAST(i.key AS STRING)
-- Optional idempotency guard (enabled on web hourly #49; off in app source):
-- WHERE CONCAT(CAST(a.hitid_high AS INT64), '_', CAST(a.hitid_low AS INT64)) NOT IN (
--   SELECT hit_id
--   FROM `{{ project_id }}.{{ refined }}.analytics_datafeed_app`
--   WHERE visit_start_time_gmt_dt >= CURRENT_DATE() - 1
-- )
