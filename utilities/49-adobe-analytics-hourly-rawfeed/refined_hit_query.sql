-- Refined Adobe Analytics hit enrich (sanitized).
-- Joins staging hit rows to lookup key/value dumps, derives visit/hit IDs,
-- hashes visitor IP, and appends only hits not already in refined for the
-- last calendar day (hourly idempotency without a full window rewrite).
--
-- Placeholders match the DAG Variable / constant names:
--   {{ project_id }}, {{ staging }}, {{ refined }}

SELECT DISTINCT
  CONCAT(
    CAST(post_visid_high AS INT64), '_',
    CAST(post_visid_low AS INT64), '_',
    CAST(visit_num AS INT64), '_',
    FORMAT_TIMESTAMP('%Y%m%d %H%M%S', visit_start_time_gmt)
  ) AS visit_id,
  CONCAT(CAST(hitid_high AS INT64), '_', CAST(hitid_low AS INT64)) AS hit_id,
  CASE
    WHEN exclude_hit > 0 OR hit_source IN (5, 7, 8, 9) THEN 0
    ELSE 1
  END AS valid_hit,
  CASE
    WHEN post_prop2 IS NOT NULL AND post_prop6 IS NOT NULL THEN
      CASE
        WHEN post_prop6 = 'RU' THEN CONCAT('RU_', CAST(post_prop2 AS STRING))
        ELSE CONCAT('EU_', CAST(post_prop2 AS STRING))
      END
    WHEN post_evar4 IS NOT NULL AND post_evar2 IS NOT NULL THEN
      CASE
        WHEN post_evar2 = 'RU' THEN CONCAT('RU_', CAST(post_evar4 AS STRING))
        ELSE CONCAT('EU_', CAST(post_evar4 AS STRING))
      END
    ELSE CAST(NULL AS STRING)
  END AS business_id_sk,
  accept_language AS accepted_languages,
  browser AS browser_id,
  b.value AS browser_value,
  bb.value AS browser_type_value,
  c_color AS color_depth,
  carrier,
  code_ver AS code_version,
  color AS color_depth_id,
  c.value AS color_depth_value,
  connection_type AS connection_type_id,
  d.value AS connection_type_value,
  country AS country_id,
  e.value AS country_value,
  curr_factor AS currency_factor,
  curr_rate AS currency_fx_rate,
  daily_visitor AS new_daily_visitor,
  date_time AS hit_date_time,
  `domain`,
  duplicate_purchase,
  exclude_hit AS excluded_hit,
  CASE
    WHEN exclude_hit IN (1, 3) THEN 'Exclusion based on user agent'
    WHEN exclude_hit IN (2, 4) THEN 'Exclusion based on IP address'
    WHEN exclude_hit = 5 THEN 'Hit missing page_url / pagename / page_event / event_list'
    WHEN exclude_hit = 6 THEN 'JavaScript escape value found in hit'
    WHEN exclude_hit IN (7, 8) THEN 'Account-specific exclusion (VISTA rule)'
    WHEN exclude_hit = 9 THEN 'Unused'
    WHEN exclude_hit = 10 THEN 'Invalid currency code'
    WHEN exclude_hit = 11 THEN 'Hit missing timestamp on timestamp-only suite'
    ELSE ''
  END AS excluded_hit_value,
  first_hit_page_url,
  first_hit_pagename,
  first_hit_ref_type AS first_hit_ref_type_id,
  kk.value AS first_hit_ref_type_value,
  first_hit_referrer,
  first_hit_time_gmt,
  geo_city,
  geo_country,
  geo_dma,
  geo_region,
  geo_zip,
  hit_source AS hit_source_id,
  CASE
    WHEN hit_source = 1 THEN 'Standard image request without timestamp'
    WHEN hit_source = 2 THEN 'Standard image request with timestamp'
    WHEN hit_source = 3 THEN 'Live data source upload with timestamps'
    WHEN hit_source = 5 THEN 'Generic data source upload'
    WHEN hit_source = 6 THEN 'Full processing data source upload'
    WHEN hit_source = 7 THEN 'TransactionID data source upload'
    ELSE ''
  END AS hit_source_value,
  hit_time_gmt,
  CAST(hitid_high AS INT64) AS hitid_high,
  CAST(hitid_low AS INT64) AS hitid_low,
  hourly_visitor AS visitor_new_in_hour,
  TO_HEX(MD5(ip)) AS visitor_ip,
  j_jscript AS j_jscript_version,
  javascript AS javascript_id,
  g.value AS javascript_value,
  `language` AS language_id,
  h.value AS language_value,
  CASE
    WHEN h.value = 'Deuts' THEN 'German'
    WHEN h.value LIKE 'Norwegian%' THEN 'Norwegian'
    WHEN STRPOS(h.value, ' (') > 0 THEN RTRIM(SUBSTR(h.value, 1, STRPOS(h.value, ' (')))
    ELSE h.value
  END AS language_aggr,
  last_hit_time_gmt,
  last_purchase_num,
  CASE
    WHEN last_purchase_num = 0 THEN 'No prior purchases'
    WHEN last_purchase_num = 1 THEN '1 prior purchase'
    WHEN last_purchase_num = 2 THEN '2 prior purchases'
    WHEN last_purchase_num = 3 THEN '3 or more prior purchases'
    ELSE ''
  END AS last_purchase_num_value,
  last_purchase_time_gmt,
  mcvisid,
  mobile_id,
  monthly_visitor AS visitor_new_in_month,
  new_visit,
  os AS os_id,
  i.value AS os_value,
  CASE
    WHEN i.value LIKE 'Windows%' AND i.value NOT LIKE '%Phone%' THEN 'Windows'
    WHEN i.value = 'Macintosh (iPhone)' OR i.value LIKE 'Mobile iOS%' THEN 'iOS'
    WHEN i.value = 'Macintosh' THEN 'OS X'
    ELSE RTRIM(REGEXP_REPLACE(i.value, '[0-9.]', ''))
  END AS os_aggr,
  paid_search,
  post_browser_height AS browser_height,
  post_browser_width AS browser_width,
  CASE
    WHEN post_cookies = 'Y' THEN TRUE
    WHEN post_cookies = 'N' THEN FALSE
    ELSE CAST(NULL AS BOOLEAN)
  END AS cookies,
  post_currency AS currency,
  post_cust_hit_time_gmt AS cust_hit_time_gmt,
  -- Sanitized eVar / prop remaps (production maps product telemetry here)
  post_evar1 AS var_product_section,
  post_evar2 AS var_business_country,
  post_evar3 AS var_site_language,
  post_evar4 AS var_business_id,
  CASE
    WHEN post_evar4 IS NOT NULL AND SUBSTR(pagename, 1, 2) = 'RU'
      THEN CONCAT('RU_', CAST(post_evar4 AS STRING))
    WHEN post_evar4 IS NOT NULL
      THEN CONCAT('EU_', CAST(post_evar4 AS STRING))
    ELSE CAST(NULL AS STRING)
  END AS var_business_id_sk,
  post_evar5 AS var_business_name,
  post_evar6 AS var_business_category,
  post_evar7 AS var_visitor_type,
  post_evar8 AS var_business_domain,
  post_evar9 AS var_product_type,
  post_evar15 AS var_user_type,
  post_evar26 AS var_establishment_id,
  post_event_list AS event_list,
  CASE
    WHEN post_java_enabled = 'Y' THEN TRUE
    WHEN post_java_enabled = 'N' THEN FALSE
    ELSE CAST(NULL AS BOOLEAN)
  END AS java_enabled,
  post_keywords AS keywords,
  post_page_event AS page_event,
  post_page_event_var1 AS page_event_var1,
  post_page_event_var2 AS page_event_var2,
  post_page_url AS page_url,
  post_pagename AS pagename,
  post_pagename_no_url AS pagename_no_url,
  post_persistent_cookie AS persistent_cookie,
  post_product_list AS product_list,
  post_prop1 AS prop_product_type,
  post_prop2 AS prop_business_id,
  post_prop3 AS prop_product_section,
  post_prop4 AS prop_business_name,
  post_prop5 AS prop_site_language,
  post_prop6 AS prop_business_country,
  post_prop7 AS prop_business_zip,
  post_prop8 AS prop_business_category,
  post_prop9 AS prop_business_domain,
  post_prop11 AS prop_visitor_type,
  post_prop21 AS prop_establishment_id,
  post_referrer AS referrer,
  post_search_engine AS search_engine_id,
  m.value AS post_search_engine_value,
  post_t_time_info AS visitor_local_time_info,
  CAST(post_visid_high AS INT64) AS visid_high,
  CAST(post_visid_low AS INT64) AS visid_low,
  post_visid_type AS visid_type_id,
  CASE
    WHEN post_visid_type = 0 THEN 'Custom visitorID'
    WHEN post_visid_type = 1 THEN 'IP and user agent fallback'
    WHEN post_visid_type = 2 THEN 'HTTP Mobile Subscriber Header'
    WHEN post_visid_type = 3 THEN 'Legacy cookie value (s_vi)'
    WHEN post_visid_type = 4 THEN 'Fallback cookie value (s_fid)'
    WHEN post_visid_type = 5 THEN 'Experience Cloud ID Service'
    ELSE ''
  END AS visid_type_value,
  quarterly_visitor AS visitor_new_in_quarter,
  ref_domain AS referrer_domain,
  ref_type AS referrer_type_id,
  CASE
    WHEN ref_type = 1 THEN 'Inside your site'
    WHEN ref_type = 2 THEN 'Other web sites'
    WHEN ref_type = 3 THEN 'Search engines'
    WHEN ref_type = 4 THEN 'Hard drive'
    WHEN ref_type = 5 THEN 'USENET'
    WHEN ref_type = 6 THEN 'Typed/Bookmarked (no referrer)'
    WHEN ref_type = 7 THEN 'Email'
    WHEN ref_type = 8 THEN 'No JavaScript'
    WHEN ref_type = 9 THEN 'Social Networks'
    ELSE ''
  END AS referrer_type_value,
  resolution AS resolution_id,
  l.value AS resolution_value,
  s_resolution AS screen_resolution_raw,
  search_page_num,
  secondary_hit,
  truncated_hit,
  user_agent,
  visid_new,
  visid_timestamp,
  visit_keywords,
  visit_num,
  visit_page_num,
  visit_ref_domain,
  visit_ref_type AS visit_ref_type_id,
  kkk.value AS visit_ref_type_value,
  visit_referrer,
  visit_search_engine AS visit_search_engine_id,
  mm.value AS visit_search_engine_value,
  visit_start_page_url,
  visit_start_pagename,
  visit_start_time_gmt,
  CAST(visit_start_time_gmt AS DATE) AS visit_start_time_gmt_dt,
  weekly_visitor AS visitor_new_in_week,
  yearly_visitor AS visitor_new_in_year,
  CASE
    WHEN post_prop21 IS NOT NULL AND post_prop6 IS NOT NULL THEN
      CASE
        WHEN post_prop6 = 'RU' THEN CONCAT('RU_', CAST(post_prop21 AS STRING))
        ELSE CONCAT('EU_', CAST(post_prop21 AS STRING))
      END
    WHEN post_evar26 IS NOT NULL AND post_evar2 IS NOT NULL THEN
      CASE
        WHEN post_evar2 = 'RU' THEN CONCAT('RU_', CAST(post_evar26 AS STRING))
        ELSE CONCAT('EU_', CAST(post_evar26 AS STRING))
      END
    ELSE CAST(NULL AS STRING)
  END AS establishment_id_sk,
  campaign,
  clickmaplink,
  clickmaplinkbyregion,
  clickmappage
FROM `{{ project_id }}.{{ staging }}.aa_feed_hit_data` a
LEFT JOIN `{{ project_id }}.{{ staging }}.aa_feed_browser` b
  ON a.browser = b.key
LEFT JOIN `{{ project_id }}.{{ staging }}.aa_feed_browser_type` bb
  ON a.browser = bb.key
LEFT JOIN `{{ project_id }}.{{ staging }}.aa_feed_color_depth` c
  ON a.color = c.key
LEFT JOIN `{{ project_id }}.{{ staging }}.aa_feed_connection_type` d
  ON a.connection_type = d.key
LEFT JOIN `{{ project_id }}.{{ staging }}.aa_feed_country` e
  ON a.country = e.key
LEFT JOIN `{{ project_id }}.{{ staging }}.aa_feed_javascript_version` g
  ON a.javascript = g.key
LEFT JOIN `{{ project_id }}.{{ staging }}.aa_feed_languages` h
  ON a.LANGUAGE = h.key
LEFT JOIN `{{ project_id }}.{{ staging }}.aa_feed_operating_systems` i
  ON a.os = i.key
LEFT JOIN `{{ project_id }}.{{ staging }}.aa_feed_referrer_type` k
  ON a.ref_type = k.key
LEFT JOIN `{{ project_id }}.{{ staging }}.aa_feed_referrer_type` kk
  ON a.first_hit_ref_type = kk.key
LEFT JOIN `{{ project_id }}.{{ staging }}.aa_feed_referrer_type` kkk
  ON a.visit_ref_type = kkk.key
LEFT JOIN `{{ project_id }}.{{ staging }}.aa_feed_resolution` l
  ON a.resolution = l.key
LEFT JOIN `{{ project_id }}.{{ staging }}.aa_feed_search_engines` m
  ON a.post_search_engine = m.key
LEFT JOIN `{{ project_id }}.{{ staging }}.aa_feed_search_engines` mm
  ON a.visit_search_engine = mm.key
WHERE CONCAT(CAST(hitid_high AS INT64), '_', CAST(hitid_low AS INT64)) NOT IN (
  SELECT hit_id
  FROM `{{ project_id }}.{{ refined }}.analytics_datafeed`
  WHERE visit_start_time_gmt_dt >= CURRENT_DATE() - 1
)
