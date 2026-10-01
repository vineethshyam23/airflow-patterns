"""SQL builders for derived-events change detection (representative subset).

Production ships ~58 sequential BigQueryInsertJobOperators in one DAG.
This module keeps six queries that cover the distinct detection styles:

1. CMS timestamp LAG (no hash anti-join)
2. CMS attribute LAG + nth_record + hash anti-join + bad-date filter
3. Analytics hit unnest + custom-event dictionary map
4. Reservation user login watermark (GROUP BY, no LAG)
5. Reservation establishment boolean LAG
6. Reservation composite JSON payload + one-record-per-entity-per-day

All paths WRITE_APPEND into a shared trusted.derived_events table and
dedupe with an MD5 _rowhash (Adobe uses a hit-scoped hash formula).
"""

from __future__ import annotations


def _rowhash_expr(
    id_col: str = "id",
    sourcesystem_col: str = "_sourcesystem",
) -> str:
    return (
        "to_hex(md5(CONCAT("
        "id_type,"
        f"CAST({id_col} AS string),"
        "event,"
        "CAST(ifnull(derived_event_int,0) AS string),"
        "CAST(ifnull(derived_event_timestamp, TIMESTAMP('1970-01-01 00:00:00')) AS string),"
        f"ifnull(derived_event_string,''),{sourcesystem_col}"
        ")))"
    )


def query_cms_modification_date() -> str:
    """Emit an event when CMS establishment last_modification_date changes."""
    return """
WITH t1 AS (
  SELECT
    'trusted.cms_establishments.id' AS id_type,
    id,
    id_sk AS idchar,
    'Establishment modification date change' AS event,
    1 AS derived_event_int,
    last_modification_date AS derived_event_timestamp,
    CAST(NULL AS string) AS derived_event_string,
    DATETIME_DIFF(
      DATETIME(last_modification_date), DATETIME(creation_date), DAY
    ) AS days_after_creation,
    _sourcesystem,
    ROW_NUMBER() OVER (
      PARTITION BY id_sk, last_modification_date ORDER BY _valid_from
    ) AS rn,
    lag(last_modification_date) OVER (
      PARTITION BY id_sk ORDER BY _valid_from
    ) AS lag
  FROM `trusted.cms_establishments`
)
SELECT DISTINCT
  * EXCEPT (rn, lag),
  {rowhash} AS _rowhash
FROM t1
WHERE lag <> derived_event_timestamp
""".format(
        rowhash=_rowhash_expr()
    ).strip()


def query_cms_loc_name_change(destination: str) -> str:
    """Attribute change on localised CMS name with hash anti-join."""
    rowhash = _rowhash_expr()
    return f"""
WITH t1 AS (
  SELECT
    'trusted.cms_establishments.id' AS id_type,
    id_id AS id,
    id_id_sk AS idchar,
    'Establishment loc name change' AS event,
    1 AS derived_event_int,
    timestamp_sub(a._valid_from, INTERVAL 1 DAY) AS derived_event_timestamp,
    CAST(name AS string) AS derived_event_string,
    TIMESTAMP_DIFF(
      timestamp_sub(a._valid_from, INTERVAL 1 DAY), creation_date, DAY
    ) AS days_after_creation,
    a._sourcesystem,
    lag(name) OVER (PARTITION BY id_id_sk ORDER BY a._valid_from) AS lag,
    ROW_NUMBER() OVER (PARTITION BY id_id_sk ORDER BY a._valid_from) AS nth_record
  FROM `trusted.cms_establishmentsloc` a
  INNER JOIN `trusted.cms_establishments` b
    ON a.id_id_sk = b.id_sk
  WHERE lang = 'default'
)
SELECT DISTINCT
  * EXCEPT (lag, nth_record),
  {rowhash} AS _rowhash
FROM t1
WHERE nth_record > 1
  AND ifnull(lag, '') != derived_event_string
  AND CAST(derived_event_timestamp AS date) != DATE('2018-10-09')
  AND {rowhash} NOT IN (
    SELECT _rowhash FROM `{destination}`
    WHERE event = 'Establishment loc name change'
  )
""".strip()


def query_adobe_datafeed(destination: str) -> str:
    """Flatten Adobe hit event_list and map Custom Events to names.

    id_type is resolved from the event number so downstream consumers
    know which surrogate (branch office, claim, platform, reservation,
    CMS) the hit belongs to. Hash formula is hit-scoped — different
    from the SCD paths — so partial re-runs must use this expression.
    """
    return f"""
WITH eventlist AS (
  WITH events AS (
    SELECT
      CONCAT(CAST(hitid_high AS string), '_', CAST(hitid_low AS string)) AS hitid,
      SPLIT(event_list, ',') AS raw_event,
      product_type,
      sf_id,
      bo_id,
      date_time
    FROM (
      SELECT
        hitid_high, hitid_low, event_list, date_time, exclude_hit, hit_source,
        COALESCE(post_prop1, post_evar9) AS product_type,
        COALESCE(post_prop21, post_evar26) AS sf_id,
        COALESCE(post_prop2, post_evar4) AS bo_id
      FROM `trusted.adobe_hit_data`
      UNION ALL
      SELECT
        hitid_high, hitid_low, event_list, date_time, exclude_hit, hit_source,
        COALESCE(post_prop1, post_evar9) AS product_type,
        COALESCE(post_prop21, post_evar26) AS sf_id,
        COALESCE(post_prop2, post_evar4) AS bo_id
      FROM `trusted.adobe_hit_data_hist`
    )
    WHERE event_list IS NOT NULL
      AND exclude_hit = 0
      AND hit_source NOT IN (5, 8, 9)
  )
  SELECT hitid, product_type, sf_id, bo_id, date_time, flat_event
  FROM events
  CROSS JOIN UNNEST(events.raw_event) AS flat_event
)
SELECT
  CASE
    WHEN REGEXP_CONTAINS(b.value, r'\\s4$|\\s5$|\\s6$|\\s7$|\\s8$|2\\d|\\s14$|\\s15$|\\s16$|\\s17$|\\s18$|\\s19$|\\s30$|\\s31$')
      THEN 'recipe_branch_office_id'
    WHEN REGEXP_CONTAINS(b.value, r'\\s9$|\\s13$')
      THEN 'cost_portal_branch_office_id'
    WHEN REGEXP_CONTAINS(b.value, r'\\s10$|\\s11$|\\s12$')
      THEN 'claiming_sf_id'
    WHEN REGEXP_CONTAINS(b.value, r'\\s36$|\\s37$|\\s38$|\\s43$')
      THEN 'platform_sf_id'
    WHEN REGEXP_CONTAINS(b.value, r'\\s50$|\\s47$|\\s51$')
      THEN 'reservation_sf_id'
    ELSE 'cms_sf_id'
  END AS id_type,
  -1 AS id,
  CASE
    WHEN LOWER(product_type) IN ('recipe_tool', 'cost_portal') THEN bo_id
    ELSE sf_id
  END AS idchar,
  CASE
    WHEN b.value = 'Custom Event 1' THEN 'phone_click'
    WHEN b.value = 'Custom Event 2' THEN 'mail_click'
    WHEN b.value = 'Custom Event 3' THEN 'login'
    WHEN b.value = 'Custom Event 4' THEN 'recipe_tool_login'
    WHEN b.value = 'Custom Event 10' THEN 'claiming_activation'
    WHEN b.value = 'Custom Event 11' THEN 'claiming_deactivation'
    WHEN b.value = 'Custom Event 32' THEN 'reservation_tool_activation'
    WHEN b.value = 'Custom Event 33' THEN 'reservation_tool_deactivation'
    WHEN b.value = 'Custom Event 36' THEN 'platform_registration_start'
    WHEN b.value = 'Custom Event 37' THEN 'platform_registration_success'
    WHEN b.value = 'Custom Event 43' THEN 'platform_login'
    WHEN b.value = 'Custom Event 47' THEN 'reservation_mail_book_confirmation'
    WHEN b.value = 'Custom Event 50' THEN 'dashboard_reservation_configuration'
    WHEN b.value = 'Custom Event 57' THEN 'interactive_link_click'
    WHEN b.value = 'Custom Event 59' THEN 'interactive_form_submit'
    ELSE b.value
  END AS event,
  b.key AS derived_event_int,
  date_time AS derived_event_timestamp,
  b.value AS derived_event_string,
  0 AS days_after_creation,
  'Adobe datafeed' AS _sourcesystem,
  to_hex(md5(CONCAT(
    b.value, eventlist.hitid, b.value, eventlist.flat_event,
    CAST(date_time AS string), b.value
  ))) AS _rowhash
FROM eventlist
INNER JOIN `trusted.adobe_event` b
  ON eventlist.flat_event = CAST(b.key AS string)
WHERE b.value NOT LIKE '%eVar%'
  AND to_hex(md5(CONCAT(
    b.value, eventlist.hitid, b.value, eventlist.flat_event,
    CAST(date_time AS string), b.value
  ))) NOT IN (
    SELECT _rowhash FROM `{destination}`
    WHERE event = 'Adobe datafeed'
  )
""".strip()


def query_reservation_user_logindate(destination: str) -> str:
    """Login watermark: distinct (user, last_login_date) not yet hashed in."""
    rowhash = _rowhash_expr()
    return f"""
WITH t1 AS (
  SELECT
    'trusted.reservation_users.id' AS id_type,
    id,
    id_sk AS idchar,
    'RT user last login date change' AS event,
    1 AS derived_event_int,
    last_login_date AS derived_event_timestamp,
    CAST(NULL AS string) AS derived_event_string,
    TIMESTAMP_DIFF(
      timestamp_sub(last_login_date, INTERVAL 1 DAY), creation_date, DAY
    ) AS days_after_creation,
    _sourcesystem
  FROM `trusted.reservation_users`
  WHERE last_login_date > (
    SELECT MIN(_valid_from) FROM `trusted.reservation_users`
  )
  GROUP BY id, id_sk, last_login_date, creation_date, _sourcesystem
)
SELECT DISTINCT
  *,
  {rowhash} AS _rowhash
FROM t1
WHERE {rowhash} NOT IN (
  SELECT _rowhash FROM `{destination}`
  WHERE event = 'RT user last login date change'
)
""".strip()


def query_reservation_auto_arrivals(destination: str) -> str:
    """Classic SCD boolean LAG on reservation establishments."""
    rowhash = _rowhash_expr()
    return f"""
WITH t2 AS (
  WITH t1 AS (
    SELECT
      'trusted.reservation_establishments.id' AS id_type,
      id,
      id_sk AS idchar,
      'RT establishment automatic_arrivals_enabled change' AS event,
      1 AS derived_event_int,
      _valid_from AS derived_event_timestamp,
      CAST(automatic_arrivals_enabled AS string) AS derived_event_string,
      TIMESTAMP_DIFF(
        timestamp_sub(_valid_from, INTERVAL 1 DAY), creation_date, DAY
      ) AS days_after_creation,
      _sourcesystem,
      lag(automatic_arrivals_enabled) OVER (
        PARTITION BY id_sk ORDER BY _valid_from
      ) AS lag,
      ROW_NUMBER() OVER (PARTITION BY id_sk ORDER BY _valid_from) AS nth_record
    FROM `trusted.reservation_establishments`
  )
  SELECT
    id_type, id, idchar, event, derived_event_int, derived_event_timestamp,
    derived_event_string, days_after_creation, _sourcesystem,
    {rowhash} AS _rowhash
  FROM t1
  WHERE nth_record > 1
    AND CAST(lag AS string) != derived_event_string
)
SELECT * FROM t2
WHERE _rowhash NOT IN (
  SELECT _rowhash FROM `{destination}`
  WHERE event = 'RT establishment automatic_arrivals_enabled change'
)
""".strip()


def query_reservation_channels_change(destination: str) -> str:
    """Composite JSON payload + keep one change per entity per day."""
    rowhash = _rowhash_expr()
    return f"""
WITH t2 AS (
  WITH t1 AS (
    SELECT
      'trusted.reservation_establishments.id' AS id_type,
      id,
      id_sk AS idchar,
      'RT establishment reservation channels change' AS event,
      1 AS derived_event_int,
      _valid_from AS derived_event_timestamp,
      CONCAT(
        '{{"rwg_enabled": "', CAST(rwg_enabled AS string),
        '", "widget_enabled": "', CAST(widget_enabled AS string), '"}}'
      ) AS derived_event_string,
      TIMESTAMP_DIFF(
        timestamp_sub(_valid_from, INTERVAL 1 DAY), creation_date, DAY
      ) AS days_after_creation,
      _sourcesystem,
      lag(CONCAT(
        '{{"rwg_enabled": "', CAST(rwg_enabled AS string),
        '", "widget_enabled": "', CAST(widget_enabled AS string), '"}}'
      )) OVER (PARTITION BY id_sk ORDER BY _valid_from) AS lag,
      ROW_NUMBER() OVER (PARTITION BY id_sk ORDER BY _valid_from) AS nth_record
    FROM `trusted.reservation_establishments`
  )
  SELECT
    id_type, id, idchar, event, derived_event_int, derived_event_timestamp,
    derived_event_string, days_after_creation, _sourcesystem,
    {rowhash} AS _rowhash
  FROM t1
  WHERE nth_record > 1
    AND CAST(lag AS string) != derived_event_string
)
SELECT * EXCEPT (record_rank_per_day)
FROM (
  SELECT
    ROW_NUMBER() OVER (
      PARTITION BY id, EXTRACT(DATE FROM derived_event_timestamp)
      ORDER BY derived_event_timestamp DESC
    ) AS record_rank_per_day,
    *
  FROM (
    SELECT * FROM t2
    WHERE _rowhash NOT IN (
      SELECT _rowhash FROM `{destination}`
      WHERE event = 'RT establishment reservation channels change'
    )
  )
)
WHERE record_rank_per_day = 1
""".strip()
