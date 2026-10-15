"""
Hash-delta helpers for the establishment-attribute event export.

Today / yesterday staging tables hold the last successful export
snapshot. New or changed _keyhash/_rowhash pairs become the Avro
payload; yesterday rows that no longer match are soft-closed.

Source (read-only):
  dags/horeca_digital/dana_deepideas_query.py  (Establishment class)
  dags/etl_dana_deep_ideas_export.py          (wiring pattern)
"""

from __future__ import annotations

import logging

log = logging.getLogger(__name__)

TODAY = "staging.di_establishment_export_today"
YESTERDAY = "staging.di_establishment_export_yesterday"


def send_data_query() -> str:
    """Delta rows to Avro-encode and POST."""
    query = f"""
SELECT
  wholesale_id,
  CAST(price_range AS STRING) AS price_range,
  CAST(popularity_rate AS STRING) AS popularity_rate,
  CAST(store_distance_air_km AS STRING) AS store_distance_air_km,
  CAST(store_distance_km AS STRING) AS store_distance_km,
  CAST(store_distance_minutes AS STRING) AS store_distance_minutes,
  CAST(competitor_density AS INT64) AS competitor_density,
  CAST(has_online_reservation AS STRING) AS has_online_reservation,
  CAST(has_delivery_takeaway AS STRING) AS has_delivery_takeaway,
  CAST(poi_density AS INT64) AS poi_density,
  CAST(cuisine_competitor_density AS INT64) AS cuisine_competitor_density,
  CAST(discounter_density AS STRING) AS discounter_density,
  CAST(supermarket_density AS STRING) AS supermarket_density,
  CAST(cash_carry_density AS STRING) AS cash_carry_density,
  CAST(establishment_type AS STRING) AS establishment_type,
  CAST(cuisine_type AS STRING) AS cuisine_type,
  CAST(digitalisation_index AS INT64) AS digitalisation_index,
  CAST(zip_community_type AS STRING) AS zip_community_type,
  CAST(purchasing_power_person AS STRING) AS purchasing_power_person,
  CAST(population_density AS STRING) AS population_density,
  CAST(food_proportion AS STRING) AS food_proportion,
  CAST(avg_food_price AS STRING) AS avg_food_price
FROM `{TODAY}`
WHERE
  _keyhash NOT IN (SELECT _keyhash FROM `{YESTERDAY}`)
  OR (
    _keyhash IN (SELECT _keyhash FROM `{YESTERDAY}`)
    AND _rowhash NOT IN (SELECT _rowhash FROM `{YESTERDAY}`)
  )
"""
    log.info("Retrieved query: send_data_query")
    return query


def copy_yesterday_query() -> str:
    """Append today's delta onto the yesterday snapshot table."""
    query = f"""
SELECT * FROM `{TODAY}`
WHERE
  _keyhash NOT IN (SELECT _keyhash FROM `{YESTERDAY}`)
  OR (
    _keyhash IN (SELECT _keyhash FROM `{YESTERDAY}`)
    AND _rowhash NOT IN (SELECT _rowhash FROM `{YESTERDAY}`)
  )
"""
    log.info("Retrieved query: copy_yesterday_query")
    return query


def update_yesterday_query() -> str:
    """Soft-close yesterday rows superseded by today's hash change."""
    query = f"""
UPDATE `{YESTERDAY}`
SET
  _valid_until = TIMESTAMP(
    FORMAT_TIMESTAMP(
      "%Y-%m-%d 23:59:59",
      TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
    )
  ),
  _valid_flag = FALSE
WHERE
  _valid_flag = TRUE
  AND _keyhash IN (SELECT _keyhash FROM `{TODAY}`)
  AND CONCAT(_keyhash, _rowhash) NOT IN (
    SELECT CONCAT(_keyhash, _rowhash) FROM `{TODAY}`
  )
"""
    log.info("Retrieved query: update_yesterday_query")
    return query


if __name__ == "__main__":
    for name, fn in (
        ("send", send_data_query),
        ("copy", copy_yesterday_query),
        ("update", update_yesterday_query),
    ):
        sql = fn()
        print(name, "chars", len(sql), "has_today", TODAY in sql)
