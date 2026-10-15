"""
BigQuery builders for the establishment-attribute Deepideas export.

Builds the "today" staging snapshot: active wholesale buyers joined to
geo densities, ratings, menu mix, digitalisation, and market-area
stats. One row per wholesale_id (ROW_NUMBER tie-break). Emits
_keyhash / _rowhash for the delta contract in delta_queries.py.

Source (read-only):
  dags/horeca_digital/dana_deepideas_query.py  (Establishment class)

Sanitized vs production:
  - metro_id → wholesale_id
  - analytical_mcc_* / mcc_geo_* → analytical_wholesale_* / wholesale_geo_*
  - mcc_distance_* columns → store_distance_*
  - Dataset prefixes generalized (refined / trusted / discovery)
  - Market-specific rating vendor names kept as generic rating_* slots
"""

from __future__ import annotations

import logging

log = logging.getLogger(__name__)

PROJECT = "dwh_project"
REFINED = "refined"
TRUSTED = "trusted"
TRUSTED_VIEWS = "trusted_views"
DISCOVERY = "discovery"
COUNTRY = "DE"


def insert_today_query(
    project: str = PROJECT,
    refined: str = REFINED,
    trusted: str = TRUSTED,
    trusted_views: str = TRUSTED_VIEWS,
    discovery: str = DISCOVERY,
    country: str = COUNTRY,
) -> str:
    """TRUNCATE-load staging.di_establishment_export_today."""
    query = f"""
WITH active_buying_customer AS (
  SELECT DISTINCT wholesale_id
  FROM `{project}.{refined}.analytical_wholesale_customers_{country}`
  WHERE last_transaction >= DATE_ADD(CURRENT_DATE(), INTERVAL -1 YEAR)
    AND status_cd = 1
    AND branch_main_group_id != 15
    AND branch_main_group_id <= 19
    AND is_deleted = 0
)
, est_rating AS (
  SELECT DISTINCT
    wholesale_id,
    MAX(CAST(rating_a AS FLOAT64)) AS rating_a,
    MAX(CAST(rating_b AS FLOAT64)) AS rating_b,
    MAX(CAST(rating_google AS FLOAT64)) AS rating_google,
    MAX(CAST(rating_social AS FLOAT64)) AS rating_social,
    MAX(IF(rating_a IS NOT NULL, 1, 0)) AS has_rating_a,
    MAX(IF(rating_b IS NOT NULL, 1, 0)) AS has_rating_b,
    MAX(IF(rating_google IS NOT NULL, 1, 0)) AS has_rating_g,
    MAX(IF(rating_social IS NOT NULL, 1, 0)) AS has_rating_s
  FROM `{project}.{refined}.all_establishments_{country}`
  WHERE data_source = "all"
  GROUP BY 1
)
, menu AS (
  SELECT DISTINCT
    establishment_id,
    ROUND(1 - SUM(flag_drink) / COUNT(DISTINCT IFNULL(menu_item_id, menu_item_name)), 4)
      AS food_proportion,
    ROUND(
      AVG(IF(flag_drink = 0 AND item_price IS NOT NULL, CAST(item_price AS FLOAT64), NULL)),
      2
    ) AS avg_food_price
  FROM `{project}.{refined}.all_menu_items`
  WHERE data_source = 'all'
    AND iso_code = '{country}'
    AND flag_drink IS NOT NULL
    AND _valid_flag = TRUE
  GROUP BY 1
)
, output AS (
  SELECT DISTINCT
    b.wholesale_id,
    all_est.price_range,
    ROUND(
      SAFE_DIVIDE(
        SUM(
          IFNULL(r.rating_a, 0) + IFNULL(r.rating_b, 0)
          + IFNULL(r.rating_google, 0) + IFNULL(r.rating_social, 0)
        ),
        SUM(r.has_rating_a + r.has_rating_b + r.has_rating_g + r.has_rating_s)
      ),
      2
    ) AS popularity_rate,
    ROUND(
      IFNULL(all_est.store_distance_air_km, geo.store_distance_air_km), 3
    ) AS store_distance_air_km,
    ROUND(
      IFNULL(all_est.store_distance_km, geo.store_distance_km), 3
    ) AS store_distance_km,
    IFNULL(all_est.store_distance_minutes, geo.store_distance_minutes)
      AS store_distance_minutes,
    IFNULL(all_est.competitor_density, geo.competitor_density) AS competitor_density,
    all_est.has_online_reservation,
    all_est.has_delivery_takeaway,
    IFNULL(all_est.poi_density, geo.poi_density) AS poi_density,
    all_est.cuisine_competitor_density,
    IFNULL(all_est.discounter_density, IFNULL(geo.discounter_density, 0))
      AS discounter_density,
    IFNULL(all_est.supermarket_density, IFNULL(geo.supermarket_density, 0))
      AS supermarket_density,
    IFNULL(all_est.cash_carry_density, IFNULL(geo.cash_carry_density, 0))
      AS cash_carry_density,
    all_est.establishment_type,
    all_est.cuisine_type,
    di.digitalisation_index,
    mbi.zip_community_type,
    mbi.purchasing_power_person,
    mbi.population_density,
    m.food_proportion,
    m.avg_food_price,
    ROW_NUMBER() OVER (
      PARTITION BY b.wholesale_id
      ORDER BY
        all_est.price_range DESC,
        IFNULL(all_est.store_distance_air_km, geo.store_distance_air_km) DESC,
        IFNULL(all_est.store_distance_km, geo.store_distance_km) DESC,
        IFNULL(all_est.store_distance_minutes, geo.store_distance_minutes) DESC,
        all_est.competitor_density DESC,
        all_est.has_online_reservation DESC,
        all_est.has_delivery_takeaway DESC,
        m.food_proportion DESC,
        m.avg_food_price DESC,
        IFNULL(all_est.poi_density, geo.poi_density) DESC,
        all_est.cuisine_competitor_density DESC,
        di.digitalisation_index DESC
    ) AS rn
  FROM active_buying_customer b
  LEFT JOIN `{project}.{refined}.all_establishments_{country}` all_est
    ON all_est.wholesale_id = b.wholesale_id
  LEFT JOIN `{project}.{trusted_views}.digitalisation_establishments` di
    ON CAST(all_est.md_establishment_id AS INT64) = di.md_establishment_id
  LEFT JOIN est_rating r
    ON all_est.wholesale_id = r.wholesale_id
  LEFT JOIN (
    SELECT
      zip_community_type,
      ROUND(purchasing_power_person, 4) AS purchasing_power_person,
      ROUND(population_density, 4) AS population_density,
      postal_code,
      country
    FROM `{project}.{trusted}.market_area_stats`
  ) mbi
    ON all_est.postal_code = mbi.postal_code AND mbi.country = "{country}"
  LEFT JOIN menu m
    ON all_est.md_establishment_id = m.establishment_id
  LEFT JOIN `{project}.{discovery}.wholesale_geo_{country}` geo
    ON b.wholesale_id = geo.wholesale_id
  WHERE data_source = "all"
  GROUP BY
    1, 2, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22,
    all_est.store_distance_air_km, geo.store_distance_air_km,
    all_est.store_distance_km, geo.store_distance_km,
    all_est.store_distance_minutes, geo.store_distance_minutes,
    all_est.poi_density, geo.poi_density,
    all_est.discounter_density, geo.discounter_density,
    all_est.supermarket_density, geo.supermarket_density,
    all_est.cash_carry_density, geo.cash_carry_density,
    all_est.competitor_density, geo.competitor_density
)
SELECT
  wholesale_id,
  IFNULL(price_range, '') AS price_range,
  IFNULL(ROUND(popularity_rate, 2), 0) AS popularity_rate,
  IFNULL(ROUND(store_distance_air_km, 2), 0) AS store_distance_air_km,
  IFNULL(ROUND(store_distance_km, 2), 0) AS store_distance_km,
  IFNULL(store_distance_minutes, 0) AS store_distance_minutes,
  IFNULL(competitor_density, 0) AS competitor_density,
  IFNULL(CAST(has_online_reservation AS STRING), '') AS has_online_reservation,
  IFNULL(CAST(has_delivery_takeaway AS STRING), '') AS has_delivery_takeaway,
  IFNULL(poi_density, 0) AS poi_density,
  IFNULL(cuisine_competitor_density, 0) AS cuisine_competitor_density,
  IFNULL(discounter_density, 0) AS discounter_density,
  IFNULL(supermarket_density, 0) AS supermarket_density,
  IFNULL(cash_carry_density, 0) AS cash_carry_density,
  IFNULL(establishment_type, '') AS establishment_type,
  IFNULL(cuisine_type, '') AS cuisine_type,
  IFNULL(digitalisation_index, 0) AS digitalisation_index,
  IFNULL(zip_community_type, '') AS zip_community_type,
  IFNULL(ROUND(purchasing_power_person, 4), 0) AS purchasing_power_person,
  IFNULL(ROUND(population_density, 4), 0) AS population_density,
  IFNULL(ROUND(food_proportion, 4), 0) AS food_proportion,
  IFNULL(ROUND(avg_food_price, 2), 0) AS avg_food_price,
  TO_HEX(MD5(CAST(wholesale_id AS STRING))) AS _keyhash,
  TO_HEX(MD5(CONCAT(
    IFNULL(CAST(wholesale_id AS STRING), ''), '|',
    IFNULL(CAST(price_range AS STRING), ''), '|',
    IFNULL(CAST(popularity_rate AS STRING), ''), '|',
    IFNULL(CAST(store_distance_air_km AS STRING), ''), '|',
    IFNULL(CAST(store_distance_km AS STRING), ''), '|',
    IFNULL(CAST(store_distance_minutes AS STRING), ''), '|',
    IFNULL(CAST(competitor_density AS STRING), ''), '|',
    IFNULL(CAST(has_online_reservation AS STRING), ''), '|',
    IFNULL(CAST(has_delivery_takeaway AS STRING), ''), '|',
    IFNULL(CAST(poi_density AS STRING), ''), '|',
    IFNULL(CAST(cuisine_competitor_density AS STRING), ''), '|',
    IFNULL(CAST(discounter_density AS STRING), ''), '|',
    IFNULL(CAST(supermarket_density AS STRING), ''), '|',
    IFNULL(CAST(cash_carry_density AS STRING), ''), '|',
    IFNULL(CAST(digitalisation_index AS STRING), ''), '|',
    IFNULL(CAST(purchasing_power_person AS STRING), ''), '|',
    IFNULL(CAST(population_density AS STRING), ''), '|',
    IFNULL(CAST(food_proportion AS STRING), ''), '|',
    IFNULL(CAST(establishment_type AS STRING), ''), '|',
    IFNULL(CAST(cuisine_type AS STRING), ''), '|',
    IFNULL(CAST(avg_food_price AS STRING), '')
  ))) AS _rowhash,
  CURRENT_TIMESTAMP AS _valid_from,
  TIMESTAMP('2099-12-31 23:59:59') AS _valid_until,
  TRUE AS _valid_flag
FROM output
WHERE rn = 1
"""
    log.info("Retrieved query: insert_today_query")
    return query


if __name__ == "__main__":
    sql = insert_today_query()
    print("chars", len(sql), "has_wholesale", "wholesale_id" in sql)
