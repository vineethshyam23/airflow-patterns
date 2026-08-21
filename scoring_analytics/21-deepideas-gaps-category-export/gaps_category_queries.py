"""
BigQuery builders for the Deepideas main-category gaps export.

"Gap" here means: the buyer's establishment menu implies a product
main category (via ingredient mapping), but they have no wholesale
purchase revenue in that category over the last year.

Grain is wholesale_id × product_main_cat_id. Emits _keyhash /
_rowhash for the delta contract in delta_queries.py.

Source (read-only):
  dags/horeca_digital/dana_deepideas_query.py  (GapsCategory class)

Sanitized vs production:
  - metro_id → wholesale_id
  - mge_main_cat_* → product_main_cat_*
  - analytical_mcc_* → analytical_wholesale_*
  - Vertex / stream project ids → dwh_project / foodgraph_* datasets
  - Fixed production typo (double-dot dataset path) to a single path
  - Active-buyer filter preserved; branch group bounds unchanged
"""

from __future__ import annotations

import logging

log = logging.getLogger(__name__)

PROJECT = "dwh_project"
REFINED = "refined"
FOODGRAPH = "foodgraph"
FOODGRAPH_PRE = "foodgraph_preprocessed"
COUNTRY = "DE"


def insert_today_query(
    project: str = PROJECT,
    refined: str = REFINED,
    foodgraph: str = FOODGRAPH,
    foodgraph_pre: str = FOODGRAPH_PRE,
    country: str = COUNTRY,
) -> str:
    """TRUNCATE-load staging.di_gaps_category_export_today."""
    query = f"""
WITH establishments AS (
  SELECT DISTINCT
    iso_code,
    wholesale_id,
    establishment_id,
    md_establishment_id,
    branch_desc
  FROM `{project}.{foodgraph}.all_establishments_{country}` est
  WHERE est.data_source = "all"
    AND wholesale_id IN (
      SELECT DISTINCT wholesale_id
      FROM `{project}.{refined}.analytical_wholesale_customers_{country}`
      WHERE last_transaction >= DATE_ADD(CURRENT_DATE(), INTERVAL -1 YEAR)
        AND status_cd = 1
        AND branch_main_group_id != 15
        AND branch_main_group_id <= 19
        AND is_deleted = 0
    )
)
, menues AS (
  SELECT DISTINCT
    iso_code,
    establishment_id AS md_establishment_id,
    menu_item_id
  FROM `{project}.{foodgraph}.all_menu_items`
  WHERE _valid_flag IS TRUE
    AND data_source = "all"
    AND iso_code = "{country}"
    AND menu_type <> "menu_recommender"
)
, m2r AS (
  SELECT
    m2r.menu_item_iso_code,
    m2r.menu_item_id,
    IFNULL(m2r.unique_recipe_id, "") AS unique_recipe_id,
    m2r.recipe_iso_code
  FROM `{project}.{foodgraph_pre}.menu_items_to_recipes` m2r
)
, mi_extracted AS (
  SELECT
    iso_code,
    menu_item_id,
    ARRAY(
      SELECT CAST(num AS INT64)
      FROM UNNEST(SPLIT(normalised_ingredients, ';')) AS num
    ) AS normalised_ingredients
  FROM `{project}.{foodgraph_pre}.menu_items_extracted_ingredients`
)
, r2i AS (
  SELECT
    r2i.iso_code,
    r2i.unique_recipe_id,
    ARRAY_AGG(r2i.ing_id) AS normalised_ingredients
  FROM `{project}.{foodgraph_pre}.recipes_to_ingredients` r2i
  GROUP BY 1, 2
)
, m2i AS (
  SELECT
    menu_item_iso_code,
    menu_item_id,
    unique_recipe_id,
    recipe_iso_code,
    ing_id
  FROM (
    SELECT
      m2r.menu_item_iso_code,
      m2r.menu_item_id,
      m2r.unique_recipe_id,
      m2r.recipe_iso_code,
      ARRAY(
        SELECT DISTINCT x
        FROM UNNEST(
          IFNULL(mi_extracted.normalised_ingredients, [])
          || IFNULL(r2i.normalised_ingredients, [])
        ) AS x
        ORDER BY x
      ) AS ingredients
    FROM m2r
    LEFT JOIN mi_extracted
      ON m2r.menu_item_iso_code = mi_extracted.iso_code
     AND m2r.menu_item_id = mi_extracted.menu_item_id
    LEFT JOIN r2i
      ON m2r.recipe_iso_code = r2i.iso_code
     AND m2r.unique_recipe_id = r2i.unique_recipe_id
  )
  CROSS JOIN UNNEST(ingredients) AS ing_id
)
, transactions AS (
  SELECT
    wholesale_id,
    product_main_cat_id,
    SUM(sale_money) AS revenue
  FROM `{project}.{refined}.analytical_wholesale_transactions_{country}`
  JOIN `{project}.{foodgraph_pre}.articles_to_ingredients` a2i
    USING (iso_code, art_no)
  JOIN `{project}.{refined}.analytical_wholesale_articles_{country}`
    USING (art_no)
  WHERE date_of_day > DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)
  GROUP BY 1, 2
)
, establishment_need AS (
  SELECT
    wholesale_id,
    product_main_cat_desc,
    product_main_cat_id,
    ROUND(AVG(relevance), 2) AS avg_relevance
  FROM establishments
  JOIN menues USING (iso_code, md_establishment_id)
  JOIN m2i
    ON menues.iso_code = m2i.menu_item_iso_code
   AND menues.menu_item_id = m2i.menu_item_id
  JOIN (
    SELECT
      ing_id,
      SAFE_CAST(product_main_cat_id AS INT64) AS product_main_cat_id
    FROM `{project}.{foodgraph_pre}.ingredients`
  ) USING (ing_id)
  JOIN (
    SELECT product_main_cat_id, product_main_cat_desc
    FROM `{project}.{refined}.analytical_wholesale_articles_{country}`
  ) USING (product_main_cat_id)
  JOIN `{project}.{foodgraph_pre}.ingredient_prioritization`
    USING (branch_desc, ing_id)
  GROUP BY wholesale_id, product_main_cat_desc, product_main_cat_id
)
SELECT DISTINCT
  wholesale_id,
  product_main_cat_desc,
  product_main_cat_id,
  avg_relevance,
  -- Production keyed _keyhash on customer id only (not category).
  -- Keep that contract so delta / soft-close semantics match Composer.
  TO_HEX(MD5(CAST(wholesale_id AS STRING))) AS _keyhash,
  TO_HEX(MD5(CONCAT(
    IFNULL(CAST(wholesale_id AS STRING), ''), '|',
    IFNULL(CAST(product_main_cat_desc AS STRING), ''), '|',
    IFNULL(CAST(product_main_cat_id AS STRING), ''), '|',
    IFNULL(CAST(avg_relevance AS STRING), '')
  ))) AS _rowhash,
  CURRENT_TIMESTAMP() AS _valid_from,
  TIMESTAMP('2099-12-31 23:59:59') AS _valid_until,
  TRUE AS _valid_flag
FROM establishment_need
LEFT JOIN transactions USING (wholesale_id, product_main_cat_id)
WHERE revenue IS NULL
ORDER BY avg_relevance DESC, wholesale_id
"""
    log.info("Retrieved query: insert_today_query (gaps_category)")
    return query


if __name__ == "__main__":
    sql = insert_today_query()
    print("chars", len(sql), "has_gap_filter", "revenue IS NULL" in sql)
