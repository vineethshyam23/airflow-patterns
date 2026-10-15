"""
BigQuery builders for the Deepideas gap-ingredients export.

"Gap" here means: the buyer's establishment menu implies an ingredient
(via recipe mapping), but they have no wholesale purchase revenue for
articles mapped to that ingredient over the last year.

Grain is wholesale_id × ingredient_name × product_main_cat_id.
Emits _keyhash / _rowhash for the delta contract in delta_queries.py.

Distinct from pattern 21 (category-level anti-join). This feed keeps
ingredient grain and a thinner join path (menu→recipe→ingredient only;
no extracted-ingredient union).

Source (read-only):
  dags/horeca_digital/dana_deepideas_query.py  (GapIngredients class)

Sanitized vs production:
  - metro_id → wholesale_id
  - mge_main_cat_id → product_main_cat_id
  - analytical_mcc_* → analytical_wholesale_*
  - Vertex / stream project ids → dwh_project / foodgraph_* datasets
  - dwh_trusted.fg_ingredients → trusted.fg_ingredients
  - Active-buyer filter preserved; branch group bounds unchanged
"""

from __future__ import annotations

import logging

log = logging.getLogger(__name__)

PROJECT = "dwh_project"
REFINED = "refined"
TRUSTED = "trusted"
FOODGRAPH_PRE = "foodgraph_preprocessed"
COUNTRY = "DE"


def insert_today_query(
    project: str = PROJECT,
    refined: str = REFINED,
    trusted: str = TRUSTED,
    foodgraph_pre: str = FOODGRAPH_PRE,
    country: str = COUNTRY,
) -> str:
    """TRUNCATE-load staging.di_gap_ingredients_export_today."""
    query = f"""
WITH establishments AS (
  SELECT wholesale_id, establishment_id, branch_desc
  FROM `{project}.{refined}.all_establishments_{country}` est
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
  SELECT establishment_id, menu_item_id
  FROM `{project}.{refined}.all_menu_items`
  WHERE _valid_flag IS TRUE
    AND data_source = "all"
    AND iso_code = "{country}"
    AND menu_type <> "menu_recommender"
)
, m2r AS (
  SELECT menu_item_id, unique_recipe_id
  FROM `{project}.{foodgraph_pre}.menu_items_to_recipes`
  WHERE menu_item_iso_code = "{country}"
)
, r2i AS (
  SELECT unique_recipe_id, ing_id
  FROM `{project}.{foodgraph_pre}.recipes_to_ingredients`
  WHERE iso_code = "{country}"
)
, transactions AS (
  SELECT DISTINCT
    wholesale_id,
    ing_id,
    SUM(sale_money) AS revenue
  FROM `{project}.{refined}.analytical_wholesale_transactions_{country}`
  JOIN `{project}.{foodgraph_pre}.articles_to_ingredients` a2i
    USING (iso_code, art_no)
  WHERE date_of_day > DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)
  GROUP BY 1, 2
)
, establishment_need AS (
  SELECT DISTINCT
    wholesale_id,
    ing_id,
    ingredient_name,
    relevance
  FROM establishments
  JOIN menues USING (establishment_id)
  JOIN m2r USING (menu_item_id)
  JOIN r2i USING (unique_recipe_id)
  JOIN (
    SELECT ing_id, proper_name AS ingredient_name
    FROM `{project}.{foodgraph_pre}.ingredients_translations`
    WHERE iso_code = "{country}"
  ) USING (ing_id)
  JOIN `{project}.{foodgraph_pre}.ingredient_prioritization`
    USING (branch_desc, ing_id)
  WHERE iso_code = "{country}"
)
SELECT DISTINCT
  wholesale_id,
  est.ingredient_name AS ingredient_name,
  ing.product_main_cat_id AS product_main_cat_id,
  ROUND(relevance, 4) AS relevance,
  -- Production keyed _keyhash on customer id only (not ingredient).
  -- Keep that contract so delta / soft-close semantics match Composer.
  TO_HEX(MD5(CAST(wholesale_id AS STRING))) AS _keyhash,
  TO_HEX(MD5(CONCAT(
    IFNULL(CAST(wholesale_id AS STRING), ''), '|',
    IFNULL(CAST(ingredient_name AS STRING), ''), '|',
    IFNULL(CAST(product_main_cat_id AS STRING), ''), '|',
    IFNULL(CAST(ROUND(relevance, 4) AS STRING), '')
  ))) AS _rowhash,
  CURRENT_TIMESTAMP() AS _valid_from,
  TIMESTAMP('2099-12-31 23:59:59') AS _valid_until,
  TRUE AS _valid_flag
FROM establishment_need est
LEFT JOIN transactions USING (wholesale_id, ing_id)
LEFT JOIN `{project}.{trusted}.fg_ingredients` ing
  ON est.ing_id = ing.ing_id
WHERE revenue IS NULL
ORDER BY wholesale_id, relevance, 2
"""
    log.info("Retrieved query: insert_today_query (gap_ingredients)")
    return query


if __name__ == "__main__":
    sql = insert_today_query()
    print("chars", len(sql), "has_gap_filter", "revenue IS NULL" in sql)
