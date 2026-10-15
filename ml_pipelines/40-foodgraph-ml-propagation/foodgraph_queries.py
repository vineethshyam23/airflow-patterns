"""Food Graph query builders for ML propagation and ranked menu gaps.

Sanitized from production ``horeca_digital/foodgraph_queries.py``.
Only builders used by the Food Graph Composer DAG are kept.
Market-data extracts are pattern 17; analytics-zone builders belong to
the refined Food Graph DAG (not shipped here).
"""

from __future__ import annotations

VERTEX_PROJECT = "vertex_ml_project"
DWH_PROJECT = "dwh_project"

# Per-country wholesale CRM prefixes for enriching ranked gaps.
# Production used real store-system prefixes; keep the shape only.
WHOLESALE_LIST = {
    "DE": ["27601", "ger", "wholesale_cardholder"],
    "FR": ["25001", "fra", "wholesale_fra_cardholder"],
    "NL": ["52801", "ned", "wholesale_ned_cardholder"],
    "ES": ["72401", "esp", "wholesale_esp_cardholder"],
    "PL": ["61601", "pol", "wholesale_pol_cardholder"],
    "HR": ["19101", "cro", "wholesale_cro_cardholder"],
    "IT": ["38001", "ita", "wholesale_ita_cardholder"],
    "PT": ["62001", "por", "wholesale_prt_cardholder"],
}

ISOCODE_LIST = list(WHOLESALE_LIST.keys())
NON_WHOLESALE_MENU_GAPS_ACTIVE = ["ES"]


def gaps_unnested_query(fg_dataset: str) -> str:
    """Flatten ML gap predictions from a Vertex preprocessed dataset."""
    return f"""
SELECT
  iso_code,
  data_source,
  establishment_id,
  google_places_api_id,
  menu_type,
  prediction_id,
  model_version,
  normalized_ingredient,
  ingredient_name,
  menu_item_name,
  confidence_score,
  recipe_names,
  relevance,
  article_no,
  mikg_article_no,
  var_tu_key,
  var_type_desc,
  article_name,
  days_since_last_purchase,
  purchase_frequency_days,
  execution_date,
  created_at,
  prediction_type,
  source_valid_flag AS _valid_flag,
  CURRENT_TIMESTAMP() AS _create_ts,
  CAST(NULL AS TIMESTAMP) AS _update_ts,
  '' AS _job_name,
  0 AS _job_id,
  'Foodgraph' AS _sourcesystem,
  CAST(NULL AS STRING) AS _keyhash,
  CAST(NULL AS STRING) AS _rowhash,
  CAST(NULL AS TIMESTAMP) AS _valid_from,
  CAST('2099-12-31' AS TIMESTAMP) AS _valid_until
FROM `{VERTEX_PROJECT}.{fg_dataset}.gaps_unnested`
""".strip()


gold_ingredients_synonyms_query = f"""
SELECT DISTINCT
  ing_id,
  iso_code,
  synonym,
  CURRENT_TIMESTAMP() AS _create_ts,
  CAST(NULL AS TIMESTAMP) AS _update_ts,
  '' AS _job_name,
  ing.is_convenience,
  0 AS _job_id,
  'Foodgraph' AS _sourcesystem,
  TO_HEX(MD5(CAST(ing_id AS STRING))) AS _keyhash,
  TO_HEX(MD5(CONCAT(CAST(iso_code AS STRING), '|', CAST(synonym AS STRING)))) AS _rowhash
FROM `{VERTEX_PROJECT}.foodgraph_dev_preprocessed.ingredients_synonyms`
LEFT JOIN `{VERTEX_PROJECT}.foodgraph_dev_preprocessed.ingredients` AS ing
USING (ing_id)
""".strip()


gold_ingredient_ingredients_query = f"""
SELECT DISTINCT
  ing_id,
  image_url,
  parent_ing_id,
  mge_main_cat_id,
  mge_cat_id,
  mge_sub_cat_id,
  CURRENT_TIMESTAMP() AS _create_ts,
  is_convenience,
  CAST(NULL AS TIMESTAMP) AS _update_ts,
  '' AS _job_name,
  0 AS _job_id,
  'Foodgraph' AS _sourcesystem,
  TO_HEX(MD5(CAST(ing_id AS STRING))) AS _keyhash,
  TO_HEX(MD5(CONCAT(
    CAST(image_url AS STRING), '|',
    CAST(parent_ing_id AS STRING), '|',
    CAST(mge_main_cat_id AS STRING), '|',
    CAST(mge_cat_id AS STRING), '|',
    CAST(mge_sub_cat_id AS STRING)
  ))) AS _rowhash
FROM `{VERTEX_PROJECT}.foodgraph_dev_preprocessed.ingredients`
""".strip()


gold_ingredients_translation_query = f"""
SELECT DISTINCT
  ing_id,
  iso_code,
  translation_id,
  name,
  proper_name,
  CURRENT_TIMESTAMP() AS _create_ts,
  CAST(NULL AS TIMESTAMP) AS _update_ts,
  '' AS _job_name,
  0 AS _job_id,
  'Foodgraph' AS _sourcesystem,
  TO_HEX(MD5(CAST(ing_id AS STRING))) AS _keyhash,
  TO_HEX(MD5(CONCAT(
    CAST(iso_code AS STRING), '|',
    CAST(translation_id AS STRING), '|',
    CAST(name AS STRING), '|',
    CAST(proper_name AS STRING)
  ))) AS _rowhash
FROM `{VERTEX_PROJECT}.foodgraph_dev_preprocessed.ingredients_translations`
""".strip()


menu_items_drink_classification = f"""
SELECT *
FROM `{VERTEX_PROJECT}.foodgraph_dev_preprocessed.menu_items_drink_classification`
""".strip()


def rex_menu_gaps_ranked(iso_code: str) -> str:
    """Rank ingredient gaps and attach wholesale account / person keys.

    Score mixes own-brand, revenue, purchase frequency, and FAISS
    article-to-ingredient confidence. CRM account-identifier join is
    kept in shape with sanitized dataset names.
    """
    wholesale_prefix, account_table_prefix, cardholder_table = WHOLESALE_LIST[iso_code]
    return f"""
WITH articles AS (
  SELECT article_no, var_tu_key, ingredient_name, is_ownbrand,
         article_qty, article_unit, department_flag
  FROM `{DWH_PROJECT}.refined.analytical_wholesale_articles_{iso_code}`
),
art_revenue AS (
  SELECT DISTINCT
    article_no, var_tu_key, trans.branch_desc,
    COUNT(1) AS purchase_freq,
    SUM(sale_money) AS one_year_revenue,
    SUM(sale_qty) AS sale_qty
  FROM `{DWH_PROJECT}.refined.analytical_wholesale_transactions_{iso_code}` AS trans
  JOIN `{DWH_PROJECT}.refined.all_establishments_{iso_code}` AS estab
    ON trans.wholesale_id = estab.wholesale_id
   AND trans.date_of_day >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)
  GROUP BY 1, 2, 3
),
top_articles AS (
  SELECT DISTINCT
    ingredient_name, articles.article_no, ar.var_tu_key, branch_desc,
    article_qty, article_unit, one_year_revenue, purchase_freq, sale_qty,
    is_ownbrand, department_flag
  FROM art_revenue AS ar
  JOIN articles USING (article_no, var_tu_key)
  WHERE ingredient_name IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY ingredient_name, branch_desc
    ORDER BY is_ownbrand DESC, one_year_revenue DESC
  ) <= 10
),
faiss_data AS (
  SELECT * FROM `{VERTEX_PROJECT}.foodgraph_acc_preprocessed.articles_to_ingredients`
  WHERE iso_code = '{iso_code}'
),
features AS (
  SELECT
    t.*,
    CAST(t.is_ownbrand AS INT64) AS is_ownbrand_score,
    MIN(t.one_year_revenue) OVER (PARTITION BY t.ingredient_name, t.branch_desc) AS min_rev,
    MAX(t.one_year_revenue) OVER (PARTITION BY t.ingredient_name, t.branch_desc) AS max_rev,
    MIN(t.purchase_freq) OVER (PARTITION BY t.ingredient_name, t.branch_desc) AS min_freq,
    MAX(t.purchase_freq) OVER (PARTITION BY t.ingredient_name, t.branch_desc) AS max_freq
  FROM top_articles AS t
),
scores AS (
  SELECT
    f.*,
    CASE WHEN max_rev = min_rev THEN 1.0
         ELSE (one_year_revenue - min_rev) / (max_rev - min_rev) END AS revenue_score,
    CASE WHEN max_freq = min_freq THEN 1.0
         ELSE (purchase_freq - min_freq) / (max_freq - min_freq) END AS freq_score
  FROM features AS f
),
merged_data AS (
  SELECT
    s.*,
    COALESCE(fd.score, 0.1) AS faiss_score,
    fd.iso_code AS faiss_iso_code,
    fd.ing_id AS faiss_ing_id
  FROM scores AS s
  LEFT JOIN faiss_data AS fd ON s.article_no = fd.article_no
),
final_calc AS (
  SELECT
    *,
    (1 + (is_ownbrand_score + revenue_score + freq_score) / 3.0) / 2.0 * faiss_score AS final_score
  FROM merged_data
  WHERE faiss_iso_code IS NOT NULL
),
ingredients_articles_branch_desc_rank AS (
  SELECT
    ingredient_name, article_no, var_tu_key, one_year_revenue, branch_desc,
    department_flag, final_score,
    CAST(DENSE_RANK() OVER (
      PARTITION BY ingredient_name, branch_desc ORDER BY final_score DESC
    ) AS INT64) AS rank
  FROM final_calc
),
valid_wholesale_ids AS (
  SELECT DISTINCT establishment_id, wholesale_id, estab.branch_desc
  FROM `{DWH_PROJECT}.refined.all_establishments_{iso_code}` AS estab
  JOIN `{DWH_PROJECT}.refined.analytical_wholesale_customers_{iso_code}`
  USING (wholesale_id)
),
gaps AS (
  SELECT
    iso_code,
    wholesale_id,
    gaps.establishment_id,
    branch_desc,
    ANY_VALUE(menu_type) AS menu_type,
    ingredient_name,
    STRING_AGG(DISTINCT menu_item_name, ', ' ORDER BY menu_item_name) AS menu_item_name,
    MAX(confidence_score) AS confidence_score,
    MAX(relevance) AS relevance,
    ANY_VALUE(prediction_type) AS prediction_type,
    MAX(created_at) AS created_at
  FROM `{DWH_PROJECT}.trusted.fg_gaps_unnested_acc` AS gaps
  JOIN valid_wholesale_ids
    ON gaps.establishment_id = valid_wholesale_ids.establishment_id
  WHERE iso_code = '{iso_code}'
    AND prediction_type IN ('GAP', 'MATCH')
    AND wholesale_id IS NOT NULL
    AND branch_desc IS NOT NULL
  GROUP BY iso_code, wholesale_id, establishment_id, branch_desc, ingredient_name
),
ranked_gaps AS (
  SELECT *,
    RANK() OVER (PARTITION BY wholesale_id ORDER BY relevance DESC, one_year_revenue DESC) AS rank_
  FROM (
    SELECT DISTINCT
      wholesale_id, iso_code, establishment_id,
      ingredient_name AS ingredient, prediction_type AS type,
      menu_type, menu_item_name, relevance, branch_desc,
      article_no, var_tu_key, department_flag,
      CAST(CONCAT(article_no, LPAD(CAST(var_tu_key AS STRING), 6, '0')) AS INT64) AS product_key,
      one_year_revenue, created_at
    FROM ingredients_articles_branch_desc_rank AS ra
    JOIN gaps USING (ingredient_name, branch_desc)
    WHERE ra.rank = 1
  )
),
account_source AS (
  SELECT
    a.id AS account_id,
    account_identifiers,
    is_deleted,
    partner_ingestion_timestamp,
    ROW_NUMBER() OVER (PARTITION BY a.id ORDER BY partner_ingestion_timestamp DESC) AS rnk
  FROM `{DWH_PROJECT}.trusted_wholesale.{account_table_prefix}_partner_account` AS a
),
accounts AS (
  SELECT
    account_id,
    CAST(LTRIM(SUBSTR(data, 6, 11), '0') AS INT64) AS wholesale_customer_key,
    partner_ingestion_timestamp
  FROM account_source,
  UNNEST(account_identifiers) AS ai
  WHERE rnk = 1
    AND is_deleted = FALSE
    AND ai.type = 'WHOLESALE_CC'
),
cardholder_mapping AS (
  SELECT
    CAST(CONCAT(
      '{wholesale_prefix}',
      LPAD(CAST(ch.store_key AS STRING), 3, '0'),
      LPAD(CAST(ch.cust_no AS STRING), 8, '0')
    ) AS INT64) AS wholesale_id,
    a.account_id,
    CAST(NULL AS STRING) AS person_id,
    CAST(CONCAT(
      CAST(ch.store_key AS STRING),
      LPAD(CAST(ch.cust_no AS STRING), 8, '0'),
      LPAD(CAST(ch.auth_person_id AS STRING), 2, '0')
    ) AS INT64) AS wholesale_cardholder_key,
    a.wholesale_customer_key,
    CAST(CONCAT(
      '{wholesale_prefix}',
      LPAD(CAST(ch.store_key AS STRING), 3, '0'),
      LPAD(CAST(ch.cust_no AS STRING), 8, '0')
    ) AS INT64) AS unique_wholesale_id
  FROM accounts AS a
  JOIN `{DWH_PROJECT}.trusted_wholesale.{cardholder_table}` AS ch
    ON a.wholesale_customer_key = CAST(CONCAT(
         CAST(ch.store_key AS STRING),
         LPAD(CAST(ch.cust_no AS STRING), 8, '0')
       ) AS INT64)
)
SELECT
  wholesale_id,
  iso_code,
  establishment_id,
  ingredient,
  type,
  menu_type,
  relevance,
  branch_desc,
  article_no,
  var_tu_key,
  product_key,
  CAST(NULL AS STRING) AS article_name,
  SAFE_CAST(one_year_revenue AS FLOAT64) AS one_year_revenue,
  CURRENT_DATE() AS created_at,
  rank_,
  account_id,
  person_id,
  wholesale_cardholder_key,
  wholesale_customer_key,
  unique_wholesale_id
FROM ranked_gaps
JOIN cardholder_mapping USING (wholesale_id)
ORDER BY wholesale_id, relevance DESC, one_year_revenue DESC
""".strip()


def non_wholesale_menu_gaps(iso_code: str) -> str:
    """Menu gaps for establishments without a wholesale customer id."""
    return f"""
WITH independent_establishments AS (
  SELECT
    establishment_id, iso_code, establishment_name,
    postal_code, city, street_name, street_number, address,
    geo_lat, geo_long, google_places_id, phone, email, website,
    establishment_type, cuisine_type
  FROM `{DWH_PROJECT}.refined.all_establishments_{iso_code}`
  WHERE data_source = 'all'
    AND wholesale_id IS NULL
),
gaps AS (
  SELECT DISTINCT
    g.iso_code,
    g.establishment_id,
    g.ingredient_name AS ingredient,
    g.menu_type,
    g.menu_item_name,
    COALESCE(e.google_places_id, g.google_places_api_id) AS google_places_api_id
  FROM `{DWH_PROJECT}.trusted.fg_gaps_unnested_acc` AS g
  INNER JOIN independent_establishments AS e
    ON g.establishment_id = e.establishment_id
   AND g.iso_code = e.iso_code
  WHERE g.prediction_type IN ('GAP', 'MATCH')
    AND IFNULL(g._valid_flag, TRUE) IS TRUE
)
SELECT DISTINCT
  e.establishment_id,
  g.iso_code,
  e.establishment_name,
  e.postal_code,
  e.city,
  e.street_name,
  e.street_number,
  e.address,
  e.geo_lat,
  e.geo_long,
  COALESCE(e.google_places_id, g.google_places_api_id) AS google_places_id,
  e.phone,
  e.email,
  e.website,
  e.establishment_type,
  e.cuisine_type,
  g.menu_type,
  g.menu_item_name,
  g.ingredient,
  CURRENT_DATE() AS created_at
FROM gaps AS g
INNER JOIN independent_establishments AS e
  ON g.establishment_id = e.establishment_id
 AND g.iso_code = e.iso_code
ORDER BY establishment_id, menu_item_name, ingredient
""".strip()


if __name__ == "__main__":
    print(gaps_unnested_query("foodgraph_dev_preprocessed")[:180])
    print("---")
    print(rex_menu_gaps_ranked("DE")[:180])
