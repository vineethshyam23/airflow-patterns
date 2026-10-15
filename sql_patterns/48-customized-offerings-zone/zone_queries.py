"""Offer Tool zone query builders (sanitized).

Used by ``dag_customized_offerings_zone.py`` to WRITE_TRUNCATE
warehouse refined / trusted tables into per-stage Offer Tool GCP
projects. Distinct from pattern 27 (Cloud SQL SCD ingest) — this
module is the *consumption zone* contract for the field-sales tool.

Source (read-only): dags/horeca_digital/customized_offering_queries.py
"""

from __future__ import annotations


# Default warehouse project id used inside SQL builders. Override in tests
# by monkeypatching ``DWH_PROJECT``.
DWH_PROJECT = "dwh_project"


def _dwh() -> str:
    return DWH_PROJECT

def fg_gaps_unnested_query(env) -> str:
    query = f"""
    with 
fg as (
select distinct 
iso_code,
data_source,
establishment_id,
google_places_api_id,
prediction_id,
prediction_type,
model_version,
execution_date,
created_at,
_valid_flag source_valid_flag,
_create_ts,
_update_ts,
_job_name,
_job_id,
_sourcesystem,
_keyhash,
_rowhash,
_valid_from,
_valid_until,
_valid_flag,
APPROX_TOP_COUNT(menu_type, 1)[OFFSET(0)].value AS menu_type,
 from `{DWH_PROJECT}.trusted.fg_gaps_unnested_{env}`
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20
#   where 
#    menu_type is not null
),
ing as (
SELECT
  iso_code,
  establishment_id,
  ingredient_name,
  relevance,
  ARRAY_AGG(STRUCT( menu_item_name,
      menu_type,
      confidence_score,
      -- TO_JSON_STRING(
        recipe_names
      -- )
       AS recipe_names )) AS menu_item_names
FROM
  `{DWH_PROJECT}.trusted.fg_gaps_unnested_{env}`
WHERE
  prediction_type = 'GAP'
GROUP BY
  iso_code,
  establishment_id,
  ingredient_name,
  relevance
)
,
GAP as (
select iso_code,
        establishment_id,
        'GAP' AS prediction_type,
        TO_JSON_STRING(
        ARRAY_AGG(
            STRUCT(
                ingredient_name AS normalized_ingredient,
                [STRUCT(
                    ingredient_name AS ingredient_name,
                    menu_item_names,
                    ROUND(relevance, 2) AS relevance
                )] AS ingredients
            )
        )
        ) AS gap_ingredients from ing

        GROUP BY
  iso_code,
  establishment_id
),
 
art_M as (
SELECT
  iso_code,
        establishment_id,
        ingredient_name,
        relevance,

        ARRAY_AGG(STRUCT(
        art_no,
        mikg_art_no,
        var_tu_key,
        var_type_desc,

        art_name AS art_name,
        days_since_last_purchase AS days_since_last_purchase,
        NULL AS purchase_frequency_days
        )) AS articles,
        ARRAY_AGG(STRUCT( menu_item_name,
      menu_type,
      confidence_score,
      TO_JSON_STRING(recipe_names) AS recipe_names )) AS menu_item_names
FROM
  `{DWH_PROJECT}.trusted.fg_gaps_unnested_{env}`
WHERE
   prediction_type = 'MATCH'
GROUP BY
  iso_code,
  establishment_id,
  ingredient_name,
  relevance
),

MATCH as (

select iso_code,
        establishment_id,
        'MATCH' AS prediction_type,
        TO_JSON_STRING(
        ARRAY_AGG(
            STRUCT(
                ingredient_name AS normalized_ingredient,
                [STRUCT(
                    ingredient_name AS ingredient_name,
                    menu_item_names,
                    articles,
                    ROUND(relevance, 2) AS relevance
                )] AS ingredients
            )
        )
        ) AS gap_ingredients from art_M

        GROUP BY
  iso_code,
  establishment_id
),

art_NM as (
SELECT
  iso_code,
        establishment_id,
        ingredient_name,
        relevance,

        ARRAY_AGG(STRUCT(
        art_no,
        mikg_art_no,
        var_tu_key,
        var_type_desc,

        art_name AS art_name,
        days_since_last_purchase AS days_since_last_purchase,
        NULL AS purchase_frequency_days
        )) AS articles,
        ARRAY_AGG(STRUCT( menu_item_name,
      menu_type,
      confidence_score,
      TO_JSON_STRING(recipe_names) AS recipe_names )) AS menu_item_names
FROM
  `{DWH_PROJECT}.trusted.fg_gaps_unnested_{env}`
WHERE

  prediction_type = 'NOT_PREDICTED'
GROUP BY
  iso_code,
  establishment_id,
  ingredient_name,
  relevance
),
NOT_PREDICTED as 
(
select iso_code,
        establishment_id,
        'NOT_PREDICTED' AS prediction_type,
        TO_JSON_STRING(
          ARRAY_AGG(
              STRUCT(ingredient_name AS normalized_ingredient,
                  [STRUCT(articles,
                   ROUND(relevance, 2) AS relevance
                  )] AS ingredients)
              )
        ) AS gap_ingredients from art_NM

        GROUP BY
  iso_code,
  establishment_id
)

select 
fg.iso_code,
data_source,
fg.establishment_id,
google_places_api_id,
menu_type,
prediction_id,
model_version,
IFNULL(gap_ingredients, '[]') AS gap_ingredients,
execution_date,
created_at,
'GAP' AS prediction_type,
source_valid_flag,
_create_ts,
_update_ts,
_job_name,
_job_id,
_sourcesystem,
_keyhash,
_rowhash,
_valid_from,
_valid_until,
_valid_flag,
 from fg join GAP using(iso_code,establishment_id,prediction_type)
union DISTINCT
select 
fg.iso_code,
data_source,
fg.establishment_id,
google_places_api_id,
menu_type,
prediction_id,
model_version,
IFNULL(gap_ingredients, '[]') AS gap_ingredients,
execution_date,
created_at,
'MATCH' AS prediction_type,
source_valid_flag,
_create_ts,
_update_ts,
_job_name,
_job_id,
_sourcesystem,
_keyhash,
_rowhash,
_valid_from,
_valid_until,
_valid_flag,
 from fg join MATCH using(iso_code,establishment_id,prediction_type)
union DISTINCT
select 
fg.iso_code,
data_source,
fg.establishment_id,
google_places_api_id,
null as menu_type,
prediction_id,
model_version,
IFNULL(gap_ingredients, '[]') AS gap_ingredients,
execution_date,
created_at,
'NOT_PREDICTED' AS prediction_type,
source_valid_flag,
_create_ts,
_update_ts,
_job_name,
_job_id,
_sourcesystem,
_keyhash,
_rowhash,
_valid_from,
_valid_until,
_valid_flag,
 from fg join NOT_PREDICTED using(iso_code,establishment_id,prediction_type)
--  where menu_type is not null

    """
    return query 


def article_recommendation_branch_query(env) -> str:
    query = f"""
    WITH article_data AS (
  SELECT DISTINCT
    iso_code,
    art_no,
    mikg_art_no,
    art_name,
    ingredient_name,
    mge_main_cat_desc,
    mge_cat_desc,
    mge_sub_cat_desc,
    is_ownbrand,
    department_flag
  FROM `{DWH_PROJECT}.refined.analytical_wholesale_articles_*`
),
purchase_data AS (
  SELECT
    iso_code,
    wholesale_id,
    branch_desc,
    art_no,
    mikg_art_no,
    MAX(date_of_day) AS last_purchase_date,
    -- Aggregation over 1 year
    COUNT(1) AS customer_purchase_freq,
    SUM(sale_money) AS customer_total_revenue,
    SUM(sale_qty) AS customer_total_quantity,
    AVG(sale_money/IF(sale_qty = 0, 1, sale_qty)/(tunit_qty/min_tunit_qty)) AS avg_cust_article_price,
    -- Aggregation over 1 month
    COUNT(CASE WHEN date_of_day > DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH) THEN 1 ELSE 0 END) AS customer_purchase_freq_1m,
    SUM(CASE WHEN date_of_day > DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH) THEN sale_money ELSE 0 END) AS customer_total_revenue_1m,
    SUM(CASE WHEN date_of_day > DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH) THEN sale_qty ELSE 0 END) AS customer_total_quantity_1m,
    AVG(CASE WHEN date_of_day > DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH) THEN sale_money/IF(sale_qty = 0, 1, sale_qty)/(tunit_qty/min_tunit_qty) ELSE NULL END) AS avg_cust_article_price_1m
  FROM (
    SELECT
      iso_code,
      wholesale_id,
      branch_desc,
      art_no,
      mikg_art_no,
      sale_money,
      sale_qty,
      tunit_qty,
      date_of_day,
      MIN(tunit_qty) OVER(PARTITION BY iso_code, art_no) AS min_tunit_qty
    FROM `{DWH_PROJECT}.refined.analytical_wholesale_transactions_*`
    WHERE date_of_day > DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)
  )
  GROUP BY iso_code, wholesale_id, branch_desc, art_no, mikg_art_no
),
top80_customers AS (
  SELECT DISTINCT
    iso_code,
    wholesale_id,
    branch_desc
    FROM (
        SELECT 
        iso_code,
        wholesale_id,
        branch_desc,
        SUM(customer_total_revenue) OVER (PARTITION BY iso_code, branch_desc ORDER BY customer_total_revenue DESC) AS running_total_rev,
        SUM(customer_total_revenue) OVER (PARTITION BY iso_code, branch_desc) AS total_revenue
        FROM purchase_data
    )
    WHERE running_total_rev <= 0.8 * total_revenue
),
top80_revenues AS (
  SELECT
    iso_code,
    branch_desc,
    art_no,
    mikg_art_no,
    -- Aggregation over 1 year
    ROUND(SUM(customer_total_revenue), 2) AS branch_total_revenue,
    ROUND(SUM(customer_total_quantity), 2) AS branch_total_quantity,
    ROUND(SUM(customer_purchase_freq), 2) AS branch_purchase_freq,
    ROUND(AVG(customer_total_revenue), 2) AS avg_branch_article_revenue,
    ROUND(AVG(avg_cust_article_price), 2) AS avg_branch_article_price,
    -- Aggregation ober 1 month
    ROUND(SUM(customer_total_revenue_1m), 2) AS branch_total_revenue_1m,
    ROUND(SUM(customer_total_quantity_1m), 2) AS branch_total_quantity_1m,
    ROUND(SUM(customer_purchase_freq_1m), 2) AS branch_purchase_freq_1m,
    ROUND(AVG(customer_total_revenue_1m), 2) AS avg_branch_article_revenue_1m,
    ROUND(AVG(avg_cust_article_price_1m), 2) AS avg_branch_article_price_1m,
  FROM purchase_data
  JOIN top80_customers USING(iso_code, branch_desc, wholesale_id)
  JOIN article_data USING(iso_code, art_no, mikg_art_no)
  GROUP BY iso_code, branch_desc, art_no, mikg_art_no
),
article_ranking AS (
  SELECT DISTINCT
    iso_code,
    branch_desc,
    art_no,
    mikg_art_no,
    own_brand_score,
    revenue_score,
    purchase_freq_score,
    faiss_score,
    (1 + ((own_brand_score + revenue_score + purchase_freq_score) / 3))/2*faiss_score AS article_rank,
    (1 + ((own_brand_score + revenue_score_1m + purchase_freq_score_1m) / 3))/2*faiss_score AS article_rank_1m
    FROM (
      SELECT DISTINCT
        iso_code,
        branch_desc,
        art_no,
        mikg_art_no,
        own_brand_score,
        faiss_score,
        CASE WHEN min_branch_ingredient_revenue = max_branch_ingredient_revenue THEN 1
        ELSE (branch_total_revenue - min_branch_ingredient_revenue) / (max_branch_ingredient_revenue - min_branch_ingredient_revenue)
        END AS revenue_score,
        CASE WHEN min_branch_purchase_freq = max_branch_purchase_freq THEN 1
        ELSE (branch_purchase_freq - min_branch_purchase_freq) / (max_branch_purchase_freq - min_branch_purchase_freq)
        END AS purchase_freq_score,
        CASE WHEN min_branch_ingredient_revenue_1m = max_branch_ingredient_revenue_1m THEN 1
        ELSE (branch_total_revenue_1m - min_branch_ingredient_revenue_1m) / (max_branch_ingredient_revenue_1m - min_branch_ingredient_revenue_1m)
        END AS revenue_score_1m,
        CASE WHEN min_branch_purchase_freq_1m = max_branch_purchase_freq_1m THEN 1
        ELSE (branch_purchase_freq_1m - min_branch_purchase_freq_1m) / (max_branch_purchase_freq_1m - min_branch_purchase_freq_1m)
        END AS purchase_freq_score_1m
      FROM (
        SELECT DISTINCT
          iso_code,
          branch_desc,
          ingredient_name,
          art_no,
          mikg_art_no,
          branch_total_revenue,
          branch_purchase_freq,
          branch_total_revenue_1m,
          branch_purchase_freq_1m,
          CAST(is_ownbrand AS INT) AS own_brand_score,
          MIN(branch_total_revenue) OVER (PARTITION BY ingredient_name, branch_desc) AS min_branch_ingredient_revenue,
          MAX(branch_total_revenue) OVER (PARTITION BY ingredient_name, branch_desc) AS max_branch_ingredient_revenue,
          MIN(branch_purchase_freq) OVER (PARTITION BY ingredient_name, branch_desc) AS min_branch_purchase_freq,
          MAX(branch_purchase_freq) OVER (PARTITION BY ingredient_name, branch_desc) AS max_branch_purchase_freq,
          MIN(branch_total_revenue_1m) OVER (PARTITION BY ingredient_name, branch_desc) AS min_branch_ingredient_revenue_1m,
          MAX(branch_total_revenue_1m) OVER (PARTITION BY ingredient_name, branch_desc) AS max_branch_ingredient_revenue_1m,
          MIN(branch_purchase_freq_1m) OVER (PARTITION BY ingredient_name, branch_desc) AS min_branch_purchase_freq_1m,
          MAX(branch_purchase_freq_1m) OVER (PARTITION BY ingredient_name, branch_desc) AS max_branch_purchase_freq_1m,
          IFNULL(score, 1) AS faiss_score
        FROM top80_revenues
        JOIN article_data USING(iso_code, art_no, mikg_art_no)
        JOIN `{DWH_PROJECT}.trusted.fg_articles_to_ingredients_{env}` USING(iso_code, art_no)
      )
    )
),
branch_data AS (
  SELECT
    iso_code,
    branch_desc,
    ingredient_name,
    art_no,
    mikg_art_no,
    art_name,
    mge_main_cat_desc,
    mge_cat_desc,
    mge_sub_cat_desc,
    is_ownbrand,
    department_flag,
    avg_branch_article_price,
    avg_branch_article_revenue,
    branch_total_revenue,
    SUM(branch_total_revenue) OVER (PARTITION BY iso_code, department_flag) AS branch_department_total_revenue,
    article_rank,
    avg_branch_article_price_1m,
    avg_branch_article_revenue_1m,
    branch_total_revenue_1m,
    SUM(branch_total_revenue_1m) OVER (PARTITION BY iso_code, department_flag) AS branch_department_total_revenue_1m,
    article_rank_1m
  FROM top80_revenues
  JOIN article_data USING(iso_code, art_no, mikg_art_no)
  JOIN article_ranking USING(iso_code, branch_desc, art_no, mikg_art_no)
)
SELECT *
FROM branch_data
    """
    return query 


def article_recommendation_query(iso_code: str, branch_table: str) -> str:
    query = f"""
    WITH
purchase_data AS (
  SELECT
    iso_code,
    wholesale_id,
    branch_desc,
    art_no,
    mikg_art_no,
    MAX(date_of_day) AS last_purchase_date,
    -- Aggregation over 1 year
    COUNT(1) AS customer_purchase_freq,
    SUM(sale_money) AS customer_total_revenue,
    SUM(sale_qty) AS customer_total_quantity,
    AVG(sale_money/IF(sale_qty = 0, 1, sale_qty)/(tunit_qty/min_tunit_qty)) AS avg_cust_article_price,
    -- Aggregation over 1 month
    COUNT(CASE WHEN date_of_day > DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH) THEN 1 ELSE 0 END) AS customer_purchase_freq_1m,
    SUM(CASE WHEN date_of_day > DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH) THEN sale_money ELSE 0 END) AS customer_total_revenue_1m,
    SUM(CASE WHEN date_of_day > DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH) THEN sale_qty ELSE 0 END) AS customer_total_quantity_1m,
    AVG(CASE WHEN date_of_day > DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH) THEN sale_money/IF(sale_qty = 0, 1, sale_qty)/(tunit_qty/min_tunit_qty) ELSE NULL END) AS avg_cust_article_price_1m
  FROM (
    SELECT
      iso_code,
      wholesale_id,
      branch_desc,
      art_no,
      mikg_art_no,
      sale_money,
      sale_qty,
      tunit_qty,
      date_of_day,
      MIN(tunit_qty) OVER(PARTITION BY iso_code, art_no) AS min_tunit_qty
    FROM `{DWH_PROJECT}.refined.analytical_wholesale_transactions_{iso_code}`
    WHERE date_of_day > DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)
  )
  GROUP BY iso_code, wholesale_id, branch_desc, art_no, mikg_art_no
),
customer_data AS (
  SELECT
    iso_code,
    wholesale_id,
    branch_desc,
    ingredient_name,
    art_no,
    mikg_art_no,
    art_name,
    mge_main_cat_desc,
    mge_cat_desc,
    mge_sub_cat_desc,
    is_ownbrand,
    department_flag,
    last_purchase_date,
    avg_cust_article_price,
    customer_total_quantity,
    customer_total_revenue,
    SUM(customer_total_revenue) OVER (PARTITION BY iso_code, department_flag) AS customer_department_total_revenue,
    avg_branch_article_price,
    avg_branch_article_revenue,
    branch_total_revenue,
    SUM(branch_total_revenue) OVER (PARTITION BY iso_code, department_flag) AS branch_department_total_revenue,
    article_rank,
    avg_cust_article_price_1m,
    customer_total_quantity_1m,
    customer_total_revenue_1m,
    SUM(customer_total_revenue_1m) OVER (PARTITION BY iso_code, department_flag) AS customer_department_total_revenue_1m,
    avg_branch_article_price_1m,
    avg_branch_article_revenue_1m,
    branch_total_revenue_1m,
    SUM(branch_total_revenue_1m) OVER (PARTITION BY iso_code, department_flag) AS branch_department_total_revenue_1m,
    article_rank_1m
  FROM purchase_data
  LEFT JOIN {branch_table} USING(iso_code, branch_desc, art_no, mikg_art_no)
)
SELECT *
FROM customer_data
    """
    return query


def exclude_deleted_statement(*, field: str, iso_code: str = '*') -> str:
    """
    Returns filter for query to exclude deleted establishments.

    The is_deleted flag is on wholesale_id level. To exclude (field=) unique_wholesale_id, we check that all wholesale_id are
    deleted.
    :param field: wholesale_id or unique_wholesale_id
    :param iso_code: ISO country code
    :return: sub-query string
    """
    if field == 'wholesale_id':
        statement = f"""({field} IS NULL 
                            OR  SAFE_CAST({field} AS INT64) NOT IN 
                                (
                                    SELECT {field} 
                                    FROM `{DWH_PROJECT}.refined.analytical_wholesale_customers_{iso_code}` 
                                    WHERE is_deleted = 1
                                ))"""
    elif field == 'unique_wholesale_id':
        statement = f"""({field} IS NULL 
                            OR  SAFE_CAST({field} AS INT64) NOT IN 
                                (
                                    SELECT {field} 
                                    FROM `{DWH_PROJECT}.refined.analytical_wholesale_customers_{iso_code}` 
                                    GROUP BY {field}
                                    HAVING sum(is_deleted) = count(*)
                                ))"""
    else:
        raise ValueError(f"Unsupported input: {field}")
    return statement


def wholesale_assortment_single_country(iso_code: str) -> str:
    
    if iso_code in ('PL', 'NL'):
        domain_desc = "stratbuy_domain_desc"
        level_2_group = "mge_main_cat_desc"
        level_3_group = "mge_cat_desc"
        level_4_group = "mge_sub_cat_desc"
    else:
        domain_desc = "catman_buy_domain_desc"
        level_2_group = "pcg_main_cat_desc"
        level_3_group = "pcg_cat_desc"
        level_4_group = "pcg_sub_cat_desc"

    query = f"""SELECT iso_code,
                pcg_cat_full_id,
                mge_cat_full_id,
                art_no, var_no,
                tunit_no,
                tunit_qty,
                {domain_desc} AS article_family_column,
                {level_2_group} AS level_2_group,
                {level_3_group} AS level_3_group,
                {level_4_group} AS level_4_group,
                var_tu_key,
                mikg_art_no,
                art_name,
                parsed_art_name,
                art_name_tl,
                art_name_altern2,
                mge_main_cat_id,
                mge_cat_id,
                mge_sub_cat_id,
                mge_main_cat_desc,
                mge_main_cat_desc_tl,
                mge_cat_desc,
                mge_cat_desc_tl,
                mge_sub_cat_desc,
                mge_sub_cat_desc_tl,
                pcg_main_cat_id,
                pcg_cat_id,
                pcg_sub_cat_id,
                pcg_main_cat_desc,
                pcg_main_cat_desc_tl,
                pcg_cat_desc,
                pcg_cat_desc_tl,
                pcg_sub_cat_desc,
                pcg_sub_cat_desc_tl,
                is_ownbrand,
                food_flag,
                drink_flag,
                mshop_display_id, mshop_name, mshop_image_url,
                mshop_shop_url,
                mshop_url_large, mshop_url_small, row_number,
                catman_buy_domain_id,
                catman_buy_domain_desc,
                stratbuy_domain_desc  ,
                department_flag
                FROM `{DWH_PROJECT}.refined.analytical_wholesale_articles_{iso_code}`"""

    return query 


def fbo_scores_export(iso_code: str) -> str:
    if iso_code in ('DE','FR'):
        query = f"""with 
                nbo_mapping as (
                SELECT
                DISTINCT 
                hd.sfdc_establishment_id,hd.wholesale_id
                FROM `{DWH_PROJECT}.refined.all_platform_establishments` hd
                where hd.iso_code = "{iso_code}"
                )
                ,
                di_mapping as 
                (
                SELECT
                DISTINCT 
                di.md_establishment_id,di.wholesale_id
                FROM `{DWH_PROJECT}.refined.all_deepideas_establishments` di
                where di.iso_code = "{iso_code}"
                and wholesale_id is not null
                )
                ,scores_agg_fbo_metro AS (
                SELECT
                DISTINCT CAST(wholesale_id AS INT64) wholesale_id,
                CAST(wholesale_id AS STRING) est_id,
                MAX(CASE
                WHEN do_cust_prob >= mi_do.cutoff AND rt_cust_prob >= mi_rt.cutoff THEN "Platform Premium"
                WHEN do_cust_prob >= mi_do.cutoff THEN "Platform Prof Ord"
                WHEN rt_cust_prob >= mi_rt.cutoff THEN "Platform Prof Res"
                ELSE "Platform Starter"
                END) AS bundle_recommendation_fbo,
                CAST(NULL AS STRING) bundle_recommendation_nbo,
                FROM `{DWH_PROJECT}.refined.platform_fbo_wholesale_score` score
                LEFT JOIN `{DWH_PROJECT}.refined.platform_fbo_model_info`  mi_sf ON mi_sf.model_id = score.sf_cust_model_id
                LEFT JOIN `{DWH_PROJECT}.refined.platform_fbo_model_info`  mi_rt ON mi_rt.model_id = score.rt_cust_model_id
                LEFT JOIN `{DWH_PROJECT}.refined.platform_fbo_model_info`  mi_do ON mi_do.model_id = score.do_cust_model_id
                LEFT JOIN `{DWH_PROJECT}.refined.platform_fbo_model_info`  mi_wb ON mi_wb.model_id = score.wb_cust_model_id
                where sf_cust_prob >= mi_sf.cutoff
                and country_code = "{iso_code}"
                GROUP BY 1,2)

                ,scores_agg_nbo as (
                SELECT
                DISTINCT 
                CAST(wholesale_id AS Int64) as wholesale_id,
                CAST(UID__C AS STRING) est_id,
                CAST(NULL AS STRING) bundle_recommendation_fbo,
                CASE
                WHEN rt_cust_pred = 1 AND do_cust_pred = 1 THEN "Platform Premium"
                WHEN do_cust_pred= 1 THEN "Platform Prof Ord"
                WHEN rt_cust_pred= 1 THEN "Platform Prof Res"
                ELSE "Platform Starter"
                END AS bundle_recommendation_nbo
                FROM `{DWH_PROJECT}.refined.prod_recommendation_score_dev`
                LEFT JOIN 
                nbo_mapping on UID__c = sfdc_establishment_id
                WHERE country_code = "{iso_code}"
                and (rt_cust_pred = 1 OR do_cust_pred = 1)
                )
                , scores_agg_fbo_di as 
                (
                SELECT distinct
                CAST(wholesale_id AS Int64) as wholesale_id,
                CAST(md_establishment_id AS STRING) as est_id,
                MAX(CASE
                WHEN do_cust_prob >= mi_do.cutoff AND rt_cust_prob >= mi_rt.cutoff THEN "Platform Premium"
                WHEN do_cust_prob >= mi_do.cutoff THEN "Platform Prof Ord"
                WHEN rt_cust_prob >= mi_rt.cutoff THEN "Platform Prof Res"
                ELSE "Platform Starter"
                END) AS bundle_recommendation_fbo,
                CAST(NULL AS STRING) bundle_recommendation_nbo
                FROM `{DWH_PROJECT}.refined.platform_fbo_di_wo_wholesale_score`  score
                LEFT JOIN `{DWH_PROJECT}.refined.platform_fbo_model_info` mi_sf ON mi_sf.model_id = score.sf_cust_model_id
                LEFT JOIN `{DWH_PROJECT}.refined.platform_fbo_model_info` mi_rt ON mi_rt.model_id = score.rt_cust_model_id
                LEFT JOIN `{DWH_PROJECT}.refined.platform_fbo_model_info` mi_do ON mi_do.model_id = score.do_cust_model_id
                LEFT JOIN `{DWH_PROJECT}.refined.platform_fbo_model_info` mi_wb ON mi_wb.model_id = score.wb_cust_model_id
                LEFT JOIN di_mapping using(md_establishment_id)
                where sf_cust_prob >= mi_sf.cutoff
                and country_code = "{iso_code}"
                GROUP BY 1,2
                )

                , combined as (

                SELECT * from scores_agg_nbo
                UNION ALL
                SELECT * from scores_agg_fbo_metro
                UNION ALL 
                SELECT * from scores_agg_fbo_di

                )
                ,pos_high_potential as 
                (
                SELECT distinct wholesale_id, True as POS_high_potential 
                FROM `{DWH_PROJECT}.trusted_views.pos_fbo_wholesale_score`
                where label in ("Highest Potential","High Potential")
                and country = "{iso_code}"
                )


                SELECT 
                distinct 
                est_id establishment_id,
                bundle_recommendation_fbo FBO_Platform_Bundle,
                bundle_recommendation_nbo NBO_Platform_Bundle,
                IFNULL(POS_high_potential,False) POS_high_potential
                from combined
                FULL OUTER JOIN pos_high_potential using(wholesale_id)"""
    else:
        query=f"""
            with 
            scores_agg_fbo_metro AS (
            SELECT
                CAST(wholesale_id AS STRING) est_id,
                MAX(CASE
                    WHEN do_cust_prob >= mi_do.cutoff AND rt_cust_prob >= mi_rt.cutoff THEN "Platform Premium"
                    WHEN do_cust_prob >= mi_do.cutoff THEN "Platform Prof Ord"
                    WHEN rt_cust_prob >= mi_rt.cutoff THEN "Platform Prof Res"
                    ELSE "Platform Starter"
                    END) AS bundle_recommendation_fbo,
                CAST(NULL AS STRING) bundle_recommendation_nbo,
            FROM `{DWH_PROJECT}.refined.platform_fbo_wholesale_score` score
            LEFT JOIN `{DWH_PROJECT}.refined.platform_fbo_model_info`  mi_sf ON mi_sf.model_id = score.sf_cust_model_id
            LEFT JOIN `{DWH_PROJECT}.refined.platform_fbo_model_info`  mi_rt ON mi_rt.model_id = score.rt_cust_model_id
            LEFT JOIN `{DWH_PROJECT}.refined.platform_fbo_model_info`  mi_do ON mi_do.model_id = score.do_cust_model_id
            LEFT JOIN `{DWH_PROJECT}.refined.platform_fbo_model_info`  mi_wb ON mi_wb.model_id = score.wb_cust_model_id
            where sf_cust_prob >= mi_sf.cutoff
                and country_code = "{iso_code}"
            GROUP BY 1
            )

            ,scores_agg_nbo as (
            SELECT DISTINCT 
                CAST(UID__C AS STRING) est_id,
                CAST(NULL AS STRING) bundle_recommendation_fbo,
                CASE
                WHEN rt_cust_pred = 1 AND do_cust_pred = 1 THEN "Platform Premium"
                WHEN do_cust_pred = 1 THEN "Platform Prof Ord"
                WHEN rt_cust_pred = 1 THEN "Platform Prof Res"
                ELSE "Platform Starter"
                END AS bundle_recommendation_nbo
            FROM `{DWH_PROJECT}.refined.prod_recommendation_score_dev`
            WHERE country_code = "{iso_code}"
                and (rt_cust_pred = 1 OR do_cust_pred = 1)
            )
            , scores_agg_fbo_di as (
            SELECT DISTINCT
                CAST(md_establishment_id AS STRING) as est_id,
                MAX(CASE
                    WHEN do_cust_prob >= mi_do.cutoff AND rt_cust_prob >= mi_rt.cutoff THEN "Platform Premium"
                    WHEN do_cust_prob >= mi_do.cutoff THEN "Platform Prof Ord"
                    WHEN rt_cust_prob >= mi_rt.cutoff THEN "Platform Prof Res"
                    ELSE "Platform Starter"
                    END) AS bundle_recommendation_fbo,
                CAST(NULL AS STRING) bundle_recommendation_nbo
            FROM `{DWH_PROJECT}.refined.platform_fbo_di_wo_wholesale_score`  score
            LEFT JOIN `{DWH_PROJECT}.refined.platform_fbo_model_info` mi_sf ON mi_sf.model_id = score.sf_cust_model_id
            LEFT JOIN `{DWH_PROJECT}.refined.platform_fbo_model_info` mi_rt ON mi_rt.model_id = score.rt_cust_model_id
            LEFT JOIN `{DWH_PROJECT}.refined.platform_fbo_model_info` mi_do ON mi_do.model_id = score.do_cust_model_id
            LEFT JOIN `{DWH_PROJECT}.refined.platform_fbo_model_info` mi_wb ON mi_wb.model_id = score.wb_cust_model_id
            where sf_cust_prob >= mi_sf.cutoff
                and country_code = "{iso_code}"
            GROUP BY 1
            )

            , combined as (

            SELECT * from scores_agg_nbo
            UNION ALL
            SELECT * from scores_agg_fbo_metro
            UNION ALL 
            SELECT * from scores_agg_fbo_di

            )


            SELECT distinct 
            est_id establishment_id,
            bundle_recommendation_fbo AS FBO_Platform_Bundle,
            bundle_recommendation_nbo AS NBO_Platform_Bundle,
            CAST(NULL as BOOLEAN) as POS_high_potential
            from combined
        """
    return query


def article_recommender_query(iso_code: str, env: str) -> str:
    if iso_code in ('PL'):
        article_info = f""",
            -- Below CTE is only for Poland currently
            -- 3 columns are taken from this CTE week_id, Contribution_Margin, Contribution_Income_per_kg
            article_info
            AS
              (
                SELECT DISTINCT
                  week_id,
                  art_no,
                  mikg_art_no,
                  master_id,
                  Contribution_Margin,
                  Contribution_Income_per_kg
                FROM
                  `{DWH_PROJECT}.refined.pl_article_potential`
                WHERE
                  week_id = (SELECT MAX(week_id) FROM `{DWH_PROJECT}.refined.pl_article_potential`)
              )"""
        additional_columns = f""",ai.week_id,ai.Contribution_Margin,ai.Contribution_Income_per_kg"""
        join_condition = f"""LEFT JOIN
                              article_info ai
                              ON
                              ai.master_id = gaps.master_id
                              AND ai.art_no = ad.art_no
                              AND ai.mikg_art_no = ad.mikg_art_no"""
    else:
        article_info = ""
        additional_columns = ",CAST(NULL AS INTEGER) as week_id,CAST(NULL AS NUMERIC) as Contribution_Margin,CAST(NULL AS NUMERIC) as Contribution_Income_per_kg"
        join_condition = ""

    if env == 'dev':
        table_suffix = "_dev"
    else:
        table_suffix = "_acc"
    
    query = f"""
    WITH gaps
      AS
        (
          SELECT DISTINCT
            a.iso_code,
            a.establishment_id,
            b.wholesale_id,
            b.master_id,
            b.branch_desc,
            a.ingredient_name,
            a.prediction_type
          FROM
            -- `ml_project.foodgraph_dev_preprocessed.gaps_unnested`
            `{DWH_PROJECT}.trusted.fg_gaps_unnested{table_suffix}` a
            JOIN
              (
                SELECT DISTINCT
                  wholesale_id,
                  CONCAT(wholesale_home_store_id, "_", wholesale_cust_no)           AS master_id, -- adding master_id
                  establishment_id,
                  branch_desc
                FROM
                  `{DWH_PROJECT}.refined.all_establishments_{iso_code}`
                WHERE
                  data_source = 'all'
              ) b
            ON a.establishment_id = b.establishment_id
          WHERE
            iso_code = '{iso_code}'
            AND prediction_type IS NOT NULL

        ),
      article_data
      AS
        (
          SELECT DISTINCT
            a.iso_code,
            a.art_no,
            a.mikg_art_no,
            a.art_name,
            a.ing_id,
            a.ingredient_name,
            a.mge_main_cat_desc,
            a.mge_cat_desc,
            a.mge_sub_cat_desc,
            a.is_ownbrand,
            a.department_flag,
          FROM `{DWH_PROJECT}.refined.analytical_wholesale_articles_{iso_code}` a
        ),
      -- customer purchase data to get flag for buying/non buying
      purchase_data
      AS
        (
          SELECT
            iso_code,
            wholesale_id,
            branch_desc,
            master_id,
            establishment_id,
            art_no,
            mikg_art_no,
            MAX(date_of_day) AS last_purchase_date,
            -- Aggregation over 1 year
            SUM(1) AS customer_purchase_freq,
            SUM(sale_money) AS customer_total_revenue,
            SUM(sale_qty) AS customer_total_quantity,
            AVG(sale_money/IF(sale_qty = 0, 1, sale_qty)/(tunit_qty/min_tunit_qty)) AS avg_cust_article_price,
            -- Aggregation over 3 months
            SUM(CASE WHEN date_of_day > DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH) THEN 1 ELSE 0 END) AS customer_purchase_freq_3m,
            SUM(CASE WHEN date_of_day > DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH) THEN sale_money ELSE 0 END) AS customer_total_revenue_3m,
            SUM(CASE WHEN date_of_day > DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH) THEN sale_qty ELSE 0 END) AS customer_total_quantity_3m,
            AVG(CASE WHEN date_of_day > DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH) THEN sale_money/IF(sale_qty = 0, 1, sale_qty)/(tunit_qty/min_tunit_qty) ELSE NULL END) AS avg_cust_article_price_3m
          FROM (
            SELECT
              a.iso_code,
              a.wholesale_id,
              CONCAT(a.home_store_id, "_", a.cust_no)       AS master_id,
              branch_desc,
              b.establishment_id,
              a.art_no,
              a.mikg_art_no,
              a.sale_money,
              a.sale_qty,
              a.tunit_qty,
              a.date_of_day,
              MIN(a.tunit_qty) OVER(PARTITION BY a.iso_code, a.art_no) AS min_tunit_qty
            FROM `{DWH_PROJECT}.refined.analytical_wholesale_transactions_{iso_code}` a
            JOIN
              (
                SELECT DISTINCT
                  wholesale_id,
                  establishment_id, -- adding establishment ids
                FROM
                  `{DWH_PROJECT}.refined.all_establishments_{iso_code}`
                WHERE
                  data_source = 'all'
              ) b
            ON a.wholesale_id = b.wholesale_id
            WHERE
              date_of_day > DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)
          )

          GROUP BY iso_code, wholesale_id,branch_desc,establishment_id,master_id,art_no, mikg_art_no
        ),
      top80_customers
        AS
          (
            SELECT DISTINCT
              iso_code,
              wholesale_id,
              branch_desc
            FROM
              (
                  SELECT 
                  iso_code,
                  wholesale_id,
                  branch_desc,
                  SUM(customer_total_revenue) OVER (PARTITION BY iso_code, branch_desc ORDER BY customer_total_revenue DESC) AS running_total_rev,
                  SUM(customer_total_revenue) OVER (PARTITION BY iso_code, branch_desc) AS total_revenue
                  FROM purchase_data
              )
            WHERE running_total_rev <= 0.8 * total_revenue
          ),
      top80_revenues
        AS
        (
          SELECT
            iso_code,
            branch_desc,
            art_no,
            mikg_art_no,
            -- Aggregation over 1 year
            ROUND(AVG(customer_total_revenue), 2) AS avg_branch_article_revenue,
            -- Aggregation over 3 month
            ROUND(AVG(customer_total_revenue_3m), 2) AS avg_branch_article_revenue_3m,
          FROM purchase_data
          JOIN top80_customers USING(iso_code, branch_desc, wholesale_id)
          JOIN article_data USING(iso_code, art_no, mikg_art_no)
          GROUP BY iso_code, branch_desc, art_no, mikg_art_no
        )
      -- Below CTE is only for Poland currently
      -- 3 columns are taken from this CTE week_id, Contribution_Margin, Contribution_Income_per_kg
      {article_info}

      SELECT
      gaps.iso_code,
      gaps.wholesale_id,
      gaps.master_id,
      gaps.establishment_id,
      gaps.ingredient_name,
      gaps.prediction_type,
      b.branch_desc,
      curr.currency,
      CURRENT_DATE() AS creation_date,
      ARRAY_AGG(STRUCT(
      ad.ing_id,
      ad.art_name,
      ad.art_no,
      ad.mikg_art_no,
      ad.is_ownbrand,
      ad.department_flag,
      CASE
      WHEN customer_total_revenue > 0 THEN 1
      ELSE 0
      END AS buy_status_12m,
      CASE
      WHEN customer_total_revenue_3m > 0 THEN 1
      ELSE 0
      END AS buy_status_3m,
      pd.last_purchase_date,
      pd.customer_purchase_freq,
      pd.customer_total_revenue,
      pd.customer_total_quantity,
      pd.avg_cust_article_price,
      pd.customer_purchase_freq_3m,
      pd.customer_total_revenue_3m,
      pd.customer_total_quantity_3m,
      pd.avg_cust_article_price_3m,
      price.avg_branch_article_revenue,
      price.avg_branch_article_revenue_3m
      {additional_columns})) AS article_data_array
      FROM
      gaps
      LEFT JOIN
      article_data ad
      ON
      gaps.iso_code = ad.iso_code
      AND gaps.ingredient_name = ad.ingredient_name
      LEFT JOIN
      purchase_data pd
      ON
      gaps.iso_code = pd.iso_code
      AND gaps.establishment_id = pd.establishment_id
      AND ad.art_no = pd.art_no
      AND  ad.mikg_art_no = pd.mikg_art_no
      {join_condition}
      LEFT JOIN
        top80_revenues price
      ON
        gaps.branch_desc = price.branch_desc
        AND ad.art_no = price.art_no
        AND ad.mikg_art_no = price.mikg_art_no
      LEFT JOIN
      (
      SELECT DISTINCT
        establishment_id, 
        branch_desc -- getting branch_desc
      FROM
        `{DWH_PROJECT}.refined.all_establishments_{iso_code}`
      WHERE
        data_source = 'all'
      ) b
      ON gaps.establishment_id = b.establishment_id
      LEFT JOIN
      (select distinct iso_code, currency from `{DWH_PROJECT}.refined_foodgraph.wholesale_analytics_article_{iso_code}`) curr -- this table is created from `{DWH_PROJECT}.refined_foodgraph.wholesale_analytics_article_*
      ON
      gaps.iso_code = curr.iso_code
      where gaps.iso_code = '{iso_code}'
      group by 1,2,3,4,5,6,7,8,9
    """
    return query
        
