"""
Peer benchmarking / gap analysis SQL builders.

Per-country queries that materialize:
  - topsellers (80% revenue concentration per segment × article family)
  - gap skeletons (nested article arrays for peer comparison)
  - establishment and transaction staging arrays
  - final gaps table with article-family scores and customer potential bands

Country taxonomy differs: PL/NL use MGE category columns; other markets
use PCG. Domain description columns also fork (stratbuy vs catman).

Source (read-only):
  dags/horeca_digital/benchmarking_gaps_queries.py

Sanitized: project/dataset names, wholesale_id rename, analytics_relevant,
article_family_translations. Engineering structure preserved.
"""

def benchmarking_topsellers_query(iso_code: str, project_id: str, dataset_staging: str, dataset: str,
                                  run_date: str) -> str:
    if iso_code in ('PL', 'NL'):
        domain_desc = "stratbuy_domain_desc"
        domain_desc_id = "stratbuy_domain_id"
    else:
        domain_desc = "catman_buy_domain_desc"
        domain_desc_id = "catman_buy_domain_id"

    query = f"""
    WITH 
    all_establishment_data AS (
      SELECT DISTINCT establishment_id, wholesale_id, branch_desc, rb_group
      FROM `dwh_project.refined.all_establishments_{iso_code}`
      WHERE data_source = "all"
      AND analytics_relevant IS TRUE
      AND (branch_desc IS NOT NULL OR rb_group IS NOT NULL)
      AND (wholesale_id IN (
        SELECT DISTINCT wholesale_id
        FROM `dwh_project.refined.analytical_wholesale_customers_{iso_code}`
        WHERE is_deleted = 0
      ) or rb_group IS NOT NULL)
    ),

    establishment_data AS (
      SELECT DISTINCT establishment_id, wholesale_id, segment, segment_id, segment_column FROM (
      (SELECT DISTINCT establishment_id, wholesale_id, branch_desc AS segment, 'branch_desc' AS segment_column, FARM_FINGERPRINT(CONCAT("branch_desc", branch_desc)) AS segment_id FROM all_establishment_data WHERE branch_desc IS NOT NULL) UNION ALL
      (SELECT DISTINCT establishment_id, wholesale_id, rb_group AS segment, 'rb_group' AS segment_column, FARM_FINGERPRINT(CONCAT("rb_group", rb_group)) AS segment_id FROM all_establishment_data WHERE rb_group IS NOT NULL)
    )
    ),

    customer_transactions AS (
        SELECT DISTINCT
        wholesale_id, 
        establishment_id, 
        segment,
        segment_column,
        segment_id,
        {domain_desc} AS article_family, 
        '{domain_desc}' AS article_family_column, 
        cust_article_family_revenue
        FROM establishment_data
        LEFT JOIN (
            SELECT wholesale_id, branch_desc, {domain_desc}, SUM(sale_money) AS cust_article_family_revenue
            FROM `dwh_project.refined.analytical_wholesale_transactions_{iso_code}`
            JOIN (
                SELECT DISTINCT art_no, COALESCE(article_family_tl, {domain_desc}) AS {domain_desc}
                FROM `dwh_project.refined.analytical_wholesale_articles_{iso_code}`
                LEFT JOIN (
                    SELECT DISTINCT article_family_id AS {domain_desc_id}, article_family_tl
                    FROM `dwh_project.refined.article_family_translations`
                    WHERE iso_code = '{iso_code}'
                ) USING({domain_desc_id})
                WHERE (food_flag IS TRUE
                OR drink_flag IS TRUE)
                AND (article_family_tl != 'REMOVE' OR article_family_tl IS NULL)
            ) USING(art_no)
            WHERE DATE(date_of_day) BETWEEN DATE_SUB(current_date(), interval 1 year) AND current_date()
            AND sale_money != 0
            GROUP BY 1,2,3
        ) USING(wholesale_id)
        WHERE cust_article_family_revenue != 0
    ),

    customer_selection AS (
        SELECT DISTINCT
        wholesale_id,
        segment,
        segment_column,
        segment_id,
        article_family,
        article_family_column,
        cust_article_family_revenue
        FROM (
            SELECT DISTINCT
            wholesale_id,
            segment,
            segment_column,
            segment_id,
            article_family,
            article_family_column,
            cust_article_family_revenue,
            SUM(cust_article_family_revenue) OVER (PARTITION BY segment, segment_column, segment_id, article_family ORDER BY cust_article_family_revenue DESC) AS running_total_rev,
            SUM(cust_article_family_revenue) OVER (PARTITION BY segment, segment_column, segment_id, article_family) AS total_revenue
            FROM customer_transactions
        )
        WHERE running_total_rev <= 0.8 * total_revenue
    ),

    topsellers AS (
        SELECT DISTINCT
        segment,
        segment_column,
        segment_id,
        article_family,
        article_family_column,
        article_family_customer_count,
        article_family_revenue,
        ROUND(article_family_revenue / article_family_customer_count, 2) AS avg_article_family_revenue,
        CONCAT(DATE_SUB(current_date(), interval 1 year), '_', current_date()) AS time_frame
        FROM (
            SELECT segment, segment_column, segment_id,
            article_family, article_family_column,
            ROUND(SUM(cust_article_family_revenue), 2) AS article_family_revenue
            FROM customer_selection 
            GROUP BY 1,2,3,4,5
        )
        JOIN (
            SELECT
            segment,
            segment_column,
            segment_id,
            article_family,
            COUNT(wholesale_id) AS article_family_customer_count
            FROM customer_selection 
            GROUP BY 1,2,3,4
        ) USING(segment, segment_column, segment_id, article_family)
    )

    SELECT *,
     TIMESTAMP('{run_date}') AS _create_ts,
      TIMESTAMP('{run_date}') AS _valid_from,
      TIMESTAMP("2099-12-31 00:00:00") AS _valid_until,
      TRUE AS _valid_flag,
      to_hex(md5(concat(
                                      IFNULL(cast(segment as string),''),'|',
                                      IFNULL(cast(segment_column as string),''),'|',
                                      IFNULL(cast(segment_id as string),'')))) as _keyhash,
                          to_hex(md5(concat(
                                      IFNULL(cast(article_family as string),''),'|',
                                      IFNULL(cast(article_family_column as string),''),'|',
                                      IFNULL(cast(article_family_customer_count as string),''),'|',
                                      IFNULL(cast(article_family_revenue as string),''),'|',
                                      IFNULL(cast(avg_article_family_revenue as string),''),'|',
                                      IFNULL(cast(time_frame as string),'')))) as _rowhash
       FROM topsellers
    """
    return query


def benchmarking_gaps_skeletons_query(iso_code: str, project_id: str, dataset_staging: str, dataset: str,
                                      run_date: str):
    if iso_code in ('PL', 'NL'):
        domain_desc = "stratbuy_domain_desc"
        domain_desc_id = "stratbuy_domain_id"
        level_2_group = "mge_main_cat_desc"
        level_3_group = "mge_cat_desc"
        level_4_group = "mge_sub_cat_desc"
    else:
        domain_desc = "catman_buy_domain_desc"
        domain_desc_id = "catman_buy_domain_id"
        level_2_group = "pcg_main_cat_desc"
        level_3_group = "pcg_cat_desc"
        level_4_group = "pcg_sub_cat_desc"

    query = f"""
    WITH 
    all_establishment_data AS (
      SELECT DISTINCT iso_code, establishment_id, wholesale_id, branch_desc, rb_group
      FROM (
        SELECT DISTINCT '{iso_code}' AS iso_code, establishment_id, wholesale_id, branch_desc, rb_group
        FROM `dwh_project.refined.all_establishments_{iso_code}`
        WHERE data_source = "all" AND analytics_relevant IS TRUE
        AND (branch_desc IS NOT NULL OR rb_group IS NOT NULL)
      )
      JOIN (
        SELECT DISTINCT '{iso_code}' AS iso_code, wholesale_id, is_deleted
        FROM `dwh_project.refined.analytical_wholesale_customers_{iso_code}`
      ) USING(iso_code, wholesale_id)
      WHERE (is_deleted = 0 or rb_group IS NOT NULL)
      AND iso_code = '{iso_code}'
    ),

    establishment_data AS (
      SELECT DISTINCT iso_code, establishment_id, wholesale_id, segment, segment_column, segment_id, FROM (
      (SELECT DISTINCT iso_code, establishment_id, wholesale_id, branch_desc AS segment,
      'branch_desc' AS segment_column, FARM_FINGERPRINT(CONCAT("branch_desc", branch_desc)) AS segment_id 
      FROM all_establishment_data WHERE branch_desc IS NOT NULL) 
      UNION ALL
      (SELECT DISTINCT iso_code, establishment_id, wholesale_id, rb_group AS segment,
      'rb_group' AS segment_column, FARM_FINGERPRINT(CONCAT("rb_group", rb_group)) AS segment_id 
      FROM all_establishment_data WHERE rb_group IS NOT NULL)
    )
    ),

    transactions AS (
      SELECT *,
      ROUND(SUM(cust_article_revenue) OVER (PARTITION BY wholesale_id, article_family), 2) AS cust_article_family_revenue
      FROM (
        SELECT '{iso_code}' AS iso_code, wholesale_id, art_no, mikg_art_no, var_tu_key, article_family, article_family_column,
        SUM(sale_qty) AS cust_article_qty, MIN(pack_type_cd) AS pack_type_cd, SUM(sale_money) AS cust_article_revenue,
        ROUND(AVG(cust_article_price), 2) AS avg_cust_article_price,
        FROM (
          SELECT '{iso_code}' as iso_code, wholesale_id, art_no, mikg_art_no, var_tu_key, pack_type_cd, art.article_family, 
          '{domain_desc}' AS article_family_column, sale_qty, sale_money,
          ROUND(sale_money/IF(sale_qty = 0, 1, sale_qty)/(art.tunit_qty/art.min_tunit_qty), 2) AS cust_article_price
          FROM `dwh_project.refined.analytical_wholesale_transactions_{iso_code}`
          JOIN (
              SELECT
              '{iso_code}' as iso_code,
              art_no,
              mikg_art_no,
              var_tu_key,
              COALESCE(article_family_tl, {domain_desc}) AS article_family,
              tunit_qty,
              pack_type_cd,
              MIN(tunit_qty) OVER (PARTITION BY art_no) AS min_tunit_qty
              FROM `dwh_project.refined.analytical_wholesale_articles_{iso_code}`
              LEFT JOIN (
                    SELECT DISTINCT article_family_id AS {domain_desc_id}, article_family_tl
                    FROM `dwh_project.refined.article_family_translations`
                    WHERE iso_code = '{iso_code}'
                ) USING({domain_desc_id})
                WHERE (article_family_tl != 'REMOVE' OR article_family_tl IS NULL)
              GROUP BY 1,2,3,4,5,6,7
          ) art
          USING(iso_code, art_no, mikg_art_no, var_tu_key)
          WHERE date(date_of_day) BETWEEN DATE_SUB(current_date(), interval 1 year) AND current_date()
        )
        GROUP BY 1,2,3,4,5,6,7
      )
    ),

    customer_selection AS (
        SELECT DISTINCT
        iso_code,
        wholesale_id,
        segment,
        segment_column,
        segment_id,
        article_family,
        article_family_column,
        cust_article_family_revenue,
        running_total_rev,
        total_revenue,
        FIRST_VALUE(running_total_rev)  over(PARTITION BY segment, segment_column, segment_id, article_family order by running_total_rev ASC) first_val 
        FROM (
            SELECT 
            iso_code,
            wholesale_id,
            segment,
            segment_column,
            segment_id,
            article_family,
            article_family_column,
            cust_article_family_revenue,
            SUM(cust_article_family_revenue) OVER (PARTITION BY iso_code, segment, segment_column, article_family) AS total_revenue,
            SUM(cust_article_family_revenue) OVER (PARTITION BY segment, segment_column, segment_id, article_family ORDER BY cust_article_family_revenue DESC
            RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total_rev
            FROM transactions
            LEFT JOIN (
                SELECT DISTINCT iso_code, wholesale_id, segment, segment_column, segment_id
                FROM establishment_data
            ) USING(iso_code, wholesale_id)
            WHERE segment IS NOT NULL
        )
            QUALIFY (running_total_rev <= 0.8 * total_revenue)
                    OR
                    (running_total_rev > 0.8 * total_revenue) AND ((first_val > 0.8 * total_revenue) and (running_total_rev = first_val))
    ),

    article_selection AS (
        SELECT DISTINCT
        iso_code,
        segment,
        segment_column,
        segment_id,
        art_no,
        mikg_art_no,
        var_tu_key,
        article_rank,
        art_name,
        var_type_desc,
        tunit_qty,
        level_2_group,
        level_3_group,
        level_4_group,
        pcg_main_cat_desc,
        pcg_cat_desc,
        pcg_sub_cat_desc,
        is_ownbrand,
        article_family,
        article_family_column,
        total_article_revenue,
        total_article_qty,
        pack_type_cd,
        avg_article_price
        FROM (
            SELECT DISTINCT
            iso_code,
            cs.segment,
            cs.segment_column,
            cs.segment_id,
            cs.article_family,
            cs.article_family_column,
            art_no,
            mikg_art_no,
            var_tu_key,
            pack_type_cd,
            SUM(cust_article_revenue) AS total_article_revenue,
            SUM(cust_article_qty) AS total_article_qty,
            ROUND(AVG(avg_cust_article_price), 2) AS avg_article_price,
            RANK() OVER (PARTITION BY iso_code, segment, segment_column, segment_id ORDER BY SUM(cust_article_revenue) DESC) AS article_rank
            FROM customer_selection cs
            LEFT JOIN transactions USING(iso_code, wholesale_id, article_family)
            GROUP BY 1,2,3,4,5,6,7,8,9,10
        )
        JOIN (
            SELECT DISTINCT
            '{iso_code}' AS iso_code,
            art_no,
            mikg_art_no,
            var_tu_key,
            parsed_art_name AS art_name,
            var_type_desc,
            tunit_qty,
            {level_2_group} AS level_2_group,
            {level_3_group} AS level_3_group,
            {level_4_group} AS level_4_group,
            pcg_main_cat_desc,
            pcg_cat_desc,
            pcg_sub_cat_desc,
            is_ownbrand
            FROM `dwh_project.refined.analytical_wholesale_articles_{iso_code}`
            WHERE food_flag IS TRUE
            OR drink_flag IS TRUE
        ) USING(iso_code, art_no, mikg_art_no, var_tu_key)
        WHERE article_rank <= 2000
    ),

    article_nesting AS (
        SELECT 
        iso_code,
        time_frame,
        segment,
        segment_column,
        segment_id,
        article_family,
        article_family_column,
        avg_article_family_revenue,
        ARRAY_AGG(STRUCT(
        art_no, mikg_art_no, var_tu_key, 
        art_name, var_type_desc, tunit_qty, pack_type_cd, level_2_group AS article_sub_family,
        level_2_group,
        level_3_group,
        level_4_group,
        pcg_main_cat_desc,
        pcg_cat_desc,
        pcg_sub_cat_desc,
        is_ownbrand,
        ROUND(total_article_revenue / article_family_customer_count, 2) AS avg_article_revenue,
        avg_article_price
        ) ORDER BY article_rank) AS article_data
        FROM article_selection 
        LEFT JOIN (
            SELECT distinct
            '{iso_code}' AS iso_code,
            segment,
            segment_column,
            segment_id,
            article_family,
            avg_article_family_revenue,
            time_frame,
            article_family_customer_count
            FROM `{project_id}.refined.benchmarking_topsellers_{iso_code}`
            where _valid_flag = True 
            --and CAST(DATE(_valid_from) as STRING) = '{run_date}'
            )
        USING(iso_code, segment, segment_column, segment_id, article_family) WHERE time_frame is not null
        GROUP BY 1,2,3,4,5,6,7,8
    ),

    article_family_nesting AS (
        SELECT
        iso_code,
        time_frame,
        segment,
        segment_column,
        segment_id,
        ARRAY_AGG(STRUCT(article_family, article_family_column, avg_article_family_revenue, article_data) 
                  ORDER BY avg_article_family_revenue DESC) AS article_family_data
        FROM article_nesting
        GROUP BY 1,2,3,4,5
    ),

    segment_nesting AS (
        SELECT 
        iso_code,
        segment,
        segment_column,
        segment_id,
        ARRAY_AGG(STRUCT(time_frame, article_family_data)) as segment_data
        FROM article_family_nesting
        group by 1,2,3,4
    )

    SELECT
    iso_code,
    segment,
    segment_column,
    segment_id,
    segment_data AS benchmarking_gaps,
     TIMESTAMP('{run_date}') AS _create_ts,
      TIMESTAMP('{run_date}') AS _valid_from,
      TIMESTAMP("2099-12-31 00:00:00") AS _valid_until,
      TRUE AS _valid_flag,
      to_hex(md5(concat(
                                      IFNULL(cast(segment as string),''),'|',
                                      IFNULL(cast(segment_column as string),''),'|',
                                      IFNULL(cast(segment_id as string),'')))) as _keyhash,
                          to_hex(md5(concat(
                                      IFNULL(cast(iso_code as string),''),'|',
                                      IFNULL(TO_JSON_STRING(segment_data),'')))) as _rowhash
    FROM segment_nesting
    """
    return query


def benchmarking_gaps_establishment_query(iso_code: str) -> str:
    rb_groups_query = "" if iso_code == "HR" else f"""
    UNION ALL
    (SELECT DISTINCT '{iso_code}' as iso_code, establishment_id, wholesale_id, last_transaction, rb_group AS segment, 'rb_group' AS segment_column, 
    FARM_FINGERPRINT(CONCAT("rb_group" ,rb_group)) AS segment_id 
    FROM all_establishment_data WHERE rb_group IS NOT NULL)
    """
    query = f"""
   WITH 
    all_establishment_data AS (
      SELECT DISTINCT establishment_id, wholesale_id, last_transaction, branch_desc, rb_group
      FROM `dwh_project.refined.all_establishments_{iso_code}`
      LEFT JOIN (
        SELECT wholesale_id, is_deleted, last_transaction
        FROM `dwh_project.refined.analytical_wholesale_customers_{iso_code}`
      ) cu USING(wholesale_id)
      WHERE data_source = "all"
      AND analytics_relevant IS TRUE
      AND ((branch_desc IS NOT NULL AND is_deleted = 0) OR rb_group IS NOT NULL)
    )
    SELECT segment, segment_column, segment_id,
     ARRAY_AGG(STRUCT(iso_code, establishment_id, wholesale_id, last_transaction)) as establishment_data FROM (
    (SELECT DISTINCT '{iso_code}' as iso_code, establishment_id, wholesale_id, last_transaction, branch_desc AS segment, 'branch_desc' AS segment_column, 
    FARM_FINGERPRINT(CONCAT("branch_desc", branch_desc)) AS segment_id 
    FROM all_establishment_data WHERE branch_desc IS NOT NULL) 
    {rb_groups_query}
    )
    group by 1,2,3
   """
    return query


def benchmarking_gaps_transactions_query(iso_code: str) -> str:
    if iso_code in ('PL', 'NL'):
        domain_desc = "stratbuy_domain_desc"
        domain_desc_id = "stratbuy_domain_id"
    else:
        domain_desc = "catman_buy_domain_desc"
        domain_desc_id = "catman_buy_domain_id"
    query = f"""
   SELECT *
      FROM (
        SELECT wholesale_id, art_no, mikg_art_no, var_tu_key, 
        article_family, article_family_column, IFNULL(ROUND(SUM(sale_money), 2), 0) AS cust_article_revenue,
        ROUND(AVG(cust_article_price), 2) AS avg_cust_article_price,
        FROM (
          SELECT wholesale_id, art_no, mikg_art_no, var_tu_key, art.article_family, '{domain_desc}' AS article_family_column, sale_qty, sale_money,
          ROUND(sale_money/IF(sale_qty = 0, 1, sale_qty)/(art.tunit_qty/art.min_tunit_qty), 2) AS cust_article_price
          FROM `dwh_project.refined.analytical_wholesale_transactions_{iso_code}`
          JOIN (
              SELECT
              art_no,
              var_tu_key,
              COALESCE(article_family_tl, {domain_desc}) AS article_family,
              tunit_qty,
              MIN(tunit_qty) OVER (PARTITION BY art_no) AS min_tunit_qty
              FROM `dwh_project.refined.analytical_wholesale_articles_{iso_code}`
              LEFT JOIN (
                    SELECT DISTINCT article_family_id AS {domain_desc_id}, article_family_tl
                    FROM `dwh_project.refined.article_family_translations`
                    WHERE iso_code = '{iso_code}'
                ) USING({domain_desc_id})
                WHERE (article_family_tl != 'REMOVE' OR article_family_tl IS NULL)
              GROUP BY 1,2,3,4
          ) art
          USING(art_no, var_tu_key)
          WHERE date(date_of_day) BETWEEN DATE_SUB(current_date(), interval 1 year) AND current_date()
        )
        GROUP BY 1,2,3,4,5,6
      )
   """
    return query


def benchmarking_gaps_query(iso_code: str, project_id: str, dataset_staging: str, dataset: str, run_date: str) -> str:
    if iso_code in ('PL', 'NL'):
        level_2_group = "mge_main_cat_desc"
        level_3_group = "mge_cat_desc"
        level_4_group = "mge_sub_cat_desc"
    else:
        level_2_group = "pcg_main_cat_desc"
        level_3_group = "pcg_cat_desc"
        level_4_group = "pcg_sub_cat_desc"

    query = f"""
    with skeleton_establishment as (SELECT * 
        FROM (select * EXCEPT(segment_id) from {project_id}.{dataset}.benchmarking_gaps_establishments_{iso_code})
        JOIN {project_id}.{dataset}.benchmarking_gaps_skeletons_{iso_code} USING(segment, segment_column)
        where _valid_flag = True 
        --and CAST(DATE(_valid_from) AS STRING) = '{run_date}'
    ),
    benchmarking_data as (
      select * EXCEPT(iso_code, establishment_data,benchmarking_gaps, article_family_data,article_data,cust_article_revenue, avg_cust_article_price),
      IFNULL(cust_article_revenue, 0) AS cust_article_revenue,
      IFNULL(avg_cust_article_price, 0) AS avg_cust_article_price,
      IFNULL(ROUND(SUM(cust_article_revenue) OVER (PARTITION BY wholesale_id, segment, article_family), 2), 0) AS cust_article_family_revenue,
      IFNULL(ROUND(SUM(cust_article_revenue) OVER (PARTITION BY wholesale_id, segment), 2), 0) AS cust_segment_revenue,
      IFNULL(ROUND(SUM(avg_article_revenue) OVER (PARTITION BY segment), 2), 0) AS segment_revenue
      from skeleton_establishment,
      unnest(establishment_data) as est_data,
      unnest(benchmarking_gaps) as bg,
      unnest(bg.article_family_data) as bg_art_family,
      unnest(bg_art_family.article_data) as bg_art
      left join {project_id}.{dataset}.benchmarking_gaps_transactions_{iso_code} AS t
      USING(wholesale_id, art_no, mikg_art_no, var_tu_key, article_family, article_family_column)
    ),
    article_family_scores AS (
        SELECT DISTINCT
            segment,
            segment_column,
            establishment_id,
            article_family,
            IFNULL(SAFE_DIVIDE((avg_article_family_revenue - cust_article_family_revenue), segment_revenue), 0) AS score
        FROM benchmarking_data
    ),
    article_family_scores_with_median AS (
        SELECT DISTINCT
            segment,
            segment_column,
            establishment_id,
            article_family,
            IFNULL(SAFE_DIVIDE(score, PERCENTILE_DISC(score, 0.5) OVER (PARTITION BY establishment_id)), 0) AS score_divided_by_median
        FROM article_family_scores
    ),
    article_family_percentile_threshold AS (
        -- Get the 25th percentile of score_divided_by_median for each establishment_id
        SELECT DISTINCT
            segment,
            segment_column,
            establishment_id,
            article_family,
            score_divided_by_median AS article_family_score,
            1 AS article_family_high_pot_threshold,
            PERCENTILE_CONT(score_divided_by_median, 0.250) OVER (PARTITION BY establishment_id) AS article_family_low_pot_threshold
        FROM article_family_scores_with_median
    ),
    customer_scores AS (
        SELECT
            establishment_id,
            segment,
            segment_column,
            SUM(cust_article_family_revenue) AS cust_total_revenue,
            SUM(avg_article_family_revenue - cust_article_family_revenue) AS absolute_potential
        FROM benchmarking_data
        GROUP BY establishment_id, segment, segment_column
    ),
    customer_potential_percentiles AS (
    -- Calculate the 20th, 40th, 60th, and 80th percentiles using APPROX_QUANTILES
    SELECT 
        APPROX_QUANTILES(absolute_potential, 100)[OFFSET(20)] AS p20_value,
        APPROX_QUANTILES(absolute_potential, 100)[OFFSET(40)] AS p40_value,
        APPROX_QUANTILES(absolute_potential, 100)[OFFSET(60)] AS p60_value,
        APPROX_QUANTILES(absolute_potential, 100)[OFFSET(80)] AS p80_value
    FROM customer_scores
    WHERE absolute_potential >= 0
    ),
    customer_potential AS (
        SELECT DISTINCT
            establishment_id,
            segment,
            segment_column,
            CASE 
            -- Assign potential 1 if absolute_potential <= 0
            WHEN absolute_potential <= 0 THEN 1
            -- Group 1: Below the 20th percentile (New Boundaries)
            WHEN absolute_potential <= p20_value THEN 1
            -- Group 2: Between 20th and 40th percentiles (New Boundaries)
            WHEN absolute_potential > p20_value 
                 AND absolute_potential <= p40_value THEN 2
            -- Group 3: Between 40th and 60th percentiles (Boundaries)
            WHEN absolute_potential > p40_value 
                 AND absolute_potential <= p60_value THEN 3
            -- Group 4: Between 60th and 80th percentiles (Boundaries)
            WHEN absolute_potential > p60_value 
                 AND absolute_potential <= p80_value THEN 4
            -- Group 5: Above the 80th percentile (Boundaries)
            WHEN absolute_potential > p80_value THEN 5
            END AS customer_potential_category
        FROM customer_scores
        CROSS JOIN customer_potential_percentiles
    ),
    article_nesting AS (
        SELECT
        wholesale_id,
        establishment_id,
        last_transaction,
        segment,
        segment_column,
        segment_id,
        cust_segment_revenue,
        article_family,
        article_family_column,
        cust_article_family_revenue,
        avg_article_family_revenue,
        article_family_score,
        article_family_high_pot_threshold,
        article_family_low_pot_threshold,
        ARRAY_AGG(STRUCT(art_no, mikg_art_no, var_tu_key,
        art_name, var_type_desc, tunit_qty, pack_type_cd, level_2_group AS article_sub_family,
        level_2_group,
        level_3_group,
        level_4_group,
        pcg_main_cat_desc,
        pcg_cat_desc,
        pcg_sub_cat_desc,
        is_ownbrand, cust_article_revenue, avg_cust_article_price,
        avg_article_revenue,
        avg_article_price
        ) ORDER BY avg_article_revenue DESC) AS article_data
        FROM benchmarking_data
        JOIN article_family_percentile_threshold
        USING(establishment_id, segment, segment_column, article_family)
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14
    ),
    article_family_nesting AS (
        SELECT
        wholesale_id,
        establishment_id,
        last_transaction,
        segment,
        segment_column,
        segment_id,
        cust_segment_revenue,
        ARRAY_AGG(STRUCT(
            article_family, article_family_column, cust_article_family_revenue, avg_article_family_revenue,
            article_family_score, article_family_high_pot_threshold, article_family_low_pot_threshold, article_data
        ) ORDER BY avg_article_family_revenue DESC) AS article_family_data
        FROM article_nesting
        GROUP BY 1,2,3,4,5,6,7
    ),

    segment_nesting AS (
        SELECT
        wholesale_id,
        establishment_id,
        last_transaction,
        segment,
        segment_column, 
        segment_id,
        ARRAY_AGG(STRUCT(CONCAT(DATE_SUB(current_date(), interval 1 year), '_', current_date()) AS time_frame, cust_segment_revenue, article_family_data)) as segment_data
        FROM article_family_nesting
        JOIN customer_scores
        USING(establishment_id, segment, segment_column)
        group by 1,2,3,4,5,6
    )

    SELECT
    wholesale_id AS unique_wholesale_id,
    establishment_id,
    last_transaction,
    segment,
    segment_column, 
    segment_id,
    customer_potential_category,
    segment_data AS benchmarking_gaps,
     TIMESTAMP('{run_date}') AS _create_ts,
      TIMESTAMP('{run_date}') AS _valid_from,
      TIMESTAMP("2099-12-31 00:00:00") AS _valid_until,
      TRUE AS _valid_flag,
      to_hex(md5(concat(
                                      IFNULL(cast(wholesale_id as string),''),'|',
                                      IFNULL(cast(establishment_id as string),'')))) as _keyhash,
                          to_hex(md5(concat(
                                      IFNULL(cast(last_transaction as string),''),'|',
                                      IFNULL(cast(segment as string),''),'|',
                                      IFNULL(cast(segment_column as string),''),'|',
                                      IFNULL(cast(segment_id as string),''),'|',
                                      IFNULL(TO_JSON_STRING(segment_data),'')))) as _rowhash
    FROM segment_nesting
    LEFT JOIN customer_potential
    USING(establishment_id, segment, segment_column)
    """
    return query

# if __name__ == '__main__':
#     iso_code = "PT"
#     # benchmarking_topsellers_query = benchmarking_topsellers_query(
#     #     iso_code=iso_code, project_id="dwh_project", dataset_staging="", dataset="", run_date="2023-04-04"
#     # )
#     # benchmarking_gaps_skeletons_query = benchmarking_gaps_skeletons_query(
#     #     iso_code=iso_code, project_id="dwh_project", dataset_staging="", dataset="", run_date="2023-04-04"
#     # )
#     # benchmarking_gaps_establishment_query = benchmarking_gaps_establishment_query(
#     #     iso_code=iso_code
#     # )
#     # benchmarking_gaps_transactions_query = benchmarking_gaps_transactions_query(
#     #     iso_code=iso_code
#     # )
#     benchmarking_gaps_query = benchmarking_gaps_query(
#         iso_code=iso_code, project_id="dwh_project", dataset_staging="",
#         dataset="refined", run_date="2023-04-04"
#     )
#     print(benchmarking_gaps_query)
