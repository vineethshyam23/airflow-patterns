"""
Multi-country FBO/NBO scoring SQL builders.

Builds the BigQuery statements that:
  1. Union First-Best-Offer (non-CRM) and Next-Best-Offer (CRM-linked) scores
  2. Apply a customer whitelist + authorized-person join
  3. Emit keyhash / rowhash for SCD-style delta detection
  4. Produce insert / hist / update / per-country send queries

Source (read-only): dags/horeca_digital/dana_scoring_query.py
"""

import logging


class ScoringQuery:
    # ISO-2 → ISO-3 used for country-partitioned refined / trusted tables.
    # PL is intentionally absent here: send list still includes PL and returns
    # empty gracefully when no scoring model exists for that market.
    countries = {
        "de": "ger",
        "cz": "cze",
        "fr": "fra",
        "es": "esp",
        "pt": "por",
        "nl": "ned",
        "ro": "rom",
        "hu": "hun",
        "it": "ita",
        "hr": "cro",
        "sk": "svk",
        "tr": "tur",
    }

    @staticmethod
    def get_country_query(iso_code2="FR", iso_code3="fra"):
        """
        Per-country SELECT that joins establishment dims to scoring models.

        FBO path: establishments without a CRM establishment id.
        NBO path: establishments already linked in CRM.
        Both are filtered through a trusted customer whitelist.
        """
        query = f"""
WITH
  metro_non_hd AS (
  SELECT
    DISTINCT metro_id,
    establishment_name,
    sfdc_establishment_id,
    street_name,
    street_number,
    postal_code,
    city,
    phone,
    email,
    establishment_type,
    iso_code
  FROM `refined.all_metro_establishments_{iso_code2.upper()}`
  WHERE sfdc_establishment_id IS NULL
    AND sfdc_account_id IS NULL
  )
  , scores_agg_fbo AS (
    SELECT DISTINCT
      CAST(metro_id AS INT64) metro_id,
      country_code,
      FBO_dish_prob AS score,
      FBO_dish_recommendation AS prediction,
      CASE
        WHEN FBO_dish_bundle_cat = 'very_high_potential' THEN 1
        WHEN FBO_dish_bundle_cat = 'high_potential' THEN 2
        ELSE 3
      END AS potential_level,
      ROUND(FBO_dish_bundle_prob, 4) AS bundle_potential_score,
      FBO_dish_bundle_recommendation AS bundle_recommendation,
      CAST(FBO_dish_pos_recommendation AS INT64) AS pos_recommendation,
      ROUND(FBO_dish_pos_prob, 4) AS pos_potential_score,
      FBO_dish_pay_recommendation AS pay_recommendation,
      ROUND(FBO_dish_pay_prob, 4) AS pay_potential_score
    FROM `refined.analytical_scoring_metro_customer`
  )
  , fbo AS (
  SELECT
    "FBO" AS score_type,
    metro_non_hd.*,
    scores_agg_fbo.* EXCEPT(metro_id)
  FROM metro_non_hd
  JOIN scores_agg_fbo USING (metro_id)
  )

, metro_hd AS (
  SELECT
    DISTINCT metro_id,
    establishment_name,
    sfdc_establishment_id,
    street_name,
    street_number,
    postal_code,
    city,
    phone,
    email,
    establishment_type,
    iso_code
  FROM `refined.all_metro_establishments_{iso_code2.upper()}`
  WHERE sfdc_establishment_id IS NOT NULL
  )

, scores_agg_nbo AS (
    SELECT DISTINCT
      establishment_sfid AS UID__C,
      country_code,
      CASE
        WHEN NBO_dish_reservation_recommendation = 1
         AND NBO_dish_order_recommendation = 1
          THEN (NBO_dish_reservation_prob + NBO_dish_order_prob) / 2
        WHEN NBO_dish_order_recommendation = 1 THEN NBO_dish_order_prob
        WHEN NBO_dish_reservation_recommendation = 1 THEN NBO_dish_reservation_prob
        ELSE (NBO_dish_reservation_prob + NBO_dish_order_prob) / 2
      END AS upsell_proba,
      IF(
        NBO_dish_reservation_recommendation = 1
        OR NBO_dish_order_recommendation = 1, 1, 0
      ) AS sf_cust_pre,
      CASE
        WHEN NBO_dish_bundle_cat = 'High Potential' THEN 1
        WHEN NBO_dish_bundle_cat = 'Potential' THEN 2
        ELSE 3
      END AS potential_level,
      ROUND(NBO_dish_bundle_prob, 4) AS bundle_potential_score,
      NBO_dish_bundle_recommendation AS bundle_recommendation,
      NBO_dish_pos_recommendation AS pos_recommendation,
      ROUND(NBO_dish_pos_prob, 4) AS pos_potential_score,
      NBO_dish_pay_recommendation AS pay_recommendation,
      ROUND(NBO_dish_pay_prob, 4) AS pay_potential_score
    FROM `refined.analytical_scoring_dish_customer`
  )

, nbo AS (
  SELECT
    "NBO" AS score_type,
    metro_hd.*,
    scores_agg_nbo.* EXCEPT(UID__C)
  FROM metro_hd
  JOIN scores_agg_nbo ON sfdc_establishment_id = UID__C
  )

, combined AS (
  SELECT * FROM fbo
  UNION ALL
  SELECT * FROM nbo
  )

, white_list AS (
  SELECT DISTINCT metro_id
  FROM `trusted_mcc.{iso_code3.lower()}_hd_customer`
  WHERE status_cd = 1
    AND blocking_reason_cd NOT IN ('2', '3', '4', '6', '7', '8', '9')
    AND entry_check_cd NOT IN (65, 68)
    AND checkout_check_cd NOT IN (31, 35)
  )

SELECT DISTINCT
  *,
  TO_HEX(MD5(CONCAT(
    IFNULL(iso_code, ''), '|',
    CAST(metro_id AS STRING)
  ))) AS _keyhash,
  TO_HEX(MD5(CONCAT(
    IFNULL(CAST(metro_id AS STRING), ''), '|',
    IFNULL(CAST(establishment_id AS STRING), ''), '|',
    IFNULL(CAST(street_name AS STRING), ''), '|',
    IFNULL(CAST(street_number AS STRING), ''), '|',
    IFNULL(CAST(postal_code AS STRING), ''), '|',
    IFNULL(CAST(city AS STRING), ''), '|',
    IFNULL(CAST(iso_code AS STRING), ''), '|',
    IFNULL(CAST(manager_information AS STRING), ''), '|',
    IFNULL(CAST(first_name AS STRING), ''), '|',
    IFNULL(CAST(mobilephone AS STRING), ''), '|',
    IFNULL(CAST(establishment_type AS STRING), ''), '|',
    IFNULL(CAST(potential_level AS STRING), ''), '|',
    IFNULL(CAST(bundle_recommendation AS STRING), ''), '|',
    IFNULL(CAST(bundle_potential_score AS STRING), ''), '|',
    IFNULL(CAST(pos_recommendation AS STRING), ''), '|',
    IFNULL(CAST(pos_potential_score AS STRING), ''), '|',
    IFNULL(CAST(pay_recommendation AS STRING), ''), '|',
    IFNULL(CAST(pay_potential_score AS STRING), '')
  ))) AS _rowhash,
  CURRENT_TIMESTAMP AS _valid_from,
  TIMESTAMP('2099-12-31 23:59:59') AS _valid_until,
  TRUE AS _valid_flag
FROM (
  SELECT
    metro_id,
    sfdc_establishment_id AS establishment_id,
    establishment_name,
    street_name,
    street_number,
    postal_code,
    city,
    iso_code,
    CONCAT(first_name, ', ', last_name, ', ', phone, ', ', email) AS manager_information,
    first_name,
    phone AS mobilephone,
    establishment_type,
    potential_level,
    bundle_recommendation,
    bundle_potential_score,
    pos_recommendation,
    pos_potential_score,
    pay_recommendation,
    pay_potential_score,
    CAST(CURRENT_DATE AS STRING) AS _ldts
  FROM combined
  INNER JOIN white_list USING (metro_id)
  INNER JOIN (
    SELECT DISTINCT
      unique_metro_id,
      first_name,
      last_name
    FROM `trusted_mcc.{iso_code3.lower()}_hd_cust_auth_person`
    WHERE auth_person_id = 1
  ) ON metro_id = unique_metro_id
)
WHERE bundle_recommendation IS NOT NULL
"""
        logging.getLogger().info("Retrieved query: get_country_query_%s", iso_code2)
        return query

    @staticmethod
    def get_scoring_export_insert_query():
        """UNION ALL of every country query → scoring_data_export_today."""
        return " UNION ALL ".join(
            [
                f"({ScoringQuery.get_country_query(k.upper(), v.lower())})"
                for k, v in ScoringQuery.countries.items()
            ]
        )

    @staticmethod
    def get_scoring_export_hist_query(today_table, hist_table):
        """
        Delta rows to append onto the history table.

        New keyhash = brand new establishment.
        Same keyhash + new rowhash = attribute / score change.
        """
        query = f"""
SELECT
  metro_id,
  establishment_id,
  establishment_name,
  street_name,
  street_number,
  postal_code,
  city,
  iso_code,
  manager_information,
  first_name,
  mobilephone,
  establishment_type,
  potential_level,
  bundle_recommendation,
  bundle_potential_score,
  pos_recommendation,
  pos_potential_score,
  pay_recommendation,
  pay_potential_score,
  _ldts,
  _keyhash,
  _rowhash,
  _valid_from,
  _valid_until,
  _valid_flag
FROM `trusted_staging.{today_table}`
WHERE
  _keyhash NOT IN (SELECT _keyhash FROM `trusted_staging.{hist_table}`)
  OR (
    _keyhash IN (SELECT _keyhash FROM `trusted_staging.{hist_table}`)
    AND _rowhash NOT IN (SELECT _rowhash FROM `trusted_staging.{hist_table}`)
  )
"""
        logging.getLogger().info("Retrieved query: get_scoring_export_hist_query")
        return query

    @staticmethod
    def get_scoring_export_update_query(today_table, hist_table):
        """Expire previously-active hist rows whose payload changed today."""
        query = f"""
UPDATE `trusted_staging.{hist_table}`
SET
  _valid_until = TIMESTAMP(
    FORMAT_TIMESTAMP(
      '%Y-%m-%d 23:59:59',
      TIMESTAMP(DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY))
    )
  ),
  _valid_flag = FALSE
WHERE
  _valid_flag = TRUE
  AND _keyhash IN (SELECT _keyhash FROM `trusted_staging.{today_table}`)
  AND CONCAT(_keyhash, _rowhash) NOT IN (
    SELECT CONCAT(_keyhash, _rowhash) FROM `trusted_staging.{today_table}`
  )
"""
        logging.getLogger().info("Retrieved query: get_scoring_export_update_query")
        return query

    @staticmethod
    def get_scoring_export_send_query(today_table, hist_table, country_code):
        """Per-country delta payload for the event-ingest API."""
        query = f"""
SELECT
  metro_id,
  IFNULL(CAST(establishment_id AS STRING), '') AS establishment_id,
  IFNULL(CAST(establishment_name AS STRING), '') AS establishment_name,
  IFNULL(CAST(street_name AS STRING), '') AS street_name,
  IFNULL(CAST(street_number AS STRING), '') AS street_number,
  IFNULL(CAST(postal_code AS STRING), '') AS postal_code,
  IFNULL(CAST(city AS STRING), '') AS city,
  IFNULL(CAST(iso_code AS STRING), '') AS iso_code,
  IFNULL(CAST(manager_information AS STRING), '') AS manager_information,
  IFNULL(CAST(first_name AS STRING), '') AS first_name,
  IFNULL(CAST(mobilephone AS STRING), '') AS mobilephone,
  IFNULL(CAST(establishment_type AS STRING), '') AS establishment_type,
  IFNULL(potential_level, 99) AS potential_level,
  IFNULL(CAST(bundle_recommendation AS STRING), '') AS bundle_recommendation,
  bundle_potential_score,
  pos_recommendation,
  pos_potential_score,
  pay_recommendation,
  pay_potential_score,
  IFNULL(CAST(_ldts AS STRING), '') AS _ldts
FROM `trusted_staging.{today_table}`
WHERE
  UPPER(iso_code) = '{country_code.upper()}'
  AND (
    _keyhash NOT IN (SELECT _keyhash FROM `trusted_staging.{hist_table}`)
    OR (
      _keyhash IN (SELECT _keyhash FROM `trusted_staging.{hist_table}`)
      AND _rowhash NOT IN (SELECT _rowhash FROM `trusted_staging.{hist_table}`)
    )
  )
"""
        logging.getLogger().info(
            "Retrieved query for %s: get_scoring_export_send_query", country_code
        )
        return query


if __name__ == "__main__":
    logging.basicConfig(
        format="%(asctime)s %(levelname)-4s %(message)s",
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    print(ScoringQuery.get_country_query("DE", "ger"))
