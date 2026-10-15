"""
Country-scoped SQL for the platform-customer footprint export.

Two query roles:
  1. insert — build one country's rows into the shared staging table
  2. send   — read today's valid refined rows for Avro ingest

Matching path (insert):
  wholesale account identifiers
    ∪ CRM-cleaned wholesale↔establishment map
    ∪ fuzzy match_result (top_list)
    ∪ product-spot assets / establishment base
    ∪ optional POS match via secondary establishment source

Source (read-only):
  dags/horeca_digital/dana_DISH_customer_query.py
"""

from __future__ import annotations

import logging


class PlatformCustomer:
    """ISO2 → warehouse country suffix used in per-market tables."""

    countries = {
        "BE": "bel",
        "PL": "pol",
        "DE": "ger",
        "PT": "por",
        "FR": "fra",
        "ES": "esp",
        "NL": "ned",
        "RO": "rom",
        "HR": "cro",
        "HU": "hun",
        "IT": "ita",
        "SK": "svk",
        "CZ": "cze",
        "TR": "tur",
        "UA": "ukr",
    }

    @staticmethod
    def get_insert_query(country: str) -> str:
        if country is None:
            raise ValueError("country is required")
        logging.getLogger().info("insert query for %s", country)
        return PlatformCustomer._country_insert_sql(country)

    @staticmethod
    def get_send_query(country: str) -> str:
        """Select today's valid refined rows for one country (Avro ingest)."""
        query = f"""
SELECT
  wholesale_id
  ,ifnull(cast(country_iso as string), '') as country_iso
  ,ifnull(cast(cust_no as integer), -1) as cust_no
  ,ifnull(cast(home_store_id as integer), -1) as home_store_id
  ,ifnull(cast(platform_active_customer as string), '') as platform_active_customer
  ,ifnull(cast(platform_bundle as string), '') as platform_bundle
  ,ifnull(cast(platform_bundle_timestamp as string), '') as platform_bundle_timestamp
  ,ifnull(cast(has_POS as string), '') as has_POS
  ,ifnull(cast(POS_timestamp as string), '') as POS_timestamp
  ,ifnull(cast(has_Reservation as string), '') as has_Reservation
  ,ifnull(cast(has_Website as string), '') as has_Website
  ,ifnull(cast(has_Weblisting as string), '') as has_Weblisting
  ,ifnull(cast(has_Order as string), '') as has_Order
  ,ifnull(cast(has_Menukit as string), '') as has_Menukit
  ,ifnull(cast(Website_creation_ts as string), '') as Website_creation_ts
  ,ifnull(cast(Reservation_creation_ts as string), '') as Reservation_creation_ts
  ,ifnull(cast(WebListing_creation_ts as string), '') as WebListing_creation_ts
  ,ifnull(cast(Order_creation_ts as string), '') as Order_creation_ts
  ,ifnull(cast(Website_deletion_ts as string), '') as Website_deletion_ts
  ,ifnull(cast(Reservation_deletion_ts as string), '') as Reservation_deletion_ts
  ,ifnull(cast(WebListing_deletion_ts as string), '') as WebListing_deletion_ts
  ,ifnull(cast(Order_deletion_ts as string), '') as Order_deletion_ts
  ,ifnull(cast(POS_deletion_ts as string), '') as POS_deletion_ts
  ,ifnull(cast(Starter_creation_ts as string), '') as Starter_creation_ts
  ,ifnull(cast(Starter_deletion_ts as string), '') as Starter_deletion_ts
  ,ifnull(cast(ProfReservation_creation_ts as string), '') as ProfReservation_creation_ts
  ,ifnull(cast(ProfReservation_deletion_ts as string), '') as ProfReservation_deletion_ts
  ,ifnull(cast(ProfOrder_creation_ts as string), '') as ProfOrder_creation_ts
  ,ifnull(cast(ProfOrder_deletion_ts as string), '') as ProfOrder_deletion_ts
  ,ifnull(cast(Premium_creation_ts as string), '') as Premium_creation_ts
  ,ifnull(cast(Premium_deletion_ts as string), '') as Premium_deletion_ts
  ,ifnull(cast(POS_Referrer as string), '') as POS_Referrer
  ,ifnull(cast(Bundle_Referrer as string), '') as Bundle_Referrer
  ,ifnull(cast(wholesale_account_id as string), '') as wholesale_account_id
  ,ifnull(cast(has_POS_flag as string), '') as has_POS_flag
  ,ifnull(cast(date_acquisition as string), '') as date_acquisition
  ,ifnull(cast(date_deletion as string), '') as date_deletion
  ,ifnull(cast(POS_creation_ts as string), '') as POS_creation_ts
  ,ifnull(cast(has_Pay as string), '') as has_Pay
  ,ifnull(cast(Pay_creation_ts as string), '') as Pay_creation_ts
  ,ifnull(cast(Pay_deletion_ts as string), '') as Pay_deletion_ts
FROM `refined.platform_customer_export`
WHERE _valid_flag = True
  AND UPPER(country_iso) = '{country.upper()}'
  AND date(_valid_from) >= current_date()
"""
        logging.getLogger().info("send query for %s", country)
        return query

    @staticmethod
    def _country_insert_sql(country: str) -> str:
        iso3 = PlatformCustomer.countries.get(country, country.lower())
        iso2 = country.upper()

        # Structure mirrors production: account identifiers → CRM clean map
        # → match_result → establishment product footprint → optional POS
        # match → full outer join + HR status filter quirk.
        return f"""
with account_source AS (
  SELECT
    a.id AS accountId,
    accountIdentifiers,
    isDeleted,
    ingestion_timestamp,
    ROW_NUMBER() OVER (PARTITION BY a.id ORDER BY ingestion_timestamp DESC) AS rnk
  FROM `dwh_project.trusted_wholesale.{iso3}_alex_account` a
)
, wholesale_id_map as (
  SELECT DISTINCT
    CAST(ai.data AS int64) AS unique_wholesale_id,
    ai.id AS account_id
  FROM account_source,
  UNNEST(accountIdentifiers) AS ai
  WHERE rnk = 1
    AND isDeleted = FALSE
    AND ai.type = "WHOLESALE_CC"
)
, crm_clean_id as (
  SELECT DISTINCT
    cast(clean_wholesale_id as string) as wholesale_id,
    establishment_id
  FROM `dwh_project.refined.refined_cleaned_crm_wholesale_id`
  WHERE establishment_id NOT IN (
    SELECT establishment_id
    FROM (
      SELECT establishment_id, count(distinct clean_wholesale_id) as n_id
      FROM (SELECT DISTINCT * FROM `dwh_project.refined.refined_cleaned_crm_wholesale_id`)
      GROUP BY 1
    )
    WHERE n_id > 1
  )
)
, match_result as (
  SELECT DISTINCT
    regexp_replace(id_source_2, '[.]0$', '') as id_source_2,
    regexp_replace(id_source_1, '[.]0$', '') as id_source_1,
    original_request,
    upper(m.iso_code) as country_iso,
    match_quality,
    fm_mean
  FROM `trusted.match_result` m
  WHERE list_type = "top_list"
    AND original_request IN (
      'wholesale_company_erp_establishment',
      'wholesale_company_pos_establishment'
    )
    AND _valid_flag IS TRUE
    AND upper(m.iso_code) = '{iso2}'
)
, crm_wholesale_match as (
  SELECT DISTINCT
    country_iso,
    cast(id_source_1 as INT64) as wholesale_id,
    id_source_2 as establishment_id,
    IF(
      ROW_NUMBER() OVER (PARTITION BY id_source_1 ORDER BY match_quality ASC, fm_mean DESC) = 1
      AND ROW_NUMBER() OVER (PARTITION BY id_source_2 ORDER BY match_quality ASC, fm_mean DESC) = 1,
      true, false
    ) as valid
  FROM match_result
  WHERE original_request = "wholesale_company_erp_establishment"
    AND id_source_2 NOT IN (SELECT establishment_id FROM crm_clean_id)
    AND id_source_1 NOT IN (SELECT wholesale_id FROM crm_clean_id)
)
, output as (
  SELECT CAST(wholesale_id as int64) as wholesale_id, establishment_id
  FROM crm_clean_id
  UNION ALL
  SELECT CAST(wholesale_id as int64) as wholesale_id, establishment_id
  FROM crm_wholesale_match
  WHERE valid IS TRUE
)
, crm_wholesale_match_ext as (
  SELECT
    mcc.iso_code as country_iso,
    mcc.unique_wholesale_id,
    o.wholesale_id,
    mcc.cust_no,
    mcc.home_store_id,
    o.establishment_id,
    wholesale_id_map.account_id,
    mcc.status_cd
  FROM output o
  JOIN `refined.analytical_wholesale_customers_{iso2}` mcc
    ON cast(o.wholesale_id as int64) = mcc.wholesale_id
  LEFT JOIN wholesale_id_map
    ON mcc.unique_wholesale_id = wholesale_id_map.unique_wholesale_id
)
, asset as (
  SELECT establishment_id, asset_created_dt, product_code, asset_referrer,
    rank() OVER (
      PARTITION BY account_id, establishment_id, partner_id
      ORDER BY asset_created_dt DESC
    ) rank
  FROM `dwh_project.product_spot.erp_asset`
  WHERE product_code IN (
      'SUB_Starter', 'SUB_Professional', 'SUB_Premium', 'SUB_ProfOrder'
    )
    AND asset_status = "Active"
    AND istestdata IS FALSE
    AND lower(product_name) LIKE '%subscription%'
  UNION ALL
  SELECT establishment_id, asset_created_dt, product_code, asset_referrer,
    rank() OVER (
      PARTITION BY account_id, establishment_id, partner_id
      ORDER BY asset_created_dt DESC
    ) rank
  FROM `dwh_project.product_spot.erp_asset`
  WHERE product_code LIKE 'POS_L_Lic%'
    AND asset_status = "Active"
    AND istestdata IS FALSE
)
, asset_referrer as (
  SELECT
    establishment_id,
    MAX(IF(product_code LIKE 'POS_L_Lic%', asset_referrer, null)) as POS_Referrer,
    MAX(IF(product_code NOT LIKE "POS_L_Package", asset_referrer, null)) as Bundle_Referrer
  FROM asset
  WHERE rank = 1
  GROUP BY 1
)
, platform_df as (
  SELECT DISTINCT
    m.country_iso,
    m.account_id as wholesale_account_id,
    m.cust_no,
    m.home_store_id,
    m.wholesale_id,
    m.status_cd,
    cb.date_acquisition,
    cb.date_deletion,
    rank() OVER (
      PARTITION BY m.wholesale_id
      ORDER BY
        cb.SUB_createdDate DESC,
        cb.has_POS DESC,
        cb.has_Order DESC,
        cb.has_Reservation DESC,
        cb.has_Website DESC,
        cb.establishment_active DESC,
        cb.has_WebListing DESC,
        Order_createdDate DESC,
        Reservation_createdDate DESC,
        Website_createdDate DESC,
        date_acquisition DESC
    ) as rank,
    ifnull(cb.establishment_active, 0) as platform_active_customer,
    CASE
      WHEN lower(cb.SUB_type) LIKE '%premium%' THEN 'Premium'
      WHEN lower(cb.SUB_type) LIKE '%proforder%' THEN 'Pro. Order'
      WHEN lower(cb.SUB_type) LIKE '%profession%' THEN 'Pro. Reservation'
      WHEN lower(cb.SUB_type) LIKE '%starter%' THEN 'Starter'
    END as platform_bundle,
    cb.SUB_createdDate as platform_bundle_timestamp,
    cb.has_Website as has_Website,
    cb.has_Reservation as has_Reservation,
    cb.has_Order as has_Order,
    cb.has_WebListing as has_Weblisting,
    cb.has_POS as has_POS_flag,
    cb.has_Pay as has_Pay,
    cb.Website_createdDate,
    cb.Website_deletedDate,
    cb.Reservation_createdDate,
    cb.Reservation_deletedDate,
    cb.Order_createdDate,
    cb.Order_deletedDate,
    cb.WebListing_createdDate,
    cb.WebListing_deletedDate,
    cb.POS_createdDate,
    cb.POS_deletedDate,
    cb.Pay_createdDate,
    cb.Pay_deletedDate,
    cb.SUB_Starter_CreatedDate,
    cb.SUB_Starter_DisabledDate,
    cb.SUB_Prof_CreatedDate,
    cb.SUB_Prof_DisabledDate,
    cb.SUB_ProfOrder_CreatedDate,
    cb.SUB_ProfOrder_DisabledDate,
    cb.SUB_Premium_CreatedDate,
    cb.SUB_Premium_DisabledDate,
    ar.POS_Referrer,
    ar.Bundle_Referrer
  FROM crm_wholesale_match_ext m
  JOIN `refined.platform_customer_base_establishment` cb
    USING (establishment_id)
  LEFT JOIN `dwh_project.product_spot.erp_establishment` est
    ON est.establishment_id = cb.establishment_id
  LEFT JOIN asset_referrer ar
    ON est.establishment_id = ar.establishment_id
)
, pos_active as (
  SELECT DISTINCT pos_id, POS_createdDate
  FROM `refined.platform_customer_base_establishment`
  WHERE has_POS = 1
    AND pos_id IS NOT NULL
)
, pos_wholesale_match as (
  SELECT DISTINCT
    m.country_iso,
    wholesale_id_map.account_id as wholesale_account_id,
    mcc.cust_no,
    mcc.home_store_id,
    cast(id_source_1 as INT64) as wholesale_id,
    1 as has_POS,
    min(POS_createdDate) as POS_timestamp,
    mcc.status_cd
  FROM match_result m
  JOIN pos_active b
    ON lower(m.id_source_2) = lower(b.pos_id)
  JOIN `refined.analytical_wholesale_customers_{iso2}` mcc
    ON cast(m.id_source_1 as int64) = mcc.wholesale_id
  LEFT JOIN wholesale_id_map
    ON mcc.unique_wholesale_id = wholesale_id_map.unique_wholesale_id
  WHERE original_request = "wholesale_company_pos_establishment"
  GROUP BY ALL
)
, final_output as (
  SELECT
    ifnull(m.country_iso, b.country_iso) as country_iso,
    ifnull(m.wholesale_id, b.wholesale_id) as wholesale_id,
    ifnull(m.cust_no, b.cust_no) as cust_no,
    ifnull(m.home_store_id, b.home_store_id) as home_store_id,
    max(ifnull(platform_active_customer, 1)) as platform_active_customer,
    max(platform_bundle) as platform_bundle,
    max(platform_bundle_timestamp) as platform_bundle_timestamp,
    max(ifnull(has_Reservation, 0)) as has_Reservation,
    max(ifnull(has_Website, 0)) as has_Website,
    max(ifnull(has_Weblisting, 0)) as has_Weblisting,
    max(ifnull(has_Order, 0)) as has_Order,
    max(ifnull(has_Pay, 0)) as has_Pay,
    max(Website_createdDate) as Website_creation_ts,
    max(Reservation_createdDate) as Reservation_creation_ts,
    max(WebListing_createdDate) as WebListing_creation_ts,
    max(Order_createdDate) as Order_creation_ts,
    max(Website_deletedDate) as Website_deletion_ts,
    max(Reservation_deletedDate) as Reservation_deletion_ts,
    max(WebListing_deletedDate) as WebListing_deletion_ts,
    max(Order_deletedDate) as Order_deletion_ts,
    max(POS_deletedDate) as POS_deletion_ts,
    max(SUB_Starter_CreatedDate) as Starter_creation_ts,
    max(SUB_Starter_DisabledDate) as Starter_deletion_ts,
    max(SUB_Prof_CreatedDate) as ProfReservation_creation_ts,
    max(SUB_Prof_DisabledDate) as ProfReservation_deletion_ts,
    max(SUB_ProfOrder_CreatedDate) as ProfOrder_creation_ts,
    max(SUB_ProfOrder_DisabledDate) as ProfOrder_deletion_ts,
    max(SUB_Premium_CreatedDate) as Premium_creation_ts,
    max(SUB_Premium_DisabledDate) as Premium_deletion_ts,
    max(POS_Referrer) as POS_Referrer,
    max(Bundle_Referrer) as Bundle_Referrer,
    max(ifnull(m.status_cd, b.status_cd)) as status_cd,
    ifnull(m.wholesale_account_id, b.wholesale_account_id) as wholesale_account_id,
    max(greatest(ifnull(m.has_POS_flag, 0), ifnull(b.has_POS, 0))) as has_POS_flag,
    max(date_acquisition) as date_acquisition,
    max(date_deletion) as date_deletion,
    max(ifnull(m.POS_createdDate, b.POS_timestamp)) as POS_creation_ts,
    max(Pay_createdDate) as Pay_creation_ts,
    max(Pay_deletedDate) as Pay_deletion_ts,
    0 as has_Menukit,
    0 as has_POS,
    cast(null as timestamp) as POS_timestamp
  FROM platform_df m
  FULL JOIN pos_wholesale_match b
    ON m.wholesale_id = b.wholesale_id
  WHERE rank = 1 OR rank IS NULL
  GROUP BY country_iso, wholesale_id, cust_no, home_store_id, wholesale_account_id
)
SELECT * EXCEPT(status_cd) FROM final_output WHERE country_iso != "HR"
UNION ALL
SELECT * EXCEPT(status_cd) FROM final_output
WHERE country_iso = "HR" AND status_cd = 1
"""


if __name__ == "__main__":
    q = PlatformCustomer.get_insert_query("ES")
    assert "wholesale_id" in q
    assert "ES" in PlatformCustomer.get_send_query("ES")
    print("query module OK; insert SQL length:", len(q))
