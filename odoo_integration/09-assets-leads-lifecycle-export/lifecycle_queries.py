"""
SQL helpers for the Odoo / CRM lead + asset lifecycle event-export DAG.

Three refined tables, three delta filters. Lead and asset use SCD Type 2
validity columns (_valid_flag + _valid_from). Voucher codes use a
created-date window because that table is not modelled the same way.

Source (read-only):
  dags/horeca_digital/dana_odoo_assets_leads_lifecycle_export.py
  (get_Odoo_*_export_query methods)
"""


class LifecycleQueries:
    """Static builders for the three FR delta SELECTs."""

    LEADS_TABLE = "`refined.odoo_leads_lifecycle`"
    ASSETS_TABLE = "`refined.odoo_assets_lifecycle`"
    VOUCHERS_TABLE = "`refined.odoo_voucher_code`"

    @staticmethod
    def get_leads_export_query(country: str) -> str:
        """
        SCD-aware lead delta for one country.

        Only current versions whose validity window started today. That is
        how the refined model surfaces creates + status changes without a
        separate hist/keyhash compare on the Composer side.
        """
        return f"""
SELECT DISTINCT
  IFNULL(CAST(lead_id AS STRING), '') AS lead_id,
  IFNULL(CAST(converted_account_id AS STRING), '') AS converted_account_id,
  IFNULL(CAST(lead_referrer AS STRING), '') AS lead_referrer,
  IFNULL(CAST(store AS STRING), '') AS store,
  IFNULL(CAST(metro_id AS STRING), '') AS metro_id,
  IFNULL(CAST(customer_id_sam AS STRING), '') AS customer_id_sam,
  IFNULL(CAST(establishment_name AS STRING), '') AS establishment_name,
  IFNULL(CAST(lead_full_name AS STRING), '') AS lead_full_name,
  IFNULL(CAST(product_name AS STRING), '') AS product_name,
  IFNULL(CAST(lead_creation_date AS STRING), '') AS lead_creation_date,
  IFNULL(CAST(closing_date AS STRING), '') AS closing_date,
  IFNULL(CAST(lead_source AS STRING), '') AS lead_source,
  IFNULL(CAST(status AS STRING), '') AS status,
  IFNULL(CAST(reason_lost AS STRING), '') AS reason_lost,
  IFNULL(CAST(reason_lost2 AS STRING), '') AS reason_lost2,
  IFNULL(CAST(converted_contact_id AS STRING), '') AS converted_contact_id,
  IFNULL(CAST(lead_owner_name AS STRING), '') AS lead_owner_name,
  IFNULL(CAST(channel_v2 AS STRING), '') AS channel_v2,
  IFNULL(CAST(asset_creation_date AS STRING), '') AS asset_creation_date,
  IFNULL(CAST(activated_by AS STRING), '') AS activated_by,
  IFNULL(CAST(lead_email AS STRING), '') AS lead_email,
  IFNULL(CAST(lead_street AS STRING), '') AS lead_street,
  IFNULL(CAST(lead_city AS STRING), '') AS lead_city,
  IFNULL(CAST(lead_postal_code AS STRING), '') AS lead_postal_code,
  IFNULL(CAST(lead_country_code AS STRING), '') AS lead_country_code,
  IFNULL(CAST(_ldts AS DATE), NULL) AS _ldts,
  IFNULL(CAST(ecom_traffic_source AS STRING), '') AS ecom_traffic_source,
  IFNULL(CAST(asset_id AS STRING), '') AS asset_id
FROM {LifecycleQueries.LEADS_TABLE}
WHERE _valid_flag = TRUE
  AND UPPER(lead_country_code) = '{country}'
  AND DATE(_valid_from) >= CURRENT_DATE()
""".strip()

    @staticmethod
    def get_assets_export_query(country: str) -> str:
        """
        SCD-aware asset / subscription delta for one country.

        Same validity window as leads. odoo_Onboarding_status is selected
        as-is; the Avro encoder maps it onto the schema field
        odoo_Onboarding_flag (source naming quirk kept on purpose).
        """
        return f"""
SELECT DISTINCT
  account_id,
  mcc_metro_id,
  mcc_home_store_id,
  establishment_id,
  CAST(sfdc_internal_establishment_id AS STRING) AS sfdc_internal_establishment_id,
  country_code,
  full_vat,
  vat_verified,
  company_name,
  establishment_name,
  street,
  postalcode,
  city,
  establishment_address,
  first_name,
  last_name,
  mobilephone,
  email,
  manager_information,
  dish_terms_conditions,
  asset_UID,
  subscription_id,
  asset_channel_v2,
  asset_status,
  asset_name,
  asset_creation_date,
  asset_disabled_date,
  reason_of_cancellation,
  text_reason_of_cancellation,
  asset_onboarded,
  asset_onboarding_date,
  commitment_period,
  asset_referrer,
  price,
  voucher_code,
  voucher_reduction_grant_month,
  onetime_percentage,
  recurring_percentage,
  onetime_original_net_price,
  recurring_original_net_price,
  _ldts,
  asset_activated_by,
  asset_migrated,
  asset_onboarded_sfdc,
  asset_onboarding_date_sfdc,
  product_code,
  country_specific_code,
  CAST(asset_install_date AS STRING) AS asset_install_date,
  is_subscription,
  assignees,
  quantity,
  lead_id,
  created_from_lead,
  odoo_OnboardingDate,
  odoo_Onboarding_status,
  odoo_metro_id,
  odoo_store_id,
  IF(establishment_active = 1, TRUE, FALSE) AS establishment_active
FROM {LifecycleQueries.ASSETS_TABLE}
WHERE _valid_flag = TRUE
  AND UPPER(country_code) = '{country}'
  AND DATE(_valid_from) >= CURRENT_DATE()
""".strip()

    @staticmethod
    def get_voucher_code_export_query(country: str) -> str:
        """
        Voucher / discount codes created since yesterday.

        Not SCD-filtered — the refined voucher table is closer to a fact
        extract. Window is yesterday+today so a morning miss still catches
        late previous-day creates on the afternoon run.
        """
        return f"""
SELECT
  CAST(asset_id AS STRING) AS asset_id,
  CAST(sale_order_id AS STRING) AS sale_order_id,
  partner_id,
  CAST(establishment_id AS STRING) AS establishment_id,
  CAST(order_id AS STRING) AS order_id,
  CAST(subscription_id AS STRING) AS subscription_id,
  CAST(product_code AS STRING) AS product_code,
  CAST(discount_code AS STRING) AS discount_code,
  CAST(discount_desc AS STRING) AS discount_desc,
  price,
  recurring_invoice,
  is_subscription,
  CAST(asset_installation_date AS STRING) AS asset_installation_date,
  CAST(asset_created_date AS STRING) AS asset_created_date,
  CAST(country_code AS STRING) AS country_code
FROM {LifecycleQueries.VOUCHERS_TABLE}
WHERE UPPER(country_code) = '{country}'
  AND DATE(asset_created_date) >= CURRENT_DATE() - 1
""".strip()


if __name__ == "__main__":
    print("--- leads ---")
    print(LifecycleQueries.get_leads_export_query("FR"))
    print("--- assets ---")
    print(LifecycleQueries.get_assets_export_query("FR"))
    print("--- vouchers ---")
    print(LifecycleQueries.get_voucher_code_export_query("FR"))
