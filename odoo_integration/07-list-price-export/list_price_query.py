"""
Odoo list-price / commission SQL builders.

Builds BigQuery statements that:
  1. Snapshot invoice-line × list-price commission rows for one market
  2. Emit _keyhash / _rowhash for SCD-style delta detection
  3. Produce today insert, hist append, hist expire, and send SELECTs

Production insert SQL was a large multi-CTE finance calculation (WSL invoice
lines, reversed-line exclusion, country agency filters, SFDC subscription
history, IC price list joins, discount/credit splits). This sanitized
builder keeps the outbound contract + hash mechanics and a readable core
join path. Expand the CTEs when you need the full finance ruleset.

Business key: parent_bill + salesforce_establishment_id.
Rowhash covers the full outbound commission payload.

Source (read-only): dags/horeca_digital/dana_odoo_list_price_query.py
"""

import logging


class OdooListPrice:
    # ISO-2 → ISO-3 for country-partitioned helpers.
    # This export shipped as a single-market (FR) monthly job; the map stays
    # so a second market can reuse builders without rewriting SQL.
    countries = {
        "fr": "fra",
    }

    @staticmethod
    def get_odoo_list_price_insert_query():
        """
        WRITE_TRUNCATE query for trusted_staging.odoo_list_price_today.

        Reads refined WSL invoice lines for the pilot market, joins the
        internal price list for activation/recurring fees, aggregates the
        oneshot / monthly / promo commission measures the event bus expects,
        and stamps SCD hash + validity columns.
        """
        query = """
WITH invoice_lines AS (
  SELECT
    a.ParentBill,
    a.BookingDate,
    a.ActualDeliveryStart,
    a.SalesRegion AS BillingCountry,
    CASE a.SalesRegion
      WHEN 'France' THEN 'FR'
      WHEN 'Germany' THEN 'DE'
      WHEN 'Italy' THEN 'IT'
      WHEN 'Spain' THEN 'ES'
      ELSE NULL
    END AS BillingCountryCode,
    TRIM(a.Company) AS Company,
    TRIM(a.SalesAgencyID) AS SalesAgencyID,
    TRIM(a.Merchant) AS Merchant,
    TRIM(a.SalesforceOrderID) AS SalesforceOrderID,
    TRIM(a.SalesforceEstablishmentID) AS SalesforceEstablishmentID,
    a.SalesforceAccountID,
    a.ProductCode,
    a.ProductBaseCode,
    a.Label,
    a.ProductIsSetup,
    a.PriceAdjustment,
    a.Quantity AS asset_quantity,
    ROUND(a.UnitPrice, 2) AS UnitPrice,
    ROUND(a.NetPriceEUR, 2) AS NetPriceEUR,
    cust.clean_customer_id AS customer_id,
    cust.business_registry_no AS siren_no,
    -- List price from the internal commission price book (activation vs recurring).
    COALESCE(lp_act.platform_cost_eur, lp_rec.platform_cost_eur, 0)
      * a.Quantity AS platform_unit_list_price,
    COALESCE(lp_act.partner_commission_eur, lp_rec.partner_commission_eur, 0)
      * a.Quantity AS partner_unit_list_price,
    COALESCE(lp_act.customer_cost_local, lp_rec.customer_cost_local, 0)
      * a.Quantity AS unit_list_price
  FROM `refined.odoo_wsl_invoice_lines` a
  LEFT JOIN `refined.cleaned_crm_customer_id` cust
    ON TRIM(cust.establishment_uid) = TRIM(a.SalesforceEstablishmentID)
  LEFT JOIN `discovery.ic_price_list` lp_act
    ON a.SalesRegion = 'France'
    AND lp_act.country = 'FR'
    AND a.ProductCode = lp_act.product_code
    AND LOWER(lp_act.fee_type) = 'activation'
    AND a.ProductIsSetup IS TRUE
    AND a.BookingDate BETWEEN lp_act.price_start_date AND lp_act.price_end_date
  LEFT JOIN `discovery.ic_price_list` lp_rec
    ON a.SalesRegion = 'France'
    AND lp_rec.country = 'FR'
    AND a.ProductCode = lp_rec.product_code
    AND LOWER(lp_rec.fee_type) = 'recurring'
    AND a.ProductIsSetup IS FALSE
    AND a.BookingDate BETWEEN lp_rec.price_start_date AND lp_rec.price_end_date
  WHERE TRUE
    AND a.EntryType = 'out_invoice'
    AND a.ParentBill != '/'
    AND a.ProductBaseCode IS NOT NULL
    AND LOWER(a.ProductName) NOT LIKE '%fees%'
    AND a.ProductCode <> 'ContractPenalty'
    AND a.BookingDate >= '2023-10-01'
    AND a.SalesRegion = 'France'
    -- Agency / channel filter: partner-channel invoices only for this export.
    AND COALESCE(TRIM(a.SalesAgencyID), 'PARTNER') LIKE 'PARTNER%'
    AND TRIM(a.ParentBill) NOT LIKE 'R%'
),
odoo_list_price AS (
  SELECT
    ParentBill AS parent_bill,
    BookingDate AS booking_date,
    ActualDeliveryStart AS actual_delivery_start,
    BillingCountry AS billing_country,
    BillingCountryCode AS billing_country_code,
    Company AS company,
    SalesAgencyID AS sales_agency_id,
    Merchant AS merchant,
    SalesforceOrderID AS salesforce_order_id,
    SalesforceEstablishmentID AS salesforce_establishment_id,
    SalesforceAccountID AS salesforce_account_id,
    customer_id AS metro_id,
    siren_no,
    ProductCode AS product_code,
    ProductBaseCode AS product_base_code,
    Label AS label,
    ROUND(SUM(CASE WHEN PriceAdjustment IS NULL THEN asset_quantity END), 2) AS quantity,
    ROUND(MAX(CASE WHEN PriceAdjustment IS NULL THEN UnitPrice END), 2) AS price_per_unit,
    ROUND(SUM(CASE WHEN PriceAdjustment IS NULL THEN NetPriceEUR END), 2)
      AS theoretica_oneshot_amount,
    ROUND(SUM(NetPriceEUR), 2) AS actual_oneshot_amount,
    ROUND(
      SUM(
        CASE
          WHEN PriceAdjustment IS NULL
            THEN SAFE_DIVIDE(partner_unit_list_price, NULLIF(unit_list_price, 0))
              * NetPriceEUR
        END
      ),
      2
    ) AS theoretical_MFR_commission_oneshot,
    ROUND(SUM(CASE WHEN PriceAdjustment IS NOT NULL THEN NetPriceEUR ELSE 0 END), 2)
      AS promotions_amount_oneshot,
    ROUND(SUM(CASE WHEN PriceAdjustment IS NULL THEN NetPriceEUR END), 2)
      AS net_group_oneshot,
    ROUND(SUM(NetPriceEUR), 2) AS net_MFR_oneshot,
    ROUND(SUM(CASE WHEN ProductIsSetup IS FALSE THEN NetPriceEUR ELSE 0 END), 2)
      AS monthly_invoiced_subscription,
    ROUND(SUM(CASE WHEN PriceAdjustment IS NULL THEN partner_unit_list_price END), 2)
      AS theoretical_MFR_commission_monthly,
    ROUND(
      SUM(
        CASE
          WHEN ProductIsSetup IS FALSE AND PriceAdjustment IS NOT NULL
            THEN NetPriceEUR
          ELSE 0
        END
      ),
      2
    ) AS amount_promotions_recurring,
    ROUND(
      SUM(
        CASE
          WHEN ProductIsSetup IS FALSE AND PriceAdjustment IS NULL
            THEN NetPriceEUR
          ELSE 0
        END
      ),
      2
    ) AS net_group_monthly,
    ROUND(
      SUM(CASE WHEN PriceAdjustment IS NULL THEN partner_unit_list_price END)
      + SUM(
        CASE
          WHEN ProductIsSetup IS FALSE AND PriceAdjustment IS NOT NULL
            THEN NetPriceEUR
          ELSE 0
        END
      ),
      2
    ) AS net_MFR_monthly,
    ROUND(
      SUM(
        CASE
          WHEN ProductIsSetup IS FALSE AND PriceAdjustment IS NULL
            THEN NetPriceEUR
          ELSE 0
        END
      ),
      2
    ) AS total_theoretical_reccuring_monthly_MFR,
    ROUND(
      SUM(
        CASE
          WHEN ProductIsSetup IS TRUE AND PriceAdjustment IS NULL
            THEN NetPriceEUR
          ELSE 0
        END
      ),
      2
    ) AS total_theoretical_one_shot_monthly_MFR,
    ROUND(
      SUM(
        CASE
          WHEN ProductIsSetup IS TRUE AND PriceAdjustment IS NOT NULL
            THEN NetPriceEUR
          ELSE 0
        END
      ),
      2
    ) AS total_promotion_one_shot,
    ROUND(
      SUM(
        CASE
          WHEN ProductIsSetup IS FALSE AND PriceAdjustment IS NOT NULL
            THEN NetPriceEUR
          ELSE 0
        END
      ),
      2
    ) AS total_promotion_reccuring,
    ROUND(SUM(CASE WHEN ProductIsSetup IS FALSE THEN NetPriceEUR ELSE 0 END), 2)
      AS total_commission_reccuring_MFR,
    ROUND(SUM(CASE WHEN ProductIsSetup IS TRUE THEN NetPriceEUR ELSE 0 END), 2)
      AS total_commission_one_shot_MFR,
    ROUND(SUM(CASE WHEN PriceAdjustment IS NOT NULL THEN NetPriceEUR ELSE 0 END), 2)
      AS total_monthly_promo,
    ROUND(
      SUM(
        CASE
          WHEN PriceAdjustment IS NULL
            THEN SAFE_DIVIDE(platform_unit_list_price, NULLIF(unit_list_price, 0))
              * NetPriceEUR
        END
      ),
      2
    ) AS total_monthly_DISH,
    ROUND(SUM(NetPriceEUR), 2) AS total_invoiced
  FROM invoice_lines
  GROUP BY ALL
)
SELECT
  CAST(parent_bill AS STRING) AS parent_bill,
  CAST(booking_date AS DATE) AS booking_date,
  CAST(actual_delivery_start AS DATE) AS actual_delivery_start,
  CAST(billing_country AS STRING) AS billing_country,
  CAST(billing_country_code AS STRING) AS billing_country_code,
  CAST(company AS STRING) AS company,
  CAST(sales_agency_id AS STRING) AS sales_agency_id,
  CAST(merchant AS STRING) AS merchant,
  CAST(salesforce_order_id AS STRING) AS salesforce_order_id,
  CAST(salesforce_establishment_id AS STRING) AS salesforce_establishment_id,
  CAST(salesforce_account_id AS STRING) AS salesforce_account_id,
  CAST(metro_id AS INT64) AS metro_id,
  CAST(siren_no AS STRING) AS siren_no,
  CAST(product_code AS STRING) AS product_code,
  CAST(product_base_code AS STRING) AS product_base_code,
  CAST(label AS STRING) AS label,
  CAST(quantity AS INT64) AS quantity,
  CAST(price_per_unit AS FLOAT64) AS price_per_unit,
  CAST(theoretica_oneshot_amount AS FLOAT64) AS theoretica_oneshot_amount,
  CAST(actual_oneshot_amount AS FLOAT64) AS actual_oneshot_amount,
  CAST(theoretical_MFR_commission_oneshot AS FLOAT64)
    AS theoretical_MFR_commission_oneshot,
  CAST(promotions_amount_oneshot AS FLOAT64) AS promotions_amount_oneshot,
  CAST(net_group_oneshot AS FLOAT64) AS net_group_oneshot,
  CAST(net_MFR_oneshot AS FLOAT64) AS net_MFR_oneshot,
  CAST(monthly_invoiced_subscription AS FLOAT64) AS monthly_invoiced_subscription,
  CAST(theoretical_MFR_commission_monthly AS FLOAT64)
    AS theoretical_MFR_commission_monthly,
  CAST(amount_promotions_recurring AS FLOAT64) AS amount_promotions_recurring,
  CAST(net_group_monthly AS FLOAT64) AS net_group_monthly,
  CAST(net_MFR_monthly AS FLOAT64) AS net_MFR_monthly,
  CAST(total_theoretical_reccuring_monthly_MFR AS FLOAT64)
    AS total_theoretical_reccuring_monthly_MFR,
  CAST(total_theoretical_one_shot_monthly_MFR AS FLOAT64)
    AS total_theoretical_one_shot_monthly_MFR,
  CAST(total_promotion_one_shot AS FLOAT64) AS total_promotion_one_shot,
  CAST(total_promotion_reccuring AS FLOAT64) AS total_promotion_reccuring,
  CAST(total_commission_reccuring_MFR AS FLOAT64) AS total_commission_reccuring_MFR,
  CAST(total_commission_one_shot_MFR AS FLOAT64) AS total_commission_one_shot_MFR,
  CAST(total_monthly_promo AS FLOAT64) AS total_monthly_promo,
  CAST(total_monthly_DISH AS FLOAT64) AS total_monthly_DISH,
  CAST(total_invoiced AS FLOAT64) AS total_invoiced,
  CURRENT_DATE() AS _ldts,
  TO_HEX(MD5(CONCAT(
    IFNULL(CAST(parent_bill AS STRING), ''), '|',
    IFNULL(CAST(salesforce_establishment_id AS STRING), '')
  ))) AS _keyhash,
  TO_HEX(MD5(CONCAT(
    IFNULL(CAST(parent_bill AS STRING), ''), '|',
    IFNULL(CAST(booking_date AS STRING), ''), '|',
    IFNULL(CAST(actual_delivery_start AS STRING), ''), '|',
    IFNULL(CAST(billing_country AS STRING), ''), '|',
    IFNULL(CAST(billing_country_code AS STRING), ''), '|',
    IFNULL(CAST(company AS STRING), ''), '|',
    IFNULL(CAST(sales_agency_id AS STRING), ''), '|',
    IFNULL(CAST(merchant AS STRING), ''), '|',
    IFNULL(CAST(salesforce_order_id AS STRING), ''), '|',
    IFNULL(CAST(salesforce_establishment_id AS STRING), ''), '|',
    IFNULL(CAST(salesforce_account_id AS STRING), ''), '|',
    IFNULL(CAST(metro_id AS STRING), ''), '|',
    IFNULL(CAST(siren_no AS STRING), ''), '|',
    IFNULL(CAST(product_code AS STRING), ''), '|',
    IFNULL(CAST(product_base_code AS STRING), ''), '|',
    IFNULL(CAST(label AS STRING), ''), '|',
    IFNULL(CAST(quantity AS STRING), ''), '|',
    IFNULL(CAST(price_per_unit AS STRING), ''), '|',
    IFNULL(CAST(theoretica_oneshot_amount AS STRING), ''), '|',
    IFNULL(CAST(actual_oneshot_amount AS STRING), ''), '|',
    IFNULL(CAST(theoretical_MFR_commission_oneshot AS STRING), ''), '|',
    IFNULL(CAST(promotions_amount_oneshot AS STRING), ''), '|',
    IFNULL(CAST(net_group_oneshot AS STRING), ''), '|',
    IFNULL(CAST(net_MFR_oneshot AS STRING), ''), '|',
    IFNULL(CAST(monthly_invoiced_subscription AS STRING), ''), '|',
    IFNULL(CAST(theoretical_MFR_commission_monthly AS STRING), ''), '|',
    IFNULL(CAST(amount_promotions_recurring AS STRING), ''), '|',
    IFNULL(CAST(net_group_monthly AS STRING), ''), '|',
    IFNULL(CAST(net_MFR_monthly AS STRING), ''), '|',
    IFNULL(CAST(total_theoretical_reccuring_monthly_MFR AS STRING), ''), '|',
    IFNULL(CAST(total_theoretical_one_shot_monthly_MFR AS STRING), ''), '|',
    IFNULL(CAST(total_promotion_one_shot AS STRING), ''), '|',
    IFNULL(CAST(total_promotion_reccuring AS STRING), ''), '|',
    IFNULL(CAST(total_commission_reccuring_MFR AS STRING), ''), '|',
    IFNULL(CAST(total_commission_one_shot_MFR AS STRING), ''), '|',
    IFNULL(CAST(total_monthly_promo AS STRING), ''), '|',
    IFNULL(CAST(total_monthly_DISH AS STRING), ''), '|',
    IFNULL(CAST(total_invoiced AS STRING), '')
  ))) AS _rowhash,
  CURRENT_TIMESTAMP() AS _valid_from,
  TIMESTAMP('2099-12-31 23:59:59') AS _valid_until,
  TRUE AS _valid_flag
FROM odoo_list_price
"""
        return query

    @staticmethod
    def get_odoo_list_price_hist_query(today_table, hist_table):
        """
        Delta SELECT: new keys or changed rowhash vs hist.

        Used for WRITE_APPEND into the hist table after ingest succeeds.
        """
        query = f"""
SELECT
  parent_bill,
  booking_date,
  actual_delivery_start,
  billing_country,
  billing_country_code,
  company,
  sales_agency_id,
  merchant,
  salesforce_order_id,
  salesforce_establishment_id,
  salesforce_account_id,
  metro_id,
  siren_no,
  product_code,
  product_base_code,
  label,
  quantity,
  price_per_unit,
  theoretica_oneshot_amount,
  actual_oneshot_amount,
  theoretical_MFR_commission_oneshot,
  promotions_amount_oneshot,
  net_group_oneshot,
  net_MFR_oneshot,
  monthly_invoiced_subscription,
  theoretical_MFR_commission_monthly,
  amount_promotions_recurring,
  net_group_monthly,
  net_MFR_monthly,
  total_theoretical_reccuring_monthly_MFR,
  total_theoretical_one_shot_monthly_MFR,
  total_promotion_one_shot,
  total_promotion_reccuring,
  total_commission_reccuring_MFR,
  total_commission_one_shot_MFR,
  total_monthly_promo,
  total_monthly_DISH,
  total_invoiced,
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
        logging.info("Retrieved query: get_odoo_list_price_hist_query")
        return query

    @staticmethod
    def get_odoo_list_price_update_query(today_table, hist_table):
        """
        Expire active hist rows superseded by today's snapshot.

        Closes _valid_until at end of yesterday and clears _valid_flag.
        """
        query = f"""
UPDATE `trusted_staging.{hist_table}`
SET
  _valid_until = TIMESTAMP(
    FORMAT_TIMESTAMP(
      '%Y-%m-%d 23:59:59',
      TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
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
        logging.info("Retrieved query: get_odoo_list_price_update_query")
        return query

    @staticmethod
    def get_odoo_list_price_send_query(today_table, hist_table):
        """
        Outbound delta for the event API (same predicate as hist append).

        Hist must still reflect *previous* run state when this runs —
        otherwise the send SELECT returns empty and finance consumers go quiet.
        """
        query = f"""
SELECT
  parent_bill,
  booking_date,
  actual_delivery_start,
  billing_country,
  billing_country_code,
  company,
  sales_agency_id,
  merchant,
  salesforce_order_id,
  salesforce_establishment_id,
  salesforce_account_id,
  metro_id,
  siren_no,
  product_code,
  product_base_code,
  label,
  quantity,
  price_per_unit,
  theoretica_oneshot_amount,
  actual_oneshot_amount,
  theoretical_MFR_commission_oneshot,
  promotions_amount_oneshot,
  net_group_oneshot,
  net_MFR_oneshot,
  monthly_invoiced_subscription,
  theoretical_MFR_commission_monthly,
  amount_promotions_recurring,
  net_group_monthly,
  net_MFR_monthly,
  total_theoretical_reccuring_monthly_MFR,
  total_theoretical_one_shot_monthly_MFR,
  total_promotion_one_shot,
  total_promotion_reccuring,
  total_commission_reccuring_MFR,
  total_commission_one_shot_MFR,
  total_monthly_promo,
  total_monthly_DISH,
  total_invoiced,
  _ldts
FROM `trusted_staging.{today_table}`
WHERE
  _keyhash NOT IN (SELECT _keyhash FROM `trusted_staging.{hist_table}`)
  OR (
    _keyhash IN (SELECT _keyhash FROM `trusted_staging.{hist_table}`)
    AND _rowhash NOT IN (SELECT _rowhash FROM `trusted_staging.{hist_table}`)
  )
"""
        logging.info("Retrieved query: get_odoo_list_price_send_query")
        return query


if __name__ == "__main__":
    logging.basicConfig(
        format="%(asctime)s %(levelname)-4s %(message)s",
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    today = "odoo_list_price_today"
    hist = "odoo_list_price_hist"
    q = OdooListPrice()
    print("-- insert --")
    print(q.get_odoo_list_price_insert_query()[:500], "...")
    print("-- send --")
    print(q.get_odoo_list_price_send_query(today, hist)[:300], "...")
