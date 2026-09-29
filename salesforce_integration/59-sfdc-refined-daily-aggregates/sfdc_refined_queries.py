"""SQL builders for Salesforce-facing refined daily aggregates.

Each method returns a BigQuery Standard SQL string. Project id is
parameterized so DEV/PROD can share the same builders.

Source (read-only): inline queries in ``dags/etl_refined_salesforce.py``.
"""

from __future__ import annotations


class SfdcRefinedQueries:
    """Static SQL for the eight ``refined_salesforce`` snapshot tables."""

    @staticmethod
    def orders_aggregated() -> str:
        # Daily order counts + running total per establishment.
        return """
WITH daily_orders AS (
    SELECT
        establishment_uid,
        sfdc_establishment_id,
        DATE(date_added) AS order_date,
        currency,
        COUNT(DISTINCT CASE WHEN order_completed IS TRUE THEN order_id END)
            AS completed_orders_number,
        SUM(CASE WHEN order_completed IS TRUE THEN order_total ELSE 0 END)
            AS completed_orders_revenue_lcu,
        SUM(CASE WHEN order_completed IS TRUE THEN order_total_eur ELSE 0 END)
            AS completed_orders_revenue_eur
    FROM `refined.vw_analytical_order_orders_actual`
    GROUP BY 1, 2, 3, 4
),
cumulative_totals AS (
    SELECT
        establishment_uid,
        sfdc_establishment_id,
        order_date,
        currency,
        completed_orders_number,
        completed_orders_revenue_lcu,
        completed_orders_revenue_eur,
        SUM(completed_orders_number) OVER (
            PARTITION BY establishment_uid, sfdc_establishment_id
            ORDER BY order_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS total_orders_number
    FROM daily_orders
)
SELECT
    establishment_uid,
    sfdc_establishment_id,
    order_date,
    currency,
    completed_orders_number,
    total_orders_number,
    completed_orders_revenue_lcu,
    completed_orders_revenue_eur
FROM cumulative_totals
"""

    @staticmethod
    def reservations_aggregated(project_id: str) -> str:
        return f"""
SELECT
    country_code,
    salesforce_id,
    date,
    SUM(reservations) AS total_reservations,
    SUM(success_reservations) AS success_reservations,
    SUM(customer_seated_alltime) AS customer_seated_alltime,
    SUM(customer_seated_last_4_weeks) AS customer_seated_last_4_weeks
FROM (
    SELECT
        salesforce_id,
        b.country_code,
        DATE(a.creation_date) AS date,
        COUNT(DISTINCT id_sk) AS reservations,
        IF(status = "DONE" OR status = "CONFIRMED", COUNT(DISTINCT id_sk), 0)
            AS success_reservations,
        IFNULL(
            SUM(
                CASE
                    WHEN status = "DONE" OR status = "CONFIRMED"
                    THEN capacity
                    ELSE 0
                END
            ),
            0
        ) AS customer_seated_alltime,
        IFNULL(
            SUM(
                CASE
                    WHEN (status = "DONE" OR status = "CONFIRMED")
                        AND DATE(a.creation_date)
                            >= DATE_SUB(CURRENT_DATE(), INTERVAL 4 WEEK)
                    THEN capacity
                    ELSE 0
                END
            ),
            0
        ) AS customer_seated_last_4_weeks
    FROM `{project_id}.trusted_views.rt_reservations` a
    JOIN `refined.analytical_rt_establishments_actual` b
        USING (establishment_id_sk)
    JOIN (
        SELECT DISTINCT establishment_id
        FROM `{project_id}.refined.customer_base_establishment`
        WHERE reservation_created_date IS NOT NULL
    ) cb
        ON b.salesforce_id = cb.establishment_id
    WHERE status != "FREE"
    GROUP BY 1, 2, 3, status
)
GROUP BY 1, 2, 3
"""

    @staticmethod
    def subscription_billing_info(project_id: str) -> str:
        return f"""
SELECT
    s.establishment_id,
    billing_start_date,
    product_id,
    start_date,
    order_id,
    uuid AS subscription_id
FROM `{project_id}.trusted_views.subscription_history` sh
LEFT JOIN `{project_id}.trusted_views.subscriptions` s
    ON s.id_sk = sh.subscription_id_sk
WHERE sh.start_date < sh.end_date
  AND CURRENT_TIMESTAMP() >= start_date
  AND billing_start_date IS NOT NULL
"""

    @staticmethod
    def voucher_info(project_id: str) -> str:
        return f"""
SELECT DISTINCT
    vouch.id AS voucher_id,
    country.code AS iso_code,
    merchant.code AS merchant,
    vouch.code,
    description.description,
    vouch.start_date,
    vouch.end_date,
    vouch.active,
    vouch.one_time_charge_percentage,
    vouch.recurring_charge_percentage,
    vouch.one_time_charge_reduction,
    vouch.recurring_charge_reduction,
    vouch.reduction_grant_month
FROM `{project_id}.trusted.pc_vouchers` vouch
LEFT JOIN `{project_id}.trusted.pc_vouchers2countries` country_voucher
    ON vouch.id_sk = country_voucher.voucher_id_sk
LEFT JOIN `{project_id}.trusted.pc_countries` country
    ON country_voucher.country_id_sk = country.id_sk
LEFT JOIN `{project_id}.trusted.pc_vouchers2merchants` vouchers_merchant
    ON vouch.id_sk = vouchers_merchant.voucher_id_sk
LEFT JOIN `{project_id}.trusted.pc_merchants` merchant
    ON vouchers_merchant.merchant_id_sk = merchant.id_sk
LEFT JOIN `{project_id}.trusted.pc_vouchersloc` description
    ON description.id_id = vouch.id
WHERE vouch._valid_flag
  AND country._valid_flag
  AND merchant._valid_flag
  AND vouchers_merchant._valid_flag
  AND country_voucher._valid_flag
  AND description._valid_flag
  AND country.id_sk = merchant.country_id_sk
  AND description.lang = "en"
ORDER BY voucher_id
"""

    @staticmethod
    def sfdc_odoo_export(project_id: str) -> str:
        # CRM establishment-revenue grain: Odoo WSL lines joined to SFDC
        # assets. QUALIFY + MD5 rowhash keep Salesforce upserts stable.
        return f"""
WITH t1 AS (
    SELECT
        ast.AccountId AS SalesforceAccountID__c,
        ast.Id AS Asset__c,
        CAST(odoo.BookingDate AS STRING) AS BookingDate__c,
        MIN(odoo.Currency) AS CurrencyIsoCode,
        ast.Establishment__c,
        est.establishment_id AS EstablishmentUID__c,
        SUM(odoo.NetPriceEUR) AS NetPrice__c,
        CAST(odoo.OdooID AS STRING) AS OdooId__c,
        MIN(ast.OrderUID__c) AS OrderUID__c,
        IF(MIN(odoo.PaymentStatus) = "Paid", "TRUE", "FALSE") AS Paid__c,
        MIN(odoo.ParentBill) AS ParentBill__c,
        MIN(ast.productid) AS Product__c,
        MIN(ast.ProductCode__c) AS ProductCode__c,
        SUM(odoo.Quantity) AS Quantity__c,
        ROUND((SUM(odoo.NetPrice) - SUM(odoo.UnitPrice)), 2) AS VATAmount__c,
        MIN(ActualDeliveryStart) AS ActualDeliveryStart,
        MIN(ActualDeliveryEnd) AS ActualDeliveryEnd
    FROM (
        SELECT
            x.AccountId,
            x.Id,
            x.Establishment__c,
            x.OrderUID__c,
            x.ProductCode__c,
            y.id AS productid
        FROM `{project_id}.trusted_views.sfdc_asset` x
        LEFT JOIN `trusted_views.sfdc_product` y
            ON x.productcode__c = y.productcode
    ) ast
    LEFT JOIN `{project_id}.refined.analytical_sfdc_establishment_actual` est
        ON ast.Establishment__c = est.sfdc_internal_establishment_id
    INNER JOIN `{project_id}.trusted_views.odoo_wsl_invoice_lines` odoo
        ON ast.OrderUID__c = odoo.SalesforceOrderID
        AND odoo.ProductBaseCode = ast.productcode__c
    GROUP BY 1, 2, 3, 5, 6, 8
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY OdooId__c ORDER BY OdooId__c
    ) = 1
)
SELECT
    ar.id AS establishmentrevenueid,
    t1.*,
    TO_HEX(MD5(CONCAT(
        IFNULL(CAST(t1.SalesforceAccountID__c AS STRING), ''), '|',
        IFNULL(CAST(t1.Asset__c AS STRING), ''), '|',
        IFNULL(CAST(t1.BookingDate__c AS STRING), ''), '|',
        IFNULL(CAST(t1.CurrencyIsoCode AS STRING), ''), '|',
        IFNULL(CAST(t1.Establishment__c AS STRING), ''), '|',
        IFNULL(CAST(t1.EstablishmentUID__c AS STRING), ''), '|',
        IFNULL(CAST(t1.NetPrice__c AS STRING), ''), '|',
        IFNULL(CAST(t1.OdooId__c AS STRING), ''), '|',
        IFNULL(CAST(t1.OrderUID__c AS STRING), ''), '|',
        IFNULL(CAST(t1.Paid__c AS STRING), ''), '|',
        IFNULL(CAST(t1.ParentBill__c AS STRING), ''), '|',
        IFNULL(CAST(t1.Product__c AS STRING), ''), '|',
        IFNULL(CAST(t1.ProductCode__c AS STRING), ''), '|',
        IFNULL(CAST(t1.Quantity__c AS STRING), ''), '|',
        IFNULL(CAST(t1.VATAmount__c AS STRING), '')
    ))) AS _rowhash
FROM t1
LEFT JOIN `{project_id}.trusted_staging.sfdc_establishmentrevenue` ar
    ON t1.SalesforceAccountID__c = ar.SalesforceAccountID__c
    AND t1.Establishment__c = ar.Establishment__c
    AND t1.EstablishmentUID__c = ar.EstablishmentUID__c
    AND t1.Asset__c = ar.Asset__c
    AND DATE(t1.BookingDate__c) = ar.BookingDate__c
"""

    @staticmethod
    def odoo_wsl_invoice_lines_copy(project_id: str) -> str:
        return f"""
SELECT *
FROM `{project_id}.trusted_views.odoo_wsl_invoice_lines`
"""

    @staticmethod
    def app_login(project_id: str) -> str:
        return f"""
SELECT DISTINCT
    traffic_establishment_id AS establishment_sfid,
    MIN(visit_start_time_gmt_dt) AS first_app_login_date,
    MAX(visit_start_time_gmt_dt) AS last_app_login_date,
    COUNT(
        DISTINCT IF(
            visit_start_time_gmt_dt
                >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY),
            visit_start_time_gmt_dt,
            NULL
        )
    ) AS logins_last_30_days
FROM `{project_id}.refined.app_datafeed` feed
WHERE LOWER(CAST(mobileappid AS STRING)) NOT LIKE "%order%"
  AND traffic_establishment_id IS NOT NULL
GROUP BY traffic_establishment_id
"""

    @staticmethod
    def pos_transactions_aggregated(project_id: str) -> str:
        # UNION DISTINCT across country POS payment-item tables, then join
        # SFDC/Odoo POS licenses. Demo stores filtered out.
        countries = ("DE", "FR", "IT", "ES")
        union_parts = []
        for cc in countries:
            union_parts.append(
                f"""
SELECT DISTINCT
    persistenceId AS persistence_id,
    storeId AS store_id,
    storeLicenseId AS store_license_id,
    receiptId AS receipt_id,
    WorkDay AS workday,
    receiptTotalAmount AS receipt_total_amount
FROM `{project_id}.trusted.pos_payment_items_{cc}`
WHERE storeLicenseId IS NOT NULL
  AND LOWER(storeId) NOT LIKE '%demo%'
"""
            )
        transactions_sql = "\nUNION DISTINCT\n".join(union_parts)
        return f"""
WITH transactions AS (
{transactions_sql}
),
sfdc_pos_estb AS (
    SELECT DISTINCT
        a.establishment_id,
        a.country_code,
        a.POS_license_id
    FROM `{project_id}.product_spot.odoo_asset` a
    LEFT JOIN `{project_id}.product_spot.odoo_establishment` b
        ON a.establishment_id = b.establishment_id
    WHERE product_code LIKE 'POS_L%'
      AND a.pos_license_id IS NOT NULL
)
SELECT
    a.country_code,
    a.establishment_id,
    store_license_id,
    workday AS order_date,
    COUNT(DISTINCT CONCAT(b.persistence_id, b.store_id, b.receipt_id))
        AS completed_transactions
FROM sfdc_pos_estb a
LEFT JOIN transactions b
    ON b.store_license_id = POS_license_id
GROUP BY 1, 2, 3, 4
"""


if __name__ == "__main__":
    q = SfdcRefinedQueries
    print("-- orders_aggregated")
    print(q.orders_aggregated()[:200], "...")
    print("-- reservations_aggregated")
    print(q.reservations_aggregated("dwh_project")[:200], "...")
