"""BigQuery SQL builders for product-suite API refined tables.

Source (read-only): ``dags/horeca_digital/DISH_api_query.py`` (``DISHapi``).

Each static method returns a SELECT that lands into ``refined.api_*`` via
``WRITE_TRUNCATE``. Production also UNION ALL'd a handful of demo
establishments remapped to synthetic SFIDs for partner UAT — kept here as
placeholder UUIDs so the remap pattern stays visible without real account IDs.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

# Illustrative demo remap only — replace with your UAT seed list.
_DEMO_SRC_A = "00000000-aaaa-1111-bbbb-000000000001"
_DEMO_DST_A = "11111111-aaaa-2222-bbbb-000000000001"
_DEMO_ACCT_A = "22222222-aaaa-3333-bbbb-000000000001"
_DEMO_SRC_B = "00000000-bbbb-1111-cccc-000000000002"
_DEMO_DST_B = "11111111-bbbb-2222-cccc-000000000002"
_DEMO_ACCT_B = "22222222-bbbb-3333-cccc-000000000002"


class ProductApiQueries:
    """SQL for API-facing refined tables consumed by product dashboards."""

    @staticmethod
    def get_establishment_query(project_name: str) -> str:
        query = f"""
        SELECT *
        FROM (
            SELECT DISTINCT
                establishment_sfid,
                map.account_id,
                IF(has_product_website = 1, TRUE, FALSE) AS has_product_website,
                IF(has_product_reservation = 1, TRUE, FALSE) AS has_product_reservation,
                IF(has_product_order = 1, TRUE, FALSE) AS has_product_order,
                IF(pos.establishment_id IS NOT NULL, TRUE, FALSE) AS has_product_pos,
                IFNULL(_flag_display, TRUE) AS _flag_display,
                CURRENT_TIMESTAMP() AS version_timestamp,
                establishment_sfid AS key
            FROM `{project_name}.refined.customer_base_establishment` cb
            LEFT JOIN `{project_name}.refined.dashboard_flag_display` USING (establishment_sfid)
            LEFT JOIN `{project_name}.refined.analytical_sfdc_establishment_actual` map
                ON map.establishment_id = cb.establishment_sfid
            LEFT JOIN (
                SELECT DISTINCT establishment_id
                FROM `{project_name}.product_spot.product_assets`
                WHERE product_code LIKE 'POS_L_Package%'
            ) pos ON pos.establishment_id = cb.establishment_sfid

            UNION ALL

            SELECT DISTINCT
                CASE
                    WHEN establishment_sfid = '{_DEMO_SRC_A}' THEN '{_DEMO_DST_A}'
                    WHEN establishment_sfid = '{_DEMO_SRC_B}' THEN '{_DEMO_DST_B}'
                END AS establishment_sfid,
                CASE
                    WHEN establishment_sfid = '{_DEMO_SRC_A}' THEN '{_DEMO_ACCT_A}'
                    WHEN establishment_sfid = '{_DEMO_SRC_B}' THEN '{_DEMO_ACCT_B}'
                END AS account_id,
                IF(has_product_website = 1, TRUE, FALSE) AS has_product_website,
                IF(has_product_reservation = 1, TRUE, FALSE) AS has_product_reservation,
                IF(has_product_order = 1, TRUE, FALSE) AS has_product_order,
                IF(pos.establishment_id IS NOT NULL, TRUE, FALSE) AS has_product_pos,
                TRUE AS _flag_display,
                CURRENT_TIMESTAMP() AS version_timestamp,
                establishment_sfid AS key
            FROM `{project_name}.refined.customer_base_establishment` cb
            LEFT JOIN `{project_name}.refined.dashboard_flag_display` USING (establishment_sfid)
            LEFT JOIN `{project_name}.refined.analytical_sfdc_establishment_actual` map
                ON map.establishment_id = cb.establishment_sfid
            LEFT JOIN (
                SELECT DISTINCT establishment_id
                FROM `{project_name}.product_spot.product_assets`
                WHERE product_code LIKE 'POS_L_Package%'
            ) pos ON pos.establishment_id = cb.establishment_sfid
            WHERE establishment_sfid IN ('{_DEMO_SRC_A}', '{_DEMO_SRC_B}')
        ) a
        WHERE establishment_sfid IS NOT NULL AND establishment_sfid <> ''
        """
        logger.info("Retrieved query: get_establishment_query")
        return query

    @staticmethod
    def get_order_query(project_name: str) -> str:
        query = f"""
        SELECT *
        FROM (
            SELECT DISTINCT
                sfdc_establishment_id AS establishment_sfid,
                account_id,
                order_id,
                order_status,
                CAST(order_total AS FLOAT64) AS order_total,
                CAST(currency AS STRING) AS currency,
                DATE(date_added) AS order_date,
                CURRENT_TIMESTAMP() AS version_timestamp,
                CONCAT(sfdc_establishment_id, '_', order_id) AS key
            FROM `{project_name}.refined.analytical_order_orders_actual` orders
            LEFT JOIN `{project_name}.refined.analytical_sfdc_establishment_actual` map
                ON map.establishment_id = orders.sfdc_establishment_id

            UNION ALL

            SELECT DISTINCT
                CASE
                    WHEN sfdc_establishment_id = '{_DEMO_SRC_A}' THEN '{_DEMO_DST_A}'
                    WHEN sfdc_establishment_id = '{_DEMO_SRC_B}' THEN '{_DEMO_DST_B}'
                END AS establishment_sfid,
                CASE
                    WHEN sfdc_establishment_id = '{_DEMO_SRC_A}' THEN '{_DEMO_ACCT_A}'
                    WHEN sfdc_establishment_id = '{_DEMO_SRC_B}' THEN '{_DEMO_ACCT_B}'
                END AS account_id,
                order_id,
                order_status,
                CAST(order_total AS FLOAT64) AS order_total,
                CAST(currency AS STRING) AS currency,
                DATE(date_added) AS order_date,
                CURRENT_TIMESTAMP() AS version_timestamp,
                CONCAT(sfdc_establishment_id, '_', order_id) AS key
            FROM `{project_name}.refined.analytical_order_orders_actual` orders
            LEFT JOIN `{project_name}.refined.analytical_sfdc_establishment_actual` map
                ON map.establishment_id = orders.sfdc_establishment_id
            WHERE sfdc_establishment_id IN ('{_DEMO_SRC_A}', '{_DEMO_SRC_B}')
        ) a
        WHERE establishment_sfid IS NOT NULL AND establishment_sfid <> ''
        """
        logger.info("Retrieved query: get_order_query")
        return query

    @staticmethod
    def get_dashboard_market_query(project_name: str) -> str:
        """CRM activity feed for one market dashboard (AlloyDB sync source)."""
        query = f"""
        WITH partner_map AS (
            SELECT DISTINCT
                partner.commercial_partner_id,
                leads.wholesale_account_identifier,
                COALESCE(partner.wholesale_id, leads.wholesale_id) AS wholesale_id,
                COALESCE(partner.store_id, leads.store_id) AS store_id,
                won_status
            FROM `{project_name}.refined_sales.odoo_res_partner` partner
            JOIN (
                SELECT
                    id,
                    partner_id,
                    wholesale_account_identifier,
                    wholesale_id,
                    store_id,
                    won_status,
                    create_date
                FROM `{project_name}.refined_sales.odoo_crm_lead`
                WHERE active = TRUE
            ) leads ON partner.id = leads.partner_id
        ),
        crm_lead_activities AS (
            SELECT DISTINCT
                activity.summary AS subject,
                activity.user_id AS assigned_id,
                activity.feedback AS description,
                oat.name.en_US AS type,
                activity.create_date AS created_date,
                CAST(NULL AS STRING) AS sub_type,
                COALESCE(apt_type.name, event.name) AS event_topic,
                activity.active AS is_closed,
                activity.res_id AS name_id,
                voip.direction AS outcome,
                activity.id AS activity_id,
                event.start_date AS start_date,
                partner.contact_id AS lead_account_id,
                COALESCE(
                    leads.wholesale_account_identifier,
                    partner_map.wholesale_account_identifier
                ) AS wholesale_account_identifier,
                users.id AS user_id,
                users.login AS email,
                users.profile_name,
                partner_map.won_status
            FROM `{project_name}.refined_sales.odoo_mail_activity` activity
            LEFT JOIN `{project_name}.refined_sales.odoo_crm_lead` leads
                ON activity.res_id = leads.id AND activity.res_model = 'crm.lead'
            LEFT JOIN `{project_name}.refined_sales.odoo_res_partner` partner
                ON partner.id = activity.res_id AND activity.res_model = 'res.partner'
            LEFT JOIN partner_map
                ON partner_map.commercial_partner_id = partner.commercial_partner_id
            LEFT JOIN (
                SELECT
                    ru.id,
                    ru.login,
                    STRING_AGG(g.name, ', ') AS profile_name
                FROM `{project_name}.refined_sales.odoo_res_users_role_line` rl
                JOIN `{project_name}.refined_sales.odoo_res_users_role` r ON r.id = rl.role_id
                JOIN `{project_name}.refined_sales.odoo_res_users` ru ON ru.id = rl.user_identifier
                JOIN `{project_name}.refined_sales.odoo_res_groups` g ON g.id = r.group_id
                GROUP BY 1, 2
            ) users ON users.id = activity.user_id
            LEFT JOIN `{project_name}.refined_sales.odoo_calendar_event` event
                ON event.id = activity.calendar_event_id
            LEFT JOIN `{project_name}.refined_sales.odoo_appointment_type` apt_type
                ON apt_type.id = event.appointment_type_id
            LEFT JOIN `{project_name}.refined_sales.odoo_voip_phonecall` voip
                ON voip.user_id = users.id
            JOIN `{project_name}.refined_sales.odoo_mail_activity_type` oat
                ON oat.id = activity.activity_type_id
            WHERE activity.res_model IN ('crm.lead', 'res.partner')
        )
        SELECT DISTINCT
            TO_HEX(MD5(CONCAT(
                CAST(activity_id AS STRING), '|',
                CAST(created_date AS STRING), '|',
                COALESCE(subject, 'No_Subject'), '|',
                COALESCE(outcome, 'No_Outcome'), '|',
                COALESCE(wholesale_account_identifier, 'No_wholesale_account_identifier'), '|',
                COALESCE(won_status, 'No_won_status')
            ))) AS unique_key,
            *
        FROM crm_lead_activities
        WHERE DATE(created_date) > '2024-01-01'
          AND event_topic IS NOT NULL
        """
        logger.info("Retrieved query: get_dashboard_market_query")
        return query

    @staticmethod
    def get_reservation_query(project_name: str) -> str:
        query = f"""
        SELECT *
        FROM (
            SELECT DISTINCT
                est.salesforce_id AS establishment_sfid,
                account_id,
                res.reservation_id_sk,
                capacity,
                CASE
                    WHEN res.origin NOT LIKE 'WIDGET'
                     AND res.origin NOT LIKE 'FACEBOOK'
                     AND res.origin NOT LIKE 'MAIL'
                     AND res.origin NOT LIKE 'RESERVE_WITH_GOOGLE'
                     AND res.origin NOT LIKE 'PHONE'
                     AND res.origin NOT LIKE 'WALKIN'
                    THEN 'UNKNOWN'
                    ELSE res.origin
                END AS origin,
                DATE(res.start_date) AS start_date,
                res.status,
                DATE(res.creation_date_dt) AS booking_date,
                CURRENT_TIMESTAMP() AS version_timestamp,
                CONCAT(est.salesforce_id, '_', res.reservation_id_sk) AS key
            FROM `{project_name}.refined.analytical_rt_establishments_actual` est
            LEFT JOIN `{project_name}.refined.analytical_rt_reservations_hist` res
                ON est.establishment_id_sk = res.establishment_id_sk
            LEFT JOIN `{project_name}.refined.analytical_sfdc_establishment_actual` map
                ON map.establishment_id = est.salesforce_id
            WHERE _valid_flag = TRUE AND status NOT LIKE 'FREE'

            UNION ALL

            SELECT DISTINCT
                CASE
                    WHEN est.salesforce_id = '{_DEMO_SRC_A}' THEN '{_DEMO_DST_A}'
                    WHEN est.salesforce_id = '{_DEMO_SRC_B}' THEN '{_DEMO_DST_B}'
                END AS establishment_sfid,
                CASE
                    WHEN est.salesforce_id = '{_DEMO_SRC_A}' THEN '{_DEMO_ACCT_A}'
                    WHEN est.salesforce_id = '{_DEMO_SRC_B}' THEN '{_DEMO_ACCT_B}'
                END AS account_id,
                res.reservation_id_sk,
                capacity,
                CASE
                    WHEN res.origin NOT LIKE 'WIDGET'
                     AND res.origin NOT LIKE 'FACEBOOK'
                     AND res.origin NOT LIKE 'MAIL'
                     AND res.origin NOT LIKE 'RESERVE_WITH_GOOGLE'
                     AND res.origin NOT LIKE 'PHONE'
                     AND res.origin NOT LIKE 'WALKIN'
                    THEN 'UNKNOWN'
                    ELSE res.origin
                END AS origin,
                DATE(res.start_date) AS start_date,
                res.status,
                DATE(res.creation_date_dt) AS booking_date,
                CURRENT_TIMESTAMP() AS version_timestamp,
                CONCAT(est.salesforce_id, '_', res.reservation_id_sk) AS key
            FROM `{project_name}.refined.analytical_rt_establishments_actual` est
            LEFT JOIN `{project_name}.refined.analytical_rt_reservations_hist` res
                ON est.establishment_id_sk = res.establishment_id_sk
            LEFT JOIN `{project_name}.refined.analytical_sfdc_establishment_actual` map
                ON map.establishment_id = est.salesforce_id
            WHERE _valid_flag = TRUE
              AND status NOT LIKE 'FREE'
              AND est.salesforce_id IN ('{_DEMO_SRC_A}', '{_DEMO_SRC_B}')
        ) a
        WHERE establishment_sfid IS NOT NULL AND establishment_sfid <> ''
        """
        logger.info("Retrieved query: get_reservation_query")
        return query

    @staticmethod
    def get_website_query(project_name: str) -> str:
        query = f"""
        SELECT *
        FROM (
            SELECT
                establishment_salesforce_id AS establishment_sfid,
                CAST(account_id AS STRING) AS account_id,
                visit_date,
                COUNT(DISTINCT visit_id) AS n_visits,
                COUNT(DISTINCT visitor_id) AS n_visitors,
                CURRENT_TIMESTAMP() AS version_timestamp,
                CONCAT(web.establishment_salesforce_id, '_', visit_date) AS key
            FROM `{project_name}.refined.adobe_visit_visitor` web
            LEFT JOIN `{project_name}.refined.analytical_sfdc_establishment_actual` map
                ON map.establishment_id = web.establishment_salesforce_id
            GROUP BY establishment_salesforce_id, account_id, visit_date

            UNION ALL

            SELECT
                CASE
                    WHEN establishment_salesforce_id = '{_DEMO_SRC_A}' THEN '{_DEMO_DST_A}'
                    WHEN establishment_salesforce_id = '{_DEMO_SRC_B}' THEN '{_DEMO_DST_B}'
                END AS establishment_sfid,
                CASE
                    WHEN establishment_salesforce_id = '{_DEMO_SRC_A}' THEN '{_DEMO_ACCT_A}'
                    WHEN establishment_salesforce_id = '{_DEMO_SRC_B}' THEN '{_DEMO_ACCT_B}'
                END AS account_id,
                visit_date,
                COUNT(DISTINCT visit_id) AS n_visits,
                COUNT(DISTINCT visitor_id) AS n_visitors,
                CURRENT_TIMESTAMP() AS version_timestamp,
                CONCAT(web.establishment_salesforce_id, '_', visit_date) AS key
            FROM `{project_name}.refined.adobe_visit_visitor` web
            LEFT JOIN `{project_name}.refined.analytical_sfdc_establishment_actual` map
                ON map.establishment_id = web.establishment_salesforce_id
            WHERE establishment_salesforce_id IN ('{_DEMO_SRC_A}', '{_DEMO_SRC_B}')
            GROUP BY establishment_salesforce_id, account_id, visit_date
        ) a
        WHERE establishment_sfid IS NOT NULL AND establishment_sfid <> ''
        """
        logger.info("Retrieved query: get_website_query")
        return query

    @staticmethod
    def get_pos_query(project_name: str) -> str:
        """Monthly POS KPIs with week-of-month buckets and payment-method pivot."""
        query = f"""
        WITH base AS (
            SELECT DISTINCT
                persistence_id,
                establishment_id,
                receipt_id,
                receipt_total_amount,
                created_at
            FROM `{project_name}.refined.pos_payment_transactions`
        ),
        cb AS (
            SELECT DISTINCT
                persistence_id,
                establishment_id,
                asset_created_date
            FROM `{project_name}.refined.pos_payment_transactions`
        ),
        monthly_order AS (
            SELECT
                DATE_TRUNC(DATE(created_at), MONTH) AS created_at,
                persistence_id,
                establishment_id,
                SUM(receipt_total_amount) AS total_turnover,
                COUNT(DISTINCT receipt_id) AS completed_orders
            FROM base
            WHERE DATE_TRUNC(DATE(created_at), MONTH)
                >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 12 MONTH)
            GROUP BY 1, 2, 3
        ),
        kpi1 AS (
            SELECT
                created_at,
                persistence_id,
                establishment_id,
                total_turnover AS turnover_current_month
            FROM monthly_order
        ),
        kpi2 AS (
            SELECT
                persistence_id,
                establishment_id,
                created_at_month,
                month_week_rank,
                weekly_turnover
            FROM (
                SELECT
                    persistence_id,
                    establishment_id,
                    DATE_TRUNC(DATE(created_at), MONTH) AS created_at_month,
                    CASE
                        WHEN EXTRACT(DAY FROM created_at) BETWEEN 1 AND 7 THEN 1
                        WHEN EXTRACT(DAY FROM created_at) BETWEEN 8 AND 14 THEN 2
                        WHEN EXTRACT(DAY FROM created_at) BETWEEN 15 AND 21 THEN 3
                        WHEN EXTRACT(DAY FROM created_at) >= 22 THEN 4
                    END AS month_week_rank,
                    SUM(receipt_total_amount) AS weekly_turnover
                FROM base
                WHERE DATE_TRUNC(DATE(created_at), MONTH)
                    >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 12 MONTH)
                GROUP BY 1, 2, 3, 4
            )
        ),
        kpi3 AS (
            SELECT
                cb.persistence_id,
                cb.establishment_id,
                mo.created_at,
                COALESCE(
                    SAFE_DIVIDE(
                        (COALESCE(mo.total_turnover, 0) - COALESCE(lmo.total_turnover, 0)),
                        COALESCE(lmo.total_turnover, 0)
                    ) * 100,
                    0
                ) AS turnover_percentage_change
            FROM cb
            LEFT JOIN monthly_order mo
                ON cb.persistence_id = mo.persistence_id
               AND cb.establishment_id = mo.establishment_id
            LEFT JOIN monthly_order lmo
                ON cb.persistence_id = lmo.persistence_id
               AND cb.establishment_id = lmo.establishment_id
               AND DATE_TRUNC(DATE(lmo.created_at), MONTH)
                 = DATE_SUB(DATE_TRUNC(DATE(mo.created_at), MONTH), INTERVAL 1 MONTH)
        ),
        kpi4 AS (
            SELECT created_at, persistence_id, establishment_id, completed_orders
            FROM monthly_order
        ),
        kpi5 AS (
            SELECT
                cb.persistence_id,
                cb.establishment_id,
                mo.created_at,
                COALESCE(
                    SAFE_DIVIDE(
                        (COALESCE(mo.completed_orders, 0) - COALESCE(lmo.completed_orders, 0)),
                        COALESCE(lmo.completed_orders, 0)
                    ) * 100,
                    0
                ) AS percentage_change
            FROM cb
            LEFT JOIN monthly_order mo
                ON cb.persistence_id = mo.persistence_id
               AND cb.establishment_id = mo.establishment_id
            LEFT JOIN monthly_order lmo
                ON cb.persistence_id = lmo.persistence_id
               AND cb.establishment_id = lmo.establishment_id
               AND DATE_TRUNC(DATE(lmo.created_at), MONTH)
                 = DATE_SUB(DATE(mo.created_at), INTERVAL 1 MONTH)
        ),
        payment_method AS (
            SELECT
                DATE_TRUNC(DATE(created_at), MONTH) AS created_at,
                persistence_id,
                establishment_id,
                payment_method_type_name AS payment_method_category,
                COUNT(DISTINCT receipt_id) AS completed_orders
            FROM `{project_name}.refined.pos_payment_transactions`
            WHERE DATE_TRUNC(DATE(created_at), MONTH)
                >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 12 MONTH)
            GROUP BY 1, 2, 3, 4
        ),
        kpi6 AS (
            SELECT *
            FROM payment_method
            PIVOT (
                SUM(completed_orders) FOR payment_method_category IN (
                    'Cash' AS completed_orders_by_cash,
                    'EFT' AS completed_orders_by_eft,
                    'Online' AS completed_orders_by_online,
                    'On account' AS completed_orders_by_on_account
                )
            )
        ),
        final AS (
            SELECT
                cb.persistence_id,
                cb.establishment_id,
                cb.asset_created_date,
                kpi1.created_at,
                ROUND(kpi1.turnover_current_month, 2) AS turnover_current_month,
                ROUND(kpi21.weekly_turnover, 2) AS turnover_week_1,
                ROUND(kpi22.weekly_turnover, 2) AS turnover_week_2,
                ROUND(kpi23.weekly_turnover, 2) AS turnover_week_3,
                ROUND(kpi24.weekly_turnover, 2) AS turnover_week_4,
                ROUND(kpi3.turnover_percentage_change, 2) AS turnover_percentage_change,
                kpi4.completed_orders,
                ROUND(kpi5.percentage_change, 2) AS percentage_change,
                kpi6.completed_orders_by_cash,
                kpi6.completed_orders_by_eft,
                kpi6.completed_orders_by_online,
                kpi6.completed_orders_by_on_account,
                ROUND(COALESCE(kpi7.total_turnover, 0), 2) AS turnover_last_month,
                COALESCE(kpi7.completed_orders, 0) AS completed_orders_last_month,
                CURRENT_TIMESTAMP() AS version_timestamp,
                CONCAT(cb.establishment_id, '_', cb.persistence_id) AS key
            FROM cb
            LEFT JOIN kpi1
                ON cb.persistence_id = kpi1.persistence_id
               AND cb.establishment_id = kpi1.establishment_id
            LEFT JOIN kpi2 kpi21
                ON cb.persistence_id = kpi21.persistence_id
               AND cb.establishment_id = kpi21.establishment_id
               AND kpi21.month_week_rank = 1
               AND kpi1.created_at = kpi21.created_at_month
            LEFT JOIN kpi2 kpi22
                ON cb.persistence_id = kpi22.persistence_id
               AND cb.establishment_id = kpi22.establishment_id
               AND kpi22.month_week_rank = 2
               AND kpi1.created_at = kpi22.created_at_month
            LEFT JOIN kpi2 kpi23
                ON cb.persistence_id = kpi23.persistence_id
               AND cb.establishment_id = kpi23.establishment_id
               AND kpi23.month_week_rank = 3
               AND kpi1.created_at = kpi23.created_at_month
            LEFT JOIN kpi2 kpi24
                ON cb.persistence_id = kpi24.persistence_id
               AND cb.establishment_id = kpi24.establishment_id
               AND kpi24.month_week_rank = 4
               AND kpi1.created_at = kpi24.created_at_month
            LEFT JOIN kpi3
                ON cb.persistence_id = kpi3.persistence_id
               AND cb.establishment_id = kpi3.establishment_id
               AND kpi1.created_at = kpi3.created_at
            LEFT JOIN kpi4
                ON cb.persistence_id = kpi4.persistence_id
               AND cb.establishment_id = kpi4.establishment_id
               AND kpi1.created_at = kpi4.created_at
            LEFT JOIN kpi5
                ON cb.persistence_id = kpi5.persistence_id
               AND cb.establishment_id = kpi5.establishment_id
               AND kpi1.created_at = kpi5.created_at
            LEFT JOIN kpi6
                ON cb.persistence_id = kpi6.persistence_id
               AND cb.establishment_id = kpi6.establishment_id
               AND kpi1.created_at = kpi6.created_at
            LEFT JOIN monthly_order kpi7
                ON cb.persistence_id = kpi7.persistence_id
               AND cb.establishment_id = kpi7.establishment_id
               AND DATE_SUB(kpi1.created_at, INTERVAL 1 MONTH) = kpi7.created_at
            WHERE cb.establishment_id IS NOT NULL AND cb.establishment_id <> ''
        )
        SELECT * FROM final
        """
        logger.info("Retrieved query: get_pos_query")
        return query
