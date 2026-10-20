"""
SELECT-only share queries for the weekly insights_share destination.

Each entry is one full-replace copy job loaded by the DAG. Keep statements
as pure SELECTs — no DDL, no DML. Destination table id == dict key.

Source (read-only): dags/dish_insights_share_queries.py
"""

SHARE_QUERIES = {
    "market_establishments": """
SELECT *
FROM `dwh_project.dms_spot.market_establishments`
""",
    "customer_base_establishment": """
SELECT *
FROM `dwh_project.refined.customer_base_establishment`
""",
    "pos_sold_item_transactions": """
SELECT *
FROM `dwh_project.product_spot.pos_sold_item_transactions`
""",
    "pos_payment_transactions": """
SELECT *
FROM `dwh_project.product_spot.pos_payment_transactions`
""",
    "website_establishments": """
SELECT
  salesforce_id AS establishment_id,
  a.* EXCEPT (salesforce_id, salesforce_id_sk, dwhid)
FROM `dwh_project.refined.analytical_establishments_actual` AS a
JOIN `dwh_project.refined.customer_base_establishment` AS b
  ON a.salesforce_id = b.establishment_id
""",
    "website_menu_urls": """
SELECT
  sfdc_id AS establishment_id,
  * EXCEPT (sfdc_id)
FROM `dwh_project.trusted_views.menu_urls`
""",
    "website_opening_hours": """
SELECT DISTINCT
  b.salesforce_id AS establishment_id,
  a.* EXCEPT (dwhid, establishment_id_sk)
FROM `dwh_project.refined.analytical_openingtimes_actual` AS a
JOIN `dwh_project.refined.analytical_establishments_actual` AS b
  ON a.establishment_id_sk = b.establishment_id_sk
JOIN `dwh_project.refined.customer_base_establishment` AS c
  ON b.salesforce_id = c.establishment_id
""",
    "rt_establishments": """
SELECT
  salesforce_id AS establishment_id,
  a.* EXCEPT (salesforce_id, salesforce_id_sk, dwhid)
FROM `dwh_project.refined.analytical_rt_establishments_actual` AS a
JOIN `dwh_project.refined.customer_base_establishment` AS b
  ON a.salesforce_id = b.establishment_id
""",
    "rt_reservables": """
SELECT
  c.salesforce_id AS establishment_id,
  a.*
FROM `dwh_project.trusted.rt_reservables` AS a
JOIN `dwh_project.trusted.rt_reservablegroups` AS b
  ON CAST(a.reservable_group_id AS INT64) = b.id
  AND a._valid_flag IS TRUE
  AND b._valid_flag IS TRUE
JOIN `dwh_project.trusted_views.rt_establishments` AS c
  ON CAST(b.establishment_id AS INT64) = c.id
""",
    "rt_reservations2reservables": """
SELECT *
FROM `dwh_project.trusted.rt_reservations2reservables`
WHERE _valid_flag IS TRUE
""",
    "rt_reservations_last_12m": """
SELECT
  cb.establishment_id,
  r.* EXCEPT (establishment_id, establishment_id_sk)
FROM `dwh_project.trusted.rt_reservations` AS r
JOIN `dwh_project.trusted_views.rt_establishments` AS e
  ON e.id_sk = r.establishment_id_sk
  AND r._valid_flag IS TRUE
JOIN `dwh_project.refined.customer_base_establishment` AS cb
  ON e.salesforce_id = cb.establishment_id
WHERE DATE(start_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH)
""",
    "order_establishments": """
SELECT
  sfdc_establishment_id AS establishment_id,
  * EXCEPT (sfdc_establishment_id)
FROM `dwh_project.refined.analytical_order_establishments_actual`
""",
    "order_menus": """
SELECT *
FROM `dwh_project.refined.analytical_order_menus_actual`
""",
    "order_orders": """
SELECT *
FROM `dwh_project.refined.analytical_order_orders_actual`
""",
    "pay_customer_base_spot": """
SELECT *
FROM `dwh_project.product_spot.pay_customer_base_spot`
""",
    "pay_revenue": """
SELECT *
FROM `dwh_project.product_spot.pay_revenue`
""",
}
