"""
Query helpers for single-market platform Order + Reservation export.

Sources (refined tables built by a tagged dbt job):
  refined.market_dish_orders
  refined.market_dish_reservations

Source (read-only):
  dags/horeca_digital/dana_pl_dish_orders_query.py
"""

import logging


class MarketDishOrders:
    # Single-market feed — list kept so the DAG can loop the same way
    # multi-country exporters do if another market is added later.
    countries = ["pl"]

    @staticmethod
    def get_orders_query():
        """Select order columns typed for the market_dish_orders Avro schema."""
        query = """
        SELECT
            CAST(establishment_id AS STRING) AS establishment_id,
            CAST(establishment_name AS STRING) AS establishment_name,
            CAST(wholesale_id AS STRING) AS wholesale_id,
            CAST(store_id AS STRING) AS store_id,
            CAST(postalcode AS STRING) AS postalcode,
            CAST(city AS STRING) AS city,
            CAST(owner_first_name AS STRING) AS owner_first_name,
            CAST(owner_email AS STRING) AS owner_email,
            CAST(owner_last_name AS STRING) AS owner_last_name,
            CAST(owner_phone AS STRING) AS owner_phone,
            CAST(product_code AS STRING) AS product_code,
            CAST(commitment_period AS INT64) AS commitment_period,
            CAST(asset_status AS STRING) AS asset_status,
            CAST(asset_referrer AS STRING) AS asset_referrer,
            CAST(order_id AS INT64) AS order_id,
            CAST(order_number AS STRING) AS order_number,
            FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', CAST(order_date AS TIMESTAMP)) AS order_date,
            CAST(order_status AS STRING) AS order_status,
            CAST(order_price AS FLOAT64) AS order_price
        FROM `refined.market_dish_orders`
        """
        logging.getLogger().info("Retrieved query: get_orders_query (market_dish_orders)")
        return query

    @staticmethod
    def get_reservations_query():
        """Select reservation columns typed for the market_dish_reservations Avro schema."""
        query = """
        SELECT
            CAST(establishment_id AS STRING) AS establishment_id,
            CAST(establishment_name AS STRING) AS establishment_name,
            CAST(wholesale_id AS STRING) AS wholesale_id,
            CAST(store_id AS STRING) AS store_id,
            CAST(postalcode AS STRING) AS postalcode,
            CAST(city AS STRING) AS city,
            CAST(owner_first_name AS STRING) AS owner_first_name,
            CAST(owner_last_name AS STRING) AS owner_last_name,
            CAST(owner_email AS STRING) AS owner_email,
            CAST(owner_phone AS STRING) AS owner_phone,
            CAST(product_code AS STRING) AS product_code,
            CAST(commitment_period AS INT64) AS commitment_period,
            CAST(asset_status AS STRING) AS asset_status,
            CAST(asset_referrer AS STRING) AS asset_referrer,
            CAST(reservation_id AS INT64) AS reservation_id,
            FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', CAST(reservation_created_date AS TIMESTAMP)) AS reservation_created_date
        FROM `refined.market_dish_reservations`
        """
        logging.getLogger().info(
            "Retrieved query: get_reservations_query (market_dish_reservations)"
        )
        return query
