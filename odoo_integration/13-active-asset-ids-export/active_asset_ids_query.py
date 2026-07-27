"""
SELECT builder for the weekly active sale-order-line ID snapshot.

Reads refined sales tables after the cleanup DAG has already removed
invalidated lines. The partner master-file consumer LEFT JOINs this
feed on sale_order_line_id — anything missing is treated as deleted.

Source (read-only):
  dags/horeca_digital/dana_odoo_assets_leads_lifecycle_export.py
  (DANAexport.get_active_asset_ids_query)
"""

from __future__ import annotations

import logging


class ActiveAssetIdsQueries:
    """Static SQL for the weekly active-ID snapshot, one country at a time."""

    @staticmethod
    def get_active_asset_ids_query(country: str) -> str:
        """
        All sale_order_line IDs still present in refined sales for `country`.

        Country is uppercased in SQL. The ingest path keeps the lowercase
        market code the event API expects.
        """
        country = country.upper()
        query = f"""
SELECT DISTINCT
  sol.id AS sale_order_line_id,
  sol.order_id AS sale_order_id,
  rp.partner_uuid AS establishment_id,
  CURRENT_DATE() AS _ldts
FROM `refined_sales.odoo_sale_order_line` sol
INNER JOIN `refined_sales.odoo_sale_order` so
  ON sol.order_id = so.id
INNER JOIN `refined_sales.odoo_res_country` rc
  ON rc.id = so.country_id
LEFT JOIN `refined_sales.odoo_res_partner` rp
  ON so.partner_id = rp.id
WHERE UPPER(rc.code) = '{country}'
"""
        logging.info("Retrieved query: get_active_asset_ids_query for %s", country)
        return query


if __name__ == "__main__":
    print(ActiveAssetIdsQueries.get_active_asset_ids_query("DE"))
