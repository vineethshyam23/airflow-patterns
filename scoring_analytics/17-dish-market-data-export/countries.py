"""
Active markets for the monthly establishment market-data export.

Kept as a tiny module so the DAG and export share one list without
importing the broader foodgraph / SEO query package.

Source (read-only):
  dags/horeca_digital/foodgraph_queries.py
    → dish_market_data_active_isocode_list
"""

# Lowercase ISO codes — refined tables are dish_market_data_{cc}.
# Production enabled ES + DE first; extend here when a market's
# refined table is ready and the partner schema is registered.
ACTIVE_ISO_CODES = ["es", "de"]
