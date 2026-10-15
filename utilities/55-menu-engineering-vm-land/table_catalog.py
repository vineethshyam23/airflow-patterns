"""Representative Postgres catalog for the menu-engineering VM land.

Production exports ~30 tables (per-country address slices, import
ledgers, shifts, cash systems, …). This list keeps the shape without
shipping the full schema.
"""

from __future__ import annotations

# Schema inside the Dockerised Postgres on the product VM.
PG_SCHEMA = "menu_engineering"

# Staging table prefix in BigQuery: me_<table>_tbl
BQ_STAGING_PREFIX = "me_"

# Trimmed catalog — enough to show country fan-out, core dims, and
# a couple of high-volume import facts.
POSTGRES_TABLES = [
    "address_germany",
    "address_netherlands",
    "address_spain",
    "country",
    "project",
    "team",
    "cash_register",
    "import_order_summary",
    "import_order_transaction",
    "item_name_map",
    "job",
    "snapshot",
]
