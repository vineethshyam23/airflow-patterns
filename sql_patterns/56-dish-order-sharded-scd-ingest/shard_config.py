"""
Shard + table catalog for the food-ordering Cloud SQL land.

Production ran ~44 MySQL shards plus one master. This reference keeps
a small representative map so the DAG graph stays readable while the
fan-out shape (discover → filter by IP → export → merge) stays intact.

Scripts live under scripts/ and are mounted on Composer at
``{scripts_root}/`` (production used ``/home/airflow/gcs/data/food-order/``).
"""

from __future__ import annotations

# Master holds tenant registry + reference dims; shards hold tenant DBs.
MASTER_INSTANCE = "db-prod-mysql-order-master"
MASTER_IP = "10.0.0.3"

# Representative shard map. Production had ~44 entries keyed by instance
# name with private IP used to filter the tenant→shard CSV.
SHARD_INSTANCES: dict[str, str] = {
    "shard-aa-prod-order": "10.0.1.10",
    "shard-ab-prod-order": "10.0.1.11",
    "shard-ac-prod-order": "10.0.1.12",
    "shard-ad-prod-order": "10.0.1.13",
}

# Master-only tables: exported once from the master instance, then
# GCS-copied into the raw zone (no shard merge).
MASTER_TABLES: list[str] = [
    "countries",
    "users",
    "clients",
    "flavours",
]

# Sharded tables: exported per shard, concatenated by dishordermerge.sh.
# Trimmed from ~34 production tables to a core transactional set.
SHARDED_TABLES: list[str] = [
    "orders",
    "order_menus",
    "order_totals",
    "customers",
    "menus",
    "menu_categories",
    "locations",
    "payments",
    "payment_logs",
    "reservations",
    "statuses",
    "status_history",
]

# All tables that get an SCD Type 2 load chain.
ALL_TABLES: list[str] = MASTER_TABLES + SHARDED_TABLES

# payment_logs CSV is noisy in production — elevate bad-record tolerance.
BAD_RECORDS_OVERRIDE: dict[str, int] = {
    "payment_logs": 300,
}

DEFAULT_MAX_BAD_RECORDS = 10

SOURCE_SYSTEM = "FoodOrder"
TABLE_PREFIX = "order_"

# Path layout under Composer data / GCS.
SCRIPTS_ROOT = "/home/airflow/gcs/data/food-order/scripts"
DATA_ROOT = "/home/airflow/gcs/data/food-order"
EXPORT_PREFIX = "foodorder"


def max_bad_records(table: str) -> int:
    return BAD_RECORDS_OVERRIDE.get(table, DEFAULT_MAX_BAD_RECORDS)


def is_sharded(table: str) -> bool:
    return table in SHARDED_TABLES
