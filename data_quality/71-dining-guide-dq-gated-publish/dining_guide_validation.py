"""Dining Guide publish helpers: Blake3 dine_id + proportion DQ gate.

Production kept these callables in the DAG file. Pulled out so the
hash/compare/branch contract is testable without importing Airflow
operators. Threshold is 15% degradation vs yesterday's consumer-app
snapshot — fail closed on query errors so a monitoring blip cannot
push a broken cutover.
"""

from __future__ import annotations

import base64
import logging
from typing import Any

import blake3
from google.cloud import bigquery

logger = logging.getLogger(__name__)

# Proportion / average drop that blocks cross-project publish.
DEGRADATION_THRESHOLD = 0.15


class UidShortener:
    """Blake3 → base64url short id used as public dine_id."""

    @staticmethod
    def get_hash(uid: str, hash_length: int = 10, encoding: str = "base64url") -> str:
        dk_len = max(0, hash_length - hash_length // 3)
        hasher = blake3.blake3()
        hasher.update(uid.encode("utf-8"))
        digest = hasher.digest()[:dk_len]
        if encoding != "base64url":
            raise ValueError(f"Unsupported encoding: {encoding}")
        encoded = base64.urlsafe_b64encode(digest).rstrip(b"=").decode("utf-8")
        return encoded[:hash_length]


def rebuild_establishment_id_hash(
    dwh_project: str,
    trusted_dataset: str = "trusted",
    source_table: str = "dining_guide_data_base",
    hash_table: str = "dining_guide_establishment_id_hash",
) -> str:
    """SELECT distinct establishment_id → Blake3 map → WRITE_TRUNCATE hash table.

    Uses pandas on the worker. Fine for tens/hundreds of thousands of
    establishments; move to BQ SQL if the universe grows past worker RAM.
    """
    client = bigquery.Client(dwh_project)
    fq_source = f"`{dwh_project}.{trusted_dataset}.{source_table}`"
    table_id = f"{dwh_project}.{trusted_dataset}.{hash_table}"

    df = client.query(
        f"SELECT DISTINCT establishment_id FROM {fq_source}"
    ).to_dataframe()
    df["hashed_uid"] = df["establishment_id"].apply(UidShortener.get_hash)

    job = client.load_table_from_dataframe(
        df,
        table_id,
        job_config=bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("establishment_id", "STRING"),
                bigquery.SchemaField("hashed_uid", "STRING"),
            ],
            write_disposition="WRITE_TRUNCATE",
        ),
    )
    job.result()
    logger.info("Reloaded hash table %s (%s rows)", table_id, len(df))
    return table_id


def comparison_sql(
    dwh_project: str,
    prod_app_project: str,
    trusted_dataset: str = "trusted",
    app_dataset: str = "app_data",
    monitoring_threshold: float = DEGRADATION_THRESHOLD,
) -> str:
    """Build the FULL JOIN proportion check vs yesterday's published prod table.

    t1 = today's dbt trusted build. t2 = last successful consumer-app
    publish (prod project). Test establishments excluded.
    """
    t = monitoring_threshold
    trusted = f"`{dwh_project}.{trusted_dataset}.dining_guide_data_base`"
    prod = f"`{prod_app_project}.{app_dataset}.dining_guide_data_base`"

    def prop_distinct(col: str) -> str:
        return (
            f"IF(SAFE_DIVIDE(COUNT(DISTINCT t1.{col}), COUNT(DISTINCT t2.{col})) - 1 "
            f"< -{t}, TRUE, FALSE) AS {col}_proportion_difference"
        )

    def prop_flag(col: str) -> str:
        return (
            f"IF(SAFE_DIVIDE(COUNTIF(t1.{col} = 1), COUNTIF(t2.{col} = 1)) - 1 "
            f"< -{t}, TRUE, FALSE) AS {col}_proportion_difference"
        )

    def prop_nonnull(col: str) -> str:
        return (
            f"IF((SAFE_DIVIDE("
            f"SAFE_DIVIDE(COUNTIF(t1.{col} IS NOT NULL), COUNT(DISTINCT t1.establishment_id)), "
            f"SAFE_DIVIDE(COUNTIF(t2.{col} IS NOT NULL), COUNT(DISTINCT t2.establishment_id))"
            f")) - 1 < -{t}, TRUE, FALSE) AS {col}_proportion_difference"
        )

    def prop_avg(col: str) -> str:
        return (
            f"IF(ABS(SAFE_DIVIDE(AVG(t1.{col}), AVG(t2.{col}))) - 1 > {t}, TRUE, FALSE) "
            f"AS {col}_proportion_difference"
        )

    metrics = ",\n    ".join(
        [
            prop_distinct("establishment_id"),
            prop_distinct("crm_account_id"),
            prop_distinct("crm_establishment_id"),
            prop_flag("wholesale_customer"),
            prop_distinct("iso_code"),
            prop_nonnull("google_places_id"),
            prop_nonnull("establishment_name"),
            prop_distinct("postal_code"),
            prop_nonnull("city"),
            prop_distinct("address"),
            prop_distinct("website"),
            prop_flag("order_customer"),
            prop_flag("reservation_customer"),
            prop_flag("website_customer"),
            prop_nonnull("cuisine_type"),
            prop_nonnull("establishment_type"),
            prop_nonnull("order_url"),
            prop_nonnull("reservation_url"),
            prop_nonnull("description"),
            prop_flag("order_delivery"),
            prop_flag("order_collection"),
            prop_nonnull("geo_lat"),
            prop_nonnull("geo_long"),
            prop_nonnull("rating_google"),
            prop_nonnull("open_hours"),
            prop_distinct("establishment_phone"),
            prop_avg("count_reservations_all"),
            prop_avg("count_reservations_6m"),
            prop_avg("count_orders_all"),
            prop_avg("count_orders_6m"),
        ]
    )

    return f"""
SELECT
    {metrics}
FROM (
  SELECT * FROM {trusted} WHERE test_establishment = 0
) t1
FULL JOIN (
  SELECT * FROM {prod} WHERE test_establishment = 0
) t2
USING (establishment_id)
""".strip()


# Columns OR'd in the fail-gate query (must match comparison_sql aliases).
GATE_FLAG_COLUMNS = [
    "establishment_id_proportion_difference",
    "crm_account_id_proportion_difference",
    "crm_establishment_id_proportion_difference",
    "wholesale_customer_proportion_difference",
    "iso_code_proportion_difference",
    "google_places_id_proportion_difference",
    "establishment_name_proportion_difference",
    "postal_code_proportion_difference",
    "city_proportion_difference",
    "address_proportion_difference",
    "website_proportion_difference",
    "order_customer_proportion_difference",
    "reservation_customer_proportion_difference",
    "website_customer_proportion_difference",
    "cuisine_type_proportion_difference",
    "establishment_type_proportion_difference",
    "order_url_proportion_difference",
    "reservation_url_proportion_difference",
    "description_proportion_difference",
    "order_delivery_proportion_difference",
    "order_collection_proportion_difference",
    "geo_lat_proportion_difference",
    "geo_long_proportion_difference",
    "rating_google_proportion_difference",
    "open_hours_proportion_difference",
    "establishment_phone_proportion_difference",
    "count_reservations_all_proportion_difference",
    "count_reservations_6m_proportion_difference",
    "count_orders_all_proportion_difference",
    "count_orders_6m_proportion_difference",
]


def any_flag_true(
    dwh_project: str,
    monitoring_dataset: str = "monitoring",
    results_table: str = "comparison_results_dining_guide",
) -> bool:
    """Return True when any degradation flag is TRUE (or on query error)."""
    client = bigquery.Client()
    where = " OR\n                    ".join(GATE_FLAG_COLUMNS)
    query = f"""
        SELECT 1
        FROM `{dwh_project}.{monitoring_dataset}.{results_table}`
        WHERE {where}
    """
    try:
        rows = list(client.query(query).result())
        failed = len(rows) > 0
        if failed:
            logger.warning("DQ gate FAILED: %s flag row(s)", len(rows))
        else:
            logger.info("DQ gate PASSED")
        return failed
    except Exception:
        logger.exception("DQ gate query error — fail closed")
        return True


def branch_on_validation(ti: Any) -> str:
    """BranchPython target: Slack on fail, publish path on pass."""
    failed = ti.xcom_pull(task_ids="check_query_task")
    if failed:
        return "slack_dq_fail_task"
    return "approve_publish"


def publish_select_sql(dwh_project: str, trusted_dataset: str = "trusted") -> str:
    """Trusted join hash → consumer-app row shape (dine_id = hashed_uid)."""
    return f"""
SELECT DISTINCT
    b.establishment_id,
    b.crm_account_id,
    b.crm_establishment_id,
    wholesale_customer,
    iso_code,
    google_places_id,
    establishment_name,
    postal_code,
    city,
    address,
    website,
    order_customer,
    reservation_customer,
    website_customer,
    cuisine_type,
    establishment_type,
    order_url,
    reservation_url,
    description,
    order_delivery,
    order_collection,
    geo_lat,
    geo_long,
    rating_google,
    open_hours,
    establishment_phone,
    test_establishment,
    h.hashed_uid AS dine_id,
    count_reservations_all,
    count_reservations_6m,
    count_orders_all,
    count_orders_6m,
    menu_url
FROM `{dwh_project}.{trusted_dataset}.dining_guide_data_base` b
LEFT JOIN `{dwh_project}.{trusted_dataset}.dining_guide_establishment_id_hash` h
  ON b.establishment_id = h.establishment_id
WHERE test_establishment IS NOT NULL
""".strip()
