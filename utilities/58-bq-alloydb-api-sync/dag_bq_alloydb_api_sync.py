"""Composer DAG: product API refined tables in BigQuery + AlloyDB sync.

Daily job that WRITE_TRUNCATEs six API-facing refined tables in parallel,
then incrementally copies the market dashboard feed into AlloyDB
(PostgreSQL) with ``ON CONFLICT (unique_key) DO NOTHING``.

This is a warehouse dual-store pattern for low-latency product API reads.
It is not an API Gateway / Apigee deploy — serving stays outside Composer.

Source (read-only): ``dags/etl_api_alloydb.py`` (+ ``DISH_api_query.py``).

Production quirks kept visible:
- BQ tasks hardcode ``bigquery_default`` even when DEV sets another conn.
- AlloyDB load is row-by-row Python (not COPY / execute_values).
- Incremental window is ``between max_date and current_date()`` so the
  boundary day is re-scanned; conflicts make that safe but wasteful.
- Source had a commented ``__main__`` with a plaintext DB password —
  deleted here. Creds come from Airflow Variables only.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta
from typing import Any

import psycopg2
from google.cloud import bigquery

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.helpers import chain

from product_api_queries import ProductApiQueries

logger = logging.getLogger(__name__)

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "start_date": datetime(2022, 10, 23),
    "email": ["dataops@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}

env = os.environ.get("env", Variable.get("env", default_var="PROD"))

if env == "DEV":
    project_id = Variable.get("dwh_project_id_dev", default_var="dwh_project_dev")
    gcp_conn_id = "google_cloud_dev"
    alloydb_var = "alloydb_dev_creds"
else:
    project_id = Variable.get("dwh_project_id", default_var="dwh_project")
    gcp_conn_id = "google_cloud_default"
    alloydb_var = "alloydb_prod_creds"

# Templated JSON Variable — resolved at runtime for the PythonOperator.
db_creds: Any = f"{{{{ var.json.{alloydb_var} }}}}"

MAX_DATE_QUERY = "SELECT max(date(created_date)) FROM api_refined.api_dashboard_market"
COUNT_QUERY = "SELECT count(*) AS cnt FROM api_refined.api_dashboard_market"
INSERT_QUERY = """
INSERT INTO api_refined.api_dashboard_market (
    unique_key, subject, assigned_id, description, type, created_date,
    sub_type, event_topic, is_closed, name_id, outcome, activity_id,
    start_date, lead_account_id, wholesale_account_identifier, user_id,
    email, profile_name, won_status
) VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
)
ON CONFLICT (unique_key) DO NOTHING
"""

# Fallback when AlloyDB table is empty — mirrors production bootstrap date.
ALLOYDB_BOOTSTRAP_DATE = "2024-06-01"


def get_postgres_connection(db_creds_dict: dict):
    """Open AlloyDB/Postgres and return (conn, cursor).

    Source returned only a cursor and used ``with get_postgres_connection()``,
    which never closed the underlying connection. Keep both handles here.
    """
    conn = psycopg2.connect(**db_creds_dict)
    conn.autocommit = True
    return conn, conn.cursor()


def fetchone(db_creds_dict: dict, query: str) -> tuple:
    conn = None
    try:
        conn = psycopg2.connect(**db_creds_dict)
        conn.autocommit = True
        with conn.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchone()
        return result or ()
    except Exception as error:
        logger.info("Error while fetchone: %s", error)
        return ()
    finally:
        if conn is not None:
            conn.close()


def load_data_to_alloydb(
    project_id: str,
    db_creds_dict: dict,
    max_date_query: str,
    insert_query: str,
    count_query: str = "",
):
    """Incremental BQ → AlloyDB load for the market dashboard feed."""
    client = bigquery.Client(project=project_id)

    if count_query:
        count_result = fetchone(db_creds_dict, count_query)
        if count_result:
            logger.info("Total records in AlloyDB table: %s", count_result[0])

    try:
        result = fetchone(db_creds_dict, max_date_query)
        max_date = result[0].strftime("%Y-%m-%d")
    except Exception as exc:
        logger.info("AlloyDB table empty or max-date failed (%s); bootstrapping", exc)
        max_date = ALLOYDB_BOOTSTRAP_DATE

    logger.info("Max date in AlloyDB table: %s", max_date)

    query = (
        f"SELECT DISTINCT * FROM `{project_id}.refined.api_dashboard_market` "
        f"WHERE date(created_date) BETWEEN '{max_date}' AND CURRENT_DATE()"
    )
    bq_results = client.query(query).result()
    logger.info("BigQuery rows in window: %s", bq_results.total_rows)

    conn, cur = get_postgres_connection(db_creds_dict)
    try:
        processed = inserted = skipped = 0
        for row in bq_results:
            processed += 1
            if processed % 100 == 0:
                logger.info("Processing row %s of %s", processed, bq_results.total_rows)

            # Production BQ schema used activity_Id (capital I) on the row dict.
            activity_id = row.get("activity_id", row.get("activity_Id"))
            cur.execute(
                insert_query,
                (
                    row["unique_key"],
                    row["subject"],
                    row["assigned_id"],
                    row["description"],
                    row["type"],
                    row["created_date"],
                    row["sub_type"],
                    row["event_topic"],
                    row["is_closed"],
                    row["name_id"],
                    row["outcome"],
                    activity_id,
                    row["start_date"],
                    row["lead_account_id"],
                    row.get(
                        "wholesale_account_identifier",
                        row.get("metro_account_identifier"),
                    ),
                    row["user_id"],
                    row["email"],
                    row["profile_name"],
                    row["won_status"],
                ),
            )
            if cur.rowcount > 0:
                inserted += 1
            else:
                skipped += 1

        logger.info(
            "AlloyDB load complete. processed=%s inserted=%s skipped=%s",
            processed,
            inserted,
            skipped,
        )
    finally:
        cur.close()
        conn.close()


def _bq_truncate_job(
    task_id: str,
    sql: str,
    dataset: str,
    table: str,
    cluster_fields: list[str] | None = None,
) -> BigQueryInsertJobOperator:
    query_cfg: dict[str, Any] = {
        "query": sql,
        "destinationTable": {
            "projectId": project_id,
            "datasetId": dataset,
            "tableId": table,
        },
        "writeDisposition": "WRITE_TRUNCATE",
        "createDisposition": "CREATE_IF_NEEDED",
        "allowLargeResults": True,
        "useLegacySql": False,
    }
    if cluster_fields:
        query_cfg["clustering"] = {"fields": cluster_fields}

    # Production hardcodes bigquery_default even in DEV — keep that footgun
    # visible rather than silently wiring gcp_conn_id.
    return BigQueryInsertJobOperator(
        task_id=task_id,
        configuration={"query": query_cfg},
        gcp_conn_id="bigquery_default",
    )


with DAG(
    dag_id="etl_api_alloydb",
    description="Product API refined tables in BQ + incremental AlloyDB sync",
    default_args=default_args,
    schedule_interval="2 6 * * *",
    catchup=False,
    max_active_runs=1,
    render_template_as_native_obj=True,
    tags=["api", "alloydb", "refined", "dual-store"],
) as dag:

    start = EmptyOperator(task_id="start")

    api_website = _bq_truncate_job(
        "api_product_website_refined",
        ProductApiQueries.get_website_query(project_id),
        "refined",
        "api_product_web",
        ["establishment_sfid"],
    )
    api_reservation = _bq_truncate_job(
        "api_product_reservation_refined",
        ProductApiQueries.get_reservation_query(project_id),
        "refined",
        "api_product_reservations",
        ["establishment_sfid"],
    )
    api_order = _bq_truncate_job(
        "api_product_order_refined",
        ProductApiQueries.get_order_query(project_id),
        "refined",
        "api_product_order",
        ["establishment_sfid"],
    )
    api_establishment = _bq_truncate_job(
        "api_product_establishment_refined",
        ProductApiQueries.get_establishment_query(project_id),
        "refined",
        "api_product_establishment",
        ["establishment_sfid"],
    )
    api_pos = _bq_truncate_job(
        "api_product_pos_refined",
        ProductApiQueries.get_pos_query(project_id),
        "refined",
        "api_product_pos",
        ["establishment_id"],
    )
    api_dashboard = _bq_truncate_job(
        "api_dashboard_market_refined",
        ProductApiQueries.get_dashboard_market_query(project_id),
        "refined",
        "api_dashboard_market",
    )

    # Partner/wholesale id → establishment map for CO consumers.
    api_co = BigQueryInsertJobOperator(
        task_id="api_co_map",
        configuration={
            "query": {
                "query": f"""
                    WITH ranked AS (
                        SELECT DISTINCT
                            wholesale_id,
                            establishment_id,
                            ROW_NUMBER() OVER (
                                PARTITION BY wholesale_id
                                ORDER BY google_places_id DESC
                            ) AS rn
                        FROM `{project_id}.refined.all_establishments_*`
                        WHERE data_source = 'all'
                          AND wholesale_id IS NOT NULL
                    )
                    SELECT
                        wholesale_id,
                        establishment_id,
                        MD5(CONCAT(
                            COALESCE(CAST(wholesale_id AS STRING), ''), '|',
                            COALESCE(CAST(establishment_id AS STRING), '')
                        )) AS _rowhash
                    FROM ranked
                    WHERE rn = 1
                """,
                "destinationTable": {
                    "projectId": project_id,
                    "datasetId": "trusted_staging",
                    "tableId": "api_co",
                },
                "writeDisposition": "WRITE_TRUNCATE",
                "createDisposition": "CREATE_IF_NEEDED",
                "allowLargeResults": True,
                "useLegacySql": False,
                "clustering": {"fields": ["wholesale_id"]},
            }
        },
        gcp_conn_id="bigquery_default",
    )

    sync_alloydb = PythonOperator(
        task_id="load_data_to_alloydb",
        python_callable=load_data_to_alloydb,
        op_kwargs={
            "project_id": project_id,
            "db_creds_dict": db_creds,
            "max_date_query": MAX_DATE_QUERY,
            "insert_query": INSERT_QUERY,
            "count_query": COUNT_QUERY,
        },
        execution_timeout=timedelta(hours=2),
    )

    chain(
        start,
        [
            api_website,
            api_reservation,
            api_order,
            api_establishment,
            api_pos,
            api_dashboard,
            api_co,
        ],
        sync_alloydb,
    )
