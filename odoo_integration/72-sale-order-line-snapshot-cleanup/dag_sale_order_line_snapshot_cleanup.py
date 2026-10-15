"""
Weekly soft-close of orphaned rows in a dbt snapshot of Odoo sale_order_line.

Daily full extracts do not detect deletes in Odoo. This DAG pulls the live
ID set (IDs only), lands it via GCS into a BigQuery temp table, LEFT JOINs
against the snapshot's open rows (dbt_valid_to IS NULL), and soft-closes
orphans by setting dbt_valid_to. History stays; XCom never carries the
full ID list.

Source (read-only):
  dags/etl_odoo_sale_order_line_cleanup_optimized.py
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryDeleteTableOperator,
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.utils.helpers import chain
from airflow.utils.trigger_rule import TriggerRule

from odoo_postgres import OdooPostgresClient

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 29),
    "email": ["dataops@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}

env = os.environ.get("env", Variable.get("env"))

if env == "DEV":
    odoo_env = "DM"
    odoo_creds = Variable.get("odoo_dm_creds", deserialize_json=True)
    project_id = Variable.get("dwh_project_id", default_var="dwh_project_dev")
    bucket_name = Variable.get("rawzone_bucket", default_var="dwh-rawzone-dev")
    gcp_conn_id = "google_cloud_dev"
else:
    odoo_env = "prod"
    odoo_creds = Variable.get("odoo_prod_creds", deserialize_json=True)
    project_id = Variable.get("dwh_project_id", default_var="dwh_project")
    bucket_name = Variable.get("rawzone_bucket", default_var="dwh-rawzone")
    gcp_conn_id = "google_cloud_default"

# Weekly Sundays 02:00 UTC — finish before Monday asset-ID exports.
schedule = "0 2 * * 0"

temp_dataset = "ops"
temp_table_odoo_ids = "temp_odoo_sale_order_line_ids"
snapshot_table = f"{project_id}.trusted.odoo_sale_order_line_snapshot"
temp_fqn = f"{project_id}.{temp_dataset}.{temp_table_odoo_ids}"

# Sync assessment bands for the summary task (absolute |odoo - active|).
SYNC_OK = 100
SYNC_WARN = 1000


def extract_odoo_sale_order_line_ids(**context):
    """
    SELECT id FROM sale_order_line → NDJSON → GCS.

    Connects in-task (not at parse). Uploads via GCSHook so Composer
    FUSE mounts are not required for the hand-off.
    """
    ds = context["ds"]
    local_dir = "/tmp/odoo_cleanup/"
    os.makedirs(local_dir, exist_ok=True)
    output_file = f"{local_dir}odoo_sale_order_line_ids.json"
    dest = f"odoo_cleanup/sale_order_line_ids/{ds}/odoo_sale_order_line_ids.json"

    logging.info("Starting extraction of sale_order_line IDs from Odoo")
    odoo = OdooPostgresClient(
        env=odoo_env,
        project_id=project_id,
        creds=odoo_creds,
    )
    cur = None
    try:
        cur = odoo.conn.cursor()
        cur.execute("SELECT id FROM sale_order_line ORDER BY id")

        count = 0
        with open(output_file, "w", encoding="utf-8") as outfile:
            for row in cur.fetchall():
                json.dump({"id": row[0]}, outfile)
                outfile.write("\n")
                count += 1

        logging.info("Total sale_order_line IDs extracted from Odoo: %s", count)
        hook = GCSHook(gcp_conn_id=gcp_conn_id)
        hook.upload(
            bucket_name=bucket_name,
            object_name=dest,
            filename=output_file,
        )
        logging.info(
            "Uploaded %s (%s bytes) to gs://%s/%s",
            output_file,
            os.path.getsize(output_file),
            bucket_name,
            dest,
        )
        context["task_instance"].xcom_push(key="odoo_id_count", value=count)
        return count
    except Exception as exc:
        logging.error("Error extracting Odoo IDs: %s", exc)
        raise
    finally:
        if cur is not None:
            cur.close()
        odoo.close()


def identify_orphan_records(**context):
    """Log orphan IDs (snapshot open, missing from Odoo) before UPDATE."""
    logging.info("Identifying orphan records to soft-close...")

    try:
        hook = BigQueryHook(gcp_conn_id=gcp_conn_id, use_legacy_sql=False)
        query = f"""
        SELECT snapshot.id
        FROM `{snapshot_table}` snapshot
        LEFT JOIN `{temp_fqn}` odoo_ids
            ON snapshot.id = odoo_ids.id
        WHERE odoo_ids.id IS NULL
            AND snapshot.dbt_valid_to IS NULL
        ORDER BY snapshot.id
        """
        records = hook.get_pandas_df(query)

        if records.empty:
            logging.info("No orphan records found — snapshot in sync with Odoo IDs")
            context["task_instance"].xcom_push(key="orphan_count", value=0)
            context["task_instance"].xcom_push(key="orphan_ids_sample", value=[])
            return 0

        orphan_ids = records["id"].tolist()
        count = len(orphan_ids)
        logging.info("Found %s orphan records to soft-close", f"{count:,}")

        if count <= 50:
            logging.info("IDs to be updated: %s", orphan_ids)
        else:
            logging.info("Sample IDs (first 20): %s", orphan_ids[:20])
            logging.info("Sample IDs (last 20): %s", orphan_ids[-20:])
            logging.info("... and %s more IDs", f"{count - 40:,}")

        # Cap XCom sample; full list lived in BQ for the UPDATE.
        context["task_instance"].xcom_push(key="orphan_count", value=count)
        context["task_instance"].xcom_push(
            key="orphan_ids_sample", value=orphan_ids[:100]
        )
        return count
    except Exception as exc:
        logging.error("Error identifying orphan records: %s", exc)
        raise


def get_stats_and_log_summary(**context):
    """Post-run counts: Odoo vs active snapshot, this-run closes, sync band."""
    ti = context["task_instance"]
    odoo_count = ti.xcom_pull(task_ids="extract_odoo_ids", key="odoo_id_count")
    records_updated_this_run = (
        ti.xcom_pull(task_ids="identify_orphans", key="orphan_count") or 0
    )
    orphan_ids_sample = (
        ti.xcom_pull(task_ids="identify_orphans", key="orphan_ids_sample") or []
    )

    odoo_total = odoo_count
    snapshot_total = 0
    deleted_today_all_runs = 0
    total_historical = 0
    difference = 0
    checked_at = "N/A"

    logging.info("Querying BigQuery for cleanup statistics...")
    try:
        hook = BigQueryHook(gcp_conn_id=gcp_conn_id, use_legacy_sql=False)
        query = f"""
        WITH odoo_count AS (
            SELECT COUNT(*) AS odoo_total
            FROM `{temp_fqn}`
        ),
        snapshot_active AS (
            SELECT COUNT(DISTINCT id) AS snapshot_total
            FROM `{snapshot_table}`
            WHERE dbt_valid_to IS NULL
        ),
        snapshot_deleted_today AS (
            SELECT COUNT(DISTINCT id) AS deleted_today
            FROM `{snapshot_table}`
            WHERE DATE(dbt_valid_to) = CURRENT_DATE()
        ),
        snapshot_total_historical AS (
            SELECT COUNT(DISTINCT id) AS total_historical
            FROM `{snapshot_table}`
            WHERE dbt_valid_to IS NOT NULL
        )
        SELECT
            odoo_count.odoo_total,
            snapshot_active.snapshot_total,
            snapshot_deleted_today.deleted_today,
            snapshot_total_historical.total_historical,
            (odoo_count.odoo_total - snapshot_active.snapshot_total) AS difference,
            CURRENT_TIMESTAMP() AS checked_at
        FROM odoo_count, snapshot_active, snapshot_deleted_today,
             snapshot_total_historical
        """
        records = hook.get_pandas_df(query)
        if not records.empty:
            stats = records.iloc[0].to_dict()
            odoo_total = int(stats.get("odoo_total", odoo_count))
            snapshot_total = int(stats.get("snapshot_total", 0))
            deleted_today_all_runs = int(stats.get("deleted_today", 0))
            total_historical = int(stats.get("total_historical", 0))
            difference = int(stats.get("difference", 0))
            checked_at = str(stats.get("checked_at", "N/A"))
        else:
            logging.warning("Stats query returned no rows; using XCom defaults")
    except Exception as exc:
        logging.error("Error querying BigQuery stats: %s", exc)
        logging.warning("Continuing with available XCom data...")

    logging.info("=" * 80)
    logging.info("SALE ORDER LINE CLEANUP SUMMARY")
    logging.info("=" * 80)
    logging.info("SOURCE (Odoo) total IDs: %s", f"{odoo_total:,}")

    if snapshot_total > 0 or total_historical > 0:
        logging.info(
            "SNAPSHOT active (dbt_valid_to NULL): %s", f"{snapshot_total:,}"
        )
        logging.info(
            "SNAPSHOT historical (dbt_valid_to set): %s", f"{total_historical:,}"
        )
        logging.info(
            "THIS RUN soft-closed: %s", f"{records_updated_this_run:,}"
        )
        if records_updated_this_run > 0:
            if len(orphan_ids_sample) <= 20:
                logging.info("Sample IDs updated: %s", orphan_ids_sample)
            else:
                logging.info("First 10 IDs: %s", orphan_ids_sample[:10])
                logging.info("Last 10 IDs: %s", orphan_ids_sample[-10:])
        else:
            logging.info("No orphans — open snapshot matches Odoo ID set")

        if deleted_today_all_runs > records_updated_this_run:
            logging.info(
                "Today soft-closed (all runs): %s", f"{deleted_today_all_runs:,}"
            )

        abs_diff = abs(difference)
        logging.info("Difference (Odoo - active snapshot): %s", f"{difference:,}")
        if abs_diff <= SYNC_OK:
            logging.info("Sync status: IN SYNC (abs diff < %s)", SYNC_OK)
        elif abs_diff <= SYNC_WARN:
            logging.info(
                "Sync status: MINOR (abs diff %s–%s) — timing skew possible",
                SYNC_OK,
                SYNC_WARN,
            )
        else:
            logging.info(
                "Sync status: SIGNIFICANT (abs diff > %s) — investigate before "
                "trusting downstream ID exports",
                SYNC_WARN,
            )
        logging.info("Checked at: %s", checked_at)
    else:
        logging.info("Unable to retrieve detailed BigQuery snapshot stats")

    logging.info("=" * 80)
    return {
        "status": "completed",
        "odoo_count": odoo_count,
        "snapshot_count": snapshot_total,
        "updated_this_run": records_updated_this_run,
        "deleted_today_all_runs": deleted_today_all_runs,
        "total_historical": total_historical,
        "difference": difference,
    }


dag = DAG(
    dag_id="etl_odoo_sale_order_line_snapshot_cleanup",
    default_args=default_args,
    schedule_interval=schedule,
    render_template_as_native_obj=True,
    catchup=False,
    max_active_runs=1,
    description=(
        "[WEEKLY] Soft-close deleted Odoo sale_order_line rows in the dbt "
        "snapshot (set dbt_valid_to; preserve history)"
    ),
    tags=["odoo", "sale_order_line", "snapshot", "cleanup", "weekly"],
)

start = EmptyOperator(
    task_id="start",
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
)

extract_odoo_ids = PythonOperator(
    task_id="extract_odoo_ids",
    python_callable=extract_odoo_sale_order_line_ids,
    provide_context=True,
    dag=dag,
    execution_timeout=timedelta(minutes=30),
)

load_odoo_ids_to_bq = GCSToBigQueryOperator(
    task_id="load_odoo_ids_to_bq",
    gcp_conn_id=gcp_conn_id,
    bucket=bucket_name,
    source_objects=[
        "odoo_cleanup/sale_order_line_ids/{{ ds }}/odoo_sale_order_line_ids.json"
    ],
    destination_project_dataset_table=temp_fqn,
    source_format="NEWLINE_DELIMITED_JSON",
    write_disposition="WRITE_TRUNCATE",
    create_disposition="CREATE_IF_NEEDED",
    autodetect=True,
    dag=dag,
)

identify_orphans = PythonOperator(
    task_id="identify_orphans",
    python_callable=identify_orphan_records,
    provide_context=True,
    dag=dag,
)

update_orphan_records = BigQueryInsertJobOperator(
    task_id="update_orphan_records",
    gcp_conn_id=gcp_conn_id,
    configuration={
        "query": {
            "query": f"""
            -- Soft-close: set dbt_valid_to; do not DELETE snapshot rows.
            UPDATE `{snapshot_table}`
            SET dbt_valid_to = CURRENT_TIMESTAMP()
            WHERE id IN (
                SELECT snapshot.id
                FROM `{snapshot_table}` snapshot
                LEFT JOIN `{temp_fqn}` odoo_ids
                    ON snapshot.id = odoo_ids.id
                WHERE odoo_ids.id IS NULL
                    AND snapshot.dbt_valid_to IS NULL
            )
            AND dbt_valid_to IS NULL
            """,
            "useLegacySql": False,
        }
    },
    dag=dag,
)

get_stats_and_log = PythonOperator(
    task_id="get_stats_and_log",
    python_callable=get_stats_and_log_summary,
    provide_context=True,
    dag=dag,
)

cleanup_temp_table = BigQueryDeleteTableOperator(
    task_id="cleanup_temp_table",
    gcp_conn_id=gcp_conn_id,
    deletion_dataset_table=temp_fqn,
    ignore_if_missing=True,
    dag=dag,
)

end = EmptyOperator(
    task_id="end",
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
)

chain(
    start,
    extract_odoo_ids,
    load_odoo_ids_to_bq,
    identify_orphans,
    update_orphan_records,
    get_stats_and_log,
    cleanup_temp_table,
    end,
)
