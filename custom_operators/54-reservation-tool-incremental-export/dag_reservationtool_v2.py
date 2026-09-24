"""Reservation Tool v2 — incremental Cloud SQL export → staging → dbt.

Replaces a legacy full-dump DAG. Daily volume drops from tens of GB
to a few GB by watermarking auto_increment tables on ``MAX(id)`` from
BigQuery staging, then exporting ``WHERE id > max_id``. Tables without
a usable watermark stay on full WRITE_TRUNCATE. Every Sunday the DAG
forces a full baseline (max_id=0 + truncate incremental staging) so
dbt snapshots can detect deletes.

Exports are serial (Cloud SQL one-op-per-instance). Each export fans
out to its own GCS→BQ load so loads overlap the next export.

Distinct from pattern 39 (Hydra): that path is full REPLACE every run
with no id watermark. This path is the incremental + weekly-full
sibling for an append-heavy product OLTP schema.

Source (read-only):
  dags/etl_reservationtool_v2.py
  dags/horeca_digital/rt_table_config.py
  dags/horeca_digital/operators/cloudsql_retry_operator.py
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.utils.helpers import chain
from airflow.utils.trigger_rule import TriggerRule
from google.cloud import bigquery

from cloudsql_export_operator import CloudSqlExportOperatorWithScheduleAware
from rt_table_config import (
    FULL_LOAD_TABLES,
    INCREMENTAL_TABLES,
    build_select_query,
)

try:
    from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
except ImportError:  # pragma: no cover - reference stub
    DbtCloudRunJobOperator = None

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration — real project / instance / bucket stay in Variables
# ---------------------------------------------------------------------------

PROJECT_ID = Variable.get("rt_dwh_project", default_var="dwh_project")
DATASET_STAGING = Variable.get(
    "rt_staging_dataset", default_var="dwh_trusted_staging"
)
BUCKET = Variable.get("rt_raw_bucket", default_var="dwh-rawzone")

SOURCE_DB = Variable.get("rt_cloudsql_database", default_var="reservation_prod")
SOURCE_PROJECT = Variable.get(
    "rt_cloudsql_project", default_var="reservation_project"
)
SOURCE_INSTANCE = Variable.get(
    "rt_cloudsql_instance", default_var="db-prod-mysql-master"
)

WEEKLY_SYNC_DAY = 6  # Python weekday(): Monday=0 … Sunday=6

try:
    DBT_JOB_ID = Variable.get("rt_dbt_job_id")
except KeyError:
    DBT_JOB_ID = None

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "start_date": datetime(2020, 7, 12),
    "email": ["dataops@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=15),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(hours=1),
    "dbt_cloud_conn_id": "dbt_conn",
    "account_id": 1,
}


def _get_max_ids(**context):
    """Push per-table high-water marks (and weekly flag) to XCom.

    On weekly sync day every incremental max_id is forced to 0 so the
    export SELECT becomes ``WHERE id > 0`` (full dump). Staging for
    those tables is truncated first so the subsequent APPEND is a
    clean reload.
    """
    execution_date = context["execution_date"]
    is_weekly = execution_date.weekday() == WEEKLY_SYNC_DAY
    ti = context["ti"]
    ti.xcom_push(key="is_weekly", value=is_weekly)

    client = bigquery.Client(project=PROJECT_ID)

    if is_weekly:
        logger.info(
            "Weekly sync day — forcing full export (max_id=0 for all tables)"
        )
        for table in INCREMENTAL_TABLES:
            ti.xcom_push(key=f"{table}_max_id", value=0)
            fqn = f"`{PROJECT_ID}.{DATASET_STAGING}.rt_{table}`"
            try:
                client.query(f"TRUNCATE TABLE {fqn}").result()
                logger.info("Truncated %s for weekly full reload", fqn)
            except Exception as exc:
                logger.warning(
                    "Could not truncate %s (may not exist yet): %s", fqn, exc
                )
        return

    for table in INCREMENTAL_TABLES:
        fqn = f"`{PROJECT_ID}.{DATASET_STAGING}.rt_{table}`"
        try:
            rows = list(
                client.query(
                    f"SELECT COALESCE(MAX(id), 0) AS max_id FROM {fqn}"
                ).result()
            )
            max_id = int(rows[0].max_id) if rows else 0
        except Exception as exc:
            logger.warning(
                "Could not query max(id) for %s — defaulting to 0: %s",
                table,
                exc,
            )
            max_id = 0

        ti.xcom_push(key=f"{table}_max_id", value=max_id)
        logger.info("%s: max_id = %d", table, max_id)


dag = DAG(
    dag_id="etl_reservationtool_v2",
    default_args=default_args,
    schedule_interval="0 1 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["reservation_tool", "dbt", "incremental"],
    doc_md=__doc__,
)

start = EmptyOperator(task_id="start", dag=dag)
all_loaded = EmptyOperator(
    task_id="all_loaded", trigger_rule=TriggerRule.ALL_DONE, dag=dag
)
end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE, dag=dag)

get_max_ids = PythonOperator(
    task_id="get_max_ids",
    python_callable=_get_max_ids,
    dag=dag,
)

# Cloud SQL allows only ONE concurrent export per instance, so export
# tasks are chained sequentially. Each export fans out to its own load
# so BigQuery ingestion overlaps the next export.
#
#   get_max_ids
#     → export_A → load_A ──────────────┐
#         ↓                              │
#       export_B → load_B ─────────┐     │
#         ↓                        ↓     ↓
#       export_N → load_N → all_loaded → dbt → end

ALL_TABLES = sorted(set(INCREMENTAL_TABLES) | set(FULL_LOAD_TABLES))

prev_export_task = None
load_tasks = []

for table in ALL_TABLES:
    is_incremental = table in INCREMENTAL_TABLES
    base_select = build_select_query(table)

    gcs_path = (
        "reservationtool/" + table + "/{{ ds }}/000000/" + table + ".csv"
    )
    export_uri = "gs://" + BUCKET + "/" + gcs_path

    if is_incremental:
        export_query = (
            base_select
            + " WHERE id > "
            + "{{ (ti.xcom_pull(task_ids='get_max_ids', key='"
            + table
            + "_max_id') or 0) | int }}"
        )
    else:
        export_query = base_select

    export_task = CloudSqlExportOperatorWithScheduleAware(
        task_id=f"export_{table}",
        body={
            "exportContext": {
                "uri": export_uri,
                "fileType": "CSV",
                "csvExportOptions": {"selectQuery": export_query},
                "databases": [SOURCE_DB],
            }
        },
        project_id=SOURCE_PROJECT,
        instance=SOURCE_INSTANCE,
        gcp_conn_id="bigquery_default",
        max_operation_retries=8,
        operation_retry_delay=600,
        backup_window_start_hour=3,
        backup_window_duration_hours=2,
        dag=dag,
    )

    # Schema JSON lives in GCS: gs://<bucket>/schema_json/rt_<table>.json
    load_task = GCSToBigQueryOperator(
        task_id=f"load_{table}",
        gcp_conn_id="google_cloud_default",
        bucket=BUCKET,
        source_format="CSV",
        source_objects=[gcs_path],
        destination_project_dataset_table=f"{DATASET_STAGING}.rt_{table}",
        schema_object=f"schema_json/rt_{table}.json",
        allow_quoted_newlines=True,
        max_bad_records=500,
        create_disposition="CREATE_IF_NEEDED",
        write_disposition=(
            "WRITE_APPEND" if is_incremental else "WRITE_TRUNCATE"
        ),
        dag=dag,
    )

    export_task >> load_task
    if prev_export_task is None:
        get_max_ids >> export_task
    else:
        prev_export_task >> export_task
    prev_export_task = export_task
    load_tasks.append(load_task)

for lt in load_tasks:
    lt >> all_loaded

# dbt owns PII masking, surrogate keys, SCD Type 2, and
# restricted / confidential variants of customers / establishments.
if DbtCloudRunJobOperator is not None and DBT_JOB_ID:
    dbt_rt_run = DbtCloudRunJobOperator(
        task_id="dbt_rt_run",
        job_id=int(DBT_JOB_ID),
        check_interval=30,
        timeout=7200,
        do_xcom_push=True,
        dag=dag,
    )
else:
    dbt_rt_run = EmptyOperator(
        task_id="dbt_rt_run",
        dag=dag,
    )
    logger.info(
        "dbt provider missing or rt_dbt_job_id unset — using EmptyOperator stub"
    )

chain(start, get_max_ids)
chain(all_loaded, dbt_rt_run, end)
