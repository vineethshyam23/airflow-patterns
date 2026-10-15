"""Hydra v2: Cloud SQL Admin export → GCS CSV → BigQuery staging → dbt.

Weekly full dump of website CMS tables. No incremental watermark —
historization lives in dbt snapshots downstream. Tables export
sequentially (Cloud SQL one-op-at-a-time), each paired with a GCS→BQ
load, then one dbt Cloud job tagged ``hydra_v2``.

Replaces the legacy per-table INSERT/UPDATE path for most tables;
legacy DAG remains only for a short derived-events allowlist.

Source (read-only):
  dags/etl_hydra_job_v2.py
  dags/horeca_digital/hydra_raw_export_queries.py
  dags/horeca_digital/operators/cloudsql_retry_operator.py
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

from cloudsql_export_operator import CloudSqlExportOperatorWithScheduleAware
from hydra_export_queries import HYDRA_RAW_TABLES, RAW_TABLE_EXPORT_QUERIES

try:
    from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
except ImportError:  # pragma: no cover - reference stub
    DbtCloudRunJobOperator = None

logger = logging.getLogger(__name__)

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "start_date": datetime(2026, 4, 1),
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

environment = "env"
env = os.environ.get(environment, Variable.get(environment, default_var="DEV"))

if env == "DEV":
    PROJECT_ID = "dwh_project_dev"
    EXPORT_GCP_CONN_ID = "bigquery_default_dev"
    GCS_TO_BQ_CONN_ID = "google_cloud_dev"
else:
    PROJECT_ID = "dwh_project"
    EXPORT_GCP_CONN_ID = "bigquery_default"
    GCS_TO_BQ_CONN_ID = "google_cloud_default"

# Cloud SQL source — production used a product website MySQL instance.
# Keep identifiers in Variables so real project/instance names stay out of git.
SOURCE_DB = Variable.get("hydra_cloudsql_database", default_var="website_prod")
SOURCE_PROJECT = Variable.get(
    "hydra_cloudsql_project", default_var="website_project"
)
SOURCE_INSTANCE = Variable.get(
    "hydra_cloudsql_instance", default_var="db-prod-mysql-master"
)

BUCKET = Variable.get("hydra_raw_bucket", default_var="dwh-rawzone")
DATASET_STAGING = "dwh_trusted_staging"
TABLE_PREFIX = "hyd_v2"

try:
    DBT_JOB_ID = Variable.get("hydra_v2_dbt_job_id")
except KeyError:
    DBT_JOB_ID = None

BACKUP_START_HOUR = int(
    Variable.get("hydra_backup_window_start_hour", default_var="3")
)
BACKUP_DURATION_HOURS = int(
    Variable.get("hydra_backup_window_duration_hours", default_var="2")
)

dag = DAG(
    dag_id="etl_hydra_job_v2",
    default_args=default_args,
    # Production cron was daily 01:30; docstring called it a weekly full dump.
    # Keep the production schedule; gate table set size in HYDRA_RAW_TABLES.
    schedule_interval="30 1 * * *",
    max_active_runs=1,
    catchup=False,
    tags=["etl", "hydra", "raw", "cloudsql", "weekly-full"],
    doc_md=__doc__,
)


def _cloudsql_export_task(*, table: str, select_sql: str):
    gcs_path = f"hydra_raw/{table}/{{{{ ds }}}}/000000/{table}.csv"
    export_uri = f"gs://{BUCKET}/{gcs_path}"

    return CloudSqlExportOperatorWithScheduleAware(
        task_id=f"export_{table}",
        body={
            "exportContext": {
                "uri": export_uri,
                "fileType": "CSV",
                "csvExportOptions": {"selectQuery": select_sql},
                "databases": [SOURCE_DB],
            }
        },
        project_id=SOURCE_PROJECT,
        instance=SOURCE_INSTANCE,
        gcp_conn_id=EXPORT_GCP_CONN_ID,
        max_operation_retries=3,
        operation_retry_delay=600,
        backup_window_start_hour=BACKUP_START_HOUR,
        backup_window_duration_hours=BACKUP_DURATION_HOURS,
        dag=dag,
    )


def _gcs_to_bq_load_task(*, table: str):
    bq_table = f"{TABLE_PREFIX}_{table}"
    source_object = f"hydra_raw/{table}/{{{{ ds }}}}/000000/{table}.csv"

    return GoogleCloudStorageToBigQueryOperator(
        task_id=f"load_{bq_table}",
        gcp_conn_id=GCS_TO_BQ_CONN_ID,
        bucket=BUCKET,
        source_format="CSV",
        source_objects=[source_object],
        destination_project_dataset_table=(
            f"{PROJECT_ID}.{DATASET_STAGING}.{bq_table}"
        ),
        schema_object=f"schema_json/{bq_table}.json",
        allow_quoted_newlines=True,
        ignore_unknown_values=True,
        allow_jagged_rows=False,
        quote_character='"',
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        trigger_rule=TriggerRule.ALL_SUCCESS,
        dag=dag,
    )


start = EmptyOperator(task_id="start", dag=dag)
end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE, dag=dag)

if DbtCloudRunJobOperator is not None and DBT_JOB_ID:
    hydra_v2_dbt_job = DbtCloudRunJobOperator(
        task_id="hydra_v2_dbt_job",
        job_id=int(DBT_JOB_ID),
        check_interval=10,
        do_xcom_push=True,
        timeout=300,
        reuse_existing_run=True,
        retry_from_failure=True,
        dag=dag,
    )
else:
    hydra_v2_dbt_job = EmptyOperator(task_id="hydra_v2_dbt_job", dag=dag)

prev_task = None
last_load_task = None

with TaskGroup(group_id="weekly_full_exports", dag=dag) as weekly_full_exports:
    for spec in HYDRA_RAW_TABLES:
        table = spec.name
        export_task = _cloudsql_export_task(
            table=table,
            select_sql=RAW_TABLE_EXPORT_QUERIES[table],
        )
        load_task = _gcs_to_bq_load_task(table=table)
        export_task >> load_task

        if prev_task is None:
            start >> export_task
        else:
            prev_task >> export_task

        prev_task = load_task
        last_load_task = load_task

# TaskGroup retained for UI grouping; chain already wires start → … → dbt → end.
_ = weekly_full_exports

if last_load_task is not None:
    last_load_task >> hydra_v2_dbt_job >> end
else:
    start >> end
