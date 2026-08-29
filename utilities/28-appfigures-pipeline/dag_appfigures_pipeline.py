"""Weekly AppFigures mobile-app analytics → staging → trusted → dbt.

Four parallel ingest chains (sales, ratings, ratings_product,
ratings_country): API CSV → Composer data dir → rawzone GCS →
BigQuery staging (TRUNCATE) → trusted (APPEND). A stage barrier
gates one dbt Cloud job after all four land.

Date window is previous Mon–Sun. Production computed that at module
import time (anti-pattern for backfills); this sample keeps the same
shape so the graph matches what ran, with a note in DATA_FLOW.md.

Source (read-only):
  dags/etl_appfigures_pipeline.py
  dags/horeca_digital/get_appfigures_data.py
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.bigquery_to_bigquery import (
    BigQueryToBigQueryOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.utils.helpers import chain
from airflow.utils.trigger_rule import TriggerRule

from fetch_appfigures_data import fetch_appfigures_data

try:
    from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
except ImportError:  # pragma: no cover - reference stub
    DbtCloudRunJobOperator = None

DEFAULT_RETRY_DELAY_MINUTES = 10

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "start_date": datetime(2022, 10, 23),
    "email": ["dataops@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=DEFAULT_RETRY_DELAY_MINUTES),
    "dbt_cloud_conn_id": "dbt_conn",
    "account_id": 1,
}

environment = "env"
env = os.environ.get(environment, Variable.get(environment, default_var="DEV"))

if env == "DEV":
    BUCKET_NAME = "rawzone_dev"
    PROJECT_ID = "dwh_project_dev"
    GCP_CONN_ID = "google_cloud_dev"
else:
    BUCKET_NAME = "rawzone"
    PROJECT_ID = "dwh_project"
    GCP_CONN_ID = "google_cloud_default"

COMPOSER_BUCKET = Variable.get("composer_bucket", default_var="composer-data")

# Prefer Variable / Secret Manager. Empty default keeps the sample
# importable without secrets; fetch task will fail closed on bad auth.
AUTH_TOKEN = Variable.get("appfigures_auth_token", default_var="")

try:
    DBT_JOB_ID = Variable.get("appfigures_dbt_job_id")
except KeyError:
    DBT_JOB_ID = None

dag = DAG(
    dag_id="etl_appfigures_pipeline",
    default_args=default_args,
    schedule_interval="59 3 * * 1",
    catchup=False,
    max_active_runs=1,
    tags=["appfigures", "mobile", "analytics", "weekly"],
    doc_md=__doc__,
)

start = EmptyOperator(task_id="start", trigger_rule=TriggerRule.ALL_DONE, dag=dag)
stage = EmptyOperator(task_id="stage", trigger_rule=TriggerRule.ALL_DONE, dag=dag)
end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE, dag=dag)

# Parse-time week window (production behaviour). Prefer {{ ds }}-based
# macros if you rewrite for reliable backfills.
today = datetime.today()
start_date_of_last_week = today - timedelta(days=today.weekday() + 7)
end_date_of_last_week = start_date_of_last_week + timedelta(days=6)
from_date = start_date_of_last_week.strftime("%Y-%m-%d")
to_date = end_date_of_last_week.strftime("%Y-%m-%d")

parameters = {
    "start_date": from_date,
    "end_date": to_date,
    "format": "csv",
}

filenames = ["sales", "ratings", "ratings_product", "ratings_country"]

for file_name in filenames:
    report_type = (
        "ratings" if file_name in ("ratings_product", "ratings_country") else file_name
    )

    data_fetch = PythonOperator(
        task_id=f"data_fetch_{file_name}",
        python_callable=fetch_appfigures_data,
        op_kwargs={
            "report_type": report_type,
            "parameters": parameters,
            "authorization_token": AUTH_TOKEN,
            "file_name": file_name,
        },
        do_xcom_push=True,
        dag=dag,
    )

    upload_storage = GCSToGCSOperator(
        task_id=f"upload_storage_{file_name}",
        gcp_conn_id=GCP_CONN_ID,
        source_bucket=COMPOSER_BUCKET,
        source_object=f"data/appfigures/appfigures_{file_name}.csv",
        destination_bucket=BUCKET_NAME,
        destination_object=f"appfigures/{file_name}/{to_date}/{file_name}.csv",
        dag=dag,
    )

    data_load_staging = GCSToBigQueryOperator(
        task_id=f"load_staging_{file_name}",
        gcp_conn_id=GCP_CONN_ID,
        bucket=BUCKET_NAME,
        source_format="CSV",
        field_delimiter=",",
        source_objects=[f"appfigures/{file_name}/{to_date}/{file_name}.csv"],
        destination_project_dataset_table=(
            f"{PROJECT_ID}.trusted_staging.appfigures_{file_name}"
        ),
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        skip_leading_rows=1,
        schema_object=f"schema_json/appfigures_{file_name}.json",
        dag=dag,
    )

    copy_table_trusted = BigQueryToBigQueryOperator(
        task_id=f"copy_table_trusted_appfigures_{file_name}",
        source_project_dataset_tables=(
            f"{PROJECT_ID}.trusted_staging.appfigures_{file_name}"
        ),
        destination_project_dataset_table=(
            f"{PROJECT_ID}.trusted.appfigures_{file_name}"
        ),
        write_disposition="WRITE_APPEND",
        create_disposition="CREATE_IF_NEEDED",
        gcp_conn_id=GCP_CONN_ID,
        location="EU",
        dag=dag,
    )

    chain(
        start,
        data_fetch,
        upload_storage,
        data_load_staging,
        copy_table_trusted,
        stage,
    )

if DBT_JOB_ID is not None and DbtCloudRunJobOperator is not None:
    appfigures_dbt_job = DbtCloudRunJobOperator(
        task_id="appfigures_dbt",
        job_id=int(DBT_JOB_ID),
        check_interval=10,
        do_xcom_push=True,
        timeout=300,
        dag=dag,
    )
else:
    appfigures_dbt_job = EmptyOperator(task_id="appfigures_dbt", dag=dag)

chain(stage, appfigures_dbt_job, end)
