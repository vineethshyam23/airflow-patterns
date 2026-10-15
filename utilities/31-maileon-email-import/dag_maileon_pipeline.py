"""Daily Maileon email marketing import — 8 report branches + metadata + dbt.

Per report: REST extract (XML→JSONL on Composer data/) → empty-file branch →
rawzone copy → BigQuery staging (WRITE_TRUNCATE). After all loads: dbt
transform, then mailing name/tag enrichment (per-id API) and two more dbt
jobs.

Empty API days skip the GCS copy via BranchPythonOperator; the load task
still joins with none_failed_or_skipped (production quirk — can TRUNCATE
staging with a stale/missing object). Prefer skipping the load on empty
days if you reuse this graph.

Source (read-only):
  dags/etl_maileon_import.py
  dags/horeca_digital/maileon.py
  dags/horeca_digital/get_maileon_names.py
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.utils.helpers import chain
from airflow.utils.trigger_rule import TriggerRule

from get_maileon_metadata import get_maileon_names, get_maileon_tags
from maileon_api import import_maileon_data

try:
    from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
except ImportError:  # pragma: no cover - reference stub
    DbtCloudRunJobOperator = None

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
    "email": ["dataops@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "dbt_cloud_conn_id": "dbt_conn",
    "account_id": 1,
}

REPORTS = [
    "opens",
    "opens_unique",
    "clicks",
    "clicks_unique",
    "bounces",
    "blocks",
    "unsubscriptions",
    "recipients",
]

REPORT_ENDPOINT_MAPPING = {
    "opens": "/reports/opens",
    "opens_unique": "/reports/opens/unique",
    "clicks": "/reports/clicks",
    "clicks_unique": "/reports/clicks/unique",
    "bounces": "/reports/bounces",
    "blocks": "/reports/blocks",
    "unsubscriptions": "/reports/unsubscriptions",
    "recipients": "/reports/recipients",
}


def check_file_and_branch(bucket_name, file_path, report, **context):
    """Route to copy_* when Composer blob size > 0, else skip_*."""
    from google.cloud import storage

    try:
        client = storage.Client()
        blob = client.bucket(bucket_name).blob(file_path)
        if blob.exists():
            blob.reload()
            if blob.size and blob.size > 0:
                return f"copy_{report}_to_rawzone"
        return f"skip_{report}_empty_file"
    except Exception as e:
        print(f"Error checking {file_path}: {e} — skipping copy")
        return f"skip_{report}_empty_file"


environment = "env"
env = os.environ.get(environment, Variable.get(environment, default_var="DEV"))

API_KEY = Variable.get("maileon_apikey", default_var="")
COMPOSER_BUCKET = Variable.get("composer_bucket", default_var="composer-data")

if env == "DEV":
    PROJECT_ID = Variable.get("maileon_gcp_project", default_var="dwh_project_dev")
    BUCKET_NAME = Variable.get("maileon_rawzone_bucket", default_var="rawzone_dev")
    GCP_CONN_ID = "google_cloud_dev"
    schedule = None
else:
    PROJECT_ID = Variable.get("maileon_gcp_project", default_var="dwh_project")
    BUCKET_NAME = Variable.get("maileon_rawzone_bucket", default_var="rawzone")
    GCP_CONN_ID = "google_cloud_default"
    schedule = "0 2 * * *"

# Composer data dirs — one per report (same layout in DEV/PROD).
REPORT_LOCAL_PATHS = {
    report: f"/home/airflow/gcs/data/maileon/{report}/" for report in REPORTS
}

# Production baked datetime.now() at parse time for both report_date and
# loaddate. Prefer {{ ds }} / data_interval in a real deploy; kept here so
# the sample matches the production footgun.
REPORT_DATE = datetime.now().strftime("%Y%m%d")
LOAD_DATE = datetime.now().strftime("%Y-%m-%d")

DBT_TRANSFORM_JOB = Variable.get("maileon_dbt_transform_job_id", default_var=None)
DBT_NAMES_JOB = Variable.get("maileon_dbt_names_job_id", default_var=None)
DBT_API_JOB = Variable.get("maileon_dbt_api_job_id", default_var=None)

dag = DAG(
    dag_id="etl_maileon_import",
    default_args=default_args,
    schedule_interval=schedule,
    catchup=False,
    max_active_runs=1,
    tags=["maileon", "email-marketing", "daily"],
    doc_md=__doc__,
)

stage = EmptyOperator(
    task_id="stage", trigger_rule=TriggerRule.ALL_DONE, dag=dag
)

load_tasks = []

for report in REPORTS:
    extract_task = PythonOperator(
        task_id=f"get_{report}_report",
        python_callable=import_maileon_data,
        op_kwargs={
            "api_key": API_KEY,
            "bucket_loc": COMPOSER_BUCKET,
            "project_id": PROJECT_ID,
            "report_type": report,
            "endpoint": REPORT_ENDPOINT_MAPPING[report],
            "local_path": REPORT_LOCAL_PATHS[report],
        },
        dag=dag,
    )

    branch_task = BranchPythonOperator(
        task_id=f"branch_{report}_file_check",
        python_callable=check_file_and_branch,
        op_kwargs={
            "bucket_name": COMPOSER_BUCKET,
            "file_path": f"data/maileon/{report}/{report}_{REPORT_DATE}.jsonl",
            "report": report,
        },
        dag=dag,
    )

    copy_task = GCSToGCSOperator(
        task_id=f"copy_{report}_to_rawzone",
        source_bucket=COMPOSER_BUCKET,
        source_object=f"data/maileon/{report}/{report}_{REPORT_DATE}.jsonl",
        destination_bucket=BUCKET_NAME,
        destination_object=f"maileon/{REPORT_DATE}/{report}.jsonl",
        gcp_conn_id=GCP_CONN_ID,
        dag=dag,
    )

    load_task = GCSToBigQueryOperator(
        task_id=f"load_{report}_to_bq",
        bucket=BUCKET_NAME,
        source_objects=[f"maileon/{REPORT_DATE}/{report}.jsonl"],
        destination_project_dataset_table=(
            f"{PROJECT_ID}.trusted_staging.maileon_{report}"
        ),
        source_format="NEWLINE_DELIMITED_JSON",
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
        schema_object=f"schema_json/maileon_{report}.json",
        autodetect=False,
        gcp_conn_id=GCP_CONN_ID,
        trigger_rule="none_failed_or_skipped",
        dag=dag,
    )

    skip_task = EmptyOperator(task_id=f"skip_{report}_empty_file", dag=dag)

    extract_task >> branch_task
    branch_task >> [copy_task, skip_task]
    copy_task >> load_task
    skip_task >> load_task
    load_tasks.append(load_task)


def _dbt_or_empty(task_id: str, job_id, timeout: int, **kwargs):
    if DbtCloudRunJobOperator is None or not job_id:
        return EmptyOperator(task_id=task_id, dag=dag, **kwargs)
    return DbtCloudRunJobOperator(
        task_id=task_id,
        job_id=int(job_id),
        check_interval=10,
        timeout=timeout,
        do_xcom_push=True,
        dag=dag,
        **kwargs,
    )


dbt_transform = _dbt_or_empty(
    "dbt_transform_maileon", DBT_TRANSFORM_JOB, timeout=300
)

fetch_names = PythonOperator(
    task_id="fetch_maileon_names",
    python_callable=get_maileon_names,
    op_kwargs={
        "maileon_api_key": API_KEY,
        "execution_date": LOAD_DATE,
        "tmp_loc": "/home/airflow/gcs/data/maileon_names/",
    },
    do_xcom_push=True,
    dag=dag,
)

load_names = GCSToBigQueryOperator(
    task_id="load_data_to_bq_names",
    gcp_conn_id=GCP_CONN_ID,
    bucket=COMPOSER_BUCKET,
    source_format="NEWLINE_DELIMITED_JSON",
    source_objects=[f"data/maileon_names/names_{LOAD_DATE}.json"],
    destination_project_dataset_table="trusted_staging.maileon_names_tbl",
    schema_object="data/maileon_names/maileon_names.json",
    autodetect=False,
    create_disposition="CREATE_IF_NEEDED",
    write_disposition="WRITE_TRUNCATE",
    dag=dag,
)

fetch_tags = PythonOperator(
    task_id="fetch_maileon_tags",
    python_callable=get_maileon_tags,
    op_kwargs={
        "maileon_api_key": API_KEY,
        "execution_date": LOAD_DATE,
        "tmp_loc": "/home/airflow/gcs/data/maileon_tags/",
    },
    do_xcom_push=True,
    dag=dag,
)

load_tags = GCSToBigQueryOperator(
    task_id="load_data_to_bq_tags",
    gcp_conn_id=GCP_CONN_ID,
    bucket=COMPOSER_BUCKET,
    source_format="NEWLINE_DELIMITED_JSON",
    source_objects=[f"data/maileon_tags/tags_{LOAD_DATE}.json"],
    destination_project_dataset_table="trusted_staging.maileon_tags_tbl",
    schema_object="data/maileon_tags/maileon_tags.json",
    autodetect=False,
    create_disposition="CREATE_IF_NEEDED",
    write_disposition="WRITE_TRUNCATE",
    dag=dag,
)

# Production called chain(load_tasks, dbt, fetch, load, stage) twice inside
# a names/tags loop, which duplicated edges. One linear chain here.
chain(
    load_tasks,
    dbt_transform,
    fetch_names,
    load_names,
    fetch_tags,
    load_tags,
    stage,
)

dbt_names = _dbt_or_empty("dbt_get_maileon", DBT_NAMES_JOB, timeout=15000)
dbt_api = _dbt_or_empty(
    "dbt_maileon_api",
    DBT_API_JOB,
    timeout=15000,
    trigger_rule=TriggerRule.ALL_DONE,
)

chain(stage, dbt_names, dbt_api)
