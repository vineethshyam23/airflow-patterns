"""Daily Vonage Contact Center stats → raw NDJSON → BQ staging → dbt.

Five parallel chains (agent activities, presence, status, interactions,
queue times): OAuth fetch → Composer data dir → rawzone GCS → BigQuery
staging as a single JSON column (WRITE_TRUNCATE). A stage barrier gates
one dbt Cloud job, then optional Slack status with API-vs-refined
row counts.

Date window is calendar yesterday (UTC wall clock at parse/run).
Production also computed day_before_yesterday with a TODO about vendor
lag; the live window stayed on yesterday. Documented in DATA_FLOW.md.

Source (read-only):
  dags/etl_vonage_dbt.py
  dags/horeca_digital/get_vonage_data.py
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.utils.helpers import chain
from airflow.utils.trigger_rule import TriggerRule

from fetch_vonage_data import get_loaded_data_count, get_vonage_data

try:
    from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
except ImportError:  # pragma: no cover - reference stub
    DbtCloudRunJobOperator = None

DEFAULT_RETRY_DELAY_MINUTES = 10

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "start_date": datetime(2023, 12, 20),
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

# Prefer Variable JSON: {"client_id": "...", "client_secret": "..."}
try:
    VONAGE_CREDS = Variable.get("vonage_creds", deserialize_json=True)
except KeyError:
    VONAGE_CREDS = {"client_id": "", "client_secret": ""}

CLIENT_ID = VONAGE_CREDS.get("client_id", "")
CLIENT_SECRET = VONAGE_CREDS.get("client_secret", "")

try:
    DBT_JOB_ID = Variable.get("vonage_dbt_job_id")
except KeyError:
    DBT_JOB_ID = None

SLACK_CONN_ID = Variable.get("vonage_slack_conn_id", default_var="slack_ops_channel")
SLACK_CHANNEL = Variable.get(
    "vonage_slack_channel", default_var="#data-platform-ops"
)

# Production used yesterday 00:00–23:59. A day_before_yesterday helper
# existed with a vendor-lag TODO but was not wired into the window.
yesterday = datetime.now() - timedelta(days=1)
yesterday_start = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
yesterday_end = datetime(
    yesterday.year, yesterday.month, yesterday.day, 23, 59, 59
)
LOAD_DATE = yesterday_start.strftime("%Y-%m-%d")
START_DATE = yesterday_start.strftime("%Y-%m-%dT%H:%M:%S.000Z")
END_DATE = yesterday_end.strftime("%Y-%m-%dT%H:%M:%S.000Z")

FILENAMES = [
    "vonage_agent_activities",
    "vonage_agent_presence",
    "vonage_agent_status",
    "vonage_interactions",
    "vonage_queue_times",
]

op_kwargs_base = {
    "client_id": CLIENT_ID,
    "client_secret": CLIENT_SECRET,
    "start_date": START_DATE,
    "end_date": END_DATE,
}

dag = DAG(
    dag_id="etl_vonage_dbt",
    default_args=default_args,
    schedule_interval="10 4 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["vonage", "contact-center", "stats", "daily"],
    doc_md=__doc__,
)

start = EmptyOperator(task_id="start", trigger_rule=TriggerRule.ALL_DONE, dag=dag)
stage = EmptyOperator(task_id="stage", trigger_rule=TriggerRule.ALL_DONE, dag=dag)
stage_1 = EmptyOperator(task_id="stage_1", trigger_rule=TriggerRule.ALL_DONE, dag=dag)
stage_2 = EmptyOperator(task_id="stage_2", trigger_rule=TriggerRule.ALL_DONE, dag=dag)
end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE, dag=dag)

for file_name in FILENAMES:
    file_path = f"vonage/{file_name}/{LOAD_DATE}"

    data_fetch = PythonOperator(
        task_id=f"data_fetch_{file_name}",
        python_callable=get_vonage_data,
        op_kwargs={**op_kwargs_base, "file_name": file_name},
        do_xcom_push=True,
        dag=dag,
    )

    upload_storage = GCSToGCSOperator(
        task_id=f"upload_storage_{file_name}",
        gcp_conn_id=GCP_CONN_ID,
        source_bucket=COMPOSER_BUCKET,
        source_object=f"data/vonage/{file_name}.ndjson",
        destination_bucket=BUCKET_NAME,
        destination_object=f"{file_path}/{file_name}.ndjson",
        dag=dag,
    )

    # NDJSON loaded as a single JSON column via tab delimiter + schema.
    # Keeps schema drift in the vendor payload from breaking the load;
    # dbt unpacks `value` into refined models.
    data_load_staging = GCSToBigQueryOperator(
        task_id=f"load_staging_{file_name}",
        gcp_conn_id=GCP_CONN_ID,
        bucket=BUCKET_NAME,
        source_format="CSV",
        source_objects=[f"{file_path}/{file_name}.ndjson"],
        destination_project_dataset_table=(
            f"{PROJECT_ID}.trusted_staging.{file_name}"
        ),
        skip_leading_rows=0,
        schema_fields=[{"name": "value", "type": "JSON", "mode": "NULLABLE"}],
        write_disposition="WRITE_TRUNCATE",
        autodetect=False,
        create_disposition="CREATE_IF_NEEDED",
        field_delimiter="\t",
        dag=dag,
    )

    chain(start, data_fetch, upload_storage, data_load_staging, stage)

if DBT_JOB_ID is not None and DbtCloudRunJobOperator is not None:
    vonage_dbt = DbtCloudRunJobOperator(
        task_id="vonage_dbt",
        job_id=int(DBT_JOB_ID),
        check_interval=10,
        do_xcom_push=True,
        timeout=300,
        dag=dag,
    )
else:
    vonage_dbt = EmptyOperator(task_id="vonage_dbt", dag=dag)

chain(stage, vonage_dbt, stage_1)


def check_all_success(**context):
    """Collect sibling task states for Slack status (excludes self / barriers)."""
    dr: DagRun = context["dag_run"]
    ti: TaskInstance = context["ti"]
    excluded = {"start", "stage_1", "stage_2", "end", ti.task_id}
    summary = {}
    for task in dr.get_task_instances():
        if task.task_id in excluded or task.task_id.startswith("slacknotification_"):
            continue
        summary[task.task_id] = task.state
    return summary


check_all_tasks = PythonOperator(
    task_id="check_all_tasks",
    python_callable=check_all_success,
    provide_context=True,
    do_xcom_push=True,
    dag=dag,
)


def slack_notification(**context):
    """Per-grain success / failure note. Soft-fails if Slack is unavailable."""
    ti = context["ti"]
    table_name = context["params"]["table_name"]

    def send(message: str, suffix: str) -> None:
        try:
            SlackWebhookOperator(
                task_id=f"vonage_slack_{suffix}_{table_name}",
                slack_webhook_conn_id=SLACK_CONN_ID,
                message=message,
                channel=SLACK_CHANNEL,
                username="vonage-ingest",
                dag=dag,
            ).execute({})
        except Exception as exc:  # noqa: BLE001 - notification must not fail DAG
            print(f"Slack notification failed: {exc}")

    try:
        task_status = ti.xcom_pull(task_ids="check_all_tasks", key="return_value") or {}
        failed = {t: s for t, s in task_status.items() if s == "failed"}
        load_date = datetime.today().strftime("%Y-%m-%d")

        if failed:
            lines = "\n".join(f"- {t}: {s}" for t, s in failed.items())
            send(
                f"Vonage load failed ({len(failed)} tasks)\n"
                f"{lines}\nLoad date: {load_date}\nProject: {PROJECT_ID}",
                "failures",
            )
            return

        loaded = (
            ti.xcom_pull(
                task_ids=f"get_loaded_data_count_{table_name}",
                key="return_value",
            )
            or 0
        )
        fetched = ti.xcom_pull(task_ids=f"data_fetch_{table_name}", key="return_value")
        fetched_count = fetched.get("api_records_count", 0) if fetched else 0
        send(
            f"{table_name} loaded\n"
            f"API records: {fetched_count:,}\n"
            f"Refined loaded today: {loaded:,}\n"
            f"Load date: {load_date}\n"
            f"Dest: {PROJECT_ID}.refined_sales.{table_name}",
            f"success_{table_name}",
        )
    except Exception as exc:  # noqa: BLE001
        send(f"Vonage Slack status error: {exc}", "error")


for table_name in FILENAMES:
    get_count = PythonOperator(
        task_id=f"get_loaded_data_count_{table_name}",
        python_callable=get_loaded_data_count,
        op_kwargs={"table_name": table_name, "project_id": PROJECT_ID},
        do_xcom_push=True,
        trigger_rule=TriggerRule.ALL_DONE,
        provide_context=True,
        dag=dag,
    )
    notify = PythonOperator(
        task_id=f"slacknotification_{table_name}",
        provide_context=True,
        python_callable=slack_notification,
        params={"table_name": table_name},
        trigger_rule=TriggerRule.ALL_DONE,
        dag=dag,
    )
    chain(stage_1, check_all_tasks, get_count, stage_2, notify, end)
