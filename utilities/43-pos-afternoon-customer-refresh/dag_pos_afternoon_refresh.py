"""Airflow DAG: midday POS vendor customer-master refresh.

Runs at 13:00 Europe/Amsterdam (DST-safe CronTriggerTimetable). Loads
only debtor + location dumps from the vendor GCS drop zone into BigQuery
staging, then re-runs the customer / matching / POS / Tableau dbt chain
and materializes the refined customer-base view as a physical table.

Distinct from pattern 35 (HMAC store-details API) and pattern 38
(GA4 rolling events): this is a selective afternoon re-land of the same
semicolon CSVs the overnight POS job already consumes, scoped to
customer master so sales-facing models stay current without replaying
the full overnight graph.

Source (read-only):
  dags/etl_dish_pos_afternoon.py
"""

from __future__ import annotations

import functools
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
)
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.timetables.trigger import CronTriggerTimetable
from airflow.utils.trigger_rule import TriggerRule

from customer_master_load import load_customer_master_table

try:
    from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
except ImportError:  # pragma: no cover - reference stub
    DbtCloudRunJobOperator = None

# Parse-time date token (production behaviour). Prefer {{ ds_nodash }} if
# you rewrite for backfills — list_blobs filter must match object names.
DATE_TOKEN = datetime.today().strftime("_%Y%m%dT")

PROJECT_ID = Variable.get("dwh_project", default_var="dwh_project")
SOURCE_BUCKET = Variable.get(
    "pos_vendor_drop_bucket",
    default_var="pos-vendor-drop",
)
DATASET_STAGING = Variable.get(
    "trusted_staging_dataset",
    default_var="trusted_staging",
)
DATASET_REFINED = Variable.get(
    "refined_dataset",
    default_var="refined",
)
CUSTOMER_BASE_VIEW = Variable.get(
    "pos_customer_base_view",
    default_var="vw_vendor_customer_base",
)
CUSTOMER_BASE_TABLE = Variable.get(
    "pos_customer_base_table",
    default_var="vendor_customer_base",
)
SLACK_CONN_ID = Variable.get(
    "pos_afternoon_slack_conn_id",
    default_var="slack_webhook_default",
)
SLACK_CHANNEL = Variable.get(
    "pos_afternoon_slack_channel",
    default_var="#pos-data-ops",
)

# dbt Cloud job ids — empty string → EmptyOperator stub for reference checkouts.
DBT_JOB_CUSTOMER = Variable.get("pos_afternoon_dbt_customer_job_id", default_var="")
DBT_JOB_MATCHING = Variable.get("pos_afternoon_dbt_matching_job_id", default_var="")
DBT_JOB_POS = Variable.get("pos_afternoon_dbt_pos_job_id", default_var="")
DBT_JOB_TABLEAU = Variable.get("pos_afternoon_dbt_tableau_job_id", default_var="")

CUSTOMER_TABLES = ["vendor_debtor", "vendor_location"]


def send_slack_failure_notification(context):
    """Failure callback — keep the alert short; logs have the stack."""
    ti = context.get("task_instance")
    dag_run = context.get("dag_run")
    task_id = ti.task_id if ti else "unknown"
    dag_id = dag_run.dag_id if dag_run else "unknown"
    execution_date = (
        dag_run.execution_date.strftime("%Y-%m-%d %H:%M:%S")
        if dag_run and dag_run.execution_date
        else "unknown"
    )
    message = (
        f"*POS afternoon customer refresh failed*\n"
        f"DAG: {dag_id}\n"
        f"Task: {task_id}\n"
        f"Execution: {execution_date}\n"
        f"Impact: debtor/location staging or downstream dbt may be stale."
    )
    SlackWebhookOperator(
        task_id="slack_failure_notification",
        slack_webhook_conn_id=SLACK_CONN_ID,
        message=message,
        channel=SLACK_CHANNEL,
        username="airflow-pos-afternoon",
    ).execute(context)


# 13:00 local — CronTriggerTimetable shifts with CET/CEST so midday
# sales refresh does not drift an hour twice a year.
schedule = CronTriggerTimetable(
    cron="0 13 * * *",
    timezone="Europe/Amsterdam",
)

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "start_date": datetime(2021, 6, 29),
    "email": ["dataops@example.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "dbt_cloud_conn_id": "dbt_cloud_default",
    "account_id": 1,
    "on_failure_callback": send_slack_failure_notification,
}

dag = DAG(
    dag_id="etl_pos_afternoon_customer_refresh",
    default_args=default_args,
    schedule=schedule,
    catchup=False,
    description=(
        "Midday POS vendor debtor + location refresh → dbt customer "
        "chain → materialize customer base"
    ),
    tags=["pos", "vendor", "customer-master", "afternoon", "dbt"],
    max_active_runs=1,
    max_active_tasks=5,
    doc_md=__doc__,
)


def _dbt_or_stub(task_id: str, job_id: str, timeout: int):
    if DbtCloudRunJobOperator is not None and job_id:
        return DbtCloudRunJobOperator(
            task_id=task_id,
            job_id=int(job_id),
            check_interval=10,
            do_xcom_push=True,
            dag=dag,
            timeout=timeout,
            reuse_existing_run=True,
            retry_from_failure=True,
        )
    return EmptyOperator(
        task_id=task_id,
        dag=dag,
        doc_md="Stub: set Variable and install dbt Cloud provider to run.",
    )


end_task = BashOperator(
    task_id="end_task",
    bash_command="echo afternoon customer refresh completed",
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
)

dbt_customer = _dbt_or_stub("dbt_vendor_customer", DBT_JOB_CUSTOMER, 1000)
dbt_matching = _dbt_or_stub("dbt_pos_matching_ids", DBT_JOB_MATCHING, 1500)
dbt_pos = _dbt_or_stub("dbt_pos", DBT_JOB_POS, 1500)
dbt_tableau = _dbt_or_stub("dbt_pos_tableau", DBT_JOB_TABLEAU, 1000)

customer_base = BigQueryInsertJobOperator(
    task_id="materialize_customer_base",
    configuration={
        "query": {
            "query": (
                f"SELECT * FROM `{PROJECT_ID}.{DATASET_REFINED}.{CUSTOMER_BASE_VIEW}`"
            ),
            "useLegacySql": False,
            "destinationTable": {
                "projectId": PROJECT_ID,
                "datasetId": DATASET_REFINED,
                "tableId": CUSTOMER_BASE_TABLE,
            },
            "writeDisposition": "WRITE_TRUNCATE",
            "createDisposition": "CREATE_IF_NEEDED",
        }
    },
    gcp_conn_id="bigquery_default",
    dag=dag,
)

load_tasks = []
for tbl in CUSTOMER_TABLES:
    load_task = PythonOperator(
        task_id=f"load_{tbl}",
        python_callable=functools.partial(
            load_customer_master_table,
            PROJECT_ID,
            DATASET_STAGING,
            SOURCE_BUCKET,
            tbl,
            DATE_TOKEN,
        ),
        trigger_rule=TriggerRule.ALL_DONE,
        dag=dag,
    )
    load_tasks.append(load_task)

# Both loads must finish before the shared dbt chain starts once.
for load_task in load_tasks:
    load_task >> dbt_customer

(
    dbt_customer
    >> dbt_matching
    >> dbt_pos
    >> dbt_tableau
    >> customer_base
    >> end_task
)
