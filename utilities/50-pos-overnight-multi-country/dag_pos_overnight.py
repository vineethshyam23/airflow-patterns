"""Airflow DAG: overnight multi-country POS land + dbt chain.

Runs at 07:00 Europe/Amsterdam (DST-safe CronTriggerTimetable).

Two modes, same graph:
  - Daily: yesterday's ticket JSONL + today's master CSVs
  - Backfill: set Variables pos_overnight_backfill_start/end (YYYY-MM-DD);
    transaction + mapping callables walk each day in one run

Distinct from pattern 43 (midday debtor/location only) and pattern 35
(HMAC store-details API): this is the full overnight spine — master
fan-out, five-country ticket APPEND, tenant-debtor mapping, then the
shared customer / matching / POS / Tableau dbt jobs.

Source (read-only):
  dags/etl_dish_pos.py
"""

from __future__ import annotations

import functools
import json
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

from country_transactions import (
    COUNTRY_SPECS,
    load_mapping_files_for_date_range,
    load_transaction_files_for_date_range,
    move_transaction_files_for_date_range,
)
from date_range import is_backfill_mode
from vendor_master_load import MASTER_TABLES, load_master_table

try:
    from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
except ImportError:  # pragma: no cover - reference stub
    DbtCloudRunJobOperator = None

# Parse-time date token for master CSV names (production behaviour).
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
DATASET_TRUSTED = Variable.get("trusted_dataset", default_var="trusted")
DATASET_REFINED = Variable.get("refined_dataset", default_var="refined")
CUSTOMER_BASE_VIEW = Variable.get(
    "pos_customer_base_view",
    default_var="vw_vendor_customer_base",
)
CUSTOMER_BASE_TABLE = Variable.get(
    "pos_customer_base_table",
    default_var="vendor_customer_base",
)
SLACK_CONN_ID = Variable.get(
    "pos_overnight_slack_conn_id",
    default_var="slack_webhook_default",
)
SLACK_CHANNEL = Variable.get(
    "pos_overnight_slack_channel",
    default_var="#pos-data-ops",
)

DBT_JOB_CUSTOMER = Variable.get("pos_overnight_dbt_customer_job_id", default_var="")
DBT_JOB_MATCHING = Variable.get("pos_overnight_dbt_matching_job_id", default_var="")
DBT_JOB_POS = Variable.get("pos_overnight_dbt_pos_job_id", default_var="")
DBT_JOB_TABLEAU = Variable.get("pos_overnight_dbt_tableau_job_id", default_var="")

# Per-country transaction dbt jobs (Variable JSON or empty → stub).
# Example: {"NL":"111","DE":"222","FR":"333","IT":"444","ES":"555"}
_COUNTRY_JOBS_RAW = Variable.get("pos_overnight_dbt_country_job_ids", default_var="{}")


def _country_job_id(iso: str) -> str:
    try:
        mapping = json.loads(_COUNTRY_JOBS_RAW) if _COUNTRY_JOBS_RAW else {}
    except json.JSONDecodeError:
        mapping = {}
    return str(mapping.get(iso, "") or "")


def send_slack_failure_notification(context):
    """Failure callback — short alert; logs have the stack."""
    ti = context.get("task_instance")
    dag_run = context.get("dag_run")
    task_id = ti.task_id if ti else "unknown"
    dag_id = dag_run.dag_id if dag_run else "unknown"
    execution_date = (
        dag_run.execution_date.strftime("%Y-%m-%d %H:%M:%S")
        if dag_run and dag_run.execution_date
        else "unknown"
    )
    mode = "BACKFILL" if is_backfill_mode() else "DAILY"
    message = (
        f"*POS overnight pipeline failed*\n"
        f"DAG: {dag_id}\n"
        f"Task: {task_id}\n"
        f"Execution: {execution_date}\n"
        f"Mode: {mode}\n"
        f"Owners: dataops@example.com"
    )
    SlackWebhookOperator(
        task_id="slack_failure_notification",
        slack_webhook_conn_id=SLACK_CONN_ID,
        message=message,
        channel=SLACK_CHANNEL,
        username="Airflow-POS-Overnight",
    ).execute(context)


def _dbt_or_empty(task_id: str, job_id: str, timeout: int = 1500):
    if DbtCloudRunJobOperator is None or not job_id:
        return EmptyOperator(task_id=task_id, dag=dag)
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


default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "start_date": datetime(2021, 6, 29),
    "email": ["dataops@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "dbt_cloud_conn_id": "dbt_conn",
    "account_id": 3,
    "on_failure_callback": send_slack_failure_notification,
}

schedule = CronTriggerTimetable(
    cron="0 7 * * *",
    timezone="Europe/Amsterdam",
)

dag = DAG(
    dag_id="etl_pos_overnight",
    default_args=default_args,
    schedule=schedule,
    catchup=False,
    description=(
        "Overnight multi-country POS land (master + tickets + mapping) "
        "with optional date-range backfill and shared dbt chain"
    ),
    tags=["POS", "transactions", "multi-country", "dbt", "backfill"],
    max_active_runs=1,
    max_active_tasks=15,
    doc_md=__doc__,
)

end_file_loading = EmptyOperator(
    task_id="end_file_loading",
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
)
end_of_mapping = BashOperator(
    task_id="end_of_customer_transaction_mapping",
    bash_command="echo mapping fan-in done",
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
)
end_dbt = BashOperator(
    task_id="end_dbt",
    bash_command="echo dbt chain done",
    dag=dag,
)
end_task = BashOperator(
    task_id="end_task",
    bash_command="echo end",
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
)

dbt_customer = _dbt_or_empty("dbt_vendor_customer", DBT_JOB_CUSTOMER, timeout=1000)
dbt_matching = _dbt_or_empty("pos_matching_ids", DBT_JOB_MATCHING, timeout=1500)
dbt_pos = _dbt_or_empty("pos_transforms", DBT_JOB_POS, timeout=1500)
dbt_tableau = _dbt_or_empty("dbt_pos_tableau", DBT_JOB_TABLEAU, timeout=1000)

materialize_customer_base = BigQueryInsertJobOperator(
    task_id="vendor_customer_base",
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

# --- Master fan-out ---
for tbl in MASTER_TABLES:
    PythonOperator(
        task_id=f"_load_{tbl}",
        python_callable=functools.partial(
            load_master_table,
            PROJECT_ID,
            DATASET_STAGING,
            SOURCE_BUCKET,
            tbl,
            DATE_TOKEN,
        ),
        trigger_rule=TriggerRule.ALL_DONE,
        dag=dag,
    ) >> dbt_customer

# --- Mapping fan-out (NL/DE/FR/IT/ES) ---
for iso, country, _path in COUNTRY_SPECS:
    PythonOperator(
        task_id=f"load_customer_transaction_mapping_{iso}",
        python_callable=functools.partial(
            load_mapping_files_for_date_range,
            PROJECT_ID,
            DATASET_TRUSTED,
            SOURCE_BUCKET,
            iso,
            country,
        ),
        trigger_rule=TriggerRule.ALL_DONE,
        dag=dag,
    ) >> end_of_mapping

end_of_mapping >> dbt_customer

# --- Per-country tickets: load → country dbt → archive ---
for iso, country, path in COUNTRY_SPECS:
    load_op = PythonOperator(
        task_id=f"load_date_based_csv_{iso}",
        python_callable=functools.partial(
            load_transaction_files_for_date_range,
            PROJECT_ID,
            DATASET_STAGING,
            SOURCE_BUCKET,
            iso,
            country,
            path,
        ),
        dag=dag,
    )
    country_dbt = _dbt_or_empty(
        f"pos_transactions_{iso}",
        _country_job_id(iso),
        timeout=1500,
    )
    move_op = PythonOperator(
        task_id=f"move_files_{iso}",
        python_callable=functools.partial(
            move_transaction_files_for_date_range,
            SOURCE_BUCKET,
            iso,
            country,
            path,
        ),
        dag=dag,
    )
    load_op >> country_dbt >> move_op >> end_file_loading

end_file_loading >> dbt_matching >> dbt_pos >> dbt_tableau >> end_dbt
dbt_customer >> dbt_pos
end_dbt >> materialize_customer_base >> end_task
