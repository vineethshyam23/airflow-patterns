"""Daily payment wallet API ingest: KYC + transactions + VOP → GCS → BQ → dbt.

Fan-out over three DWH API feeds with different pagination contracts,
land NDJSON on Composer, copy to rawzone, branch on empty file, load
staging (TRUNCATE for KYC, APPEND for transactions / VOP), then trigger
two dbt Cloud jobs. Ops get per-feed status notifications with API vs
loaded row counts.

Distinct from pattern 11 (outbound KYC Avro → partner event bus) and
pattern 03 (Adyen Management API terminals). This is the *inbound*
payment-product land-and-stage pipeline.

Source (read-only):
  dags/etl_dishpay_dbt.py
  dags/horeca_digital/get_dish_pay_data.py
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.utils.helpers import chain
from airflow.utils.trigger_rule import TriggerRule
from google.cloud import storage

from payment_api import get_loaded_data_count, get_payment_wallet_data

try:
    from airflow.operators.empty import EmptyOperator
except ModuleNotFoundError:  # pragma: no cover
    from airflow.operators.dummy import DummyOperator as EmptyOperator  # type: ignore

try:
    from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
except ImportError:  # pragma: no cover - reference stub
    DbtCloudRunJobOperator = None

try:
    from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
except ImportError:  # pragma: no cover
    SlackWebhookOperator = None


def check_file_vol(**kwargs):
    """Branch on whether the rawzone NDJSON blob has any bytes."""
    file_path = kwargs["file_path"]
    file_name = kwargs["file_name"]
    client = storage.Client(project=projectid)
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(f"{file_path}/{file_name}.json")
    try:
        if not blob.download_as_bytes():
            return f"no_data_{file_name}"
        return f"process_data_{file_name}"
    except Exception as exc:
        # Surface as a branch target so the DAG does not hard-fail mid-branch.
        # Prefer raising in a rewrite once empty-vs-missing is explicit.
        return f"no_data_{file_name}"


default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "start_date": datetime(2023, 12, 20),
    "email": ["dataops@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
    "dbt_cloud_conn_id": "dbt_conn",
    "account_id": 1,
}

environment = "env"
env = os.environ.get(environment, Variable.get(environment, default_var="DEV"))

# JSON Variable: {"base_url", "token_url", "client_id", "client_secret"}
_creds = Variable.get(
    "payment_wallet_creds",
    default_var={
        "base_url": "https://wallet.example.com",
        "token_url": "https://wallet.example.com/auth/oauth/token",
        "client_id": "",
        "client_secret": "",
    },
    deserialize_json=True,
)
base_url = _creds["base_url"]
token_url = _creds["token_url"]
client_id = _creds["client_id"]
client_secret = _creds["client_secret"]

if env == "DEV":
    bucket_name = "rawzone_dev"
    projectid = "dwh_project_dev"
    gcp_conn_id = "google_cloud_dev"
else:
    bucket_name = "rawzone"
    projectid = "dwh_project"
    gcp_conn_id = "google_cloud_default"

COMPOSER_BUCKET = Variable.get("composer_bucket", default_var="composer-data")

# Production used day-2 window to avoid same-day API lag. Prefer {{ ds }}
# / data_interval in a rewrite for backfill-safe paths.
_window_day = datetime.now() - timedelta(days=1)
_start = _window_day.replace(hour=0, minute=0, second=0, microsecond=0)
_end = datetime(_window_day.year, _window_day.month, _window_day.day, 23, 59, 59)

load_date = _start.strftime("%Y-%m-%d")
start_date = _start.strftime("%Y-%m-%dT%H:%M:%S.000Z")
end_date = _end.strftime("%Y-%m-%dT%H:%M:%S.000Z")

FILENAMES = (
    "payment_kyc",
    "payment_transactions",
    "payment_vop_performance",
)

# VOP is one API call per reportDate. Optional Variables enable a
# multi-day backfill in a single manual run; clear them afterwards.
VOP_EARLIEST_DATE = "2026-07-14"
vop_backfill_start = Variable.get("payment_vop_backfill_start", default_var=None)
vop_backfill_end = Variable.get("payment_vop_backfill_end", default_var=None)

if vop_backfill_start and vop_backfill_end:
    _bf_start = datetime.strptime(vop_backfill_start, "%Y-%m-%d")
    _bf_end = datetime.strptime(vop_backfill_end, "%Y-%m-%d")
    vop_report_dates = [
        (_bf_start + timedelta(days=i)).strftime("%Y-%m-%d")
        for i in range((_bf_end - _bf_start).days + 1)
    ]
else:
    vop_report_dates = [load_date]

try:
    DBT_JOB_KYC = Variable.get("payment_wallet_dbt_kyc_job_id")
except KeyError:
    DBT_JOB_KYC = None

try:
    DBT_JOB_TXN = Variable.get("payment_wallet_dbt_transactions_job_id")
except KeyError:
    DBT_JOB_TXN = None

op_kwargs_base = {
    "token_url": token_url,
    "client_id": client_id,
    "client_secret": client_secret,
    "base_url": base_url,
}

dag = DAG(
    dag_id="etl_payment_wallet_dbt",
    default_args=default_args,
    schedule_interval="10 4 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["payment", "wallet", "api", "daily"],
    doc_md=__doc__,
)

start = EmptyOperator(task_id="start", trigger_rule=TriggerRule.ALL_DONE, dag=dag)
stage_1 = EmptyOperator(task_id="stage_1", trigger_rule=TriggerRule.ALL_DONE, dag=dag)
stage_2 = EmptyOperator(task_id="stage_2", trigger_rule=TriggerRule.ALL_DONE, dag=dag)
end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE, dag=dag)

for file_name in FILENAMES:
    if "kyc" in file_name:
        file_path = f"payment-wallet/kyc/{load_date}"
    elif "vop" in file_name:
        file_path = f"payment-wallet/vop_performance/{load_date}"
    else:
        file_path = f"payment-wallet/transactions/{load_date}"

    process_data = EmptyOperator(
        task_id=f"process_data_{file_name}",
        trigger_rule=TriggerRule.ALL_DONE,
        dag=dag,
    )
    no_data = EmptyOperator(task_id=f"no_data_{file_name}", dag=dag)

    data_fetch = PythonOperator(
        task_id=f"data_fetch_{file_name}",
        python_callable=get_payment_wallet_data,
        op_kwargs={
            **op_kwargs_base,
            "file_name": file_name,
            "end_date": end_date,
            "start_date": start_date,
            "report_dates": vop_report_dates,
        },
        do_xcom_push=True,
        dag=dag,
    )

    upload_storage = GCSToGCSOperator(
        task_id=f"upload_storage_{file_name}",
        gcp_conn_id=gcp_conn_id,
        source_bucket=COMPOSER_BUCKET,
        source_object=f"data/payment_wallet/{file_name}.json",
        destination_bucket=bucket_name,
        destination_object=f"{file_path}/{file_name}.json",
        dag=dag,
    )

    is_file_empty = BranchPythonOperator(
        task_id=f"is_file_empty_{file_name}",
        python_callable=check_file_vol,
        op_kwargs={"file_path": file_path, "file_name": file_name},
        dag=dag,
    )

    chain(start, data_fetch, upload_storage, is_file_empty, [process_data, no_data])

    write_disposition = (
        "WRITE_APPEND"
        if file_name in ("payment_transactions", "payment_vop_performance")
        else "WRITE_TRUNCATE"
    )

    data_load_staging = GCSToBigQueryOperator(
        task_id=f"load_staging_{file_name}",
        gcp_conn_id=gcp_conn_id,
        bucket=bucket_name,
        source_format="CSV",
        source_objects=[f"{file_path}/{file_name}.json"],
        destination_project_dataset_table=(
            f"{projectid}.trusted_staging.{file_name}"
        ),
        skip_leading_rows=0,
        schema_fields=[{"name": "value", "type": "JSON", "mode": "NULLABLE"}],
        write_disposition=write_disposition,
        autodetect=False,
        create_disposition="CREATE_IF_NEEDED",
        field_delimiter="\t",
        dag=dag,
    )

    chain(process_data, data_load_staging, stage_1)
    chain(no_data, end)


def _dbt_or_empty(task_id: str, job_id):
    if DbtCloudRunJobOperator is not None and job_id:
        return DbtCloudRunJobOperator(
            task_id=task_id,
            dbt_cloud_conn_id="dbt_conn",
            job_id=int(job_id),
            check_interval=10,
            do_xcom_push=True,
            dag=dag,
            timeout=300,
        )
    return EmptyOperator(task_id=task_id, dag=dag)


payment_kyc_dbt = _dbt_or_empty("payment_kyc_dbt", DBT_JOB_KYC)
payment_transactions_dbt = _dbt_or_empty("payment_transactions_dbt", DBT_JOB_TXN)

# VOP models share the KYC dbt job tag in production — no separate job.
chain(stage_1, payment_kyc_dbt, stage_2)
chain(stage_1, payment_transactions_dbt, stage_2)


def check_all_success(**context):
    """Collect task states for the notification tasks (exclude self + Slack)."""
    dr: DagRun = context["dag_run"]
    ti: TaskInstance = context["ti"]
    skip = {
        ti.task_id,
        "notify_transactions",
        "notify_kyc",
        "notify_vop",
        "start",
        "stage_1",
        "stage_2",
        "end",
    }
    return {
        task.task_id: task.state
        for task in dr.get_task_instances()
        if task.task_id not in skip
    }


check_all_tasks = PythonOperator(
    task_id="check_all_tasks",
    python_callable=check_all_success,
    do_xcom_push=True,
    dag=dag,
)


def _loaded_count_callable(**context):
    return get_loaded_data_count(project_id=projectid)


get_loaded_data_count_task = PythonOperator(
    task_id="get_loaded_data_count",
    python_callable=_loaded_count_callable,
    do_xcom_push=True,
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
)


def send_notification(message: str, suffix: str = "default") -> None:
    if SlackWebhookOperator is None:
        print(f"[notify:{suffix}] {message}")
        return
    try:
        SlackWebhookOperator(
            task_id=f"payment_wallet_slack_{suffix}",
            slack_webhook_conn_id="slack_conn",
            message=message,
            channel="#payment-dwh-alerts",
            username="Payment Wallet Integration",
            dag=dag,
        ).execute({})
    except Exception as exc:
        print(f"Failed to send Slack notification: {exc}")


def notify_feed(ti, notification_type: str):
    """Per-feed success / failure summary for ops."""
    config = {
        "transactions": {
            "related_tasks": [
                "data_fetch_payment_transactions",
                "upload_storage_payment_transactions",
                "load_staging_payment_transactions",
                "payment_transactions_dbt",
            ],
            "data_fetch_task_id": "data_fetch_payment_transactions",
            "destination_table": "payment_transactions",
            "display_name": "Transactions",
            "use_loaded_count": True,
        },
        "kyc": {
            "related_tasks": [
                "data_fetch_payment_kyc",
                "upload_storage_payment_kyc",
                "load_staging_payment_kyc",
                "payment_kyc_dbt",
            ],
            "data_fetch_task_id": "data_fetch_payment_kyc",
            "destination_table": "payment_kyc",
            "display_name": "KYC",
            "use_loaded_count": False,
        },
        "vop": {
            "related_tasks": [
                "data_fetch_payment_vop_performance",
                "upload_storage_payment_vop_performance",
                "load_staging_payment_vop_performance",
                "payment_kyc_dbt",
            ],
            "data_fetch_task_id": "data_fetch_payment_vop_performance",
            "destination_table": "payment_vop_performance",
            "display_name": "VOP Performance",
            "use_loaded_count": False,
        },
    }
    if notification_type not in config:
        raise ValueError(f"Invalid notification_type: {notification_type}")

    cfg = config[notification_type]
    today = datetime.today().strftime("%Y-%m-%d")

    try:
        task_status = ti.xcom_pull(task_ids="check_all_tasks", key="return_value") or {}
        failed_tasks = {
            task: state
            for task, state in task_status.items()
            if task in cfg["related_tasks"] and state == "failed"
        }

        if failed_tasks:
            task_list = "\n".join(
                f"- *{task}*: {state}" for task, state in failed_tasks.items()
            )
            message = (
                f"*Payment wallet {cfg['display_name']} load failed*\n\n"
                f"*Failed tasks ({len(failed_tasks)})*:\n{task_list}\n\n"
                f"*Load date*: {today}\n*Project*: {projectid}"
            )
            send_notification(message, f"failures_{notification_type}")
            return

        fetched = (
            ti.xcom_pull(task_ids=cfg["data_fetch_task_id"], key="return_value") or {}
        )
        fetched_count = fetched.get("api_records_count", 0) if fetched else 0

        if cfg["use_loaded_count"]:
            loaded_count = (
                ti.xcom_pull(task_ids="get_loaded_data_count", key="return_value") or 0
            )
            message = (
                f"*Payment wallet {cfg['display_name']} load OK*\n\n"
                f"*API records*: {fetched_count:,}\n"
                f"*Loaded*: {loaded_count:,}\n"
                f"*Load date*: {today}\n"
                f"*Destination*: {projectid}.trusted.{cfg['destination_table']}"
            )
        else:
            message = (
                f"*Payment wallet {cfg['display_name']} load OK*\n\n"
                f"*API records*: {fetched_count:,}\n"
                f"*Load date*: {today}\n"
                f"*Destination*: {projectid}.trusted.{cfg['destination_table']}"
            )
        send_notification(message, f"success_{notification_type}")
    except Exception as exc:
        send_notification(
            f"*Notification error ({cfg['display_name']})*: {exc}",
            f"error_{notification_type}",
        )


notify_transactions = PythonOperator(
    task_id="notify_transactions",
    python_callable=notify_feed,
    op_kwargs={"notification_type": "transactions"},
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
)
notify_kyc = PythonOperator(
    task_id="notify_kyc",
    python_callable=notify_feed,
    op_kwargs={"notification_type": "kyc"},
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
)
notify_vop = PythonOperator(
    task_id="notify_vop",
    python_callable=notify_feed,
    op_kwargs={"notification_type": "vop"},
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
)

chain(
    stage_2,
    check_all_tasks,
    get_loaded_data_count_task,
    [notify_transactions, notify_kyc, notify_vop],
    end,
)
