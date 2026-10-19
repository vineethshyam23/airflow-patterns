"""
Airflow DAG: parallel country fan-out of export_* views → trusted_mcc.

Flow:
  start
    → one copy task per country (parallel)
    → prepare_failed_views_for_slack (ALL_DONE)
    → send_slack_notification (Slack API bot)
    → end

Each copy task lists views/tables in export_{cnt} and materializes
them into dwh_project.trusted_mcc.{cnt}_{name} (WRITE_TRUNCATE).
Failed views return as plain XCom lists; the prepare task aggregates
and builds the Slack text.

Source (read-only):
  dags/etl_hd_export_to_trusted_mcc.py
  dags/horeca_digital/hd_export_to_trusted_mcc.py

Distinct from wholesale NL inbound (#69) / HubSpot outbound (#70):
this is cross-project BigQuery view materialization with per-country
parallelism and aggregated Slack failure reporting — not an MCC API
or CSV land.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.slack.operators.slack import SlackAPIPostOperator
from airflow.utils.helpers import chain
from airflow.utils.trigger_rule import TriggerRule

from hd_export_to_trusted_mcc import (
    build_slack_message,
    collect_failed_views,
    copy_country,
)

try:
    from airflow.operators.empty import EmptyOperator
except ModuleNotFoundError:
    from airflow.operators.dummy import DummyOperator as EmptyOperator  # type: ignore

# Countries to copy in parallel (one task per country).
COUNTRY_SUFFIXES = [
    "pol",
    "por",
    "svk",
    "esp",
    "hun",
    "ger",
    "ukr",
    "srb",
    "tur",
    "fra",
    "ned",
    "cze",
    "ita",
    "bel",
    "cro",
    "rom",
    "aus",
]

SLACK_DATA_PLATFORM_BOT_CONN_ID = "slack_data_platform_app"
# Channel id belongs in a Variable in real Composer; placeholder here.
SLACK_CHANNEL_ID = "{{ var.value.slack_dwh_ops_channel }}"
DAG_ID = "etl_hd_export_to_trusted_mcc"
COPY_TASK_PREFIX = "copy_hd_export_"
PREPARE_SLACK_MESSAGE_TASK_ID = "prepare_failed_views_for_slack"


def prepare_slack_message(copy_task_ids, **context):
    """
    Build Slack text from the run outcome (ALL_DONE so failures report).

    1. Any copy task in state "failed" → DAG Failure (+ any failed views).
    2. Else failed views from XCom → completed with failed views.
    3. Else → DAG Success.
    """
    failed_task_ids = [
        t.task_id
        for t in context["dag_run"].get_task_instances()
        if t.state == "failed" and t.task_id.startswith(COPY_TASK_PREFIX)
    ]
    failed_views = collect_failed_views(
        context["ti"].xcom_pull(task_ids=copy_task_ids)
    )
    return build_slack_message(DAG_ID, failed_task_ids, failed_views)


default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "start_date": datetime(2026, 3, 1),
    "email": ["dataops@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
}

schedule = "0 2 * * *"  # 02:00 daily

dag = DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule_interval=schedule,
    catchup=False,
    max_active_runs=1,
    tags=["bigquery", "hd-export", "trusted-mcc", "parallel-fanout"],
    doc_md=(
        "Parallel country materialization of export_* views into "
        "trusted_mcc, with aggregated Slack failure reporting. "
        "See utilities/80-hd-export-to-trusted-mcc/."
    ),
)

start = EmptyOperator(task_id="start", dag=dag)
end = EmptyOperator(task_id="end", dag=dag)

copy_tasks = [
    PythonOperator(
        task_id=f"{COPY_TASK_PREFIX}{suffix}",
        python_callable=copy_country,
        op_kwargs={"cnt": suffix},
        dag=dag,
    )
    for suffix in COUNTRY_SUFFIXES
]
copy_task_ids = [t.task_id for t in copy_tasks]

prepare_slack_message_task = PythonOperator(
    task_id=PREPARE_SLACK_MESSAGE_TASK_ID,
    python_callable=prepare_slack_message,
    op_kwargs={"copy_task_ids": copy_task_ids},
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
)

send_slack_notification_task = SlackAPIPostOperator(
    task_id="send_slack_notification",
    slack_conn_id=SLACK_DATA_PLATFORM_BOT_CONN_ID,
    channel=SLACK_CHANNEL_ID,
    text=f"{{{{ ti.xcom_pull(task_ids='{PREPARE_SLACK_MESSAGE_TASK_ID}') }}}}",
    dag=dag,
)

# start -> copy tasks (parallel) -> prepare -> slack -> end
chain(start, copy_tasks, prepare_slack_message_task, send_slack_notification_task, end)
