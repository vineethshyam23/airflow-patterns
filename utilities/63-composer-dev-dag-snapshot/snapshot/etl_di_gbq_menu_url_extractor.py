"""
DAG for source_app BigQuery menu URL extraction

This DAG reads page URLs from a configurable BigQuery source table, fetches HTML at
runtime, extracts likely menu links (heuristic anchor matching), normalizes absolute
URLs, and appends new rows to a BigQuery destination table. Processing logic lives in
``modules.gbq_menu_url_extractor``.

Key Features:
- No LLM or AI APIs: rule-based HTML parsing and link heuristics only (see ``gbq_menu_url_extractor``).
- BigQuery source → HTTP fetch → parse → BigQuery sink (no full HTML persisted).
- Idempotent appends: skips ``(source_url, menu_url)`` pairs already in the destination.
- Uses the same GCP Airflow connection pattern as other source_app DAGs (no task ``env``;
  the callable reads Airflow Variables inside ``gbq_menu_url_extractor``, like ``extract_link_by_id``).

Configuration (Airflow Variables — see module docstring for defaults):
- ``GCP_PROJECT``, ``GCP_CONN_ID``, ``MENU_URL_SOURCE_TABLE``, ``MENU_URL_DEST_TABLE``,
  optional ``MENU_URL_COLUMN``, ``MENU_URL_BATCH_LIMIT``, timeouts and delay.
- Retries: 3 attempts with 5-minute delay between retries.
- Slack notifications on success and failure (same channel pattern as extract_link).

Task Flow:
1. start (EmptyOperator)
2. plan_menu_url_batches (PythonOperator) — counts distinct source URLs and logs the partition plan
3. extract_batches_group (TaskGroup) — contains num_batches individual PythonOperator tasks
   named extract_menu_url_batch_1 .. extract_menu_url_batch_N, all running in parallel;
   num_batches is set inside the task group (mirrors MENU_URL_BATCH_LIMIT / distinct URL count)
4. batch_join (EmptyOperator) — gate after all batch tasks succeed
5. slacknotification (optional) — posts outcome to Slack
6. end (EmptyOperator)

Dependencies:
- apache-airflow-providers-google
- google-cloud-bigquery, pandas, pandas-gbq, requests, selectolax
- modules.gbq_menu_url_extractor

Author: Rashmi Kedari
Created: 2025-03-23
Last Modified: 2025-03-23
"""
import logging
import os
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.decorators import task_group
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.utils.helpers import chain
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

from modules.gbq_menu_url_extractor import (
    DEFAULT_MENU_URL_DEST_TABLE,
    DEFAULT_MENU_URL_SOURCE_TABLE,
    get_bigquery_client,
    load_config,
    run_extraction_partition,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

environment = "env"
env = os.environ.get(environment, Variable.get(environment, default_var="DEV"))

if env == "DEV":
    project_id = "source_project"
    bucket_name = "source_app-dwh-rawzone"
    gcp_conn_id = "google_cloud_default"
    gcp_cloudsql_conn_id = "google_alloydb_dev"
    instance_id = "di-migration-sbx"
    database_id = "postgres"
else:
    project_id = "source_project"
    bucket_name = "source_app-dwh-rawzone"
    gcp_conn_id = "google_cloud_default"
    gcp_cloudsql_conn_id = "google_alloydb_dev"
    instance_id = "di-migration-sbx"
    database_id = "postgres"

default_args = {
    "owner": "Rashmi",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=24),
}


def slack_notification(ti, **kwargs):
    try:
        dag_run = kwargs["dag_run"]
        task_instance = dag_run.get_task_instance("batch_join")
        state = task_instance.state if task_instance else "missing"

        now_str_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        src_tbl = Variable.get(
            "MENU_URL_SOURCE_TABLE",
            default_var=DEFAULT_MENU_URL_SOURCE_TABLE,
        )
        dst_tbl = Variable.get(
            "MENU_URL_DEST_TABLE",
            default_var=DEFAULT_MENU_URL_DEST_TABLE,
        )

        if state == "success":
            emoji = ":white_check_mark:"
            final_message = f"""
            {emoji} *etl_di_gbq_menu_url_extractor* completed successfully.\n
            *Project*: {project_id}\n
            *Source table*: {src_tbl}\n
            *Destination table*: {dst_tbl}\n
            *Finished At*: {now_str_utc}\n
            *Status*: Success\n
            """
        else:
            emoji = ":x:"
            final_message = f"""
            {emoji} *etl_di_gbq_menu_url_extractor* failed.\n
            *Project*: {project_id}\n
            *Source table*: {src_tbl}\n
            *Destination table*: {dst_tbl}\n
            *Finished At*: {now_str_utc}\n
            *Status*: Failed\n
            """

        notification = SlackWebhookOperator(
            task_id="slack_notification_task",
            slack_webhook_conn_id="slack_conn_di",
            message=final_message,
            channel="#dish-source_app",
            username="source_app Monitoring",
            dag=dag,
        )
        notification.execute(dict())
        logger.info("Slack notification sent successfully")

    except Exception as e:
        logger.error("Error sending Slack notification: %s", str(e))
        raise


dag = DAG(
    dag_id="etl_di_gbq_menu_url_extractor",
    default_args=default_args,
    description="Extract menu URLs from pages listed in BigQuery and write results to BigQuery",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    doc_md=__doc__,
    tags=["etl", "bigquery", "dish", "source_app", "Parsing_Processes"],
    template_searchpath="/home/airflow/gcs/dags/modules",
    max_active_tasks=5,
)

# ---------------------------------------------------------------------------
# MAX_NUM_BATCHES: ceiling for the for-loop that creates tasks at DAG parse time.
# With static task groups the loop count must be known before any run, so this
# is a capacity ceiling (not the exact batch count).
# Actual num_batches is computed at runtime by plan_menu_url_batches and read
# via XCom by each extract task; tasks beyond the actual count are skipped.
# Increase only if plan_menu_url_batches logs a num_batches > this value.
MAX_NUM_BATCHES = 50  # 50 parallel tasks; each processes ~2 000 URLs at 100K source_limit

start = EmptyOperator(task_id="start", trigger_rule=TriggerRule.ALL_DONE, dag=dag)
end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE, dag=dag)
batch_join = EmptyOperator(task_id="batch_join", trigger_rule=TriggerRule.ALL_SUCCESS, dag=dag)


# ---------------------------------------------------------------------------
# Task 1: plan_menu_url_batches
# Counts distinct source URLs and computes num_batches = ceil(count/batch_limit).
# Pushes {"distinct_count": N, "num_batches": K} to XCom (key=return_value).
# Each extract task below reads num_batches from this XCom output at runtime.
# ---------------------------------------------------------------------------
def plan_menu_url_batches_callable(**kwargs):
    from math import ceil
    from google.cloud import bigquery as bq

    cfg = load_config()
    client, _ = get_bigquery_client(cfg.project_id, cfg.gcp_conn_id)
    col = cfg.url_column.replace("`", "")

    count_q = f"""
    SELECT COUNT(*) AS c
    FROM (
      SELECT DISTINCT TRIM(CAST(`{col}` AS STRING)) AS url
      FROM `{cfg.source_table}`
      WHERE `{col}` IS NOT NULL
        AND TRIM(CAST(`{col}` AS STRING)) != '' limit 100
    )
    """
    count_rows = list(
        client.query(
            count_q,
            job_config=bq.QueryJobConfig(labels={"gbq_menu_extractor": "source_count_only"}),
        ).result()
    )
    distinct_count = int(count_rows[0]["c"]) if count_rows else 0
    num_batches = MAX_NUM_BATCHES  # always use full ceiling so all tasks are active
    batch_limit = ceil(distinct_count / num_batches) if distinct_count > 0 else 1  # dynamic, informational

    logger.info(
        "Menu URL plan: %s distinct source URLs → %s partitions (~%s URLs each, MAX_NUM_BATCHES=%s)",
        distinct_count,
        num_batches,
        batch_limit,
        MAX_NUM_BATCHES,
    )
    return {"distinct_count": distinct_count, "num_batches": num_batches, "batch_limit": batch_limit}


plan_task = PythonOperator(
    task_id="plan_menu_url_batches",
    python_callable=plan_menu_url_batches_callable,
    provide_context=True,
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
)


# ---------------------------------------------------------------------------
# Task 2 wrapper: extract_batch_callable
# Reads actual num_batches from plan_menu_url_batches XCom at runtime.
# Skips gracefully when batch_index exceeds the actual num_batches so tasks
# created for the ceiling (MAX_NUM_BATCHES) beyond real data are harmless no-ops.
# ---------------------------------------------------------------------------
def extract_batch_callable(batch_index: int, **kwargs):
    ti = kwargs["ti"]
    plan_output = ti.xcom_pull(task_ids="plan_menu_url_batches", key="return_value")
    if not plan_output:
        raise ValueError("XCom from plan_menu_url_batches is empty — did the plan task succeed?")

    actual_num_batches = int(plan_output["num_batches"])

    if batch_index > actual_num_batches:
        logger.info(
            "Batch %s skipped — actual num_batches=%s (ceiling MAX_NUM_BATCHES=%s)",
            batch_index,
            actual_num_batches,
            MAX_NUM_BATCHES,
        )
        return 0

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    n = run_extraction_partition(batch_index, actual_num_batches)
    logger.info("Partition %s/%s finished, new rows: %s", batch_index, actual_num_batches, n)
    return n


# ---------------------------------------------------------------------------
# Task group: extract_batches_group
# Creates MAX_NUM_BATCHES PythonOperator tasks at parse time; each reads the real
# MAX_NUM_BATCHES from XCom and skips itself if batch_index exceeds it.
# All tasks run in parallel — no chain() inside the group.
# ---------------------------------------------------------------------------
extract_group_lst = []

try:
    @task_group(group_id="extract_batches_group", dag=dag)
    def extract_batches_group():
        for batch_num in range(1, MAX_NUM_BATCHES + 1):
            PythonOperator(
                task_id=f"extract_menu_url_batch_{batch_num}",
                python_callable=extract_batch_callable,
                op_kwargs={"batch_index": batch_num},
                provide_context=True,
                trigger_rule=TriggerRule.ALL_SUCCESS,
                execution_timeout=timedelta(hours=6),
                dag=dag,
            )

    extract_group_lst.append(extract_batches_group())
except Exception as e:
    logging.error("Error building extract_batches_group: %s", str(e))
    raise


# slacknotification = PythonOperator(
#     task_id="slacknotification",
#     provide_context=True,
#     python_callable=slack_notification,
#     trigger_rule=TriggerRule.ALL_DONE,
#     dag=dag,
# )

chain(start, plan_task, *extract_group_lst, batch_join, #slacknotification,
 end)
