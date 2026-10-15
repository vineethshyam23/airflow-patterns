"""Composer DAG: dbt build → Blake3 dine_id → DQ gate → multi-env publish.

Schedule: daily 07:30 UTC. Deferrable dbt Cloud job, then proportion
checks vs yesterday's consumer-app prod table. On failure: Slack only.
On pass: backup prior trusted push, WRITE_TRUNCATE into each dining-guide
env project. Optional ratings-enriched table lands to dev only.

Sanitized from production `etl_refined_dish_dine`. Destinations come from
Airflow Variables so project ids are not baked into the graph.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import timedelta
from pathlib import Path

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

from dining_guide_validation import (
    any_flag_true,
    branch_on_validation,
    comparison_sql,
    publish_select_sql,
    rebuild_establishment_id_hash,
)

logger = logging.getLogger(__name__)

dwh_project = os.getenv("PROJECT") or Variable.get("dwh_gcp_project", default_var="dwh_project")
is_dev = dwh_project.endswith("-dev") or dwh_project.endswith("_dev")
env_suffix = "_dev" if is_dev else ""
gcp_conn_id = Variable.get(
    "dining_guide_bq_conn",
    default_var="bigquery_default_dev" if is_dev else "bigquery_default",
)

# JSON list of [stage, gcp_project] pairs. Prod publishes all envs;
# Composer-dev can publish only the dining-guide-dev project.
_default_targets = (
    '[["dev", "dining-guide-dev"]]'
    if is_dev
    else (
        '[["dev", "dining-guide-dev"],'
        '["acc", "dining-guide-acc"],'
        '["stg", "dining-guide-stg"],'
        '["prod", "dining-guide-prod"]]'
    )
)
stage_and_project = json.loads(
    Variable.get("dining_guide_publish_targets", default_var=_default_targets)
)
prod_app_project = Variable.get("dining_guide_prod_project", default_var="dining-guide-prod")
dbt_job_id = int(Variable.get("dining_guide_dbt_job_id", default_var="185013"))
slack_conn = Variable.get("dining_guide_slack_conn", default_var="slack_dining_guide")
slack_channel = Variable.get("dining_guide_slack_channel", default_var="#dining-guide-data")

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email": ["dataops@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "dbt_cloud_conn_id": "dbt_conn",
    "account_id": int(Variable.get("dbt_cloud_account_id", default_var="3")),
}

dag = DAG(
    dag_id=f"etl_dining_guide_dq_gated_publish{env_suffix}",
    default_args=default_args,
    schedule_interval="30 7 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["dining_guide", "dbt", "data_quality", "bigquery"],
)


def _hash_uid(**_context):
    return rebuild_establishment_id_hash(dwh_project)


def _get_runids(ti):
    try:
        runids = [ti.xcom_pull(task_ids=["dbt_dining_guide"], key="return_value")[0]]
    except (IndexError, TypeError):
        url = ti.xcom_pull(task_ids=["dbt_dining_guide"], key="job_run_url")[0]
        runids = [int(list(filter(None, url.split("/")))[-1])]
    Variable.set(key="etl_dining_guide_dbt_runids", value=runids)
    return runids


def _data_validation(**_context):
    return any_flag_true(dwh_project)


def _approve_publish(**_context):
    logger.info("DQ gate passed — backing up trusted then publishing to app projects")
    return "load_data_approved"


start_task = EmptyOperator(task_id="start_task", dag=dag)

dbt_dining_guide = DbtCloudRunJobOperator(
    task_id="dbt_dining_guide",
    job_id=dbt_job_id,
    deferrable=True,
    execution_timeout=timedelta(minutes=60),
    check_interval=30,
    do_xcom_push=True,
    timeout=3600,
    dag=dag,
)

get_runids_task = PythonOperator(
    task_id="get_runids_task",
    python_callable=_get_runids,
    dag=dag,
)

hash_uid = PythonOperator(
    task_id="hash_uid",
    python_callable=_hash_uid,
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
)

comparison_results_dining_guide = BigQueryInsertJobOperator(
    task_id="comparison_results_dining_guide",
    configuration={
        "query": {
            "query": comparison_sql(dwh_project, prod_app_project),
            "useLegacySql": False,
            "destinationTable": {
                "projectId": dwh_project,
                "datasetId": "monitoring",
                "tableId": "comparison_results_dining_guide",
            },
            "writeDisposition": "WRITE_TRUNCATE",
            "createDisposition": "CREATE_IF_NEEDED",
        }
    },
    gcp_conn_id=gcp_conn_id,
    dag=dag,
)

check_query_task = PythonOperator(
    task_id="check_query_task",
    python_callable=_data_validation,
    do_xcom_push=True,
    dag=dag,
)

branch_task = BranchPythonOperator(
    task_id="branch_task",
    python_callable=branch_on_validation,
    dag=dag,
)

slack_dq_fail_task = SlackWebhookOperator(
    task_id="slack_dq_fail_task",
    slack_webhook_conn_id=slack_conn,
    message=(
        "*Dining Guide DQ gate blocked publish*\n"
        "DAG: `{{ dag.dag_id }}` · execution: `{{ execution_date }}`\n"
        "15%+ degradation vs yesterday's consumer-app prod snapshot. "
        "No WRITE_TRUNCATE to dining-guide projects.\n"
        f"Inspect `{dwh_project}.monitoring.comparison_results_dining_guide`."
    ),
    channel=slack_channel,
    username="Dining Guide DQ",
    dag=dag,
)

approve_publish = PythonOperator(
    task_id="approve_publish",
    python_callable=_approve_publish,
    dag=dag,
)

backup_previous_push = BigQueryInsertJobOperator(
    task_id="backup_trusted_previous_push",
    configuration={
        "query": {
            "query": f"""
                CREATE OR REPLACE TABLE `{dwh_project}.backup_trusted.dining_guide_data_base_previous_push` AS
                SELECT * FROM `{dwh_project}.backup_trusted.dining_guide_data_base`
            """,
            "useLegacySql": False,
        }
    },
    gcp_conn_id=gcp_conn_id,
    dag=dag,
)

_ratings_sql_path = Path(__file__).resolve().parent / "sql" / "dining_guide_with_ratings.sql"


def _ratings_query() -> str:
    raw = _ratings_sql_path.read_text(encoding="utf-8").strip()
    return raw.replace("{dwh_project}", dwh_project)


publish_tasks = []
for stage, project in stage_and_project:
    task = BigQueryInsertJobOperator(
        task_id=f"publish_dining_guide_{stage}",
        configuration={
            "query": {
                "query": publish_select_sql(dwh_project),
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": project,
                    "datasetId": "app_data",
                    "tableId": "dining_guide_data_base",
                },
                "writeDisposition": "WRITE_TRUNCATE",
                "createDisposition": "CREATE_IF_NEEDED",
            }
        },
        gcp_conn_id=gcp_conn_id,
        dag=dag,
    )
    publish_tasks.append(task)
    backup_previous_push >> task

# Ratings-enriched publish is intentionally dev-only in production.
ratings_targets = json.loads(
    Variable.get(
        "dining_guide_ratings_targets",
        default_var='[["dev", "dining-guide-dev"]]',
    )
)
for stage, project in ratings_targets:
    ratings_task = BigQueryInsertJobOperator(
        task_id=f"publish_dining_guide_with_ratings_{stage}",
        configuration={
            "query": {
                "query": _ratings_query(),
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": project,
                    "datasetId": "app_data",
                    "tableId": "dining_guide_data_base_with_ratings",
                },
                "writeDisposition": "WRITE_TRUNCATE",
                "createDisposition": "CREATE_IF_NEEDED",
            }
        },
        gcp_conn_id=gcp_conn_id,
        dag=dag,
    )
    backup_previous_push >> ratings_task

approve_publish >> backup_previous_push

start_task >> dbt_dining_guide >> get_runids_task
(
    start_task
    >> dbt_dining_guide
    >> hash_uid
    >> comparison_results_dining_guide
    >> check_query_task
    >> branch_task
    >> [slack_dq_fail_task, approve_publish]
)
