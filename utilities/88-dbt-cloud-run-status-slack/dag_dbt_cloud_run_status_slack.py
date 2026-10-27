"""
Airflow DAG: dbt Cloud run-id Variable scan → failed-job Slack digest.

Discovers Airflow Variables whose keys contain ``dbt_runids``, flattens
the JSON lists of run ids, asks dbt Cloud for each run's outcome, keeps
only failures, and posts one Slack webhook message per failed job.

Distinct from pattern 80 (Slack on parallel materialization failure
aggregate) and pattern 83 (BigQuery consumption cost digest): this is a
scheduled poll of *dbt Cloud* run status driven by Variable-stored run
ids that trigger DAGs push after they kick jobs.

Source (read-only):
  dags/dbt_jobs_status.py
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta
from json.decoder import JSONDecodeError

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.utils.db import create_session
from airflow.utils.helpers import chain

try:
    from airflow.providers.dbt.cloud.hooks.dbt import DbtCloudHook
except ImportError:  # older provider re-export
    from airflow.providers.dbt.cloud.operators.dbt import DbtCloudHook  # type: ignore

# --- sanitized identifiers -------------------------------------------------
DBT_CONN_ID = "dbt_cloud_conn"
DBT_ACCOUNT_ID = 3  # dbt Cloud account; override per tenant
SLACK_CONN_ID = "slack_webhook_conn"
SLACK_CHANNEL = "#dwh-dbt-ops"
SLACK_USERNAME = "Airflow-dbt Integration"
DAG_ID = "dbt_jobs_status_dag"
ENV_VAR_KEY = "env"

# Label shown in Slack — production mapped DEV/PROD to GCP project ids.
_env = os.environ.get(ENV_VAR_KEY, Variable.get(ENV_VAR_KEY, default_var="DEV"))
if _env == "DEV":
    ENVIRONMENT_LABEL = "dwh-stream-dev"
elif _env == "PROD":
    ENVIRONMENT_LABEL = "dwh-stream-prod"
else:
    ENVIRONMENT_LABEL = str(_env)


def get_list_of_dbt_runids():
    """Collect run ids from every Variable key containing ``dbt_runids``.

    Trigger DAGs write JSON lists into Variables after submitting a dbt
    Cloud job. This scanner does not own submission — it only reads.
    Malformed JSON is skipped so one bad Variable does not kill the poll.
    """
    dbt_runids = []
    with create_session() as session:
        airflow_vars = [
            var.key for var in session.query(Variable) if "dbt_runids" in var.key
        ]
        for airflow_var in airflow_vars:
            try:
                dbt_runids.append(
                    Variable.get(airflow_var, deserialize_json=True, default_var=[])
                )
            except JSONDecodeError:
                pass

    flattened = sum(dbt_runids, [])
    logging.info("Collected dbt run ids: %s", flattened)
    return flattened


def get_dbt_jobstatus(ti) -> list:
    """Resolve each run id via dbt Cloud; return only Failed rows.

    Production quirk: bare ``except`` on job-name lookup falls back to
    the numeric job_id — kept so a permissions gap on get_job does not
    drop the whole failure signal. Runs that error on get_job_run are
    logged and skipped (not treated as Failed).
    """
    hook = DbtCloudHook(DBT_CONN_ID)
    dbt_runids = ti.xcom_pull(task_ids="get_dbt_runids", key="return_value") or []
    dbt_job_status_list = []

    for runid in dbt_runids:
        dbt_jobs_status = {}
        try:
            job_run_details = hook.get_job_run(
                run_id=runid, account_id=DBT_ACCOUNT_ID
            ).json()
            data = job_run_details["data"]
            logging.info("job_run_details for %s: %s", runid, job_run_details)

            try:
                dbt_jobs_status["job_name"] = hook.get_job(
                    job_id=data["job_id"], account_id=DBT_ACCOUNT_ID
                ).json()["data"]["name"]
            except Exception:  # noqa: BLE001 — mirror prod fallback
                dbt_jobs_status["job_name"] = data["job_id"]

            dbt_jobs_status["job_id"] = data["job_id"]
            # finished_at can be null while a run is still in flight;
            # production strptime would raise — we only reach Slack for
            # Failed, which usually has a timestamp, but guard anyway.
            finished_raw = str(data.get("finished_at") or "").split(".")[0]
            if finished_raw:
                dbt_jobs_status["finished_at"] = datetime.strptime(
                    finished_raw, "%Y-%m-%d %H:%M:%S"
                )
            else:
                dbt_jobs_status["finished_at"] = None

            dbt_jobs_status["status"] = (
                "Succeeded"
                if data["is_success"] and not data["is_error"]
                else "Failed"
            )
            dbt_jobs_status["job_url"] = f"<{data['href']}|Link>"
            dbt_job_status_list.append(dbt_jobs_status)
            logging.info("dbt_jobs_status: %s", dbt_jobs_status)
        except Exception as exc:  # noqa: BLE001 — one bad run id must not abort
            logging.info("Exception for runid %s: %s", runid, exc)

    return [row for row in dbt_job_status_list if row["status"] == "Failed"]


def slack_notification(ti):
    """Post one Slack webhook message per Failed dbt Cloud run.

    Production note: SlackWebhookOperator is created and ``.execute()``d
    inside this callable (same shape as pattern 83). Airflow does not
    track each webhook as its own task — fine for a short failure list,
    awkward if you want per-message retries.
    """
    dbt_status = ti.xcom_pull(task_ids="get_dbt_job_status", key="return_value") or []
    if not dbt_status:
        return

    for row in dbt_status:
        notification = SlackWebhookOperator(
            task_id=f"dbt_fail_notification_{row['job_name']}",
            slack_webhook_conn_id=SLACK_CONN_ID,
            message=(
                f":dbt_cloud: DBT Job Failed.\n"
                f"*Environment*: {ENVIRONMENT_LABEL}\n"
                f"*job_name*: {row['job_name']}\n"
                f"*job_id*: {row['job_id']}\n"
                f"*finished_at*: {row['finished_at']}\n"
                f"*status*: {row['status']}\n"
                f"*job_url*: {row['job_url']}\n"
            ),
            channel=SLACK_CHANNEL,
            username=SLACK_USERNAME,
            dag=dag,
        )
        notification.execute({})


default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email": ["dataops@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}

dag = DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule_interval="1 5 * * *",
    catchup=False,
    render_template_as_native_obj=True,
    tags=["monitoring", "dbt-cloud", "slack"],
)

get_dbt_runids = PythonOperator(
    task_id="get_dbt_runids",
    python_callable=get_list_of_dbt_runids,
    do_xcom_push=True,
    dag=dag,
)

get_dbt_job_status_df = PythonOperator(
    task_id="get_dbt_job_status",
    python_callable=get_dbt_jobstatus,
    do_xcom_push=True,
    dag=dag,
)

notification = PythonOperator(
    task_id="slack_notification",
    python_callable=slack_notification,
    dag=dag,
)

chain(get_dbt_runids, get_dbt_job_status_df, notification)
