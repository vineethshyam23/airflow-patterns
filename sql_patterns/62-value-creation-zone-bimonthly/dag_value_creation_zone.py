"""Value Creation Zone — bi-monthly VCD source refresh (v2).

Runs on the 3rd and 8th of each month. Materializes per-country wholesale
customer / article / assortment / transaction tables plus global MAG and
mapping references into ``trusted_staging.vcd_*``, builds a short-TTL
union view, then calls the PSM uplift stored procedure for prod and dev
per eligible market.

Distinct from pattern 46 (Food Graph refined analytics zone inside DWH)
and pattern 48 (Offer Tool weekday-aware product-project publish). This
DAG owns the *dashboard staging + stored-proc barrier* contract.

Source (read-only):
  dags/etl_value_creation_zone_3rd_and_8th_of_month_v2.py
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.models import Variable
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.helpers import chain
from airflow.utils.trigger_rule import TriggerRule

import vcd_queries as q

log = logging.getLogger(__name__)

DWH_PROJECT = Variable.get("vcd_dwh_project", default_var="dwh_project")
BQ_CONN = Variable.get("vcd_bq_conn", default_var="bigquery_default")
STAGING = "trusted_staging"

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "start_date": datetime(2021, 6, 20),
    "email": ["dataops@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

# 05:15 UTC on the 3rd and 8th — mid-month + early-month refresh for the
# Value Creation Dashboard. Catchup off; overlapping runs blocked.
SCHEDULE = "15 5 3,8 * *"

dag = DAG(
    dag_id="etl_value_creation_zone_bimonthly",
    default_args=default_args,
    schedule_interval=SCHEDULE,
    catchup=False,
    max_active_runs=1,
    tags=["etl", "vcd", "value-creation", "psm", "bi-monthly"],
    doc_md=__doc__,
)


def _query_job(task_id: str, sql: str) -> BigQueryInsertJobOperator:
    return BigQueryInsertJobOperator(
        task_id=task_id,
        gcp_conn_id=BQ_CONN,
        configuration={
            "query": {
                "query": sql,
                "useLegacySql": False,
                "allowLargeResults": True,
            }
        },
        dag=dag,
    )


def _truncate_job(task_id: str, sql: str, table: str) -> BigQueryInsertJobOperator:
    return BigQueryInsertJobOperator(
        task_id=task_id,
        gcp_conn_id=BQ_CONN,
        configuration={
            "query": {
                "query": sql,
                "useLegacySql": False,
                "writeDisposition": "WRITE_TRUNCATE",
                "createDisposition": "CREATE_IF_NEEDED",
                "allowLargeResults": True,
                "destinationTable": {
                    "projectId": DWH_PROJECT,
                    "datasetId": STAGING,
                    "tableId": table,
                },
            }
        },
        dag=dag,
    )


start = EmptyOperator(task_id="start", dag=dag)
start_storeproc = EmptyOperator(task_id="start_storeproc", dag=dag)
prod = EmptyOperator(task_id="prod", dag=dag)
dev = EmptyOperator(task_id="dev", dag=dag)
stage = EmptyOperator(task_id="stage", dag=dag)
end = EmptyOperator(task_id="end", dag=dag)

# --- Phase 1: parallel CREATE OR REPLACE into staging -------------------
for iso, country in q.COUNTRY_MAP:
    jobs = [
        (
            f"vcd_source.wholesale_customer_{iso}",
            q.wholesale_customer_sql(DWH_PROJECT, iso, country),
        ),
        (
            f"vcd_source.analytical_customer_{iso}",
            q.analytical_customer_sql(DWH_PROJECT, iso),
        ),
        (
            f"vcd_source.wholesale_article_{iso}",
            q.wholesale_article_sql(DWH_PROJECT, iso, country),
        ),
        (
            f"vcd_source.wholesale_assortment_{iso}",
            q.wholesale_assortment_sql(DWH_PROJECT, iso, country),
        ),
        (
            f"vcd_source.wholesale_transactions_{iso}",
            q.wholesale_transactions_sql(DWH_PROJECT, iso, country),
        ),
    ]
    for task_id, sql in jobs:
        chain(start, _query_job(task_id, sql), start_storeproc)

for iso_u in q.ESTABLISHMENT_ISOS:
    chain(
        start,
        _query_job(
            f"vcd_source.all_establishments_{iso_u}",
            q.establishments_sql(DWH_PROJECT, iso_u),
        ),
        start_storeproc,
    )

for task_id, sql in q.global_reference_jobs(DWH_PROJECT):
    chain(start, _query_job(f"vcd_source.{task_id}", sql), start_storeproc)

# --- Phase 2: barrier → working tables → PSM stored procs ---------------
reactivation = _truncate_job(
    "vcd_source.reactivation_ids",
    q.reactivation_select(DWH_PROJECT),
    "vcd_reactivation_ids",
)
txn_union = _query_job(
    "discovery.v_wholesale_transaction_source",
    q.transaction_union_sql(DWH_PROJECT),
)
customer_est = _truncate_job(
    "vcd_source.customer_establishment",
    q.customer_establishment_select(DWH_PROJECT),
    "vcd_db_value_creation_customer_establishment",
)

chain(start_storeproc, reactivation, txn_union, customer_est, stage)

for iso, env in q.psm_iso_envs():
    sp = _query_job(
        f"call_psm_uplift_{iso}_{env}",
        q.psm_uplift_call(DWH_PROJECT, iso, env),
    )
    marker = prod if env == "prod" else dev
    chain(txn_union, marker, sp, stage)


def check_all_success(**context: Any) -> dict[str, str]:
    """Collect sibling task states for the run-status notification."""
    dr: DagRun = context["dag_run"]
    ti: TaskInstance = context["ti"]
    skip = {
        "slack_notification",
        "check_all_tasks",
        "start",
        "stage",
        "end",
        "start_storeproc",
        "prod",
        "dev",
    }
    return {
        task.task_id: task.state
        for task in dr.get_task_instances()
        if task.task_id != ti.task_id and task.task_id not in skip
    }


def notify_run_status(ti: TaskInstance, **_: Any) -> None:
    """Log / webhook stub for VCD operative channel.

    Production used SlackWebhookOperator with a dedicated conn. Portfolio
    keeps the ALL_DONE aggregation contract and logs the summary so the
    pattern is reviewable without a live webhook.
    """
    task_status = ti.xcom_pull(task_ids="check_all_tasks", key="return_value") or {}
    failed = {k: v for k, v in task_status.items() if v == "failed"}
    load_date = datetime.today().strftime("%Y-%m-%d")
    if failed:
        log.error(
            "VCD source refresh failed for %s task(s) on %s: %s",
            len(failed),
            load_date,
            sorted(failed),
        )
    else:
        log.info(
            "VCD source refresh succeeded on %s into %s.%s.vcd_*",
            load_date,
            DWH_PROJECT,
            STAGING,
        )


check_all_tasks = PythonOperator(
    task_id="check_all_tasks",
    python_callable=check_all_success,
    provide_context=True,
    do_xcom_push=True,
    dag=dag,
)

slack_notification = PythonOperator(
    task_id="slack_notification",
    python_callable=notify_run_status,
    provide_context=True,
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
)

chain(stage, check_all_tasks, slack_notification, end)
