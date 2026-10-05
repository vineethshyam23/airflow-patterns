"""Value Creation Zone — PSM uplift CSV land (sibling to pattern 62).

Runs on the 3rd and 8th of each month after the PSM stored procedure
writes CSV objects under ``PSM/{env}/{iso}/{yyyymm}/``. Fans out
GCS → BigQuery WRITE_TRUNCATE loads across file × country × env, waits
on a barrier, then triggers the VCD PSM dbt Cloud job.

Distinct from pattern 62 (BQ CREATE OR REPLACE staging + CALL SP). This
DAG owns the *vendor-layout CSV land + schema/autodetect split* contract.

Source (read-only):
  dags/etl_value_creation_zone_3rd_and_8th_of_month_psm.py
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
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.utils.helpers import chain
from airflow.utils.trigger_rule import TriggerRule

log = logging.getLogger(__name__)

GCS_BUCKET = Variable.get("vcd_psm_gcs_bucket", default_var="dwh_discovery_bucket")
GCP_CONN = Variable.get("vcd_psm_gcp_conn", default_var="google_cloud_default")
STAGING_DATASET = "trusted_staging"
DBT_JOB_ID = int(Variable.get("vcd_psm_dbt_job_id", default_var="0"))
# YYYYMM partition folder the stored proc writes into. Production used
# ``datetime.now()`` at DAG parse time; prefer an Airflow Variable so
# late/re-run months can be set without editing code.
PSM_MONTH = Variable.get(
    "vcd_psm_month",
    default_var=datetime.utcnow().strftime("%Y%m"),
)

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "start_date": datetime(2021, 6, 20),
    "email": ["dataops@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "dbt_cloud_conn_id": "dbt_conn",
    "account_id": int(Variable.get("vcd_psm_dbt_account_id", default_var="0")),
}

# 10:15 UTC on the 3rd and 8th — after pattern 62's 05:15 SP window so
# CSV objects exist under PSM/{env}/{iso}/{month}/ before land starts.
SCHEDULE = "15 10 3,8 * *"

VCD_COUNTRIES = [
    "ES",
    "IT",
    "PL",
    "DE",
    "HU",
    "FR",
    "HR",
    "SK",
    "PT",
    "CZ",
    "NL",
    "RO",
    "global",
]

FILE_NAMES = [
    "Uplift_per_quarter",
    "Uplift_per_month",
    "Uplift_per_fiscal_year",
    "BundleQuarter",
    "BundleFiscal",
    "matched_psm_data",
]

# Bundle + matched files arrived with drifting schemas across markets;
# autodetect avoids a brittle shared schema_object. The three uplift
# grains keep an explicit schema JSON under PSM/schema/.
FILES_WITH_AUTODETECT = {
    "BundleQuarter",
    "BundleFiscal",
    "matched_psm_data",
}

dag = DAG(
    dag_id="etl_value_creation_zone_psm_csv_land",
    default_args=default_args,
    schedule_interval=SCHEDULE,
    catchup=False,
    max_active_runs=1,
    tags=["etl", "vcd", "psm", "gcs", "bi-monthly"],
    doc_md=__doc__,
)

start = EmptyOperator(task_id="start", dag=dag)
prod = EmptyOperator(task_id="prod", trigger_rule=TriggerRule.ALL_DONE, dag=dag)
dev = EmptyOperator(task_id="dev", trigger_rule=TriggerRule.ALL_DONE, dag=dag)
stage_1 = EmptyOperator(
    task_id="stage_1", trigger_rule=TriggerRule.ALL_DONE, dag=dag
)
stage_2 = EmptyOperator(
    task_id="stage_2", trigger_rule=TriggerRule.ALL_DONE, dag=dag
)
end = EmptyOperator(task_id="end", dag=dag)

file_name_tasks: dict[str, EmptyOperator] = {}
for file_name in FILE_NAMES:
    for env in ("dev", "prod"):
        key = f"{file_name}_{env}"
        file_name_tasks[key] = EmptyOperator(
            task_id=f"{file_name}_tasks_{env}",
            trigger_rule=TriggerRule.ALL_DONE,
            dag=dag,
        )

staging_tasks_by_file_env: dict[str, list[GCSToBigQueryOperator]] = {
    f"{file_name}_{env}": [] for file_name in FILE_NAMES for env in ("dev", "prod")
}


def _source_object(file_name: str, env: str, iso_code: str) -> str:
    """Build the GCS object path for one PSM CSV.

    ``matched_psm_data`` lands under ``tmp/``; uplift + bundle grains
    land under ``result/``. Month folder comes from ``vcd_psm_month``.
    """
    folder = "tmp" if file_name == "matched_psm_data" else "result"
    return f"PSM/{env}/{iso_code}/{PSM_MONTH}/{folder}/{file_name}.csv"


for file_name in FILE_NAMES:
    for iso_code in VCD_COUNTRIES:
        # matched_psm_data has no global rollup object.
        if file_name == "matched_psm_data" and iso_code == "global":
            continue
        for env in ("dev", "prod"):
            table_name = f"vcd_psm_{file_name.lower()}_{env}_{iso_code}"
            operator_kwargs: dict[str, Any] = {
                "task_id": f"load_staging_{file_name}_{env}_{iso_code}",
                "gcp_conn_id": GCP_CONN,
                "bucket": GCS_BUCKET,
                "source_format": "CSV",
                "source_objects": [_source_object(file_name, env, iso_code)],
                "destination_project_dataset_table": (
                    f"{STAGING_DATASET}.{table_name}"
                ),
                "create_disposition": "CREATE_IF_NEEDED",
                "write_disposition": "WRITE_TRUNCATE",
                "allow_quoted_newlines": True,
                "ignore_unknown_values": True,
                "dag": dag,
            }
            if file_name in FILES_WITH_AUTODETECT:
                operator_kwargs["autodetect"] = True
            else:
                operator_kwargs["schema_object"] = f"PSM/schema/{file_name}.json"

            load = GCSToBigQueryOperator(**operator_kwargs)
            staging_tasks_by_file_env[f"{file_name}_{env}"].append(load)

chain(start, [dev, prod])

for file_name in FILE_NAMES:
    for env, env_marker in (("dev", dev), ("prod", prod)):
        file_marker = file_name_tasks[f"{file_name}_{env}"]
        loads = staging_tasks_by_file_env[f"{file_name}_{env}"]
        if loads:
            chain(env_marker, file_marker, loads, stage_1)

vcd_psm = DbtCloudRunJobOperator(
    task_id="vcd_psm",
    job_id=DBT_JOB_ID,
    deferrable=True,
    execution_timeout=timedelta(minutes=60),
    check_interval=30,
    do_xcom_push=True,
    dag=dag,
    timeout=3600,
    reuse_existing_run=True,
    retry_from_failure=True,
)


def check_all_success(**context: Any) -> dict[str, str]:
    """Collect sibling task states for the ALL_DONE notification."""
    dr: DagRun = context["dag_run"]
    ti: TaskInstance = context["ti"]
    skip = {
        "slack_notification",
        "check_all_tasks",
        "start",
        "stage_1",
        "stage_2",
        "end",
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
    keeps the ALL_DONE aggregation contract and logs the summary.
    """
    task_status = ti.xcom_pull(task_ids="check_all_tasks", key="return_value") or {}
    failed = {k: v for k, v in task_status.items() if v == "failed"}
    load_date = datetime.today().strftime("%Y-%m-%d")
    if failed:
        log.error(
            "VCD PSM CSV land failed for %s task(s) on %s: %s",
            len(failed),
            load_date,
            sorted(failed),
        )
    else:
        log.info(
            "VCD PSM CSV land succeeded on %s into %s.vcd_psm_* (month=%s)",
            load_date,
            STAGING_DATASET,
            PSM_MONTH,
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

chain(stage_1, vcd_psm, stage_2, check_all_tasks, slack_notification, end)
