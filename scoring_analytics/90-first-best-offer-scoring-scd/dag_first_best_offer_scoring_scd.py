"""
Airflow DAG: First Best Offer weekly VM scoring + SCD Type 2 historization.

Weekly Monday graph:

1. Start a dedicated scoring GCE VM.
2. SSH two predict jobs in parallel (wholesale account grain + independent
   establishment grain). Each writes a current-score table in BigQuery refined.
3. For each arm: stage with keyhash/rowhash → expire changed hist rows →
   append new versions.
4. Stop the VM (`all_done` so cost does not linger after a predict failure).

The predict scripts live on the VM (`predict_fbo.py`); this DAG owns
orchestration and warehouse historization only. Downstream partner export
of FBO/NBO deltas is pattern 04 — a different contract (monthly Avro
event bus), not a duplicate of this weekly SCD spine.

Distinct from:
  - Pattern 04 (multi-country FBO/NBO hash-delta Avro export)
  - Pattern 47 (absolute multi-channel activity scores)
  - Pattern 67 (daily refined-zone SCD spine — different entities)
  - Pattern 55 (Menu Engineering VM Postgres land — different product VM)

Source (read-only):
  dags/horeca_digital/archived/etl_first_best_offer.py
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.operators.compute import (
    ComputeEngineStartInstanceOperator,
    ComputeEngineStopInstanceOperator,
)
from airflow.utils.trigger_rule import TriggerRule

from fbo_scd_queries import ARMS, expire_sql, insert_sql, stage_sql

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "start_date": datetime(2022, 1, 31),
    "email": ["dataops@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}

DWH_PROJECT = Variable.get("dwh_gcp_project", default_var="dwh_project")
GCP_CONN = Variable.get("dwh_gcp_conn", default_var="bigquery_default")
COMPOSER_BUCKET = Variable.get("composer_bucket", default_var="composer-data")
INSTANCE_ZONE = Variable.get("fbo_gce_zone", default_var="europe-west1-b")
INSTANCE_PROJECT = Variable.get("fbo_gce_project", default_var=DWH_PROJECT)
VM_NAME = Variable.get("fbo_gce_vm_name", default_var="first-best-offer")
SSH_KEY_GCS = Variable.get(
    "fbo_ssh_key_gcs_uri",
    default_var=f"gs://{COMPOSER_BUCKET}/data/scoring/keys/id_fbo_rsa",
)
SSH_USER = Variable.get("fbo_ssh_user", default_var="airflow")
VM_HOME = Variable.get("fbo_vm_home", default_var="/home/airflow")
VM_VENV = Variable.get(
    "fbo_vm_venv",
    default_var=f"{VM_HOME}/environments/py38_env/bin/activate",
)
VM_APP_DIR = Variable.get("fbo_vm_app_dir", default_var=f"{VM_HOME}/fbo")
REFINED = Variable.get("fbo_refined_dataset", default_var="refined")
STAGING = Variable.get("fbo_staging_dataset", default_var="trusted_staging")

# Monday 02:00 UTC — after weekend batch windows, before Monday partner export.
SCHEDULE = "0 2 * * 1"

# Predict CLI flags. Model variants write different refined score tables;
# historization arms below map 1:1 to those outputs.
PREDICT_JOBS = {
    "wholesale": {
        "task_id": "run_fbo_wholesale_prediction",
        "cli": (
            f"python {VM_APP_DIR}/src/predict_fbo.py "
            f'--home_path="{VM_HOME}" '
            f'--destination_dataset="{REFINED}" '
            '--stage="prod" '
            '--model="wholesale"'
        ),
    },
    "independent": {
        "task_id": "run_fbo_independent_prediction",
        "cli": (
            f"python {VM_APP_DIR}/src/predict_fbo.py "
            f'--home_path="{VM_HOME}" '
            f'--destination_dataset="{REFINED}" '
            '--stage="prod" '
            '--model="independent"'
        ),
    },
}


def _ssh_bash(remote_command: str) -> str:
    """Copy SSH key from Composer bucket, then run a remote command on the VM.

    Production used StrictHostKeyChecking=no and a private key staged under
    /tmp. Keep that posture explicit — prefer an SSHOperator + Connection
    when you next rewrite this DAG.
    """
    key_basename = SSH_KEY_GCS.rstrip("/").split("/")[-1]
    return (
        "cd /tmp; "
        f"gsutil cp {SSH_KEY_GCS}* .; "
        f"chmod 600 ./{key_basename}; "
        f'ssh -o "StrictHostKeyChecking=no" '
        f"-i /tmp/{key_basename} {SSH_USER}@{VM_NAME} "
        f'"export PYTHONPATH={VM_APP_DIR}; '
        f"source {VM_VENV}; "
        f"cd {VM_APP_DIR}; "
        f'{remote_command}"'
    )


def _bq_query_job(
    task_id: str,
    sql: str,
    *,
    destination_table: dict | None = None,
    write_disposition: str | None = None,
) -> BigQueryInsertJobOperator:
    query_cfg: dict = {
        "query": sql,
        "useLegacySql": False,
        "allowLargeResults": True,
    }
    if destination_table is not None:
        query_cfg["destinationTable"] = destination_table
        query_cfg["writeDisposition"] = write_disposition or "WRITE_TRUNCATE"
        query_cfg["createDisposition"] = "CREATE_IF_NEEDED"
    elif write_disposition is not None:
        # UPDATE statements still carried writeDisposition in production;
        # kept for behavioural parity even though BQ ignores it on DML.
        query_cfg["writeDisposition"] = write_disposition
        query_cfg["createDisposition"] = "CREATE_IF_NEEDED"

    return BigQueryInsertJobOperator(
        task_id=task_id,
        configuration={"query": query_cfg},
        gcp_conn_id=GCP_CONN,
        # Production used all_done so a failed predict still attempted
        # historization against stale scores — document, do not romanticize.
        trigger_rule=TriggerRule.ALL_DONE,
    )


with DAG(
    dag_id="etl_first_best_offer",
    default_args=default_args,
    schedule_interval=SCHEDULE,
    catchup=False,
    max_active_runs=1,
    tags=["scoring", "fbo", "scd", "gce"],
) as dag:

    start_vm = ComputeEngineStartInstanceOperator(
        task_id="start_vm",
        project_id=INSTANCE_PROJECT,
        zone=INSTANCE_ZONE,
        resource_id=VM_NAME,
    )

    stop_vm = ComputeEngineStopInstanceOperator(
        task_id="stop_vm",
        project_id=INSTANCE_PROJECT,
        zone=INSTANCE_ZONE,
        resource_id=VM_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    predict_tasks = {}
    for arm_name, job in PREDICT_JOBS.items():
        predict_tasks[arm_name] = BashOperator(
            task_id=job["task_id"],
            bash_command=_ssh_bash(job["cli"]),
        )

    for arm in ARMS:
        predict = predict_tasks[arm.name]

        stage = _bq_query_job(
            f"stage_fbo_{arm.name}_score",
            stage_sql(DWH_PROJECT, REFINED, STAGING, arm),
            destination_table={
                "projectId": DWH_PROJECT,
                "datasetId": STAGING,
                "tableId": arm.staging_table,
            },
            write_disposition="WRITE_TRUNCATE",
        )
        expire = _bq_query_job(
            f"expire_fbo_{arm.name}_hist",
            expire_sql(DWH_PROJECT, REFINED, STAGING, arm),
            write_disposition="WRITE_APPEND",
        )
        insert = _bq_query_job(
            f"insert_fbo_{arm.name}_hist",
            insert_sql(DWH_PROJECT, REFINED, STAGING, arm),
            destination_table={
                "projectId": DWH_PROJECT,
                "datasetId": REFINED,
                "tableId": arm.hist_table,
            },
            write_disposition="WRITE_APPEND",
        )

        start_vm >> predict
        predict >> stop_vm
        predict >> stage >> expire >> insert
