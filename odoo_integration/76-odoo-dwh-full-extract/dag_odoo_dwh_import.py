"""
Airflow DAG: Odoo Postgres multi-table extract → GCS → BQ staging → dbt.

Primary inbound Odoo spine for the warehouse. Production runs ~105 tables
and ~25 dbt Cloud jobs (~350 tasks). This folder ships a representative
cross-domain subset that keeps the orchestration contracts:

- one TaskGroup at parse time (not a loop of TaskGroups — that blew
  DagBag import timeout)
- max_active_tasks throttle so the Postgres replica is not stampeded
- local disk extract + GCSHook.upload (avoid Composer FUSE races)
- WRITE_TRUNCATE staging per table, dated GCS paths via {{ ds }}
- sync barrier then parallel deferrable dbt jobs + final datacore refresh
- dagrun_timeout so a stuck dbt HTTP trigger cannot block max_active_runs=1

Distinct from pattern 15 (helpdesk-only pull) and patterns 06–14 / 60 / 72
(outbound or reverse / cleanup). Those consume trusted tables this DAG
feeds.

Source (read-only):
  dags/etl_odoo_data_import_DWH.py
  dags/horeca_digital/odoo_data_export.py
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.utils.helpers import chain
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

from odoo_data_export import OdooDataPull

try:
    from airflow.operators.empty import EmptyOperator
except ModuleNotFoundError:
    from airflow.operators.dummy import DummyOperator as EmptyOperator  # type: ignore


default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
    "email": ["dataops@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=20),
    "dbt_cloud_conn_id": "dbt_conn",
    "account_id": int(Variable.get("dbt_cloud_account_id", default_var="1")),
}

ENV = os.environ.get("env", Variable.get("env", default_var="DEV"))

if ENV == "DEV":
    odoo_env = "DM"
    odoo_creds = Variable.get("odoo_dm_creds", deserialize_json=True)
    project_id = "dwh_project_dev"
    bucket_name = "data-platform-dev-rawzone"
    gcp_conn_id = "google_cloud_dev"
else:
    odoo_env = "prod"
    odoo_creds = Variable.get("odoo_prod_creds", deserialize_json=True)
    project_id = "dwh_project"
    bucket_name = "data-platform-rawzone"
    gcp_conn_id = "google_cloud_default"

# Representative subset of the ~105 production tables. Covers CRM, sales,
# helpdesk, partners, products, mail (rename + log notes), and the
# chunked tax/SOL junction table.
TABLE_LIST = [
    "crm_stage",
    "crm_lead",
    "crm_tag",
    "crm_tag_rel",
    "ir_model_data",
    "mail_message",
    "mail_message_crm_log_notes",
    "helpdesk_ticket",
    "helpdesk_team",
    "res_partner",
    "res_country",
    "sale_order",
    "sale_order_line",
    "account_tax_sale_order_line_rel",
    "product_template",
    "product_category",
]

# Domain dbt jobs keyed by Variable name (production hard-codes Cloud job IDs).
DBT_JOB_VARS = {
    "leads": "dbt_odoo_leads_job_id",
    "sale_order": "dbt_odoo_sale_order_job_id",
    "sale_order_line": "dbt_odoo_sale_order_line_job_id",
    "account_tax_sale_order_line_rel": "dbt_odoo_tax_sol_rel_job_id",
    "odoo_helpdesk": "dbt_odoo_helpdesk_job_id",
    "res_partner": "dbt_odoo_res_partner_job_id",
    "odoo_mail": "dbt_odoo_mail_job_id",
    "product_category": "dbt_odoo_product_category_job_id",
    "odoo_crm_tag": "dbt_odoo_crm_tag_job_id",
}

DATACORE_JOB_VAR = "dbt_odoo_datacore_job_id"

TMP_LOC = "/tmp/odoo_extract/"
MAX_ACTIVE_TASKS = 80

dag = DAG(
    dag_id="etl_odoo_data_import_dwh",
    default_args=default_args,
    schedule_interval="0 1 * * *",
    render_template_as_native_obj=True,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=MAX_ACTIVE_TASKS,
    # A hung dbt HTTPS trigger once held this DAG for 2d+ and blocked
    # later runs. Kill the whole run if extracts + dbt exceed this.
    dagrun_timeout=timedelta(hours=8),
    description=(
        "Daily Odoo Postgres extract into BigQuery staging + dbt trusted/datacore"
    ),
    tags=["import", "odoo", "dwh"],
)

start = EmptyOperator(task_id="start", trigger_rule=TriggerRule.ALL_DONE, dag=dag)
pause = EmptyOperator(task_id="pause", trigger_rule=TriggerRule.ALL_DONE, dag=dag)
pause_2 = EmptyOperator(task_id="pause_2", trigger_rule=TriggerRule.ALL_DONE, dag=dag)
end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE, dag=dag)


def fetch_odoo_table(table: str, **context) -> None:
    """Extract to local disk and upload that file to rawzone (avoid FUSE race)."""
    ds = context["ds"]
    safe_run = "".join(
        c if c.isalnum() or c in "-_" else "_" for c in str(context["run_id"])
    )
    folder_name = "mail_message_email_activities" if table == "mail_message" else table
    local_dir = f"{TMP_LOC}{safe_run}/"
    os.makedirs(local_dir, exist_ok=True)

    odoo = OdooDataPull(
        env=odoo_env,
        project_id=project_id,
        tmp_loc=local_dir,
        creds=odoo_creds,
    )
    hook = GCSHook(gcp_conn_id=gcp_conn_id)
    try:
        getattr(odoo, f"odoo_{table}_pull")()
        if table == "account_tax_sale_order_line_rel":
            batch_dir = os.path.join(local_dir, table)
            uploaded = 0
            for name in sorted(os.listdir(batch_dir)):
                if not name.endswith(".json"):
                    continue
                local_file = os.path.join(batch_dir, name)
                dest = f"odoo/{folder_name}/{ds}/{name}"
                hook.upload(
                    bucket_name=bucket_name,
                    object_name=dest,
                    filename=local_file,
                )
                uploaded += 1
                logging.info(
                    "Uploaded %s (%s bytes) to gs://%s/%s",
                    name,
                    os.path.getsize(local_file),
                    bucket_name,
                    dest,
                )
            if uploaded == 0:
                raise FileNotFoundError(f"No JSON chunks in {batch_dir}")
        else:
            local_file = f"{local_dir}{folder_name}.json"
            if not os.path.isfile(local_file):
                raise FileNotFoundError(local_file)
            dest = f"odoo/{folder_name}/{ds}/{folder_name}.json"
            hook.upload(
                bucket_name=bucket_name,
                object_name=dest,
                filename=local_file,
            )
            logging.info(
                "Uploaded %s (%s bytes) to gs://%s/%s",
                folder_name,
                os.path.getsize(local_file),
                bucket_name,
                dest,
            )
    finally:
        odoo.close()


with TaskGroup(group_id="extract_odoo_tables", dag=dag) as extract_odoo_tables:
    for table in TABLE_LIST:
        folder_name = (
            "mail_message_email_activities" if table == "mail_message" else table
        )
        if table == "account_tax_sale_order_line_rel":
            file_name = f"{table}_*.json"
        else:
            file_name = f"{folder_name}.json"
        source_object = f"odoo/{folder_name}/{{{{ ds }}}}/{file_name}"

        data_fetch = PythonOperator(
            task_id=f"data_fetch_{table}",
            python_callable=fetch_odoo_table,
            op_kwargs={"table": table},
            provide_context=True,
            dag=dag,
            execution_timeout=timedelta(minutes=300),
        )

        data_load_staging = GCSToBigQueryOperator(
            task_id=f"load_staging_{table}",
            gcp_conn_id=gcp_conn_id,
            bucket=bucket_name,
            source_format="NEWLINE_DELIMITED_JSON",
            source_objects=[source_object],
            destination_project_dataset_table=f"staging.odoo_{folder_name}",
            schema_object=f"schema_json/odoo_{folder_name}.json",
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_TRUNCATE",
            allow_quoted_newlines=True,
            dag=dag,
        )

        data_fetch >> data_load_staging


chain(start, extract_odoo_tables, pause)

_DBT_TASK_KW = dict(
    deferrable=True,
    check_interval=30,
    dag=dag,
    do_xcom_push=True,
    timeout=3600,
    retries=1,
    retry_delay=timedelta(minutes=5),
    execution_timeout=timedelta(minutes=60),
)

dbt_tasks = []
for domain, var_name in DBT_JOB_VARS.items():
    job_id = int(Variable.get(var_name, default_var="0"))
    dbt_tasks.append(
        DbtCloudRunJobOperator(
            task_id=f"dbt_odoo_{domain}_run",
            job_id=job_id,
            **_DBT_TASK_KW,
        )
    )

datacore_dbt_job = DbtCloudRunJobOperator(
    task_id="odoo_datacore_refresh",
    job_id=int(Variable.get(DATACORE_JOB_VAR, default_var="0")),
    **_DBT_TASK_KW,
)

chain(pause, dbt_tasks, pause_2, datacore_dbt_job, end)
