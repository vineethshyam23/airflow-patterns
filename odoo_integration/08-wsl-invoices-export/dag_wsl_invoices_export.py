"""
Airflow DAG: daily Odoo WSL invoices refresh → recommender append → event ingest.

Flow:
  1. dbt Cloud job refreshes trusted.int_odoo_wsl_invoices
  2. WRITE_APPEND the same rowset into ml_recommender.odoo_wsl_invoices
  3. Avro bulk ingest for the pilot market (DE)

Source (read-only):
  dags/etl_dana_odoo_wsl_invoices_export.py
  dags/horeca_digital/dana_odoo_wsl_invoices.py

Distinct from pattern 07 (list-price monthly hash-delta): this ships the
wholesale invoice fact table itself, and fans out to both an event bus and
an ML recommender history table in one DAG.
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.helpers import chain

from wsl_invoices_export import send_wsl_invoices_data
from wsl_invoices_query import OdooWslInvoices

try:
    from airflow.operators.empty import EmptyOperator
except ModuleNotFoundError:
    from airflow.operators.dummy import DummyOperator as EmptyOperator  # type: ignore

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "start_date": datetime(2025, 11, 2),
    "email": ["dataops@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "dbt_cloud_conn_id": "dbt_conn",
    "account_id": 3,
}

ENV_VAR_NAME = "env"
env = os.environ.get(ENV_VAR_NAME, Variable.get(ENV_VAR_NAME))

if env == "DEV":
    bigquery_conn_id = "bigquery_default_dev"
    project_name = "dwh_project_dev"
else:
    bigquery_conn_id = "bigquery_default"
    project_name = "dwh_project"

# Pilot market. Kept as a single country for now — invoice semantics differ
# enough across markets that a second country should be an explicit add.
country = "de"

# dbt Cloud job that materializes the trusted WSL intermediate.
# Externalized — source hard-coded a numeric job id in the DAG.
DBT_JOB_ID = int(Variable.get("dbt_job_odoo_wsl_invoices", default_var="0"))

# Daily after Odoo → warehouse extract and the dbt trusted refresh window.
# 05:55 UTC matched the production Composer slot; keep it so ops runbooks
# stay familiar.
schedule = "55 5 * * *"

# catchup=False on purpose. Production had catchup=True, which on a
# WRITE_APPEND recommender table creates duplicate daily snapshots after
# scheduler gaps. Prefer an explicit manual re-run when you need backfill.
dag = DAG(
    dag_id="etl_odoo_wsl_invoices_export",
    default_args=default_args,
    schedule_interval=schedule,
    catchup=False,
    max_active_runs=1,
    tags=["odoo", "wsl", "invoices", "event-ingest", "recommender"],
    doc_md=(
        "Daily Odoo WSL invoices: dbt refresh → recommender APPEND → "
        "Avro event ingest. See odoo_integration/08-wsl-invoices-export/."
    ),
)

send_data_query = OdooWslInvoices.get_send_query()
recommender_query = OdooWslInvoices.get_recommender_copy_query()

start = EmptyOperator(task_id="start", dag=dag)
end = EmptyOperator(task_id="end", dag=dag)

wsl_dbt_job = DbtCloudRunJobOperator(
    task_id="dbt_odoo_wsl_invoices_run",
    job_id=DBT_JOB_ID,
    check_interval=10,
    dag=dag,
    do_xcom_push=True,
    timeout=600,
)

copy_to_recommender = BigQueryInsertJobOperator(
    task_id="copy_to_recommender",
    configuration={
        "query": {
            "query": recommender_query,
            "useLegacySql": False,
            "destinationTable": {
                "projectId": project_name,
                "datasetId": "ml_recommender",
                "tableId": "odoo_wsl_invoices",
            },
            "writeDisposition": "WRITE_APPEND",
        }
    },
    gcp_conn_id=bigquery_conn_id,
    dag=dag,
)

ingest_data = PythonOperator(
    task_id=f"ingest_wsl_invoices_{country}",
    python_callable=send_wsl_invoices_data,
    op_kwargs={"country": country.lower(), "query": send_data_query},
    trigger_rule="all_success",
    dag=dag,
)

# Linear: trusted refresh must land before both consumers read it.
# Recommender APPEND before event ingest so a failed API call still leaves
# ML history current for the day (ingest can retry independently).
chain(
    start,
    wsl_dbt_job,
    copy_to_recommender,
    ingest_data,
    end,
)
