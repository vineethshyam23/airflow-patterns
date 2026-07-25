"""
Airflow DAG: daily payment KYC refresh → Avro event ingest.

Flow:
  1. dbt Cloud job refreshes refined.payment_kyc_export
     (staging filter → SCD2 snapshot → int → current valid rows)
  2. Per-country Avro bulk ingest (pilot: PL)

Source (read-only):
  dags/etl_dana_dishpay_kyc_export.py
  dags/horeca_digital/dana_dishpay_kyc_export.py
  dags/horeca_digital/dana_dishpay_kyc_query.py

Distinct from pattern 03 (Adyen Management API terminal inventory).
This DAG ships KYC *onboarding status* for a payment product to a
partner event bus — it does not call the PSP Management API.
"""

from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from airflow.utils.helpers import chain

from kyc_export import send_payment_kyc_data
from kyc_query import PaymentKyc

try:
    from airflow.operators.empty import EmptyOperator
except ModuleNotFoundError:
    from airflow.operators.dummy import DummyOperator as EmptyOperator  # type: ignore

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
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

# dbt Cloud job that materializes refined.payment_kyc_export.
# Externalized — keep job ids out of the DAG body.
DBT_JOB_ID = int(Variable.get("dbt_job_payment_kyc_export", default_var="0"))

# Daily after the warehouse extract window and before partner reporting
# pulls. 06:00 UTC matched the production Composer slot.
schedule = "0 6 * * *"

# Pilot market list. Filters (country + payment product) are applied in
# dbt staging — this list only drives ingest task fan-out.
country_list = PaymentKyc.countries  # ['pl']

dag = DAG(
    dag_id="etl_payment_kyc_export",
    default_args=default_args,
    schedule_interval=schedule,
    catchup=False,
    max_active_runs=1,
    tags=["payment", "kyc", "event-ingest", "avro", "dbt"],
    doc_md=(
        "Daily payment KYC: dbt refresh → Avro event ingest. "
        "See payment_processing/11-dishpay-kyc-export/."
    ),
)

start = EmptyOperator(task_id="start", dag=dag)
end = EmptyOperator(task_id="end", dag=dag)

# Step 1: dbt pipeline
#   stg_payment_kyc → payment_kyc_snapshot (SCD2) → int_payment_kyc
#   → payment_kyc_export (_valid_flag = true)
kyc_dbt_job = DbtCloudRunJobOperator(
    task_id="dbt_payment_kyc_refresh",
    job_id=DBT_JOB_ID,
    check_interval=10,
    dag=dag,
    do_xcom_push=True,
    timeout=600,
)

ingest_tasks = []
for country in country_list:
    task = PythonOperator(
        task_id=f"export_payment_kyc_{country.upper()}",
        python_callable=send_payment_kyc_data,
        op_kwargs={"country": country},
        trigger_rule="all_success",
        dag=dag,
    )
    ingest_tasks.append(task)

# start → dbt → country ingest(s) → end
# Production left `start` wired only to dbt and export only to end;
# chain makes the full path visible in the graph.
chain(start, kyc_dbt_job, *ingest_tasks, end)
