"""
Airflow DAG: daily Odoo invoices dbt refresh → partner Avro ingest.

Flow:
  1. dbt Cloud job refreshes refined.odoo_invoices
  2. Sequential per-country blocks (FR, then ES)
  3. Inside each country, five parallel FARM_FINGERPRINT batches
     stream invoice lines to partner ingestbulk

Source (read-only):
  dags/etl_dana_odoo_invoices_export.py
  dags/horeca_digital/dana_odoo_invoices_export.py

Distinct from pattern 08 (WSL wholesale dual export: trusted +
recommender APPEND + single DE market). This DAG owns the general
invoice-line partner feed for FR/ES with hash partitions and no
ML history sink.
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from airflow.utils.trigger_rule import TriggerRule

import odoo_invoices_export

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
    "max_active_runs": 1,
    "retry_delay": timedelta(minutes=5),
    "dbt_cloud_conn_id": "dbt_conn",
    "account_id": 3,
}

ENV_VAR_NAME = "env"
env = os.environ.get(ENV_VAR_NAME, Variable.get(ENV_VAR_NAME))

TOTAL_BATCHES = 5
COUNTRIES = odoo_invoices_export.COUNTRY_ISO_CODES  # fr, es

# Externalized — source hard-coded a numeric dbt Cloud job id.
DBT_JOB_ID = int(Variable.get("dbt_job_odoo_invoices", default_var="0"))

# Daily after Odoo → warehouse extract and the refined dbt window.
# 05:30 UTC matched the production Composer slot.
schedule = "30 5 * * *"

dag = DAG(
    dag_id="etl_odoo_invoices_export",
    default_args=default_args,
    schedule_interval=schedule,
    catchup=False,
    max_active_tasks=3,
    tags=["odoo", "invoices", "event-ingest"],
    doc_md=(
        "Daily Odoo invoices: dbt refresh → FR/ES fingerprint batches → "
        "Avro event ingest. See odoo_integration/84-odoo-invoices-partner-export/."
    ),
)

start = EmptyOperator(task_id="start", dag=dag)
end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE, dag=dag)

dbt_odoo_invoices = DbtCloudRunJobOperator(
    task_id="dbt_odoo_invoices_refresh",
    job_id=DBT_JOB_ID,
    deferrable=True,
    execution_timeout=timedelta(minutes=60),
    check_interval=30,
    do_xcom_push=True,
    timeout=3600,
    dag=dag,
)

start >> dbt_odoo_invoices

prev_country_end = dbt_odoo_invoices

for country in COUNTRIES:
    country_start = EmptyOperator(task_id=f"start_{country}", dag=dag)
    country_end = EmptyOperator(
        task_id=f"end_{country}",
        trigger_rule=TriggerRule.ALL_DONE,
        dag=dag,
    )

    prev_country_end >> country_start

    for batch_num in range(TOTAL_BATCHES):
        task = PythonOperator(
            task_id=f"export_{country}_batch_{batch_num}",
            python_callable=odoo_invoices_export.send_ranged_batch,
            op_kwargs={
                "iso_code_lower": country,
                "batch_number": batch_num,
                "total_batches": TOTAL_BATCHES,
            },
            execution_timeout=timedelta(hours=4),
            trigger_rule="all_success",
            dag=dag,
        )
        country_start >> task >> country_end

    prev_country_end = country_end

prev_country_end >> end
