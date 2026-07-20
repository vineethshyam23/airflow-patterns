"""
Airflow DAG: daily Odoo helpdesk tickets export to an external event API.

Flow:
  1. dbt Cloud job refreshes refined.odoo_helpdesk_ticket
  2. Query yesterday's creates → Avro encode → bulk ingest (one market)

Source (read-only):
  dags/etl_dana_odoo_helpdesk_tickets.py
  dags/horeca_digital/dana_odoo_helpdesk_ticket.py

Date-delta (create_date = yesterday), not hash-delta. Simpler than the
scoring / SFDC history exports — support tickets are append-mostly and
the consumer only needs the prior day's opens.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from airflow.utils.helpers import chain

from helpdesk_tickets_export import send_helpdesk_tickets_data
from helpdesk_tickets_query import get_helpdesk_tickets_send_query

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
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "dbt_cloud_conn_id": "dbt_conn",
    "account_id": 3,
}

# Pilot / production market for this export. Kept as a single value because
# the refined helpdesk model was scoped to one country when this shipped.
# BQ project / event-API env split lives in helpdesk_tickets_export.py.
country = "de"

# dbt Cloud job that rebuilds refined.odoo_helpdesk_ticket.
# Externalize so env swaps do not require a DAG edit.
DBT_JOB_ID = int(
    Variable.get("dbt_job_helpdesk_tickets", default_var="434396")
)

# Daily 04:30 UTC — after overnight Odoo → warehouse landing + before
# morning support analytics consumers pull the event bus.
schedule = "30 4 * * *"

dag = DAG(
    dag_id="etl_odoo_helpdesk_tickets_export",
    default_args=default_args,
    schedule_interval=schedule,
    catchup=False,
    max_active_runs=1,
    tags=["odoo", "helpdesk", "event-ingest", "support"],
    doc_md=(
        "dbt refresh of refined Odoo helpdesk tickets → yesterday's creates "
        "→ Avro bulk ingest. See odoo_integration/06-helpdesk-tickets-export/."
    ),
)

send_data_query = get_helpdesk_tickets_send_query()

start = EmptyOperator(task_id="start", dag=dag)
end = EmptyOperator(task_id="end", dag=dag)

helpdesk_dbt_job = DbtCloudRunJobOperator(
    task_id="dbt_helpdesk_tickets_run",
    job_id=DBT_JOB_ID,
    check_interval=10,
    dag=dag,
    do_xcom_push=True,
    timeout=300,
)

ingest_data = PythonOperator(
    task_id=f"ingest_helpdesk_tickets_data_{country}",
    python_callable=send_helpdesk_tickets_data,
    op_kwargs={"country": country.lower(), "query": send_data_query},
    trigger_rule="all_success",
    dag=dag,
)

chain(start, helpdesk_dbt_job, ingest_data, end)
