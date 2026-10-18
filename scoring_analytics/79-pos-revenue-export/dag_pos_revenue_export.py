"""
Airflow DAG: monthly platform POS revenue potentials → Avro event ingest.

Flow:
  product_spot POS transactions + ERP asset lifecycle
    + refined match_result_erp_wholesale
  → sequential countries (DE, FR, ES, IT)
  → stream BQ → Avro (500-row chunks) → partner bulk ingest

Schedule sits 30 minutes after the upstream SAM / pos_potential dbt
job so annual_sale and this export stay on the same monthly cut.

Source (read-only):
  dags/etl_dana_pos_revenue_export.py
  dags/horeca_digital/dana_pos_revenue_export.py

Distinct from pattern 78 (Advanced Replenishment predicted quantity),
patterns 12/14 (ranked menu gaps), and pattern 53 (POS Intelligence
article recommendations). This feed ships *customer-grain annualised
POS revenue potential* — the SAM annual_sale contract — not demand
forecasts or article rankings.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from pos_revenue_export import COUNTRY_ISO_CODES, send_pos_revenue_data

try:
    from airflow.operators.empty import EmptyOperator
except ModuleNotFoundError:
    from airflow.operators.dummy import DummyOperator as EmptyOperator  # type: ignore

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "start_date": datetime(2026, 9, 1),
    "email": ["dataops@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# 1st of month 06:30 UTC — after upstream pos_potential dbt (06:00).
schedule = "30 6 1 * *"
COUNTRIES = COUNTRY_ISO_CODES

dag = DAG(
    dag_id="etl_pos_revenue_export",
    default_args=default_args,
    schedule_interval=schedule,
    catchup=False,
    max_active_runs=1,
    # One country at a time — full-table stream is heavy enough without
    # parallel markets fighting for the same OAuth / ingest quota.
    max_active_tasks=1,
    tags=["pos-revenue", "event-ingest", "avro", "monthly-full-load"],
    doc_md=(
        "Monthly platform POS revenue potentials: sequential countries → "
        "Avro bulk ingest. "
        "See scoring_analytics/79-pos-revenue-export/."
    ),
)

start = EmptyOperator(task_id="start", dag=dag)
end = EmptyOperator(
    task_id="end",
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
)

prev = start
for country in COUNTRIES:
    export_task = PythonOperator(
        task_id=f"export_pos_revenue_{country.upper()}",
        python_callable=send_pos_revenue_data,
        op_kwargs={"country": country},
        execution_timeout=timedelta(hours=4),
        dag=dag,
    )
    prev >> export_task
    prev = export_task

prev >> end
