"""
Airflow DAG: monthly POS Intelligence recommendations → Avro event ingest.

Flow:
  Vertex / ML preprocessed table
    pos_article_final_recommendation_{CC}
  → sequential countries (pilot: FR)
  → stream BQ → Avro (500-row chunks) → partner bulk ingest

Source (read-only):
  dags/etl_dana_pos_intelligence_recommendations_export.py
  dags/horeca_digital/dana_pos_intelligence_export.py

Distinct from patterns 12/14 (ranked menu gaps) and pattern 16 (peer
spend gaps). This DAG ships *wholesale article recommendations*
driven by POS usage vs purchase gap — not a menu-gap score table.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from pos_intelligence_export import COUNTRY_ISO_CODES, send_pos_intelligence_data

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

# Same monthly window as ranked menu-gap exports. Run after the Vertex
# / foodgraph job that lands the recommendation table.
schedule = "15 6 1 * *"
COUNTRIES = COUNTRY_ISO_CODES

dag = DAG(
    dag_id="etl_pos_intelligence_recommendations_export",
    default_args=default_args,
    schedule_interval=schedule,
    catchup=False,
    max_active_runs=1,
    # One country at a time — full-table stream is heavy enough without
    # parallel markets fighting for the same OAuth / ingest quota.
    max_active_tasks=1,
    tags=["pos-intelligence", "event-ingest", "avro", "foodgraph", "vertex"],
    doc_md=(
        "Monthly POS Intelligence article recommendations: sequential "
        "countries → Avro bulk ingest. "
        "See scoring_analytics/53-pos-intelligence-recommendations-export/."
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
        task_id=f"export_pos_intelligence_{country.upper()}",
        python_callable=send_pos_intelligence_data,
        op_kwargs={"country": country},
        execution_timeout=timedelta(hours=4),
        dag=dag,
    )
    prev >> export_task
    prev = export_task

prev >> end
