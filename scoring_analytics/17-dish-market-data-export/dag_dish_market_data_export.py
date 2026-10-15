"""
Airflow DAG: monthly establishment market-data → Avro event ingest.

Topology:
  start → {start_cc → [export_cc_batch_0..N-1] → end_cc}×countries → end

Countries run sequentially. Inside a country, TOTAL_BATCHES parallel
tasks each ship one FARM_FINGERPRINT shard of md_establishment_id.
Schedule sits after the upstream foodgraph / SEO refine job on the 1st.

Source (read-only):
  dags/etl_dana_dish_market_data_export.py
  dags/horeca_digital/dana_dish_market_data_export.py
  dags/horeca_digital/foodgraph_queries.py (active ISO list)

Distinct from patterns 12/14 (menu-gap opportunity ranking) and
pattern 16 (peer spend gaps). This ships the listing / geo / contact
attributes of establishments — not purchase-gap scores.
"""

from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from market_data_export import COUNTRY_ISO_CODES, send_ranged_batch

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
    "retry_delay": timedelta(minutes=5),
}

ENV_VAR_NAME = "env"
env = os.environ.get(ENV_VAR_NAME, Variable.get(ENV_VAR_NAME))

TOTAL_BATCHES = 5
COUNTRIES = COUNTRY_ISO_CODES

# 1st of month 06:30 UTC — after the upstream market-data refine DAG.
schedule = "30 6 1 * *"

dag = DAG(
    dag_id="etl_establishment_market_data_export",
    default_args=default_args,
    schedule_interval=schedule,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=5,
    tags=["event-ingest", "avro", "market-data", "foodgraph"],
    doc_md=(
        "Monthly full-load export of refined.establishment_market_data_{cc} "
        "to partner event ingest. See scoring_analytics/17-dish-market-data-export/."
    ),
)

start = EmptyOperator(task_id="start", dag=dag)
end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE, dag=dag)

prev_country_end = start

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
            python_callable=send_ranged_batch,
            op_kwargs={
                "iso_code_lower": country,
                "batch_number": batch_num,
                "total_batches": TOTAL_BATCHES,
            },
            execution_timeout=timedelta(hours=4),
            dag=dag,
        )
        country_start >> task >> country_end

    prev_country_end = country_end

prev_country_end >> end
