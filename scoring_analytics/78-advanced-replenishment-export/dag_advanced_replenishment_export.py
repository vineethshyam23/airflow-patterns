"""
Airflow DAG: Advanced Replenishment → partner Avro ingest.

Three subjects in one DAG:
  - predicted ingredient quantity — daily, 10 parallel hash batches
  - platform POS customers — monthly full load on the 5th (UTC)
  - articles → ingredients — monthly full load on the 5th (UTC)

The daily arm always runs. A BranchPythonOperator gates the monthly
arm on UTC day == 5 so we do not schedule a second DAG just for two
slower reference feeds.

Source (read-only):
  dags/etl_dana_advanced_replenishment_export.py
  dags/horeca_digital/dana_advanced_replenishment_export.py

Distinct from pattern 12/14 (ranked menu gaps) and pattern 53
(POS Intelligence article recommendations). This feed ships
*predicted ingredient demand* plus the customer / article lookup
tables the partner needs to join it.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from advanced_replenishment_export import (
    send_articles_to_ingredients,
    send_platform_pos_customers,
    send_predicted_total_quantity,
)

try:
    from airflow.operators.empty import EmptyOperator
except ModuleNotFoundError:
    from airflow.operators.dummy import DummyOperator as EmptyOperator  # type: ignore

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "start_date": datetime(2026, 10, 1),
    "email": ["dataops@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# Daily at 06:15 UTC — after overnight Vertex / foodgraph materialization.
schedule = "15 6 * * *"
TOTAL_BATCHES = 10


def choose_monthly_branch():
    """Customer and articles feeds run on the 5th UTC with the daily quantity arm."""
    if datetime.utcnow().day == 5:
        return "export_ar_platform_pos_customers"
    return "skip_monthly_exports"


dag = DAG(
    dag_id="etl_advanced_replenishment_export",
    default_args=default_args,
    schedule_interval=schedule,
    catchup=False,
    max_active_runs=1,
    # Matches TOTAL_BATCHES so the daily arm can saturate the pool.
    max_active_tasks=10,
    tags=["advanced-replenishment", "event-ingest", "avro", "hash-partition"],
    doc_md=(
        "Advanced Replenishment: daily FARM_FINGERPRINT quantity batches + "
        "monthly customer/articles on the 5th. "
        "See scoring_analytics/78-advanced-replenishment-export/."
    ),
)

start = EmptyOperator(task_id="start", dag=dag)
end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE, dag=dag)

predicted_start = EmptyOperator(task_id="start_predicted", dag=dag)
predicted_end = EmptyOperator(
    task_id="end_predicted",
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
)

for batch_num in range(TOTAL_BATCHES):
    export_predicted = PythonOperator(
        task_id=f"export_ar_predicted_total_quantity_batch_{batch_num}",
        python_callable=send_predicted_total_quantity,
        op_kwargs={
            "batch_number": batch_num,
            "total_batches": TOTAL_BATCHES,
        },
        execution_timeout=timedelta(hours=4),
        dag=dag,
    )
    predicted_start >> export_predicted >> predicted_end

monthly_gate = BranchPythonOperator(
    task_id="monthly_on_5th",
    python_callable=choose_monthly_branch,
    dag=dag,
)

skip_monthly = EmptyOperator(task_id="skip_monthly_exports", dag=dag)

export_customers = PythonOperator(
    task_id="export_ar_platform_pos_customers",
    python_callable=send_platform_pos_customers,
    execution_timeout=timedelta(hours=4),
    dag=dag,
)

export_a2i = PythonOperator(
    task_id="export_ar_articles_to_ingredients",
    python_callable=send_articles_to_ingredients,
    execution_timeout=timedelta(hours=4),
    dag=dag,
)

start >> predicted_start
predicted_end >> end
start >> monthly_gate
monthly_gate >> export_customers >> export_a2i >> end
monthly_gate >> skip_monthly >> end
