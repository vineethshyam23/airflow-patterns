"""
Airflow DAG: monthly independent-establishment menu-gaps → Avro event ingest.

Flow:
  1. For each country (sequential): fan out TOTAL_BATCHES parallel
     hash partitions via FARM_FINGERPRINT MOD N
  2. Each batch streams BQ → Avro → partner bulk ingest

No dbt step here — refined.menu_gaps_independent_{cc} is owned by an
upstream job. This DAG is pure export.

Source (read-only):
  dags/etl_dana_rex_menu_gaps_non_metro_export.py
  dags/horeca_digital/dana_rex_menu_gaps_non_metro_export.py

Distinct from pattern 12 (ranked wholesale-account menu gaps with
dbt refresh + article/rank schema). This DAG ships *independent*
establishment gaps under an address/geo/contact Avro contract.
"""

from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from menu_gaps_indep_export import COUNTRY_ISO_CODES, send_ranged_batch

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

# Monthly on the 1st at 06:30 UTC — offset from the ranked sibling
# (06:15) so both feeds do not saturate Composer + the ingest API
# at the same minute.
schedule = "30 6 1 * *"

TOTAL_BATCHES = 5
COUNTRIES = COUNTRY_ISO_CODES

dag = DAG(
    dag_id="etl_menu_gaps_independent_export",
    default_args=default_args,
    schedule_interval=schedule,
    catchup=False,
    max_active_runs=1,
    # Caps parallel batch tasks at 5 — matches TOTAL_BATCHES so one
    # country saturates the pool and the next waits on country_end.
    max_active_tasks=5,
    tags=["menu-gaps", "independent", "event-ingest", "avro", "hash-partition"],
    doc_md=(
        "Monthly independent-establishment menu-gaps: sequential "
        "countries, parallel FARM_FINGERPRINT batches → Avro ingest. "
        "See scoring_analytics/14-menu-gaps-independent-export/."
    ),
)

start = EmptyOperator(task_id="start", dag=dag)
end = EmptyOperator(
    task_id="end",
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
)

prev_country_end = start

for country in COUNTRIES:
    country_start = EmptyOperator(task_id=f"start_{country}", dag=dag)
    country_end = EmptyOperator(
        task_id=f"end_{country}",
        # ALL_DONE: a failed batch does not strand later countries.
        # Tradeoff — partial delivery is possible; watch failed-task
        # counts rather than assuming end == complete export.
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
