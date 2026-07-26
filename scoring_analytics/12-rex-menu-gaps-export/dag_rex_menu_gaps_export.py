"""
Airflow DAG: monthly ranked menu-gaps refresh → Avro event ingest.

Flow:
  1. dbt Cloud job refreshes refined.menu_gaps_ranked_{cc}
  2. For each country (sequential): fan out TOTAL_BATCHES parallel
     hash partitions via FARM_FINGERPRINT MOD N
  3. Each batch streams BQ → Avro → partner bulk ingest

Source (read-only):
  dags/etl_dana_rex_menu_gaps_export.py
  dags/horeca_digital/dana_rex_menu_gaps_export.py
  dags/horeca_digital/dana_rex_menu_gaps_query.py

Distinct from pattern 04 (FBO/NBO scoring hash-delta) and pattern 10
(matching-engine service rows). This DAG ships *ranked menu-gap
opportunities* with a sequential-country / parallel-batch concurrency
model — not a monthly score delta table.
"""

from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from airflow.utils.trigger_rule import TriggerRule

from menu_gaps_export import COUNTRY_ISO_CODES, send_ranged_batch

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
    "dbt_cloud_conn_id": "dbt_conn",
    "account_id": 3,
}

ENV_VAR_NAME = "env"
env = os.environ.get(ENV_VAR_NAME, Variable.get(ENV_VAR_NAME))

# Externalized — keep Cloud job ids out of the DAG body.
DBT_JOB_ID = int(Variable.get("dbt_job_menu_gaps_ranked_export", default_var="0"))

# Monthly on the 1st at 06:15 UTC. Menu-gap models are heavy; daily
# would burn dbt + API quota for a feed the partner consumed monthly.
schedule = "15 6 1 * *"

TOTAL_BATCHES = 5
COUNTRIES = COUNTRY_ISO_CODES

dag = DAG(
    dag_id="etl_menu_gaps_ranked_export",
    default_args=default_args,
    schedule_interval=schedule,
    catchup=False,
    max_active_runs=1,
    # Caps parallel batch tasks at 5 — matches TOTAL_BATCHES so one
    # country saturates the pool and the next waits on country_end.
    max_active_tasks=5,
    tags=["menu-gaps", "event-ingest", "avro", "dbt", "hash-partition"],
    doc_md=(
        "Monthly ranked menu-gaps: dbt refresh → sequential countries, "
        "parallel FARM_FINGERPRINT batches → Avro ingest. "
        "See scoring_analytics/12-rex-menu-gaps-export/."
    ),
)

start = EmptyOperator(task_id="start", dag=dag)
end = EmptyOperator(
    task_id="end",
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
)

dbt_refresh = DbtCloudRunJobOperator(
    task_id="dbt_menu_gaps_ranked_refresh",
    job_id=DBT_JOB_ID,
    check_interval=10,
    dag=dag,
    do_xcom_push=True,
    timeout=1200,
)

start >> dbt_refresh

prev_country_end = dbt_refresh

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
