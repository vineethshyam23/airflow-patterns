"""
Airflow DAG: weekly matching-engine export to partner event bus.

Flow:
  1. Rebuild staging table (match_result + customer base + wholesale attrs,
     unpivot product flags → one row per service)
  2. Fan out per-country Avro bulk ingest tasks

Source (read-only):
  dags/horeca_digital/archived/etl_dana_matching_engine_export.py
  dags/horeca_digital/matching_export_to_DANA.py
  dags/horeca_digital/dana_matching_engine_export.py

Distinct from pattern 01 (SCD Type 2 history of match_result). This DAG
ships the *current* valid matches shaped as service rows to an event
ingest API — it does not write SCD history.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.helpers import chain

from matching_event_export import send_matching_engine_data
from matching_prepare import matching_export_prepare

try:
    from airflow.operators.empty import EmptyOperator
except ModuleNotFoundError:
    from airflow.operators.dummy import DummyOperator as EmptyOperator  # type: ignore

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "start_date": datetime(2023, 9, 15),
    "email": ["dataops@example.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
}

# Thursday 08:00 UTC — weekly partner refresh cadence when this shipped.
# Matching quality drifts slowly; daily would burn BQ + API quota for
# little consumer value.
schedule = "0 8 * * 4"

# Markets the partner registered for this schema. Note: prepare SQL also
# covers be/sk, but ingest was scoped to the markets below in production.
# Adding a market is a schema + consumer decision, not just a list edit.
COUNTRIES = [
    "hr",
    "cz",
    "fr",
    "de",
    "hu",
    "it",
    "pl",
    "pt",
    "es",
    "nl",
    "ro",
    "tr",
    "ua",
]

dag = DAG(
    dag_id="etl_matching_engine_event_export",
    default_args=default_args,
    schedule_interval=schedule,
    catchup=False,
    max_active_runs=1,
    tags=["matching", "event-ingest", "multi-country", "avro"],
    doc_md=(
        "Weekly wholesale↔SaaS matching export → Avro event ingest. "
        "See sql_patterns/10-matching-engine-event-export/."
    ),
)

start = EmptyOperator(task_id="start", dag=dag)
end = EmptyOperator(task_id="end", dag=dag)

# Staging rebuild is shared. Per-country ingest must not run against a
# half-written replace — Airflow serializes the PythonOperator, and
# to_gbq(if_exists='replace') is atomic enough for this volume once the
# task succeeds.
prepare_staging = PythonOperator(
    task_id="prepare_matching_staging",
    python_callable=matching_export_prepare,
    op_kwargs={
        "project_id": "dwh_project",
        "destination_dataset": "refined",
        "destination_table_name": "partner_matching_export",
    },
    dag=dag,
)

ingest_tasks = []
for country in COUNTRIES:
    task = PythonOperator(
        task_id=f"ingest_{country}",
        python_callable=send_matching_engine_data,
        op_kwargs={"country": country},
        dag=dag,
    )
    ingest_tasks.append(task)

# prepare first, then fan-out country ingests, then end.
chain(start, prepare_staging, *ingest_tasks, end)
