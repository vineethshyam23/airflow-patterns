"""
Airflow DAG: MAG acquisition + penetration monthly Avro export.

Two independent country chains share one Composer DAG:
  - acquisition: historical product-bundle sales values
  - penetration: active / buying wholesale vs platform subscription rates

Countries run sequentially inside each chain with ALL_DONE so a failed
market does not stop the rest. The two chains have no cross-edge —
Composer can interleave them.

Source (read-only):
  dags/etl_dana_mag_export.py
  dags/horeca_digital/dana_mag_acquisition.py
  dags/horeca_digital/dana_mag_penetration.py

Distinct from pattern 04 (FBO/NBO scores), pattern 17 (establishment
listings), and pattern 23 (customer product footprint). This feed is
management reporting: acquisition dollars and penetration rates, not
per-establishment enrichment.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from mag_acquisition import send_mag_acquisition_data
from mag_penetration import send_mag_penetration_data

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

# 2nd of each month at 15:45 UTC — after month-end refine of the
# historical MAG reporting tables.
schedule = "45 15 2 * *"

# ISO markets + `ag` (aggregate / corporate rollup → warehouse `corp`).
countries = [
    "hr",
    "cz",
    "fr",
    "de",
    "hu",
    "it",
    "pl",
    "pt",
    "es",
    "at",
    "be",
    "nl",
    "ro",
    "sk",
    "tr",
    "ua",
    "ag",
]

dag = DAG(
    dag_id="etl_mag_reporting_export",
    default_args=default_args,
    schedule_interval=schedule,
    catchup=False,
    max_active_runs=1,
    tags=["mag", "acquisition", "penetration", "event-ingest", "avro", "monthly"],
    doc_md=(
        "Monthly MAG acquisition + penetration export to the partner "
        "event bus. Two independent sequential country chains."
    ),
)

last_acq = None
last_pen = None

for cnt in countries:
    ingest_acquisition = PythonOperator(
        task_id=f"ingest_acquisition_{cnt}",
        python_callable=send_mag_acquisition_data,
        op_kwargs={"country": cnt},
        trigger_rule=TriggerRule.ALL_DONE,
        dag=dag,
    )
    if last_acq is not None:
        last_acq >> ingest_acquisition
    last_acq = ingest_acquisition

for cnt in countries:
    ingest_penetration = PythonOperator(
        task_id=f"ingest_penetration_{cnt}",
        python_callable=send_mag_penetration_data,
        op_kwargs={"country": cnt},
        trigger_rule=TriggerRule.ALL_DONE,
        dag=dag,
    )
    if last_pen is not None:
        last_pen >> ingest_penetration
    last_pen = ingest_penetration
