"""
Airflow DAG: weekly active Odoo sale_order_line ID snapshot → event bus.

No dbt step. Queries refined sales tables directly (cleanup already
applied upstream) and fans out 13 parallel country ingest tasks.

Why a separate DAG from pattern 09:
  Pattern 09 ships SCD deltas for lead/asset lifecycle + vouchers.
  This DAG ships the full active ID set so the partner master-file
  consumer can LEFT JOIN and treat missing IDs as deleted.

Source (read-only):
  dags/etl_dana_odoo_active_asset_ids_export.py
  dags/horeca_digital/dana_odoo_assets_leads_lifecycle_export.py
  (get_active_asset_ids_query / send_active_asset_ids_data)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.helpers import chain
from airflow.utils.trigger_rule import TriggerRule

from active_asset_ids_export import send_active_asset_ids_data
from active_asset_ids_query import ActiveAssetIdsQueries

try:
    from airflow.operators.empty import EmptyOperator
except ModuleNotFoundError:
    from airflow.operators.dummy import DummyOperator as EmptyOperator  # type: ignore

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email": ["dataops@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}

# ENV drives Variable defaults inside active_asset_ids_export.py.
# Keep the lookup so Composer env overrides work the same way.
_ = Variable.get("env", default_var="DEV")

# 13 markets that register the active-ID schema with the partner bus.
# Same set as production when this shipped — not a silent "all countries".
COUNTRY_LIST = [
    "CZ",
    "DE",
    "ES",
    "FR",
    "HR",
    "HU",
    "IT",
    "NL",
    "PL",
    "PT",
    "RO",
    "SK",
    "TR",
]

# Sundays 08:00 UTC — after the weekly sale_order_line cleanup window.
schedule = "0 8 * * 0"

dag = DAG(
    dag_id="etl_odoo_active_asset_ids_export",
    default_args=default_args,
    schedule_interval=schedule,
    catchup=False,
    max_active_runs=1,
    description=(
        "[WEEKLY] Send active sale_order_line IDs to the event bus so "
        "the partner master-file can filter deleted rows"
    ),
    tags=["odoo", "active-ids", "weekly", "asset-lifecycle", "event-ingest"],
    doc_md=(
        "Weekly multi-country active sale_order_line ID snapshot → Avro "
        "event ingest. See odoo_integration/13-active-asset-ids-export/."
    ),
)

start = EmptyOperator(task_id="start", dag=dag)

ingest_list = []
for country in COUNTRY_LIST:
    query = ActiveAssetIdsQueries.get_active_asset_ids_query(country)
    task = PythonOperator(
        task_id=f"ingest_active_asset_ids_{country}",
        python_callable=send_active_asset_ids_data,
        # API path expects lowercase market code; SQL uses upper.
        op_kwargs={"country": country.lower(), "query": query},
        trigger_rule="all_success",
        execution_timeout=timedelta(minutes=60),
        dag=dag,
    )
    ingest_list.append(task)

# ALL_DONE: end still runs if one country fails so the DAG run closes
# and ops can see which markets succeeded from the graph. Tradeoff —
# a partial week can look "green" at the DAG level. Prefer paging on
# task failures, not only DAG state.
end = EmptyOperator(
    task_id="end",
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
)

# chain(start, [t1..t13], end) fans out the country tasks in parallel.
chain(start, ingest_list, end)
