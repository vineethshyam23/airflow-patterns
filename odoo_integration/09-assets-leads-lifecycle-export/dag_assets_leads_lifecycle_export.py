"""
Airflow DAG: twice-daily Odoo / CRM lead + asset lifecycle + voucher export.

Flow:
  1. dbt Cloud job refreshes refined lead/asset lifecycle (+ voucher) models
  2. Three parallel Avro bulk ingest tasks for one pilot market (FR):
       - lead lifecycle (SCD delta: _valid_from >= today)
       - asset lifecycle (SCD delta: _valid_from >= today)
       - voucher codes (created since yesterday)

Source (read-only):
  dags/etl_dana_odoo_assets_leads_export.py
  dags/horeca_digital/dana_odoo_assets_leads_lifecycle_export.py

Distinct from pattern 05 (SFDC asset history hash-delta) and pattern 08
(WSL invoices dual sink). This one fans out three related CRM/Odoo
lifecycle feeds after a single dbt refresh, using SCD validity columns
instead of a hist/keyhash compare.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from airflow.utils.helpers import chain

from lifecycle_export import (
    send_asset_lifecycle_data,
    send_lead_lifecycle_data,
    send_voucher_code_data,
)
from lifecycle_queries import LifecycleQueries

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

# Pilot market. Lead/asset semantics and partner schema registration are
# FR-specific in the source deployment — keep it explicit, not a loop.
# ENV still drives Variable defaults inside lifecycle_export.py.
country = "fr"

# dbt Cloud job that refreshes lead + asset lifecycle refined models.
# Externalized — source hard-coded a numeric job id in the DAG.
DBT_JOB_ID = int(
    Variable.get("dbt_job_odoo_leads_assets_lifecycle", default_var="0")
)

# Twice daily: morning + midday so partner FR reporting sees same-day
# status changes without waiting for next-day batch.
schedule = "0 6,13 * * *"

# Datasets shipped in parallel after dbt. Order in the list is stable for
# task_id naming; Airflow runs them as siblings under the chain fan-out.
DATASETS = (
    (
        "lead_lifecycle",
        LifecycleQueries.get_leads_export_query(country.upper()),
        send_lead_lifecycle_data,
    ),
    (
        "asset_lifecycle",
        LifecycleQueries.get_assets_export_query(country.upper()),
        send_asset_lifecycle_data,
    ),
    (
        "voucher_code",
        LifecycleQueries.get_voucher_code_export_query(country.upper()),
        send_voucher_code_data,
    ),
)

dag = DAG(
    dag_id="etl_odoo_assets_leads_lifecycle_export",
    default_args=default_args,
    schedule_interval=schedule,
    catchup=False,
    max_active_runs=1,
    tags=["odoo", "crm", "lifecycle", "event-ingest", "scd-delta"],
    doc_md=(
        "Twice-daily FR lead/asset lifecycle + voucher SCD deltas → Avro "
        "event ingest. See odoo_integration/09-assets-leads-lifecycle-export/."
    ),
)

start = EmptyOperator(task_id="start", dag=dag)
end = EmptyOperator(task_id="end", dag=dag)

dbt_lifecycle_job = DbtCloudRunJobOperator(
    task_id="dbt_odoo_leads_assets_lifecycle_refresh",
    job_id=DBT_JOB_ID,
    check_interval=10,
    dag=dag,
    do_xcom_push=True,
    timeout=600,
)

ingest_tasks = []
for name, query, send_fn in DATASETS:
    task = PythonOperator(
        task_id=f"ingest_odoo_{name}_data",
        python_callable=send_fn,
        op_kwargs={"country": country, "query": query},
        trigger_rule="all_success",
        dag=dag,
    )
    ingest_tasks.append(task)

# dbt first (shared refresh), then three parallel ingests, then end.
# chain(*list) fans out siblings correctly after the dbt task.
chain(start, dbt_lifecycle_job, *ingest_tasks, end)
