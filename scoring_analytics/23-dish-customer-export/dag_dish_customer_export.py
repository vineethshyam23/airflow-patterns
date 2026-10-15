"""
Airflow DAG: multi-country platform-customer footprint → Avro event ingest.

Flow:
  1. Parallel BQ inserts per country into one staging table
     (WRITE_TRUNCATE on first country, WRITE_APPEND on the rest)
  2. dbt Cloud job refreshes refined.platform_customer_export
  3. Parallel per-country Avro bulk ingest

Source (read-only):
  dags/etl_dana_DISH_customer_data_export.py
  dags/horeca_digital/dana_DISH_customer_export.py
  dags/horeca_digital/dana_DISH_customer_query.py

Distinct from pattern 11 (payment KYC, single pilot market) and
pattern 17 (monthly market-listing dump). This DAG ships product
subscription / activation flags for matched wholesale customers
across 14 countries, with a dbt refine step between staging and
ingest.
"""

from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.helpers import chain
from airflow.utils.trigger_rule import TriggerRule

from customer_export import send_platform_customer_data
from customer_query import PlatformCustomer

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

ENV_VAR_NAME = "env"
env = os.environ.get(ENV_VAR_NAME, Variable.get(ENV_VAR_NAME))

if env == "DEV":
    bigquery_conn_id = "bigquery_default_dev"
    project_name = "dwh_project_dev"
else:
    bigquery_conn_id = "bigquery_default"
    project_name = "dwh_project"

# Export markets. BE exists in the query module map but was not on the
# production export list — keep that intentional gap visible.
country_list = [
    "PL",
    "DE",
    "PT",
    "FR",
    "ES",
    "NL",
    "RO",
    "HR",
    "HU",
    "IT",
    "SK",
    "CZ",
    "TR",
    "UA",
]

table_name = "platform_customer_staging"

# dbt job that turns staging → SCD/refined.platform_customer_export.
# Externalized — keep job ids out of the DAG body.
DBT_JOB_ID = int(Variable.get("dbt_job_platform_customer_export", default_var="0"))

# Daily after upstream MCC / ERP / match loads. 05:05 UTC matched the
# production Composer slot.
schedule = "5 5 * * *"

dag = DAG(
    dag_id="etl_platform_customer_export",
    default_args=default_args,
    schedule_interval=schedule,
    catchup=False,
    max_active_runs=1,
    tags=["customer", "event-ingest", "avro", "dbt", "multi-country"],
    doc_md=(
        "Multi-country platform-customer footprint: BQ staging → dbt → "
        "Avro event ingest. See scoring_analytics/23-dish-customer-export/."
    ),
)

start = EmptyOperator(task_id="start", dag=dag)
pause = EmptyOperator(task_id="pause", dag=dag)
end = EmptyOperator(task_id="end", dag=dag)

dbt_refresh = DbtCloudRunJobOperator(
    task_id="dbt_platform_customer_table_refresh",
    job_id=DBT_JOB_ID,
    check_interval=10,
    dag=dag,
    do_xcom_push=True,
    timeout=300,
)

insert_list = []
for country in country_list:
    insert_sql = PlatformCustomer.get_insert_query(country)
    insert_task = BigQueryInsertJobOperator(
        task_id=f"insert_table_{table_name}_{country}",
        configuration={
            "query": {
                "query": insert_sql,
                "useLegacySql": False,
                # First country truncates; the rest append into one table.
                "writeDisposition": (
                    "WRITE_APPEND" if country != country_list[0] else "WRITE_TRUNCATE"
                ),
                "createDisposition": "CREATE_IF_NEEDED",
                "allowLargeResults": True,
                "destinationTable": {
                    "projectId": project_name,
                    "datasetId": "staging",
                    "tableId": table_name,
                },
            }
        },
        gcp_conn_id=bigquery_conn_id,
        trigger_rule=TriggerRule.ALL_SUCCESS,
        dag=dag,
    )
    insert_list.append(insert_task)

ingest_list = []
for country in country_list:
    send_query = PlatformCustomer.get_send_query(country)
    ingest_task = PythonOperator(
        task_id=f"ingest_platform_customer_data_{country}",
        python_callable=send_platform_customer_data,
        op_kwargs={"country": country, "query": send_query},
        trigger_rule="all_success",
        dag=dag,
    )
    ingest_list.append(ingest_task)

chain(start, *insert_list, pause, dbt_refresh, *ingest_list, end)
