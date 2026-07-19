"""
Airflow DAG: daily Salesforce asset history export to an external event API.

Flow:
  1. WRITE_TRUNCATE sfdc_asset_history_today from refined SFDC asset snapshot
  2. Delta ingest (Avro bulk) for the pilot market
  3. APPEND delta rows onto sfdc_asset_history_hist
  4. Expire superseded hist rows (_valid_flag = false)

Source (read-only):
  dags/horeca_digital/archived/etl_dana_SFDC_asset_history_export.py
  dags/horeca_digital/dana_sfdc_asset_query.py
  dags/horeca_digital/dana_sfdc_asset_export.py

Same ordering constraint as the scoring export: hist stays frozen while
ingest runs, otherwise the delta SELECT returns empty.
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.helpers import chain

from sfdc_asset_export import send_sfdc_asset_history_data
from sfdc_asset_query import AssetQuery

try:
    from airflow.operators.empty import EmptyOperator
except ModuleNotFoundError:
    from airflow.operators.dummy import DummyOperator as EmptyOperator  # type: ignore

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "start_date": datetime(2024, 7, 22),
    "email": ["dataops@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}

ENV_VAR_NAME = "env"
env = os.environ.get(ENV_VAR_NAME, Variable.get(ENV_VAR_NAME))

if env == "DEV":
    bigquery_conn_id = "bigquery_default_dev"
    project_name = "dwh_project_dev"
else:
    bigquery_conn_id = "bigquery_default"
    project_name = "dwh_project"

# Pilot market. Country list stays a list so a second market can be added
# without reshaping the DAG wiring.
country_list = ["ES"]
today_table = "sfdc_asset_history_today"
hist_table = "sfdc_asset_history_hist"

# Daily at 03:40 — after overnight CRM → warehouse sync lands.
schedule = "40 3 * * *"

dag = DAG(
    dag_id="etl_sfdc_asset_history_export",
    default_args=default_args,
    schedule_interval=schedule,
    catchup=False,
    max_active_runs=1,
    tags=["salesforce", "crm", "event-ingest", "assets"],
    doc_md=(
        "Daily SFDC asset history snapshot → Avro delta → SCD-style history. "
        "See salesforce_integration/05-sfdc-asset-history-export/."
    ),
)

insert_table_query = AssetQuery.get_asset_export_insert_query()
send_data_query = AssetQuery.get_asset_export_send_query(today_table, hist_table)
copy_table_query = AssetQuery.get_asset_export_hist_query(today_table, hist_table)
update_query = AssetQuery.get_asset_export_update_query(today_table, hist_table)

start = EmptyOperator(task_id="start", dag=dag)
pause = EmptyOperator(task_id="pause", dag=dag)
end = EmptyOperator(task_id="end", dag=dag)

insert_table_export_today = BigQueryInsertJobOperator(
    task_id=f"insert_table_{today_table}",
    configuration={
        "query": {
            "query": insert_table_query,
            "useLegacySql": False,
            "destinationTable": {
                "projectId": project_name,
                "datasetId": "trusted_staging",
                "tableId": today_table,
            },
            "writeDisposition": "WRITE_TRUNCATE",
            "createDisposition": "CREATE_IF_NEEDED",
            "allowLargeResults": True,
        }
    },
    gcp_conn_id=bigquery_conn_id,
    dag=dag,
)

ingest_list = []
for country in country_list:
    ingest_list.append(
        PythonOperator(
            task_id=f"ingest_sfdc_asset_history_{country}",
            python_callable=send_sfdc_asset_history_data,
            op_kwargs={"query": send_data_query, "country": country.lower()},
            trigger_rule="all_success",
            dag=dag,
        )
    )

copy_table_export_hist = BigQueryInsertJobOperator(
    task_id=f"copy_table_{hist_table}",
    configuration={
        "query": {
            "query": copy_table_query,
            "useLegacySql": False,
            "destinationTable": {
                "projectId": project_name,
                "datasetId": "trusted_staging",
                "tableId": hist_table,
            },
            "writeDisposition": "WRITE_APPEND",
            "createDisposition": "CREATE_IF_NEEDED",
            "allowLargeResults": True,
        }
    },
    gcp_conn_id=bigquery_conn_id,
    trigger_rule="all_success",
    dag=dag,
)

update_table_export_hist = BigQueryInsertJobOperator(
    task_id=f"update_table_{hist_table}",
    configuration={
        "query": {
            "query": update_query,
            "useLegacySql": False,
        }
    },
    gcp_conn_id=bigquery_conn_id,
    trigger_rule="all_success",
    dag=dag,
)

# hist must stay at previous state while ingest runs — otherwise the delta
# query would see today's rows already in hist and send nothing.
chain(
    start,
    insert_table_export_today,
    pause,
    *ingest_list,
    copy_table_export_hist,
    update_table_export_hist,
    end,
)
