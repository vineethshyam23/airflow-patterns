"""
Airflow DAG: monthly FBO/NBO scoring export to an external event API.

Flow:
  1. WRITE_TRUNCATE scoring_data_export_today from multi-country UNION SQL
  2. Fan-out per-country delta ingest (Avro bulk)
  3. APPEND delta rows onto scoring_data_export_hist
  4. Expire superseded hist rows (_valid_flag = false)

Source (read-only):
  dags/etl_dana_scoring_data_export.py
  dags/horeca_digital/dana_scoring_query.py
  dags/horeca_digital/dana_scoring_export.py

A later production revision moved steps 1/3/4 into dbt Cloud jobs so hist
stays frozen while ingest runs. This sample keeps the original BigQuery
operator shape — the delta contract is the same either way.
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.helpers import chain

from scoring_export import send_scoring_data
from scoring_query import ScoringQuery

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
}

ENV_VAR_NAME = "env"
env = os.environ.get(ENV_VAR_NAME, Variable.get(ENV_VAR_NAME))

if env == "DEV":
    bigquery_conn_id = "bigquery_default_dev"
    project_name = "dwh_project_dev"
else:
    bigquery_conn_id = "bigquery_default"
    project_name = "dwh_project"

# PL has no country model in ScoringQuery.countries; send returns empty OK.
country_list = [
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
today_table = "scoring_data_export_today"
hist_table = "scoring_data_export_hist"

# 10th of each month at 00:55 — after monthly scoring models refresh.
schedule = "55 0 10 * *"

dag = DAG(
    dag_id="etl_scoring_data_export",
    default_args=default_args,
    schedule_interval=schedule,
    catchup=False,
    max_active_runs=1,
    tags=["scoring", "event-ingest", "fbo-nbo"],
    doc_md=(
        "Monthly FBO/NBO scoring snapshot → per-country Avro delta → "
        "SCD-style history table. See scoring_analytics/04-dana-scoring/."
    ),
)

insert_table_query = ScoringQuery.get_scoring_export_insert_query()
copy_table_query = ScoringQuery.get_scoring_export_hist_query(today_table, hist_table)
update_query = ScoringQuery.get_scoring_export_update_query(today_table, hist_table)

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
        }
    },
    gcp_conn_id=bigquery_conn_id,
    dag=dag,
)

ingest_list = []
for country in country_list:
    send_data_query = ScoringQuery.get_scoring_export_send_query(
        today_table, hist_table, country.lower()
    )
    ingest_list.append(
        PythonOperator(
            task_id=f"ingest_scoring_data_{country}",
            python_callable=send_scoring_data,
            op_kwargs={"country": country.lower(), "query": send_data_query},
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
