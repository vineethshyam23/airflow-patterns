"""
Airflow DAG: weekly Deepideas gap-ingredients export.

One feed from the production Deepideas sibling set (establishment /
gaps_category / gap_ingredients / benchmarking_gaps). This DAG ships
only the ingredient-gap contract:

  insert today → Avro delta POST → append yesterday → soft-close

Establishment attributes already shipped as pattern 20. Category-level
zero-purchase gaps are pattern 21. Peer purchase gaps are pattern 16.

Source (read-only):
  dags/etl_dana_deep_ideas_export.py
  dags/horeca_digital/dana_deepideas_gaps_ingredients_export.py
  dags/horeca_digital/dana_deepideas_query.py (GapIngredients)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

import delta_queries as dq
import gaps_ingredients_export as export
import gaps_ingredients_queries as gq

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

dag = DAG(
    dag_id="etl_deepideas_gaps_ingredients_export",
    default_args=default_args,
    # Weekly with the other Deepideas siblings. Ingredient grain is
    # denser than category; do not stack catchup if Composer lags.
    schedule_interval="@weekly",
    catchup=False,
    max_active_runs=1,
    tags=["deepideas", "gaps-ingredients", "avro", "event-export"],
    doc_md=(
        "Weekly hash-delta Avro export of menu-implied ingredients "
        "with no wholesale purchase revenue. See "
        "scoring_analytics/22-deepideas-gaps-ingredients-export/."
    ),
)

PROJECT_ID = "dwh_project"
DATASET_STAGING = "staging"
GCP_CONN_ID = "bigquery_default"
# Single-market feed in production; keep ISO as a constant so a second
# country is an explicit change, not a silent loop.
EXPORT_COUNTRY = "de"

insert_today = BigQueryInsertJobOperator(
    task_id="insert_table_di_gap_ingredients_export_today",
    configuration={
        "query": {
            "query": gq.insert_today_query(),
            "useLegacySql": False,
            "writeDisposition": "WRITE_TRUNCATE",
            "createDisposition": "CREATE_IF_NEEDED",
            "allowLargeResults": True,
            "destinationTable": {
                "projectId": PROJECT_ID,
                "datasetId": DATASET_STAGING,
                "tableId": "di_gap_ingredients_export_today",
            },
        }
    },
    gcp_conn_id=GCP_CONN_ID,
    dag=dag,
)

ingest_data = PythonOperator(
    task_id="ingest_di_gap_ingredients_data",
    python_callable=export.send_gaps_ingredients_data,
    op_kwargs={"country": EXPORT_COUNTRY, "query": dq.send_data_query()},
    dag=dag,
)

copy_yesterday = BigQueryInsertJobOperator(
    task_id="copy_table_di_gap_ingredients_export_yesterday",
    configuration={
        "query": {
            "query": dq.copy_yesterday_query(),
            "useLegacySql": False,
            "writeDisposition": "WRITE_APPEND",
            "createDisposition": "CREATE_IF_NEEDED",
            "allowLargeResults": True,
            "destinationTable": {
                "projectId": PROJECT_ID,
                "datasetId": DATASET_STAGING,
                "tableId": "di_gap_ingredients_export_yesterday",
            },
        }
    },
    gcp_conn_id=GCP_CONN_ID,
    dag=dag,
)

update_yesterday = BigQueryInsertJobOperator(
    task_id="update_table_di_gap_ingredients_export_yesterday",
    configuration={
        "query": {
            "query": dq.update_yesterday_query(),
            "useLegacySql": False,
        }
    },
    gcp_conn_id=GCP_CONN_ID,
    dag=dag,
)

insert_today >> ingest_data >> copy_yesterday >> update_yesterday
