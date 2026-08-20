"""
Airflow DAG: weekly establishment-attribute Deepideas export.

One feed from the production Deepideas sibling set (establishment /
gaps_category / gap_ingredients / benchmarking_gaps). This DAG ships
only the establishment enrichment contract:

  insert today → Avro delta POST → append yesterday → soft-close

Sibling category / ingredient feeds are separate patterns. Peer
purchase gaps already shipped as pattern 16.

Source (read-only):
  dags/etl_dana_deep_ideas_export.py
  dags/horeca_digital/dana_deepideas_establishment_export.py
  dags/horeca_digital/dana_deepideas_query.py (Establishment)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

import delta_queries as dq
import establishment_export as export
import establishment_queries as eq

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
    dag_id="etl_deepideas_establishment_export",
    default_args=default_args,
    # Weekly after weekend refined refreshes. Establishment join is
    # heavy; do not stack catchup if Composer lags.
    schedule_interval="@weekly",
    catchup=False,
    max_active_runs=1,
    tags=["deepideas", "establishment", "avro", "event-export"],
    doc_md=(
        "Weekly hash-delta Avro export of establishment enrichment "
        "attributes for active wholesale buyers. See "
        "scoring_analytics/20-deepideas-establishment-export/."
    ),
)

PROJECT_ID = "dwh_project"
DATASET_STAGING = "staging"
GCP_CONN_ID = "bigquery_default"
# Single-market feed in production; keep ISO as a constant so a second
# country is an explicit change, not a silent loop.
EXPORT_COUNTRY = "de"

insert_today = BigQueryInsertJobOperator(
    task_id="insert_table_di_establishment_export_today",
    configuration={
        "query": {
            "query": eq.insert_today_query(),
            "useLegacySql": False,
            "writeDisposition": "WRITE_TRUNCATE",
            "createDisposition": "CREATE_IF_NEEDED",
            "allowLargeResults": True,
            "destinationTable": {
                "projectId": PROJECT_ID,
                "datasetId": DATASET_STAGING,
                "tableId": "di_establishment_export_today",
            },
        }
    },
    gcp_conn_id=GCP_CONN_ID,
    dag=dag,
)

ingest_data = PythonOperator(
    task_id="ingest_di_establishment_data",
    python_callable=export.send_establishment_data,
    op_kwargs={"country": EXPORT_COUNTRY, "query": dq.send_data_query()},
    dag=dag,
)

copy_yesterday = BigQueryInsertJobOperator(
    task_id="copy_table_di_establishment_export_yesterday",
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
                "tableId": "di_establishment_export_yesterday",
            },
        }
    },
    gcp_conn_id=GCP_CONN_ID,
    dag=dag,
)

update_yesterday = BigQueryInsertJobOperator(
    task_id="update_table_di_establishment_export_yesterday",
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
