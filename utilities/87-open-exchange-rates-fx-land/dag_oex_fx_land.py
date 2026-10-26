"""Daily Open Exchange Rates land: API → nested JSON → staging → FX trusted.

Fetches USD-base latest.json, lands a nested JSONL snapshot in rawzone,
truncates staging, then appends currency rows with EUR- and USD-relative
factors. Idempotent on API timestamp — re-runs skip known snapshots.

Distinct from finance-kit CSV hash-delta (pattern 86) and AppFigures
weekly CSV API ingest (pattern 28): this is a single FX API call with
nested JSON staging and SQL-side EUR cross-rates, not multi-entity
CSV land or report-grain fan-out.

Source (read-only):
  dags/etl_oex.py
  dags/horeca_digital/exchangerates.py (legacy alternate extract)
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

from oex_extract import build_fx_insert_sql, extract_oex

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "start_date": datetime(2019, 6, 12),
    "email": ["dataops@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}

PROJECT_ID = Variable.get("dwh_project_id", default_var="dwh_project")
COMPOSER_BUCKET = Variable.get("composer_bucket", default_var="composer_bucket")
RAW_BUCKET = Variable.get("rawzone_bucket", default_var="rawzone")
STAGING_DATASET = Variable.get("staging_dataset", default_var="trusted_staging")
TRUSTED_DATASET = Variable.get("trusted_dataset", default_var="trusted")
OEX_CONN = Variable.get("oex_conn_id", default_var="api_openexchangerates")
GCP_CONN = Variable.get("gcp_conn_id", default_var="google_cloud_default")
BQ_CONN = Variable.get("bq_conn_id", default_var="bigquery_default")

# Production used date.today() at DAG parse time for the rawzone object
# name — that breaks backfills. Template with ds instead.
LOAD_DS = "{{ ds }}"
RAW_OBJECT = f"oex/oex_{LOAD_DS}.json"
STAGING_TABLE = f"{STAGING_DATASET}.oex_data"
SCHEMA_OBJECT = "schema_json/oex_data.json"

dag = DAG(
    dag_id="etl_oex",
    default_args=default_args,
    schedule_interval="0 5 * * *",
    catchup=False,
    tags=["fx", "oex", "exchange-rates", "daily"],
    doc_md=__doc__,
)

extract = PythonOperator(
    task_id="extract_oex",
    python_callable=extract_oex,
    op_kwargs={"conn_id": OEX_CONN},
    dag=dag,
)

copy_to_raw = GCSToGCSOperator(
    task_id="copy_to_raw",
    gcp_conn_id=GCP_CONN,
    source_bucket=COMPOSER_BUCKET,
    source_object="data/oex/oex.json",
    destination_bucket=RAW_BUCKET,
    destination_object=RAW_OBJECT,
    dag=dag,
)

load_staging = GCSToBigQueryOperator(
    task_id="load_staging",
    gcp_conn_id=BQ_CONN,
    bucket=RAW_BUCKET,
    source_format="NEWLINE_DELIMITED_JSON",
    source_objects=[RAW_OBJECT],
    destination_project_dataset_table=f"{PROJECT_ID}.{STAGING_TABLE}",
    schema_object=SCHEMA_OBJECT,
    create_disposition="CREATE_IF_NEEDED",
    write_disposition="WRITE_TRUNCATE",
    dag=dag,
)

insert_trusted = BigQueryInsertJobOperator(
    task_id="insert_trusted",
    gcp_conn_id=GCP_CONN,
    configuration={
        "query": {
            "query": build_fx_insert_sql(
                project_id=PROJECT_ID,
                staging_dataset=STAGING_DATASET,
                trusted_dataset=TRUSTED_DATASET,
            ),
            "useLegacySql": False,
            "writeDisposition": "WRITE_APPEND",
            "createDisposition": "CREATE_IF_NEEDED",
            "allowLargeResults": True,
            "destinationTable": {
                "projectId": PROJECT_ID,
                "datasetId": TRUSTED_DATASET,
                "tableId": "exchange_rates",
            },
        }
    },
    dag=dag,
)

extract >> copy_to_raw >> load_staging >> insert_trusted
