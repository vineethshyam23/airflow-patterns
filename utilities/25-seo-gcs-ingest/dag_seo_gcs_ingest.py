"""
Airflow DAG: SEO business-listing dumps → GCS promote → BigQuery → dbt → archive.

Manual / on-demand schedule. Landing objects land under uploads/; this DAG:

  1. Stream-decompress / copy into stg_to_load/ and preserve vendor bytes in archive_raw/
  2. Load NDJSON from stg_to_load/ into BQ staging (WRITE_TRUNCATE)
  3. Trigger the dbt job that builds refined listing tables
  4. Move stg_to_load/ objects to archive_ingested/ only after load + dbt succeed

Distinct from pattern 18 (menu URL extraction from already-refined listings).
This is the vendor dump landing path into the warehouse.

Source (read-only):
  dags/etl_dataforseo_ingestion.py
  dags/horeca_digital/dataforseo_gcs_ingest.py
  dags/schema_json/dataforseo_business_listing.json
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.utils.helpers import chain

from seo_gcs_ingest import (
    PREFIX_STG_TO_LOAD,
    archive_all_stg_to_load,
    ingest_all_uploads,
)

try:
    from airflow.operators.empty import EmptyOperator
except ModuleNotFoundError:
    from airflow.operators.dummy import DummyOperator as EmptyOperator  # type: ignore


default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "start_date": datetime(2025, 4, 1),
    "email": ["dataops@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "dbt_cloud_conn_id": "dbt_conn",
    "account_id": Variable.get("dbt_cloud_account_id", default_var=1),
}

ENV = os.environ.get("env", Variable.get("env", default_var="DEV"))

INGEST_BUCKET = Variable.get(
    "seo_listing_ingest_bucket", default_var="dwh-seo-business-listing"
)
BQ_TABLE = "seo_establishments"
SCHEMA_OBJECT = "schema_json/seo_business_listing.json"
STG_SOURCE_OBJECTS = [f"{PREFIX_STG_TO_LOAD}uncompressed_*.json"]

if ENV == "DEV":
    schema_bucket = "data-platform-dev-rawzone"
    project_id = "dwh_project_dev"
    gcp_conn_id = "google_cloud_dev"
    dbt_job_id = Variable.get("dbt_seo_listings_job_id_dev", default_var="")
else:
    schema_bucket = "data-platform-rawzone"
    project_id = "dwh_project"
    gcp_conn_id = "google_cloud_default"
    dbt_job_id = Variable.get("dbt_seo_listings_job_id", default_var="")

dag = DAG(
    dag_id="etl_seo_business_listing_ingest",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["seo", "gcs-ingest", "staging", "dbt"],
    doc_md=(
        "SEO business-listing dumps → GCS promote → BQ staging → dbt → archive. "
        "Manual trigger. See utilities/25-seo-gcs-ingest/."
    ),
)

start = EmptyOperator(task_id="start", dag=dag)
end = EmptyOperator(task_id="end", dag=dag)

if dbt_job_id:
    dbt_seo_listings = DbtCloudRunJobOperator(
        task_id="dbt_seo_listings",
        job_id=int(dbt_job_id),
        check_interval=10,
        timeout=3600,
        dag=dag,
    )
else:
    # DEV / unset Variable: keep the graph shape without calling dbt Cloud.
    dbt_seo_listings = EmptyOperator(task_id="dbt_seo_listings", dag=dag)

ingest_all_uploads_task = PythonOperator(
    task_id="ingest_all_uploads",
    python_callable=ingest_all_uploads,
    op_kwargs={
        "bucket_name": INGEST_BUCKET,
        "gcp_conn_id": gcp_conn_id,
    },
    dag=dag,
)

load_seo_to_bq = GCSToBigQueryOperator(
    task_id="load_seo_to_bq",
    gcp_conn_id=gcp_conn_id,
    bucket=INGEST_BUCKET,
    source_format="NEWLINE_DELIMITED_JSON",
    source_objects=STG_SOURCE_OBJECTS,
    destination_project_dataset_table=f"{project_id}.staging.{BQ_TABLE}",
    schema_object=SCHEMA_OBJECT,
    schema_object_bucket=schema_bucket,
    create_disposition="CREATE_IF_NEEDED",
    write_disposition="WRITE_TRUNCATE",
    dag=dag,
)

archive_all_stg_to_load_task = PythonOperator(
    task_id="archive_all_stg_to_load",
    python_callable=archive_all_stg_to_load,
    op_kwargs={
        "bucket_name": INGEST_BUCKET,
        "gcp_conn_id": gcp_conn_id,
    },
    dag=dag,
)

chain(
    start,
    ingest_all_uploads_task,
    load_seo_to_bq,
    dbt_seo_listings,
    archive_all_stg_to_load_task,
    end,
)
