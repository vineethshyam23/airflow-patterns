"""SEO business listing (gzip NDJSON) → BigQuery.

GCS layout (gs://seo-listings-ingest/):
  uploads/           — landing (.json or .json.gz)
  archive_raw/       — vendor bytes preserved ({stem}.json.gz or {stem}.json)
  stg_to_load/       — uncompressed NDJSON pending BQ load (uncompressed_*.json)
  archive_ingested/  — flat archive after successful load (uncompressed_*.json)

Flow: ingest_all_uploads → load_seo_listings_to_bq → dbt_seo_listings
      → archive_all_stg_to_load

Manual trigger (schedule=None). Vendor dumps land in uploads/; ops kick
this DAG when a new dump is ready rather than polling on a cron.
"""

import os
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.utils.helpers import chain

from gcs_ingest import (
    PREFIX_STG_TO_LOAD,
    archive_all_stg_to_load,
    ingest_all_uploads,
)

# In production this is a custom DbtCloudRunJobOperator. Stubbed here so
# the DAG graph is readable without pulling the operator package.
try:
    from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
except ImportError:  # pragma: no cover - reference stub
    DbtCloudRunJobOperator = None

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "start_date": datetime(2026, 4, 1),
    "email": ["dataops@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "dbt_cloud_conn_id": "dbt_conn",
    "account_id": 1,
}

environment = "env"
env = os.environ.get(environment, Variable.get(environment))

INGEST_BUCKET = "seo-listings-ingest"

if env == "DEV":
    schema_bucket = "rawzone_dev"
    projectid = "dwh_project_dev"
    gcp_conn_id = "google_cloud_dev"
    dbt_job_id = None
else:
    schema_bucket = "rawzone"
    projectid = "dwh_project"
    gcp_conn_id = "google_cloud_default"
    # Production: set Airflow Variable seo_listings_dbt_job_id
    try:
        dbt_job_id = Variable.get("seo_listings_dbt_job_id")
    except KeyError:
        dbt_job_id = None

BQ_TABLE = "seo_business_listings"
SCHEMA_OBJECT = "schema_json/seo_business_listing.json"
STG_SOURCE_OBJECTS = [f"{PREFIX_STG_TO_LOAD}uncompressed_*.json"]

dag = DAG(
    dag_id="etl_seo_listings_ingestion",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["seo", "listings", "gcs"],
    doc_md=__doc__,
)

start = EmptyOperator(task_id="start", dag=dag)
end = EmptyOperator(task_id="end", dag=dag)

if dbt_job_id is not None and DbtCloudRunJobOperator is not None:
    dbt_seo_listings = DbtCloudRunJobOperator(
        task_id="dbt_seo_listings",
        job_id=int(dbt_job_id),
        check_interval=10,
        timeout=3600,
        dag=dag,
    )
else:
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

load_seo_listings_to_bq = GCSToBigQueryOperator(
    task_id="load_seo_listings_to_bq",
    gcp_conn_id=gcp_conn_id,
    bucket=INGEST_BUCKET,
    source_format="NEWLINE_DELIMITED_JSON",
    source_objects=STG_SOURCE_OBJECTS,
    destination_project_dataset_table=f"{projectid}.trusted_staging.{BQ_TABLE}",
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
    load_seo_listings_to_bq,
    dbt_seo_listings,
    archive_all_stg_to_load_task,
    end,
)
