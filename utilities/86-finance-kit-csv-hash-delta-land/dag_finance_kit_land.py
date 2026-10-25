"""Multi-entity finance-kit CSV land: landing → rawzone → staging → hash-delta trusted.

Vendor drops one dated CSV per entity into the landing bucket. Composer
lists the object, copies it into a date-partitioned rawzone path, loads
staging with WRITE_TRUNCATE, appends unseen (_keyhash|_rowhash) pairs
into trusted, then deletes the landing object.

Distinct from SEO listing NDJSON land (pattern 25) and POS HMAC CSV
ingest (pattern 35): this is a pure operator fan-out over many entities
with in-DAG hash-delta SQL — no vendor API, no four-prefix archive
contract, no dbt step.

Source (read-only):
  dags/etl_liquikit.py
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.gcs import (
    GCSDeleteObjectsOperator,
    GCSListObjectsOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

from finance_kit_entities import ENTITIES, build_hash_delta_insert_sql

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "start_date": datetime(2021, 2, 15),
    "email": ["dataops@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

PROJECT_ID = Variable.get("dwh_project_id", default_var="dwh_project")
LANDING_BUCKET = Variable.get("landingzone_bucket", default_var="landingzone")
RAW_BUCKET = Variable.get("rawzone_bucket", default_var="rawzone")
STAGING_DATASET = Variable.get("staging_dataset", default_var="trusted_staging")
TRUSTED_DATASET = Variable.get("trusted_dataset", default_var="trusted")
GCP_CONN = Variable.get("gcp_conn_id", default_var="google_cloud_default")
BQ_CONN = Variable.get("bq_conn_id", default_var="bigquery_default")

# File date macro: production used tomorrow_ds with a 23:30 schedule so
# the overnight vendor drop is named for the next calendar day.
FILE_DS = "{{ tomorrow_ds }}"

dag = DAG(
    dag_id="etl_finance_kit",
    default_args=default_args,
    schedule_interval="30 23 * * *",
    max_active_runs=1,
    catchup=False,
    tags=["finance_kit", "gcs", "hash-delta", "daily"],
    doc_md=__doc__,
)

for entity in ENTITIES:
    landing_prefix = f"finance_kit/{entity}_{FILE_DS}.csv"
    raw_object = f"finance_kit/{entity}/{FILE_DS}/{entity}.csv"
    staging_table = f"{STAGING_DATASET}.finance_kit_{entity}"
    schema_object = f"schema_json/finance_kit_{entity}.json"

    list_landing = GCSListObjectsOperator(
        task_id=f"list_landing_{entity}",
        bucket=LANDING_BUCKET,
        prefix=landing_prefix,
        gcp_conn_id=GCP_CONN,
        dag=dag,
    )

    copy_to_raw = GCSToGCSOperator(
        task_id=f"copy_to_raw_{entity}",
        gcp_conn_id=GCP_CONN,
        source_bucket=LANDING_BUCKET,
        source_object=landing_prefix,
        destination_bucket=RAW_BUCKET,
        destination_object=raw_object,
        dag=dag,
    )

    load_staging = GCSToBigQueryOperator(
        task_id=f"load_staging_{entity}",
        gcp_conn_id=BQ_CONN,
        bucket=RAW_BUCKET,
        source_format="CSV",
        field_delimiter=";",
        skip_leading_rows=1,
        allow_quoted_newlines=True,
        source_objects=[raw_object],
        destination_project_dataset_table=f"{PROJECT_ID}.{staging_table}",
        schema_object=schema_object,
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        dag=dag,
    )

    insert_trusted = BigQueryInsertJobOperator(
        task_id=f"insert_trusted_{entity}",
        gcp_conn_id=GCP_CONN,
        configuration={
            "query": {
                "query": build_hash_delta_insert_sql(
                    project_id=PROJECT_ID,
                    staging_dataset=STAGING_DATASET,
                    trusted_dataset=TRUSTED_DATASET,
                    entity=entity,
                ),
                "useLegacySql": False,
                "createDisposition": "CREATE_IF_NEEDED",
                "writeDisposition": "WRITE_APPEND",
                "allowLargeResults": True,
                "destinationTable": {
                    "projectId": PROJECT_ID,
                    "datasetId": TRUSTED_DATASET,
                    "tableId": f"finance_kit_{entity}",
                },
            }
        },
        dag=dag,
    )

    remove_landing = GCSDeleteObjectsOperator(
        task_id=f"remove_landing_{entity}",
        bucket_name=LANDING_BUCKET,
        prefix=landing_prefix,
        gcp_conn_id=GCP_CONN,
        dag=dag,
    )

    # Landing delete only after trusted append — failed inserts leave
    # the drop file for retry. list_landing is a soft check (empty list
    # does not fail); missing objects fail on copy_to_raw.
    list_landing >> copy_to_raw >> load_staging >> insert_trusted >> remove_landing
