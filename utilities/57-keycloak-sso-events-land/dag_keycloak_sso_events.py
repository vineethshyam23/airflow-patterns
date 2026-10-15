"""Composer DAG: Keycloak SSO event land → staging → trusted append.

Daily job that picks up the identity platform's Keycloak event export
(a single ``.tar.gz`` dropped into a backup bucket), unpacks it on the
Composer worker via the GCS FUSE data mount, stages the CSV in the DWH
rawzone, loads staging with WRITE_TRUNCATE, then APPENDS into trusted
``sso_events`` with standard DWH lineage columns.

Production quirk preserved on purpose: ``loaddate`` is computed at DAG
*parse* time as yesterday, not from ``{{ ds }}``. That matched how the
upstream backup job named the object, but it means a mid-day re-parse
or a backfill from a different calendar day can target the wrong file.
Documented in DATA_FLOW.md; prefer templating if you rebuild this.

Source (read-only): ``dags/etl_sso.py``
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "start_date": datetime(2018, 10, 18),
    "email": ["dataops@example.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    # Production ran with retries=0. Fragile for a six-hop chain; keep
    # the behaviour visible and call it out in docs rather than silently
    # "fixing" the sample.
    "retries": 0,
    "retry_delay": timedelta(minutes=10),
}

PROJECT_ID = Variable.get("dwh_project_id", default_var="dwh_project")
COMPOSER_BUCKET = Variable.get("composer_bucket", default_var="composer-data")
BACKUP_BUCKET = Variable.get(
    "identity_backup_bucket", default_var="identity-backups"
)
RAWZONE_BUCKET = Variable.get("rawzone_bucket", default_var="rawzone")
STAGING_DATASET = "trusted_staging"
TRUSTED_DATASET = "trusted"
SOURCE_OBJECT = Variable.get(
    "sso_events_source_object", default_var="keycloak_events"
)
WORKER_DATA_DIR = "/home/airflow/gcs/data/sso"

# Parse-time yesterday — mirrors production. Prefer macros.ds_add(ds, -1)
# when rebuilding so backfills and re-parses stay execution-relative.
_LOAD_DATE = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")

_SQL_PATH = Path(__file__).with_name("trusted_append.sql")


def _trusted_append_sql() -> str:
    raw = _SQL_PATH.read_text(encoding="utf-8")
    return (
        raw.replace("{{ project_id }}", PROJECT_ID)
        .replace("{{ staging_dataset }}", STAGING_DATASET)
    )


with DAG(
    dag_id="etl_sso_events",
    default_args=default_args,
    schedule_interval="0 7 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["sso", "keycloak", "identity"],
) as dag:

    tar_name = f"{_LOAD_DATE}-kc-events-table.csv.tar.gz"
    csv_name = f"{_LOAD_DATE}-kc-events-table.csv"
    composer_tar = f"data/sso/{tar_name}"
    composer_csv = f"data/sso/{csv_name}"
    rawzone_csv = f"sso/events/{_LOAD_DATE}/events.csv"

    copy_sso_file = GCSToGCSOperator(
        task_id="copy_sso_file",
        gcp_conn_id="google_cloud_default",
        source_bucket=BACKUP_BUCKET,
        source_object=SOURCE_OBJECT,
        destination_bucket=COMPOSER_BUCKET,
        destination_object=composer_tar,
    )

    # Composer mounts the data/ prefix locally. Unpack there so the
    # next GCSToGCS can re-publish a plain CSV into the rawzone.
    gunzip_sso_file = BashOperator(
        task_id="gunzip_sso_file",
        bash_command=(
            f"tar -xzf {WORKER_DATA_DIR}/{tar_name} -C {WORKER_DATA_DIR}/"
        ),
    )

    remove_gz_file = BashOperator(
        task_id="remove_gz_file",
        bash_command=f"rm -f {WORKER_DATA_DIR}/{tar_name}",
    )

    sso_file_to_bucket = GCSToGCSOperator(
        task_id="sso_file_to_bucket",
        gcp_conn_id="google_cloud_default",
        source_bucket=COMPOSER_BUCKET,
        source_object=composer_csv,
        destination_bucket=RAWZONE_BUCKET,
        destination_object=rawzone_csv,
    )

    load_sso_data = GCSToBigQueryOperator(
        task_id="load_sso_data",
        gcp_conn_id="google_cloud_default",
        bucket=RAWZONE_BUCKET,
        source_format="CSV",
        field_delimiter=",",
        max_bad_records=100,
        skip_leading_rows=1,
        source_objects=[rawzone_csv],
        destination_project_dataset_table=f"{STAGING_DATASET}.sso_events",
        schema_object="schema_json/sso_events.json",
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
    )

    data_insert_sso = BigQueryInsertJobOperator(
        task_id="data_insert_sso",
        gcp_conn_id="bigquery_default",
        configuration={
            "query": {
                "query": _trusted_append_sql(),
                "useLegacySql": False,
                "writeDisposition": "WRITE_APPEND",
                "createDisposition": "CREATE_IF_NEEDED",
                "allowLargeResults": True,
                "destinationTable": {
                    "projectId": PROJECT_ID,
                    "datasetId": TRUSTED_DATASET,
                    "tableId": "sso_events",
                },
            }
        },
    )

    (
        copy_sso_file
        >> gunzip_sso_file
        >> remove_gz_file
        >> sso_file_to_bucket
        >> load_sso_data
        >> data_insert_sso
    )
