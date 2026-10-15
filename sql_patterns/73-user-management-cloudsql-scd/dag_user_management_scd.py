"""User Management: Cloud SQL MySQL → GCS CSV → BigQuery SCD Type 2.

Daily identity/access land for a microservice user-management database.
Each table runs a nine-step chain: echo → schedule-aware Cloud SQL
export → dual-bucket copy → staging CSV load → first-run bootstrap →
trusted snapshot to tmp → expire obsolete hashes → append new/changed
→ promote tmp to trusted.

Exports are chained serially (Cloud SQL Admin one-op-at-a-time) with
elevated retries and a backup-window aware delay. Semicolon CSV +
inline schema_fields match the European MySQL export contract.

Distinct from:
- pattern 27 (Offer Tool SCD) — different surrogate / bootstrap / CSV
- pattern 39 (Hydra weekly full dump) — no SCD in the DAG
- pattern 54 (Reservation Tool) — id-watermark + dbt historization
- pattern 56 (food-order shards) — master + N-shard merge, not one UM DB

Source (read-only):
  dags/etl_user_management.py
  dags/horeca_digital/operators/cloudsql_retry_operator.py
"""

from __future__ import annotations

import functools
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.bigquery_to_bigquery import (
    BigQueryToBigQueryOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.utils.trigger_rule import TriggerRule
from google.cloud import bigquery

from cloudsql_export_operator import CloudSqlExportOperatorWithScheduleAware
from um_table_config import EXPORT_QUERIES, TABLE_NAMES, schema_fields

logger = logging.getLogger(__name__)

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "email": ["dataops@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=3),
}

PROJECT_ID = Variable.get("dwh_project_id", default_var="dwh_project")
SOURCE_DB = Variable.get("um_cloudsql_database", default_var="identity_um")
SOURCE_PROJECT = Variable.get(
    "um_cloudsql_project", default_var="identity-microservices-prod"
)
SOURCE_INSTANCE = Variable.get(
    "um_cloudsql_instance", default_var="db-prod-mysql-identity-master"
)
BUCKET_EXPORT = Variable.get(
    "um_export_bucket", default_var="db-export-identity-microservices-prod"
)
BUCKET_RAW = Variable.get("um_raw_bucket", default_var="dwh-rawzone")

DATASET = "trusted"
DATASET_STAGING = "trusted_staging"
PREFIX = "um_"
TIMESTAMP_FOLDER = "000000"

BACKUP_START_HOUR = int(
    Variable.get("um_backup_window_start_hour", default_var="3")
)
BACKUP_DURATION_HOURS = int(
    Variable.get("um_backup_window_duration_hours", default_var="2")
)

dag = DAG(
    dag_id="etl_user_management",
    default_args=default_args,
    schedule_interval="0 7 * * *",
    start_date=datetime(2024, 7, 1),
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=300),
    catchup=False,
    tags=["etl", "user-management", "identity", "cloudsql", "scd2"],
    doc_md=__doc__,
)


def echo_table(tablename: str, **_context) -> str:
    """Lightweight breadcrumb before each serial export."""
    logger.info(
        "UM export start table=%s export_bucket=%s raw_bucket=%s",
        tablename,
        BUCKET_EXPORT,
        BUCKET_RAW,
    )
    return tablename


def ensure_trusted_table(
    source_table: str,
    staging_table: str,
    **_context,
) -> None:
    """Bootstrap trusted table from staging schema on first run.

    Production closed this gap after cold-start failures: SCD update/
    insert assume a trusted table already exists. When it does not, copy
    staging columns and append ``_valid_flag`` / ``_valid_from`` /
    ``_valid_until``.
    """
    client = bigquery.Client(project=PROJECT_ID)
    source_fqn = f"{PROJECT_ID}.{source_table}"
    staging_fqn = f"{PROJECT_ID}.{staging_table}"

    try:
        client.get_table(source_fqn)
        logger.info("Trusted table exists: %s", source_fqn)
        return
    except Exception as exc:
        msg = str(exc).lower()
        if "not found" not in msg and "404" not in msg:
            raise

    logger.warning(
        "Trusted table missing; creating from staging schema: %s", source_fqn
    )
    try:
        staging_ref = client.get_table(staging_fqn)
    except Exception as staging_exc:
        raise RuntimeError(
            f"Cannot bootstrap {source_fqn}: staging {staging_fqn} missing"
        ) from staging_exc

    schema = list(staging_ref.schema)
    existing = {field.name for field in schema}
    for name, field_type in (
        ("_valid_flag", "BOOLEAN"),
        ("_valid_from", "TIMESTAMP"),
        ("_valid_until", "TIMESTAMP"),
    ):
        if name not in existing:
            schema.append(bigquery.SchemaField(name, field_type))

    client.create_table(bigquery.Table(source_fqn, schema=schema), exists_ok=True)
    logger.info("Created empty trusted table %s (%s cols)", source_fqn, len(schema))


def _gcs_object(tablename: str) -> str:
    # Production used parse-time date.today(); {{ ds }} keeps backfills honest.
    return (
        f"{SOURCE_DB}/{tablename}/{{{{ ds }}}}/{TIMESTAMP_FOLDER}/{tablename}.csv"
    )


last_export = None

for tablename in TABLE_NAMES:
    querytext = EXPORT_QUERIES[tablename]
    source_object = _gcs_object(tablename)
    export_uri = f"gs://{BUCKET_EXPORT}/{source_object}"

    trusted_short = f"{DATASET}.{PREFIX}{tablename}"
    staging_short = f"{DATASET_STAGING}.stg_{PREFIX}{tablename}"
    tmp_short = f"{DATASET_STAGING}.tmp_{PREFIX}{tablename}"

    echo = PythonOperator(
        task_id=f"_echo_{tablename}",
        python_callable=functools.partial(echo_table, tablename),
        trigger_rule=TriggerRule.ALL_DONE,
        dag=dag,
    )

    sql_export = CloudSqlExportOperatorWithScheduleAware(
        task_id=f"sql_export_{tablename}",
        body={
            "exportContext": {
                "uri": export_uri,
                "fileType": "CSV",
                "csvExportOptions": {
                    "selectQuery": querytext,
                    "fieldsTerminatedBy": "3B",  # semicolon
                },
                "databases": [SOURCE_DB],
            }
        },
        project_id=SOURCE_PROJECT,
        instance=SOURCE_INSTANCE,
        gcp_conn_id="bigquery_default",
        max_operation_retries=8,
        operation_retry_delay=600,
        backup_window_start_hour=BACKUP_START_HOUR,
        backup_window_duration_hours=BACKUP_DURATION_HOURS,
        dag=dag,
    )

    download = GCSToGCSOperator(
        task_id=f"download_file_{SOURCE_DB}_{tablename}",
        source_bucket=BUCKET_EXPORT,
        source_object=source_object,
        destination_bucket=BUCKET_RAW,
        destination_object=source_object,
        trigger_rule=TriggerRule.ALL_DONE,
        gcp_conn_id="google_cloud_default",
        dag=dag,
    )

    load_staging = GCSToBigQueryOperator(
        task_id=f"load_{tablename}",
        bucket=BUCKET_RAW,
        source_objects=[source_object],
        destination_project_dataset_table=f"{PROJECT_ID}.{staging_short}",
        schema_fields=schema_fields(tablename),
        source_format="CSV",
        skip_leading_rows=1,
        allow_quoted_newlines=True,
        allow_jagged_rows=True,
        ignore_unknown_values=True,
        max_bad_records=100,
        field_delimiter=";",
        quote_character='"',
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        gcp_conn_id="google_cloud_default",
        dag=dag,
    )

    bootstrap = PythonOperator(
        task_id=f"copy_table_{tablename}_stg",
        python_callable=ensure_trusted_table,
        op_kwargs={
            "source_table": trusted_short,
            "staging_table": staging_short,
        },
        dag=dag,
    )

    snapshot_tmp = BigQueryToBigQueryOperator(
        task_id=f"copy_table_{tablename}_tmp",
        source_project_dataset_tables=f"{PROJECT_ID}.{trusted_short}",
        destination_project_dataset_table=f"{PROJECT_ID}.{tmp_short}",
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
        gcp_conn_id="bigquery_default",
        dag=dag,
    )

    expire = BigQueryInsertJobOperator(
        task_id=f"update_{tablename}",
        configuration={
            "query": {
                "query": (
                    f"UPDATE `{PROJECT_ID}.{tmp_short}` "
                    f"SET _update_ts=current_timestamp, "
                    f"_valid_until=timestamp_sub("
                    f'timestamp(format_timestamp("%Y-%m-%d %H:00:00", current_timestamp)), '
                    f"interval 1 second), "
                    f"_valid_flag=False "
                    f"WHERE _valid_flag=True "
                    f"AND concat(_keyhash, _rowhash) NOT IN ("
                    f"  SELECT concat(_keyhash, _rowhash) "
                    f"  FROM `{PROJECT_ID}.{staging_short}`"
                    f")"
                ),
                "useLegacySql": False,
            }
        },
        gcp_conn_id="bigquery_default",
        dag=dag,
    )

    insert_new = BigQueryInsertJobOperator(
        task_id=f"insert_{tablename}",
        configuration={
            "query": {
                "query": (
                    f"SELECT *, "
                    f'timestamp(format_timestamp("%Y-%m-%d %H:00:00", current_timestamp)) '
                    f"AS _valid_from, "
                    f'timestamp("2099-12-31 00:00:00") AS _valid_until, '
                    f"TRUE AS _valid_flag "
                    f"FROM `{PROJECT_ID}.{staging_short}` "
                    f"WHERE concat(_keyhash, _rowhash) NOT IN ("
                    f"  SELECT concat(_keyhash, _rowhash) "
                    f"  FROM `{PROJECT_ID}.{trusted_short}` "
                    f"  WHERE _valid_flag=True"
                    f")"
                ),
                "useLegacySql": False,
                "writeDisposition": "WRITE_APPEND",
                "allowLargeResults": True,
                "destinationTable": {
                    "projectId": PROJECT_ID,
                    "datasetId": DATASET_STAGING,
                    "tableId": f"tmp_{PREFIX}{tablename}",
                },
            }
        },
        gcp_conn_id="bigquery_default",
        dag=dag,
    )

    promote = BigQueryToBigQueryOperator(
        task_id=f"copy_table_{tablename}",
        source_project_dataset_tables=f"{PROJECT_ID}.{tmp_short}",
        destination_project_dataset_table=f"{PROJECT_ID}.{trusted_short}",
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
        gcp_conn_id="bigquery_default",
        dag=dag,
    )

    if last_export is not None:
        last_export >> echo
    last_export = sql_export

    (
        echo
        >> sql_export
        >> download
        >> load_staging
        >> bootstrap
        >> snapshot_tmp
        >> expire
        >> insert_new
        >> promote
    )
