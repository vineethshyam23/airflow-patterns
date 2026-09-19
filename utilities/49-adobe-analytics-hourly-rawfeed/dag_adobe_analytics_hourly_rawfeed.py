"""Composer DAG: Adobe Analytics hourly Data Feed land → trusted → refined.

Schedule: :15 past every hour. Landing zone receives Adobe Data Feed
drops (``.tar.gz`` lookup packs + ``.tsv.gz`` hit files). Workers unpack
into the Composer data prefix, load TSV into staging, fan out lookup
tables, then append an enriched refined hit table with a one-day hit_id
dedupe window.

Source (read-only): ``dags/etl_aa_adobe_rawfeed_hourly.py``
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.utils.trigger_rule import TriggerRule

try:
    from airflow.operators.empty import EmptyOperator
except ModuleNotFoundError:  # Airflow 1.x fallback
    from airflow.operators.dummy import DummyOperator as EmptyOperator

from rawfeed_extract import process_tar_files, process_tsv_gz_files

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "start_date": datetime(2023, 6, 10),
    "email": ["dataops@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

PROJECT_ID = Variable.get("dwh_project_id", default_var="dwh_project")
DATASET_TRUSTED = "trusted"
DATASET_STAGING = "trusted_staging"
DATASET_REFINED = "refined"
COMPOSER_BUCKET = Variable.get("composer_bucket", default_var="composer-data")
SCHEMA_BUCKET = Variable.get("rawzone_bucket", default_var="rawzone")

# Report-suite stem used in Adobe Data Feed object names (sanitized).
REPORT_SUITE = "web_report_suite"
HIT_GLOB = f"data/analytics-rawfeed-hourly/new/01-{REPORT_SUITE}_*"
LOOKUP_TABLES = [
    "browser_type",
    "browser",
    "color_depth",
    "connection_type",
    "country",
    "event",
    "javascript_version",
    "languages",
    "operating_systems",
    "plugins",
    "referrer_type",
    "resolution",
    "search_engines",
]

_SQL_PATH = Path(__file__).with_name("refined_hit_query.sql")


def _load_refined_sql() -> str:
    raw = _SQL_PATH.read_text(encoding="utf-8")
    return (
        raw.replace("{{ project_id }}", PROJECT_ID)
        .replace("{{ staging }}", DATASET_STAGING)
        .replace("{{ refined }}", DATASET_REFINED)
    )


def _extract_tar(**_context):
    # Prefer Variable-backed buckets when running on Composer.
    source = Variable.get("aa_landing_bucket", default_var="landingzone")
    dest = Variable.get("composer_bucket", default_var=COMPOSER_BUCKET)
    return process_tar_files(source_bucket=source, dest_bucket_name=dest)


def _extract_tsv(**_context):
    source = Variable.get("aa_landing_bucket", default_var="landingzone")
    dest = Variable.get("composer_bucket", default_var=COMPOSER_BUCKET)
    return process_tsv_gz_files(source_bucket=source, dest_bucket_name=dest)


with DAG(
    dag_id="etl_aa_adobe_rawfeed_hourly",
    default_args=default_args,
    schedule_interval="15 * * * *",
    max_active_runs=1,
    catchup=False,
    tags=["adobe-analytics", "rawfeed", "hourly"],
) as dag:

    extract_tar_task = PythonOperator(
        task_id="extract_tar_gz_files",
        python_callable=_extract_tar,
    )

    extract_tsv_task = PythonOperator(
        task_id="extract_tsv_gz_files",
        python_callable=_extract_tsv,
    )

    load_staging_hit_data = GCSToBigQueryOperator(
        task_id="load_staging_hit_data",
        gcp_conn_id="bigquery_default",
        bucket=COMPOSER_BUCKET,
        source_format="CSV",
        field_delimiter="\t",
        source_objects=[HIT_GLOB],
        destination_project_dataset_table=f"{DATASET_STAGING}.aa_feed_hit_data",
        schema_object_bucket=SCHEMA_BUCKET,
        schema_object="schema_json/aa_feed_hit_data.json",
        create_disposition="CREATE_IF_NEEDED",
        ignore_unknown_values=True,
        allow_jagged_rows=True,
        allow_quoted_newlines=True,
        write_disposition="WRITE_TRUNCATE",
        max_bad_records=50000,
        quote_character="",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    load_hit_data = BigQueryInsertJobOperator(
        task_id="load_hit_data",
        configuration={
            "query": {
                "query": f"""
                SELECT
                  *,
                  CURRENT_TIMESTAMP() AS _create_ts,
                  TIMESTAMP(NULL) AS _update_ts,
                  'etl_aa_hourly_rawfeed' AS _job_name,
                  0 AS _job_id,
                  'Adobe Analytics' AS _sourcesystem,
                  CURRENT_TIMESTAMP() AS _valid_from,
                  TIMESTAMP('2099-12-31 00:00:00') AS _valid_until,
                  TRUE AS _valid_flag
                FROM `{PROJECT_ID}.{DATASET_STAGING}.aa_feed_hit_data`
                """,
                "destinationTable": {
                    "projectId": PROJECT_ID,
                    "datasetId": DATASET_TRUSTED,
                    "tableId": "aa_hit_data",
                },
                "writeDisposition": "WRITE_APPEND",
                "createDisposition": "CREATE_IF_NEEDED",
                "useLegacySql": False,
                "allowLargeResults": True,
            }
        },
        gcp_conn_id="bigquery_default",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    archive_hit_data_file = GCSToGCSOperator(
        task_id="archive_hit_data_file",
        gcp_conn_id="bigquery_default",
        source_bucket=COMPOSER_BUCKET,
        source_object=HIT_GLOB,
        destination_bucket=COMPOSER_BUCKET,
        destination_object=(
            f"data/analytics-rawfeed-hourly/new/processed/01-{REPORT_SUITE}_*"
        ),
        move_object=True,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    lookup_and_hit_data_end = EmptyOperator(task_id="lookup_and_hit_data_end")

    for table in LOOKUP_TABLES:
        load_staging = GCSToBigQueryOperator(
            task_id=f"load_staging_{table}_data",
            gcp_conn_id="bigquery_default",
            bucket=COMPOSER_BUCKET,
            source_format="CSV",
            field_delimiter="\t",
            source_objects=[
                f"data/analytics-rawfeed-hourly/new/{table}_{REPORT_SUITE}_*"
            ],
            destination_project_dataset_table=f"{DATASET_STAGING}.aa_feed_{table}",
            schema_fields=[
                {"name": "key", "mode": "NULLABLE", "type": "INTEGER"},
                {"name": "value", "mode": "NULLABLE", "type": "STRING"},
            ],
            create_disposition="CREATE_IF_NEEDED",
            ignore_unknown_values=True,
            allow_jagged_rows=True,
            allow_quoted_newlines=True,
            write_disposition="WRITE_TRUNCATE",
            max_bad_records=0,
            quote_character="",
            trigger_rule=TriggerRule.ALL_SUCCESS,
        )

        dedupe_staging = BigQueryInsertJobOperator(
            task_id=f"data_duplicate_removal_{table}_data",
            configuration={
                "query": {
                    "query": f"""
                    SELECT DISTINCT *
                    FROM `{PROJECT_ID}.{DATASET_STAGING}.aa_feed_{table}`
                    """,
                    "destinationTable": {
                        "projectId": PROJECT_ID,
                        "datasetId": DATASET_STAGING,
                        "tableId": f"aa_feed_{table}",
                    },
                    "writeDisposition": "WRITE_TRUNCATE",
                    "createDisposition": "CREATE_IF_NEEDED",
                    "useLegacySql": False,
                    "allowLargeResults": True,
                }
            },
            gcp_conn_id="bigquery_default",
            trigger_rule=TriggerRule.ALL_SUCCESS,
        )

        load_trusted = BigQueryInsertJobOperator(
            task_id=f"load_{table}_data",
            configuration={
                "query": {
                    "query": f"""
                    SELECT DISTINCT
                      *,
                      CURRENT_TIMESTAMP() AS _create_ts,
                      TIMESTAMP(NULL) AS _update_ts,
                      'etl_aa_hourly_rawfeed' AS _job_name,
                      0 AS _job_id,
                      'Adobe Analytics' AS _sourcesystem,
                      TO_HEX(MD5(CAST(key AS STRING))) AS _keyhash,
                      TO_HEX(
                        MD5(CONCAT(CAST(key AS STRING), CAST(value AS STRING)))
                      ) AS _rowhash,
                      CURRENT_TIMESTAMP() AS _valid_from,
                      TIMESTAMP('2099-12-31 00:00:00') AS _valid_until,
                      TRUE AS _valid_flag
                    FROM `{PROJECT_ID}.{DATASET_STAGING}.aa_feed_{table}`
                    """,
                    "destinationTable": {
                        "projectId": PROJECT_ID,
                        "datasetId": DATASET_TRUSTED,
                        "tableId": f"aa_{table}",
                    },
                    "writeDisposition": "WRITE_TRUNCATE",
                    "createDisposition": "CREATE_IF_NEEDED",
                    "useLegacySql": False,
                    "allowLargeResults": True,
                }
            },
            gcp_conn_id="bigquery_default",
            trigger_rule=TriggerRule.ALL_SUCCESS,
        )

        archive_lookup = GCSToGCSOperator(
            task_id=f"archive_{table}_data_file",
            gcp_conn_id="bigquery_default",
            source_bucket=COMPOSER_BUCKET,
            source_object=(
                f"data/analytics-rawfeed-hourly/new/{table}_{REPORT_SUITE}_*"
            ),
            destination_bucket=COMPOSER_BUCKET,
            destination_object=(
                f"data/analytics-rawfeed-hourly/new/processed/"
                f"{table}_{REPORT_SUITE}_*"
            ),
            move_object=True,
            trigger_rule=TriggerRule.ALL_SUCCESS,
        )

        (
            extract_tar_task
            >> load_staging
            >> dedupe_staging
            >> load_trusted
            >> archive_lookup
            >> lookup_and_hit_data_end
        )

    load_refined = BigQueryInsertJobOperator(
        task_id="load_adobe_feed_data",
        configuration={
            "query": {
                "query": _load_refined_sql(),
                "destinationTable": {
                    "projectId": PROJECT_ID,
                    "datasetId": DATASET_REFINED,
                    "tableId": "analytics_datafeed",
                },
                "writeDisposition": "WRITE_APPEND",
                "createDisposition": "CREATE_IF_NEEDED",
                "useLegacySql": False,
                "allowLargeResults": True,
            }
        },
        gcp_conn_id="bigquery_default",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    (
        extract_tsv_task
        >> load_staging_hit_data
        >> load_hit_data
        >> archive_hit_data_file
        >> lookup_and_hit_data_end
        >> load_refined
    )
