"""Daily Medallia survey feedback → GCS CSV → BQ staging → SCD Type 2.

Six sequential tasks. GraphQL extract lands a headerless CSV under
rawzone, staging is truncate-loaded, then inline BigQuery SQL appends
new/changed hash pairs and closes obsolete rows inside a 366-day
window before writing the tmp snapshot back to trusted.

Production hardcoded a single GCP project (no DEV/PROD branch) and
computed `loaddate` / `oldest_record_allowed` at DAG parse time. This
sample keeps that behaviour but documents the backfill risk.

Source (read-only):
  dags/etl_medallia.py
  dags/horeca_digital/medallia.py
"""

from __future__ import annotations

from datetime import date, datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.transfers.bigquery_to_bigquery import (
    BigQueryToBigQueryOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)

from extract_medallia import extract_data

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "start_date": datetime(2020, 9, 15),
    "email": ["dataops@example.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}

dag = DAG(
    dag_id="etl_medallia",
    default_args=default_args,
    schedule_interval="0 5 * * *",
    catchup=False,
    tags=["medallia", "nps", "feedback", "scd2"],
    doc_md=__doc__,
)

# Parse-time dates — same quirk as production. Prefer {{ ds }} on rewrite.
LOAD_DATE = date.today().strftime("%Y-%m-%d")
REQUEST_PERIOD_DAYS = 366
OLDEST_RECORD_ALLOWED = datetime.now().date() - timedelta(
    days=REQUEST_PERIOD_DAYS
)

BUCKET_NAME = Variable.get("medallia_rawzone_bucket", default_var="rawzone")
PROJECT_ID = Variable.get("medallia_gcp_project", default_var="dwh_project")
GCP_CONN_ID = Variable.get(
    "medallia_gcp_conn_id", default_var="bigquery_default"
)
GCS_CONN_ID = Variable.get(
    "medallia_gcs_conn_id", default_var="google_cloud_default"
)

try:
    MEDALLIA_CREDS = Variable.get("medallia_creds", deserialize_json=True)
except KeyError:
    MEDALLIA_CREDS = {"client_id": "", "client_secret": ""}

TRUSTED_TABLE = "trusted.medallia_feedback_record"
STAGING_TABLE = "trusted_staging.medallia_feedback_record"
TMP_TABLE = "trusted_staging.tmp_medallia_feedback_record"
SCHEMA_OBJECT = "schema_json/medallia_feedback_record.json"

extract_from_medallia = PythonOperator(
    task_id="extract_from_medallia",
    python_callable=extract_data,
    op_kwargs={
        "destination_bucket": BUCKET_NAME,
        "destination_file": f"medallia/medallia_{LOAD_DATE}.csv",
        "oldest_record_allowed": OLDEST_RECORD_ALLOWED,
        "creds": MEDALLIA_CREDS,
        "gcp_project": PROJECT_ID,
    },
    dag=dag,
)

# Snapshot trusted → tmp before mutating hashes.
copy_table_tmp = BigQueryToBigQueryOperator(
    task_id="copy_table_tmp",
    source_project_dataset_tables=TRUSTED_TABLE,
    destination_project_dataset_table=TMP_TABLE,
    write_disposition="WRITE_TRUNCATE",
    create_disposition="CREATE_IF_NEEDED",
    gcp_conn_id=GCP_CONN_ID,
    dag=dag,
)

data_load_staging = GCSToBigQueryOperator(
    task_id="load_staging",
    gcp_conn_id=GCS_CONN_ID,
    bucket=BUCKET_NAME,
    source_format="CSV",
    source_objects=[f"medallia/medallia_{LOAD_DATE}.csv"],
    destination_project_dataset_table=STAGING_TABLE,
    schema_object=SCHEMA_OBJECT,
    create_disposition="CREATE_IF_NEEDED",
    write_disposition="WRITE_TRUNCATE",
    dag=dag,
)

# Append rows whose (keyhash, rowhash) pair is not currently valid.
# Fixed missing comma after english_translation_promoter_reason_comment
# that existed in the production SELECT list.
data_insert = BigQueryInsertJobOperator(
    task_id="data_insert",
    configuration={
        "query": {
            "query": f"""
                SELECT
                    establishment_id,
                    user_country_iso_code,
                    user_language_iso_code,
                    product_name,
                    CAST(
                        (CASE WHEN nps_value = '' THEN NULL ELSE nps_value END)
                        AS INT64
                    ) AS nps_value,
                    promoter_reason_alt,
                    promoter_reason_comment,
                    english_translation_promoter_reason_comment,
                    detractor_reason_alt,
                    detractor_reason_comment,
                    english_translation_detractor_reason_comment,
                    additional_comment,
                    response_date,
                    churn_initial_choice_alt,
                    churn_leaving_choice_alt,
                    churn_leaving_choice_comment,
                    english_translation_churn_leaving_choice_cmt,
                    churn_willingness_call_alt,
                    churn_willingness_stay_alt,
                    downgrade_main_reason_alt,
                    downgrade_main_reason_other_comment,
                    english_translation_downgrade_main_reason_other_cmt,
                    english_translation_additional_comment_cmt,
                    unique_survey_id,
                    text_custom_parameter,
                    _create_ts,
                    _update_ts,
                    _job_name,
                    _job_id,
                    _sourcesystem,
                    _keyhash,
                    _rowhash,
                    TIMESTAMP(
                        FORMAT_TIMESTAMP(
                            '%Y-%m-%d %H:00:00', CURRENT_TIMESTAMP()
                        )
                    ) AS _valid_from,
                    TIMESTAMP('2099-12-31 00:00:00') AS _valid_until,
                    TRUE AS _valid_flag
                FROM `{STAGING_TABLE}`
                WHERE CONCAT(_keyhash, _rowhash) NOT IN (
                    SELECT CONCAT(_keyhash, _rowhash)
                    FROM `{TRUSTED_TABLE}`
                    WHERE _valid_flag = TRUE
                      AND _sourcesystem = 'Medallia'
                )
            """,
            "useLegacySql": False,
            "destinationTable": {
                "projectId": PROJECT_ID,
                "datasetId": "trusted_staging",
                "tableId": "tmp_medallia_feedback_record",
            },
            "writeDisposition": "WRITE_APPEND",
            "createDisposition": "CREATE_IF_NEEDED",
        }
    },
    gcp_conn_id=GCP_CONN_ID,
    dag=dag,
)

# Close currently-valid rows inside the lookback window that vanished
# from today's staging extract (changed survey fields or removed).
oldest_str = OLDEST_RECORD_ALLOWED.strftime("%Y-%m-%d")
data_update = BigQueryInsertJobOperator(
    task_id="data_update",
    configuration={
        "query": {
            "query": f"""
                UPDATE `{TMP_TABLE}`
                SET
                    _update_ts = CURRENT_TIMESTAMP(),
                    _valid_until = TIMESTAMP_SUB(
                        TIMESTAMP(
                            FORMAT_TIMESTAMP(
                                '%Y-%m-%d %H:00:00', CURRENT_TIMESTAMP()
                            )
                        ),
                        INTERVAL 1 SECOND
                    ),
                    _valid_flag = FALSE
                WHERE _valid_flag = TRUE
                  AND response_date > '{oldest_str}'
                  AND _sourcesystem = 'Medallia'
                  AND CONCAT(_keyhash, _rowhash) NOT IN (
                      SELECT CONCAT(_keyhash, _rowhash)
                      FROM `{STAGING_TABLE}`
                  )
            """,
            "useLegacySql": False,
        }
    },
    gcp_conn_id=GCP_CONN_ID,
    dag=dag,
)

copy_table = BigQueryToBigQueryOperator(
    task_id="copy_table",
    source_project_dataset_tables=TMP_TABLE,
    destination_project_dataset_table=TRUSTED_TABLE,
    write_disposition="WRITE_TRUNCATE",
    create_disposition="CREATE_IF_NEEDED",
    gcp_conn_id=GCP_CONN_ID,
    dag=dag,
)

(
    extract_from_medallia
    >> copy_table_tmp
    >> data_load_staging
    >> data_insert
    >> data_update
    >> copy_table
)
