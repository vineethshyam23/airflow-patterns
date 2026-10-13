"""ETL v2: POS partner Adyen reports → staging → dbt.

Runs alongside ``etl_adyen_report_land_v1`` (v1 stays on). Differences:

* **1-day window** — previous calendar day (``ds``). The 05:30 UTC run
  asks Adyen for yesterday's file; today's file is not published yet.
* **No CSV merge** — each file is uploaded and loaded on its own.
* **``file_name``** is stamped onto every staging row (original Adyen / drop
  basename) and passed through dbt so DA uses trusted models, not staging.
* **Schema JSON** on rawzone (same convention as Hydra / Reservation Tool):
  ``gs://<rawzone>/schema_json/adyen_report_v2_*.json``.
* Helper: ``adyen_report_land_v2.py`` (v1 helper stays separate).
* **Ensure sources** — ``ensure_dbt_source_tables`` creates empty
  ``trusted_staging.adyen_report_v2_*`` tables from schema JSON if they
  do not exist (loads still skipped when there is no file).

Staging tables: ``trusted_staging.adyen_report_v2_*``.
dbt models (stg → int) under tag ``adyen_report_v2``; job id from Variable
``adyen_report_v2_dbt_job_id``. DA reads ``trusted.int_adyen_report_v2_*``
(includes ``file_name``).
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.utils.helpers import chain

from adyen_report_land_v2 import (
    DEFAULT_ANALYTICS_DROP_BUCKET,
    DEFAULT_PLATFORM_MARGINS_LANDING_PREFIX,
    assert_gcs_csv_uploads,
    ensure_dbt_source_tables,
    fetch_received_payments_date_range_to_gcs,
    fetch_settlement_date_range_to_gcs,
    stage_drop_csvs_with_file_name,
)

try:
    from airflow.operators.empty import EmptyOperator
except ModuleNotFoundError:  # pragma: no cover - Airflow < 2.4 fallback
    from airflow.operators.dummy import DummyOperator as EmptyOperator  # type: ignore

ADYEN_ACCOUNT_NAME = "PartnerPayments"
ADYEN_ACCOUNT_TYPE = "Company"

# Previous calendar day. Cron 05:30 UTC: data_interval_end is "today",
# and Adyen does not have today's settlement/received file yet.
REPORT_DATE = "{{ ds }}"

SCHEMA_OBJECT_SETTLEMENT = "schema_json/adyen_report_v2_settlement_details.json"
SCHEMA_OBJECT_RECEIVED_PAYMENTS = (
    "schema_json/adyen_report_v2_received_payment_details.json"
)
SCHEMA_OBJECT_MARGINS = "schema_json/adyen_report_v2_platform_margins.json"

SETTLEMENT_OBJECT_NAME = "settlement_details.csv"
RECEIVED_PAYMENTS_OBJECT_NAME = "received_payment_details.csv"

ANALYTICS_DROP_BUCKET = Variable.get(
    "adyen_analytics_drop_bucket", default_var=DEFAULT_ANALYTICS_DROP_BUCKET
).strip()
MARGINS_DROP_PREFIX = Variable.get(
    "adyen_platform_margins_drop_prefix",
    default_var=DEFAULT_PLATFORM_MARGINS_LANDING_PREFIX,
).strip()

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "start_date": datetime(2026, 9, 7),
    "email": ["dataops@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "dbt_cloud_conn_id": "dbt_conn",
    "account_id": 3,
}

env = os.environ.get("env", Variable.get("env"))

if env == "DEV":
    bucket_name = "dp_dev_rawzone"
    projectid = "dwh_project_dev"
    gcp_conn_id = "google_cloud_dev"
    dbt_job_id = None
elif env == "PROD":
    bucket_name = "dp_rawzone"
    projectid = "dwh_project"
    gcp_conn_id = "google_cloud_default"
    dbt_job_id = Variable.get("adyen_report_v2_dbt_job_id", default_var="").strip() or None
else:
    raise ValueError(f"Unknown environment: {env}")

adyen_report_creds = Variable.get(
    "adyen_report_creds", deserialize_json=True
)

dag = DAG(
    dag_id="etl_adyen_report_land_v2",
    default_args=default_args,
    schedule_interval="30 5 * * *",
    dagrun_timeout=timedelta(hours=2),
    catchup=False,
    max_active_runs=1,
    tags=["etl", "adyen", "reports", "dbt"],
    doc_md=__doc__,
)

start = EmptyOperator(task_id="start", dag=dag)
ensure_sources = PythonOperator(
    task_id="ensure_dbt_source_tables",
    python_callable=ensure_dbt_source_tables,
    op_kwargs={
        "gcp_conn_id": gcp_conn_id,
        "project_id": projectid,
        "dataset_id": "trusted_staging",
        "location": "EU",
        "schema_bucket": bucket_name,
        "tables": [
            {
                "table_id": "adyen_report_v2_settlement_details",
                "schema_object": SCHEMA_OBJECT_SETTLEMENT,
            },
            {
                "table_id": "adyen_report_v2_received_payment_details",
                "schema_object": SCHEMA_OBJECT_RECEIVED_PAYMENTS,
            },
            {
                "table_id": "adyen_report_v2_platform_margins",
                "schema_object": SCHEMA_OBJECT_MARGINS,
            },
        ],
    },
    dag=dag,
)
stage_1 = EmptyOperator(
    task_id="stage_1",
    trigger_rule="none_failed_min_one_success",
    dag=dag,
)
end = EmptyOperator(task_id="end", trigger_rule="none_failed", dag=dag)

_gcs_to_bq = dict(
    gcp_conn_id=gcp_conn_id,
    source_format="CSV",
    field_delimiter=",",
    skip_leading_rows=1,
    quote_character='"',
    schema_object_bucket=bucket_name,
    create_disposition="CREATE_IF_NEEDED",
    write_disposition="WRITE_TRUNCATE",
    allow_quoted_newlines=True,
    ignore_unknown_values=True,
    allow_jagged_rows=True,
    dag=dag,
)

# --- Settlement: one daily file → rawzone → BQ ---
settlement_raw_prefix = "adyen/report_v2/settlement_details/{{ ds }}"

fetch_settlement = ShortCircuitOperator(
    task_id="fetch_settlement_details",
    python_callable=fetch_settlement_date_range_to_gcs,
    ignore_downstream_trigger_rules=False,
    op_kwargs={
        "creds": adyen_report_creds,
        "account_name": ADYEN_ACCOUNT_NAME,
        "account_type": ADYEN_ACCOUNT_TYPE,
        "start_date": REPORT_DATE,
        "end_date": REPORT_DATE,
        "dest_bucket": bucket_name,
        "dest_prefix": settlement_raw_prefix,
        "dest_object_name": SETTLEMENT_OBJECT_NAME,
        "add_file_name_column": True,
        "gcp_conn_id": gcp_conn_id,
    },
    execution_timeout=timedelta(hours=1),
    show_return_value_in_logs=False,
    dag=dag,
)

load_settlement = GCSToBigQueryOperator(
    task_id="load_staging_settlement_details",
    bucket=bucket_name,
    source_objects=[f"{settlement_raw_prefix}/{SETTLEMENT_OBJECT_NAME}"],
    destination_project_dataset_table=(
        f"{projectid}.trusted_staging.adyen_report_v2_settlement_details"
    ),
    schema_object=SCHEMA_OBJECT_SETTLEMENT,
    **_gcs_to_bq,
)

chain(start, ensure_sources, fetch_settlement, load_settlement, stage_1)

# --- Received payment details: one daily file → rawzone → BQ ---
received_payments_raw_prefix = "adyen/report_v2/received_payment_details/{{ ds }}"

fetch_received_payments = ShortCircuitOperator(
    task_id="fetch_received_payment_details",
    python_callable=fetch_received_payments_date_range_to_gcs,
    ignore_downstream_trigger_rules=False,
    op_kwargs={
        "creds": adyen_report_creds,
        "account_name": ADYEN_ACCOUNT_NAME,
        "account_type": ADYEN_ACCOUNT_TYPE,
        "start_date": REPORT_DATE,
        "end_date": REPORT_DATE,
        "dest_bucket": bucket_name,
        "dest_prefix": received_payments_raw_prefix,
        "dest_object_name": RECEIVED_PAYMENTS_OBJECT_NAME,
        "add_file_name_column": True,
        "gcp_conn_id": gcp_conn_id,
    },
    execution_timeout=timedelta(hours=1),
    show_return_value_in_logs=False,
    dag=dag,
)

load_received_payments = GCSToBigQueryOperator(
    task_id="load_staging_received_payment_details",
    bucket=bucket_name,
    source_objects=[
        f"{received_payments_raw_prefix}/{RECEIVED_PAYMENTS_OBJECT_NAME}"
    ],
    destination_project_dataset_table=(
        f"{projectid}.trusted_staging.adyen_report_v2_received_payment_details"
    ),
    schema_object=SCHEMA_OBJECT_RECEIVED_PAYMENTS,
    **_gcs_to_bq,
)

chain(ensure_sources, fetch_received_payments, load_received_payments, stage_1)

# --- Platform margins: stamp each drop file, load, then delete the drop ---
margins_key = "platform_margins"
margins_raw_prefix = f"adyen/report_v2/{margins_key}/{{{{ ds }}}}"

check_margins_upload = ShortCircuitOperator(
    task_id=f"check_{margins_key}_upload",
    python_callable=assert_gcs_csv_uploads,
    ignore_downstream_trigger_rules=False,
    op_kwargs={
        "bucket": ANALYTICS_DROP_BUCKET,
        "prefix": MARGINS_DROP_PREFIX,
        "gcp_conn_id": gcp_conn_id,
    },
    dag=dag,
)

stage_margins = PythonOperator(
    task_id=f"stage_{margins_key}_with_file_name",
    python_callable=stage_drop_csvs_with_file_name,
    op_kwargs={
        "source_bucket": ANALYTICS_DROP_BUCKET,
        "dest_bucket": bucket_name,
        "dest_prefix": margins_raw_prefix,
        "gcp_conn_id": gcp_conn_id,
        "source_task_id": f"check_{margins_key}_upload",
    },
    dag=dag,
)

load_margins = GCSToBigQueryOperator(
    task_id=f"load_staging_{margins_key}",
    bucket=bucket_name,
    source_objects=[f"{margins_raw_prefix}/*.csv"],
    destination_project_dataset_table=(
        f"{projectid}.trusted_staging.adyen_report_v2_platform_margins"
    ),
    schema_object=SCHEMA_OBJECT_MARGINS,
    **_gcs_to_bq,
)

# Leave the drop folder for v1 (`etl_adyen_report_land_v1`) to archive/delete.
chain(ensure_sources, check_margins_upload, stage_margins, load_margins, stage_1)

if dbt_job_id:
    run_dbt = DbtCloudRunJobOperator(
        task_id="adyen_report_v2_run_dbt_job",
        job_id=dbt_job_id,
        deferrable=True,  # run polling in the triggerer
        execution_timeout=timedelta(minutes=60),  # hard backstop for the whole task
        check_interval=30,  # triggerer poll interval (seconds)
        timeout=3600,
        do_xcom_push=True,
        reuse_existing_run=False,
        retry_from_failure=True,
        dag=dag,
    )
    chain(stage_1, run_dbt, end)
else:
    chain(stage_1, end)
