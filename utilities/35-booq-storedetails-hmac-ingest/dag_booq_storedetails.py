"""Daily POS vendor store-details: HMAC CSV → GCS repair → staging → dbt.

Fetches establishment / product-activation flags from a vendor webservice
authenticated with a date-bound HMAC-MD5 signature, lands a repaired CSV
on the Composer data volume, copies to rawzone, normalizes column counts
in GCS, loads BigQuery staging (TRUNCATE), then runs one dbt Cloud job.

Distinct from payment KYC export (pattern 11) and Adyen terminal ingest
(pattern 03): this is master-data for POS establishments, not payments.

Source (read-only):
  dags/etl_booq_storedetails.py
  dags/horeca_digital/booq_storedetails.py
"""

from __future__ import annotations

import os
from datetime import date, datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.utils.helpers import chain

from storedetails_api import main as fetch_storedetails
from storedetails_api import repair_csv_in_gcs

try:
    from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
except ImportError:  # pragma: no cover - reference stub
    DbtCloudRunJobOperator = None

# Parse-time stamp (production behaviour). Prefer {{ ds }} for backfills.
TODAY = date.today().strftime("%Y-%m-%d")

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "start_date": datetime(2021, 4, 3),
    "email": ["dataops@example.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "account_id": 1,
}

environment = "env"
env = os.environ.get(environment, Variable.get(environment, default_var="DEV"))

if env == "DEV":
    bucket_name = "rawzone_dev"
    projectid = "dwh_project_dev"
    gcp_conn_id = "google_cloud_dev"
else:
    bucket_name = "rawzone"
    projectid = "dwh_project"
    gcp_conn_id = "google_cloud_default"

COMPOSER_BUCKET = Variable.get("composer_bucket", default_var="composer-data")
ENDPOINT_URL = Variable.get(
    "vendor_storedetails_endpoint",
    default_var="https://vendor.example.com/webservice/getStoreDetails.aspx",
)

# JSON Variable: {"hmac_key": "..."}. Empty default keeps DAG importable.
_hmac_cfg = Variable.get(
    "vendor_storedetails_hmac_key",
    default_var="{}",
    deserialize_json=True,
)
AUTH_KEY = (_hmac_cfg or {}).get("hmac_key", "")

try:
    DBT_JOB_ID = Variable.get("booq_storedetails_dbt_job_id")
except KeyError:
    DBT_JOB_ID = None

dag = DAG(
    dag_id="etl_booq_storedetails",
    default_args=default_args,
    schedule_interval="0 7 * * *",
    dagrun_timeout=timedelta(minutes=25),
    catchup=False,
    max_active_runs=1,
    tags=["pos", "storedetails", "hmac", "daily"],
    doc_md=__doc__,
)

start = EmptyOperator(task_id="start", dag=dag)
stage_1 = EmptyOperator(task_id="stage_1", dag=dag)
stage_2 = EmptyOperator(task_id="stage_2", dag=dag)
end = EmptyOperator(task_id="end", dag=dag)

fetch_data = PythonOperator(
    task_id="fetch_storedetails",
    python_callable=fetch_storedetails,
    execution_timeout=timedelta(minutes=10),
    op_kwargs={
        "key": AUTH_KEY,
        "endpoint_url": ENDPOINT_URL,
        "as_of": TODAY,
    },
    dag=dag,
)

raw_object = f"vendor_storedetails/booq_storedetails_{TODAY}.csv"

file_to_bucket = GCSToGCSOperator(
    task_id="upload_storage_storedetails",
    source_bucket=COMPOSER_BUCKET,
    source_object=f"data/booq_storedetails/{TODAY}.csv",
    destination_bucket=bucket_name,
    destination_object=raw_object,
    gcp_conn_id=gcp_conn_id,
    dag=dag,
)

repair_csv_before_load = PythonOperator(
    task_id="repair_csv_storedetails",
    python_callable=repair_csv_in_gcs,
    op_kwargs={
        "bucket_name": bucket_name,
        "object_key": raw_object,
    },
    dag=dag,
)

bucket_to_bq = GCSToBigQueryOperator(
    task_id="load_data_storedetails",
    gcp_conn_id=gcp_conn_id,
    bucket=bucket_name,
    source_format="CSV",
    field_delimiter=",",
    skip_leading_rows=1,
    source_objects=[raw_object],
    destination_project_dataset_table=(
        f"{projectid}.trusted_staging.booq_storedetails"
    ),
    schema_object="schema_json/booq_storedetails.json",
    create_disposition="CREATE_IF_NEEDED",
    write_disposition="WRITE_TRUNCATE",
    dag=dag,
)

chain(
    start,
    fetch_data,
    file_to_bucket,
    repair_csv_before_load,
    bucket_to_bq,
    stage_1,
)

if DbtCloudRunJobOperator is not None and DBT_JOB_ID:
    booq_storedetails_dbt = DbtCloudRunJobOperator(
        task_id="booq_storedetails_dbt",
        dbt_cloud_conn_id="dbt_conn",
        job_id=int(DBT_JOB_ID),
        check_interval=10,
        do_xcom_push=True,
        dag=dag,
    )
else:
    booq_storedetails_dbt = EmptyOperator(
        task_id="booq_storedetails_dbt",
        dag=dag,
    )


def get_runids(ti):
    """Persist dbt Cloud run id for ops follow-up. No-op when stubbed."""
    if DbtCloudRunJobOperator is None or not DBT_JOB_ID:
        return []

    runids = []
    try:
        runids.append(
            ti.xcom_pull(task_ids=["booq_storedetails_dbt"], key="return_value")[0]
        )
    except (IndexError, TypeError):
        job_url = ti.xcom_pull(
            task_ids=["booq_storedetails_dbt"], key="job_run_url"
        )
        if not job_url:
            return []
        return_lst = list(filter(None, job_url[0].split("/")))
        runids.append(int(return_lst[-1]))

    Variable.set(key="etl_booq_storedetails_dbt_runids", value=runids)
    return runids


get_runids_task = PythonOperator(
    task_id="get_runids_task",
    python_callable=get_runids,
    dag=dag,
)

chain(stage_1, booq_storedetails_dbt, stage_2, get_runids_task, end)
