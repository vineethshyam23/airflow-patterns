"""Wholesale NL dual-source land — MCC API + CHD CSV → staging → dbt.

Three independent chains under one daily DAG:

1. Partner MCC customer base (OAuth2 paginated JSON → Composer → rawzone
   → single-column JSON staging → dbt).
2. Customer merge-request extract (parallel to 1; no dbt).
3. Competitive market (CHD) CSV land with ShortCircuit clean/validate,
   strict BigQuery load, archive to processed/, then a second dbt job.

Distinct from SEO NDJSON land (pattern 25) and POS HMAC CSV land
(pattern 35): this couples an authenticated mutation-window API pull
with an optional landing-zone CSV branch that must survive dirty
integer fields before a max_bad_records=0 load.

Source (read-only):
  dags/etl_makro_NL.py
  dags/horeca_digital/makro_customers_api.py
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.utils.trigger_rule import TriggerRule

import makro_customers_api as dg

try:
    from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
except ImportError:  # pragma: no cover - reference stub
    DbtCloudRunJobOperator = None

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "start_date": datetime(2021, 5, 1),
    "email": ["dataops@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "account_id": 1,
}

env = os.environ.get("env", Variable.get("env", default_var="DEV"))

if env == "DEV":
    bucket_name = Variable.get("rawzone_bucket_dev", default_var="rawzone_dev")
    projectid = Variable.get("dwh_gcp_project_dev", default_var="dwh_project_dev")
    gcp_conn_id = Variable.get("dwh_gcp_conn_dev", default_var="google_cloud_dev")
    dbt_cloud_conn_id = Variable.get("dbt_cloud_conn_id", default_var="dbt_conn")
    customer_dbt_job_id = Variable.get(
        "wholesale_nl_customer_dbt_job_id_dev", default_var=""
    )
    schedule = None
else:
    bucket_name = Variable.get("rawzone_bucket", default_var="rawzone")
    projectid = Variable.get("dwh_gcp_project", default_var="dwh_project")
    gcp_conn_id = Variable.get("dwh_gcp_conn", default_var="google_cloud_default")
    dbt_cloud_conn_id = Variable.get("dbt_cloud_conn_id", default_var="dbt_conn")
    customer_dbt_job_id = Variable.get(
        "wholesale_nl_customer_dbt_job_id", default_var=""
    )
    schedule = "0 7 * * *"

chd_dbt_job_id = Variable.get("chd_market_data_nl_dbt_job_id", default_var="")
chd_landing_bucket = Variable.get("chd_landing_bucket", default_var="landingzone-chd")
chd_file_glob = Variable.get("chd_file_glob", default_var="WHOLESALE_NL_*.csv")
chd_file_prefix = Variable.get("chd_file_prefix", default_var="WHOLESALE_NL")
composer_bucket = Variable.get("composer_bucket", default_var="composer-data")

CUSTOMER_DBT_TASK = "wholesale_customer_dbt"
CHD_DBT_TASK = "chd_market_data_NL_dbt"


def get_runids_customer(ti):
    runids = []
    try:
        runids.append(ti.xcom_pull(task_ids=[CUSTOMER_DBT_TASK], key="return_value")[0])
    except (IndexError, TypeError):
        url = ti.xcom_pull(task_ids=[CUSTOMER_DBT_TASK], key="job_run_url")
        if url and url[0]:
            runids.append(int(list(filter(None, url[0].split("/")))[-1]))
    Variable.set(key="etl_wholesale_nl_customer_dbt_runids", value=runids)
    return runids


def get_runids_chd(ti):
    runids = []
    try:
        runids.append(ti.xcom_pull(task_ids=[CHD_DBT_TASK], key="return_value")[0])
    except (IndexError, TypeError):
        url = ti.xcom_pull(task_ids=[CHD_DBT_TASK], key="job_run_url")
        if url and url[0]:
            runids.append(int(list(filter(None, url[0].split("/")))[-1]))
    Variable.set(key="etl_chd_runids", value=runids)
    return runids


run_date = "{{ ds }}"

# Production lived with dagrun_timeout=20m while dbt tasks allow 60m.
# That mismatch is a real footgun — documented in ARCHITECTURE / DATA_FLOW.
# Raised here to 60m so the reference graph can finish when dbt is slow.
dag = DAG(
    dag_id="etl_wholesale_nl_dual_source",
    default_args=default_args,
    schedule_interval=schedule,
    dagrun_timeout=timedelta(minutes=60),
    catchup=False,
    max_active_runs=1,
    tags=["wholesale", "nl", "mcc", "chd", "dual-source"],
    doc_md=__doc__,
)

data_fetch_customer_base = PythonOperator(
    task_id="data_fetch_customer_base",
    execution_timeout=timedelta(minutes=30),
    python_callable=dg.getdata,
    op_kwargs={"run_date": run_date},
    dag=dag,
)

data_fetch_merge_requests = PythonOperator(
    task_id="data_fetch_merge_requests",
    execution_timeout=timedelta(minutes=10),
    python_callable=dg.getdata_merge_requests,
    op_kwargs={"run_date": run_date},
    dag=dag,
)

file_to_bucket_customer_base = GCSToGCSOperator(
    task_id="upload_storage_customer_base",
    source_bucket=composer_bucket,
    source_object=f"data/wholesale/customer_NL/customer_base_{run_date}.json",
    destination_bucket=bucket_name,
    destination_object=f"wholesale/customer_base_NL/{run_date}/customer_base.json",
    gcp_conn_id=gcp_conn_id,
    dag=dag,
)

file_to_bucket_merge_requests = GCSToGCSOperator(
    task_id="upload_storage_merge_requests",
    source_bucket=composer_bucket,
    source_object=f"data/wholesale/customer_NL/merge_requests_{run_date}.json",
    destination_bucket=bucket_name,
    destination_object=f"wholesale/customer_base_NL/{run_date}/merge_requests.json",
    gcp_conn_id=gcp_conn_id,
    dag=dag,
)

# Production loads JSONL with source_format=CSV + tab delimiter into a
# single JSON column named `value`. Odd, but it works for opaque partner
# payloads and keeps schema drift out of the load step — dbt unnests.
bucket_to_bq_customers = GCSToBigQueryOperator(
    task_id="load_data_wholesale_customers",
    gcp_conn_id=gcp_conn_id,
    bucket=bucket_name,
    source_format="CSV",
    source_objects=[f"wholesale/customer_base_NL/{run_date}/customer_base.json"],
    skip_leading_rows=0,
    destination_project_dataset_table=(
        f"{projectid}.trusted_staging.wholesale_customer_NL"
    ),
    schema_fields=[{"name": "value", "type": "JSON", "mode": "NULLABLE"}],
    create_disposition="CREATE_IF_NEEDED",
    write_disposition="WRITE_TRUNCATE",
    trigger_rule="all_done",
    field_delimiter="\t",
    dag=dag,
)

bucket_to_bq_merge_requests = GCSToBigQueryOperator(
    task_id="load_data_merge_requests",
    gcp_conn_id=gcp_conn_id,
    bucket=bucket_name,
    source_format="NEWLINE_DELIMITED_JSON",
    source_objects=[f"wholesale/customer_base_NL/{run_date}/merge_requests.json"],
    destination_project_dataset_table=(
        f"{projectid}.trusted_staging.wholesale_customer_NL_merge_requests"
    ),
    schema_object="schema_json/wholesale_customer_merge_requests.json",
    create_disposition="CREATE_IF_NEEDED",
    write_disposition="WRITE_TRUNCATE",
    trigger_rule="all_done",
    dag=dag,
)

if DbtCloudRunJobOperator and customer_dbt_job_id:
    wholesale_customer_dbt = DbtCloudRunJobOperator(
        task_id=CUSTOMER_DBT_TASK,
        dbt_cloud_conn_id=dbt_cloud_conn_id,
        job_id=int(customer_dbt_job_id),
        deferrable=True,
        execution_timeout=timedelta(minutes=60),
        check_interval=30,
        do_xcom_push=True,
        dag=dag,
        timeout=3600,
    )
else:
    wholesale_customer_dbt = EmptyOperator(task_id=CUSTOMER_DBT_TASK, dag=dag)

get_runids_task_customer = PythonOperator(
    task_id="get_runids_task_wholesale_nl",
    python_callable=get_runids_customer,
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
)

(
    data_fetch_customer_base
    >> file_to_bucket_customer_base
    >> bucket_to_bq_customers
    >> wholesale_customer_dbt
    >> get_runids_task_customer
)
data_fetch_merge_requests >> file_to_bucket_merge_requests >> bucket_to_bq_merge_requests

# --- CHD market branch (independent) ---

chd_check_file = ShortCircuitOperator(
    task_id="check_file",
    python_callable=dg.check_file,
    dag=dag,
)

chd_bucket_to_bq = GCSToBigQueryOperator(
    task_id="load_gcs_to_bq",
    gcp_conn_id=gcp_conn_id,
    bucket=chd_landing_bucket,
    source_format="CSV",
    source_objects=[chd_file_glob],
    destination_project_dataset_table=(
        f"{projectid}.trusted_staging.chd_market_data_NL"
    ),
    schema_fields=dg.schema_fields,
    max_bad_records=0,
    create_disposition="CREATE_IF_NEEDED",
    write_disposition="WRITE_TRUNCATE",
    field_delimiter=",",
    skip_leading_rows=1,
    autodetect=False,
    ignore_unknown_values=True,
    allow_jagged_rows=True,
    allow_quoted_newlines=True,
    dag=dag,
)

move_files_chd = GCSToGCSOperator(
    task_id="move_files_chd",
    source_bucket=chd_landing_bucket,
    source_object=chd_file_glob,
    destination_bucket=chd_landing_bucket,
    destination_object=f"processed/{chd_file_prefix}_",
    move_object=True,
    gcp_conn_id=gcp_conn_id,
    dag=dag,
)

if DbtCloudRunJobOperator and chd_dbt_job_id:
    chd_market_data_NL_dbt = DbtCloudRunJobOperator(
        task_id=CHD_DBT_TASK,
        dbt_cloud_conn_id=dbt_cloud_conn_id,
        job_id=int(chd_dbt_job_id),
        deferrable=True,
        execution_timeout=timedelta(minutes=60),
        check_interval=30,
        do_xcom_push=True,
        dag=dag,
        timeout=3600,
    )
else:
    chd_market_data_NL_dbt = EmptyOperator(task_id=CHD_DBT_TASK, dag=dag)

get_runids_task_chd = PythonOperator(
    task_id="get_runids_task_chd",
    python_callable=get_runids_chd,
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
)

(
    chd_check_file
    >> chd_bucket_to_bq
    >> move_files_chd
    >> chd_market_data_NL_dbt
    >> get_runids_task_chd
)
