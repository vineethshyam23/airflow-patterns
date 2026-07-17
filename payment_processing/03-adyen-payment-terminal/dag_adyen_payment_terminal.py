"""
Airflow DAG: Adyen payment terminal ETL + light terminal management.

Nightly pull of merchants / stores / terminals / settings from the Adyen
Management API into Composer local disk → raw GCS → BigQuery staging, then
a dbt Cloud job, then optional reassign + settings PATCH driven by a trusted
match table.

Source (read-only):
  dags/etl_adyen_payment_terminal.py
  dags/horeca_digital/adyen_payment_terminal_integration.py

Sanitized: project IDs, buckets, emails, dbt job id, package imports.
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.utils.dates import days_ago
from airflow.utils.helpers import chain
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

# In production this lived under the Composer package path.
# For this sample, import from the sibling module in the pattern folder.
from adyen_payment_terminal import (
    AdyenConfig,
    fetch_merchant_data,
    fetch_store_data,
    fetch_terminal_data,
    fetch_terminal_settings_data,
    run_terminal_default_settings_patches_from_bigquery,
    run_terminal_reassignments_from_bigquery,
)

try:
    from airflow.operators.empty import EmptyOperator
except ModuleNotFoundError:
    from airflow.operators.dummy import DummyOperator as EmptyOperator  # type: ignore

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email": ["dataops@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
}

ENV_VAR_NAME = "env"
env = os.environ.get(ENV_VAR_NAME, Variable.get(ENV_VAR_NAME))

# Creds stay in an Airflow Variable (JSON). Never hardcode.
adyen_payment_terminal_creds = Variable.get(
    "adyen_payment_terminal_creds", deserialize_json=True
)

if env == "DEV":
    bucket_name = "dp_dev_rawzone"
    project_id = "dwh_project_dev"
    gcp_conn_id = "google_cloud_dev"
elif env == "PROD":
    bucket_name = "dp_rawzone"
    project_id = "dwh_project"
    gcp_conn_id = "google_cloud_default"
else:
    raise ValueError(f"Unknown environment: {env}")

config = AdyenConfig(
    api_key=adyen_payment_terminal_creds["api_key"],
    username=adyen_payment_terminal_creds["username"],
    password=adyen_payment_terminal_creds["password"],
    environment=adyen_payment_terminal_creds["environment"],
)

load_date = datetime.now().strftime("%Y-%m-%d")

# Placeholder: production used a custom DbtCloudRunJobOperator with a real job id.
DBT_CLOUD_JOB_ID = int(Variable.get("adyen_terminal_dbt_job_id", default_var="0"))


def trigger_dbt_adyen_models(**_kwargs):
    """Stand-in for the Composer-specific dbt Cloud operator.

    Production triggered a fixed dbt Cloud job after staging loads so the
    serial↔store match model was fresh before terminal management API calls.
    Wire your own operator / API client here.
    """
    if not DBT_CLOUD_JOB_ID:
        raise ValueError(
            "Set Airflow Variable adyen_terminal_dbt_job_id to the dbt Cloud job id"
        )
    # TODO: call dbt Cloud run API (or your internal operator) with DBT_CLOUD_JOB_ID
    print(f"Would trigger dbt Cloud job_id={DBT_CLOUD_JOB_ID}")


dag = DAG(
    dag_id="etl_adyen_payment_terminal",
    default_args=default_args,
    schedule_interval="0 2 * * *",
    catchup=False,
    doc_md=__doc__,
    tags=["payments", "adyen", "api", "terminals"],
)

start = EmptyOperator(task_id="start", trigger_rule=TriggerRule.ALL_SUCCESS, dag=dag)
stage_1 = EmptyOperator(task_id="stage_1", trigger_rule=TriggerRule.ALL_SUCCESS, dag=dag)
stage_2 = EmptyOperator(task_id="stage_2", trigger_rule=TriggerRule.ALL_SUCCESS, dag=dag)
end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_SUCCESS, dag=dag)

merchant_data_fetch = PythonOperator(
    task_id="merchant_data_fetch",
    python_callable=fetch_merchant_data,
    op_kwargs={"config": config},
    do_xcom_push=True,
    show_return_value_in_logs=False,
    dag=dag,
)

# Store fetch needs the merchant list; XCom of the full list is heavy but
# kept here because that is how the production DAG was wired.
store_data_fetch = PythonOperator(
    task_id="store_data_fetch",
    python_callable=fetch_store_data,
    op_kwargs={"config": config, "merchant_data": merchant_data_fetch.output},
    do_xcom_push=True,
    show_return_value_in_logs=False,
    dag=dag,
)

terminal_data_fetch = PythonOperator(
    task_id="terminal_data_fetch",
    python_callable=fetch_terminal_data,
    op_kwargs={"config": config},
    do_xcom_push=True,
    show_return_value_in_logs=False,
    dag=dag,
)

terminal_settings_data_fetch = PythonOperator(
    task_id="terminal_settings_data_fetch",
    python_callable=fetch_terminal_settings_data,
    op_kwargs={"config": config, "terminal_data": terminal_data_fetch.output},
    do_xcom_push=True,
    show_return_value_in_logs=False,
    dag=dag,
)

chain(
    start,
    merchant_data_fetch,
    store_data_fetch,
    terminal_data_fetch,
    terminal_settings_data_fetch,
    stage_1,
)

for file_name in [
    "merchant_data",
    "store_data",
    "terminal_data",
    "terminal_settings_data",
]:
    upload_storage = GCSToGCSOperator(
        task_id=f"upload_storage_{file_name}",
        gcp_conn_id=gcp_conn_id,
        source_bucket=Variable.get("composer_bucket"),
        source_objects=[f"data/adyen/payment_terminal/{file_name}.json"],
        destination_bucket=bucket_name,
        destination_object=(
            f"adyen/payment_terminal/{file_name}/{load_date}/{file_name}.json"
        ),
        dag=dag,
    )

    data_load_staging = GCSToBigQueryOperator(
        task_id=f"load_staging_{file_name}",
        gcp_conn_id=gcp_conn_id,
        bucket=bucket_name,
        source_format="NEWLINE_DELIMITED_JSON",
        source_objects=[
            f"adyen/payment_terminal/{file_name}/{load_date}/{file_name}.json"
        ],
        destination_project_dataset_table=(
            f"trusted_staging.adyen_payment_terminal_{file_name}"
        ),
        schema_object_bucket=bucket_name,
        schema_object=f"schema_json/adyen_payment_terminal_{file_name}.json",
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        dag=dag,
    )

    chain(stage_1, upload_storage, data_load_staging, stage_2)

adyen_run_dbt_job = PythonOperator(
    task_id="adyen_run_dbt_job",
    python_callable=trigger_dbt_adyen_models,
    dag=dag,
)

# PATCH uses ALL_DONE so settings still run if a reassign attempt fails mid-batch.
with TaskGroup(group_id="terminal_management_api", dag=dag) as terminal_management_api:
    terminal_reassign_from_bq = PythonOperator(
        task_id="terminal_reassign_from_bq",
        python_callable=run_terminal_reassignments_from_bigquery,
        op_kwargs={"config": config, "project_id": project_id},
        dag=dag,
    )

    terminal_default_settings_from_bq = PythonOperator(
        task_id="terminal_default_settings_from_bq",
        python_callable=run_terminal_default_settings_patches_from_bigquery,
        op_kwargs={"config": config, "project_id": project_id},
        trigger_rule=TriggerRule.ALL_DONE,
        dag=dag,
    )
    # Settings PATCH assumes inventory=false assignment from the reassign step.
    terminal_reassign_from_bq >> terminal_default_settings_from_bq

chain(stage_2, adyen_run_dbt_job, terminal_management_api)
terminal_management_api >> end
