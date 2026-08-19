"""
Airflow DAG: Freshdesk REST → NDJSON → GCS → BigQuery staging → dbt.

One DAG, two cadences via BranchPythonOperator:
  - Hourly (default): tickets only, month-to-date updated_since window
  - Monthly (1st @ 01:00): contacts, agents, roles, groups, companies

Distinct from pattern 15 (Odoo Postgres helpdesk pull) and pattern 06
(refined ticket event export). This lands the SaaS helpdesk API into
the warehouse landing zone.

Source (read-only):
  dags/horeca_digital/freshdesk_extract.py
  dags/horeca_digital/archived/etl_freshdesk_import.py
"""

from __future__ import annotations

import os
from datetime import date, datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.utils.helpers import chain
from airflow.utils.trigger_rule import TriggerRule

from freshdesk_client import FreshdeskClient

try:
    from airflow.operators.empty import EmptyOperator
except ModuleNotFoundError:
    from airflow.operators.dummy import DummyOperator as EmptyOperator  # type: ignore


default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "start_date": datetime(2024, 9, 1),
    "email": ["dataops@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
    "dbt_cloud_conn_id": "dbt_conn",
    "account_id": Variable.get("dbt_cloud_account_id", default_var=1),
}

ENV = os.environ.get("env", Variable.get("env", default_var="DEV"))

if ENV == "DEV":
    project_id = "dwh_project_dev"
    bucket_name = "data-platform-dev-rawzone"
    gcp_conn_id = "google_cloud_dev"
else:
    project_id = "dwh_project"
    bucket_name = "data-platform-rawzone"
    gcp_conn_id = "google_cloud_default"

# Subdomain only — API key stays in Variable freshdesk_apikey.
FRESHDESK_DOMAIN = Variable.get(
    "freshdesk_domain", default_var="helpdesk-tenant"
)
TMP_LOC = "/home/airflow/gcs/data/freshdesk/"
LOAD_DATE = date.today().strftime("%Y-%m-%d")

HOURLY_RESOURCES = ["tickets"]
MONTHLY_RESOURCES = ["contacts", "agents", "roles", "groups", "companies"]

# Source schedule was @hourly. Monthly branch fires only on day 1 hour 1.
schedule = "@hourly"

dag = DAG(
    dag_id="etl_freshdesk_api_ingest",
    default_args=default_args,
    schedule_interval=schedule,
    catchup=False,
    max_active_runs=1,
    tags=["freshdesk", "helpdesk", "api-ingest", "staging", "dbt"],
    doc_md=(
        "Freshdesk REST → NDJSON → GCS → BQ staging → dbt. "
        "Hourly tickets; monthly dims on the 1st @ 01:00. "
        "See utilities/19-freshdesk-api-ingest/."
    ),
)

start = EmptyOperator(
    task_id="start", trigger_rule=TriggerRule.ALL_DONE, dag=dag
)
pause = EmptyOperator(
    task_id="pause", trigger_rule=TriggerRule.ALL_SUCCESS, dag=dag
)
end = EmptyOperator(
    task_id="end", trigger_rule=TriggerRule.ALL_DONE, dag=dag
)
monthly_task_start = EmptyOperator(
    task_id="monthly_task_start",
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
)
hourly_task_start = EmptyOperator(
    task_id="hourly_task_start",
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
)


def choose_task_to_run(
    current_datetime: datetime | None = None,
) -> str:
    """
    Branch: monthly dims on the 1st at 01:00, otherwise tickets.

    Source docstring claimed the 16th @ 10:00; the runnable check was
    day==1 and hour==1. Keep the code behaviour — that is what Composer
    actually executed.
    """
    now = current_datetime or datetime.now()
    if now.day == 1 and now.hour == 1:
        return "monthly_task_start"
    return "hourly_task_start"


def _make_fetch(resource: str):
    """Factory so DAG parse does not read the API key at import time."""

    def _fetch(url_suffix: str, temp_loc: str, **_):
        client = FreshdeskClient(
            api_key=Variable.get("freshdesk_apikey"),
            domain=FRESHDESK_DOMAIN,
            project_id=project_id,
        )
        client.endpoint(url_suffix=url_suffix, temp_loc=temp_loc)

    return _fetch


get_date_time = BranchPythonOperator(
    task_id="choose_task_to_run",
    python_callable=choose_task_to_run,
    dag=dag,
)

chain(start, get_date_time, [monthly_task_start, hourly_task_start])


def _resource_chain(resource: str, upstream, include_dbt: bool):
    fetch = PythonOperator(
        task_id=f"freshdesk_fetch_{resource}",
        python_callable=_make_fetch(resource),
        op_kwargs={"url_suffix": resource, "temp_loc": TMP_LOC},
        dag=dag,
    )

    upload = GCSToGCSOperator(
        task_id=f"upload_storage_{resource}",
        gcp_conn_id=gcp_conn_id,
        source_bucket=Variable.get("composer_bucket"),
        source_object=f"data/freshdesk/{resource}.json",
        destination_bucket=bucket_name,
        destination_object=f"freshdesk/{LOAD_DATE}/{resource}.json",
        dag=dag,
    )

    load = GCSToBigQueryOperator(
        task_id=f"load_staging_{resource}",
        gcp_conn_id=gcp_conn_id,
        bucket=bucket_name,
        source_format="NEWLINE_DELIMITED_JSON",
        source_objects=[f"freshdesk/{LOAD_DATE}/{resource}.json"],
        destination_project_dataset_table=f"staging.freshdesk_{resource}",
        schema_object=f"schema_json/freshdesk_{resource}.json",
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        allow_quoted_newlines=True,
        dag=dag,
    )

    if include_dbt:
        dbt_job = DbtCloudRunJobOperator(
            task_id=f"freshdesk_{resource}_dbt_run",
            job_id=int(
                Variable.get("dbt_freshdesk_tickets_job_id", default_var="0")
            ),
            check_interval=10,
            timeout=300,
            do_xcom_push=True,
            trigger_rule=TriggerRule.ALL_DONE,
            dag=dag,
        )
        chain(upstream, fetch, upload, load, dbt_job, end)
    else:
        chain(upstream, fetch, upload, load, pause)


for name in HOURLY_RESOURCES:
    _resource_chain(name, hourly_task_start, include_dbt=True)

for name in MONTHLY_RESOURCES:
    _resource_chain(name, monthly_task_start, include_dbt=False)

freshdesk_dims_dbt = DbtCloudRunJobOperator(
    task_id="freshdesk_dims_dbt_run",
    job_id=int(Variable.get("dbt_freshdesk_dims_job_id", default_var="0")),
    check_interval=10,
    timeout=300,
    do_xcom_push=True,
    dag=dag,
)

chain(pause, freshdesk_dims_dbt, end)
