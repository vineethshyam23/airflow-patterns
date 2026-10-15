###################################################################
# DAG: etl_sales_manager_activities                               #
#                                                                 #
# Multi-country field-sales activities → GCS → BQ → dbt → Odoo.  #
###################################################################

"""
# Field-sales activities → Odoo CRM

## Overview
Daily ingest of completed field-sales (SAM) activities for eight EU
markets. Lands NDJSON on GCS, appends country staging tables in
BigQuery, runs dbt to build the lead model, then pushes to Odoo CRM.
Same-day re-queues branch to skip so accidental double triggers do not
re-pull the API.

## Flow
```
etl_sales_manager_activities
├── branch: same-calendar-day? → skip | continue
├── per country: fetch API → copy Composer→rawzone → BQ APPEND
├── load product-mapping snapshot (TRUNCATE)
├── dbt: sam_leads job
├── Odoo crm.lead create (pattern 02 engine)
├── count monitor + Slack
└── end
```

## Config (Airflow Variables)
| Variable | Purpose |
|----------|---------|
| `env` | `DEV` or `PROD` |
| `sam_oauth2_url` | OAuth2 token URL |
| `sam_base_url` | Activities API base |
| `sam_client_id` / `sam_client_secret` | OAuth client |
| `sam_username` / `sam_password` | Resource-owner credentials |
| `composer_bucket` | Composer data bucket (fetch landing) |
| `odoo_prod_creds` / `odoo_pp_creds` | JSON OdooRPC host/db/user |
| `sam_leads_dbt_job_id` | dbt Cloud job (empty → skip) |
| `slack_conn_odoo` | Slack webhook conn id |

## Schedule
Daily 03:00 UTC. `max_active_runs=1`. Same-day branch skips the API
pull if the DAG is cleared/re-queued on the same calendar day.

Distinct from pattern 02 (Odoo lead class only) and pattern 36 (Mach2
Excel email). This DAG owns the field-sales API contract and the dbt
handoff into CRM.
"""

from __future__ import annotations

import os
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

from airflow import DAG
from airflow.models import DagRun, Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.utils.helpers import chain

from odoo_lead_push import OdooLeadPush
from sam_activities_api import SalesManagerActivitiesAPI

try:
    from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
except ImportError:  # portfolio / local parse without provider
    DbtCloudRunJobOperator = None  # type: ignore[misc, assignment]


default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "start_date": datetime(2022, 10, 23),
    "email": ["dataops@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "dbt_cloud_conn_id": "dbt_conn",
    "account_id": 3,
}

# Window is "yesterday → today" at parse/run time for the API filter.
# Prefer logical date in a deploy if you need catchup-correct windows.
FROM_DATE = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
TO_DATE = datetime.utcnow().strftime("%Y-%m-%d")
LOAD_DATE = date.today().strftime("%Y%m%d")

COUNTRIES: List[str] = ["FR", "ES", "DE", "NL", "RO", "HU", "IT", "HR"]

ENV = os.environ.get("env", Variable.get("env", default_var="DEV"))

if ENV == "DEV":
    PROJECT_ID = Variable.get("dwh_project_dev", default_var="dwh_project_dev")
    RAW_BUCKET = Variable.get("rawzone_dev", default_var="rawzone_dev")
    GCP_CONN = "google_cloud_dev"
    ODOO_CREDS_KEY = "odoo_pp_creds"
    SLACK_CHANNEL = "#data-pipeline-test"
    SLACK_CONN = Variable.get("slack_conn_odoo_dev", default_var="slack_conn_test")
else:
    PROJECT_ID = Variable.get("dwh_project", default_var="dwh_project")
    RAW_BUCKET = Variable.get("rawzone", default_var="rawzone")
    GCP_CONN = "google_cloud_default"
    ODOO_CREDS_KEY = "odoo_prod_creds"
    SLACK_CHANNEL = "#crm-data-ops"
    SLACK_CONN = Variable.get("slack_conn_odoo", default_var="slack_conn")

COMPOSER_BUCKET = Variable.get("composer_bucket", default_var="composer-data")
DBT_JOB_ID = Variable.get("sam_leads_dbt_job_id", default_var="")

odoo_push = OdooLeadPush()
odoo_creds = Variable.get(ODOO_CREDS_KEY, deserialize_json=True, default_var={})


def _api_client() -> SalesManagerActivitiesAPI:
    return SalesManagerActivitiesAPI(
        oauth2_url=Variable.get("sam_oauth2_url"),
        client_id=Variable.get("sam_client_id"),
        client_secret=Variable.get("sam_client_secret"),
        username=Variable.get("sam_username"),
        password=Variable.get("sam_password"),
        base_url=Variable.get("sam_base_url"),
    )


def fetch_sam_data(country: str) -> str:
    """Pull one country into the Composer data folder as NDJSON."""
    client = _api_client()
    output_dir = f"/home/airflow/gcs/data/sam/{country.lower()}/"
    return client.fetch_activities_data(
        country=country,
        from_date=FROM_DATE,
        to_date=TO_DATE,
        output_dir=output_dir,
        filename="sam_response.json",
    )


def same_day_branch(**_: Any) -> str:
    """
    Skip API work when this DAG is re-queued on the same calendar day.

    Production used DagRun.queued_at dates. Keeps accidental clear/re-run
    from double-appending staging for the same window.
    """
    runs = DagRun.find(dag_id="etl_sales_manager_activities")
    runs.sort(key=lambda r: r.queued_at or r.execution_date, reverse=True)
    dates = [
        (r.queued_at or r.execution_date).date().isoformat()
        for r in runs
        if (r.queued_at or r.execution_date) is not None
    ]
    if not dates:
        return "continue_pipeline"
    current = dates[0]
    previous = dates[1] if len(dates) > 1 else None
    if previous is None or current != previous:
        return "continue_pipeline"
    return "skip_pipeline"


def notify_odoo_status(ti: Any, **_: Any) -> None:
    status: Optional[Dict[str, Any]] = ti.xcom_pull(task_ids="lead_monitoring_odoo")
    status = status or {"status": "FAILED", "sam": "UNKNOWN", "odoo": "UNKNOWN"}

    if status.get("status") == "MATCH":
        message = (
            f"*Field-sales → Odoo*\n"
            f"SAM: {status.get('sam')}\n"
            f"Odoo: {status.get('odoo')}\n"
        )
    elif status.get("status") == "STUB":
        message = (
            "*Field-sales → Odoo*\n"
            "Monitor stub — wire pattern 02 before prod.\n"
        )
    else:
        message = (
            f"*Field-sales → Odoo FAILED*\n"
            f"status={status.get('status')} sam={status.get('sam')} "
            f"odoo={status.get('odoo')}\n"
        )

    SlackWebhookOperator(
        task_id="slack_odoo_alert_exec",
        slack_webhook_conn_id=SLACK_CONN,
        message=message,
        channel=SLACK_CHANNEL,
        username="sam-odoo-monitor",
    ).execute({})


with DAG(
    dag_id="etl_sales_manager_activities",
    default_args=default_args,
    schedule_interval="0 3 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["odoo", "field-sales", "crm"],
    description="Field-sales activities API → BigQuery → dbt → Odoo CRM",
    doc_md=__doc__,
) as dag:
    start = EmptyOperator(task_id="start")
    continue_pipeline = EmptyOperator(task_id="continue_pipeline")
    skip_pipeline = EmptyOperator(task_id="skip_pipeline")
    countries_done = EmptyOperator(task_id="countries_done")
    end = EmptyOperator(task_id="end", trigger_rule="none_failed_min_one_success")

    branch = BranchPythonOperator(
        task_id="same_day_branch",
        python_callable=same_day_branch,
    )

    start >> branch >> [continue_pipeline, skip_pipeline]
    skip_pipeline >> end

    for country in COUNTRIES:
        fetch = PythonOperator(
            task_id=f"fetch_{country}",
            python_callable=fetch_sam_data,
            op_kwargs={"country": country},
        )
        copy_raw = GCSToGCSOperator(
            task_id=f"copy_raw_{country}",
            gcp_conn_id=GCP_CONN,
            source_bucket=COMPOSER_BUCKET,
            source_object=f"data/sam/{country.lower()}/sam_response.json",
            destination_bucket=RAW_BUCKET,
            destination_object=f"sam/{country.lower()}/sam_response_{LOAD_DATE}.json",
        )
        stage = GCSToBigQueryOperator(
            task_id=f"stage_{country}",
            gcp_conn_id=GCP_CONN,
            bucket=RAW_BUCKET,
            source_objects=[f"sam/{country.lower()}/sam_response_{LOAD_DATE}.json"],
            destination_project_dataset_table=(
                f"{PROJECT_ID}.trusted_staging.sales_manager_activities_{country.lower()}"
            ),
            schema_object="schema_json/sam_activities.json",
            source_format="NEWLINE_DELIMITED_JSON",
            write_disposition="WRITE_APPEND",
            create_disposition="CREATE_IF_NEEDED",
            ignore_unknown_values=True,
        )
        chain(continue_pipeline, fetch, copy_raw, stage, countries_done)

    load_product_map = GCSToBigQueryOperator(
        task_id="load_crm_product_mapping",
        gcp_conn_id=GCP_CONN,
        bucket=RAW_BUCKET,
        source_objects=["sam/leads/crm_lead_product_mapping.json"],
        destination_project_dataset_table=f"{PROJECT_ID}.trusted.crm_lead_product_mapping",
        source_format="NEWLINE_DELIMITED_JSON",
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
    )

    if DbtCloudRunJobOperator is not None and DBT_JOB_ID:
        dbt_sam = DbtCloudRunJobOperator(
            task_id="dbt_sam_leads",
            job_id=int(DBT_JOB_ID),
            check_interval=10,
            timeout=300,
            do_xcom_push=True,
        )
    else:
        dbt_sam = EmptyOperator(task_id="dbt_sam_leads_skipped")

    push_odoo = PythonOperator(
        task_id="push_odoo_leads",
        python_callable=odoo_push.load_data,
        op_kwargs={"odoo_creds": odoo_creds, "project_name": PROJECT_ID},
        execution_timeout=timedelta(hours=1),
    )

    monitor = PythonOperator(
        task_id="lead_monitoring_odoo",
        python_callable=odoo_push.lead_engine_monitoring,
        op_kwargs={"odoo_creds": odoo_creds, "project": PROJECT_ID},
        execution_timeout=timedelta(minutes=10),
    )

    slack = PythonOperator(
        task_id="slack_notification_odoo",
        python_callable=notify_odoo_status,
        execution_timeout=timedelta(minutes=5),
    )

    chain(
        countries_done,
        load_product_map,
        dbt_sam,
        push_odoo,
        monitor,
        slack,
        end,
    )
