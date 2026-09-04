"""Daily Mailchimp email analytics — serial extracts, then parallel land+load.

Phase 1 pulls six grains serially (campaign list first; report endpoints
depend on campaign IDs already in staging). Phase 2 fans out from a
pause barrier: Composer data → rawzone → staging APPEND → trusted
WRITE_TRUNCATE, one chain per grain.

Distinct from pattern 31 (Maileon): official Mailchimp Marketing SDK,
campaign-scoped report fan-out driven by a 90-day BigQuery lookup, and
staging APPEND + trusted full replace instead of per-report TRUNCATE
plus dbt.

Source (read-only):
  dags/etl_mailchimp.py
  dags/horeca_digital/mailchimp.py
"""

from __future__ import annotations

import os
from datetime import date, datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.bigquery_to_bigquery import (
    BigQueryToBigQueryOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.utils.helpers import chain
from airflow.utils.trigger_rule import TriggerRule

from mailchimp_api import (
    CampaignList,
    CampaignReports,
    ClickReport,
    EmailActivity,
    Recipients,
    Unsubscribes,
)

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "start_date": datetime(2022, 10, 23),
    "email": ["dataops@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}

ENTITY_NAMES = [
    "campaign_list",
    "campaign_reports",
    "click_report",
    "unsubscribes",
    "email_activity",
    "recipients",
]

environment = "env"
env = os.environ.get(environment, Variable.get(environment, default_var="DEV"))

# Production stamped loaddate at DAG parse time. Prefer {{ ds }} when
# hardening; kept here so the sample matches the inherited footgun.
loaddate = date.today().strftime("%Y-%m-%d")

API_KEY = Variable.get("mailchimp_apikey", default_var="")
SERVER = Variable.get("mailchimp_server_prefix", default_var="us1")
COMPOSER_BUCKET = Variable.get("composer_bucket", default_var="composer-data")

if env == "DEV":
    PROJECT_ID = Variable.get("mailchimp_gcp_project", default_var="dwh_project_dev")
    BUCKET_NAME = Variable.get("mailchimp_rawzone_bucket", default_var="rawzone_dev")
    GCP_CONN_ID = "google_cloud_dev"
    schedule = None
else:
    PROJECT_ID = Variable.get("mailchimp_gcp_project", default_var="dwh_project")
    BUCKET_NAME = Variable.get("mailchimp_rawzone_bucket", default_var="rawzone")
    GCP_CONN_ID = "google_cloud_default"
    schedule = "0 1 * * *"

LOCAL_PATHS = {
    name: f"/home/airflow/gcs/data/mailchimp/{name}/" for name in ENTITY_NAMES
}

dag = DAG(
    dag_id="etl_mailchimp",
    default_args=default_args,
    schedule_interval=schedule,
    catchup=False,
    tags=["email", "mailchimp", "marketing"],
)

start = EmptyOperator(task_id="start", trigger_rule=TriggerRule.ALL_DONE, dag=dag)
pause = EmptyOperator(task_id="pause", trigger_rule=TriggerRule.ALL_DONE, dag=dag)
end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE, dag=dag)

cl_obj = CampaignList(api_key=API_KEY, server=SERVER, project_id=PROJECT_ID)
cr_obj = CampaignReports(api_key=API_KEY, server=SERVER, project_id=PROJECT_ID)
clr_obj = ClickReport(api_key=API_KEY, server=SERVER, project_id=PROJECT_ID)
us_obj = Unsubscribes(api_key=API_KEY, server=SERVER, project_id=PROJECT_ID)
st_obj = Recipients(api_key=API_KEY, server=SERVER, project_id=PROJECT_ID)
ea_obj = EmailActivity(api_key=API_KEY, server=SERVER, project_id=PROJECT_ID)

campaign_list_fetch = PythonOperator(
    task_id="campaign_list_fetch",
    python_callable=cl_obj.fetch_campaigns_list,
    op_kwargs={"campaign_list_loc": LOCAL_PATHS["campaign_list"]},
    dag=dag,
)

campaign_report_fetch = PythonOperator(
    task_id="campaign_report_fetch",
    python_callable=cr_obj.fetch_campaign_report,
    op_kwargs={"campaign_reports_loc": LOCAL_PATHS["campaign_reports"]},
    dag=dag,
)

click_report_fetch = PythonOperator(
    task_id="click_report_fetch",
    python_callable=clr_obj.fetch_click_report,
    op_kwargs={"click_report_loc": LOCAL_PATHS["click_report"]},
    dag=dag,
)

unsubscribes_fetch = PythonOperator(
    task_id="unsubscribes_fetch",
    python_callable=us_obj.fetch_unsubscribes,
    op_kwargs={"unsubscribes_loc": LOCAL_PATHS["unsubscribes"]},
    dag=dag,
)

email_activity_fetch = PythonOperator(
    task_id="email_activity_fetch",
    python_callable=ea_obj.fetch_email_activity,
    op_kwargs={"email_activity_loc": LOCAL_PATHS["email_activity"]},
    dag=dag,
)

recipients_fetch = PythonOperator(
    task_id="recipients_fetch",
    python_callable=st_obj.fetch_recipients,
    op_kwargs={"recipients_loc": LOCAL_PATHS["recipients"]},
    dag=dag,
)

# Serial extract: campaign list must land before report tasks query staging.
# In production the list is loaded in a prior run / same-day land for
# reports to see IDs — the 90-day window usually covers yesterday's list.
chain(
    start,
    campaign_list_fetch,
    campaign_report_fetch,
    click_report_fetch,
    unsubscribes_fetch,
    email_activity_fetch,
    recipients_fetch,
    pause,
)

for name in ENTITY_NAMES:
    upload_storage = GCSToGCSOperator(
        task_id="upload_storage_" + name,
        gcp_conn_id=GCP_CONN_ID,
        source_bucket=COMPOSER_BUCKET,
        source_object=f"data/mailchimp/{name}/*.json",
        destination_bucket=BUCKET_NAME,
        destination_object=f"mailchimp/{name}/{loaddate}/",
        dag=dag,
    )

    data_load_staging = GCSToBigQueryOperator(
        task_id="load_staging_" + name,
        gcp_conn_id=GCP_CONN_ID,
        bucket=BUCKET_NAME,
        source_format="NEWLINE_DELIMITED_JSON",
        source_objects=[f"mailchimp/{name}/{loaddate}/*.json"],
        destination_project_dataset_table="trusted_staging.mailchimp_" + name,
        schema_object="schema_json/" + name + ".json",
        create_disposition="CREATE_IF_NEEDED",
        time_partitioning={"time_partitioning_type": "DAY"},
        write_disposition="WRITE_APPEND",
        allow_quoted_newlines=True,
        dag=dag,
    )

    copy_table_trusted = BigQueryToBigQueryOperator(
        task_id=f"copy_table_to_trusted_mailchimp_{name}",
        source_project_dataset_tables=f"trusted_staging.mailchimp_{name}",
        destination_project_dataset_table=f"trusted.mailchimp_{name}",
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
        gcp_conn_id=GCP_CONN_ID,
        location="EU",
        dag=dag,
    )

    chain(pause, upload_storage, data_load_staging, copy_table_trusted, end)
