"""
Airflow DAG: Mailchimp campaign analytics ETL.

Daily pull of six Mailchimp report entities into Composer local disk, then
GCS raw zone, BigQuery staging (append), and trusted (truncate copy).

Source (read-only):
  dags/etl_mailchimp.py
  dags/horeca_digital/mailchimp.py

Sanitized: project IDs, buckets, emails, package imports, schema paths.
"""

import os
from datetime import date, datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.bigquery_to_bigquery import (
    BigQueryToBigQueryOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.utils.helpers import chain
from airflow.utils.trigger_rule import TriggerRule

from mailchimp_client import (
    CampaignList,
    CampaignReports,
    ClickReport,
    EmailActivity,
    Recipients,
    Unsubscribes,
)

try:
    from airflow.operators.empty import EmptyOperator
except ModuleNotFoundError:
    from airflow.operators.dummy import DummyOperator as EmptyOperator  # type: ignore

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

ENV_VAR_NAME = "env"
env = os.environ.get(ENV_VAR_NAME, Variable.get(ENV_VAR_NAME))
loaddate = date.today().strftime("%Y-%m-%d")

api_key = Variable.get("mailchimp_apikey")
server = Variable.get("mailchimp_server", default_var="us1")

if env == "DEV":
    project_id = "dwh_project_dev"
    bucket_name = "dp_dev_rawzone"
    gcp_conn_id = "google_cloud_dev"
else:
    project_id = "dwh_project"
    bucket_name = "dp_rawzone"
    gcp_conn_id = "google_cloud_default"

base_loc = "/home/airflow/gcs/data/mailchimp"
campaign_list_loc = f"{base_loc}/campaign_list/"
campaign_reports_loc = f"{base_loc}/campaign_reports/"
click_report_loc = f"{base_loc}/click_report/"
email_activity_loc = f"{base_loc}/email_activity/"
recipients_loc = f"{base_loc}/recipients/"
unsubscribes_loc = f"{base_loc}/unsubscribes/"

# In production, schemas lived under Composer bucket schema_json/{entity}.json.
# This pattern ships copies under schemas/ for reference.
SCHEMA_PREFIX = "schemas"

schedule = "0 1 * * *"

dag = DAG(
    dag_id="etl_mailchimp",
    default_args=default_args,
    schedule_interval=schedule,
    catchup=False,
)

start = EmptyOperator(task_id="start", trigger_rule=TriggerRule.ALL_DONE, dag=dag)
pause = EmptyOperator(task_id="pause", trigger_rule=TriggerRule.ALL_DONE, dag=dag)
end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE, dag=dag)

cl_obj = CampaignList(api_key=api_key, server=server, project_id=project_id)
cr_obj = CampaignReports(api_key=api_key, server=server, project_id=project_id)
clr_obj = ClickReport(api_key=api_key, server=server, project_id=project_id)
us_obj = Unsubscribes(api_key=api_key, server=server, project_id=project_id)
st_obj = Recipients(api_key=api_key, server=server, project_id=project_id)
ea_obj = EmailActivity(api_key=api_key, server=server, project_id=project_id)

name_list = [
    "campaign_list",
    "campaign_reports",
    "click_report",
    "unsubscribes",
    "email_activity",
    "recipients",
]

campaign_list_fetch = PythonOperator(
    task_id="campaign_list_fetch",
    python_callable=cl_obj.fetch_campaigns_list,
    op_kwargs={"campaign_list_loc": campaign_list_loc},
    dag=dag,
)

campaign_report_fetch = PythonOperator(
    task_id="campaign_report_fetch",
    python_callable=cr_obj.fetch_campaign_report,
    op_kwargs={"campaign_reports_loc": campaign_reports_loc},
    dag=dag,
)

click_report_fetch = PythonOperator(
    task_id="click_report_fetch",
    python_callable=clr_obj.fetch_click_report,
    op_kwargs={"click_report_loc": click_report_loc},
    dag=dag,
)

unsubscribes_fetch = PythonOperator(
    task_id="unsubscribes_fetch",
    python_callable=us_obj.fetch_unsubscribes,
    op_kwargs={"unsubscribes_loc": unsubscribes_loc},
    dag=dag,
)

email_activity_fetch = PythonOperator(
    task_id="email_activity_fetch",
    python_callable=ea_obj.fetch_email_activity,
    op_kwargs={"email_activity_loc": email_activity_loc},
    dag=dag,
)

recipients_fetch = PythonOperator(
    task_id="recipients_fetch",
    python_callable=st_obj.fetch_recipients,
    op_kwargs={"recipients_loc": recipients_loc},
    dag=dag,
)

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

for name in name_list:
    upload_storage = GCSToGCSOperator(
        task_id="upload_storage_" + name,
        gcp_conn_id=gcp_conn_id,
        source_bucket=Variable.get("composer_bucket"),
        source_object=f"data/mailchimp/{name}/*.json",
        destination_bucket=bucket_name,
        destination_object=f"mailchimp/{name}/{loaddate}/",
        dag=dag,
    )

    data_load_staging = GCSToBigQueryOperator(
        task_id="load_staging_" + name,
        gcp_conn_id=gcp_conn_id,
        bucket=bucket_name,
        source_format="NEWLINE_DELIMITED_JSON",
        source_objects=[f"mailchimp/{name}/{loaddate}/*.json"],
        destination_project_dataset_table=f"trusted_staging.mailchimp_{name}",
        schema_object=f"{SCHEMA_PREFIX}/{name}.json",
        create_disposition="CREATE_IF_NEEDED",
        time_partitioning={"type": "DAY"},
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
        gcp_conn_id=gcp_conn_id,
        location="EU",
        dag=dag,
    )

    chain(
        pause,
        upload_storage,
        data_load_staging,
        copy_table_trusted,
        end,
    )
