"""
Airflow DAG: daily Adobe marketing UTM / conversion attribution.

Appends today's website visits from the refined Adobe datafeed into
`adobe_marketing_utm` with funnel flags (pricing page, request-demo,
form submit), referral-channel pivots, and UTM eVars.

Source (read-only):
  dags/adobe_marketing_utm.py

Distinct from patterns 49 / 51 (Data Feed land) and 77 (site-traffic
CSV publish): this is a refined *attribution* projection on top of
an already-landed hit table — not unpack, not file publish.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

from adobe_marketing_utm_query import build_insert_sql

# --- sanitized identifiers -------------------------------------------------
DAG_ID = "adobe_marketing_utm"
GCP_CONN_ID = "bigquery_default"
PROJECT_ID = Variable.get("dwh_project_id", default_var="dwh_project")
REFINED_DATASET = Variable.get("refined_dataset", default_var="refined")
# Website product filter in the source datafeed (was a brand code).
PRODUCT_TYPE = Variable.get("adobe_utm_product_type", default_var="PLATFORM")

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "start_date": datetime(2021, 12, 8),
    "email": ["dataops@example.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule_interval="0 6 * * *",
    catchup=False,
    tags=["adobe", "marketing", "attribution", "refined"],
)

# Production debt kept intentional: CURRENT_DATE() + bare INSERT.
# Re-runs on the same calendar day duplicate rows. Prefer MERGE or
# delete-then-insert + {{ ds }} when you harden this.
insert_sql = build_insert_sql(
    project_id=PROJECT_ID,
    refined_dataset=REFINED_DATASET,
    product_type=PRODUCT_TYPE,
)

update_adobe_marketing_utm = BigQueryInsertJobOperator(
    task_id="update_adobe_marketing_utm_query",
    configuration={
        "query": {
            "query": insert_sql,
            "useLegacySql": False,
        }
    },
    gcp_conn_id=GCP_CONN_ID,
    dag=dag,
)

update_adobe_marketing_utm
