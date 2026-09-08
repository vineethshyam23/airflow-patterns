"""POS vendor GA4 rolling 7-day event load → Data Transfer → dbt.

Native GA4→BigQuery export lands daily shards (`events_YYYYMMDD`).
This DAG reloads yesterday through 7 days ago into a staging table
with DELETE+INSERT per day (idempotent, late-arrival friendly), kicks
a BigQuery Data Transfer config, then runs one dbt Cloud job.

Distinct from pattern 35 (HMAC CSV store-details for the same vendor)
and from Adobe rawfeed jobs — this is product-web/app event analytics
from GA4 export tables, not a vendor REST dump.

Source (read-only):
  dags/etl_booq_google_analytics.py
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.trigger_rule import TriggerRule

from ga4_transfer import collect_dbt_run_ids, run_data_transfer

try:
    from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
except ImportError:  # pragma: no cover - reference stub
    DbtCloudRunJobOperator = None

LOOKBACK_DAYS = 7
DBT_TASK_ID = "dbt_ga4"
RUNIDS_VARIABLE = "etl_booq_google_analytics_runids"

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "start_date": datetime(2021, 4, 3),
    "email": ["dataops@example.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "dbt_cloud_conn_id": "dbt_conn",
    "account_id": 1,
}

environment = "env"
env = os.environ.get(environment, Variable.get(environment, default_var="DEV"))

if env == "DEV":
    PROJECT_ID = "dwh_project_dev"
    GCP_CONN_ID = "google_cloud_dev"
else:
    PROJECT_ID = "dwh_project"
    GCP_CONN_ID = "google_cloud_default"

# GA4 property / dataset ids — production hardcoded a numeric property.
# Variables keep real ids out of git; defaults are placeholders.
GA4_PROPERTY = Variable.get("booq_ga4_property_id", default_var="000000000")
SOURCE_DATASET = f"analytics_{GA4_PROPERTY}"
STAGING_DATASET = f"analytics_{GA4_PROPERTY}_temp"
STAGING_TABLE = "pos_vendor_ga_events"

try:
    DBT_JOB_ID = Variable.get("booq_ga4_dbt_job_id")
except KeyError:
    DBT_JOB_ID = None

dag = DAG(
    dag_id="etl_booq_google_analytics",
    default_args=default_args,
    schedule_interval="0 8 * * *",
    # Production used 20 minutes. With 7 BQ jobs + ~6 min transfer sleep
    # + dbt timeout 1000s that budget is tight — documented, not silently
    # rewritten. Raise to 45–60m in a real rewrite.
    dagrun_timeout=timedelta(minutes=20),
    catchup=False,
    max_active_runs=1,
    tags=["ga4", "pos-vendor", "analytics", "daily"],
    doc_md=__doc__,
)


def _day_load_sql(day_offset: int) -> str:
    """DELETE+INSERT one GA4 export shard into staging for day_offset ago.

    Uses CURRENT_DATE() at query runtime (production behaviour). Prefer
    Airflow {{ ds }} macros if you need deterministic backfills.
    """
    return f"""
    DELETE FROM `{PROJECT_ID}.{STAGING_DATASET}.{STAGING_TABLE}`
    WHERE event_date = DATE_SUB(CURRENT_DATE(), INTERVAL {day_offset} DAY);

    INSERT INTO `{PROJECT_ID}.{STAGING_DATASET}.{STAGING_TABLE}`
    SELECT
      PARSE_DATE('%Y%m%d', event_date) AS event_date,
      event_timestamp,
      event_name,
      event_params,
      event_previous_timestamp,
      event_value_in_usd,
      event_bundle_sequence_id,
      event_server_timestamp_offset,
      user_id,
      user_pseudo_id,
      privacy_info,
      user_properties,
      user_first_touch_timestamp,
      user_ltv,
      device,
      geo,
      app_info,
      traffic_source,
      stream_id,
      platform,
      event_dimensions,
      ecommerce,
      items,
      collected_traffic_source,
      is_active_user,
      batch_event_index,
      batch_page_id,
      batch_ordering_id
    FROM `{PROJECT_ID}.{SOURCE_DATASET}.events_*`
    WHERE _TABLE_SUFFIX = FORMAT_DATE(
      '%Y%m%d',
      DATE_SUB(CURRENT_DATE(), INTERVAL {day_offset} DAY)
    )
    """


def _get_runids(ti):
    return collect_dbt_run_ids(ti, [DBT_TASK_ID], RUNIDS_VARIABLE)


run_data_transfers = PythonOperator(
    task_id="run_Data_transfers",
    python_callable=run_data_transfer,
    dag=dag,
)

if DbtCloudRunJobOperator is not None and DBT_JOB_ID:
    dbt_ga4 = DbtCloudRunJobOperator(
        task_id=DBT_TASK_ID,
        job_id=int(DBT_JOB_ID),
        check_interval=10,
        do_xcom_push=True,
        timeout=1000,
        dag=dag,
    )
else:
    dbt_ga4 = EmptyOperator(task_id=DBT_TASK_ID, dag=dag)

get_runids_task = PythonOperator(
    task_id="get_runids_task",
    python_callable=_get_runids,
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
)

# Seven parallel day loads (offsets 1..7). Fan-in to transfer → dbt → runids.
for day_offset in range(1, LOOKBACK_DAYS + 1):
    ga_events = BigQueryInsertJobOperator(
        task_id=f"booq_ga_events_{day_offset}",
        configuration={
            "query": {
                "query": _day_load_sql(day_offset),
                "useLegacySql": False,
                "allowLargeResults": True,
            }
        },
        gcp_conn_id=GCP_CONN_ID,
        dag=dag,
    )
    ga_events >> run_data_transfers >> dbt_ga4 >> get_runids_task
