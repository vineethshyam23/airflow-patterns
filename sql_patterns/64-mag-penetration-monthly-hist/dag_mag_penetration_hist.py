"""MAG penetration rates — monthly historization (2nd of month).

Appends prior-month country actuals, then a corporate rollup row whose
platform totals are deltas against the prior month's hist. Feeds group
reporting and the partner penetration export (pattern 24).

Distinct from pattern 24 (outbound Avro export of the same hist),
pattern 46 / 48 / 62 (zone rebuilds), and the archived 1st-of-month
sales/acquisitions historization sibling.

Source (read-only):
  dags/etl_refined_zone_2nd_of_month.py
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
)
from airflow.utils.trigger_rule import TriggerRule

import penetration_queries as q

DWH_PROJECT = Variable.get("mag_hist_dwh_project", default_var="dwh_project")
BQ_CONN = Variable.get("mag_hist_bq_conn", default_var="bigquery_default")
DEST_DATASET = "refined"
DEST_TABLE = "hist_penetration_rates_reporting"

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "start_date": datetime(2022, 7, 1),
    "email": ["dataops@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# 07:16 UTC on the 2nd — one day after the 1st-of-month MAG sales /
# acquisitions historization sibling so country views are closed.
SCHEDULE = "16 7 2 * *"

dag = DAG(
    dag_id="etl_mag_penetration_monthly_hist",
    default_args=default_args,
    schedule_interval=SCHEDULE,
    catchup=False,
    max_active_runs=1,
    tags=["etl", "mag", "penetration", "monthly", "hist"],
    doc_md=__doc__,
)


def _append_job(task_id: str, sql: str) -> BigQueryInsertJobOperator:
    return BigQueryInsertJobOperator(
        task_id=task_id,
        gcp_conn_id=BQ_CONN,
        configuration={
            "query": {
                "query": sql,
                "useLegacySql": False,
                "writeDisposition": "WRITE_APPEND",
                "allowLargeResults": True,
                "destinationTable": {
                    "projectId": DWH_PROJECT,
                    "datasetId": DEST_DATASET,
                    "tableId": DEST_TABLE,
                },
            }
        },
        # Production used ALL_DONE on both tasks. Kept here so a failed
        # country append still schedules the corp delta — operators must
        # treat that as a risk, not a feature (see DATA_FLOW.md).
        trigger_rule=TriggerRule.ALL_DONE,
        dag=dag,
    )


append_country = _append_job(
    "append_hist_penetration_country",
    q.country_actuals_sql(DWH_PROJECT),
)
append_corp = _append_job(
    "append_hist_penetration_corp_delta",
    q.corp_delta_sql(DWH_PROJECT),
)

append_country >> append_corp
