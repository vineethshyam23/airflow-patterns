"""MAG sales / acquisitions monthly historization + partner-ID clean.

Three jobs on the 1st of each month:

1. WRITE_APPEND prior-month product-bundle sales into hist
2. WRITE_APPEND acquisitions with sales_all_time carry-forward
3. WRITE_TRUNCATE CRM partner-ID cleaning for FR/RO matching engine
   (independent — runs in parallel with the MAG chain)

Sibling of pattern 64 (penetration hist on the 2nd). Distinct tables
and a different engineering question: cumulative sales carry-forward
plus messy CRM ID normalization, not corp-delta penetration.

Source (read-only):
  dags/horeca_digital/archived/etl_refined_zone_monthly.py

Production note: archived with schedule_interval=None after CRM raw
land froze (2025-05). Portfolio restores the intended calendar
``15 7 1 * *`` so the monthly contract stays visible.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
)
from airflow.utils.trigger_rule import TriggerRule

import hist_queries as hq
import partner_id_queries as pq

DWH_PROJECT = Variable.get(
    "mag_sales_hist_dwh_project", default_var="dwh_project"
)
BQ_CONN = Variable.get(
    "mag_sales_hist_bq_conn", default_var="bigquery_default"
)
DEST_DATASET = "refined"

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "start_date": datetime(2021, 12, 31),
    "email": ["dataops@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    # Production used retries=0. Kept so monthly hist failures force
    # an operator decision instead of silent re-append.
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

# Intended schedule from production docstring (archived DAG set None).
SCHEDULE = "15 7 1 * *"

dag = DAG(
    dag_id="etl_mag_sales_acquisitions_monthly_hist",
    default_args=default_args,
    schedule_interval=SCHEDULE,
    catchup=False,
    max_active_runs=1,
    tags=["etl", "mag", "sales", "acquisitions", "monthly", "hist"],
    doc_md=__doc__,
)


def _bq_job(
    task_id: str,
    sql: str,
    table_id: str,
    write_disposition: str,
) -> BigQueryInsertJobOperator:
    return BigQueryInsertJobOperator(
        task_id=task_id,
        gcp_conn_id=BQ_CONN,
        configuration={
            "query": {
                "query": sql,
                "useLegacySql": False,
                "writeDisposition": write_disposition,
                "allowLargeResults": True,
                "destinationTable": {
                    "projectId": DWH_PROJECT,
                    "datasetId": DEST_DATASET,
                    "tableId": table_id,
                },
            }
        },
        # Production used ALL_DONE on every task. Documented risk:
        # acquisitions can still append after a failed sales task.
        trigger_rule=TriggerRule.ALL_DONE,
        dag=dag,
    )


append_sales = _bq_job(
    "append_hist_sales_reporting",
    hq.sales_hist_sql(DWH_PROJECT),
    hq.HIST_SALES,
    "WRITE_APPEND",
)
append_acquisitions = _bq_job(
    "append_hist_acquisitions_reporting",
    hq.acquisitions_hist_sql(DWH_PROJECT),
    hq.HIST_ACQUISITIONS,
    "WRITE_APPEND",
)
clean_partner_ids = _bq_job(
    "crm_establishment_clean_partner_id",
    pq.partner_id_clean_sql(DWH_PROJECT),
    pq.DEST_TABLE,
    "WRITE_TRUNCATE",
)

# MAG chain is sequential; partner-ID clean is independent.
append_sales >> append_acquisitions
# clean_partner_ids has no upstream — parallel with the MAG chain
