"""Sales data mart — two-phase BigQuery object copy.

Daily replication of analytical views and trusted tables into a
dedicated ``refined_sales`` dataset so Sales / BI consumers get a
stable, permission-scoped mart without IAM on the full trusted layer.

Engineering contract (what makes this more than SELECT *):

1. Phase 1 — materialize analytical *views* via ``SELECT *`` insert jobs
   (views are not first-class copy sources for BigQueryToBigQuery).
2. Pause barrier — intentional slot-pressure gap before the heavier
   native table copies.
3. Phase 2 — ``BigQueryToBigQueryOperator`` WRITE_TRUNCATE for physical
   tables (EU location, create-if-needed).
4. ``chain(start, *views, pause, *tables, end)`` so each phase fans out
   in parallel and phases stay ordered.
5. Post-CRM-decommission catalog: SFDC objects stay frozen in the mart;
   this DAG only refreshes the live subscription / Hydra / catalog /
   Odoo / analytical-actual set.

Source (read-only):
  dags/etl_dwh_sales_export.py
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.bigquery_to_bigquery import (
    BigQueryToBigQueryOperator,
)
from airflow.utils.helpers import chain

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

ENV = os.environ.get("env", Variable.get("env", default_var="PROD"))

if ENV == "DEV":
    PROJECT_ID = Variable.get("dwh_gcp_project_dev", default_var="dwh_project_dev")
    GCP_CONN = Variable.get("dwh_gcp_conn_dev", default_var="google_cloud_dev")
else:
    PROJECT_ID = Variable.get("dwh_gcp_project", default_var="dwh_project")
    GCP_CONN = Variable.get("dwh_gcp_conn", default_var="google_cloud_default")

DEST_DATASET = "refined_sales"

# Analytical views over historized sources. Production originally also
# copied ~20 CRM (asfdc_*) views; those are frozen after CRM
# decommission and are intentionally absent from this live catalog.
VIEW_CATALOG = [
    ("trusted_views", "a_subscription_history"),
    ("trusted_views", "a_subscriptions"),
    ("trusted_views", "a_hyd_establishments"),
    ("trusted_views", "a_catalog_product_price_schemes"),
    ("trusted_views", "a_catalog_countries"),
    ("trusted_views", "a_catalog_products"),
    ("trusted_views", "a_catalog_merchants"),
    ("trusted_views", "a_odoo_wsl_invoice_lines"),
]

# Physical tables. Same story for CRM trusted tables — frozen copies
# remain in refined_sales; we only refresh live refined / Odoo sources.
TABLE_CATALOG = [
    ("refined", "analytical_crm_establishment_actual"),
    ("refined", "analytical_rt_establishments_actual"),
    ("refined", "analytical_order_establishments_actual"),
    ("trusted_odoo", "odoo_erp_timesheets_timestamped"),
    ("trusted_odoo", "odoo_erp_invoice_lines_timestamped"),
    ("trusted_odoo", "odoo_wsl_customers"),
]

# 08:05 UTC — after overnight refined / trusted land, before most Sales
# BI refresh windows.
SCHEDULE = "5 8 * * *"

dag = DAG(
    dag_id="etl_sales_datamart_two_phase_copy",
    default_args=default_args,
    schedule_interval=SCHEDULE,
    catchup=False,
    max_active_runs=1,
    render_template_as_native_obj=True,
    tags=["sales", "datamart", "bigquery", "copy"],
)

start = EmptyOperator(task_id="start", dag=dag)
pause = EmptyOperator(task_id="pause", dag=dag)
end = EmptyOperator(task_id="end", dag=dag)

view_tasks = []
for dataset, view in VIEW_CATALOG:
    # Views must be SELECT *'d into a table — BQ-to-BQ copy does not
    # accept views as source_project_dataset_tables in this operator.
    # Destination project follows ENV (production historically hard-coded
    # the prod project even in DEV; fixed here).
    copy_view = BigQueryInsertJobOperator(
        task_id=f"copy_view_{dataset}_{view}",
        configuration={
            "query": {
                "query": f"SELECT * FROM `{PROJECT_ID}.{dataset}.{view}`",
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": PROJECT_ID,
                    "datasetId": DEST_DATASET,
                    "tableId": view,
                },
                "writeDisposition": "WRITE_TRUNCATE",
                "createDisposition": "CREATE_IF_NEEDED",
                "allowLargeResults": True,
            }
        },
        gcp_conn_id=GCP_CONN,
        # Soft gate: one sibling failure must not discard a whole phase
        # when at least one upstream succeeded. Documented risk if start
        # is the only upstream — still fine.
        trigger_rule="none_failed_min_one_success",
        dag=dag,
    )
    view_tasks.append(copy_view)

table_tasks = []
for dataset, table in TABLE_CATALOG:
    copy_table = BigQueryToBigQueryOperator(
        task_id=f"copy_table_{dataset}_{table}",
        source_project_dataset_tables=f"{PROJECT_ID}.{dataset}.{table}",
        destination_project_dataset_table=f"{PROJECT_ID}.{DEST_DATASET}.{table}",
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
        gcp_conn_id=GCP_CONN,
        location="EU",
        trigger_rule="none_failed_min_one_success",
        dag=dag,
    )
    table_tasks.append(copy_table)

# chain() expands lists as sequential groups of parallel siblings:
# start → (all views ||) → pause → (all tables ||) → end
chain(start, *view_tasks, pause, *table_tasks, end)
