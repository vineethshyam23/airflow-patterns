"""
Airflow DAG: cross-project BigQuery bridge for menu-engineering menus.

One daily ``BigQueryToBigQueryOperator`` copies the refined ``co_menus``
table from the menu-engineering GCP project into the main DWH trusted
layer with WRITE_TRUNCATE.

This is the *downstream* bridge. Upstream land (VM Postgres → GCS → BQ
→ dbt inside the menu-engineering project) is pattern 55. This DAG does
not re-land Postgres; it only makes the refined table readable under
the DWH project's IAM and naming.

Engineering contract:
  - Cross-project source (menu_eng_project.refined.co_menus)
  - DWH destination (dwh_project.trusted.co_menus)
  - Full replace daily — no watermark, no MERGE, no upstream sensor
  - Single task; schedule late enough that upstream land usually finished

Distinct from:
  - Pattern 55 (Menu Engineering VM Postgres land into the source project)
  - Pattern 68 (intra-DWH sales mart two-phase view + table copy)
  - Pattern 81 (weekly multi-table copy_jobs WRITE_TRUNCATE registry)

Source (read-only):
  dags/etl_di_import.py
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.transfers.bigquery_to_bigquery import (
    BigQueryToBigQueryOperator,
)

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "start_date": datetime(2024, 8, 12),
    "email": ["dataops@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

ENV = os.environ.get("env", Variable.get("env", default_var="PROD"))

if ENV == "DEV":
    DWH_PROJECT = Variable.get("dwh_gcp_project_dev", default_var="dwh_project_dev")
    GCP_CONN = Variable.get("dwh_gcp_conn_dev", default_var="google_cloud_dev")
else:
    DWH_PROJECT = Variable.get("dwh_gcp_project", default_var="dwh_project")
    GCP_CONN = Variable.get("dwh_gcp_conn", default_var="google_cloud_default")

# Authoritative menu-engineering project. Source stays fixed even when
# destination flips with ENV — same cross-project posture as pattern 81
# REX market copies (one upstream zone, env-scoped DWH write).
MENU_ENG_PROJECT = Variable.get(
    "menu_eng_gcp_project", default_var="menu_eng_project"
)
SOURCE_DATASET = "refined"
DEST_DATASET = "trusted"
TABLE = "co_menus"

# 22:00 UTC — after the upstream menu-engineering land/dbt window.
# Production never wired an ExternalTaskSensor; schedule is the gate.
SCHEDULE = "0 22 * * *"

dag = DAG(
    dag_id="etl_di_import",
    default_args=default_args,
    schedule_interval=SCHEDULE,
    max_active_runs=1,
    catchup=False,
    tags=["menu-engineering", "bigquery", "cross-project", "bridge"],
)

# Production originally omitted the destination project id and relied on
# the connection default. Made explicit here so DEV/PROD switching is
# visible in the task config, not only in the connection.
import_co_menus = BigQueryToBigQueryOperator(
    task_id="import_co_menus",
    source_project_dataset_tables=(
        f"{MENU_ENG_PROJECT}.{SOURCE_DATASET}.{TABLE}"
    ),
    destination_project_dataset_table=(
        f"{DWH_PROJECT}.{DEST_DATASET}.{TABLE}"
    ),
    write_disposition="WRITE_TRUNCATE",
    create_disposition="CREATE_IF_NEEDED",
    gcp_conn_id=GCP_CONN,
    dag=dag,
)
