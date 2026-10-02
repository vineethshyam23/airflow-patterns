"""
DAG for source_app Data Pipeline

This DAG executes SQL queries on AlloyDB for PostgreSQL to process and update data for the source_app project.

Key Features:
- Executes SQL queries stored in external files
- Uses AlloyDB for PostgreSQL as the database
- Implements proper error handling and retries
- Includes logging for better monitoring
- Manual trigger (no schedule)

Configuration:
- Connection: Uses 'google_alloydb_dev' connection (GCP Cloud SQL type)
- Database: postgres
- SQL Files Location: /home/airflow/gcs/dags/sql/
- Retries: 3 attempts with 10-minute delay between retries
- Email notifications on failure

Task Flow:
1. Start Task (EmptyOperator)
2. Execute SQL Query (CloudSQLExecuteQueryOperator)
   - Executes start_matchingall.sql
3. End Task (EmptyOperator)

Dependencies:
- apache-airflow-providers-google
- SQL file: start_matchingall.sql

Author: Apurva Ghume
Created: 2025-06-11
Last Modified: 2025-06-11
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.cloud_sql import (
    CloudSQLExecuteQueryOperator,
)
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.utils.helpers import chain
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.empty import EmptyOperator
import os
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

environment = "env"
env = os.environ.get(environment, Variable.get(environment))

if env == "DEV":
    # odoo_wsl_creds = Variable.get("odoo_wsl_creds")
    project_id = "source_project"
    bucket_name = "source_app-dwh-rawzone"
    gcp_conn_id = "google_cloud_default"
    gcp_cloudsql_conn_id = "google_alloydb_dev"
    # AlloyDB instance details for DEV
    instance_id = "di-migration-sbx"  # Replace with your actual instance ID
    database_id = "postgres"  # Replace with your actual database name

else:
    project_id = "source_project"
    bucket_name = "source_app-dwh-rawzone"
    gcp_conn_id = "google_cloud_default"
    gcp_cloudsql_conn_id = "google_alloydb_dev"
    # AlloyDB instance details for PROD
    instance_id = "di-migration-sbx"  # Replace with your actual instance ID
    database_id = "postgres"  # Replace with your actual database name

# Default arguments for the DAG
default_args = {
    "owner": "Apurva",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email": [
        "dataops@example.com",
        "dataops@example.com",
        "dataops@example.com",
    ],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
}

# "*/17 * * * 1,2,6,7"

# Define the DAG
dag = DAG(
    dag_id="etl_di_matching_process_per_country",
    default_args=default_args,
    description="ETL process for Dish source_app data using AlloyDB",
    schedule_interval="*/17 * * * 1,2,6,7",
    start_date=days_ago(1),
    catchup=False,
    doc_md=__doc__,
    tags=["etl", "alloydb", "dish", "source_app"],
    template_searchpath="/home/airflow/gcs/dags/sql",
)

start = EmptyOperator(task_id="start", trigger_rule=TriggerRule.ALL_DONE, dag=dag)
end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE, dag=dag)

# Task to execute SQL on AlloyDB
execute_sql = CloudSQLExecuteQueryOperator(
    task_id="execute_sql_alloydb",
    sql="start_matchingall.sql",
    gcp_conn_id=gcp_conn_id,
    gcp_cloudsql_conn_id=gcp_cloudsql_conn_id,
    autocommit=True,
    dag=dag,
)

# # Task to execute SQL on AlloyDB
# execute_sql_analyze = CloudSQLExecuteQueryOperator(
#     task_id="execute_sql_alloydb_analyze",
#     sql="vacuum (analyze, verbose) smartdatastagdb.matching_base;",
#     gcp_conn_id=gcp_conn_id,
#     gcp_cloudsql_conn_id=gcp_cloudsql_conn_id,
#     autocommit=True,
#     dag=dag,
# )

# Set task dependencies
chain(start, execute_sql, #execute_sql_analyze,
end)
