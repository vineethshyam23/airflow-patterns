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
   - Executes update_Table_Kommunikation_und_rating.sql
3. End Task (EmptyOperator)

Dependencies:
- apache-airflow-providers-google
- SQL file: update_Table_Kommunikation_und_rating.sql

Author: Sai Mammahi
Created: 2023-12-20
Last Modified: 2024-03-21
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
    "owner": "Sai",
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

# Define the DAG
dag = DAG(
    dag_id="etl_di_update_communication_and_ratings",
    default_args=default_args,
    description="ETL process for Dish source_app data using AlloyDB",
    schedule_interval="0 19 * * 2",
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
    sql="update_Table_Kommunikation_und_rating.sql",
    gcp_conn_id=gcp_conn_id,
    gcp_cloudsql_conn_id=gcp_cloudsql_conn_id,
    autocommit=True,
    dag=dag,
)

# Set task dependencies
chain(start, execute_sql, end)
