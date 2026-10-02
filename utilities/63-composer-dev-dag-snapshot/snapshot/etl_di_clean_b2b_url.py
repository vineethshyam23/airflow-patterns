"""
DAG for source_app B2B URL Cleaning Pipeline

This DAG processes and cleans B2B URLs stored in the AlloyDB PostgreSQL database. It normalizes URLs by:
- Removing 'www.' prefixes
- Standardizing URL paths
- Removing common file extensions (.html, .htm)
- Handling index pages
- Normalizing domain names
- Maintaining proper URL structure

Key Features:
- Processes URLs in batches (default limit: 50,000)
- Updates cleaned URLs in the smartdatadb.b2b_urls table
- Implements proper error handling and retries
- Includes comprehensive logging
- Runs every 3 hours

Configuration:
- Connection: Uses 'google_alloydb_dev' connection (GCP Cloud SQL type)
- Database: postgres
- Table: smartdatadb.b2b_urls
- Retries: 3 attempts with 10-minute delay between retries
- Email notifications on failure

Task Flow:
1. Start Task (EmptyOperator)
2. Clean B2B URLs (PythonOperator)
   - Fetches URLs with null url_clean field
   - Processes and normalizes URLs
   - Updates the database with cleaned URLs
3. End Task (EmptyOperator)

Dependencies:
- apache-airflow-providers-google
- psycopg2
- modules.cleanb2b_URLsched

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
from airflow.operators.python import PythonOperator
from modules.cleanb2b_URLsched import Extractor
import os
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

environment = "env"
# env = os.environ.get(environment, Variable.get(environment))
env = "DEV"

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


url_cleaner = Extractor(conn_id=gcp_cloudsql_conn_id)

# Define the DAG
dag = DAG(
    dag_id="etl_di_clean_b2b_url",
    default_args=default_args,
    description="ETL process for Dish source_app data using AlloyDB",
    schedule_interval="* */3 * * *",
    start_date=days_ago(1),
    catchup=False,
    doc_md=__doc__,
    tags=["etl", "alloydb", "dish", "source_app", "cleanb2b_url"],
    template_searchpath="/home/airflow/gcs/dags/sql",
)

start = EmptyOperator(task_id="start", trigger_rule=TriggerRule.ALL_DONE, dag=dag)
end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE, dag=dag)

# Task to execute SQL on AlloyDB
clean_b2b_url = PythonOperator(
    task_id="clean_b2b_url",
    python_callable=getattr(url_cleaner, f"process_urls"),
    dag=dag,
    provide_context=True,
    execution_timeout=timedelta(minutes=300),
)

# Set task dependencies
chain(start, clean_b2b_url, end)
