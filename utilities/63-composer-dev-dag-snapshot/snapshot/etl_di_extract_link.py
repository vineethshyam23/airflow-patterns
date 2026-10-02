"""
DAG for source_app link extraction Pipeline

This DAG is designed to process HTML documents from a database, extract links and keywords, and insert relevant information back into the database. 
It uses a combination of SQL queries, regular expressions, and HTML parsing.

Key Features:
- Regex-based pattern matching.
- Iterates over documents:
    - Cleans and parses HTML.
    - Extracts links and checks for keyword matches.
    - Inserts new links and keyword matches into the database.
    - Processes up to 1000 latest HTML documents per run (limit enforced in SQL query)
- Implements proper exception and error handling and retries.
- Includes comprehensive logging.

Configuration:
- Connection: Uses 'google_alloydb_dev' connection (GCP Cloud SQL type)
- Database: postgres
- Retries: 3 attempts with 5-minute delay between retries
- Slack notifications on success and failure 

Task Flow:
1. Start Task (EmptyOperator)
2. extract link by id (PythonOperator)
    - Fetches the latest HTML documents from the database that haven’t been processed yet.
    - Parses each document, extracts links and keywords using regex.
    - Inserts new links and keyword matches into the database, ensuring no duplicates.
    - Logs errors and progress for debugging and monitoring.
3. SlackNotificationOperator: Sends a slack notification on failure and success
4. End Task (EmptyOperator)

Dependencies:
- apache-airflow-providers-google
- psycopg2
- apache-airflow-providers-slack
- modules.extract_link_by_id
- Tables used: smartdatadb.b2b_html, smartdatadb.b2b_urls, smartdatadb.b2b_keywords_match, smartdatadb.b2b_keywords, smartdatadb.idxmapping
- Stored Procedure used: smartdatastagdb.insert_url_idxmapping

Author: Rashmi Kedari
Created: 2025-06-12
Last Modified: 2025-06-12
"""
import os
import logging
from datetime import datetime, timedelta,timezone
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
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from modules.extract_link_by_id import main_function
from airflow.decorators import task_group
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

environment = "env"
#env = os.environ.get(environment, Variable.get(environment))
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
    "owner": "Rashmi",
    "depends_on_past": False,
    "start_date": days_ago(1),
    #  Integrated Slack notification
    # "email": [
    #     "dataops@example.com",
    #     "dataops@example.com",
    #     "dataops@example.com",
    #     "dataops@example.com"
    # ],
    #"email_on_failure": True,
    #"email_on_retry": False,
    "retries":3, 
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=8),
    #"max_active_tasks": 5,
    #"max_active_runs": 1
}

def slack_notification(ti,**kwargs):
    try:
     # Get the task instance state for 'extract_link_by_id'
        dag_run = kwargs['dag_run']
        task_instance = dag_run.get_task_instance('extract_link_by_id') 
        state = task_instance.state  # 'success', 'failed', etc.

        now_str_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        if state == "success":
            emoji = ":white_check_mark:" 
            final_message = f"""
            {emoji} *etl_di_extract_link* Job is Successfully completed.\n
            *Project*: {project_id}\n
            *Instance Id*: {instance_id}\n
            *Stored Procedure*: smartdatastagdb.insert_url_idxmapping()\n
            *Finished At*: {now_str_utc}\n
            *Status*: Success\n
            """
        else:
            emoji = ":x:"
            final_message = f"""
            {emoji} *etl_di_extract_link* Job Failed.\n
            *Project*: {project_id}\n
            *Instance Id*: {instance_id}\n
            *Stored Procedure*: smartdatastagdb.insert_url_idxmapping()\n
            *Finished At*: {now_str_utc}\n
            *Status*: Failed \n
            """

        notification = SlackWebhookOperator(
            task_id="slack_notification_task",
            slack_webhook_conn_id="slack_conn_di",
            message=final_message,
            channel="#dish-source_app",
            username="source_app Monitoring",
            dag=dag,
        )
        notification.execute(dict())
        logger.info("Slack notification sent successfully")

    except Exception as e:
        logger.error(f"Error sending Slack notification: {str(e)}")
        raise
    
#link_extractor = LinkExtractor(conn_id=gcp_cloudsql_conn_id)

# Define the DAG
dag = DAG(
    dag_id="etl_di_extract_link",
    default_args=default_args,
    description="ETL process for Dish source_app data using AlloyDB",
    schedule_interval="10 */1 * * 1,2,3,6,7", #The job runs at 10 minutes past every hour, but only on Monday, Tuesday, Wednesday, Saturday, and Sunday.
    #schedule_interval="*/5 * * * *", # Run every minute
    start_date=days_ago(1),
    catchup=False,
    doc_md=__doc__,
    tags=["etl", "alloydb", "dish", "source_app", "Parsing_Processes"],
    template_searchpath="/home/airflow/gcs/dags/modules",
)

start = EmptyOperator(task_id="start", trigger_rule=TriggerRule.ALL_DONE, dag=dag)
end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE, dag=dag)

# Task to execute SQL on AlloyDB
extract_link_by_id = PythonOperator(
    task_id="extract_link_by_id",
    python_callable=main_function,
    dag=dag,
    provide_context=True,
    execution_timeout=timedelta(minutes=300),
)

slacknotification = PythonOperator(
    task_id="slacknotification",
    provide_context=True,
    python_callable=slack_notification,
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
)

# Set task dependencies
chain(start, extract_link_by_id , slacknotification , end)
