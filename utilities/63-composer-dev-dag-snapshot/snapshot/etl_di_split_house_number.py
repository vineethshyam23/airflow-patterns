
"""
DAG for source_app Data Pipeline

This DAG executes SQL queries on AlloyDB for PostgreSQL to process and update data for the source_app project.
A stored procedure that splits house numbers from street names in address data. 

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
- Retries: 3 attempts with 5-minute delay between retries
- slack notification on failure and success

Task Flow:
1. Start Task (EmptyOperator)
2. Execute SQL Query (CloudSQLExecuteQueryOperator)
   - Executes ac_splithausnr.sql
    - Identifies addresses where hausnr is null 
    - Uses regex to extract house number from street name 
    - Separates street name and house number 
    - Updates address records with split data 
    - Includes performance monitoring and logging 
    - Processes in batches with commits every 200 records 
    - Filters by country code (LKZ) and configured countries
3. SlackNotificationOperator: Sends a slack notification on failure and success
4. End Task (EmptyOperator)

Dependencies:
- apache-airflow-providers-google
- SQL file: ac_splithausnr.sql

Author: Rashmi Kedari
Created: 2025-06-11
Last Modified: 2025-06-11
"""
import os
import logging
from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.providers.google.cloud.operators.cloud_sql import (
    CloudSQLExecuteQueryOperator,
)
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.utils.helpers import chain
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator


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
    "owner": "Rashmi",
    "depends_on_past": False,
    "start_date": days_ago(1),
    #  Integrated Slack notification
    # "email": [
    #     "dataops@example.com",
    #     "dataops@example.com",
    #     "dataops@example.com",
    #     "dataops@example.com",
    # ],
    # "email_on_failure": True,
    # "email_on_retry": False,
    "retries": 3,  
    "retry_delay": timedelta(minutes=5),#reduced retry delay as schedule interval is 17 minutes
}

def slack_notification(ti,**kwargs):
    try:
     # Get the task instance state for 'execute_sql'
        dag_run = kwargs['dag_run']
        task_instance = dag_run.get_task_instance('execute_sql')
        state = task_instance.state  # 'success', 'failed', etc.

        now_str_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        if state == "success":
            emoji = ":white_check_mark:" 
            final_message = f"""
            {emoji} *etl_di_split_house_number* Job is Successfully completed.\n
            *Project*: {project_id}\n
            *Instance Id*: {instance_id}\n
            *Stored Procedure*: smartdatadb.ac_splithausnr()\n
            *Finished At*: {now_str_utc}\n
            *Status*: Success\n
            """
        else:
            emoji = ":x:"
            final_message = f"""
            {emoji} *etl_di_split_house_number* Job Failed.\n
            *Project*: {project_id}\n
            *Instance Id*: {instance_id}\n
            *Stored Procedure*: smartdatadb.ac_splithausnr()\n
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

# Define the DAG
dag = DAG(
    dag_id="etl_di_split_house_number",
    default_args=default_args,
    description="ETL process for Dish source_app data using AlloyDB",
    schedule_interval="*/17 * * * *", #runs the task every 17 minutes
    start_date=days_ago(1),
    catchup=False,
    doc_md=__doc__,
    tags=["etl", "alloydb", "dish", "source_app","Cleaning_Processes"],
    template_searchpath="/home/airflow/gcs/dags/sql",
)

start = EmptyOperator(task_id="start", trigger_rule=TriggerRule.ALL_DONE, dag=dag)
end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE, dag=dag)

# Task to execute SQL on AlloyDB
execute_sql = CloudSQLExecuteQueryOperator(
    task_id="execute_sql",
    sql="ac_splithausnr.sql",
    gcp_conn_id=gcp_conn_id,
    gcp_cloudsql_conn_id=gcp_cloudsql_conn_id,
    autocommit=True,
    dag=dag,
)
slacknotification = PythonOperator(
    task_id="slacknotification",
    provide_context=True,
    python_callable=slack_notification,
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
)
# Set task dependencies
chain(start, execute_sql, slacknotification, end)