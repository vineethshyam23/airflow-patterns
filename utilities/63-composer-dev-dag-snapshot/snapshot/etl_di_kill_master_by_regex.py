"""
DAG for source_app Data Pipeline

This DAG executes Python script on AlloyDB for PostgreSQL to process and update data for the source_app project.

Key Features:
- Executes SQL queries stored in external files
- Uses AlloyDB for PostgreSQL as the database
- Implements proper error handling and retries
- Includes logging for better monitoring
- Manual trigger (no schedule)

Configuration:
- Connection: Uses 'google_alloydb_dev' connection (GCP Cloud SQL type)
- Database: postgres
- SQL Files Location: /home/airflow/gcs/dags/modules/
- Retries: 3 attempts with 2-minute delay between retries
- Email notifications on failure

Task Flow:
1. Start Task (EmptyOperator)
2. Execute Python Script (PythonOperator)
   - Executes kill_master_by_regex.py
3. Send Slack notification on success and failure of the stored procedure
4. End Task (EmptyOperator)

Dependencies:
- apache-airflow-providers-google
- Python file: kill_master_by_regex.py

Author: Apurva Ghume
Created: 2025-06-25
Last Modified: 2025-06-27
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.cloud_sql import (
    CloudSQLExecuteQueryOperator,
)
from airflow.operators.python import PythonOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.utils.helpers import chain
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.empty import EmptyOperator
import os
import logging
from datetime import datetime, timezone
from modules.kill_master_by_regex import main_function

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
        "dataops@example.com",
    ],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}

def slack_notification(ti):
    """
    Triggers the task status message in text format to slack channel

    :param ti: task_instance from `execute_kill_master_by_regex` task
    :return None|SlackWebhookOperator trigger: if `ti` is empty then it
    returns None else it sends a message to slack channel
    """
    try:
        dbt_status = ti.xcom_pull(task_ids="execute_kill_master_by_regex", key="return_value")
        
        # Handle case where dbt_status might be None
        if dbt_status is None:
            logger.warning("No status returned from execute_kill_master_by_regex task")
            return
            
        if len(dbt_status) > 0:
            now_str_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            
            if "Failed" in dbt_status:
                emoji = ":x:"
                final_message = f"""
                {emoji} *Kill Master By Regex* Job Failed.\n
                *Project*: {project_id}\n
                *Instance Id*: {instance_id}\n
                *Stored Procedure*: smartdatadb.kill_master_by_regex()\n
                *Finished At*: {now_str_utc}\n
                *Status*: Failed\n
                """
            else:
                emoji = ":white_check_mark:" 
                final_message = f"""
                {emoji} *Kill Master By Regex* Job is Successfully completed.\n
                *Project*: {project_id}\n
                *Instance Id*: {instance_id}\n
                *Stored Procedure*: smartdatadb.kill_master_by_regex()\n
                *Finished At*: {now_str_utc}\n
                *Status*: Success\n
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
        else:
            logger.info("No status message to send to Slack")
    except Exception as e:
        logger.error(f"Error sending Slack notification: {str(e)}")
        raise

# 0 14 * * 2,4,6

# Define the DAG
dag = DAG(
    dag_id="etl_di_kill_master_by_regex",
    default_args=default_args,
    description="ETL process for Dish source_app data using AlloyDB",
    schedule_interval="0 14 * * 2,4,6",
    start_date=days_ago(1),
    catchup=False,
    doc_md=__doc__,
    tags=["etl", "alloydb", "dish", "source_app"],
    template_searchpath="/home/airflow/gcs/dags/sql",
)

start = EmptyOperator(task_id="start", trigger_rule=TriggerRule.ALL_DONE, dag=dag)
end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE, dag=dag)

# Task to execute Python script on AlloyDB
execute_kill_master_by_regex = PythonOperator(
    task_id="execute_kill_master_by_regex",
    python_callable=main_function,
    do_xcom_push=True,
    dag=dag,
    provide_context=True,
    # execution_timeout=timedelta(minutes=300),
)

notification = PythonOperator(
    task_id="slack_notification",
    provide_context=True,
    python_callable=slack_notification,
    dag=dag,
)

# Set task dependencies
chain(start,execute_kill_master_by_regex,notification, end)
