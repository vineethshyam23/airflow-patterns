"""
DAG for Address Validation Pipeline

This DAG performs address validation and normalization using smartdatastagdb.ac_addresscheck function
for up to 5,000 records from the adresse table. It updates address fields only if they differ from
validated values and manages historical address status changes.

Key Features:
- Executes SQL script adresscheck.sql on AlloyDB
- Processes up to 5,000 records per run (limit enforced in SQL query)
- Updates only changed address fields
- Maintains historical address status
- Runs during off-peak hours (3am-7am every 3 minutes)
- Sends Slack notifications on success and failure

Configuration:
- Script Location: /home/airflow/gcs/dags/sql/adresscheck.sql
- Retries: 3 attempts with 10-minute delay
- Email notifications on failure
- Slack notifications on success and failure
- Uses AlloyDB PostgreSQL connection

Task Flow:
1. Start Task (EmptyOperator)
2. Execute Address Validation SQL (CloudSQLExecuteQueryOperator)
3. Send Slack notification on success and failure
4. End Task (EmptyOperator)

Dependencies:
- SQL script: adresscheck.sql
- PostgreSQL AlloyDB connection
- smartdatastagdb.ac_addresscheck function

Author: Yousuf Kaleem
Created: 2025-06-12
Last Modified: 2025-01-27 (Added Slack notifications)
"""

from datetime import datetime, timedelta, timezone
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

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

environment = "env"
env = os.environ.get(environment, Variable.get(environment))

if env == "DEV":
    project_id = "source_project"
    bucket_name = "source_app-dwh-rawzone"
    gcp_conn_id = "google_cloud_default"
    gcp_cloudsql_conn_id = "google_alloydb_dev"
    instance_id = "di-migration-sbx"
    database_id = "postgres"
else:
    project_id = "source_project"
    bucket_name = "source_app-dwh-rawzone"
    gcp_conn_id = "google_cloud_default"
    gcp_cloudsql_conn_id = "google_alloydb_dev"
    instance_id = "di-migration-sbx"
    database_id = "postgres"

# Default arguments for the DAG
default_args = {
    "owner": "Yousuf",
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
    "retry_delay": timedelta(minutes=10),
}

def slack_success_notification():
    """
    Sends success notification to Slack channel
    """
    try:
        now_str_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        
        success_message = f"""
        :white_check_mark: *Address Validation* Job is Successfully completed.\n
        *Project*: {project_id}\n
        *Instance Id*: {instance_id}\n
        *Stored Procedure*: smartdatastagdb.ac_addresscheck()\n
        *Finished At*: {now_str_utc}\n
        *Status*: Success\n
        *Details*: Processed up to 5,000 addresses for validation\n
        """
        
        logger.info(f"Attempting to send success Slack notification")
        
        notification = SlackWebhookOperator(
            task_id="slack_success_notification_task",
            slack_webhook_conn_id="slack_conn_di",
            message=success_message,
            channel="#dish-source_app",
            username="source_app Monitoring",
            dag=dag,
        )
        notification.execute(dict())
        logger.info("Success Slack notification sent successfully")
        
    except Exception as e:
        logger.error(f"Error sending success Slack notification: {str(e)}")
        raise

def slack_failure_notification():
    """
    Sends failure notification to Slack channel
    """
    try:
        now_str_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        
        failure_message = f"""
        :x: *Address Validation* Job Failed.\n
        *Project*: {project_id}\n
        *Instance Id*: {instance_id}\n
        *Stored Procedure*: smartdatastagdb.ac_addresscheck()\n
        *Finished At*: {now_str_utc}\n
        *Status*: Failed\n
        *Error*: Task execution failed - check Airflow logs for details\n
        """
        
        logger.info(f"Attempting to send failure Slack notification")
        
        notification = SlackWebhookOperator(
            task_id="slack_failure_notification_task",
            slack_webhook_conn_id="slack_conn_di",
            message=failure_message,
            channel="#dish-source_app",
            username="source_app Monitoring",
            dag=dag,
        )
        notification.execute(dict())
        logger.info("Failure Slack notification sent successfully")
        
    except Exception as e:
        logger.error(f"Error sending failure Slack notification: {str(e)}")
        # Try to send a fallback notification if the main one fails
        try:
            fallback_now_str_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            fallback_message = f"""
            :warning: *Address Validation* Failure Slack notification failed to send.\n
            *Project*: {project_id}\n
            *Instance Id*: {instance_id}\n
            *Error*: {str(e)}\n
            *Time*: {fallback_now_str_utc}\n
            """
            fallback_notification = SlackWebhookOperator(
                task_id="fallback_slack_notification",
                slack_webhook_conn_id="slack_conn_di",
                message=fallback_message,
                channel="#dish-source_app",
                username="source_app Monitoring",
                dag=dag,
            )
            fallback_notification.execute(dict())
            logger.info("Fallback Slack notification sent successfully")
        except Exception as fallback_error:
            logger.error(f"Fallback Slack notification also failed: {str(fallback_error)}")
        raise

# Define the DAG with the specified cron schedule
dag = DAG(
    dag_id="etl_di_address_check",
    default_args=default_args,
    description="Address validation and normalization using ac_addresscheck function",
    #schedule_interval="*/5 0-7 * * *",  # Every 5 minutes between 12am-7am
    schedule_interval="*/8 * * * *",  #TODO: Change to 5 minutes
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,  # Only allow one DAG run at a time is needed because this is based on last processed adress id.
    doc_md=__doc__,
    tags=["address", "validation", "alloydb", "postgresql"],
    template_searchpath="/home/airflow/gcs/dags/sql",
)

start = EmptyOperator(task_id="start", trigger_rule=TriggerRule.ALL_DONE, dag=dag)
end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE, dag=dag)

# Task to execute address validation SQL
address_validation_task = CloudSQLExecuteQueryOperator(
    task_id="execute_address_validation",
    sql="adresscheck.sql",
    gcp_conn_id=gcp_conn_id,
    gcp_cloudsql_conn_id=gcp_cloudsql_conn_id,
    autocommit=True,
    do_xcom_push=True,
    dag=dag,
)

# Task to send success Slack notification
success_notification = PythonOperator(
    task_id="slack_success_notification",
    python_callable=slack_success_notification,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    dag=dag,
)

# Task to send failure Slack notification
failure_notification = PythonOperator(
    task_id="slack_failure_notification",
    python_callable=slack_failure_notification,
    trigger_rule=TriggerRule.ONE_FAILED,
    dag=dag,
)

# Set task dependencies using chain operator
chain(start, address_validation_task, [success_notification, failure_notification], end) 