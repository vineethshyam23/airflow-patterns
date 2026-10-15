"""
DAG for source_app Keyword Match Refresh

This DAG executes SQL queries on AlloyDB for PostgreSQL to refresh keyword matching data for HD export.

Key Features:
- Executes SQL queries stored in external files
- Uses AlloyDB for PostgreSQL as the database
- Implements proper error handling and retries
- Includes logging for better monitoring
- Weekly schedule (Monday at 19:30)

Configuration:
- Connection: Uses 'google_alloydb_dev' connection (GCP Cloud SQL type)
- Database: postgres
- SQL Files Location: /home/airflow/gcs/dags/sql/
- Retries: 3 attempts with 10-minute delay between retries
- Email notifications on failure

Task Flow:
1. Start Task (EmptyOperator)
2. Execute SQL Query (CloudSQLExecuteQueryOperator)
   - Executes refresh_hd_export_Keywordmatch.sql
3. Slack Notification (PythonOperator)
4. End Task (EmptyOperator)

Dependencies:
- apache-airflow-providers-google
- SQL file: refresh_hd_export_Keywordmatch.sql

Author: Yousuf Kaleem
Created: 2025-06-25
Last Modified: 2025-07-03
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.cloud_sql import CloudSQLExecuteQueryOperator
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

# Environment configuration
environment = "env"
env = os.environ.get(environment, Variable.get(environment))

if env == "DEV":
    project_id = "source_project"
    bucket_name = "source_app-dwh-rawzone"
    gcp_conn_id = "google_cloud_default"
    gcp_cloudsql_conn_id = "google_alloydb_dev"
    # AlloyDB instance details for DEV
    instance_id = "di-migration-sbx"
    database_id = "postgres"
else:
    project_id = "source_project"
    bucket_name = "source_app-dwh-rawzone"
    gcp_conn_id = "google_cloud_default"
    gcp_cloudsql_conn_id = "google_alloydb_dev"
    # AlloyDB instance details for PROD
    instance_id = "di-migration-sbx"
    database_id = "postgres"

# Default arguments for the DAG
default_args = {
    "owner": "Yousuf",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
    "email": [
        "dataops@example.com",
        "dataops@example.com"
    ],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
}

def slack_notification_with_context(**context):
    """
    Sends appropriate notification to Slack channel based on task execution results
    """
    try:
        now_str_utc = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        dag_run = context['dag_run']
        task_instances = dag_run.get_task_instances()
        sql_task_id = "execute_keyword_sql"
        sql_task_state = None
        for ti in task_instances:
            if ti.task_id == sql_task_id:
                sql_task_state = ti.state
                break

        if sql_task_state == "success":
            emoji = ":white_check_mark:"
            status = "Success"
            details = "Keyword match refresh completed successfully"
            message = f"""
            {emoji} *Keyword Match Refresh* Job is Successfully completed.\n
            *Project*: {project_id}\n
            *Instance Id*: {instance_id}\n
            *SQL File*: refresh_hd_export_Keywordmatch.sql\n
            *Finished At*: {now_str_utc}\n
            *Status*: {status}\n
            *Details*: {details}\n
            """
        else:
            emoji = ":x:"
            status = "Failed"
            details = "Keyword match refresh failed"
            message = f"""
            {emoji} *Keyword Match Refresh* Job Failed.\n
            *Project*: {project_id}\n
            *Instance Id*: {instance_id}\n
            *SQL File*: refresh_hd_export_Keywordmatch.sql\n
            *Finished At*: {now_str_utc}\n
            *Status*: {status}\n
            *Details*: {details}\n
            """

        logger.info(f"Attempting to send Slack notification for status: {status}")

        notification = SlackWebhookOperator(
            task_id="slack_notification_task",
            slack_webhook_conn_id="slack_conn_di",
            message=message,
            channel="#dish-source_app",
            username="source_app Monitoring",
            dag=dag,
        )
        notification.execute(dict())
        logger.info(f"Slack notification sent successfully for status: {status}")

    except Exception as e:
        logger.error(f"Error sending Slack notification: {str(e)}")
        # Try to send a fallback notification if the main one fails
        try:
            fallback_now_str_utc = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            fallback_message = f"""
            :warning: *Keyword Match Refresh* Slack notification failed to send.\n
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

# Define the DAG
dag = DAG(
    dag_id="etl_di_refresh_hd_export_keyword_match",
    default_args=default_args,
    description="Refreshes keyword matching data for HD export",
    schedule_interval="30 19 * * 1",  # Every Monday at 19:30
    start_date=datetime(2023, 1, 1),
    catchup=False,
    doc_md=__doc__,
    tags=["etl", "alloydb", "dish", "source_app"],
    template_searchpath="/home/airflow/gcs/dags/sql",
)

start = EmptyOperator(task_id="start", trigger_rule=TriggerRule.ALL_DONE, dag=dag)
end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE, dag=dag)

# Task to execute SQL on AlloyDB
execute_keyword_sql = CloudSQLExecuteQueryOperator(
    task_id="execute_keyword_sql",
    sql="refresh_hd_export_Keywordmatch.sql",
    gcp_conn_id=gcp_conn_id,
    gcp_cloudsql_conn_id=gcp_cloudsql_conn_id,
    autocommit=True,
    dag=dag,
)

# Task to send Slack notification with context
slack_notification = PythonOperator(
    task_id="slack_notification",
    python_callable=slack_notification_with_context,
    provide_context=True,
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
)

# Set task dependencies using chain operator
chain(start, execute_keyword_sql, slack_notification, end)