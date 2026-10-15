"""
DAG for HD Export Refresh

This DAG executes a comprehensive refresh of various materialized views and procedures
relevant for the export process in AlloyDB for PostgreSQL.

Key Features:
- Executes SQL queries stored in external files
- Uses AlloyDB for PostgreSQL as the database
- Implements proper error handling and retries
- Includes logging for better monitoring
- Scheduled to run every Wednesday at 17:00
- Sends Slack notifications on success, partial success, and failure

Configuration:
- Connection: Uses 'google_alloydb_dev' connection (GCP Cloud SQL type)
- Database: postgres
- SQL Files Location: /home/airflow/gcs/dags/sql/
- Retries: 3 attempts with 10-minute delay between retries
- Email notifications on failure
- Slack notifications on success and failure

Task Flow:
1. Start Task (EmptyOperator)
2. Execute SQL statements sequentially (CloudSQLExecuteQueryOperator)
3. Send Slack notification (single, context-aware)
4. End Task (EmptyOperator)

Author: Yousuf Kaleem
Created: 24th June 2025
Last Modified: 2025-01-27 (Added context-aware Slack notification)
"""

from datetime import datetime, timedelta, timezone
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
import re

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

default_args = {
    "owner": "Yousuf",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email": [
        "dataops@example.com",
        "dataops@example.com",
        "dataops@example.com"
    ],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
}

def load_sql_statements(sql_file_path):
    """Load and parse SQL statements from file"""
    with open(sql_file_path, 'r') as file:
        sql = file.read()
    # Remove block comments (/* ... */)
    sql = re.sub(r'/\\*.*?\\*/', '', sql, flags=re.DOTALL)
    # Remove line comments (-- ...)
    sql = re.sub(r'--.*', '', sql)
    # Split on semicolon, but ignore empty statements
    statements = [stmt.strip() for stmt in sql.split(';') if stmt.strip()]
    return statements

def slack_notification_with_context(**context):
    """
    Sends appropriate notification to Slack channel based on task execution results
    """
    try:
        now_str_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        dag_run = context['dag_run']
        task_instances = dag_run.get_task_instances()
        successful_tasks = 0
        failed_tasks = 0
        failed_task_names = []

        for ti in task_instances:
            if ti.task_id.startswith('execute_sql_'):
                if ti.state == 'success':
                    successful_tasks += 1
                elif ti.state == 'failed':
                    failed_tasks += 1
                    failed_task_names.append(ti.task_id)

        total_tasks = len(sql_statements)

        if failed_tasks == 0:
            emoji = ":white_check_mark:"
            status = "Success"
            details = "All materialized views and export procedures refreshed successfully"
            message = f"""
            {emoji} *HD Export Refresh* Job is Successfully completed.\n
            *Project*: {project_id}\n
            *Instance Id*: {instance_id}\n
            *SQL File*: refresh_hd_export.sql\n
            *Statements Executed*: {successful_tasks} / {total_tasks}\n
            *Finished At*: {now_str_utc}\n
            *Status*: {status}\n
            *Details*: {details}\n
            """
        elif successful_tasks > 0:
            emoji = ":warning:"
            status = "Partial Success"
            details = "Some materialized views and export procedures refreshed successfully"
            message = f"""
            {emoji} *HD Export Refresh* Job Partially Completed.\n
            *Project*: {project_id}\n
            *Instance Id*: {instance_id}\n
            *SQL File*: refresh_hd_export.sql\n
            *Statements Executed*: {successful_tasks} / {total_tasks}\n
            *Successful*: {successful_tasks}\n
            *Failed*: {failed_tasks}\n
            *Failed Tasks*: {', '.join(failed_task_names) if failed_task_names else 'None'}\n
            *Finished At*: {now_str_utc}\n
            *Status*: {status}\n
            *Details*: {details}\n
            """
        else:
            emoji = ":x:"
            status = "Failed"
            details = "All materialized views and export procedures failed to refresh"
            message = f"""
            {emoji} *HD Export Refresh* Job Failed.\n
            *Project*: {project_id}\n
            *Instance Id*: {instance_id}\n
            *SQL File*: refresh_hd_export.sql\n
            *Statements Executed*: {successful_tasks} / {total_tasks}\n
            *Failed Tasks*: {', '.join(failed_task_names) if failed_task_names else 'All'}\n
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
            fallback_now_str_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            fallback_message = f"""
            :warning: *HD Export Refresh* Slack notification failed to send.\n
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
    dag_id="etl_di_refresh_hd_export",
    default_args=default_args,
    description="Refreshes materialized views and runs export procedures for HD export in AlloyDB.",
    schedule_interval="0 17 * * 3",  # Every Wednesday at 17:00
    start_date=days_ago(1),
    catchup=False,
    tags=["etl", "alloydb", "hd", "export"],
    template_searchpath="/home/airflow/gcs/dags/sql",
)

start = EmptyOperator(task_id="start", trigger_rule=TriggerRule.ALL_DONE, dag=dag)
end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE, dag=dag)

# Load SQL statements
sql_file_path = '/home/airflow/gcs/dags/sql/refresh_hd_export.sql'
sql_statements = load_sql_statements(sql_file_path)

# Create tasks for each SQL statement
sql_tasks = {}
for i, statement in enumerate(sql_statements):
    task_id = f"execute_sql_{i}"
    sql_tasks[task_id] = CloudSQLExecuteQueryOperator(
        task_id=task_id,
        sql=statement,
        gcp_conn_id=gcp_conn_id,
        gcp_cloudsql_conn_id=gcp_cloudsql_conn_id,
        autocommit=True,
        do_xcom_push=True,
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
chain(
    start,
    *sql_tasks.values(),
    slack_notification,
    end
)