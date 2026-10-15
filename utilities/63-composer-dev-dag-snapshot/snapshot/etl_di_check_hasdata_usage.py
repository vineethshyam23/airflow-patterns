"""
Utility script to check HasData API credit usage and account status.

This script provides a simple interface to check the remaining credits and usage statistics
for your HasData API account. It's useful for monitoring API consumption and planning.

Typical usage:
    $ python hasdata_usage_api.py

Returns:
    Prints current HasData API usage statistics to stdout
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.models import Variable
from airflow.utils.helpers import chain
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.empty import EmptyOperator
import os
import logging
from datetime import datetime, timezone
import requests
import json
from airflow.utils.dates import days_ago

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
   
else:
    project_id = "source_project"
    bucket_name = "source_app-dwh-rawzone"
    gcp_conn_id = "google_cloud_default"
  

# Dev DB credentials
dev_creds = Variable.get("alloydb_dev_details", deserialize_json=True)

api_key = Variable.get("di_hasdata_apikey")

# Default arguments for the DAG
default_args = {
    "owner": "Rashmi Kedari",
    "depends_on_past": False,
    "start_date": days_ago(1),
    # "email": [
    #     "dataops@example.com",
    #     "dataops@example.com",
    # ],
    # "email_on_failure": True,
    # "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}

# Define the DAG
dag = DAG(
    dag_id="etl_di_check_hasdata_usage",
    default_args=default_args,
    description="Check HasData API usage",
    schedule_interval="0 1 * * *", #Runs every day at 1:00 AM
    start_date=days_ago(1),
    catchup=False,
    doc_md=__doc__,
    tags=["etl", "alloydb", "dish", "source_app"],
    template_searchpath="/home/airflow/gcs/dags/sql",
)

def check_hasdata_usage_function(api_key: str) -> dict:
    """Check HasData API usage and return usage data."""
    try:
        response = requests.get(
            'https://api.hasdata.com/user/me/usage',
            headers={
                'x-api-key': api_key,
                'Content-Type': 'application/json'
            }
        )
        response.raise_for_status()
        usage_data = response.json()

        print(f"HasData Usage as of {datetime.now()}:")
        print(json.dumps(usage_data, indent=4))
        
        # Return usage_data so it gets pushed to XCom
        return usage_data
        
    except requests.exceptions.RequestException as e:
        print(f"Error checking usage: {str(e)}")    
        raise e

def slack_notification(ti, **kwargs):
    """Send Slack notification with HasData usage information."""
    try:
        # Get the task instance state for 'check_hasdata_usage'
        dag_run = kwargs['dag_run']
        task_instance = dag_run.get_task_instance('check_hasdata_usage') 
        state = task_instance.state  # 'success', 'failed', etc.

        now_str_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        if state == "success":
            # Pull XCom data from the previous task
            xcom_data = ti.xcom_pull(task_ids='check_hasdata_usage', key='return_value')
            
            # Format XCom content for display
            if xcom_data:
                usage_info = ""
                if isinstance(xcom_data, dict):
                    # Format the usage data in a readable way
                    usage_info = "\n*HasData API Usage Details:*\n"
                    for key, value in xcom_data.items():
                        # Format key names nicely (e.g., 'creditsRemaining' -> 'Credits Remaining')
                        formatted_key = key.replace('_', ' ').title()
                        usage_info += f"• *{formatted_key}*: {value}\n"
                else:
                    usage_info = f"\n*Usage Data:*\n```{json.dumps(xcom_data, indent=2)}```\n"
            else:
                usage_info = "\n*Usage Data:* Not available\n"
            
            emoji = ":white_check_mark:" 
            final_message = f"""
{emoji} *etl_di_check_hasdata_usage* Job is Successfully completed.
*Project*: {project_id}
*Function*: check_hasdata_usage_function()
*Finished At*: {now_str_utc}
*Status*: Success
{usage_info}
"""
        else:
            emoji = ":x:"
            final_message = f"""
{emoji} *etl_di_check_hasdata_usage* Job Failed.
*Project*: {project_id}
*Function*: check_hasdata_usage_function()
*Finished At*: {now_str_utc}
*Status*: Failed
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

start = EmptyOperator(task_id="start", trigger_rule=TriggerRule.ALL_DONE, dag=dag)
end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE, dag=dag)

# Script to check how many credits are left
check_hasdata_usage = PythonOperator(
    task_id="check_hasdata_usage",
    python_callable=check_hasdata_usage_function,
    op_kwargs={"api_key": api_key},
    do_xcom_push=True, 
    provide_context=True,
    dag=dag,
)
slacknotification = PythonOperator(
    task_id="slacknotification",
    provide_context=True,
    python_callable=slack_notification,
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
)

chain(start, check_hasdata_usage, slacknotification, end)

