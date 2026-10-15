"""
DAG for source_app Export Process Pipeline which will run on everyfriday at 2:00 AM.

Key Features:
- This dag will take export data from source_app project postgres alloydb to source_app bigquery. 
- This data transfer is reducing overhead of gcs to gcs storage transfer.

Configuration:
- Connection: Uses 'google_alloydb_dev' connection (GCP Cloud SQL type)
- Database: postgres
- Retries: 3 attempts with 10-minute delay between retries
- Slack notifications on failure and success

Task Flow:  
1. Start Task (EmptyOperator)
2. Execute SQL Query (CloudSQLExecuteQueryOperator)
   - Executes the SQL query to refresh the materialized views 
4. Export Establishment data to BigQuery (PythonOperator)
   - Export Establishment data to BigQuery -can be updated to menuitems later
5. Take Backup of the data (CloudSQLExecuteQueryOperator)
    - run store procedure smartdatastagdb.backup_table_smartdata_analytic_stamm()
5. SlackNotificationOperator: Sends a slack notification on failure and success
6. End Task (EmptyOperator)

Dependencies:
- apache-airflow-providers-google
- psycopg2
- apache-airflow-providers-slack
- modules.export_estdata_frm_deepalloydb_to_hdstreambq
- modules.export_menuitems_frm_deepalloydb_to_hdstreambq
- Materialized Views Used: smartdatastagdb.mv_menufilterall, smartdatastagdb.mv_menuitems_final

- Tables used: smartdatastagdb.stamm_e_export
- Stored Procedures used: smartdatastagdb.backup_table_smartdata_analytic_stamm()

Author: Rashmi Kedari
Created: 2025-07-10
Last Modified: 2025-11-26
"""
#Views used: smartdatastagdb.v_menuitems_final, smartdatastagdb.v_fb_delivery_data_establishment_new

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
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from modules.export_menuitems_frm_alloydb_to_bq import exp_menuitems_main_function 
from modules.export_estdata_frm_alloydb_to_bq import exp_establishment_main_function
from airflow.decorators import task_group
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance


# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

environment = "env"
env = os.environ.get(environment, Variable.get(environment))

# source project details
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
    "retry_delay": timedelta(minutes=5),  # Increased retry delay 
    "execution_timeout": timedelta(
        hours=15
    ), 
    #"max_active_runs_per_dag": 1,  
    "max_active_runs": 1,
    "max_active_tasks": 1,
}

# Define the DAG
dag = DAG(
    dag_id="etl_di_refresh_hd_export_update",
    default_args=default_args,
    description="ETL process for Dish source_app data using AlloyDB",
    schedule_interval="0 2 * * 5", #This schedule runs every Friday at 2:00 AM. 
    start_date=days_ago(1),
    catchup=False,
    doc_md=__doc__,
    tags=["etl", "alloydb", "dish", "source_app", "Export_Processes"],
    template_searchpath="/home/airflow/gcs/dags/modules",
)

start = EmptyOperator(task_id="start", trigger_rule=TriggerRule.ALL_DONE, dag=dag)
step1 = EmptyOperator(task_id="step1", trigger_rule=TriggerRule.ALL_DONE, dag=dag)
step2 = EmptyOperator(task_id="step2", trigger_rule=TriggerRule.ALL_DONE, dag=dag)
step3 = EmptyOperator(task_id="step3", trigger_rule=TriggerRule.ALL_DONE, dag=dag)
step4 = EmptyOperator(task_id="step4", trigger_rule=TriggerRule.ALL_DONE, dag=dag)
step5 = EmptyOperator(task_id="step5", trigger_rule=TriggerRule.ALL_DONE, dag=dag)
end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE, dag=dag)


# Task to execute SQL on AlloyDB
refresh_mv_menufilterall = CloudSQLExecuteQueryOperator(
    task_id="refresh_mv_menufilterall",
    sql=" REFRESH MATERIALIZED VIEW smartdatastagdb.mv_menufilterall; ",
    gcp_conn_id=gcp_conn_id,
    gcp_cloudsql_conn_id=gcp_cloudsql_conn_id,
    autocommit=True,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    dag=dag,
)

# Task to execute SQL on AlloyDB dependent on refresh_mv_menufilterall
refresh_mv_menuitems_final = CloudSQLExecuteQueryOperator(
    task_id="refresh_mv_menuitems_final",
    sql=" REFRESH MATERIALIZED VIEW smartdatastagdb.mv_menuitems_final; ",
    gcp_conn_id=gcp_conn_id,
    gcp_cloudsql_conn_id=gcp_cloudsql_conn_id,
    autocommit=True,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    dag=dag,
)

# take backup of both tables
take_backup = CloudSQLExecuteQueryOperator(
    task_id="take_backup",
    sql=" CALL smartdatastagdb.backup_table_smartdata_analytic_stamm(); ",
    gcp_conn_id=gcp_conn_id,
    gcp_cloudsql_conn_id=gcp_cloudsql_conn_id,
    autocommit=True,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    dag=dag,
)

tbl_lst=['establishment'] #,'menuitems']
export_group_lst=[]
#menu_export_task_lst = []
est_export_task_lst = []
for tbl in tbl_lst:
    try:
        @task_group(group_id=f"{tbl}_export_group",dag=dag)
        def export_tasks_group():
            #mi_total_buckets = 30
            et_total_buckets = 20
            # if tbl == 'menuitems':
            #         for bucket_num in range(1, mi_total_buckets + 1): 
            #             if bucket_num % 8 == 0:   
            #                 menu_pause_task = EmptyOperator(task_id=f"menu_pause_task_{bucket_num}", trigger_rule=TriggerRule.ALL_DONE, dag=dag)
            #                 menu_export_task_lst.append(menu_pause_task) 
            #             export_task = PythonOperator(
            #                 task_id=f"{tbl}_export_bucket_{bucket_num}",
            #                 provide_context=True,
            #                 python_callable=exp_menuitems_main_function,
            #                 op_kwargs={
            #                     "menu_total_buckets": mi_total_buckets, 
            #                     "menu_ntile_bucket": bucket_num,
            #                 },
            #                 trigger_rule=TriggerRule.ALL_SUCCESS,
            #                 #execution_timeout=timedelta(hours=13),
            #                 dag=dag,
            #             )
            #             menu_export_task_lst.append(export_task) 

            #         chain(*menu_export_task_lst)
                    
            # elif tbl == 'establishment':
            if tbl == 'establishment':
                for bucket_num in range(1, et_total_buckets + 1): 
                    if bucket_num % 7 == 0:   
                        est_pause_task = EmptyOperator(task_id=f"est_pause_task_{bucket_num}", trigger_rule=TriggerRule.ALL_DONE, dag=dag)
                        est_export_task_lst.append(est_pause_task) 
                    export_task = PythonOperator(
                        task_id=f"{tbl}_export_bucket_{bucket_num}",
                        provide_context=True,
                        python_callable=exp_establishment_main_function,
                        op_kwargs={
                            "est_total_buckets": et_total_buckets, 
                            "est_ntile_bucket": bucket_num,
                        },
                        trigger_rule=TriggerRule.ALL_SUCCESS,
                        #execution_timeout=timedelta(hours=12),
                        dag=dag,
                    )
                    est_export_task_lst.append(export_task)
                chain(*est_export_task_lst)
        export_group_lst.append(export_tasks_group())
    except Exception as e:
        logging.error(f"Error while running export tasks for {tbl}: {str(e)}")
        raise Exception(f"Error while running export tasks for {tbl}: {str(e)}")


# ------------------------------------------------------------------------------------------------
# Notification Tasks
# ------------------------------------------------------------------------------------------------


# def check_all_success(**context):
#     """
#     Collects task status information for Slack notification

#     :param context["ti"]: task_instance of current task
#     :param context["dag_run"]: dag_instance of current dag
#     :return ti_summary: Returns the dictionary of the task status of all the
#     tasks in the current dag
#     """
#     dr: DagRun = context["dag_run"]
#     ti: TaskInstance = context["ti"]

#     # Define tasks to exclude from status check
#     exclude_tasks = {
#         "slacknotification",  
#         "start",
#         "step1",
#         "step2",
#         "step3",
#         "step4",
#         "step5",
#         "end",
#         "check_all_tasks",
#     }

#     # Collect task status excluding the current task and control tasks
#     ti_summary = {
#         task.task_id: task.state
#         for task in dr.get_task_instances()
#         if task.task_id != ti.task_id and task.task_id not in exclude_tasks
#     }

#     return ti_summary


# check_all_tasks = PythonOperator(
#     task_id="check_all_tasks",
#     python_callable=check_all_success,
#     provide_context=True,
#     trigger_rule=TriggerRule.ALL_DONE,
#     do_xcom_push=True,
#     dag=dag,
# ) 


# def send_slack_notification(**context):
#     """
#     Optimized Slack notification function that sends consolidated notifications

#     :param context: Airflow context containing task instance and dag run info
#     """
#     try:
#         ti = context["ti"]
#         task_status = ti.xcom_pull(task_ids="check_all_tasks", key="return_value")

#         if not task_status:
#             logger.warning("No task status found in XCom")
#             return

#         # Filter failed tasks
#         failed_tasks = {
#             task: state for task, state in task_status.items() if state == "failed"
#         }

#         # Get current timestamp
#         now_str_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

#         # Calculate success rate
#         total_tasks = len(task_status)
#         failed_count = len(failed_tasks)
#         success_count = total_tasks - failed_count
#         #success_rate = (success_count / total_tasks * 100) if total_tasks > 0 else 0

#         # Create notification message
#         if failed_tasks:
#             # Failure notification
#             emoji = ":x:"
#             failed_task_list = "\n".join(
#                 [f"• {task}: {state}" for task, state in failed_tasks.items()]
#             )

#             message = f"""{emoji} *source_app Export Process Failed*

# *Project*: `{project_id}`
# *Instance*: `{instance_id}`
# *Environment*: `{env}`
# *Execution Time*: {now_str_utc}
# *Summary*:
# • Total Tasks: {total_tasks}
# • Failed: {failed_count}
# *DAG Run ID*: `{context['dag_run'].run_id}`
# *Task Instance*: `{ti.task_id}`"""
#         else:
#             # Success notification
#             emoji = ":white_check_mark:"
#             message = f"""{emoji} *source_app Export Process Completed Successfully*

# *Project*: `{project_id}`
# *Instance*: `{instance_id}`
# *Environment*: `{env}`
# *Execution Time*: {now_str_utc}
# *Summary*:
# • Total Tasks: {total_tasks}
# • Success: {success_count}
# *DAG Run ID*: `{context['dag_run'].run_id}`
# *Task Instance*: `{ti.task_id}`"""

#         # Send notification using SlackWebhookOperator
#         slack_notification = SlackWebhookOperator(
#             task_id="source_app_slack_notification",
#             slack_webhook_conn_id="slack_conn_di",
#             message=message,
#             channel="#dish-source_app",
#             username="Airflow-source_app Integration",
#             icon_emoji=":robot_face:",
#             dag=dag,
#         )

#         # Execute the notification
#         slack_notification.execute(context)
#         logger.info(
#             f"Slack notification sent successfully. Status: {'Failed' if failed_tasks else 'Success'}"
#         )

#     except Exception as e:
#         logger.error(f"Error sending Slack notification: {str(e)}")
#         # Don't raise the exception to prevent DAG failure due to notification issues
#         # Instead, log the error and continue
#         logger.error(f"Slack notification failed but continuing DAG execution: {e}")


# slacknotification = PythonOperator(
#     task_id="slacknotification",
#     provide_context=True,
#     python_callable=send_slack_notification,
#     trigger_rule=TriggerRule.ALL_DONE,
#     dag=dag,
# )

# Set task dependencies     
chain(
    start,
    step1,
    refresh_mv_menufilterall, 
    refresh_mv_menuitems_final,
    step2,
    #*export_group_lst,--
    take_backup,
    step3,
    # #take_backup,--
   *export_group_lst,
    step4,
    #check_all_tasks,
   # slacknotification,,
    step5,
    end,
)

