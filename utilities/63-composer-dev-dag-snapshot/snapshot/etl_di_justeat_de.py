"""
DAG for source_app justeat Pipeline.
This DAG is designed to process comprehensive justeat API crawling framework that parses restaurant information from justeat API and stores the results in the database. 

Key Features:
- Extracts restaurant information from justeat API.
- Implements proper exception and error handling and retries.
- Includes comprehensive logging.

Configuration:
- Connection: Uses 'google_alloydb_dev' connection (GCP AlloyDB type)
- Database: postgres
- Retries: 3 attempts with 5-minute delay between retries
- Email notifications on failure - need to implement this in future

Task Flow:
1. Start Task (EmptyOperator)
2. extract justeat restaurants from justeat API (PythonOperator)
    - using scrapy framework
    - Extracts restaurant information (addresses, phone numbers, VAT IDs, manager details) from justeat API.
    - stores extracted data in structured database tables as json.
3. SlackNotificationOperator: Sends a slack notification on failure and success
4. End Task (EmptyOperator)

Dependencies:
- apache-airflow-providers-google
- psycopg2
- apache-airflow-providers-slack
- scrapy
- modules.extract_justeat_restaurants
- Tables Used: smartdatastagdb.jsonimport

Author: Rashmi Kedari
Created: 2025-08-08
Last Modified: 2025-08-08
"""
import os
import logging
import subprocess
from datetime import datetime, timedelta,timezone
from airflow import DAG
# from airflow.providers.google.cloud.operators.cloud_sql import (
#     CloudSQLExecuteQueryOperator,
# )
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.utils.helpers import chain
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from modules.db_connections import initialize_db_connection
import psycopg2 
from airflow.decorators import task_group


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
    #  emailnotification
    # "email": [
    #     "dataops@example.com",
    #     "dataops@example.com",
    #     "dataops@example.com",
    #     "dataops@example.com"
    # ],
    #"email_on_failure": True,
    #"email_on_retry": False,
    #"retries": 3,
    #"retry_delay": timedelta(minutes=5),#reduced retry delay as schedule interval is 12 minutes
    "execution_timeout": timedelta(hours=55),
}

# Define the DAG
dag = DAG(
    dag_id="etl_di_justeat_de",
    default_args=default_args,
    description="ETL process for Dish source_app data using AlloyDB",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    concurrency=5,
    max_active_tasks=5,
    doc_md=__doc__,
    tags=["etl", "alloydb", "dish", "source_app", "Crawling_Processes"],
    template_searchpath="/home/airflow/gcs/dags/modules/justeat/",
)

def get_total_zipcodes_count():
    conn = initialize_db_connection(env)
    logging.info(f"Getting total zipcodes count for country Germany(DE)")
    sql = "select count(*) from smartdatastagdb.zipcode_details_sample where country_code = 'DE'"
    
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            total_count = cursor.fetchone()[0]

        if total_count > 0:
            logging.info(f"Total zipcodes for country Germany(DE): {total_count}")
        else:  
            raise Exception(f"No zipcodes found for country Germany(DE)")
    finally:
        conn.close()

def clear_all_checkpoints() -> bool: 
    """
    Clear all checkpoint records for jeat_de spider before running the spider
    Returns:
        bool: True if cleanup was successful
    """
    try:
        conn = initialize_db_connection(env)
        with conn.cursor() as cursor:
            query = f"""
                DELETE FROM smartdatastagdb.jsonimport_sample
                WHERE spider = 'jeat_de'
                AND (zipcode IS NOT NULL OR scraping_status IS NOT NULL)
            """
            cursor.execute(query)
            deleted_count = cursor.rowcount
            conn.commit()
            logging.info(f"Cleared {deleted_count} checkpoint records for jeat_de spider") 
            return True
            
    except Exception as e:
        logging.error(f"Error clearing checkpoints: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()

def run_justeat_spider(tile, total_tiles, **context): 
    """Run the JustEat spider with proper database connection and error handling"""
    try:
        # Change to the JustEat directory
        justeat_dir = f"{dag.template_searchpath[0]}jeat_spiders" 
        if not os.path.exists(justeat_dir):
            raise FileNotFoundError(f"JustEat directory not found: {justeat_dir}")
        
        os.chdir(justeat_dir)
        logger.info(f"Changed to directory: {os.getcwd()}")
        
        # Check if jeat_de_spider.py exists
        if not os.path.exists("jeat_de_spider.py"):
            raise FileNotFoundError(f"jeat_de_spider.py not found in JustEat directory")
        
        # Set Python path
        python_path = os.environ.get('PYTHONPATH', '')
        os.environ['PYTHONPATH'] = f"{dag.template_searchpath[0]}jeat_spiders"  
        
        # Run the spider
        logger.info(f"Starting JustEat spider for tile {tile} with total tiles {total_tiles}...")
        
        result = subprocess.run(['python', 'jeat_de_spider.py', '-t', str(tile), '-T', str(total_tiles)], 
                                capture_output=True, text=True, check=False, env=os.environ)
        
        logger.info(f"Spider return code: {result.returncode}")
        
        # Parse spider output for essential information only
        critical_errors = []
        non_critical_errors = []
        scraping_summary = None
        error_summary = None
        
        if result.stdout:
            lines = result.stdout.split('\n')
            for i, line in enumerate(lines):
                line = line.strip()
                if not line:
                    continue
                    
                # Extract critical errors
                if '[CRITICAL ERROR]' in line:
                    error_msg = line.split('[CRITICAL ERROR]', 1)[1].strip()
                    critical_errors.append(error_msg)
                    logger.error(f" Spider Critical Error in line: {error_msg}")
                    # Extract critical error details
                    if 'Critical Error Details:' in line:
                        logger.error(f"Spider: {line}")
                        logger.error(f"Spider Critical Error: {error_msg}")
                
                # Extract critical error details (multi-line format)
                elif '[CRITICAL ERROR DETAIL]' in line:
                    error_detail = line.split('[CRITICAL ERROR DETAIL]', 1)[1].strip()
                    logger.error(f" Spider Critical Error Detail: {error_detail}")
                    # Add to the last critical error if it exists
                    if critical_errors:
                        critical_errors[-1] += f"\n{error_detail}"
                    else:
                        critical_errors.append(f"Error detail: {error_detail}")
                
                # Extract non-critical errors
                elif '[NON-CRITICAL ERROR]' in line:
                    error_msg = line.split('[NON-CRITICAL ERROR]', 1)[1].strip()
                    non_critical_errors.append(error_msg)
                    logger.warning(f" Spider Non-Critical Error in line: {error_msg}")
                    # Extract non-critical error details
                    if 'Non-Critical Error Details:' in line:
                        logger.warning(f"Spider: {line}")
                        logger.warning(f"Spider Non-Critical Error: {error_msg}")
                
                # Extract non-critical error details (multi-line format)
                elif '[NON-CRITICAL ERROR DETAIL]' in line:
                    error_detail = line.split('[NON-CRITICAL ERROR DETAIL]', 1)[1].strip()
                    logger.warning(f" Spider Non-Critical Error Detail: {error_detail}")
                    # Add to the last non-critical error if it exists
                    if non_critical_errors:
                        non_critical_errors[-1] += f"\n{error_detail}"
                    else:
                        non_critical_errors.append(f"Error detail: {error_detail}")
                
                elif 'Fatal error in main execution:' in line:
                    error_msg = line.split('Fatal error in main execution:', 1)[1].strip()
                    critical_errors.append(f"Fatal error in main execution: {error_msg}")
                    logger.error(f"Spider Fatal Error: {error_msg}")

                #NEW: Capture stderr traceback if available
                if result.stderr:
                    logger.error("Spider stderr (full traceback):")
                    for serr_line in result.stderr.splitlines():
                        logger.error(f"  {serr_line}")
                        critical_errors.append(f"STDERR: {serr_line}")
                
                # Extract scraping summary
                elif 'Scraping Summary' in line:
                    scraping_summary = line
                    logger.info(f"Spider: {line}")
                
                # # Extract error summary
                # elif 'Error Summary' in line:
                #     error_summary = line
                #     logger.info(f"Spider: {line}")                
        
        # Always log stderr for debugging when return code is not 0
        if result.stderr and result.returncode != 0:
            logger.error(f"Spider stderr output:")
            for line in result.stderr.split('\n'):
                if line.strip():
                    logger.error(f"  stderr: {line.strip()}")
        
        # Also log full stdout for debugging when there are issues
        if result.returncode != 0:
            logger.error(f"Spider full stdout output:")
            for line in result.stdout.split('\n'):
                if line.strip():
                    logger.error(f"  stdout: {line.strip()}")
        
        # Determine success/failure based on return code and critical errors
        if result.returncode == 0:
            if critical_errors:
                logger.error(f"Spider completed with exit code 0 but had {len(critical_errors)} critical errors - this should not happen")
                raise Exception(f"Spider completed with exit code 0 but had {len(critical_errors)} critical errors")
            elif non_critical_errors:
                logger.info(f"Spider completed successfully with {len(non_critical_errors)} non-critical errors")
                return True
            else:
                logger.info("Spider completed successfully with no errors")
                return True
        elif result.returncode == 1:
            logger.error(f"Spider failed with return code {result.returncode} - critical errors detected")
            if critical_errors:
                logger.error(f"Critical errors that caused spider failure:")
                for error in critical_errors:
                    logger.error(f"  - {error}")
            # Include stderr in the exception message for better debugging
            error_msg = f"Spider failed with return code {result.returncode} - critical errors detected"
            if result.stderr:
                error_msg += f"\nStderr: {result.stderr}"
            raise Exception(error_msg)
        else:
            logger.error(f"Spider failed with unexpected return code {result.returncode}")
            raise Exception(f"Spider failed with unexpected return code {result.returncode}")
        
    except subprocess.CalledProcessError as e:
        logger.error(f"Spider subprocess failed: {e}")
        logger.error(f"Spider stderr: {e.stderr}")
        logger.error(f"Spider stdout: {e.stdout}")
        raise
    except Exception as e:
        logger.error(f"Error running JustEat spider: {e}")
        raise

def create_jeat_jsondump_table():
    """Create the jeat_jsondump table"""
    conn = initialize_db_connection(env)
    # Call the procedure
    sql = "CALL smartdatadb.create_justeat_temp_table () "
    
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            conn.commit()
        
        logging.info(f"Successfully executed create_justeat_temp_table procedure") 
        return True

    finally:
        conn.close()
    

def get_next_import_id(**context):
    """Get the next import ID from the database"""
    # Use the db_connections module to get database connection
    conn = initialize_db_connection(env)
    
    # Get the next import ID
    sql = """
    SELECT COALESCE(MAX(importid), 0) + 1 
    FROM smartdatastagdb.import_log
    """
    
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            import_id = cursor.fetchone()[0]
        
        logging.info(f"Next import ID: {import_id}")
        context['task_instance'].xcom_push(key='import_id', value=import_id)
        return import_id
    finally:
        conn.close()

def call_justeat_import_procedure(**context):
    """Call the JustEat import procedure"""
    # Use the db_connections module to get database connection
    conn = initialize_db_connection(env)
    import_id = context['task_instance'].xcom_pull(task_ids='execute_get_max_import_id', key='import_id')
    
    logging.info(f"Calling import_justeatdata procedure with import_id: {import_id}")
    
    # Call the procedure
    sql = "CALL smartdatadb.import_justeatdata(%s)"
    
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql, (import_id,))
            conn.commit()
        
        logging.info(f"Successfully executed import_justeatdata procedure")
        return import_id
    finally:
        conn.close()

def slack_notification(**context):
    try:
        dag_run = context['dag_run']
        now_str_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        
        # Check all task states
        tasks_to_check = ['execute_justeat_de_scrapper', 'execute_get_max_import_id', 'execute_import_procedure']
        failed_tasks = []
        successful_tasks = []
        error_info = ""
        
        for task_id in tasks_to_check:
            task_instance = dag_run.get_task_instance(task_id)
            if task_instance:
                state = task_instance.state
                if state == "success":
                    successful_tasks.append(task_id)
                elif state in ["failed", "skipped"]:
                    failed_tasks.append(task_id)
                    # Get error information from task logs
                    try:
                        logs = task_instance.log.read()
                        if logs:
                            log_lines = logs.split('\n')
                            for line in log_lines:
                                if "Error Summary" in line:
                                    error_info += f"\n*{task_id} Error Summary*: {line}"
                                elif "Critical Error Details:" in line:
                                    error_info += f"\n*{task_id} Critical Errors*:"
                                elif "Non-Critical Error Details:" in line:
                                    error_info += f"\n*{task_id} Non-Critical Errors*:"
                                elif line.strip().startswith('  1.') or line.strip().startswith('  2.') or line.strip().startswith('  3.'):
                                    # Extract numbered error details
                                    error_msg = line.strip()
                                    if error_msg:
                                        error_info += f"\nâ€¢ {error_msg}"
                                elif "Spider: âŒ" in line:
                                    # Extract final spider status
                                    error_info += f"\n*{task_id} Final Status*: {line.strip()}"
                    except Exception as log_err:
                        logger.warning(f"Could not extract error info from {task_id} logs: {log_err}")
                        error_info += f"\n*{task_id}*: Failed to retrieve error details"

        # Determine overall status
        if not failed_tasks:
            emoji = ":white_check_mark:"
            status = "Success"
            final_message = f"""
            {emoji} *etl_di_justeat_de* Pipeline Successfully Completed.\n
            *Project*: {project_id}\n
            *Instance Id*: {instance_id}\n
            *Finished At*: {now_str_utc}\n
            *Status*: {status}\n
            *Completed Tasks*: {', '.join(successful_tasks)}\n
            """
        else:
            emoji = ":x:"
            status = "Failed"
            final_message = f"""
            {emoji} *etl_di_justeat_de* Pipeline Failed.\n
            *Project*: {project_id}\n
            *Instance Id*: {instance_id}\n
            *Finished At*: {now_str_utc}\n
            *Status*: {status}\n
            *Successful Tasks*: {', '.join(successful_tasks) if successful_tasks else 'None'}\n
            *Failed Tasks*: {', '.join(failed_tasks)}{error_info}\n
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


task_get_total_zipcodes_count = PythonOperator(
    task_id="execute_get_total_zipcodes_count",
    python_callable=get_total_zipcodes_count,
    provide_context=True,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    dag=dag,
)

task_clear_checkpoints = PythonOperator(
    task_id="clear_checkpoints",
    python_callable=clear_all_checkpoints,
    provide_context=True,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    dag=dag,
)

cntry_lst=['DE']
# Create tasks for each tile using task groups
group_list = []
task_group_list = []
for cntry in cntry_lst:
    @task_group(group_id=f"justeat_spider_group_de", dag=dag)
    def create_spider_tasks():
        for tile in range(1, 21):
            
            # Create the spider task for this tile
            spider_task = PythonOperator(
                task_id=f"execute_justeat_spider_{tile}",
                python_callable=run_justeat_spider,
                op_kwargs={'tile': tile, 'total_tiles': 20},
                provide_context=True,
                trigger_rule=TriggerRule.ALL_SUCCESS,
                dag=dag,
            )    
            task_group_list.append(spider_task)
              
            if tile %4 ==0:
                pause_task = EmptyOperator(task_id=f"pause_task_{tile}", trigger_rule=TriggerRule.ALL_SUCCESS, dag=dag)
                task_group_list.append(pause_task)

            chain(*task_group_list[:6]) 
            chain(*task_group_list[6:12]) 
            chain(*task_group_list[12:18]) 
            chain(*task_group_list[18:])

    group_list.append(create_spider_tasks())
    
task_create_jeat_jsondump_table = PythonOperator(
    task_id="execute_create_jeat_jsondump_table",
    python_callable=create_jeat_jsondump_table,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    dag=dag,
)

# task_get_import_id = PythonOperator(
#     task_id="execute_get_max_import_id",
#     python_callable=get_next_import_id,
#     provide_context=True,
#     trigger_rule=TriggerRule.ALL_SUCCESS,
#     dag=dag,
# )

# task_call_import_procedure = PythonOperator(
#     task_id="execute_import_procedure",
#     python_callable=call_justeat_import_procedure,
#     provide_context=True,
#     trigger_rule=TriggerRule.ALL_SUCCESS,
#     dag=dag,
# )

# # Set up parallel execution for both batches with pause between them
# first_batch = task_group_list[:5]  # Tiles 1-5
# second_batch = task_group_list[5:]  # Tiles 6-10

# Use chain operator for cleaner dependency management
# First batch runs in parallel, then pause, then second batch runs in parallel, then import tasks
chain(start,task_get_total_zipcodes_count, task_clear_checkpoints, *group_list, task_create_jeat_jsondump_table, 
#task_get_import_id, task_call_import_procedure,
 end)









