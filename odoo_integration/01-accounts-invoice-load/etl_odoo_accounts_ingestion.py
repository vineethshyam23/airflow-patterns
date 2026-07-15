"""
DAG for loading Customer Invoices/Lines daily in Odoo

Created by: Vineeth Shyam
Pattern: Dynamic TaskGroups with Chunked Processing
Production Usage: 50K+ invoices/day
"""

from airflow import DAG
from airflow.models import Variable
from airflow.utils.helpers import chain
from airflow.utils.timezone import make_aware
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
from airflow.utils.session import provide_session
from airflow.models import DagRun, TaskInstance
import logging
from google.cloud import bigquery
from datetime import datetime, timedelta
import os

# Import domain-specific class
from odoo_accounts_load import Odoo

# Configuration
default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email": ["dataops@company.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 5,
    "retry_delay": timedelta(minutes=10),
    "provide_context": True,
    "priority_weight": 12
}

environment = "env"
env = os.environ.get(environment, Variable.get(environment))

if env == "DEV":
    projectid = "dwh-project-dev"
    bucket_name = "data-platform-dev-rawzone"
    bigquery_conn_id = "bigquery_default_dev"
else:
    projectid = "dwh-project"
    bucket_name = "data-platform-rawzone"
    bigquery_conn_id = "bigquery_default"


def get_total_invoices():
    """
    Get count of invoices to process today.
    
    Returns:
        int: Total number of invoices
    """
    query = f"""
    SELECT COUNT(*) as total
    FROM `{projectid}.dwh_trusted.int_odoo_wsl_account_move_consolidated`
    WHERE DATE(write_date) BETWEEN CURRENT_DATE() - 1 AND CURRENT_DATE()
    """
    client = bigquery.Client(project=projectid)
    query_job = client.query(query=query)
    results = query_job.result()
    total = list(results)[0]['total']
    
    logging.info(f"Total rows retrieved from query: {total}")
    return total


def echo_total_invoices(**context):
    """Echo total for visibility in logs."""
    total = context['task_instance'].xcom_pull(
        task_ids='get_total_invoices', 
        key='return_value'
    )
    print(f"Total invoices to be processed: {total}")
    return total


def account_query(start, stop) -> str:
    """
    Build query for specific chunk of invoices.
    
    Args:
        start: Starting row number
        stop: Ending row number
    
    Returns:
        str: BigQuery SQL query
    """
    query = f"""
    SELECT
        *,
        ROW_NUMBER() OVER(ORDER BY move_name) as rnk
    FROM `{projectid}.dwh_trusted.int_odoo_wsl_account_move_consolidated`
    WHERE DATE(write_date) BETWEEN CURRENT_DATE() - 1 AND CURRENT_DATE()
    QUALIFY rnk BETWEEN {start} AND {stop}
    """
    return query


@provide_session
def get_latest_upstream_dag_run(execution_date, session=None, **kwargs):
    """
    Get latest successful run of upstream DAG.
    
    Used by ExternalTaskSensor to wait for data preparation.
    """
    today = make_aware(datetime.now().replace(hour=0, minute=0, second=0, microsecond=0))
    yesterday = today + timedelta(days=-1)
    
    latest_task_instance = session.query(TaskInstance).join(DagRun).filter(
        DagRun.dag_id == 'etl_wsl_accounts',
        TaskInstance.task_id == 'dbt_wsl_accounts_run',
        TaskInstance.state == 'success',
        TaskInstance.end_date.isnot(None),
        DagRun.execution_date >= yesterday,
        DagRun.execution_date < today
    ).order_by(DagRun.execution_date.desc()).first()
    
    if latest_task_instance and latest_task_instance.end_date:
        result = latest_task_instance.execution_date
    else:
        result = execution_date
    
    return result


# DAG Definition
schedule = "0 4 * * *"  # Daily at 4 AM UTC

with DAG(
    dag_id="etl_odoo_accounts_ingestion",
    description="Load customer invoices/lines data into Odoo",
    default_args=default_args,
    schedule_interval=schedule,
    render_template_as_native_obj=True,
    max_active_runs=1,
    concurrency=5,  # Max 5 parallel tasks
    max_active_tasks=5,
    catchup=False,
    tags=['odoo', 'accounts', 'invoices', 'production']
) as dag:
    
    start = DummyOperator(task_id="start")
    
    # Wait for upstream data preparation
    wait_for_data_prep = ExternalTaskSensor(
        task_id="wait_for_data_prep",
        external_dag_id="etl_wsl_accounts",
        external_task_id="dbt_wsl_accounts_run",
        allowed_states=['success'],
        failed_states=['failed'],
        execution_date_fn=get_latest_upstream_dag_run,
        mode="reschedule",
        poke_interval=30 * 60,  # Check every 30 minutes
        timeout=23 * 60 * 60,  # 23 hour timeout
    )
    
    # Get total invoices to process
    get_total = PythonOperator(
        task_id="get_total_invoices",
        python_callable=get_total_invoices
    )
    
    # Echo total for visibility
    echo_total = PythonOperator(
        task_id="echo_total_invoices",
        python_callable=echo_total_invoices,
        provide_context=True,
    )
    
    # Dynamic TaskGroup: Creates tasks based on row count at DAG parse time
    with TaskGroup(group_id='load_invoice_data', dag=dag) as load_invoices_group:
        # Initialize domain class
        accounts = Odoo(credentials=Variable.get("odoo_prod_creds", deserialize_json=True))
        
        # Get total for task generation
        total_rows = get_total_invoices()
        
        # Create task for each 500-record chunk
        rank_split = [rank for rank in range(1, total_rows, 500)]
        
        for rank in rank_split:
            rank_start = rank
            rank_end = rank_start + 499
            
            load_invoices = PythonOperator(
                task_id=f"load_invoice_rank_{rank_start}",
                provide_context=True,
                python_callable=accounts.move_load_data,
                execution_timeout=timedelta(hours=3),
                op_kwargs={
                    'odoo_creds': "{{ var.json.odoo_prod_creds }}",
                    'query': account_query(start=rank_start, stop=rank_end),
                    'project_name': projectid
                }
            )
    
    end = DummyOperator(task_id="end")
    
    # Task dependencies
    chain(
        start,
        wait_for_data_prep,
        get_total,
        echo_total,
        load_invoices_group,
        end
    )
