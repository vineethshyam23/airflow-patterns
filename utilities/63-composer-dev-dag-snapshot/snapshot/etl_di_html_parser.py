"""
DAG for HTML Parser Data Pipeline

This DAG executes the HTML parser script to crawl and process web content for the source_app project.

Key Features:
- Executes HTML parser script with configurable parameters
- Uses AlloyDB for PostgreSQL as the database
- Implements proper error handling and retries
- Includes logging for better monitoring
- Scheduled to run weekly

Configuration:
- Connection: Uses AlloyDB credentials from Airflow variable 'alloydb_dev_details'
- Database: postgres
- Script Location: dags/modules/html_parser.py
- Retries: 3 attempts with 10-minute delay between retries
- Email notifications on failure

Task Flow:
1. Start Task (EmptyOperator)
2. Execute HTML Parser (PythonOperator)
   - Executes html_parser.py with AlloyDB credentials
   - Maximum runtime: 300 minutes
3. End Task (EmptyOperator)

Dependencies:
- apache-airflow-providers-google
- psycopg2-binary
- requests
- Required Python modules:
  - dags.modules.html_parser
  - dags.modules.db_connections
  - dags.modules.db_credentials

Author: Yousuf Kaleem
Created: 2025-07-24
Last Modified: 2025-07-24
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.utils.helpers import chain
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.empty import EmptyOperator
from modules.html_parser import run_html_parser
import os
import logging
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Parallel processing configuration
PARALLEL_CONFIG = {
    "max_parallel_tasks": 20,
    "task_timeout_minutes": 300,  # Increased to 300 minutes (5 hours)
    "retry_attempts": 2,
    "retry_delay_minutes": 5,
}

# Get AlloyDB credentials from Airflow variable
alloydb_details = json.loads(Variable.get("alloydb_dev_details"))

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
    "depends_on_past": True,  # Prevent multiple runs on same day
    "start_date": days_ago(1),
    "email": [
        "dataops@example.com",
        #"dataops@example.com",
        #"dataops@example.com",
        #"dataops@example.com",
    ],
    "email_on_failure": True,
    "email_on_retry": True,  # Enable email on retry
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=60),
    "wait_for_downstream": True,  # Wait for previous run to complete
}

# Define the DAG
dag = DAG(
    dag_id="etl_di_html_parser",
    default_args=default_args,
    description="HTML parser process for Dish source_app data using AlloyDB",
    schedule_interval="0 0 * * 1,2,3,5,6,7",  # Daily at 00:00 on Monday, Tuesday, Wednesday, Friday, Saturday, Sunday
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,  # Only allow one DAG run at a time
    max_active_tasks=20,  # Allow all 20 tasks to run in parallel
    doc_md=__doc__,
    tags=["etl", "alloydb", "dish", "source_app", "html-parser"],
    template_searchpath="/home/airflow/gcs/dags/modules",
)

start = EmptyOperator(task_id="start", trigger_rule=TriggerRule.ALL_DONE, dag=dag)
end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE, dag=dag)

# Parallel Processing Configuration
# This setup creates 20 parallel tasks that will run simultaneously
# Each task processes a different subset of URLs using NTILE partitioning
# The tasks are independent and can run in parallel without conflicts
# The system automatically selects the country with the most unprocessed URLs
# Note: Parallel processing works without pools - Airflow handles concurrency automatically
total_ntile = 20
list_of_tasks = []
for i in range(1, total_ntile + 1):
    # SQL with workload prioritization - automatically selects country with most unprocessed URLs
    sql = f"""
with country_priority as (
    -- Get country with most unprocessed URLs for optimal resource utilization
    select c.country, count(u.idx) as url_count
    from smartdatastagdb.config_linkextract_order c
    left join smartdatadb.b2b_urls u on u.country = c.country
    where u.idx = coalesce(u.master_idx, u.idx)
        and u.url not like 'MAILTO:%'
        and u.url not like 'mailto:%'
        AND NOT EXISTS(SELECT 1 FROM smartdatadb.b2b_html h WHERE h.idx=u.idx AND h.eingefuegtam > NOW() - INTERVAL '12 hours' - INTERVAL '360 days')  
        AND NOT EXISTS(SELECT 1 FROM smartdatadb.b2b_url_call c WHERE c.idx=u.idx and c.datum > NOW() - interval '12 hours' -interval '360 days')
    group by c.country
    order by url_count desc
    limit 1
),
u as (
    select u.idx, u.url, u.country, u.url_absolute, up.url as parent_url, case when u.objektid is not null then u.objektid else up.objektid end as objektid
    from smartdatadb.b2b_urls u 
    LEFT JOIN smartdatadb.b2b_urls up ON up.idx = u.parent_idx
    CROSS JOIN country_priority cp
    where u.idx = coalesce(u.master_idx, u.idx)
        and u.url not like 'MAILTO:%'
        and u.url not like 'mailto:%'
        AND NOT EXISTS(SELECT 1 FROM smartdatadb.b2b_html h WHERE h.idx=u.idx AND h.eingefuegtam > NOW() - INTERVAL '12 hours' - INTERVAL '360 days')  
        AND NOT EXISTS(SELECT 1 FROM smartdatadb.b2b_url_call c WHERE c.idx=u.idx and c.datum > NOW() - interval '12 hours' -interval '360 days')
        and u.country = cp.country
    limit 60000
),
final_query as (
    SELECT u.objektid, u.idx, u.url, u.url_absolute, u.parent_url, u.country,
    ntile({total_ntile}) over(order by 1,2,3,4) as ntile_no
    FROM u 
    left join smartdatadb.objekt o on o.objektid = u.objektid
    left join (
        select objektid from smartdatadb.importstatushist 
        where importstatusid = 30 group by objektid
    ) ih on ih.objektid = o.masterobjektid 				
                    order by u.idx, case when ih.objektid is null then 1 else 0 end, random()
    limit 60000
)
select * from final_query
where ntile_no = {i}
"""
    # Task to execute HTML parser using Python with parallel processing optimization
    execute_html_parser = PythonOperator(
        task_id=f"execute_html_parser_{i}",
        python_callable=run_html_parser,
        op_kwargs={
            "env": env,
            "maxminutes": 300,  # Increased to 300 minutes (5 hours)
            "modulo": "",
            "test": False,
            "sql": sql,
            "task_id": i,  # Pass task ID for logging
            "total_tasks": total_ntile,  # Pass total tasks for progress tracking
        },
        execution_timeout=timedelta(minutes=PARALLEL_CONFIG["task_timeout_minutes"]),
        retries=PARALLEL_CONFIG["retry_attempts"],
        retry_delay=timedelta(minutes=PARALLEL_CONFIG["retry_delay_minutes"]),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(minutes=30),
        dag=dag,
    )
    list_of_tasks.append(execute_html_parser)

# Set task dependencies for parallel execution
# All tasks start after 'start' and end before 'end' - enabling true parallelism
start >> list_of_tasks >> end