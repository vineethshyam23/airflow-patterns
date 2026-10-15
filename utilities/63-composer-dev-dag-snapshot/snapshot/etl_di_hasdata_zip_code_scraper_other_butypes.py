"""
DAG for source_app Data Pipeline

This DAG executes Python script on AlloyDB for PostgreSQL to process and update data for the source_app project.

Key Features:
- Uses AlloyDB for PostgreSQL as the database
- Implements proper error handling and retries
- Includes logging for better monitoring
- Manual trigger (no schedule)

Configuration:
- Connection: Uses 'google_alloydb_dev' connection (GCP Cloud SQL type)
- Database: postgres
- Retries: 3 attempts with 2-minute delay between retries
- Email notifications on failure

Task Flow:
1. Start Task (EmptyOperator)
2. Execute Python Script (PythonOperator)
   - Executes execute_hasdata_zip_code_script_other_butypes.py
3. Execute Python Script (PythonOperator)
   - Execute run_get_max_import_id_other_butypes() function to get maximum import_id.
4. Run SQL query to trigger stored procedure with input argument of 'import_id'
5. End Task (EmptyOperator)

Dependencies:
- apache-airflow-providers-google
- Python file: execute_hasdata_zip_code_script_other_butypes.py

Author: Rashmi Kedari
Created: 2026-02-03
Modified By: Rashmi Kedari --for new business types based on keywords search
Last Modified: 2026-02-05

"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.cloud_sql import (
    CloudSQLExecuteQueryOperator,
)
from airflow.operators.python import PythonOperator
# from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.utils.helpers import chain
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.empty import EmptyOperator
import os
import logging
from datetime import datetime, timezone
#from modules.hasdata_zip_code_scraper import *
from modules.execute_hasdata_zip_code_script_other_butypes import *

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

# Dev DB credentials
dev_creds = Variable.get("alloydb_dev_details", deserialize_json=True)

api_key = Variable.get("di_hasdata_apikey")

# Default arguments for the DAG
default_args = {
    "owner": "Rashmi Kedari",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email": [
        "dataops@example.com"
    ],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}

# 0 14 * * 2,4,6
# At 02:00 PM, only on Tuesday, Thursday, and Saturday
# schedule_interval = On-demand

# Define the DAG
dag = DAG(
    dag_id="etl_di_hasdata_zip_code_scraper_other_butypes",
    default_args=default_args,
    description="ETL process for Dish source_app data using AlloyDB",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    doc_md=__doc__,
    tags=["etl", "alloydb", "dish", "source_app"],
    template_searchpath="/home/airflow/gcs/dags/sql",
)

start = EmptyOperator(task_id="start", trigger_rule=TriggerRule.ALL_DONE, dag=dag)
end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE, dag=dag)

# Define keywords list for HasData API queries
# Modify this list to change the search keywords
keywords_list = [
    "Indoor playground",
    "children's amusement center",
    "Playground",
    "Recreation center",
    "Escape room center",
    "Padel court",
    "Pool hall",
    "Youth center",
    "Tennis center",
    "Futsal court",
    "Amusement park"
]

#uncomment when need to crawl ne data from API as it consumes credits
# Task to get data from HasData API for Germany
# Keywords must be provided from DAG level
# execute_hasdata_zip_code_script_other_butypes = PythonOperator(
#     task_id="execute_hasdata_zip_code_script_other_butypes",
#     python_callable=run_hasdata_scraper,
#     op_kwargs={
#         "creds": dev_creds,
#         "hasdata_api_key": api_key,
#         "keywords": keywords_list
#     },
#     do_xcom_push=True,
#     dag=dag,
#     provide_context=True,
#     # execution_timeout=timedelta(minutes=300),
#     execution_timeout=timedelta(hours=72),
# )

#Task to get max(importid) from import_log table
execute_get_max_import_id_other_butypes = PythonOperator(
    task_id="execute_get_max_import_id_other_butypes",
    python_callable=run_get_max_import_id_other_butypes,
    op_kwargs={
        "creds": dev_creds
    },
    do_xcom_push=True,
    provide_context=True,
    dag=dag,
)

# Task to run stored procedures which will flatten JSON results 
execute_stored_procedures_other_butypes = CloudSQLExecuteQueryOperator(
    task_id="execute_stored_procedures_other_butypes",
    gcp_cloudsql_conn_id=gcp_cloudsql_conn_id,
    sql="""
    CALL smartdatadb.flattening_hasdata_other_butypes({{ ti.xcom_pull(task_ids="execute_get_max_import_id_other_butypes", key="return_value")}});
    """,
    autocommit=True,
    doc_md="""This task triggers 'smartdatadb.flattening_hasdata_other_butypes' stored procedure. It creates MATERIALIZED VIEW smartdata_analyticdb.mv_flattened_hasdata_other_butypes. 
     """,
    dag=dag,
)

#refresh materialized view
# execute_refresh_materialized_view_other_butypes = CloudSQLExecuteQueryOperator(
#     task_id="refresh_materialized_view_other_butypes",
#     gcp_cloudsql_conn_id=gcp_cloudsql_conn_id,
#     sql="""
#     REFRESH MATERIALIZED VIEW smartdata_analyticdb.mv_flattened_hasdata_other_butypes;
#     """,
#     autocommit=True,
#     doc_md="""This task refreshes the materialized view smartdata_analyticdb.mv_flattened_hasdata_other_butypes.
#     """,
#     dag=dag,
# )

# Set task dependencies
chain(start,
    #execute_hasdata_zip_code_script_other_butypes,
    execute_get_max_import_id_other_butypes,
    execute_stored_procedures_other_butypes,
    # execute_refresh_materialized_view_other_butypes,
    end
    )
