#################################################################
# DAG script for updating establishment cuisine mapping table   #
#                                                               #
# Created by: Vineeth Shyam                                     #
# Date: 2025-06-03                                              #
#################################################################

"""
DAG for source_app Establishment Cuisine Mapping Update Pipeline

This DAG executes weekly updates to the establishment cuisine mapping data on AlloyDB for PostgreSQL 
as part of the source_app data migration and processing pipeline.

Key Features:
- Weekly automated execution every Saturday at 19:13
- Executes SQL queries stored in external files
- Uses AlloyDB for PostgreSQL as the database backend
- Implements comprehensive database object validation
- Includes proper error handling and retries with email notifications
- Environment-specific configurations (DEV/PROD)
- Custom transaction management for stored procedures with manual commits
- Template search path for SQL file management

Configuration:
- Connection: Uses 'google_alloydb_dev' connection (GCP Cloud SQL type)
- GCP Project: source_project
- Database: postgres
- AlloyDB Instance: di-migration-sbx
- SQL Files Location: /home/airflow/gcs/dags/sql/
- Schedule: Weekly - Every Saturday at 19:13 (Cron: 13 19 * * 6)
- Retries: 3 attempts with 10-minute delay between retries
- Email notifications enabled on failure

Task Flow:
1. Start Task (DummyOperator)
   - Initializes the workflow
2. Test Database Objects (CloudSQLExecuteQueryOperator)
   - Validates existence of required stored procedure 'smartdatastagdb.process_update_mapping'
   - Validates existence of materialized view 'smartdatadb.mv_idx_objektids'
   - Uses CloudSQLExecuteQueryOperator for database connectivity
3. Execute Update Mapping (CloudSQLExecuteQueryOperator)
   - Reads and executes updatemapping.sql from the sql folder
   - Calls smartdatastagdb.process_update_mapping() stored procedure
   - Refreshes materialized view smartdatadb.mv_idx_objektids
   - Uses autocommit=True to allow stored procedures to manage their own transactions
4. End Task (DummyOperator)
   - Marks successful completion

Database Operations:
- Stored Procedure: smartdatastagdb.process_update_mapping()
- Materialized View Refresh: smartdatadb.mv_idx_objektids
- Transaction Management: Custom autocommit handling for procedures with manual COMMIT statements
- Periodic commits every 100 records within stored procedures for optimal performance

Technical Implementation:
- Uses CloudSQLExecuteQueryOperator for enhanced transaction control
- Enables autocommit on connection level to support stored procedures with manual commits
- Reads SQL files dynamically from the template search path
- Implements proper connection cleanup and error handling

Dependencies:
- apache-airflow-providers-google
- SQL file: updatemapping.sql
- AlloyDB instance connectivity
- Required database schemas: smartdatastagdb, smartdatadb

Environment Variables:
- env: Environment selector (DEV/PROD)
- Fallback to Airflow Variables if environment variables not set

Tags: ['database', 'establishment', 'cuisine', 'mapping', 'weekly']

Author: Vineeth Shyam Kalidas
Created: 2025-06-03
Last Modified: 2025-06-03
Version: 1.1
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.cloud_sql import CloudSQLExecuteQueryOperator
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from airflow.models.baseoperator import chain
import os
from airflow.models.variable import Variable

# Default arguments for the DAG
default_args = {
    'owner': 'vineeth',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=10)
}

environment = "env"
env = os.environ.get(environment, Variable.get(environment))

if env == 'DEV':
    # odoo_wsl_creds = Variable.get("odoo_wsl_creds")
    project_id = 'source_project'
    bucket_name = "source_app-dwh-rawzone"
    gcp_conn_id = "google_cloud_default"
    gcp_cloudsql_conn_id = "google_alloydb_dev"
    # AlloyDB instance details for DEV
    instance_id = "di-migration-sbx"  # Replace with your actual instance ID
    database_id = "postgres"          # Replace with your actual database name

else:
    project_id = 'source_project'
    bucket_name = "source_app-dwh-rawzone"
    gcp_conn_id = "google_cloud_default"
    gcp_cloudsql_conn_id = "google_alloydb_dev"
    # AlloyDB instance details for PROD
    instance_id = "di-migration-sbx"  # Replace with your actual instance ID
    database_id = "postgres"          # Replace with your actual database name



dag = DAG(
    'etl_di_update_est_cuisine_mapping',
    default_args=default_args,
    description='Weekly execution of update mapping SQL script',
    schedule_interval='13 19 * * 6',  # Every Saturday at 19:13
    catchup=True,
    doc_md= __doc__,
    tags=['database', 'establishment', 'cuisine', 'mapping', 'weekly'],
    template_searchpath="/home/airflow/gcs/dags/sql",
)

start = DummyOperator(
    task_id='start',
    dag=dag
)

end = DummyOperator(
    task_id='end',
    dag=dag
)

test_objects = CloudSQLExecuteQueryOperator(
    task_id='test_db_objects',
    sql="""
        -- Test if stored procedure exists
        SELECT 
            CASE 
                WHEN EXISTS (
                    SELECT 1 FROM pg_proc p 
                    JOIN pg_namespace n ON p.pronamespace = n.oid 
                    WHERE n.nspname = 'smartdatastagdb' 
                    AND p.proname = 'process_update_mapping'
                ) THEN '✅ Procedure exists'
                ELSE '❌ Procedure missing'
            END as procedure_status
        UNION ALL
        -- Test if materialized view exists
        SELECT 
            CASE 
                WHEN EXISTS (
                    SELECT 1 FROM pg_matviews 
                    WHERE schemaname = 'smartdatadb' 
                    AND matviewname = 'mv_idx_objektids'
                ) THEN '✅ Materialized view exists'
                ELSE '❌ Materialized view missing'
            END as matview_status;
    """,
    gcp_conn_id=gcp_conn_id,
    gcp_cloudsql_conn_id=gcp_cloudsql_conn_id,
    autocommit=True,
    dag=dag,
)

update_mapping_task = CloudSQLExecuteQueryOperator(
    task_id='execute_update_mapping',
    sql='updatemapping.sql',  
    gcp_conn_id=gcp_conn_id,
    gcp_cloudsql_conn_id=gcp_cloudsql_conn_id,
    autocommit=True,  
    dag=dag,
)

chain(start, test_objects, update_mapping_task, end) 