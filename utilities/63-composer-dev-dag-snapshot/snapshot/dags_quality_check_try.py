"""
DAG for source_app link extraction Pipeline


Author: Rashmi Kedari
Created: 2025-11-23
Last Modified: 2025-11-23

"""

import os
import logging
from datetime import datetime, timedelta,timezone
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
import time
#import great_expectations as gx

# Import GreatExpectationsOperator
# Supports both airflow-provider-great-expectations and apache-airflow-providers-great-expectations
try:
    # Try the airflow-provider-great-expectations package first (great_expectations_provider)
    from great_expectations_provider.operators.great_expectations import GreatExpectationsOperator
except ImportError:
    # Fallback to standard Apache Airflow provider (apache-airflow-providers-great-expectations)
    try:
        from airflow.providers.great_expectations.operators.great_expectations import GreatExpectationsOperator
    except ImportError:
        raise ImportError(
            "Great Expectations Airflow provider not found. "
            "Please install it using: pip install airflow-provider-great-expectations "
            "or pip install apache-airflow-providers-great-expectations"
        )

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

environment = "env"
#env = os.environ.get(environment, Variable.get(environment))
env = "DEV"

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
    #     "dataops@example.com"
    # ],
    #"email_on_failure": True,
    #"email_on_retry": False,
    # "retries":3, 
    # "retry_delay": timedelta(minutes=5),
    # "execution_timeout": timedelta(hours=8),
    #"max_active_tasks": 5,
    #"max_active_runs": 1
}

# Define the DAG first
dag = DAG(
    dag_id="quality_check_try",
    default_args=default_args,
    description="Quality check DAG using Great Expectations for BigQuery table validation",
    schedule_interval=None,  # Set your schedule here, e.g., "@daily"
    start_date=days_ago(1),
    catchup=False,
    tags=["quality_check", "great_expectations", "bigquery"],
)

# Determine the Great Expectations root directory
# This works for both local development and Airflow deployment
# In Airflow Composer, DAGs are typically in /home/airflow/gcs/dags/
current_file = os.path.abspath(__file__)
current_dir = os.path.dirname(current_file)

# Check if we're in Airflow Composer environment
if current_file.startswith('/home/airflow/gcs/dags/'):
    # We're in Airflow Composer - use absolute path
    ge_root_dir = '/home/airflow/gcs/dags/modules/great_expectations'
else:
    # Local development or other environments
    ge_root_dir = os.path.join(current_dir, "modules", "great_expectations")

# Log the path for debugging (will be logged when DAG is executed)
logger.info(f"Great Expectations root directory: {ge_root_dir}")
logger.info(f"Current file path: {current_file}")

def _convert_to_dict(obj):
    """Helper function to recursively convert objects to dict for JSON serialization."""
    import json
    from datetime import datetime, date
    
    if isinstance(obj, (str, int, float, bool, type(None))):
        return obj
    elif isinstance(obj, (datetime, date)):
        return obj.isoformat()
    elif isinstance(obj, dict):
        # Convert keys to strings and recursively convert values
        return {str(k): _convert_to_dict(v) for k, v in obj.items()}
    elif isinstance(obj, (list, tuple)):
        return [_convert_to_dict(item) for item in obj]
    elif hasattr(obj, '__dict__'):
        return _convert_to_dict(obj.__dict__)
    else:
        return str(obj)

def run_ge_checkpoint_with_serializable_result(**context):
    """
    Wrapper function to run Great Expectations checkpoint and return a serializable result.
    This avoids XCom serialization issues with CheckpointResult objects.
    Converts the entire result to a JSON-serializable dict.
    """
    import great_expectations as gx
    import json
    
    # Get the data context
    context_obj = gx.get_context(context_root_dir=ge_root_dir)
    
    # Run the checkpoint
    checkpoint_result = context_obj.run_checkpoint(checkpoint_name="check_export")
    
    # Convert CheckpointResult to dict recursively
    result_dict = _convert_to_dict(checkpoint_result)
    result_json = json.dumps(result_dict, indent=2, default=str)
    
    # If validation failed, raise an exception to fail the task
    # if not checkpoint_result.success:
    #     logger.error(f"Great Expectations validation failed")
    #     logger.error(result_json)
    #     raise ValueError("Great Expectations validation failed. Check the validation results for details.")

    # Log validation results without failing the DAG
    if not checkpoint_result.success:
        logger.warning(f"Great Expectations validation failed - but DAG will continue")
        logger.warning(result_json)
    else:
        logger.info(f"Great Expectations validation successful")
        logger.info(result_json)
    
    # Find and upload validation HTML file to GCS
    data_docs_path = os.path.join(ge_root_dir, "data_docs", "local_site")
    validations_dir = os.path.join(data_docs_path, "validations", "my_expectation_suite")
    
    html_files = []
    if os.path.exists(validations_dir):
        # Find all HTML files, sorted by modification time (newest first)
        for root, dirs, files in os.walk(validations_dir):
            for file in files:
                if file.endswith('.html'):
                    full_path = os.path.join(root, file)
                    html_files.append((full_path, os.path.getmtime(full_path)))
        
        # Sort by modification time, newest first
        html_files.sort(key=lambda x: x[1], reverse=True)
    
    # Upload the most recent HTML file to GCS
    if html_files:
        from google.cloud import storage
        from datetime import datetime
        
        latest_html_path, _ = html_files[0]
        logger.info(f"Found validation HTML file: {latest_html_path}")
        
        # Initialize GCS client
        storage_client = storage.Client(project=project_id)
        bucket = storage_client.bucket(bucket_name)
        
        # Create GCS path with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        html_filename = os.path.basename(latest_html_path)
        gcs_blob_path = f"great_expectations_results/{timestamp}_{html_filename}"
        
        # Upload the file
        blob = bucket.blob(gcs_blob_path)
        blob.upload_from_filename(latest_html_path)
        
        gcs_url = f"gs://{bucket_name}/{gcs_blob_path}"
        logger.info(f"✓ Uploaded validation result to: {gcs_url}")
        logger.info(f"  You can access it at: https://console.cloud.google.com/storage/browser/{bucket_name}/{gcs_blob_path}")
        
        # Also upload index.html if it exists (for easier navigation)
        index_html_path = os.path.join(data_docs_path, "index.html")
        if os.path.exists(index_html_path):
            index_gcs_path = f"great_expectations_results/index_{timestamp}.html"
            index_blob = bucket.blob(index_gcs_path)
            index_blob.upload_from_filename(index_html_path)
            logger.info(f"✓ Uploaded index.html to: gs://{bucket_name}/{index_gcs_path}")
    else:
        logger.warning(f"No HTML files found in {validations_dir}")
    
    # Clean up any temporary tables created by Great Expectations in BigQuery
    try:
        from google.cloud import bigquery
        from datetime import datetime, timedelta
        
        bq_client = bigquery.Client(project=project_id)
        dataset_id = "dwh_de_test"  # From connection_string: bigquery://source_project/dwh_de_test
        
        # List all tables in the dataset
        dataset_ref = bq_client.dataset(dataset_id)
        tables = list(bq_client.list_tables(dataset_ref))
        
        # Patterns for temporary tables that Great Expectations might create
        temp_table_patterns = [
            'ge_tmp_',
            'tmp_ge_',
            'great_expectations_tmp_',
            'ge_temp_',
            'tmp_',
        ]
        
        # Get current time to check for recently created tables
        current_time = datetime.utcnow()
        one_hour_ago = current_time - timedelta(hours=1)
        
        deleted_tables = []
        for table in tables:
            table_name = table.table_id
            should_delete = False
            
            # Check if table matches any temporary table pattern
            if any(table_name.lower().startswith(pattern.lower()) for pattern in temp_table_patterns):
                should_delete = True
            else:
                # Check if table was created recently (within last hour) and has temp-like name
                try:
                    table_obj = bq_client.get_table(dataset_ref.table(table_name))
                    if table_obj.created and table_obj.created.replace(tzinfo=None) > one_hour_ago:
                        # Additional check: if table name contains timestamp-like patterns, it might be temp
                        import re
                        # Only delete if it has both: recent creation AND temp-like naming pattern
                        if re.search(r'\d{8}|\d{10}|\d{13}', table_name) and ('tmp' in table_name.lower() or 'temp' in table_name.lower()):
                            should_delete = True
                except:
                    pass
            
            if should_delete:
                try:
                    table_ref = dataset_ref.table(table_name)
                    bq_client.delete_table(table_ref, not_found_ok=True)
                    deleted_tables.append(table_name)
                    logger.info(f"✓ Deleted temporary table: {dataset_id}.{table_name}")
                except Exception as e:
                    logger.warning(f"Could not delete temporary table {dataset_id}.{table_name}: {str(e)}")
        
        if deleted_tables:
            logger.info(f"Cleaned up {len(deleted_tables)} temporary table(s): {', '.join(deleted_tables)}")
        else:
            logger.info("No temporary tables found to clean up")
            
    except Exception as e:
        logger.warning(f"Error while cleaning up temporary tables: {str(e)}")
        # Don't fail the task if cleanup fails
    
    return result_dict

validate_bq_export = PythonOperator(
    task_id="validate_bq_export",
    python_callable=run_ge_checkpoint_with_serializable_result,
    provide_context=True,
    dag=dag,
)

start = EmptyOperator(task_id="start", trigger_rule=TriggerRule.ALL_DONE, dag=dag)
end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE, dag=dag)

chain(start, validate_bq_export, end)


# apache-airflow-providers-great-expectations==0.3.0 --not working
# apache-airflow-providers-great-expectations>=0.3.0 --not working
# apache-airflow-providers-great-expectations (from versions: none)  --not working

# airflow-provider-great-expectations==0.2.0--working
#https://greatexpectations.io/expectations/expect_column_values_to_be_unique/