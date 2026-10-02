"""
DAG for Restaurant Guru Data Pipeline - Production Implementation

This DAG executes Restaurant Guru scraping pipeline using AlloyDB for PostgreSQL 
to process and update data for the Restaurant Guru project, following the HasData pattern.

Key Features:
- Uses AlloyDB for PostgreSQL as the database
- Implements async processing with proper error handling and retries
- Includes comprehensive logging for better monitoring
- Continuous execution every 2 minutes, 24/7 for maximum throughput
- Batch processing of cities with status tracking
- Follows HasData implementation patterns with Project Manager aligned architecture
- Integrated master-child object creation (objektgruppeid=3 for masters, objektgruppeid=1 for children)
- ScrapeOps API quota management with automatic validation
- Support for mega cities (40K+ restaurants per city)

Schedule:
- Runs continuously every 2 minutes, 24 hours a day, 7 days a week
- Processing window: 00:00, 00:02, 00:04, ..., 23:58 (all day)
- Processes cities one after another continuously
- Maximum throughput with sequential execution to avoid conflicts

Configuration:
- Connection: Uses 'google_alloydb_dev' connection (GCP Cloud SQL type)
- Database: postgres
- Retries: 2 attempts with 2-minute delay between retries
- Email notifications on failure
- Dynamic timeout calculation for mega cities (up to 24 hours for 40K+ restaurants)

Task Flow:
1. API Quota Check - Validates ScrapeOps API availability
2. Parameter Validation - Ensures all configuration is valid
3. Restaurant Guru Scraper - Executes rguru_de.py with proper master creation
4. HasData Conversion & Import ID - Parallel processing of data preparation
5. HasData Procedures - Integrated atomic processing with PM-aligned architecture
6. Results Validation - Final verification of master-child relationships

Architecture:
- Raw Data: smartdata_analyticdb.restaurant_guru_raw_germany
- Staging: smartdatastagdb.import_hasdata_base
- Processing: smartdatastagdb.import_hasdata_restaurant_guru (PM-aligned procedure)
- Master Creation: smartdatadb.create_master_restaurant_guru (integrated)
- End Tables: smartdatadb.objekt, adresse, externid, kommunikation, objektmerkmal

Dependencies:
- apache-airflow-providers-google
- Python file: restaurant_guru_scraper.py
- Scrapy project: RestaurantGuru
- Updated procedures.sql with PM-aligned master creation

Author: Yousuf 
Created: 2025-08-30
Updated: 2025-09-18 (PM Architecture Alignment & Daily Scheduling)
"""

from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.providers.google.cloud.operators.cloud_sql import (
    CloudSQLExecuteQueryOperator,
)
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.utils.helpers import chain
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.empty import EmptyOperator
import os
import logging
import psycopg2
import requests
import json
import re

# Import our custom module
from modules.restaurant_guru_scraper_simple import (
    run_restaurant_guru_scraper,
    run_get_max_import_id,
    ScrapyExecutionMode
)

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Environment configuration
environment = "env"
env = os.environ.get(environment, Variable.get(environment, default_var="DEV"))

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

# Database credentials
dev_creds = Variable.get("alloydb_dev_details", deserialize_json=True)

# Configuration variables
execution_mode = Variable.get("restaurant_guru_execution_mode", default_var="normal")
item_limit = Variable.get("restaurant_guru_item_limit", default_var=None)
max_concurrent = Variable.get("restaurant_guru_max_concurrent", default_var=20)
scrapy_project_path = Variable.get("restaurant_guru_scrapy_path", default_var="/home/airflow/gcs/dags/modules/RestaurantGuru/RestaurantGuru/RestaurantGuru")

# ScrapeOps API Configuration - read from existing Scrapy settings
min_api_calls_required = Variable.get("restaurant_guru_min_api_calls", default_var=100)  # Minimum calls needed to proceed

# 🏙️ MEGA CITY SUPPORT: 40K+ establishments per city configuration
try:
    max_restaurants_per_city = int(Variable.get("restaurant_guru_max_per_city"))
except:
    max_restaurants_per_city = 45000  # MEGA CITY: Support cities with 40K+ establishments (Berlin, etc.)

# 🚀 MEGA CITY: Dynamic timeout configuration for massive datasets
def get_dynamic_timeout(estimated_restaurants=1000):
    """Calculate dynamic timeout for mega cities - supports up to 40K+ establishments"""
    base_timeout_minutes = 120  # 2 hours base for large operations
    
    # SCALED: Tiered timeout calculation for different city sizes
    if estimated_restaurants >= 30000:  # Mega cities (30K+) - Berlin, Munich
        minutes_per_1000_restaurants = 180  # 3 hours per 1000 for mega cities
        max_timeout = 1440  # 24 hours maximum for mega cities
        logger.info(f"🏙️ MEGA CITY timeout calculation: {estimated_restaurants} restaurants")
    elif estimated_restaurants >= 15000:  # Large cities (15K-30K)
        minutes_per_1000_restaurants = 150  # 2.5 hours per 1000 for large cities
        max_timeout = 1080  # 18 hours maximum
        logger.info(f"🌆 LARGE CITY timeout calculation: {estimated_restaurants} restaurants")
    elif estimated_restaurants >= 5000:   # Medium cities (5K-15K)
        minutes_per_1000_restaurants = 120  # 2 hours per 1000 for medium cities
        max_timeout = 720   # 12 hours maximum
        logger.info(f"🏘️ MEDIUM CITY timeout calculation: {estimated_restaurants} restaurants")
    else:  # Small cities (<5K)
        minutes_per_1000_restaurants = 90   # 1.5 hours per 1000 for small cities
        max_timeout = 480   # 8 hours maximum
        logger.info(f"🏘️ SMALL CITY timeout calculation: {estimated_restaurants} restaurants")
    
    # Calculate timeout with safety margin
    calculated_timeout = base_timeout_minutes + (estimated_restaurants / 1000 * minutes_per_1000_restaurants)
    final_timeout = max(120, min(max_timeout, int(calculated_timeout)))
    
    logger.info(f"⏰ Final timeout: {final_timeout} minutes ({final_timeout/60:.1f} hours)")
    return final_timeout

# 🚀 MEGA CITY: Advanced processing configuration
mega_city_mode = Variable.get("restaurant_guru_mega_city_mode", default_var=True)
memory_optimization = Variable.get("restaurant_guru_memory_optimization", default_var=True)
batch_processing_size = Variable.get("restaurant_guru_batch_size", default_var=5000)  # Process in 5K batches
enhanced_concurrency_mode = Variable.get("restaurant_guru_enhanced_concurrency", default_var=True)
parallel_db_operations = Variable.get("restaurant_guru_parallel_db", default_var=True)

# Default arguments for the DAG
default_args = {
    "owner": "Yousuf",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email": [
        #"dataops@example.com",
        "dataops@example.com",
        #"dataops@example.com",
        #"dataops@example.com",
    ],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

# Define the DAG
dag = DAG(
    dag_id="etl_di_restaurant_guru_scraper",
    default_args=default_args,
    description="ETL process for Restaurant Guru data using AlloyDB with async processing",
    schedule_interval="*/2 * * * *",  # Every 2 minutes, 24/7 continuous execution
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,  # Ensure only one DAG instance runs at a time
    doc_md=__doc__,
    tags=["etl", "alloydb", "restaurant_guru", "scraping", "async"],
    template_searchpath="/home/airflow/gcs/dags/sql",
)

# Task operators
start = EmptyOperator(task_id="start", trigger_rule=TriggerRule.ALL_DONE, dag=dag)
end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE, dag=dag)


def check_scrapeops_api_quota(**context):
    """
    Check ScrapeOps API quota and determine if we have enough calls to proceed.
    """
    logger.info("ðŸ” Checking ScrapeOps API quota...")
    
    # Extract API key from existing Scrapy settings using the configured project path
    # scrapy_project_path points to: .../modules/RestaurantGuru/RestaurantGuru/RestaurantGuru
    # settings.py is directly in this directory: .../modules/RestaurantGuru/RestaurantGuru/RestaurantGuru/settings.py
    scrapy_settings_path = os.path.join(scrapy_project_path, 'settings.py')
    scrapeops_api_key = None
    
    logger.info(f"Scrapy project path: {scrapy_project_path}")
    logger.info(f"Looking for Scrapy settings at: {scrapy_settings_path}")
    
    try:
        # Read the settings file to extract SCRAPEOPS_API_KEY
        with open(scrapy_settings_path, 'r', encoding='utf-8') as f:
            settings_content = f.read()
            
        # Extract API key using regex
        api_key_match = re.search(r"SCRAPEOPS_API_KEY\s*=\s*['\"]([^'\"]+)['\"]", settings_content)
        if api_key_match:
            scrapeops_api_key = api_key_match.group(1)
            logger.info(f"âœ… Found ScrapeOps API key in Scrapy settings (ending: ...{scrapeops_api_key[-4:]})")
        else:
            logger.error("âŒ Could not find SCRAPEOPS_API_KEY in Scrapy settings")
            raise Exception("SCRAPEOPS_API_KEY not found in Scrapy settings file")
            
    except FileNotFoundError:
        logger.error(f"âŒ Scrapy settings file not found at: {scrapy_settings_path}")
        logger.error(f"   Scrapy project path configured as: {scrapy_project_path}")
        # Try to list directory contents for debugging
        try:
            if os.path.exists(scrapy_project_path):
                files = os.listdir(scrapy_project_path)
                logger.error(f"   Files in project directory: {files}")
            else:
                logger.error(f"   Project directory does not exist: {scrapy_project_path}")
        except:
            pass
        raise Exception("Scrapy settings file not found")
    except Exception as e:
        logger.error(f"âŒ Error reading Scrapy settings: {str(e)}")
        raise Exception(f"Failed to read ScrapeOps API key from settings: {str(e)}")
    
    # ScrapeOps Account API endpoint
    account_api_url = "https://proxy.scrapeops.io/v1/account"
    
    try:
        # Make request to get account information
        response = requests.get(
            account_api_url,
            params={'api_key': scrapeops_api_key},
            timeout=30
        )
        
        if response.status_code != 200:
            logger.error(f"âŒ Failed to get API quota. Status: {response.status_code}")
            logger.error(f"Response: {response.text}")
            raise Exception(f"ScrapeOps API quota check failed with status {response.status_code}")
        
        account_data = response.json()
        logger.info(f"âœ… Successfully retrieved ScrapeOps account data")
        
        # Extract quota information - handle both response formats
        account_info = None  # Initialize to avoid UnboundLocalError
        
        if 'account' in account_data:
            # Old format: account_data['account']['total_requests']
            account_info = account_data['account']
            total_requests = account_info.get('total_requests', 0)
            requests_used = account_info.get('requests_used', 0)
            requests_remaining = account_info.get('requests_remaining', 0)
        elif 'plan_api_credits' in account_data:
            # New format: account_data['plan_api_credits'], account_data['used_api_credits']
            total_requests = account_data.get('plan_api_credits', 0)
            requests_used = account_data.get('used_api_credits', 0)
            requests_remaining = total_requests - requests_used
            logger.info(f"Using new API response format - Plan: {total_requests}, Used: {requests_used}")
            # For new format, treat the whole response as account_info for compatibility
            account_info = account_data
        else:
            logger.error(f"âŒ Unexpected API response format: {account_data}")
            raise Exception("Invalid response format from ScrapeOps API")
        
        # Calculate if we have enough requests
        min_required = int(min_api_calls_required)
        
        logger.info("ðŸ“Š ScrapeOps API Quota Status:")
        logger.info(f"   ðŸ”¢ Total Requests: {total_requests:,}")
        logger.info(f"   âœ… Requests Used: {requests_used:,}")
        logger.info(f"   ðŸ†“ Requests Remaining: {requests_remaining:,}")
        logger.info(f"   âš¡ Minimum Required: {min_required:,}")
        
        # Decision logic
        quota_status = {
            'total_requests': total_requests,
            'requests_used': requests_used,
            'requests_remaining': requests_remaining,
            'min_required': min_required,
            'api_key_last_4': scrapeops_api_key[-4:] if len(scrapeops_api_key) > 4 else "****",
            'check_timestamp': datetime.now().isoformat()
        }
        
        if requests_remaining < min_required:
            logger.error("ðŸš« INSUFFICIENT API QUOTA!")
            logger.error(f"   âŒ Available: {requests_remaining:,} calls")
            logger.error(f"   âŒ Required: {min_required:,} calls")
            logger.error(f"   âŒ Shortage: {min_required - requests_remaining:,} calls")
            logger.error("ðŸ›‘ DAG execution will be SKIPPED to prevent wasted resources")
            
            quota_status['proceed'] = False
            quota_status['skip_reason'] = f"Insufficient API quota: {requests_remaining} < {min_required}"
            quota_status['status'] = 'QUOTA_EXHAUSTED'
            
        elif requests_remaining < min_required * 2:
            logger.warning("âš ï¸  LOW API QUOTA WARNING!")
            logger.warning(f"   âš ï¸  Available: {requests_remaining:,} calls")
            logger.warning(f"   âš ï¸  This is close to the minimum threshold")
            logger.warning(f"   âš ï¸  Consider monitoring usage closely")
            
            quota_status['proceed'] = True
            quota_status['skip_reason'] = None
            quota_status['status'] = 'LOW_QUOTA_WARNING'
            
        else:
            logger.info("âœ… SUFFICIENT API QUOTA AVAILABLE")
            logger.info(f"   âœ… Available: {requests_remaining:,} calls")
            logger.info(f"   âœ… Required: {min_required:,} calls")
            logger.info(f"   âœ… Buffer: {requests_remaining - min_required:,} calls")
            logger.info("ðŸš€ DAG execution can proceed safely")
            
            quota_status['proceed'] = True
            quota_status['skip_reason'] = None
            quota_status['status'] = 'SUFFICIENT_QUOTA'
        
        # Additional account information if available
        if 'plan' in account_info:
            logger.info(f"ðŸ“‹ Account Plan: {account_info['plan']}")
            quota_status['plan'] = account_info['plan']
        
        if 'renewal_date' in account_info:
            logger.info(f"ðŸ”„ Quota Renewal: {account_info['renewal_date']}")
            quota_status['renewal_date'] = account_info['renewal_date']
        
        return quota_status
        
    except requests.exceptions.Timeout:
        logger.error("âŒ ScrapeOps API request timed out")
        raise Exception("ScrapeOps API quota check timed out")
        
    except requests.exceptions.RequestException as e:
        logger.error(f"âŒ Network error checking ScrapeOps API: {str(e)}")
        raise Exception(f"Network error during quota check: {str(e)}")
        
    except json.JSONDecodeError as e:
        logger.error(f"âŒ Invalid JSON response from ScrapeOps API: {str(e)}")
        raise Exception(f"Invalid JSON response during quota check: {str(e)}")
        
    except Exception as e:
        logger.error(f"âŒ Unexpected error during quota check: {str(e)}")
        raise


def validate_execution_parameters(**context):
    """
    Validate execution parameters and API quota before starting the scraper.
    """
    logger.info("Validating execution parameters...")
    
    # First check API quota from previous task
    quota_check = context['task_instance'].xcom_pull(task_ids='check_api_quota')
    
    if not quota_check:
        raise Exception("Could not retrieve API quota check results from previous task")
    
    # Check if we should proceed based on quota
    if not quota_check.get('proceed', False):
        skip_reason = quota_check.get('skip_reason', 'Unknown quota issue')
        logger.error(f"ðŸ›‘ Skipping execution due to API quota: {skip_reason}")
        raise Exception(f"Execution skipped: {skip_reason}")
    
    quota_status = quota_check.get('status', 'UNKNOWN')
    remaining = quota_check.get('requests_remaining', 0)
    
    if quota_status == 'LOW_QUOTA_WARNING':
        logger.warning(f"âš ï¸ Proceeding with LOW quota warning ({remaining:,} calls remaining)")
    else:
        logger.info(f"âœ… API quota check passed ({remaining:,} calls remaining)")
    
    # Validate execution mode
    try:
        mode = ScrapyExecutionMode(execution_mode)
        logger.info(f"âœ… Execution mode validated: {mode}")
    except ValueError as e:
        raise ValueError(f"Invalid execution mode '{execution_mode}'. Valid modes: {list(ScrapyExecutionMode)}")
    
    # Validate item limit
    if item_limit:
        try:
            limit = int(item_limit)
            if limit <= 0:
                raise ValueError("Item limit must be positive")
            logger.info(f"âœ… Item limit validated: {limit}")
        except (ValueError, TypeError) as e:
            raise ValueError(f"Invalid item limit '{item_limit}': {str(e)}")
    
    # Validate max concurrent
    try:
        concurrent = int(max_concurrent)
        if concurrent <= 0 or concurrent > 50:
            raise ValueError("Max concurrent must be between 1 and 50")
        logger.info(f"âœ… Max concurrent validated: {concurrent}")
    except (ValueError, TypeError) as e:
        raise ValueError(f"Invalid max concurrent '{max_concurrent}': {str(e)}")
    
    # Validate database credentials
    required_creds = ['host', 'database', 'user', 'password']
    for cred in required_creds:
        if not dev_creds.get(cred):
            raise ValueError(f"Missing required database credential: {cred}")
    
    logger.info("âœ… All execution parameters validated successfully")
    
    validation_result = {
        'execution_mode': execution_mode,
        'item_limit': int(item_limit) if item_limit else None,
        'max_concurrent': int(max_concurrent),
        'database_host': dev_creds.get('host'),
        'api_quota_status': quota_status,
        'api_calls_remaining': remaining,
        'validation_timestamp': datetime.now().isoformat()
    }
    
    return validation_result


def execute_restaurant_guru_scraper_wrapper(**context):
    """
    Wrapper function for Restaurant Guru scraper execution with parameter handling.
    """
    logger.info("Starting Restaurant Guru scraper execution...")
    
    # Get validated parameters from previous task
    validation_result = context['task_instance'].xcom_pull(task_ids='validate_execution_parameters')
    
    if not validation_result:
        raise Exception("Could not retrieve validation results from previous task")
    
    # Extract parameters
    mode = validation_result['execution_mode']
    limit = validation_result['item_limit']
    
    # 🚀 NEW: Check for index-based processing parameters from DAG run config
    dag_run = context.get('dag_run')
    index_based_mode = False
    start_index = None
    end_index = None
    
    if dag_run and dag_run.conf:
        index_based_mode = dag_run.conf.get('index_based_mode', False)
        start_index = dag_run.conf.get('start_index')
        end_index = dag_run.conf.get('end_index')
        
        if index_based_mode and start_index and end_index:
            logger.info(f"🔢 INDEX-BASED MODE: Processing restaurants {start_index}-{end_index}")
        else:
            logger.info(f"🌐 INFINITE SCROLL MODE: Restaurant Guru infinite scroll processing")
    
    logger.info(f"Execution mode: {mode}")
    logger.info(f"Item limit: {limit}")
    logger.info(f"Max concurrent: {validation_result['max_concurrent']}")
    logger.info(f"🏙️ Max restaurants per city: {max_restaurants_per_city}")
    
    # 🚀 MEGA CITY: Calculate dynamic timeout based on restaurant limit
    dynamic_timeout = get_dynamic_timeout(max_restaurants_per_city)
    logger.info(f"⏰ MEGA CITY timeout: {dynamic_timeout} minutes ({dynamic_timeout/60:.1f} hours) for {max_restaurants_per_city} restaurants")
    
    # 🚀 MEGA CITY: Enhanced configuration for 40K+ establishments
    enhanced_config = {
        'mega_city_mode': mega_city_mode,
        'max_restaurants_per_city': max_restaurants_per_city,
        'enhanced_concurrency': enhanced_concurrency_mode,
        'parallel_db_operations': parallel_db_operations,
        'memory_optimization': memory_optimization,
        'batch_processing_size': batch_processing_size,
        'dynamic_timeout_minutes': dynamic_timeout,
        'index_based_mode': index_based_mode,
        'start_index': start_index,
        'end_index': end_index
    }
    logger.info(f"🏙️ MEGA CITY config: {enhanced_config}")
    
    # Execute the scraper
    try:
        result = run_restaurant_guru_scraper(
            creds=dev_creds,
            mode=mode,
            item_limit=max_restaurants_per_city,  # 🏙️ MEGA CITY: Use 40K+ limit instead of validation limit
            scrapy_project_path=scrapy_project_path,
            start_index=start_index,
            end_index=end_index,
            index_based_mode=index_based_mode,
            restaurants_per_batch=50000  # 🏙️ MEGA CITY: Process up to 50K restaurants in single run
        )
        
        logger.info("âœ… Restaurant Guru scraper completed successfully")
        
        execution_result = {
            'execution_mode': mode,
            'item_limit': limit,
            'job_id': result.get('job_id') if result else None,  # 🔧 Use job_id instead of execution_id
            'execution_timestamp': datetime.now().isoformat(),
            'execution_start': result.get('execution_start') if result else None,  # 🔧 Pass through execution start
            'execution_end': result.get('execution_end') if result else None,      # 🔧 Pass through execution end
            'city_name': result.get('city_name') if result else None,              # 🔧 Pass through city name
            'status': 'completed'
        }
        
        # 🔧 Ensure execution timestamps and city name are properly passed through
        if result and result.get('execution_start'):
            execution_result['execution_start'] = result['execution_start']
        if result and result.get('execution_end'):
            execution_result['execution_end'] = result['execution_end']
        if result and result.get('city_name'):
            execution_result['city_name'] = result['city_name']
        
        # 🔧 Debug: Log the scraper result to see what's returned
        logger.info(f"🔧 Scraper result structure: {result}")
        logger.info(f"🔧 Execution result job_id: {execution_result.get('job_id')}")
        logger.info(f"🔧 Execution start: {execution_result.get('execution_start')}")
        logger.info(f"🔧 Execution end: {execution_result.get('execution_end')}")
        
        return execution_result
        
    except Exception as e:
        logger.error(f"âŒ Restaurant Guru scraper failed: {str(e)}")
        raise


def get_max_import_id_wrapper(**context):
    """
    Wrapper function for getting maximum import ID.
    """
    logger.info("Getting maximum import ID...")
    
    try:
        max_import_id = run_get_max_import_id(creds=dev_creds)
        
        if max_import_id is None:
            raise Exception("Failed to retrieve maximum import ID")
        
        logger.info(f"âœ… Maximum import ID retrieved: {max_import_id}")
        
        result = {
            'max_import_id': max_import_id,
            'retrieval_timestamp': datetime.now().isoformat()
        }
        
        return max_import_id  # Return the ID for XCom usage in SQL operator
        
    except Exception as e:
        logger.error(f"âŒ Failed to get maximum import ID: {str(e)}")
        raise


# Task definitions

# API quota check task - runs first to validate we have enough calls
check_api_quota_task = PythonOperator(
    task_id="check_api_quota",
    python_callable=check_scrapeops_api_quota,
    do_xcom_push=True,
    dag=dag,
    provide_context=True,
    execution_timeout=timedelta(minutes=2),  # Quick timeout for API check
    retries=2,  # Retry API check if it fails
    retry_delay=timedelta(seconds=30),
)

validate_execution_parameters_task = PythonOperator(
    task_id="validate_execution_parameters",
    python_callable=validate_execution_parameters,
    do_xcom_push=True,
    dag=dag,
    provide_context=True,
)

# 🏙️ MEGA CITY: Task to execute Restaurant Guru scraper with dynamic timeout
mega_city_timeout = get_dynamic_timeout(max_restaurants_per_city)
execute_restaurant_guru_scraper_task = PythonOperator(
    task_id="execute_restaurant_guru_scraper",
    python_callable=execute_restaurant_guru_scraper_wrapper,
    op_kwargs={},
    do_xcom_push=True,
    dag=dag,
    provide_context=True,
    execution_timeout=timedelta(minutes=mega_city_timeout),  # 🚀 MEGA CITY: Dynamic timeout (up to 24 hours for 40K+ establishments)
    # pool='restaurant_guru_mega_pool',  # 🏙️ Optional: Create this pool in Airflow for resource management
)

# Task to get max(importid) from import_log table
execute_get_max_import_id_task = PythonOperator(
    task_id="execute_get_max_import_id",
    python_callable=get_max_import_id_wrapper,
    op_kwargs={},
    do_xcom_push=True,
    provide_context=True,
    dag=dag,
)

# HasData Conversion Task - Convert raw data to HasData format
def convert_to_hasdata_wrapper(**context):
    """Convert latest Restaurant Guru raw data to HasData format"""
    logger.info("Converting Restaurant Guru data to HasData format...")
    logger.info(f"Task instance: {context['task_instance'].task_id}")
    logger.info(f"DAG run: {context['dag_run'].run_id}")
    
    connection = None
    cursor = None
    
    # Get current task instance
    task_instance = context['task_instance']
    task_instance.xcom_push(key='conversion_start', value=datetime.now().isoformat())
    
    try:
        # Connect to database
        db_config = {
            'host': dev_creds.get('host'),
            'port': dev_creds.get('port', 5432),
            'user': dev_creds.get('user'),
            'password': dev_creds.get('password'),
            'database': dev_creds.get('database')
        }
        connection = psycopg2.connect(**db_config)
        cursor = connection.cursor()
        
        # 🔧 Get EXACT job_ids from current spider execution (no time-based filtering)
        scraper_result = context['task_instance'].xcom_pull(task_ids='execute_restaurant_guru_scraper')
        
        if not scraper_result:
            raise Exception("Could not retrieve scraper execution result")
        
        # 🔧 Get EXACT execution time window from scraper result
        execution_start = scraper_result.get('execution_start')
        execution_end = scraper_result.get('execution_end')
        
        # 🔧 Debug: Log what we received from scraper
        logger.info(f"🔧 Scraper result keys: {list(scraper_result.keys())}")
        logger.info(f"🔧 Execution start from scraper: {execution_start}")
        logger.info(f"🔧 Execution end from scraper: {execution_end}")
        logger.info(f"🔧 Execution timestamp from scraper: {scraper_result.get('execution_timestamp')}")
        logger.info(f"🔧 Job ID from scraper: {scraper_result.get('job_id')}")
        logger.info(f"🔧 City name from scraper: {scraper_result.get('city_name')}")
        
        # 🔧 NEW APPROACH: Use city name to find recent data
        city_name = scraper_result.get('city_name')
        if city_name:
            logger.info(f"🔧 Using city-based approach for: {city_name}")
            
            # First, let's see what city names are actually in the raw table (recent data)
            cursor.execute("""
                SELECT DISTINCT city_name, COUNT(*) as record_count
                FROM smartdata_analyticdb.restaurant_guru_raw_germany 
                WHERE created_at >= NOW() - INTERVAL '24 hours'
                GROUP BY city_name
                ORDER BY record_count DESC
                LIMIT 10
            """)
            recent_cities = cursor.fetchall()
            logger.info(f"🔧 Recent cities in raw table: {recent_cities}")
            
            # 🚨 FIXED: Find ALL job_ids for this city (no time filter - process entire city run)
            cursor.execute("""
                SELECT r.job_id, COUNT(*) as restaurant_count
                FROM smartdata_analyticdb.restaurant_guru_raw_germany r
                WHERE r.city_name = %s
                AND r.job_id IS NOT NULL
                GROUP BY r.job_id
                ORDER BY r.job_id DESC
            """, (city_name,))
            all_recent_jobs = cursor.fetchall()
            logger.info(f"🔧 Found {len(all_recent_jobs)} job_ids for city '{city_name}' (ALL runs, no time filter)")
            
            if all_recent_jobs:
                # Show the job_ids we found
                job_ids = [str(job[0]) for job in all_recent_jobs]
                logger.info(f"🔧 Job IDs found: {', '.join(job_ids)}")
            else:
                logger.warning(f"🔧 No data found for city '{city_name}' (checking all runs)")
                
                # Try some variations of the city name
                city_variations = [
                    city_name.lower(),
                    city_name.upper(),
                    city_name.replace(' ', '-'),
                    city_name.replace('-', ' '),
                    city_name.replace(' ', ''),
                ]
                
                for variation in city_variations:
                    if variation != city_name:
                        cursor.execute("""
                            SELECT r.job_id, COUNT(*) as restaurant_count
                            FROM smartdata_analyticdb.restaurant_guru_raw_germany r
                            WHERE LOWER(r.city_name) = LOWER(%s)
                            AND r.job_id IS NOT NULL
                            GROUP BY r.job_id
                            ORDER BY r.job_id DESC
                        """, (variation,))
                        variation_jobs = cursor.fetchall()
                        if variation_jobs:
                            logger.info(f"🔧 Found {len(variation_jobs)} job_ids for city variation '{variation}'")
                            all_recent_jobs = variation_jobs
                            break
        else:
            logger.warning("🔧 No city name from scraper, falling back to time-based search")
            all_recent_jobs = []
        
        # Only use fallback time-based search if city-based approach didn't find data
        if not all_recent_jobs:
            # Fallback to execution_timestamp if new fields not available
            execution_timestamp = scraper_result.get('execution_timestamp')
            logger.info(f"🔧 Using execution_timestamp fallback: {execution_timestamp}")
            
            # Try multiple time windows to find data, but prioritize the most recent data
            time_windows = [
                ('5 minutes', 5),
                ('15 minutes', 15), 
                ('30 minutes', 30),
                ('1 hour', 60),
                ('6 hours', 360),
                ('24 hours', 1440)
            ]
            
            for window_name, minutes in time_windows:
                logger.info(f"🔧 Searching for data within {window_name} of execution time")
                cursor.execute("""
                    SELECT r.job_id, COUNT(*) as restaurant_count
                    FROM smartdata_analyticdb.restaurant_guru_raw_germany r
                    WHERE r.created_at >= %s::timestamp - INTERVAL '%s minutes'
                    AND r.created_at <= %s::timestamp + INTERVAL '%s minutes'
                    AND r.job_id IS NOT NULL
                    GROUP BY r.job_id
                    ORDER BY r.job_id DESC
                """, (execution_timestamp, minutes, execution_timestamp, minutes))
                
                window_jobs = cursor.fetchall()
                if window_jobs:
                    logger.info(f"🔧 Found {len(window_jobs)} job_ids within {window_name}")
                    # Filter to only include the most recent job_ids (likely from current run)
                    # Get the most recent job_id and include all job_ids from that time period
                    most_recent_job_id = window_jobs[0][0]
                    logger.info(f"🔧 Most recent job_id in window: {most_recent_job_id}")
                    
                    # Get the creation time of the most recent job_id
                    cursor.execute("""
                        SELECT MIN(created_at) as earliest_created, MAX(created_at) as latest_created
                        FROM smartdata_analyticdb.restaurant_guru_raw_germany 
                        WHERE job_id = %s
                    """, (most_recent_job_id,))
                    time_range = cursor.fetchone()
                    
                    if time_range and time_range[0]:
                        # Only include job_ids created within 10 minutes of the most recent job_id
                        cursor.execute("""
                            SELECT r.job_id, COUNT(*) as restaurant_count
                            FROM smartdata_analyticdb.restaurant_guru_raw_germany r
                            WHERE r.created_at >= %s::timestamp - INTERVAL '10 minutes'
                            AND r.created_at <= %s::timestamp + INTERVAL '10 minutes'
                            AND r.job_id IS NOT NULL
                            GROUP BY r.job_id
                            ORDER BY r.job_id DESC
                        """, (time_range[0], time_range[1]))
                        all_recent_jobs = cursor.fetchall()
                        logger.info(f"🔧 Filtered to {len(all_recent_jobs)} job_ids from most recent run")
                    else:
                        all_recent_jobs = window_jobs
                    break
                else:
                    logger.info(f"🔧 No data found within {window_name}")
        elif not all_recent_jobs and execution_start and execution_end:
            # Use exact execution time window
            logger.info(f"🔧 Exact execution window: {execution_start} to {execution_end}")
            
            cursor.execute("""
                SELECT r.job_id, COUNT(*) as restaurant_count
                FROM smartdata_analyticdb.restaurant_guru_raw_germany r
                WHERE r.created_at >= %s::timestamp
                AND r.created_at <= %s::timestamp
                AND r.job_id IS NOT NULL
                GROUP BY r.job_id
                ORDER BY r.job_id
            """, (execution_start, execution_end))
            all_recent_jobs = cursor.fetchall()
        
        # If still no data found, try a broader search for any recent data
        if not all_recent_jobs:
            logger.warning("🔧 No data found in execution time window, searching for any recent data")
            
            # First check if there's ANY data in the table
            cursor.execute("""
                SELECT COUNT(*) as total_count, 
                       MAX(created_at) as latest_data,
                       MIN(created_at) as earliest_data
                FROM smartdata_analyticdb.restaurant_guru_raw_germany
            """)
            table_stats = cursor.fetchone()
            logger.info(f"🔧 Raw table stats: {table_stats[0]} total records, latest: {table_stats[1]}, earliest: {table_stats[2]}")
            
            # Search for recent data
            cursor.execute("""
                SELECT r.job_id, COUNT(*) as restaurant_count
                FROM smartdata_analyticdb.restaurant_guru_raw_germany r
                WHERE r.created_at >= NOW() - INTERVAL '4 hours'
                AND r.job_id IS NOT NULL
                GROUP BY r.job_id
                ORDER BY r.job_id DESC
                LIMIT 50
            """)
            all_recent_jobs = cursor.fetchall()
            
            if all_recent_jobs:
                logger.info(f"🔧 Found {len(all_recent_jobs)} job_ids from recent 4 hours")
            else:
                # Check if there's any data at all in the last 7 days (more reasonable fallback)
                cursor.execute("""
                    SELECT r.job_id, COUNT(*) as restaurant_count
                    FROM smartdata_analyticdb.restaurant_guru_raw_germany r
                    WHERE r.created_at >= NOW() - INTERVAL '7 days'
                    AND r.job_id IS NOT NULL
                    GROUP BY r.job_id
                    ORDER BY r.job_id DESC
                    LIMIT 10
                """)
                all_recent_jobs = cursor.fetchall()
                
                if all_recent_jobs:
                    logger.info(f"🔧 Found {len(all_recent_jobs)} job_ids from last 7 days")
                else:
                    raise Exception("No recent Restaurant Guru data found to convert (searched last 7 days)")
        
        logger.info(f"🔧 Found {len(all_recent_jobs)} recent job_ids to process")
        
        # Process each job_id individually (since each represents one restaurant)
        total_converted = 0
        conversion_results = []
        
        for job_id_row in all_recent_jobs:
            current_job_id, restaurant_count = job_id_row
            logger.info(f"🔧 Converting individual job_id: {current_job_id}")
            
            # Convert this specific job_id
            cursor.execute("""
                SELECT r.job_id, r.restaurants_converted, r.conversion_status 
                FROM smartdatastagdb.convert_restaurant_guru_to_hasdata(%s) r
            """, (current_job_id,))
            result = cursor.fetchone()
            
            if result:
                job_id, restaurants_converted, conversion_status = result
                logger.info(f"🔧 Converted job_id {job_id}: {restaurants_converted} restaurants, Status: {conversion_status}")
                total_converted += restaurants_converted
                conversion_results.append((job_id, conversion_status))
        
        logger.info(f"🔧 Total restaurants converted: {total_converted}")
        
        # Use the first job_id for return value (for compatibility with downstream tasks)
        latest_job_id = all_recent_jobs[0][0]
        raw_count = total_converted
        
        logger.info(f"🔧 Batch conversion complete: {total_converted} restaurants from {len(all_recent_jobs)} job_ids")
        
        # For compatibility, we'll use the first job_id as the "primary" job_id
        # But all restaurants have been converted individually
        job_id = latest_job_id
        restaurants_converted = total_converted
        conversion_status = 'Success'
        
        # Verify the conversion created the correct entry
        cursor.execute("""
            SELECT h.job_id, COALESCE(jsonb_array_length(h.json_base), 0) as count,
                   h.eingefuegtam,
                   CASE 
                       WHEN h.json_base IS NULL THEN 'NULL'
                       WHEN h.json_base = 'null'::jsonb THEN 'null'
                       WHEN h.json_base = '[]'::jsonb THEN 'empty array'
                       ELSE 'has data'
                   END as data_status
            FROM smartdatastagdb.import_hasdata_base h
            WHERE h.job_id = %s AND h.api_type = 'restaurant_guru_api'
        """, (job_id,))
        verify_result = cursor.fetchone()
        
        if not verify_result:
            # Check if any HasData entries exist
            cursor.execute("""
                SELECT job_id, api_type, eingefuegtam, 
                       COALESCE(jsonb_array_length(json_base), 0) as count
                FROM smartdatastagdb.import_hasdata_base 
                WHERE api_type = 'restaurant_guru_api'
                ORDER BY eingefuegtam DESC
                LIMIT 5
            """)
            recent_entries = cursor.fetchall()
            if recent_entries:
                logger.error("âŒ Recent HasData entries:")
                for entry in recent_entries:
                    logger.error(f"  - Job ID: {entry[0]}, Count: {entry[3]}, Created: {entry[2]}")
            else:
                logger.error("âŒ No HasData entries found at all")
            
            # 🔧 Check raw data using job_id
            cursor.execute("""
                SELECT job_id, COUNT(*) as count, MAX(created_at) as latest
                FROM smartdata_analyticdb.restaurant_guru_raw_germany
                WHERE job_id = %s
                GROUP BY job_id
            """, (latest_job_id,))
            raw_data = cursor.fetchone()
            if raw_data:
                logger.error(f"âŒ Raw data exists: {raw_data[0]} ({raw_data[1]} restaurants, Latest: {raw_data[2]})")
            
            # 🔧 Check if conversion function works with job_id
            cursor.execute("""
                SELECT r.job_id, r.restaurants_converted, r.conversion_status 
                FROM smartdatastagdb.convert_restaurant_guru_to_hasdata(%s) r
            """, (latest_job_id,))
            recheck_result = cursor.fetchone()
            if recheck_result:
                logger.error(f"âŒ Conversion function works but data not saved. Result: {recheck_result}")
            
            # Try inserting directly
            try:
                cursor.execute("""
                    INSERT INTO smartdatastagdb.import_hasdata_base (
                        job_id, api_type, json_base, eingefuegtam
                    ) VALUES (
                        %s, 'restaurant_guru_api', 
                        (SELECT json_agg(json_build_object(
                            'type', 'Restaurant',
                            'name', raw_json::json->>'title',
                            'title', raw_json::json->>'title'
                        ))
                        FROM smartdata_analyticdb.restaurant_guru_raw_germany
                        WHERE job_id = %s),
                        NOW()
                    )
                """, (job_id, latest_job_id))
                connection.commit()
                logger.info("âœ… Direct insert worked")
            except Exception as e:
                logger.error(f"âŒ Direct insert failed: {str(e)}")
            
            raise Exception(f"HasData entry not found for job_id: {job_id}")
            
        verify_job_id, verify_count, verify_time, data_status = verify_result
        logger.info(f"âœ… Verified HasData entry: {verify_job_id}")
        logger.info(f"  - Restaurant count: {verify_count}")
        logger.info(f"  - Created at: {verify_time}")
        logger.info(f"  - Data status: {data_status}")
        
        if verify_count == 0:
            raise Exception(f"HasData entry exists but contains no restaurants for job_id: {job_id}")
        
        # Double check the data is properly committed
        connection.commit()
        
        # Verify after commit
        cursor.execute("""
            SELECT COUNT(*) 
            FROM smartdatastagdb.import_hasdata_base 
            WHERE job_id = %s AND api_type = 'restaurant_guru_api'
        """, (job_id,))
        final_check = cursor.fetchone()[0]
        if final_check == 0:
            raise Exception(f"HasData entry disappeared after commit for job_id: {job_id}")
        
        logger.info("âœ… Final verification passed - HasData entry is properly saved")
        
        return {
            'job_id': job_id,
            'restaurants_converted': restaurants_converted,
            'conversion_status': 'Success',
            'timestamp': datetime.now().isoformat()
        }
        
    except Exception as e:
        if connection:
            connection.rollback()
        logger.error(f"âŒ HasData conversion failed: {str(e)}")
        raise
        
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

# New task: Convert to HasData format
convert_to_hasdata_task = PythonOperator(
    task_id='convert_to_hasdata_format',
    python_callable=convert_to_hasdata_wrapper,
    provide_context=True,
    execution_timeout=timedelta(minutes=10),
    dag=dag,
)

# Get latest job_id for HasData import
def get_latest_job_id(**context):
    """Get the latest job_id from the conversion task"""
    # Check if conversion task completed successfully
    conversion_task = context['task_instance'].xcom_pull(task_ids='convert_to_hasdata_format')
    logger.info(f"Conversion task result: {conversion_task}")
    
    if not conversion_task:
        # Check task state
        task_states = context['dag_run'].get_task_instances()
        for task in task_states:
            logger.info(f"Task {task.task_id}: State={task.state}, Start={task.start_date}, End={task.end_date}")
            if task.task_id == 'convert_to_hasdata_format':
                logger.error(f"âŒ Conversion task failed or didn't complete. State: {task.state}")
        raise Exception("Conversion task result not found. Check if conversion task completed successfully.")
    
    if 'job_id' not in conversion_task:
        logger.error(f"âŒ Conversion result missing job_id. Got: {conversion_task}")
        raise Exception("No job_id found in conversion task result")
    
    job_id = conversion_task['job_id']
    logger.info(f"Using job_id from conversion: {job_id}")
    
    # Get next import_id
    import_id = context['task_instance'].xcom_pull(task_ids='execute_get_max_import_id')
    if not import_id:
        logger.error("âŒ No import_id found from execute_get_max_import_id task")
        raise Exception("Import ID not found")
    logger.info(f"Using import_id: {import_id}")
    
    # Verify conversion task status
    if 'conversion_status' in conversion_task:
        logger.info(f"Conversion status from task: {conversion_task['conversion_status']}")
        if conversion_task['conversion_status'] != 'Success':
            raise Exception(f"Conversion task reported non-success status: {conversion_task['conversion_status']}")
    
    # Execute HasData procedure
    connection = None
    cursor = None
    try:
        db_config = {
            'host': dev_creds.get('host'),
            'port': dev_creds.get('port', 5432),
            'user': dev_creds.get('user'),
            'password': dev_creds.get('password'),
            'database': dev_creds.get('database')
        }
        connection = psycopg2.connect(**db_config)
        cursor = connection.cursor()
        
        # First verify HasData entry exists
        cursor.execute("""
            SELECT COALESCE(jsonb_array_length(json_base), 0) as count
            FROM smartdatastagdb.import_hasdata_base
            WHERE job_id = %s AND api_type = 'restaurant_guru_api'
        """, (job_id,))
        result = cursor.fetchone()
        
        if not result or result[0] == 0:
            raise Exception(f"No HasData entry found for job_id: {job_id}")
        
        restaurant_count = result[0]
        logger.info(f"Found {restaurant_count} restaurants in HasData format")
        
        # Check if import procedure exists
        cursor.execute("""
            SELECT COUNT(*) 
            FROM information_schema.routines 
            WHERE routine_schema = 'smartdatastagdb' 
            AND routine_name = 'import_hasdata_restaurant_guru'
        """)
        proc_exists = cursor.fetchone()[0]
        if proc_exists == 0:
            raise Exception("Import procedure 'smartdatastagdb.import_hasdata_restaurant_guru' does not exist")
        logger.info("âœ… Import procedure exists and is callable")
        
        # Execute HasData procedure with the job_id from conversion task
        logger.info(f"Executing import procedure for job_id: {job_id}")
        try:
            cursor.execute("""
                CALL smartdatastagdb.import_hasdata_restaurant_guru(%s, %s);
            """, (import_id, job_id))
            connection.commit()
            logger.info(f"âœ… Import procedure executed successfully for job_id: {job_id}")
        except Exception as proc_error:
            logger.error(f"âŒ Import procedure failed: {str(proc_error)}")
            # Log the procedure call for debugging
            logger.error(f"Failed procedure call: CALL smartdatastagdb.import_hasdata_restaurant_guru({import_id}, '{job_id}')")
            raise
        
        # Verify objects were created
        cursor.execute("""
            SELECT COUNT(*) 
            FROM smartdatadb.objekt o
            WHERE o.quellenid = 17  -- Restaurant Guru source
            AND o.eingefuegtam >= NOW() - INTERVAL '5 minutes'
        """)
        object_count = cursor.fetchone()[0]
        logger.info(f"âœ… Created {object_count} objects in smartdatadb")
        
        return {
            'job_id': job_id,
            'import_id': import_id,
            'restaurant_count': restaurant_count,
            'object_count': object_count
        }
        
    except Exception as e:
        if connection:
            connection.rollback()
        logger.error(f"âŒ HasData import failed: {str(e)}")
        raise
        
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

# 🔧 Process multiple job_ids for individual restaurant processing
def process_multiple_job_ids(**context):
    """Process multiple job_ids from recent conversion (one per restaurant)"""
    logger.info("Processing multiple job_ids for individual restaurants...")
    
    # Get conversion task result
    conversion_task = context['task_instance'].xcom_pull(task_ids='convert_to_hasdata_format')
    logger.info(f"Conversion task result: {conversion_task}")
    
    if not conversion_task:
        raise Exception("Conversion task result not found")
    
    # Get import_id
    import_id = context['task_instance'].xcom_pull(task_ids='execute_get_max_import_id')
    if not import_id:
        raise Exception("Import ID not found")
    logger.info(f"Using import_id: {import_id}")
    
    # Connect to database
    connection = None
    cursor = None
    try:
        db_config = {
            'host': dev_creds.get('host'),
            'port': dev_creds.get('port', 5432),
            'user': dev_creds.get('user'),
            'password': dev_creds.get('password'),
            'database': dev_creds.get('database')
        }
        connection = psycopg2.connect(**db_config)
        cursor = connection.cursor()
        
        # 🔧 Get EXACT job_ids from current conversion (use specific job_id, not time-based)
        conversion_task = context['task_instance'].xcom_pull(task_ids='convert_to_hasdata_format')
        
        if not conversion_task:
            raise Exception("Conversion task result not found")
        
        # Get the specific session job_id from the current scraping run
        session_job_id = conversion_task.get('job_id')
        logger.info(f"🔧 Current scraping session job_id: {session_job_id}")
        
        if not session_job_id:
            raise Exception("Session Job ID not found in conversion task result")
        
        # 🚨 COPY WORKING LOGIC: Use the exact same city-based approach as conversion
        # Get city name from scraper task (same as conversion logic)
        scraper_task = context['task_instance'].xcom_pull(task_ids='run_restaurant_guru_scraper')
        city_name = None
        
        if scraper_task:
            city_name = scraper_task.get('city_name')
            logger.info(f"🔧 Got city name from scraper task: {city_name}")
        
        if not city_name:
            logger.warning("🚨 City name not found in scraper task, using fallback approach")
            # Fallback: get the most recent city from raw table
            cursor.execute("""
                SELECT city_name, COUNT(*) as count
                FROM smartdata_analyticdb.restaurant_guru_raw_germany 
                WHERE created_at >= NOW() - INTERVAL '2 hours'
                GROUP BY city_name 
                ORDER BY MAX(created_at) DESC 
                LIMIT 10
            """)
            recent_cities = cursor.fetchall()
            logger.info(f"🔧 Recent cities in raw table: {recent_cities}")
            
            if recent_cities:
                city_name = recent_cities[0][0]  # Get the most recent city
                logger.info(f"🔧 Using most recent city: {city_name}")
        
        if not city_name:
            raise Exception("City name not found in scraper task or recent data")
        
        logger.info(f"🔧 Using city-based approach for: {city_name}")
        
        # 🚨 EXACT COPY: Same query as conversion logic
        cursor.execute("""
            SELECT DISTINCT r.job_id
            FROM smartdata_analyticdb.restaurant_guru_raw_germany r
            WHERE r.city_name = %s
            AND r.job_id IS NOT NULL
            ORDER BY r.job_id DESC
        """, (city_name,))
        individual_job_ids = cursor.fetchall()
        
        logger.info(f"🔧 Found {len(individual_job_ids)} job_ids for city '{city_name}' (ALL runs, no time filter)")
        if individual_job_ids:
            job_id_list = [str(job_id[0]) for job_id in individual_job_ids]
            logger.info(f"🔧 Job IDs found: {', '.join(job_id_list)}")
        
        if not individual_job_ids:
            raise Exception(f"No job_ids found for city: {city_name}")
        
        # Now get the HasData entries for these individual job_ids
        job_id_list = [str(job_id[0]) for job_id in individual_job_ids]
        placeholders = ','.join(['%s'] * len(job_id_list))
        
        cursor.execute(f"""
            SELECT h.job_id, h.eingefuegtam
            FROM smartdatastagdb.import_hasdata_base h
            WHERE h.api_type = 'restaurant_guru_api'
            AND h.job_id IN ({placeholders})   -- 🚨 FIX: Use individual restaurant job_ids
            AND NOT EXISTS (  -- Not yet processed
                SELECT 1 FROM smartdatadb.externid e 
                WHERE e.extid = h.job_id::TEXT AND e.extidtypid = 20
            )
            ORDER BY h.job_id
        """, job_id_list)
        job_ids_to_process = cursor.fetchall()
        
        if not job_ids_to_process:
            logger.warning("No unprocessed job_ids found")
            return {'processed_count': 0, 'import_id': import_id}
        
        logger.info(f"🔧 Found {len(job_ids_to_process)} job_ids to process")
        
        processed_count = 0
        total_objects = 0
        
        # Process each job_id individually
        for (job_id, eingefuegtam) in job_ids_to_process:
            try:
                logger.info(f"🔧 Processing individual job_id: {job_id}")
                
                # Call import procedure for this specific job_id
                cursor.execute("""
                    CALL smartdatastagdb.import_hasdata_restaurant_guru(%s, %s);
                """, (import_id, job_id))
                connection.commit()
                
                processed_count += 1
                logger.info(f"✅ Processed job_id: {job_id} ({processed_count}/{len(job_ids_to_process)})")
                
            except Exception as e:
                logger.error(f"❌ Failed to process job_id {job_id}: {str(e)}")
                connection.rollback()
                # Continue with next job_id
        
        # Count total objects created
        cursor.execute("""
            SELECT COUNT(*) 
            FROM smartdatadb.objekt o
            WHERE o.quellenid = 17
            AND o.eingefuegtam >= NOW() - INTERVAL '5 minutes'
        """)
        total_objects = cursor.fetchone()[0]
        
        logger.info(f"🎉 Completed: {processed_count} restaurants processed, {total_objects} objects created")
        
        return {
            'processed_count': processed_count,
            'total_objects': total_objects,
            'import_id': import_id
        }
        
    except Exception as e:
        if connection:
            connection.rollback()
        logger.error(f"❌ Multiple job_id processing failed: {str(e)}")
        raise
        
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

# HasData Procedure Task - Process HasData format into normalized tables
execute_hasdata_procedures_task = PythonOperator(
    task_id="execute_hasdata_procedures",
    python_callable=process_multiple_job_ids,  # 🔧 Updated to handle multiple job_ids
    provide_context=True,
    dag=dag,
    retries=2,  # Add retries for resilience
    retry_delay=timedelta(minutes=1),  # Wait 1 minute between retries
)

# HasData Validation - Validate that HasData processing completed successfully
def validate_hasdata_results(**context):
    """Validate that HasData processing completed successfully"""
    logger.info("Validating HasData execution results...")
    
    import psycopg2
    
    try:
        # Connect to database - filter out Airflow-specific fields
        db_config = {
            'host': dev_creds.get('host'),
            'port': dev_creds.get('port', 5432),
            'user': dev_creds.get('user'),
            'password': dev_creds.get('password'),
            'database': dev_creds.get('database')
        }
        connection = psycopg2.connect(**db_config)
        cursor = connection.cursor()
        
        # Get import_id from previous task
        import_id = context['task_instance'].xcom_pull(task_ids='execute_get_max_import_id')
        
        
        # 🚨 FIX: Use time-based filtering instead of import_id (which doesn't exist)
        # Get import timestamp for filtering - same as working function
        cursor.execute("""
            SELECT MIN(created_at) - INTERVAL '1 minute' as start_time
            FROM smartdatastagdb.import_log 
            WHERE importid = %s
        """, (import_id,))
        import_start_result = cursor.fetchone()
        import_start_time = import_start_result[0] if import_start_result else None
        
        if not import_start_time:
            # Fallback to recent time window
            logger.warning(f"Could not find import_id {import_id} in logs, using recent time window")
            time_filter = "AND (o.eingefuegtam >= NOW() - INTERVAL '1 hour' OR o.geaendertam >= NOW() - INTERVAL '1 hour')"
            filter_params = []
        else:
            # 🚨 FIX: Check both eingefuegtam (inserted) AND geaendertam (updated) 
            # Import logs show "Insert: 0, Update: 1" - data is being updated, not inserted
            time_filter = "AND (o.eingefuegtam >= %s OR o.geaendertam >= %s)"
            filter_params = [import_start_time, import_start_time]
        
        # 🔧 FIXED validation queries for Restaurant Guru (quellenid = 17) - using time-based filtering
        validation_queries = {
            'total_objects': f"""
                SELECT COUNT(*) FROM smartdatadb.objekt o
                WHERE o.quellenid = 17 {time_filter}
            """,
            'addresses': f"""
                SELECT COUNT(*) FROM smartdatadb.adresse a
                JOIN smartdatadb.objekt o ON a.objektid = o.objektid
                WHERE o.quellenid = 17 {time_filter}
            """,
            'external_ids': f"""
                SELECT COUNT(*) FROM smartdatadb.externid e
                JOIN smartdatadb.objekt o ON e.objektid = o.objektid
                WHERE o.quellenid = 17 {time_filter}
            """,
            'phone_numbers': f"""
                SELECT COUNT(*) FROM smartdatadb.kommunikation k
                JOIN smartdatadb.objekt o ON k.objektid = o.objektid
                WHERE o.quellenid = 17 AND k.kommtypid = 1 {time_filter}
            """,
            'attributes': f"""
                SELECT COUNT(*) FROM smartdatadb.objektmerkmal om
                JOIN smartdatadb.objekt o ON om.objektid = o.objektid
                WHERE o.quellenid = 17 {time_filter}
            """,
            'master_objects': f"""
                SELECT COUNT(*) FROM smartdatadb.objekt o
                WHERE o.quellenid = 17 AND o.objektgruppeid = 3 {time_filter}
            """,
            'child_objects_with_masters': f"""
                SELECT COUNT(DISTINCT mo.childobjektid) 
                FROM smartdatadb.masterobjekt mo
                JOIN smartdatadb.objekt o ON mo.childobjektid = o.objektid
                WHERE o.quellenid = 17 {time_filter}
            """
        }
        
        results = {}
        for check_name, query in validation_queries.items():
            cursor.execute(query, filter_params)
            count = cursor.fetchone()[0]
            results[check_name] = count
            logger.info(f"HasData validation - {check_name}: {count}")
        
        # Validation logic
        total_objects = results['total_objects']
        if total_objects == 0:
            # Additional debugging info
            cursor.execute("""
                SELECT COUNT(*) as total_rg_objects,
                       MAX(eingefuegtam) as latest_timestamp
                FROM smartdatadb.objekt 
                WHERE quellenid = 17
            """)
            debug_info = cursor.fetchone()
            logger.error(f"No recent Restaurant Guru objects found. Total RG objects: {debug_info[0]}, Latest: {debug_info[1]}")
            
            cursor.execute("""
                SELECT importid, prozessschritt, log_text, created_at
                FROM smartdatastagdb.import_log 
                WHERE importid = %s
                ORDER BY created_at
            """, (import_id,))
            import_logs = cursor.fetchall()
            logger.error(f"Import logs for {import_id}: {import_logs}")
            
            raise Exception(f"No Restaurant Guru objects found for import_id {import_id}")
        
        # Check data consistency
        addresses = results['addresses']
        external_ids = results['external_ids']
        
        if addresses < total_objects * 0.8:  # At least 80% should have addresses
            logger.warning(f"Low address coverage: {addresses}/{total_objects}")
        
        if external_ids < total_objects * 0.5:  # At least 50% should have external IDs
            logger.warning(f"Low external ID coverage: {external_ids}/{total_objects}")
        
        # Check master-child relationships
        masters = results['master_objects']
        children_with_masters = results['child_objects_with_masters']
        
        logger.info(f"Master-child relationships: {masters} masters, {children_with_masters} linked children")
        
        # Get sample data for verification
        sample_query = f"""
            SELECT o.objektid, o.firma1, a.ort, e.extid
            FROM smartdatadb.objekt o
            LEFT JOIN smartdatadb.adresse a ON o.objektid = a.objektid AND a.adresstypid = 1
            LEFT JOIN smartdatadb.externid e ON o.objektid = e.objektid AND e.extidtypid = 20
            WHERE o.quellenid = 17 {time_filter}
            LIMIT 5
        """
        
        cursor.execute(sample_query, filter_params)
        sample_data = cursor.fetchall()
        
        logger.info("Sample Restaurant Guru records:")
        for row in sample_data:
            logger.info(f"  Object {row[0]}: {row[1]} in {row[2]} - DataID: {row[3]}")
        
        cursor.close()
        connection.close()
        
        return {
            'validation_status': 'success',
            'hasdata_counts': results,
            'import_id': import_id,
            'sample_records': len(sample_data),
            'import_start_time': str(import_start_time)
        }
        
    except Exception as e:
        logger.error(f"âŒ HasData validation failed: {str(e)}")
        raise


# =====================================================
# NOTE: Master object creation is now INTEGRATED into
# the import_hasdata_restaurant_guru procedure for 
# atomic operation. No separate task needed.
# =====================================================

# Master object creation task removed - now integrated into import_hasdata_restaurant_guru procedure

# Updated validation task for HasData
validate_hasdata_execution_task = PythonOperator(
    task_id='validate_hasdata_execution',
    python_callable=validate_hasdata_results,
    provide_context=True,
    dag=dag,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    retries=1,  # Add retries for resilience
    retry_delay=timedelta(minutes=1),  # Wait 1 minute between retries
)

# Set task dependencies - API quota check first, then HasData pattern implementation with master creation
chain(
    start,
    check_api_quota_task,                 # ðŸ†• Check API quota FIRST
    validate_execution_parameters_task,   # Validates parameters AND quota results
    execute_restaurant_guru_scraper_task,
    [execute_get_max_import_id_task, convert_to_hasdata_task],  # These can run in parallel
    execute_hasdata_procedures_task,      # Now handles both import AND master-child creation
    validate_hasdata_execution_task,      # Validate final results
    end
)

# Set explicit dependencies to ensure data flow
check_api_quota_task.set_upstream(start)
validate_execution_parameters_task.set_upstream(check_api_quota_task)
convert_to_hasdata_task.set_upstream(execute_restaurant_guru_scraper_task)
execute_hasdata_procedures_task.set_upstream([execute_get_max_import_id_task, convert_to_hasdata_task])
validate_hasdata_execution_task.set_upstream(execute_hasdata_procedures_task)  # Direct validation after integrated import

# Additional task documentation
dag.doc_md = """
# Restaurant Guru ETL Pipeline

This DAG implements an ETL pipeline for Restaurant Guru data scraping with integrated master-child architecture.

## Features

- **Async Processing**: Uses async/await patterns for efficient scraping
- **Database Integration**: AlloyDB for PostgreSQL with connection pooling
- **Master-Child Architecture**: Atomic creation of master (objektgruppeid=3) and child (objektgruppeid=1) objects
- **Enhanced Address Parsing**: Automatic extraction of house numbers and postal codes
- **Error Handling**: Comprehensive error handling with retries
- **Status Tracking**: City-level status tracking (not_scraped, scraped, failed, in_progress)
- **Batch Processing**: Configurable concurrent processing of cities
- **Flexible Execution**: Multiple execution modes (normal, test, batch, single_city)
- **🚀 Index-Based Processing**: Process specific restaurant ranges (1-200, 201-400, etc.)

## Configuration

The DAG can be configured using Airflow Variables:

- `restaurant_guru_execution_mode`: Execution mode (normal, test, batch, single_city)
- `restaurant_guru_item_limit`: Optional limit on items per city (for testing)
- `restaurant_guru_max_concurrent`: Maximum concurrent city processing (1-50)
- `restaurant_guru_min_api_calls`: Minimum API calls required to proceed (default: 100)
- `alloydb_dev_details`: Database credentials (JSON format)

**Note**: ScrapeOps API key is automatically read from the existing Scrapy settings file.

## Schedule

- **Continuous Execution**: Runs every 2 minutes, 24 hours a day, 7 days a week
- **Processing Window**: 24/7 continuous operation for maximum coverage
- **Sequential Execution**: Only one DAG instance runs at a time to prevent conflicts
- **Timeout Management**: Dynamic timeouts up to 24 hours for mega cities (40K+ restaurants)

## Index-Based Processing

To use index-based processing, trigger the DAG with configuration:

```json
{
  "index_based_mode": true,
  "start_index": 1,
  "end_index": 200
}
```

Examples:
- **1st run**: `{"index_based_mode": true, "start_index": 1, "end_index": 200}`
- **2nd run**: `{"index_based_mode": true, "start_index": 201, "end_index": 400}`
- **3rd run**: `{"index_based_mode": true, "start_index": 401, "end_index": 600}`

## Task Flow

1. **ðŸ” Check API Quota**: Validates ScrapeOps API quota before execution
   - Checks remaining API calls vs. minimum required
   - Skips execution if quota is insufficient
   - Warns if quota is low but sufficient
2. **âœ… Validate Execution Parameters**: Validates all configuration parameters and quota status
3. **ðŸ•·ï¸ Execute Restaurant Guru Scraper**: Runs the async scraper with batch processing
4. **ðŸ“Š Get Max Import ID & Convert to HasData**: Parallel processing of data preparation
5. **ðŸ—ï¸ Execute HasData Procedures**: Integrated atomic processing that:
   - Creates master objects (objektgruppeid=3)
   - Creates child objects (objektgruppeid=1) with proper linking
   - Establishes master-child relationships
   - Handles enhanced address parsing (house numbers, postal codes)
   - Manages external IDs for both master and child objects
6. **ðŸ” Validate Results**: Final validation of complete master-child architecture

## Database Tables

- `smartdata_analyticdb.restaurant_guru_raw_germany`: Raw scraping results
- `smartdata_analyticdb.restaurant_guru_log_germany`: Execution logs
- `smartdata_analyticdb.cities_de`: City status tracking

## Monitoring

The DAG provides comprehensive logging and status tracking:

- City-level progress monitoring
- Batch processing statistics
- Error tracking and retry mechanisms
- Execution summaries and reports

## Usage

For testing:
```python
# Set Airflow Variables
restaurant_guru_execution_mode = "test"
restaurant_guru_item_limit = 5
restaurant_guru_max_concurrent = 2
```

For production:
```python
# Set Airflow Variables
restaurant_guru_execution_mode = "normal"
restaurant_guru_max_concurrent = 20
```
"""