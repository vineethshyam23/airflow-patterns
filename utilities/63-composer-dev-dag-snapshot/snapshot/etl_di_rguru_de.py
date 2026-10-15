#!/usr/bin/env python3
"""
Advanced Airflow DAG for RestaurantGuru Spider
Features:
- One city per DAG run
- Proper error handling and retries
- City status management
- Monitoring and notifications
- Configuration management
- Failure recovery
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.sensors.python import PythonSensor
from airflow.models import Variable
from airflow.exceptions import AirflowSkipException, AirflowFailException
import psycopg2
import logging
import subprocess
import os
import json
import time
import requests
import re

# Database credentials are configured directly in get_config()

# Configuration with default values (can be overridden via Airflow Variables if needed)
def get_config():
    """Get configuration from Airflow Variables and secure credentials"""
    # Use provided database credentials directly
    return {
        'db_host': '10.32.48.200',
        'db_user': 'postgres',
        'db_password': 'xqhCcs&"c#Y*}S,_',
        'db_database': 'postgres',
        'db_port': 5432,
        'scrapy_project_path': Variable.get("RGURU_SCRAPY_PATH", default_var="/home/airflow/gcs/dags/modules/Rguru_de/RestaurantGuru"),
        'max_processing_time': int(Variable.get("RGURU_MAX_PROCESSING_TIME", default_var="14400")),  # 4 hours for large cities
        'notification_webhook': Variable.get("RGURU_NOTIFICATION_WEBHOOK", default_var=""),
        'environment': Variable.get("RGURU_ENVIRONMENT", default_var="PROD"),  # DEV or PROD
        'min_api_calls_required': int(Variable.get("RGURU_MIN_API_CALLS", default_var="100")),  # Minimum API calls needed
    }

def get_db_connection():
    """Get database connection with retry logic"""
    config = get_config()
    max_retries = 3
    
    for attempt in range(max_retries):
        try:
            conn = psycopg2.connect(
                host=config['db_host'],
                user=config['db_user'],
                password=config['db_password'],
                database=config['db_database'],
                port=config['db_port'],
                connect_timeout=10
            )
            return conn
        except Exception as e:
            logging.warning(f"Database connection attempt {attempt + 1} failed: {e}")
            if attempt == max_retries - 1:
                raise
            time.sleep(5)


def check_scrapeops_api_quota(**context):
    """
    Check ScrapeOps API quota and determine if we have enough calls to proceed.
    """
    config = get_config()
    min_api_calls_required = config['min_api_calls_required']
    
    logging.info("🔍 Checking ScrapeOps API quota...")
    
    # Use the provided ScrapeOps API key directly
    scrapeops_api_key = 'REDACTED'
    
    logging.info(f"✅ Using ScrapeOps API key (ending: ...{scrapeops_api_key[-4:]})")
    
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
            logging.error(f"❌ Failed to get API quota. Status: {response.status_code}")
            logging.error(f"Response: {response.text}")
            raise Exception(f"ScrapeOps API quota check failed with status {response.status_code}")
        
        account_data = response.json()
        logging.info(f"✅ Successfully retrieved ScrapeOps account data")
        
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
            logging.info(f"Using new API response format - Plan: {total_requests}, Used: {requests_used}")
            # For new format, treat the whole response as account_info for compatibility
            account_info = account_data
        else:
            logging.error(f"❌ Unexpected API response format: {account_data}")
            raise Exception("Invalid response format from ScrapeOps API")
        
        # Calculate if we have enough requests
        min_required = int(min_api_calls_required)
        
        logging.info("📊 ScrapeOps API Quota Status:")
        logging.info(f"   📢 Total Requests: {total_requests:,}")
        logging.info(f"   ✅ Requests Used: {requests_used:,}")
        logging.info(f"   🆓 Requests Remaining: {requests_remaining:,}")
        logging.info(f"   ⚡ Minimum Required: {min_required:,}")
        
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
            logging.error("🚫 INSUFFICIENT API QUOTA!")
            logging.error(f"   ❌ Available: {requests_remaining:,} calls")
            logging.error(f"   ❌ Required: {min_required:,} calls")
            logging.error(f"   ❌ Shortage: {min_required - requests_remaining:,} calls")
            logging.error("🛑 DAG execution will be SKIPPED to prevent wasted resources")
            
            quota_status['proceed'] = False
            quota_status['skip_reason'] = f"Insufficient API quota: {requests_remaining} < {min_required}"
            quota_status['status'] = 'QUOTA_EXHAUSTED'
            
        elif requests_remaining < min_required * 2:
            logging.warning("⚠️ LOW API QUOTA WARNING!")
            logging.warning(f"   ⚠️ Available: {requests_remaining:,} calls")
            logging.warning(f"   ⚠️ This is close to the minimum threshold")
            logging.warning(f"   ⚠️ Consider monitoring usage closely")
            
            quota_status['proceed'] = True
            quota_status['skip_reason'] = None
            quota_status['status'] = 'LOW_QUOTA_WARNING'
            
        else:
            logging.info("✅ SUFFICIENT API QUOTA AVAILABLE")
            logging.info(f"   ✅ Available: {requests_remaining:,} calls")
            logging.info(f"   ✅ Required: {min_required:,} calls")
            logging.info(f"   ✅ Buffer: {requests_remaining - min_required:,} calls")
            logging.info("🚀 DAG execution can proceed safely")
            
            quota_status['proceed'] = True
            quota_status['skip_reason'] = None
            quota_status['status'] = 'SUFFICIENT_QUOTA'
        
        # Additional account information if available
        if 'plan' in account_info:
            logging.info(f"📋 Account Plan: {account_info['plan']}")
            quota_status['plan'] = account_info['plan']
        
        if 'renewal_date' in account_info:
            logging.info(f"📅 Quota Renewal: {account_info['renewal_date']}")
            quota_status['renewal_date'] = account_info['renewal_date']
        
        # Store quota status in XCom for other tasks
        context['task_instance'].xcom_push(key='quota_status', value=quota_status)
        
        return quota_status
        
    except requests.exceptions.Timeout:
        logging.error("❌ ScrapeOps API request timed out")
        raise Exception("ScrapeOps API quota check timed out")
        
    except requests.exceptions.RequestException as e:
        logging.error(f"❌ Network error checking ScrapeOps API: {str(e)}")
        raise Exception(f"Network error during quota check: {str(e)}")
        
    except json.JSONDecodeError as e:
        logging.error(f"❌ Invalid JSON response from ScrapeOps API: {str(e)}")
        raise Exception(f"Invalid JSON response during quota check: {str(e)}")
        
    except Exception as e:
        logging.error(f"❌ Unexpected error during quota check: {str(e)}")
        raise

def check_pending_cities_exist():
    """Check if there are any cities to process (pending or incomplete processing)"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT COUNT(*) 
            FROM smartdatastagdb.city_processing_status 
            WHERE processing_status IN ('pending', 'processing')
        """)
        
        count = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        
        logging.info(f"Found {count} cities to process (processing cities get priority, then pending)")
        return count > 0
        
    except Exception as e:
        logging.error(f"Error checking pending cities: {e}")
        raise

def decide_processing_branch(**context):
    """Decide whether to process a city or skip based on quota and pending cities"""
    # Check quota status from previous task
    quota_status = context['task_instance'].xcom_pull(task_ids='check_quota', key='quota_status')
    
    if not quota_status or not quota_status.get('proceed', False):
        logging.info("🚫 Skipping processing due to insufficient API quota")
        return 'no_cities_to_process'
    
    if check_pending_cities_exist():
        logging.info("✅ Quota sufficient and cities pending - proceeding with processing")
        return 'get_city_info'
    else:
        logging.info("ℹ️ No pending cities to process")
        return 'no_cities_to_process'

def get_and_reserve_city(**context):
    """Get up to 10 cities and mark them as processing for parallel execution"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Use transaction to atomically get and reserve cities
        cursor.execute("BEGIN")
        
        # Get up to 10 cities to process (processing cities first to complete chunks, then pending cities)
        cursor.execute("""
            SELECT city_id, city_name, city_slug 
            FROM smartdatastagdb.city_processing_status 
            WHERE processing_status IN ('pending', 'processing')
            ORDER BY 
                CASE WHEN processing_status = 'processing' THEN 1 ELSE 2 END,  -- Processing cities first
                city_id 
            LIMIT 10
            FOR UPDATE SKIP LOCKED
        """)
        
        results = cursor.fetchall()
        
        if results:
            cities_list = []
            for city_id, city_name, city_slug in results:
                city_info = {
                    'city_id': city_id,
                    'city_name': city_name,
                    'city_slug': city_slug
                }
                cities_list.append(city_info)
                
                # Mark city as processing to prevent other DAG runs from picking it up
                cursor.execute("""
                    UPDATE smartdatastagdb.city_processing_status 
                    SET processing_status = 'processing',
                        last_execution_id = %s
                    WHERE city_id = %s
                """, (context['dag_run'].run_id, city_id))
            
            cursor.execute("COMMIT")
            
            logging.info(f"🏙️ Selected {len(cities_list)} cities for parallel processing:")
            for city in cities_list:
                logging.info(f"   - {city['city_name']} (ID: {city['city_id']})")
            
            # Store cities list in XCom for parallel tasks
            context['task_instance'].xcom_push(key='cities_list', value=cities_list)
            return cities_list
            
        else:
            cursor.execute("ROLLBACK")
            logging.info("No pending cities found")
            raise AirflowSkipException("No pending cities to process")
            
    except AirflowSkipException:
        raise
    except Exception as e:
        try:
            cursor.execute("ROLLBACK")
        except:
            pass
        logging.error(f"Error reserving cities: {e}")
        raise
    finally:
        try:
            cursor.close()
            conn.close()
        except:
            pass

def run_scrapy_spider(city_index, **context):
    """Run the scrapy spider with proper monitoring for a specific city from the batch"""
    config = get_config()
    
    # Get the list of cities from XCom
    cities_list = context['task_instance'].xcom_pull(task_ids='get_city_info', key='cities_list')
    
    if not cities_list or city_index >= len(cities_list):
        logging.warning(f"No city found at index {city_index}, skipping")
        raise AirflowSkipException(f"No city at index {city_index}")
    
    city_info = cities_list[city_index]
    logging.info(f"🏙️ Processing city {city_index + 1}/{len(cities_list)}: {city_info['city_name']}")
    
    try:
        # Log current working directory and target directory
        current_dir = os.getcwd()
        target_dir = config['scrapy_project_path']
        logging.info(f"Current directory: {current_dir}")
        logging.info(f"Target scrapy directory: {target_dir}")
        
        # Check if target directory exists
        if not os.path.exists(target_dir):
            raise ValueError(f"Scrapy project directory does not exist: {target_dir}")
        
        # List contents of target directory for debugging
        try:
            contents = os.listdir(target_dir)
            logging.info(f"Contents of {target_dir}: {contents}")
        except Exception as e:
            logging.error(f"Error listing directory contents: {e}")
        
        # Change to scrapy project directory
        os.chdir(target_dir)
        logging.info(f"Changed to directory: {os.getcwd()}")
        
        # Check if scrapy.cfg exists
        if not os.path.exists('scrapy.cfg'):
            raise ValueError("scrapy.cfg not found in project directory")
        
        # Check if spider exists
        spider_path = os.path.join('spiders', 'rguru_de.py')
        if not os.path.exists(spider_path):
            raise ValueError(f"Spider file not found: {spider_path}")
        
        logging.info(f"Starting spider for city: {city_info['city_name']}")
        
        # Try to find scrapy executable
        import shutil
        scrapy_path = shutil.which('scrapy')
        logging.info(f"Scrapy executable found at: {scrapy_path}")
        
        # Add the parent directory to Python path so RestaurantGuru module can be imported
        parent_dir = os.path.dirname(target_dir)
        logging.info(f"Adding to PYTHONPATH: {parent_dir}")
        
        # Set up environment with PYTHONPATH
        env = os.environ.copy()
        if 'PYTHONPATH' in env:
            env['PYTHONPATH'] = f"{parent_dir}:{env['PYTHONPATH']}"
        else:
            env['PYTHONPATH'] = parent_dir
        
        # Run scrapy command with city information passed as arguments
        cmd = [
            'scrapy', 'crawl', 'rguru_de', 
            '--loglevel=INFO',
            '-a', f'city_id={city_info["city_id"]}',
            '-a', f'city_name={city_info["city_name"]}',
            '-a', f'city_slug={city_info["city_slug"]}'
        ]
        logging.info(f"Running command: {' '.join(cmd)}")
        logging.info(f"PYTHONPATH: {env.get('PYTHONPATH', 'Not set')}")
        logging.info(f"Passing city info to spider: {city_info}")
        
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=config['max_processing_time'],
            cwd=target_dir,  # Explicitly set working directory
            env=env  # Pass environment with PYTHONPATH
        )
        
        # Log both stdout and stderr for debugging
        logging.info(f"Command return code: {result.returncode}")
        
        # Log COMPLETE stdout in chunks to avoid truncation
        if result.stdout:
            stdout_lines = result.stdout.split('\n')
            logging.info(f"📋 COMPLETE SPIDER OUTPUT ({len(stdout_lines)} lines):")
            logging.info("=" * 80)
            
            # Log in chunks of 50 lines to avoid single log message limits
            for i in range(0, len(stdout_lines), 50):
                chunk = stdout_lines[i:i+50]
                chunk_text = '\n'.join(chunk)
                logging.info(f"STDOUT Lines {i+1}-{min(i+50, len(stdout_lines))}:\n{chunk_text}")
            
            logging.info("=" * 80)
            logging.info("📋 END OF SPIDER OUTPUT")
        
        # Log complete stderr
        if result.stderr:
            stderr_lines = result.stderr.split('\n')
            logging.error(f"📋 COMPLETE STDERR OUTPUT ({len(stderr_lines)} lines):")
            for i in range(0, len(stderr_lines), 50):
                chunk = stderr_lines[i:i+50]
                chunk_text = '\n'.join(chunk)
                logging.error(f"STDERR Lines {i+1}-{min(i+50, len(stderr_lines))}:\n{chunk_text}")
        
        if result.returncode == 0:
            logging.info(f"Spider completed successfully for {city_info['city_name']}")
            
            # Extract some stats from output
            stdout_lines = result.stdout.split('\n')
            stats_info = {}
            for line in stdout_lines:
                if 'item_scraped_count' in line:
                    stats_info['items_scraped'] = line.split(':')[-1].strip()
                elif 'finish_reason' in line:
                    stats_info['finish_reason'] = line.split(':')[-1].strip()
            
            context['task_instance'].xcom_push(key='spider_stats', value=stats_info)
            return True
            
        else:
            error_msg = f"Spider failed with return code {result.returncode}"
            if result.stderr:
                error_msg += f": {result.stderr}"
            elif result.stdout:
                # Sometimes errors are in stdout
                error_msg += f": {result.stdout}"
            else:
                error_msg += ": No error message available"
                
            logging.error(f"Spider failed for {city_info['city_name']}: {error_msg}")
            
            # Mark city as failed
            mark_city_failed(city_info['city_id'], f"Spider failed: {error_msg[:500]}")
            raise ValueError(f"Spider execution failed: {error_msg}")
            
    except subprocess.TimeoutExpired:
        error_msg = f"Spider timed out after {config['max_processing_time']} seconds"
        logging.error(error_msg)
        mark_city_failed(city_info['city_id'], error_msg)
        raise ValueError(error_msg)
        
    except Exception as e:
        error_msg = f"Error running spider: {str(e)}"
        logging.error(error_msg)
        mark_city_failed(city_info['city_id'], error_msg)
        raise

def mark_city_failed(city_id, error_message):
    """Mark city as failed in database"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            UPDATE smartdatastagdb.city_processing_status 
            SET processing_status = 'failed',
                error_message = %s,
                retry_count = retry_count + 1
            WHERE city_id = %s
        """, (error_message, city_id))
        
        conn.commit()
        cursor.close()
        conn.close()
        
    except Exception as e:
        logging.error(f"Error marking city as failed: {e}")

def verify_city_completion(city_index, **context):
    """Verify that the city was processed successfully"""
    # Get the list of cities from XCom
    cities_list = context['task_instance'].xcom_pull(task_ids='get_city_info', key='cities_list')
    
    if not cities_list or city_index >= len(cities_list):
        logging.warning(f"No city found at index {city_index}, skipping verification")
        raise AirflowSkipException(f"No city at index {city_index}")
    
    city_info = cities_list[city_index]
    
    # Give the spider a moment to update the database
    time.sleep(5)
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT processing_status, restaurants_found, restaurants_processed, error_message
            FROM smartdatastagdb.city_processing_status 
            WHERE city_id = %s
        """, (city_info['city_id'],))
        
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        
        if result:
            status, found, processed, error_msg = result
            
            completion_info = {
                'status': status,
                'restaurants_found': found,
                'restaurants_processed': processed,
                'error_message': error_msg
            }
            
            context['task_instance'].xcom_push(key='completion_info', value=completion_info)
            
            logging.info(f"📊 City {city_info['city_name']} Status Check:")
            logging.info(f"   Status: {status}")
            logging.info(f"   Restaurants Found: {found}")
            logging.info(f"   Restaurants Processed: {processed}")
            logging.info(f"   Error Message: {error_msg}")
            
            if status == 'completed':
                logging.info(f"✅ City {city_info['city_name']} completed successfully. Found: {found}, Processed: {processed}")
                return True
            elif status == 'failed':
                logging.error(f"❌ City {city_info['city_name']} marked as FAILED by spider")
                logging.error(f"   Reason: {error_msg}")
                logging.error(f"   Found: {found}, Processed: {processed}")
                # Use AirflowFailException to mark as permanent failure (no retry)
                raise AirflowFailException(f"City processing failed: {error_msg}")
            elif status == 'processing':
                # Spider intentionally marked as "processing" - it will resume on next run
                logging.warning(f"⚠️ City {city_info['city_name']} marked as 'processing' by spider")
                logging.warning(f"   Reason: Partial scrape - {processed}/{found} restaurants completed")
                logging.warning(f"   This city will automatically resume on the next DAG run")
                
                # Check if we made some progress (processed > 0)
                if processed > 0:
                    logging.info(f"✅ Partial progress saved: {processed}/{found} restaurants")
                    logging.info(f"   Will resume from last checkpoint on next run")
                    return True  # Consider this a success - it will resume later
                else:
                    # No progress made at all - this is a real failure (permanent)
                    raise AirflowFailException(f"City processing made no progress: 0/{found} restaurants")
            else:
                logging.error(f"❌ City {city_info['city_name']} processing failed. Status: {status}, Error: {error_msg}")
                raise AirflowFailException(f"City processing incomplete: {status}")
        else:
            raise AirflowFailException("City not found in database")
            
    except Exception as e:
        logging.error(f"Error verifying city completion: {e}")
        raise


def handle_failure(**context):
    """Handle DAG failure - reset any cities that failed to process"""
    try:
        # Get the list of cities that were being processed
        cities_list = context['task_instance'].xcom_pull(task_ids='get_city_info', key='cities_list')
        
        if cities_list:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            for city_info in cities_list:
                # Check if city is still in processing state (failed to complete)
                cursor.execute("""
                    SELECT processing_status 
                    FROM smartdatastagdb.city_processing_status 
                    WHERE city_id = %s
                """, (city_info['city_id'],))
                
                result = cursor.fetchone()
                if result and result[0] == 'processing':
                    # Reset to pending for retry
                    cursor.execute("""
                        UPDATE smartdatastagdb.city_processing_status 
                        SET processing_status = 'pending'
                        WHERE city_id = %s
                    """, (city_info['city_id'],))
                    
                    logging.info(f"Reset city {city_info['city_name']} status to pending for retry")
            
            conn.commit()
            cursor.close()
            conn.close()
            
    except Exception as e:
        logging.error(f"Error in failure handler: {e}")

# DAG configuration
default_args = {
    'owner': 'Yousuf',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 30),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=600),
    'execution_timeout': timedelta(hours=10),
    'on_failure_callback': handle_failure,
}

# Create DAG
dag = DAG(
    'etl_di_rguru_de',
    default_args=default_args,
    description="ETL process for Restaurant Guru data using AlloyDB with async processing",
    schedule_interval="*/2 * * * *",  # Every 2 minutes, 24/7 continuous execution
    catchup=False,
    max_active_runs=1,
    tags=['scrapy', 'restaurantguru', 'production'],
)

# Tasks
start_task = DummyOperator(
    task_id='start',
    dag=dag,
)

check_quota_task = PythonOperator(
    task_id='check_quota',
    python_callable=check_scrapeops_api_quota,
    dag=dag,
)

branch_task = BranchPythonOperator(
    task_id='check_cities_branch',
    python_callable=decide_processing_branch,
    dag=dag,
)

no_cities_task = DummyOperator(
    task_id='no_cities_to_process',
    dag=dag,
)

get_city_task = PythonOperator(
    task_id='get_city_info',
    python_callable=get_and_reserve_city,  # Get up to 10 cities
    dag=dag,
)

# Create 10 parallel spider tasks
run_spider_tasks = []
verify_tasks = []

for i in range(10):
    run_spider_task = PythonOperator(
        task_id=f'run_scrapy_spider_{i}',
        python_callable=run_scrapy_spider,
        op_kwargs={'city_index': i},
        dag=dag,
    )
    run_spider_tasks.append(run_spider_task)
    
    verify_task = PythonOperator(
        task_id=f'verify_completion_{i}',
        python_callable=verify_city_completion,
        op_kwargs={'city_index': i},
        dag=dag,
    )
    verify_tasks.append(verify_task)
    
    # Set up dependencies for this parallel stream
    get_city_task >> run_spider_task >> verify_task


end_task = DummyOperator(
    task_id='end',
    dag=dag,
    trigger_rule='none_failed_min_one_success',
)

# Task dependencies
start_task >> check_quota_task >> branch_task
branch_task >> [no_cities_task, get_city_task]

# Parallel processing: All verify tasks must complete before end
for verify_task in verify_tasks:
    verify_task >> end_task

no_cities_task >> end_task