"""
Odoo Incremental Sync Pattern
==============================

Daily synchronization of changed records from Odoo ERP to BigQuery.

Author: Vineeth Shyam
Pattern: Incremental Sync with State Management
Production Usage: 40+ similar DAGs
"""

from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import xmlrpc.client
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowException


# Configuration
ODOO_CONFIG = {
    'url': Variable.get('odoo_url', 'https://erp.company.com'),
    'db': Variable.get('odoo_db', 'production'),
    'username': Variable.get('odoo_username'),
    'password': Variable.get('odoo_password'),
}

BQ_CONFIG = {
    'project_id': 'dwh_project',
    'dataset_bronze': 'bronze',
    'dataset_state': 'state',
    'location': 'EU',
}

SYNC_CONFIG = {
    'odoo_model': 'res.partner',
    'target_table': 'odoo_partners_raw',
    'lookback_days': 7,  # Default if no watermark
    'batch_size': 1000,
    'fields': [
        'id', 'name', 'email', 'phone', 'street', 'city', 'country_id',
        'create_date', 'write_date', 'create_uid', 'write_uid'
    ],
}


class OdooClient:
    """Wrapper for Odoo XML-RPC API with authentication and error handling."""
    
    def __init__(self, url: str, db: str, username: str, password: str):
        self.url = url
        self.db = db
        self.username = username
        self.password = password
        self.uid = None
        self.common = None
        self.models = None
        
    def authenticate(self):
        """Authenticate with Odoo and cache UID."""
        try:
            self.common = xmlrpc.client.ServerProxy(f'{self.url}/xmlrpc/2/common')
            self.uid = self.common.authenticate(
                self.db, self.username, self.password, {}
            )
            if not self.uid:
                raise AirflowException("Odoo authentication failed")
            
            self.models = xmlrpc.client.ServerProxy(f'{self.url}/xmlrpc/2/object')
            print(f"✅ Authenticated to Odoo as UID: {self.uid}")
            return self.uid
        except Exception as e:
            raise AirflowException(f"Odoo authentication error: {str(e)}")
    
    def search_read(
        self, 
        model: str, 
        domain: List, 
        fields: List[str],
        offset: int = 0,
        limit: int = 1000
    ) -> List[Dict]:
        """Execute search_read with pagination."""
        if not self.uid:
            self.authenticate()
        
        try:
            records = self.models.execute_kw(
                self.db, self.uid, self.password,
                model, 'search_read',
                [domain],
                {'fields': fields, 'offset': offset, 'limit': limit}
            )
            return records
        except xmlrpc.client.Fault as e:
            # Handle session expiry
            if 'session' in str(e).lower():
                print("⚠️ Session expired, re-authenticating...")
                self.authenticate()
                return self.search_read(model, domain, fields, offset, limit)
            raise AirflowException(f"Odoo API error: {str(e)}")
        except Exception as e:
            raise AirflowException(f"Unexpected error: {str(e)}")


def get_last_sync_timestamp(**context) -> str:
    """
    Get last successful sync timestamp from watermark table.
    
    Returns:
        ISO format timestamp string
    """
    client = bigquery.Client(project=BQ_CONFIG['project_id'])
    
    query = f"""
    SELECT MAX(sync_timestamp) as last_sync
    FROM `{BQ_CONFIG['project_id']}.{BQ_CONFIG['dataset_state']}.sync_watermarks`
    WHERE source_system = 'odoo'
      AND table_name = '{SYNC_CONFIG['odoo_model']}'
      AND sync_status = 'SUCCESS'
    """
    
    try:
        df = client.query(query).to_dataframe()
        
        if df.empty or pd.isna(df['last_sync'].iloc[0]):
            # No watermark exists, use lookback_days
            lookback = datetime.utcnow() - timedelta(days=SYNC_CONFIG['lookback_days'])
            last_sync = lookback.strftime('%Y-%m-%d %H:%M:%S')
            print(f"📅 No watermark found, using {SYNC_CONFIG['lookback_days']} day lookback: {last_sync}")
        else:
            last_sync = df['last_sync'].iloc[0].strftime('%Y-%m-%d %H:%M:%S')
            print(f"📅 Last successful sync: {last_sync}")
        
        # Push to XCom
        context['ti'].xcom_push(key='last_sync_timestamp', value=last_sync)
        return last_sync
        
    except Exception as e:
        raise AirflowException(f"Failed to get watermark: {str(e)}")


def fetch_odoo_changes(**context) -> int:
    """
    Fetch changed records from Odoo since last sync.
    
    Returns:
        Number of records fetched
    """
    last_sync = context['ti'].xcom_pull(task_ids='get_last_sync', key='last_sync_timestamp')
    
    # Initialize Odoo client
    odoo = OdooClient(
        url=ODOO_CONFIG['url'],
        db=ODOO_CONFIG['db'],
        username=ODOO_CONFIG['username'],
        password=ODOO_CONFIG['password']
    )
    odoo.authenticate()
    
    # Build domain filter for changed records
    domain = [
        '|',
        ('create_date', '>=', last_sync),
        ('write_date', '>=', last_sync)
    ]
    
    print(f"🔍 Fetching {SYNC_CONFIG['odoo_model']} records changed since {last_sync}")
    
    # Paginate through results
    all_records = []
    offset = 0
    batch_size = SYNC_CONFIG['batch_size']
    
    while True:
        print(f"   Fetching batch: offset={offset}, limit={batch_size}")
        
        batch = odoo.search_read(
            model=SYNC_CONFIG['odoo_model'],
            domain=domain,
            fields=SYNC_CONFIG['fields'],
            offset=offset,
            limit=batch_size
        )
        
        if not batch:
            break
        
        all_records.extend(batch)
        offset += batch_size
        
        print(f"   Retrieved {len(batch)} records (total: {len(all_records)})")
        
        # Safety limit
        if len(all_records) > 1_000_000:
            raise AirflowException("Safety limit exceeded: >1M records. Consider batch migration pattern.")
    
    print(f"✅ Total records fetched: {len(all_records)}")
    
    # Save to temporary file for next task
    if all_records:
        df = pd.DataFrame(all_records)
        temp_file = f"/tmp/odoo_sync_{context['dag_run'].run_id}.parquet"
        df.to_parquet(temp_file, index=False)
        context['ti'].xcom_push(key='temp_file', value=temp_file)
    
    context['ti'].xcom_push(key='record_count', value=len(all_records))
    return len(all_records)


def transform_and_validate(**context) -> bool:
    """
    Transform and validate fetched records.
    
    Returns:
        True if validation passed
    """
    temp_file = context['ti'].xcom_pull(task_ids='fetch_odoo_changes', key='temp_file')
    record_count = context['ti'].xcom_pull(task_ids='fetch_odoo_changes', key='record_count')
    
    if record_count == 0:
        print("ℹ️ No records to process, skipping transformation")
        return True
    
    # Load data
    df = pd.read_parquet(temp_file)
    print(f"📊 Loaded {len(df)} records for transformation")
    
    # Transformations
    print("🔧 Applying transformations...")
    
    # 1. Handle Many2One fields (convert to ID only)
    if 'country_id' in df.columns and isinstance(df['country_id'].iloc[0], list):
        df['country_id'] = df['country_id'].apply(lambda x: x[0] if isinstance(x, list) and x else None)
    
    # 2. Normalize text fields
    if 'email' in df.columns:
        df['email'] = df['email'].str.lower().str.strip()
    if 'name' in df.columns:
        df['name'] = df['name'].str.strip()
    
    # 3. Convert Odoo datetime strings to datetime
    for col in ['create_date', 'write_date']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col])
    
    # 4. Add metadata columns
    df['sync_timestamp'] = datetime.utcnow()
    df['dag_run_id'] = context['dag_run'].run_id
    
    # Validation
    print("✅ Running validation checks...")
    errors = []
    
    # Check 1: Primary key uniqueness
    if df['id'].duplicated().any():
        errors.append(f"Duplicate IDs found: {df[df['id'].duplicated()]['id'].tolist()}")
    
    # Check 2: Required fields not null
    required_fields = ['id', 'create_date', 'write_date']
    for field in required_fields:
        if df[field].isna().any():
            null_count = df[field].isna().sum()
            errors.append(f"Null values in {field}: {null_count} records")
    
    # Check 3: Date logic validation
    if (df['write_date'] < df['create_date']).any():
        invalid_count = (df['write_date'] < df['create_date']).sum()
        errors.append(f"write_date < create_date: {invalid_count} records")
    
    if errors:
        error_msg = "\n".join(errors)
        raise AirflowException(f"❌ Validation failed:\n{error_msg}")
    
    print(f"✅ Validation passed: {len(df)} records ready for load")
    
    # Save transformed data
    df.to_parquet(temp_file, index=False)
    
    return True


def load_to_bigquery(**context) -> int:
    """
    Load transformed data to BigQuery using upsert pattern.
    
    Returns:
        Number of records loaded
    """
    temp_file = context['ti'].xcom_pull(task_ids='fetch_odoo_changes', key='temp_file')
    record_count = context['ti'].xcom_pull(task_ids='fetch_odoo_changes', key='record_count')
    
    if record_count == 0:
        print("ℹ️ No records to load")
        return 0
    
    df = pd.read_parquet(temp_file)
    client = bigquery.Client(project=BQ_CONFIG['project_id'])
    
    # Create staging table
    run_id = context['dag_run'].run_id.replace(':', '_').replace('+', '_')
    staging_table = f"{BQ_CONFIG['project_id']}.{BQ_CONFIG['dataset_bronze']}.{SYNC_CONFIG['target_table']}_staging_{run_id}"
    
    print(f"📤 Loading {len(df)} records to staging: {staging_table}")
    
    # Load to staging
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
    )
    
    job = client.load_table_from_dataframe(df, staging_table, job_config=job_config)
    job.result()  # Wait for completion
    
    print(f"✅ Loaded to staging table")
    
    # Merge into target table
    target_table = f"{BQ_CONFIG['project_id']}.{BQ_CONFIG['dataset_bronze']}.{SYNC_CONFIG['target_table']}"
    
    merge_query = f"""
    MERGE `{target_table}` T
    USING `{staging_table}` S
    ON T.id = S.id
    WHEN MATCHED AND S.write_date > T.write_date THEN
        UPDATE SET 
            name = S.name,
            email = S.email,
            phone = S.phone,
            street = S.street,
            city = S.city,
            country_id = S.country_id,
            write_date = S.write_date,
            write_uid = S.write_uid,
            sync_timestamp = S.sync_timestamp,
            dag_run_id = S.dag_run_id
    WHEN NOT MATCHED THEN
        INSERT (
            id, name, email, phone, street, city, country_id,
            create_date, write_date, create_uid, write_uid,
            sync_timestamp, dag_run_id
        )
        VALUES (
            S.id, S.name, S.email, S.phone, S.street, S.city, S.country_id,
            S.create_date, S.write_date, S.create_uid, S.write_uid,
            S.sync_timestamp, S.dag_run_id
        )
    """
    
    print(f"🔄 Executing MERGE into {target_table}")
    merge_job = client.query(merge_query)
    merge_job.result()
    
    # Get merge stats
    rows_inserted = merge_job.num_dml_affected_rows
    print(f"✅ Merge complete: {rows_inserted} rows affected")
    
    # Cleanup staging table
    client.delete_table(staging_table, not_found_ok=True)
    print(f"🧹 Cleaned up staging table")
    
    return rows_inserted


def update_watermark(**context) -> None:
    """Record successful sync in watermark table."""
    record_count = context['ti'].xcom_pull(task_ids='fetch_odoo_changes', key='record_count')
    
    client = bigquery.Client(project=BQ_CONFIG['project_id'])
    
    watermark_data = {
        'source_system': ['odoo'],
        'table_name': [SYNC_CONFIG['odoo_model']],
        'sync_timestamp': [datetime.utcnow()],
        'records_processed': [record_count],
        'sync_status': ['SUCCESS'],
        'dag_run_id': [context['dag_run'].run_id],
    }
    
    df = pd.DataFrame(watermark_data)
    
    watermark_table = f"{BQ_CONFIG['project_id']}.{BQ_CONFIG['dataset_state']}.sync_watermarks"
    
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
    )
    
    job = client.load_table_from_dataframe(df, watermark_table, job_config=job_config)
    job.result()
    
    print(f"✅ Watermark updated: {record_count} records at {df['sync_timestamp'].iloc[0]}")


# DAG Definition
default_args = {
    'owner': 'data-platform',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['dataops@company.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

with DAG(
    'odoo_incremental_sync_partners',
    default_args=default_args,
    description='Incremental sync of Odoo partners to BigQuery',
    schedule_interval='0 2 * * *',  # Daily at 2 AM UTC
    catchup=False,
    max_active_runs=1,
    tags=['odoo', 'incremental-sync', 'partners', 'production'],
) as dag:
    
    get_last_sync = PythonOperator(
        task_id='get_last_sync',
        python_callable=get_last_sync_timestamp,
        provide_context=True,
    )
    
    fetch_changes = PythonOperator(
        task_id='fetch_odoo_changes',
        python_callable=fetch_odoo_changes,
        provide_context=True,
    )
    
    transform = PythonOperator(
        task_id='transform_and_validate',
        python_callable=transform_and_validate,
        provide_context=True,
    )
    
    load = PythonOperator(
        task_id='load_to_bigquery',
        python_callable=load_to_bigquery,
        provide_context=True,
    )
    
    watermark = PythonOperator(
        task_id='update_watermark',
        python_callable=update_watermark,
        provide_context=True,
        trigger_rule='all_success',
    )
    
    # Task dependencies
    get_last_sync >> fetch_changes >> transform >> load >> watermark
