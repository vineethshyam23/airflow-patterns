"""
Example DAG demonstrating dynamic TaskGroup pattern.

Creates tasks dynamically based on data volume for scalable parallel processing.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup
from google.cloud import bigquery
from datetime import datetime, timedelta


default_args = {
    'owner': 'data-platform',
    'start_date': datetime(2024, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}


def get_total_records():
    """
    Query to determine data volume.
    
    Returns:
        int: Total number of records to process
    """
    query = """
    SELECT COUNT(*) as total
    FROM `dwh_trusted.int_data_to_process`
    WHERE DATE(updated_at) = CURRENT_DATE()
    """
    client = bigquery.Client()
    result = client.query(query).result()
    return list(result)[0]['total']


def build_query(start: int, stop: int) -> str:
    """
    Build query for a specific chunk.
    
    Args:
        start: Starting row number
        stop: Ending row number
    
    Returns:
        str: BigQuery SQL query
    """
    return f"""
    SELECT *, ROW_NUMBER() OVER(ORDER BY id) as rnk
    FROM `dwh_trusted.int_data_to_process`
    WHERE DATE(updated_at) = CURRENT_DATE()
    QUALIFY rnk BETWEEN {start} AND {stop}
    """


def process_chunk(query: str, **context):
    """
    Process one chunk of records.
    
    Args:
        query: BigQuery query for this chunk
        **context: Airflow context
    """
    client = bigquery.Client()
    results = client.query(query).result()
    
    processed = 0
    for row in results:
        # Process individual record
        # ... your processing logic here ...
        processed += 1
    
    # Push to XCom for tracking
    context['ti'].xcom_push(key='processed_count', value=processed)
    
    return processed


with DAG(
    'dynamic_taskgroup_example',
    default_args=default_args,
    description='Example of dynamic task generation based on data volume',
    schedule_interval='0 4 * * *',
    concurrency=5,  # Max 5 tasks running simultaneously
    catchup=False,
    tags=['example', 'dynamic-taskgroups']
) as dag:
    
    start = DummyOperator(task_id='start')
    
    # Get total records at DAG parse time
    total_records = get_total_records()
    
    print(f"📊 Total records to process: {total_records}")
    
    # Dynamic TaskGroup
    with TaskGroup(group_id='process_records') as task_group:
        chunk_size = 500
        
        # Create tasks dynamically
        # This loop runs at DAG parse time, creating actual Airflow tasks
        for start_rank in range(1, total_records, chunk_size):
            end_rank = start_rank + chunk_size - 1
            
            task = PythonOperator(
                task_id=f'process_rank_{start_rank}',
                python_callable=process_chunk,
                op_kwargs={
                    'query': build_query(start_rank, end_rank)
                },
                execution_timeout=timedelta(hours=2)
            )
    
    end = DummyOperator(task_id='end')
    
    # Task dependencies
    start >> task_group >> end


# Result: This creates a DAG with N tasks where N = ceil(total_records / 500)
# Example: 10,000 records → 20 tasks (each processing 500 records)
