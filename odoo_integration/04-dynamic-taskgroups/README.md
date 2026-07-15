# Pattern 04: Dynamic TaskGroups

> Runtime task generation based on data volume for scalable parallel processing

---

## Pattern Overview

This architectural pattern demonstrates how to create Airflow tasks dynamically at DAG parse time based on the actual data volume to be processed. It's the foundation for all Odoo patterns and enables automatic scaling from 100 to 100K+ records without code changes.

**Key Benefits**:
- Automatic scaling with data volume
- Optimal parallelization (5-10 tasks simultaneously)
- Isolated failure domains (one chunk fails, others continue)
- Easy debugging (one task = one chunk of records)

---

## The Pattern

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from google.cloud import bigquery

def get_total_records():
    """Query to determine data volume."""
    query = """
    SELECT COUNT(*) as total
    FROM `dwh_trusted.int_data_to_process`
    WHERE DATE(updated_at) = CURRENT_DATE()
    """
    client = bigquery.Client()
    result = client.query(query).result()
    return list(result)[0]['total']

def build_query(start: int, stop: int) -> str:
    """Build query for a specific chunk."""
    return f"""
    SELECT *, ROW_NUMBER() OVER(ORDER BY id) as rnk
    FROM `dwh_trusted.int_data_to_process`
    WHERE DATE(updated_at) = CURRENT_DATE()
    QUALIFY rnk BETWEEN {start} AND {stop}
    """

def process_chunk(query: str, **context):
    """Process one chunk of records."""
    client = bigquery.Client()
    results = client.query(query).result()
    
    for row in results:
        # Process individual record
        process_record(row)

with DAG(
    'dynamic_taskgroup_example',
    schedule_interval='0 4 * * *',
    concurrency=5,  # Max 5 tasks running simultaneously
    catchup=False
) as dag:
    
    # Get total records at DAG parse time
    total_records = get_total_records()
    
    print(f"Total records to process: {total_records}")
    
    # Dynamic TaskGroup
    with TaskGroup(group_id='process_records') as task_group:
        chunk_size = 500
        
        # Create tasks dynamically
        for start in range(1, total_records, chunk_size):
            end = start + chunk_size - 1
            
            task = PythonOperator(
                task_id=f'process_rank_{start}',
                python_callable=process_chunk,
                op_kwargs={
                    'query': build_query(start, end)
                },
                execution_timeout=timedelta(hours=2)
            )
    
    # Total tasks created: ceil(total_records / chunk_size)
    # e.g., 10,000 records → 20 tasks (500 each)
```

---

## Airflow UI Visualization

```
DAG: dynamic_taskgroup_example
│
├─ process_records (TaskGroup)
│  ├─ process_rank_1 (records 1-500) [success]
│  ├─ process_rank_501 (records 501-1000) [success]
│  ├─ process_rank_1001 (records 1001-1500) [running]
│  ├─ process_rank_1501 (records 1501-2000) [running]
│  ├─ process_rank_2001 (records 2001-2500) [running]
│  ├─ process_rank_2501 (records 2501-3000) [running]
│  ├─ process_rank_3001 (records 3001-3500) [running]
│  ├─ process_rank_3501 (records 3501-4000) [queued]
│  ├─ process_rank_4001 (records 4001-4500) [queued]
│  └─ ... (more tasks)
```

---

## Production Example: Accounts Load

```python
# Real example from etl_odoo_accounts_ingestion.py

def get_total_invoices():
    """Get count of invoices to process today."""
    query = """
    SELECT COUNT(*) as total
    FROM `dwh_trusted.int_odoo_wsl_account_move_consolidated`
    WHERE DATE(write_date) BETWEEN CURRENT_DATE() - 1 AND CURRENT_DATE()
    """
    client = bigquery.Client(project='dwh_project')
    result = client.query(query).result()
    return list(result)[0]['total']

def echo_total_invoices(**context):
    """Log total for visibility."""
    total = context['ti'].xcom_pull(task_ids='get_total_invoices')
    print(f"Total invoices to process: {total}")
    return total

with DAG('etl_odoo_accounts_ingestion', ...) as dag:
    
    # Step 1: Get total (stored in XCom)
    get_total = PythonOperator(
        task_id='get_total_invoices',
        python_callable=get_total_invoices
    )
    
    # Step 2: Echo for visibility
    echo_total = PythonOperator(
        task_id='echo_total_invoices',
        python_callable=echo_total_invoices
    )
    
    # Step 3: Dynamic TaskGroup
    with TaskGroup(group_id='load_invoices') as load_group:
        accounts = Odoo(credentials=Variable.get("odoo_prod_creds"))
        
        # Get total for task generation
        total_rows = get_total_invoices()
        
        # Create task for each 500-record chunk
        for rank in range(1, total_rows, 500):
            rank_start = rank
            rank_end = rank_start + 499
            
            PythonOperator(
                task_id=f'load_rank_{rank_start}',
                python_callable=accounts.move_load_data,
                execution_timeout=timedelta(hours=3),
                op_kwargs={
                    'odoo_creds': "{{ var.json.odoo_prod_creds }}",
                    'query': account_query(start=rank_start, stop=rank_end)
                }
            )
    
    get_total >> echo_total >> load_group
```

---

## Chunk Size Selection

Tested chunk sizes over 3 years:

| Chunk Size | Tasks (10K records) | Pros | Cons | Verdict |
|------------|---------------------|------|------|----------|
| 100 | 100 | Fine-grained control | Too much overhead | Too small |
| 500 | 20 | Good balance | - | **Optimal** |
| 1000 | 10 | Fewer tasks | Higher memory | Good |
| 5000 | 2 | Minimal overhead | Timeout risk, low parallelization | Too large |

**Recommendation**: **500 records per task**

Why?
- Fast enough to complete in <10 minutes
- Small enough memory footprint
- Good parallelization (20 tasks → 5 running at once)
- Easy to re-run failed chunks

---

## Advanced: Nested TaskGroups

For complex workflows, nest TaskGroups:

```python
with DAG('complex_odoo_load', ...) as dag:
    
    with TaskGroup(group_id='accounts_pipeline') as accounts:
        
        # Sub-group: Load invoices
        with TaskGroup(group_id='load_invoices') as load_invoices:
            for rank in range(1, total_invoices, 500):
                create_task(f'load_invoice_rank_{rank}')
        
        # Sub-group: Load payments
        with TaskGroup(group_id='load_payments') as load_payments:
            for rank in range(1, total_payments, 500):
                create_task(f'load_payment_rank_{rank}')
        
        # Sub-group: Reconcile
        with TaskGroup(group_id='reconcile') as reconcile:
            for rank in range(1, total_moves, 500):
                create_task(f'reconcile_rank_{rank}')
        
        # Dependencies within group
        load_invoices >> load_payments >> reconcile
```

**Airflow UI**:
```
accounts_pipeline (collapsed)
│
├─ load_invoices (collapsed)
│  ├─ load_invoice_rank_1
│  ├─ load_invoice_rank_501
│  └─ ...
│
├─ load_payments (collapsed)
│  ├─ load_payment_rank_1
│  ├─ load_payment_rank_501
│  └─ ...
│
└─ reconcile (collapsed)
   ├─ reconcile_rank_1
   ├─ reconcile_rank_501
   └─ ...
```

---

## Progress Tracking

Use XCom to track progress across tasks:

```python
def process_chunk_with_tracking(query: str, **context):
    """Process chunk and report progress."""
    client = bigquery.Client()
    results = client.query(query).result()
    
    processed = 0
    errors = []
    
    for row in results:
        try:
            process_record(row)
            processed += 1
        except Exception as e:
            errors.append({'id': row['id'], 'error': str(e)})
    
    # Push to XCom
    context['ti'].xcom_push(key='processed_count', value=processed)
    context['ti'].xcom_push(key='error_count', value=len(errors))
    
    return {'processed': processed, 'errors': len(errors)}

def summarize_results(**context):
    """Aggregate results from all chunks."""
    ti = context['ti']
    
    # Get all task instances in TaskGroup
    task_group_tasks = [
        task_id for task_id in context['dag'].task_ids 
        if 'process_rank' in task_id
    ]
    
    total_processed = 0
    total_errors = 0
    
    for task_id in task_group_tasks:
        processed = ti.xcom_pull(task_ids=task_id, key='processed_count') or 0
        errors = ti.xcom_pull(task_ids=task_id, key='error_count') or 0
        total_processed += processed
        total_errors += errors
    
    print(f"Summary: {total_processed} processed, {total_errors} errors")
    
    # Alert if error rate >5%
    if total_errors / total_processed > 0.05:
        send_alert(f"High error rate: {total_errors}/{total_processed}")
```

---

## Error Handling Strategy

```python
with TaskGroup(group_id='load_data') as task_group:
    for rank in range(1, total, 500):
        task = PythonOperator(
            task_id=f'load_rank_{rank}',
            python_callable=process_chunk,
            trigger_rule='all_done',  # Continue even if siblings fail
            retries=3,  # Retry individual chunks
            retry_delay=timedelta(minutes=5)
        )

# Add failure callback
def on_failure_callback(context):
    """Alert on task failure."""
    task_id = context['task_instance'].task_id
    exception = context.get('exception')
    
    send_slack_alert(
        f"Task {task_id} failed: {exception}"
    )

# Apply to all tasks in DAG
default_args = {
    'on_failure_callback': on_failure_callback
}
```

---

## Production Statistics

From 116 Odoo DAGs using this pattern:

- **Average chunks per DAG**: 15-20 tasks
- **Max chunks**: 200 tasks (100K records)
- **Min chunks**: 1 task (<500 records)
- **Avg execution time per chunk**: 5-8 minutes
- **Parallelization**: 5 tasks running simultaneously (Cloud Composer default)
- **Success rate**: 99.7% (with 3 retries)

---

## Best Practices

**DO**:
- Calculate total at DAG parse time
- Use ROW_NUMBER() for deterministic chunking
- Set execution_timeout per task
- Use XCom for progress tracking
- Log chunk boundaries clearly

**DON'T**:
- Query total inside TaskGroup loop (performance)
- Use LIMIT/OFFSET (non-deterministic with concurrent updates)
- Make chunks too large (>2000 records)
- Make chunks too small (<100 records)
- Forget trigger_rule='all_done' for independent chunks

---

## Related Patterns

- [Accounts Load](../01-accounts-invoice-load/) - Real usage example
- [Leads Ingestion](../02-leads-ingestion/) - Simpler variant

---

<p align="center">
  <i>Used in 116 DAGs | Scales from 100 to 100K+ records automatically</i>
</p>
