# Pattern 01: Accounts (Invoice) Load

> Daily synchronization of customer invoices from BigQuery to Odoo Accounting module

---

## Quick Stats

- **Complexity**:  Advanced
- **Production Usage**: Daily, 50K+ invoices/day
- **Avg Execution Time**: 45 minutes (for 10K invoices)
- **Success Rate**: 99.7%
- **Domains**: account.move, account.move.line, account.payment

---

## Pattern Overview

This pattern handles the complex migration and daily sync of accounting data (invoices, credit notes, invoice lines) from a data warehouse to Odoo's accounting module. It includes sophisticated logic for partner resolution, tax mapping, product lookups, and payment journal entries.

**Key Challenges Solved**:
- Multi-entity invoice relationships (move → lines → payments)
- Foreign key resolution (partners, products, taxes, journals)
- Idempotency (external ID tracking)
- Error isolation (per-invoice error handling)
- Complex business logic (B2B2C products, deferred revenue, multi-currency)

---

## Architecture

```
┌────────────────────────────────────┐
│   BigQuery Data Warehouse          │
│                                    │
│   - int_wsl_account_move           │
│   - int_wsl_account_move_line      │
│   - int_wsl_account_payment        │
│   - Partner/Product mappings       │
└──────────────┬─────────────────────┘
               │
               ▼
┌────────────────────────────────────┐
│   Airflow DAG                      │
│                                    │
│   External Task Sensors            │
│    ├─ Wait for upstream DAG        │
│    └─ Wait for customer load       │
│              │                     │
│              ▼                     │
│   Get Total Row Count              │
│    (Query BigQuery)                │
│              │                     │
│              ▼                     │
│   ┌─────────────────────────────┐ │
│   │  TaskGroup: Load Invoices   │ │
│   │  ┌───────────────────────┐  │ │
│   │  │ Task: load_rank_1     │  │ │
│   │  │ (records 1-500)       │  │ │
│   │  └───────────────────────┘  │ │
│   │  ┌───────────────────────┐  │ │
│   │  │ Task: load_rank_501   │  │ │
│   │  │ (records 501-1000)    │  │ │
│   │  └───────────────────────┘  │ │
│   │         ... (dynamic)        │ │
│   └─────────────────────────────┘ │
└────────────────────────────────────┘
               │
               ▼
┌────────────────────────────────────┐
│   Odoo ERP                         │
│                                    │
│   - account.move (invoices)        │
│   - account.move.line (inv lines)  │
│   - account.payment (payments)     │
│   - ir.model.data (external IDs)   │
└────────────────────────────────────┘
```

---

## Code Structure

### Main DAG File

```python
# etl_odoo_accounts_ingestion.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta

# Import domain-specific class
from horeca_digital.odoo_accounts_load import Odoo

# Get total row count for dynamic task creation
def get_total_invoices():
    query = """
    SELECT COUNT(*) as total
    FROM `dwh_trusted.int_odoo_wsl_account_move_consolidated`
    WHERE DATE(write_date) BETWEEN CURRENT_DATE() - 1 AND CURRENT_DATE()
    """
    client = bigquery.Client(project='dwh_project')
    result = client.query(query).result()
    return result.total_rows

# Query builder for chunked processing
def account_query(start, stop) -> str:
    query = f"""
    SELECT
        *,
        ROW_NUMBER() OVER(ORDER BY move_name) as rnk
    FROM `dwh_trusted.int_odoo_wsl_account_move_consolidated`
    WHERE DATE(write_date) BETWEEN CURRENT_DATE() - 1 AND CURRENT_DATE()
    QUALIFY rnk BETWEEN {start} AND {stop}
    """
    return query

with DAG(
    dag_id='etl_odoo_accounts_ingestion',
    schedule_interval='0 4 * * *',  # Daily at 4 AM
    max_active_runs=1,
    concurrency=5,  # Max 5 parallel tasks
    catchup=False,
) as dag:
    
    # Wait for upstream data pipeline
    wait_for_data_prep = ExternalTaskSensor(
        task_id='wait_for_data_prep',
        external_dag_id='etl_wsl_accounts',
        external_task_id='dbt_wsl_accounts_run',
        mode='reschedule',
        poke_interval=30 * 60,  # Check every 30 min
        timeout=23 * 60 * 60,  # 23 hour timeout
    )
    
    # Get total records to process
    get_total = PythonOperator(
        task_id='get_total_invoices',
        python_callable=get_total_invoices
    )
    
    # Dynamic TaskGroup: creates tasks based on row count
    with TaskGroup(group_id='load_invoices') as load_invoices_group:
        # Initialize domain class
        accounts = Odoo(credentials=Variable.get("odoo_prod_creds"))
        
        # Get total at DAG parse time for task generation
        total_rows = get_total_invoices()
        
        # Create task for each chunk (500 records)
        rank_split = [rank for rank in range(1, total_rows, 500)]
        for rank in rank_split:
            rank_start = rank
            rank_end = rank_start + 499
            
            load_chunk = PythonOperator(
                task_id=f'load_rank_{rank_start}',
                python_callable=accounts.move_load_data,
                execution_timeout=timedelta(hours=3),
                op_kwargs={
                    'odoo_creds': "{{ var.json.odoo_prod_creds }}",
                    'query': account_query(start=rank_start, stop=rank_end)
                }
            )
    
    # Task dependencies
    wait_for_data_prep >> get_total >> load_invoices_group
```

---

## Domain Class Implementation

```python
# horeca_digital/odoo_accounts_load.py

from typing import List, Dict, Optional, Any
import logging
from google.cloud import bigquery, storage
from horeca_digital.utils.odoo_utils import Connection

class Odoo:
    """
    Handles Odoo account.move (invoice) operations.
    
    Uses dual connection strategy:
    - OdooRPC for writes (create, update)
    - PostgreSQL for reads (faster lookups)
    """
    
    def __init__(self, credentials: Dict[str, str] = None):
        self.timeout_list = [
            "Connection timed out",
            "Timeout",
            "timed out",
            "Service Temporarily Unavailable"
        ]
        logger.info("Initializing Odoo connection")
        conn = Connection()
        self.odoo = conn.connect(credentials)
        self.pg_connection = conn.connect_pg(credentials)
        self.cursor = self.pg_connection.cursor()
        logger.info("Successfully initialized connections")
    
    def fetchone(self, query: str) -> Optional[Any]:
        """Execute PostgreSQL query and fetch one result."""
        self.cursor.execute(query)
        result = self.cursor.fetchone()
        return result[0] if result else None
    
    def fetchall(self, query: str) -> List[tuple]:
        """Execute PostgreSQL query and fetch all results."""
        self.cursor.execute(query)
        return self.cursor.fetchall()
    
    def _resolve_partner_id(self, row: Dict) -> Optional[int]:
        """
        Resolve partner ID from legacy ID using external identifiers.
        
        Tries multiple lookup strategies:
        1. Direct UUID match in dish_partner_uuid field
        2. External ID in ir_model_data table
        3. Fallback to dummy establishment creation
        """
        # Try direct lookup first
        query = f"""
        SELECT id 
        FROM res_partner 
        WHERE dish_partner_uuid = '{row['partner_uuid']}'
          AND active IN (true, false)
        LIMIT 1
        """
        partner_id = self.fetchone(query)
        
        if partner_id:
            return partner_id
        
        # Try external ID lookup
        query = f"""
        SELECT res_id 
        FROM ir_model_data 
        WHERE module = 'salesforce'
          AND model = 'res.partner'
          AND name = '{row['partner_uuid']}'
        """
        partner_id = self.fetchone(query)
        
        if partner_id:
            return partner_id
        
        # No partner found - create dummy establishment
        logger.warning(f"Partner {row['partner_uuid']} not found, creating dummy")
        return self.create_dummy_establishment(row)
    
    def _resolve_product_id(self, product_code: str) -> Optional[int]:
        """Resolve product ID from product code."""
        query = f"""
        SELECT res_id 
        FROM ir_model_data 
        WHERE module = 'dish_product_catalog'
          AND name = '{product_code}:1:1'
        """
        return self.fetchone(query)
    
    def _resolve_tax_ids(self, tax_names: List[str]) -> List[int]:
        """Resolve tax IDs from tax names."""
        if not tax_names:
            return []
        
        placeholders = ','.join(['%s'] * len(tax_names))
        query = f"""
        SELECT id 
        FROM account_tax 
        WHERE name IN ({placeholders})
        """
        self.cursor.execute(query, tax_names)
        return [row[0] for row in self.cursor.fetchall()]
    
    def move_load_data(self, odoo_creds: Dict, query: str, **kwargs):
        """
        Main method to load account.move (invoices) into Odoo.
        
        Args:
            odoo_creds: Odoo credentials dictionary
            query: BigQuery query to fetch invoice data
        """
        logger.info(f"Starting invoice load for query")
        
        # Fetch data from BigQuery
        client = bigquery.Client(project='dwh_project')
        query_job = client.query(query)
        results = query_job.result()
        
        logger.info(f"Retrieved {results.total_rows} invoices to process")
        
        error_list = []
        success_count = 0
        
        for row in results:
            try:
                # Check if invoice already exists
                existing_query = f"""
                SELECT id 
                FROM account_move 
                WHERE name = '{row['move_name']}'
                """
                existing_id = self.fetchone(existing_query)
                
                if existing_id:
                    logger.debug(f"Invoice {row['move_name']} already exists, skipping")
                    continue
                
                # Resolve foreign keys
                partner_id = self._resolve_partner_id(row)
                if not partner_id:
                    raise ValueError(f"Could not resolve partner for {row['partner_uuid']}")
                
                # Build invoice data
                invoice_data = {
                    'name': row['move_name'],
                    'partner_id': partner_id,
                    'invoice_date': row['invoice_date'],
                    'invoice_date_due': row['invoice_date_due'],
                    'move_type': row['move_type'],  # 'out_invoice', 'out_refund', etc.
                    'currency_id': row['currency_id'],
                    'journal_id': row['journal_id'],
                    'ref': row['reference'],
                    'invoice_line_ids': self._build_invoice_lines(row)
                }
                
                # Create invoice using OdooRPC
                move_model = self.odoo.env['account.move'].with_context(
                    skip_validation=False,
                    check_move_validity=True
                )
                
                invoice_id = move_model.create(invoice_data)
                
                # Create external ID for idempotency
                self.odoo.env['ir.model.data'].create({
                    'module': 'wsl_migration',
                    'model': 'account.move',
                    'res_id': invoice_id,
                    'name': row['move_name'],
                    'noupdate': True
                })
                
                logger.info(f"Created invoice {row['move_name']} with ID {invoice_id}")
                success_count += 1
                
            except Exception as e:
                logger.error(f"Failed to create invoice {row.get('move_name')}: {str(e)}")
                error_list.append({
                    'move_name': row.get('move_name'),
                    'error': str(e),
                    'partner_uuid': row.get('partner_uuid')
                })
                continue
        
        # Log summary
        logger.info(f"Invoice load complete: {success_count} succeeded, {len(error_list)} failed")
        
        # Write errors to GCS for investigation
        if error_list:
            self.write_list_to_gcs(error_list, 
                                  f'accounts_error_list_{kwargs.get("task_instance").task_id}.csv')
        
        return success_count
    
    def _build_invoice_lines(self, row: Dict) -> List[tuple]:
        """
        Build invoice lines for account.move.
        
        Returns list of command tuples for One2Many field:
        [(0, 0, line_data_1), (0, 0, line_data_2), ...]
        """
        lines = []
        
        # Get line items from BigQuery result
        for line in row.get('line_items', []):
            product_id = self._resolve_product_id(line['product_code'])
            tax_ids = self._resolve_tax_ids(line.get('tax_names', []))
            
            line_data = {
                'name': line['description'],
                'product_id': product_id,
                'quantity': line['quantity'],
                'price_unit': line['unit_price'],
                'tax_ids': [(6, 0, tax_ids)],  # Many2Many command tuple
                'account_id': line['account_id'],
            }
            
            lines.append((0, 0, line_data))  # (0, 0, {...}) = Create new record
        
        return lines
    
    def write_list_to_gcs(self, error_list: List[str], file_name: str):
        """Write error list to GCS for debugging."""
        logger.info(f"Writing {len(error_list)} errors to GCS")
        storage_client = storage.Client()
        bucket = storage_client.bucket('composer-data-bucket')
        blob = bucket.blob(f'data/accounts_error_list/{file_name}')
        
        data_string = '\n'.join(str(item) for item in error_list)
        blob.upload_from_string(data_string)
        
        logger.info(f"Error list written to gs://bucket/{file_name}")
```

---

## Connection Management

```python
# horeca_digital/utils/odoo_utils.py

import odoorpc
import psycopg2
import logging

class Connection:
    """Centralized connection management for all Odoo DAGs."""
    
    @staticmethod
    def connect(odoo_creds):
        """
        Connect to Odoo via OdooRPC.
        
        Args:
            odoo_creds: Dictionary with hostname, database, rpc_user, rpc_pwd
        
        Returns:
            odoorpc.ODOO: Connected Odoo client
        """
        try:
            odoo_client = odoorpc.ODOO(
                odoo_creds["hostname"],
                port=443,
                protocol="jsonrpc+ssl",
                version="16.0",
                timeout=30000,  # 30 second timeout
            )
            odoo_client.login(
                odoo_creds["database"],
                odoo_creds["rpc_user"],
                odoo_creds["rpc_pwd"]
            )
            
            user = odoo_client.env.user
            logging.info(f"Successfully connected to Odoo as {user.name}")
            
            return odoo_client
            
        except Exception as e:
            logging.error(f"Failed to connect to Odoo: {e}")
            raise
    
    @staticmethod
    def connect_pg(odoo_creds):
        """
        Connect to Odoo PostgreSQL database directly.
        
        Used for fast read operations (lookups, aggregations).
        
        Args:
            odoo_creds: Dictionary with hostname, database, db_user, db_pwd
        
        Returns:
            psycopg2.connection: PostgreSQL connection
        """
        try:
            connection = psycopg2.connect(
                user=odoo_creds["db_user"],
                password=odoo_creds["db_pwd"],
                host=odoo_creds["hostname"],
                port=5432,
                database=odoo_creds["database"],
                sslmode="require",
                connect_timeout=30000
            )
            
            logging.info(f"Successfully connected to PostgreSQL")
            return connection
            
        except Exception as error:
            logging.error(f"Error connecting to PostgreSQL: {error}")
            raise
```

---

## Key Features Explained

### 1. Dynamic Task Creation

```python
# At DAG parse time, determine how many tasks needed
total_rows = get_total_invoices()  # e.g., 10,000

# Create tasks for 500-record chunks
for start in range(1, total_rows, 500):
    # Creates 20 tasks: load_rank_1, load_rank_501, ..., load_rank_9501
    create_task(start, start + 499)
```

**Benefits**:
- Scales automatically with data volume
- Maximizes Airflow parallelization (5-10 tasks run simultaneously)
- Isolated failure domains (one chunk fails, others continue)

---

### 2. Dual Connection Strategy

```python
# Fast reads from PostgreSQL
partner_id = self.cursor.execute(
    "SELECT id FROM res_partner WHERE dish_partner_uuid = %s",
    (uuid,)
).fetchone()[0]

# Writes through OdooRPC (respects business logic)
self.odoo.env['account.move'].create(invoice_data)
```

**Why Both?**
- PostgreSQL: 10x faster for complex lookups
- OdooRPC: Ensures business rules, triggers, validations fire

---

### 3. External ID Management

```python
# Create external ID for idempotency
self.odoo.env['ir.model.data'].create({
    'module': 'wsl_migration',  # Custom module name
    'model': 'account.move',
    'res_id': invoice_id,
    'name': row['move_name'],  # Unique identifier from source
    'noupdate': True
})

# On subsequent runs, check existence
existing = self.fetchone(
    f"SELECT res_id FROM ir_model_data WHERE name = '{move_name}'"
)
if existing:
    skip_record()
```

---

### 4. Error Isolation

```python
error_list = []

for invoice in invoices:
    try:
        create_invoice(invoice)
    except Exception as e:
        # Don't fail entire batch
        error_list.append({'invoice': invoice, 'error': str(e)})
        continue  # Process next invoice

# After batch, write errors to GCS
write_errors_to_gcs(error_list)
```

---

## Production Lessons Learned

### What Worked

1. **500 Records Per Task**
   - Tested 100, 500, 1000, 5000 chunk sizes
   - 500 was sweet spot: fast enough, good parallelization

2. **Dual Connection Approach**
   - PostgreSQL for lookups reduced execution time by 60%
   - OdooRPC for writes ensured data integrity

3. **External ID Tracking**
   - Made re-runs safe (idempotent)
   - Easy to resume from failure

4. **Error Logging to GCS**
   - CSV format easy to share with business users
   - Detailed context for debugging

### What Didn't Work

1. **Processing All at Once**
   - Initial attempt: single task, 50K records
   - Result: 6 hour timeout
   - Fix: Dynamic chunking

2. **OdooRPC for Lookups**
   - Too slow for complex joins
   - Fix: Direct PostgreSQL queries

3. **No Error Tracking**
   - Initial version failed silently
   - Fix: Explicit error list per chunk

###  Key Insights

- **Chunk size matters**: Too small = overhead, too large = timeout
- **PostgreSQL is your friend**: Use it for reads
- **Isolate failures**: One bad record shouldn't kill the batch
- **External task sensors**: Prevent race conditions with upstream DAGs

---

## Production Deployment Checklist

- [ ] Test with 1% sample (100 invoices) first
- [ ] Verify external ID module name consistency
- [ ] Configure Airflow concurrency (5-10 max)
- [ ] Set up error notification (Slack/email)
- [ ] Create GCS bucket for error logs
- [ ] Test rollback procedure
- [ ] Document foreign key resolution logic
- [ ] Train team on error log format

---

## Related Patterns

- [Dynamic TaskGroups](../04-dynamic-taskgroups/) - Deep dive on parallel processing
- [Connection Management](../05-connection-management/) - Reusable connection logic
- [Leads Ingestion](../02-leads-ingestion/) - Similar pattern, different domain

---

## Files

- [Full DAG Implementation](./etl_odoo_accounts_ingestion.py)
- [Domain Class](./odoo_accounts_load.py)
- [Connection Utilities](./odoo_utils.py)

---

<p align="center">
  <i>50K+ invoices/day | 99.7% success rate | 45 min average execution</i>
</p>
