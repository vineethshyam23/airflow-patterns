# Odoo Integration Patterns

> Production patterns from a 3-year, 116-DAG ERP migration project (2021-2024)

---

## Overview

This category contains real production patterns extracted from a large-scale Odoo ERP migration and integration project. These patterns reflect actual architectural decisions, business logic implementations, and operational learnings from managing 116 Airflow DAGs over 3 years.

**Project Scale**:
- **Duration**: 3 years (2021-2024)
- **Scope**: Complete ERP migration from legacy system to Odoo 13/14/15/16
- **DAGs**: 116 production Airflow DAGs
- **Data Volume**: 10M+ records migrated, 500K+ daily incremental
- **Domains**: Accounts, Leads, Opportunities, Subscriptions, Activities, Payments, Projects
- **Countries**: Multi-country deployment (9 countries)

---

## Architecture Overview

### Dual Connection Strategy

All patterns use a dual connection approach for optimal performance:

```python
# OdooRPC for write operations (create, update)
odoo = odoorpc.ODOO(hostname, port=443, protocol="jsonrpc+ssl")
odoo.login(database, user, password)
odoo.env['crm.lead'].create(data)

# PostgreSQL for read operations (faster, more flexible)
pg_conn = psycopg2.connect(host=hostname, database=database, ...)
cursor.execute("SELECT id, name FROM res_partner WHERE ...")
```

**Why Both?**
- **OdooRPC**: Respects business logic, triggers, validations
- **PostgreSQL**: 10x faster for complex queries, bulk reads, aggregations

---

## Common Pattern Structure

Every Odoo pattern follows this architecture:

```
┌─────────────────────────────────────┐
│  Airflow DAG                        │
│                                     │
│  ┌──────────────────────────────┐  │
│  │  External Task Sensors       │  │
│  │  (Wait for upstream DAGs)    │  │
│  └──────────────────────────────┘  │
│             │                       │
│             ▼                       │
│  ┌──────────────────────────────┐  │
│  │  Get Total Row Count         │  │
│  │  (BigQuery)                  │  │
│  └──────────────────────────────┘  │
│             │                       │
│             ▼                       │
│  ┌──────────────────────────────┐  │
│  │  Dynamic TaskGroup           │  │
│  │  - Chunked into 500 records  │  │
│  │  - Parallel execution        │  │
│  │  - Error tracking per chunk  │  │
│  └──────────────────────────────┘  │
│             │                       │
│             ▼                       │
│  ┌──────────────────────────────┐  │
│  │  Domain-Specific ETL Class   │  │
│  │  - BigQuery → Transform      │  │
│  │  - Odoo ID resolution        │  │
│  │  - OdooRPC write             │  │
│  └──────────────────────────────┘  │
└─────────────────────────────────────┘
```

---

## Available Patterns

### 01 - Accounts (Invoice) Load
**Use Case**: Daily sync of customer invoices and invoice lines to Odoo

**Key Features**:
- Dynamic task creation based on BigQuery row count
- 500 records per task for optimal parallelization
- Dual-write validation (PostgreSQL verification)
- Error tracking to GCS with detailed logging
- External ID management for idempotency

**Production Stats**:
- 50K+ invoices/day processed
- Average execution: 45 minutes
- 99.7% success rate
- Handles complex scenarios (credit notes, multi-currency, tax mapping)

[View Pattern →](./01-accounts-invoice-load/)

---

### 02 - Leads Ingestion
**Use Case**: Import leads from data warehouse into Odoo CRM

**Key Features**:
- Automatic product ID resolution from product codes
- Multi-country support with country/language mapping
- Store key lookup and establishment type handling
- Partner deduplication logic
- Business relation identifier tracking

**Production Stats**:
- 10K+ leads/month ingested
- Multi-channel: METRO SAM, MCC Salesforce, direct
- 9 countries supported
- Average execution: 15 minutes

[View Pattern →](./02-leads-ingestion/)

---

### 03 - Opportunities Load
**Use Case**: Migrate opportunities from legacy system to Odoo

**Key Features**:
- Dataclass-based data modeling for type safety
- OdooDefaults caching for performance
- Batch processing with retry logic
- Partner hierarchy handling (company → contact → establishment)
- Complex field mappings (salutation, UTM, establishment types)

**Production Stats**:
- 100K+ opportunities migrated
- Full load + daily incremental support
- ntile-based batching for large datasets
- 99.5% success rate

[View Pattern →](./03-opportunities-load/)

---

### 04 - Dynamic TaskGroups Pattern
**Use Case**: Scalable parallel processing architecture

**Key Features**:
- Runtime task generation based on data volume
- Automatic chunking (configurable: 500-1000 records)
- XCom-based coordination
- Error isolation per chunk
- Progress tracking

**Why This Pattern**:
- Handles variable data volumes (100 to 100K records)
- Maximizes Airflow parallelization
- Isolated failure domains
- Easy to debug (one task = one chunk)

[View Pattern →](./04-dynamic-taskgroups/)

---

### 05 - Connection Management Pattern
**Use Case**: Robust, reusable Odoo connection handling

**Key Features**:
- Centralized connection logic in `odoo_utils.Connection`
- Automatic retry with exponential backoff
- Connection pooling for PostgreSQL
- Timeout handling (30s default, configurable)
- Proper logout and cleanup

**Production Stats**:
- Used by all 116 DAGs
- 99.95% connection success rate
- Automatic recovery from transient failures
- Average connection time: 2-3 seconds

[View Pattern →](./05-connection-management/)

---

### 06 - Helpdesk Tickets Daily Event Export
**Use Case**: dbt refresh of refined Odoo Level-1 helpdesk tickets, then yesterday's creates as Avro events to an external ingest API

**Key Features**:
- dbt Cloud job before ingest (no stale refined snapshot)
- create_date date-delta (not hash-delta / hist table)
- Avro bulk POST in chunks of 500 with OAuth 401 refresh
- Single-market scope (`de`) matching production when this shipped

**Notes**:
- Companion to the warehouse→event-bus family (scoring, SFDC assets), but Odoo helpdesk domain
- Not the Postgres pull extractor (`helpdesk_odoo_import.py`) — that is a separate candidate

[View Pattern →](./06-helpdesk-tickets-export/)

---

### 07 - List-Price / Commission Monthly Delta Export
**Use Case**: Monthly WSL invoice × list-price snapshot, hash-delta vs hist, Avro bulk ingest for finance / partner commission consumers

**Key Features**:
- Monthly schedule (`55 2 1 * *`) after month-end billing close
- Hash-delta with today/hist (same ordering as SFDC asset / scoring)
- Keyhash on parent_bill + establishment; rowhash on commission payload
- Avro bulk POST in chunks of 500 with OAuth 401 refresh
- Single-market scope (`FR`) matching production when this shipped

**Notes**:
- Companion to the warehouse→event-bus family; Odoo finance domain rather than CRM assets or helpdesk creates
- Production insert SQL was a large multi-CTE finance calculation — sanitized builder keeps the join skeleton + hashes

[View Pattern →](./07-list-price-export/)

---

## Technology Stack

**ERP System**: Odoo 13/14/15/16  
**RPC Library**: `odoorpc` (XML-RPC)  
**Database**: PostgreSQL 13+ (direct access for reads)  
**Orchestration**: Apache Airflow 2.x (Cloud Composer)  
**Data Warehouse**: BigQuery  
**Languages**: Python 3.8+, SQL  
**Key Libraries**: `psycopg2`, `pandas`, `dataclasses`, `tenacity`

---

## Common Challenges & Solutions

### Challenge 1: Odoo API Timeouts with Large Datasets

**Problem**: OdooRPC timeouts when processing 10K+ records

**Solution**: Dynamic chunking + parallel task execution
```python
# Get total records from BigQuery
total_rows = get_total_records()

# Create tasks dynamically (500 records each)
with TaskGroup(group_id='load_data') as task_group:
    for start in range(1, total_rows, 500):
        end = start + 499
        PythonOperator(
            task_id=f'load_rank_{start}',
            python_callable=load_chunk,
            op_kwargs={'start': start, 'end': end}
        )
```

---

### Challenge 2: ID Resolution (Foreign Keys)

**Problem**: Legacy IDs don't exist in Odoo, need to map via external IDs

**Solution**: PostgreSQL queries on `ir_model_data` + caching
```python
# Cache all external IDs at start
query = """
    SELECT name, res_id 
    FROM ir_model_data 
    WHERE module = 'salesforce' 
      AND model = 'res.partner'
"""
external_ids = {name: res_id for name, res_id in cursor.fetchall()}

# Fast lookup during processing
partner_id = external_ids.get(legacy_id)
```

---

### Challenge 3: Odoo Business Logic vs Raw Data

**Problem**: Sometimes need to bypass Odoo validations for bulk loads

**Solution**: Context flags + PostgreSQL direct inserts when necessary
```python
# Skip validations for bulk operations
partner = odoo.env['res.partner'].with_context(
    no_vat_validation=True,
    skip_dish_account_sync=True
)
partner.create(data)

# For ultra-fast bulk inserts, use PostgreSQL directly
# (only for tables without complex business logic)
cursor.executemany(
    "INSERT INTO res_partner (name, email, ...) VALUES (%s, %s, ...)",
    records
)
```

---

### Challenge 4: Error Handling & Recovery

**Problem**: One bad record shouldn't fail the entire batch

**Solution**: Try-except per record + error logging
```python
error_list = []
for record in chunk:
    try:
        odoo.env['crm.lead'].create(record)
    except Exception as e:
        error_list.append({
            'record_id': record.get('id'),
            'error': str(e),
            'data': record
        })
        continue

# Write errors to GCS for investigation
write_errors_to_gcs(error_list, f'errors_{chunk_id}.csv')
```

---

### Challenge 5: Many2Many & Many2One Fields

**Problem**: Odoo relational fields require special syntax

**Solution**: Command tuples for Many2Many
```python
# Many2Many: Product IDs
data = {
    'name': 'Lead Name',
    'product_ids': [(6, 0, [product_id1, product_id2, product_id3])]
    # (6, 0, [...]) = Replace all with these IDs
}

# Many2One: Just use the ID
data = {
    'partner_id': partner_id,  # Simple integer
}
```

---

## Performance Benchmarks

From 3 years of production data:

| Operation | Records/Min | Notes |
|-----------|------------|-------|
| Leads Create | 100-200 | OdooRPC with product resolution |
| Opportunities Create | 150-300 | With partner lookups |
| Invoice Load | 50-100 | Complex: lines, taxes, payments |
| Partner Updates | 300-500 | Simple field updates |
| PostgreSQL Reads | 10K-50K | Bulk ID resolution queries |

**Optimization Lessons**:
- Batch: 500 records/task optimal (tested 100, 500, 1000, 5000)
- PostgreSQL for reads: 10x faster than OdooRPC search
- Cache external IDs: 100x faster than repeated lookups
- Parallel tasks: 5-10 tasks optimal on Cloud Composer
- Connection reuse: 30% faster than recreating per record

---

## Best Practices Learned

### Code Organization
**Separate domain classes**: `Odoo` for accounts, `Data` for leads  
**Shared utilities**: `odoo_utils.Connection` for all DAGs  
**Query libraries**: Centralized BigQuery queries  
**Dataclasses**: Type-safe data models

### Error Handling
**Retry decorator**: `@func_with_retries` for transient failures  
**Per-record try-catch**: Don't fail entire batch  
**Error logging to GCS**: Detailed investigation data  
**Alert thresholds**: Slack/email when error rate > 5%

### Performance
**Dual connections**: OdooRPC + PostgreSQL  
**Caching**: External IDs, defaults, lookups  
**Chunking**: 500 records/task sweet spot  
**Parallel execution**: Dynamic TaskGroups

### Monitoring
**XCom progress tracking**: Records processed per task  
**Execution time per chunk**: Identify slow tasks  
**Error rate dashboards**: BigQuery + Data Studio  
**External task sensors**: DAG dependency chains

---

## Migration Project Phases

### Phase 1: Assessment (3 months)
- Data profiling of legacy system
- Odoo model mapping
- Pilot migrations (1000 records per entity)
- Performance benchmarking

### Phase 2: Historical Load (9 months)
- 10M+ records migrated
- Entity-by-entity approach
- Dual-write preparation
- Reconciliation framework

### Phase 3: Incremental Sync (6 months)
- Daily sync DAGs
- Real-time validation
- Gradual traffic shift
- Performance optimization

### Phase 4: Cutover (6 months)
- Zero-downtime cutover
- Legacy system decommissioning
- Documentation handover
- Team training

---

## Success Metrics

- **10M+ records migrated** (accounts, leads, opportunities, subscriptions, invoices, payments)
- **Zero downtime** during cutover
- **116 production DAGs** deployed
- **500K+ daily records** synced
- **99.7% success rate** across all DAGs
- **9 countries** supported
- **40% faster month-end close** process

---

## Related Resources

- [Odoo Technical Documentation](https://www.odoo.com/documentation/16.0/)
- [OdooRPC Library](https://pythonhosted.org/OdooRPC/)
- [Airflow TaskGroups](https://airflow.apache.org/docs/apache-airflow/stable/concepts/dags.html#taskgroups)

---

<p align="center">
  <i>Battle-tested patterns from 116 production DAGs | 10M+ records | 3 years operation</i> 
</p>
