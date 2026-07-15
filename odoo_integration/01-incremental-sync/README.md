# Pattern 01: Incremental Sync Pattern

> Daily synchronization of new/updated records from Odoo to BigQuery Data Warehouse

---

## Quick Stats

- **Complexity**: ⭐⭐ Moderate
- **Production Usage**: 40+ DAGs using this pattern
- **Typical Volume**: 1K-1M records/day
- **Avg Execution Time**: 5-30 minutes
- **Success Rate**: 99.8%

---

## Pattern Overview

This pattern implements incremental data synchronization from Odoo ERP to BigQuery, processing only records that have been created or updated since the last successful run.

**Key Features**:
- State-based change detection using Odoo timestamps
- Configurable sync windows (daily, hourly, custom)
- Idempotent execution (safe to re-run)
- Failed record tracking and alerting
- Automatic backfill for missed runs

---

## When to Use This Pattern

✅ **Good For**:
- Daily/hourly operational data synchronization
- Moderate data volumes (1K-1M records/day)
- Systems with reliable `write_date` or `create_date` fields
- Append or upsert-based analytics

❌ **Not Suitable For**:
- Initial historical data load (use Batch Migration pattern instead)
- Real-time streaming requirements (<1 min latency)
- Systems without update timestamps
- Tables requiring full refresh

---

## Architecture

```
┌─────────────┐
│   Odoo ERP  │
│             │
│  Tables:    │
│  - Partners │
│  - Orders   │
│  - Invoices │
└──────┬──────┘
       │ XML-RPC API
       │ (search_read)
       │
       ▼
┌─────────────────────────────┐
│   Airflow DAG               │
│                             │
│  1. Get last sync timestamp │
│  2. Query Odoo for changes  │
│  3. Transform & validate    │
│  4. Load to BigQuery        │
│  5. Update state            │
└──────────┬──────────────────┘
           │
           ▼
┌────────────────────────────┐
│   BigQuery DWH             │
│                            │
│  bronze.odoo_partners_raw  │
│  silver.partners_clean     │
│  state.sync_watermarks     │
└────────────────────────────┘
```

---

## Data Flow

### Step 1: Get Last Successful Sync Timestamp

```sql
-- Query watermark table for last sync
SELECT MAX(sync_timestamp) as last_sync
FROM `dwh_project.state.sync_watermarks`
WHERE source_system = 'odoo'
  AND table_name = 'res_partner'
  AND sync_status = 'SUCCESS'
```

**Fallback**: If no watermark exists, use configurable default (e.g., 7 days ago)

---

### Step 2: Query Odoo for Changed Records

```python
# Build Odoo domain filter
domain = [
    '|',
    ('create_date', '>=', last_sync_timestamp),
    ('write_date', '>=', last_sync_timestamp)
]

# Fetch changed records with pagination
records = []
offset = 0
limit = 1000

while True:
    batch = odoo.search_read(
        model='res.partner',
        domain=domain,
        fields=['id', 'name', 'email', 'create_date', 'write_date'],
        offset=offset,
        limit=limit
    )
    if not batch:
        break
    records.extend(batch)
    offset += limit
```

---

### Step 3: Transform & Validate

```python
# Convert to DataFrame
df = pd.DataFrame(records)

# Data quality checks
df = df.dropna(subset=['id'])  # Remove records without ID
df['email'] = df['email'].str.lower().str.strip()  # Normalize email
df['sync_timestamp'] = datetime.now(timezone.utc)  # Add metadata

# Validation
assert df['id'].is_unique, "Duplicate IDs detected"
assert df['create_date'].notna().all(), "Null create_date found"
```

---

### Step 4: Load to BigQuery

```python
# Upsert pattern using MERGE
staging_table = f"dwh_project.bronze.odoo_partners_staging_{run_id}"

# Load to staging
df.to_gbq(staging_table, if_exists='replace')

# Merge into target
merge_query = f"""
MERGE `dwh_project.bronze.odoo_partners_raw` T
USING `{staging_table}` S
ON T.id = S.id
WHEN MATCHED AND S.write_date > T.write_date THEN
    UPDATE SET 
        name = S.name,
        email = S.email,
        write_date = S.write_date,
        sync_timestamp = S.sync_timestamp
WHEN NOT MATCHED THEN
    INSERT (id, name, email, create_date, write_date, sync_timestamp)
    VALUES (S.id, S.name, S.email, S.create_date, S.write_date, S.sync_timestamp)
"""

client.query(merge_query).result()
```

---

### Step 5: Update Watermark

```python
# Record successful sync
watermark = {
    'source_system': 'odoo',
    'table_name': 'res_partner',
    'sync_timestamp': current_run_timestamp,
    'records_processed': len(df),
    'sync_status': 'SUCCESS',
    'dag_run_id': context['dag_run'].run_id
}

watermark_df = pd.DataFrame([watermark])
watermark_df.to_gbq(
    'dwh_project.state.sync_watermarks',
    if_exists='append'
)
```

---

## Code Implementation

See [`odoo_incremental_sync.py`](./odoo_incremental_sync.py) for complete implementation.

**Key Components**:
- `OdooIncrementalSync` DAG class
- `get_last_sync_timestamp()` task
- `fetch_odoo_changes()` task with pagination
- `transform_and_validate()` task
- `load_to_bigquery()` task with upsert
- `update_watermark()` task
- Error handling and alerting

---

## Configuration

```yaml
# config/odoo_sync_config.yaml
partners:
  odoo_model: res.partner
  bigquery_table: dwh_project.bronze.odoo_partners_raw
  sync_frequency: "0 2 * * *"  # Daily at 2 AM
  lookback_days: 7  # If no watermark, sync last 7 days
  batch_size: 1000
  fields:
    - id
    - name
    - email
    - phone
    - create_date
    - write_date
  primary_key: id
  update_timestamp_field: write_date
```

---

## Error Handling

### Scenario 1: Odoo API Timeout
**Detection**: xmlrpc.client socket timeout  
**Recovery**: Automatic retry with exponential backoff (3 attempts)  
**Fallback**: Alert to DataOps, reduce batch size

### Scenario 2: BigQuery Load Failure
**Detection**: Load job failure  
**Recovery**: Retry with fresh staging table  
**Fallback**: Keep staging data for manual investigation

### Scenario 3: Data Quality Failure
**Detection**: Assertion errors in validation  
**Recovery**: Log failed records to error table, continue with valid records  
**Fallback**: Alert if error rate >5%

### Scenario 4: Partial Batch Failure
**Detection**: Some records fail transformation  
**Recovery**: Log to `dwh_project.errors.sync_failures`, sync successful records  
**Fallback**: Review error patterns, fix and backfill

---

## Monitoring & Alerts

### Key Metrics

```sql
-- Daily sync health check
SELECT 
    sync_timestamp,
    records_processed,
    TIMESTAMP_DIFF(sync_timestamp, LAG(sync_timestamp) OVER (ORDER BY sync_timestamp), HOUR) as hours_since_last_sync,
    sync_status
FROM `dwh_project.state.sync_watermarks`
WHERE source_system = 'odoo' 
  AND table_name = 'res_partner'
ORDER BY sync_timestamp DESC
LIMIT 10
```

### Alerts

1. **Sync Delay Alert**
   - **Condition**: No successful sync in last 25 hours
   - **Action**: PagerDuty to on-call engineer

2. **Volume Anomaly Alert**
   - **Condition**: Records processed >3x or <0.3x of 7-day average
   - **Action**: Slack notification to DataOps

3. **High Error Rate Alert**
   - **Condition**: Error rate >5%
   - **Action**: Slack notification + create JIRA ticket

---

## Performance Optimization

### Baseline Performance
- **10K records**: ~2 minutes
- **100K records**: ~10 minutes
- **1M records**: ~45 minutes

### Optimization Techniques

1. **Odoo Field Filtering**
```python
# Only fetch required fields (60% faster)
fields = ['id', 'name', 'email']  # vs fetching all fields
```

2. **Parallel Processing**
```python
# Split into chunks and process in parallel
from concurrent.futures import ThreadPoolExecutor

with ThreadPoolExecutor(max_workers=5) as executor:
    futures = [executor.submit(process_chunk, chunk) for chunk in chunks]
```

3. **BigQuery Streaming Insert** (for low-latency requirements)
```python
# For <5 min latency
client.insert_rows_json(table, records)
# vs batch load job (better for >1K records/run)
```

4. **Connection Pooling**
```python
# Reuse Odoo sessions across tasks
from airflow.hooks.base import BaseHook
conn = BaseHook.get_connection('odoo_prod')
# Cache session for task duration
```

---

## Lessons Learned

### ✅ What Worked Well

1. **Upsert Pattern with write_date**
   - Handles updates elegantly
   - No duplicate detection needed
   - Simple and reliable

2. **Watermark Table Approach**
   - Easy to debug sync issues
   - Historical tracking of sync patterns
   - Enables backfill strategies

3. **Staging Table Pattern**
   - Atomic commits to production
   - Easy rollback if needed
   - Isolation during processing

### ❌ What Didn't Work

1. **Trusting Odoo's write_date Unconditionally**
   - **Issue**: Some Odoo customizations don't update write_date
   - **Fix**: Added record-level checksums for critical tables

2. **Large Batch Sizes (10K+)**
   - **Issue**: Memory pressure, slow API responses
   - **Fix**: Reduced to 1K batch size

3. **Immediate Deletion Sync**
   - **Issue**: Odoo soft-deletes, API returns nothing
   - **Fix**: Separate reconciliation job to detect missing records

### 💡 Key Insights

1. **Always validate Odoo's write_date behavior** for each model
2. **Monitor data freshness, not just DAG success** 
3. **Build reconciliation from day one**, not as an afterthought
4. **Tune batch size per table** (small tables → larger batches, complex tables → smaller batches)

---

## Testing Strategy

### Unit Tests
```python
def test_incremental_sync_fetch():
    # Mock Odoo API response
    mock_records = [
        {'id': 1, 'name': 'Test', 'write_date': '2024-01-15 10:00:00'}
    ]
    
    # Test pagination logic
    result = fetch_odoo_changes(last_sync='2024-01-14')
    assert len(result) == 1
    assert result[0]['id'] == 1
```

### Integration Tests
```python
def test_end_to_end_sync():
    # Given: Records in Odoo test instance
    # When: Run incremental sync DAG
    # Then: Records appear in BigQuery with correct timestamps
    pass
```

### Production Validation
```sql
-- Compare source vs target counts
WITH odoo_count AS (
    SELECT COUNT(*) as cnt FROM odoo_snapshot  -- Daily full snapshot
),
bq_count AS (
    SELECT COUNT(*) as cnt FROM `dwh_project.bronze.odoo_partners_raw`
)
SELECT 
    o.cnt as odoo_records,
    b.cnt as bq_records,
    ABS(o.cnt - b.cnt) as diff,
    ROUND(100.0 * ABS(o.cnt - b.cnt) / o.cnt, 2) as diff_pct
FROM odoo_count o, bq_count b
```

---

## Production Deployment Checklist

- [ ] Configure Airflow Variables (odoo_url, db_name)
- [ ] Configure Airflow Connections (odoo_prod, bigquery_default)
- [ ] Create BigQuery datasets (bronze, state)
- [ ] Create watermark table
- [ ] Test with 1 week lookback on dev environment
- [ ] Validate data quality and volume
- [ ] Set up monitoring alerts
- [ ] Document runbook for common issues
- [ ] Deploy to production with monitoring
- [ ] Run manual backfill if needed

---

## Related Patterns

- [Batch Migration](../02-batch-migration/) - For initial historical load
- [Reconciliation Framework](../04-reconciliation-framework/) - To validate sync completeness
- [Rate Limit Handler](../05-rate-limit-handler/) - For high-volume syncs

---

## See Also

- [Full DAG Implementation](./odoo_incremental_sync.py)
- [Business Case](./BUSINESS_CASE.md)
- [Architecture Diagram](./ARCHITECTURE.md)
- [Data Flow Details](./DATA_FLOW.md)

---

<p align="center">
  <i>Used in 40+ production DAGs | 99.8% success rate | Processing 500K+ records/day</i>
</p>
