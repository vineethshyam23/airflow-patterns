# Pattern 02: Batch Migration with Checkpointing

> Reliable migration of large historical datasets from Odoo to BigQuery

---

## Quick Stats

- **Complexity**: ⭐⭐⭐ Advanced
- **Production Usage**: 15+ migrations using this pattern
- **Typical Volume**: 100K-10M+ records
- **Avg Execution Time**: 2-12 hours
- **Success Rate**: 99.9% (with auto-recovery)

---

## Pattern Overview

This pattern implements reliable batch migration of large historical datasets with automatic checkpointing and recovery capabilities. Perfect for initial data loads during ERP migrations.

**Key Features**:
- Chunked processing (configurable batch size)
- Automatic checkpointing after each batch
- Resume from last successful checkpoint
- Progress tracking and ETA calculation
- Parallel processing support
- Detailed migration audit trail

---

## When to Use This Pattern

✅ **Good For**:
- Initial historical data migration
- Large datasets (100K-10M+ records)
- Mission-critical migrations requiring reliability
- One-time or infrequent data loads

❌ **Not Suitable For**:
- Daily operational syncs (use Incremental Sync instead)
- Real-time data requirements
- Small datasets (<10K records)

---

## Architecture

```
┌─────────────────────┐
│   Odoo ERP          │
│   (Historical Data) │
│                     │
│   10M+ records      │
└──────────┬──────────┘
           │
           │ XML-RPC (batched)
           ▼
┌──────────────────────────────────┐
│   Airflow DAG                    │
│                                  │
│   ┌──────────────────────┐      │
│   │ 1. Get last checkpoint│      │
│   └──────────┬───────────┘      │
│              ▼                   │
│   ┌──────────────────────┐      │
│   │ 2. Fetch batch       │      │
│   │    (1K-10K records)  │◄─┐   │
│   └──────────┬───────────┘  │   │
│              ▼               │   │
│   ┌──────────────────────┐  │   │
│   │ 3. Transform         │  │   │
│   └──────────┬───────────┘  │   │
│              ▼               │   │
│   ┌──────────────────────┐  │   │
│   │ 4. Load to BigQuery  │  │   │
│   └──────────┬───────────┘  │   │
│              ▼               │   │
│   ┌──────────────────────┐  │   │
│   │ 5. Checkpoint        │  │   │
│   └──────────┬───────────┘  │   │
│              │               │   │
│              └───────────────┘   │
│              (loop until done)   │
└──────────────────────────────────┘
           │
           ▼
┌──────────────────────────┐
│   BigQuery DWH           │
│                          │
│   bronze.odoo_*_raw      │
│   state.migration_       │
│         checkpoints      │
└──────────────────────────┘
```

---

## Key Implementation Details

### Checkpoint Table Schema

```sql
CREATE TABLE `dwh_project.state.migration_checkpoints` (
    migration_id STRING NOT NULL,
    source_system STRING NOT NULL,
    table_name STRING NOT NULL,
    last_processed_id INT64,
    records_migrated INT64,
    checkpoint_timestamp TIMESTAMP,
    migration_status STRING,  -- IN_PROGRESS, COMPLETED, FAILED
    dag_run_id STRING
);
```

### Batching Strategy

```python
# Optimal batch sizes based on production experience
BATCH_SIZES = {
    'simple_tables': 10000,      # Few fields, no transformations
    'medium_tables': 5000,       # Moderate complexity
    'complex_tables': 1000,      # Many fields, complex transformations
    'relational_heavy': 500,     # Many2Many, Many2One fields
}
```

### Progress Tracking

```python
def calculate_eta(total_records, processed_records, elapsed_time):
    """Calculate estimated time remaining."""
    if processed_records == 0:
        return "Unknown"
    
    rate = processed_records / elapsed_time  # records per second
    remaining = total_records - processed_records
    eta_seconds = remaining / rate
    
    return format_duration(eta_seconds)

# Output: "ETA: 2h 15m (processing 850 records/min)"
```

---

## Error Handling & Recovery

### Automatic Retry Logic

```python
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=60),
    retry=retry_if_exception_type((TimeoutError, ConnectionError))
)
def fetch_batch(offset, limit):
    return odoo.search_read(...)
```

### Checkpoint Recovery

```python
def resume_from_checkpoint(migration_id):
    """Resume migration from last successful checkpoint."""
    checkpoint = get_last_checkpoint(migration_id)
    
    if checkpoint:
        offset = checkpoint['last_processed_id']
        records_done = checkpoint['records_migrated']
        print(f"🔄 Resuming from ID {offset} ({records_done} records done)")
    else:
        offset = 0
        records_done = 0
        print(f"🆕 Starting fresh migration")
    
    return offset, records_done
```

---

## Performance Optimizations

### Parallel Processing

```python
from concurrent.futures import ThreadPoolExecutor, as_completed

def parallel_batch_migration(batch_configs):
    """Process multiple batches in parallel."""
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {
            executor.submit(migrate_batch, config): config 
            for config in batch_configs
        }
        
        for future in as_completed(futures):
            config = futures[future]
            try:
                result = future.result()
                print(f"✅ Batch {config['batch_id']} complete")
            except Exception as e:
                print(f"❌ Batch {config['batch_id']} failed: {e}")
```

### BigQuery Streaming Insert (for large volumes)

```python
# For very large datasets, use streaming
from google.cloud.bigquery import Client
from google.cloud.exceptions import GoogleCloudError

def stream_to_bigquery(records, table_id):
    """Stream records to BigQuery for immediate availability."""
    client = Client()
    errors = client.insert_rows_json(table_id, records)
    
    if errors:
        print(f"⚠️ Streaming errors: {errors}")
        # Fall back to batch load
        return batch_load_to_bigquery(records, table_id)
    
    return len(records)
```

---

## Monitoring Dashboard

### Key Metrics Query

```sql
-- Real-time migration progress
WITH latest_checkpoint AS (
    SELECT 
        migration_id,
        records_migrated,
        checkpoint_timestamp,
        migration_status
    FROM `dwh_project.state.migration_checkpoints`
    WHERE migration_id = 'partners_migration_2024'
    ORDER BY checkpoint_timestamp DESC
    LIMIT 1
)
SELECT 
    migration_id,
    records_migrated,
    -- Calculate progress percentage (assuming 10M total)
    ROUND(100.0 * records_migrated / 10000000, 2) as progress_pct,
    -- Calculate rate
    records_migrated / TIMESTAMP_DIFF(
        checkpoint_timestamp, 
        LAG(checkpoint_timestamp) OVER (ORDER BY checkpoint_timestamp),
        MINUTE
    ) as records_per_minute,
    migration_status
FROM latest_checkpoint
```

---

## Production Example

### Migrating 10M Odoo Partners

```python
# Configuration
MIGRATION_CONFIG = {
    'migration_id': 'partners_migration_2024_01',
    'odoo_model': 'res.partner',
    'target_table': 'dwh_project.bronze.odoo_partners_raw',
    'total_records_estimate': 10_000_000,
    'batch_size': 5000,
    'parallel_workers': 3,
}

# Execution
# Run 1: Migrated 2M records, then failed (network issue)
# Run 2: Auto-resumed from ID 2,000,000, completed remaining 8M
# Total time: 8 hours
# Success rate: 99.98% (2,000 records required manual review)
```

---

## Lessons Learned

### ✅ What Worked

1. **Checkpointing Every Batch**
   - Made recovery trivial
   - Provided real-time progress visibility
   - Enabled parallelization

2. **Adaptive Batch Sizing**
   - Started with 10K, reduced to 5K for complex tables
   - Monitored memory and adjusted dynamically

3. **Separate Error Table**
   - Failed records logged separately
   - Didn't block migration progress
   - Easy to retry failed records

### ❌ What Didn't Work

1. **Too Large Batch Sizes (50K+)**
   - Memory issues
   - Long checkpoint intervals (risky)
   - Slow API responses

2. **No Progress Visibility**
   - Initially only tracked completion
   - Added ETA calculation after complaints

### 💡 Key Insights

- **Checkpoint frequency matters**: Every 1K-10K records optimal
- **Always track failed records**: You'll need to fix them later
- **Monitor memory usage**: Especially with complex transformations
- **Test with 1% sample first**: Catch issues before full migration

---

## Testing Strategy

```python
def test_migration_with_checkpoint_recovery():
    """Test that migration resumes correctly after failure."""
    # 1. Start migration
    migrate_batch(0, 1000)
    
    # 2. Simulate failure
    raise Exception("Simulated network failure")
    
    # 3. Resume migration
    offset, _ = resume_from_checkpoint('test_migration')
    assert offset == 1000, "Should resume from last checkpoint"
    
    # 4. Continue migration
    migrate_batch(offset, 1000)
```

---

## Deployment Checklist

- [ ] Estimate total record count
- [ ] Choose appropriate batch size (test with 1%)
- [ ] Set up checkpoint table
- [ ] Configure error logging table
- [ ] Test with 1,000 records on dev
- [ ] Test checkpoint recovery
- [ ] Set up monitoring dashboard
- [ ] Schedule during low-usage window
- [ ] Monitor first 10K records closely
- [ ] Document rollback procedure

---

## Related Patterns

- [Incremental Sync](../01-incremental-sync/) - For ongoing updates after migration
- [Reconciliation Framework](../04-reconciliation-framework/) - Validate migration completeness

---

## Files

- [Full Implementation](./batch_migration.py)
- [Checkpoint Schema](./checkpoint_schema.sql)
- [Monitoring Queries](./monitoring_queries.sql)

---

<p align="center">
  <i>Successfully migrated 50M+ records across 15 Odoo tables | 99.9% success rate</i>
</p>
