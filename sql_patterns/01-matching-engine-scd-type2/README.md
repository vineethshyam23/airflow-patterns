# Pattern: Matching Engine with SCD Type 2

> Production-tested pattern for maintaining historical records of establishment matches using Slowly Changing Dimension Type 2 in BigQuery

---

## Quick Summary

**Problem**: Track establishment matches across multiple source systems (CRM, ERP, vendors) with full audit trail

**Solution**: SCD Type 2 pattern maintaining complete history of all matches, changes, and deletions

**Result**: 85% duplicate reduction, 100% audit compliance, point-in-time query capability

---

## Documentation Index

1. **[BUSINESS_CASE.md](./BUSINESS_CASE.md)** - Why this pattern exists
   - Problem statement
   - Business impact
   - Use cases
   - Success metrics

2. **[ARCHITECTURE.md](./ARCHITECTURE.md)** - How it's designed
   - High-level architecture
   - Component details
   - Data model (tables, schemas)
   - Design decisions
   - Performance considerations

3. **[DATA_FLOW.md](./DATA_FLOW.md)** - How data moves through the system
   - Step-by-step flow (9 steps)
   - Detailed SQL execution
   - Example: tracking a match through time
   - Performance metrics

4. **[matching_engine_scd2.py](./matching_engine_scd2.py)** - Implementation code
   - Fully documented Python functions
   - SQL query generators
   - Airflow DAG example
   - Runnable test code

---

## Pattern Overview

### What is SCD Type 2?

**Slowly Changing Dimension (SCD) Type 2** is a data warehousing pattern that maintains full history by:
- Creating a new row for each change (instead of updating existing row)
- Using `valid_from` and `valid_until` timestamps to track when each version was active
- Using `valid_flag` boolean to quickly filter for current records

### Key Concepts

**Keyhash**: Unique identifier for a match pair
```sql
MD5(CONCAT(source_1, '|', id_source_1, '|', source_2, '|', id_source_2))
```
→ Identifies which establishments are matched together

**Rowhash**: Hash of all field values
```sql
MD5(CONCAT(match_quality, '|', match_type, '|', fm_mean_name, '|', ...))
```
→ Detects if match details changed

**Two-Step Update Process**:
1. **UPDATE**: Mark existing records as historical (`_valid_flag = False`, set `_valid_until`)
2. **INSERT**: Add new/changed records (`_valid_flag = True`, `_valid_until = 2099-12-31`)

---

## Quick Start

### 1. Review Documentation
```bash
# Start with business case
cat BUSINESS_CASE.md

# Then architecture
cat ARCHITECTURE.md

# Then data flow
cat DATA_FLOW.md
```

### 2. Run Test Query Generation
```bash
python matching_engine_scd2.py
```

### 3. Adapt to Your Environment

**Update table names**:
```python
dataset = "your_dataset"
table = "your_table_name"
stg_table = "your_staging_table"
```

**Update field names** (if your schema differs):
```python
# In get_insert_records_query(), modify SELECT fields
# Example: If you don't have 'original_request' field, remove it
```

### 4. Create BigQuery Tables

**Staging Table**:
```sql
CREATE TABLE dwh_trusted_staging.matching_results_stg (
    run_id STRING,
    iso_code STRING,
    source_1 STRING,
    source_2 STRING,
    id_source_1 STRING,
    id_source_2 STRING,
    match_quality FLOAT64,
    match_type STRING,
    match_quality_rule STRING,
    -- ... other match fields ...
    _keyhash STRING,
    _rowhash STRING,
    _create_ts TIMESTAMP,
    _update_ts TIMESTAMP,
    _sourcesystem STRING
);
```

**Production Table** (add SCD Type 2 fields):
```sql
CREATE TABLE dwh_trusted.matching_results (
    -- ... all fields from staging table ...
    _valid_from TIMESTAMP,
    _valid_until TIMESTAMP,
    _valid_flag BOOLEAN
)
PARTITION BY DATE(_valid_from)
CLUSTER BY iso_code, _valid_flag, _keyhash;
```

### 5. Integrate into Airflow DAG

```python
from matching_engine_scd2 import generate_scd2_merge_queries

# Generate queries
create, update, insert, drop = generate_scd2_merge_queries(
    'dwh_trusted', 'matching_results', 'matching_results_stg'
)

# Create Airflow tasks (see matching_engine_scd2.py for full example)
```

---

## Key Files

| File | Purpose | Lines | Complexity |
|------|---------|-------|------------|
| BUSINESS_CASE.md | Why this exists | 150 | Low |
| ARCHITECTURE.md | System design | 450 | Medium |
| DATA_FLOW.md | Execution flow | 550 | Medium-High |
| matching_engine_scd2.py | Implementation | 400 | Medium |
| README.md | This file | 200 | Low |

**Total**: ~1,750 lines of comprehensive documentation + code

---

## Production Deployment Checklist

### Prerequisites
- [ ] BigQuery dataset created
- [ ] Staging table created
- [ ] Production table created (with partitioning/clustering)
- [ ] Service account has BigQuery Editor permissions
- [ ] Airflow connection configured (`bigquery_default`)

### Initial Setup
- [ ] Test queries on small dataset (100-1K records)
- [ ] Verify row counts match (staging = production active)
- [ ] Check for duplicate keyhashes in active records (should be 0)
- [ ] Validate match quality scores are reasonable (avg > 0.70)

### Ongoing Monitoring
- [ ] Set up row count alerts (staging vs production mismatch)
- [ ] Monitor avg match quality (alert if drops below threshold)
- [ ] Track processing time (alert if > 2x usual duration)
- [ ] Monitor duplicate keyhashes (alert if any found)
- [ ] Weekly audit: spot-check 10-20 random matches for accuracy

### Performance Optimization
- [ ] Partition production table by `DATE(_valid_from)`
- [ ] Cluster by `iso_code`, `_valid_flag`, `_keyhash`
- [ ] Create materialized view for "current matches only"
- [ ] Process countries in parallel if volume is high

---

## Common Queries

### Get Current Matches Only
```sql
SELECT *
FROM dwh_trusted.matching_results
WHERE _valid_flag = True
```

### Get Matches at Specific Point in Time
```sql
SELECT *
FROM dwh_trusted.matching_results
WHERE _valid_from <= '2024-01-15 10:00:00'
  AND _valid_until > '2024-01-15 10:00:00'
```

### Get Full History for a Match Pair
```sql
SELECT *
FROM dwh_trusted.matching_results
WHERE _keyhash = 'abc123...'
ORDER BY _valid_from
```

### Find Match Quality Changes Over Time
```sql
SELECT 
    _keyhash,
    _valid_from,
    match_quality,
    LAG(match_quality) OVER (PARTITION BY _keyhash ORDER BY _valid_from) AS prev_quality,
    match_quality - LAG(match_quality) OVER (PARTITION BY _keyhash ORDER BY _valid_from) AS quality_delta
FROM dwh_trusted.matching_results
WHERE _keyhash = 'abc123...'
ORDER BY _valid_from
```

### Find Recently Deleted Matches
```sql
SELECT *
FROM dwh_trusted.matching_results
WHERE _valid_flag = False
  AND _valid_until >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
ORDER BY _valid_until DESC
```

---

## Performance Benchmarks

### Test Environment
- **Dataset**: dwh_trusted
- **Table Size**: 2M rows (500K active, 1.5M historical)
- **Staging**: 50K new/changed rows per run
- **BigQuery**: US multi-region, standard SQL

### Query Performance
| Query | Execution Time | Rows Processed | Cost |
|-------|----------------|----------------|------|
| Get current matches | 1.2 sec | 500K | $0.01 |
| Update historical | 45 sec | 2K | $0.05 |
| Insert new records | 2.5 min | 2.5K | $0.08 |
| Point-in-time query | 3.4 sec | 480K | $0.02 |
| Full history for match | 0.8 sec | 15 | $0.001 |

### Scaling
| Records | Update Time | Insert Time | Total Time |
|---------|-------------|-------------|------------|
| 10K | 15 sec | 1 min | 1.5 min |
| 50K | 45 sec | 2.5 min | 3.5 min |
| 100K | 1.5 min | 5 min | 7 min |
| 500K | 7 min | 25 min | 35 min |
| 1M | 15 min | 50 min | 70 min |

**Recommendation**: For > 100K records, process by country/region in parallel

---

## Troubleshooting

### Issue: Duplicate Keyhashes in Active Records
**Symptom**: Multiple rows with same `_keyhash` and `_valid_flag = True`

**Cause**: Update query didn't mark old version as historical

**Fix**:
```sql
-- Find duplicates
SELECT _keyhash, COUNT(*)
FROM dwh_trusted.matching_results
WHERE _valid_flag = True
GROUP BY _keyhash
HAVING COUNT(*) > 1;

-- Manual fix: Set older version to historical
UPDATE dwh_trusted.matching_results
SET _valid_flag = False,
    _valid_until = _valid_from  -- or appropriate timestamp
WHERE _keyhash IN (...)
  AND _valid_from < (SELECT MAX(_valid_from) FROM dwh_trusted.matching_results WHERE _keyhash = ...)
```

### Issue: Row Count Mismatch (Staging vs Production)
**Symptom**: Staging has 50K rows, production active has 48K

**Cause**: Some matches were deleted

**Expected**: This is normal if matches no longer exist

**Validation**:
```sql
-- Check how many were deleted
SELECT COUNT(*) AS deleted_count
FROM dwh_trusted.matching_results
WHERE _valid_flag = False
  AND _valid_until >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY);
```

### Issue: Query Taking Too Long
**Symptom**: INSERT query takes > 10 minutes

**Cause**: Missing partitioning/clustering, or full table scan

**Fix**:
1. Add partitioning: `PARTITION BY DATE(_valid_from)`
2. Add clustering: `CLUSTER BY iso_code, _valid_flag, _keyhash`
3. Check query explanation for full table scans
4. Add `WHERE iso_code = 'DE'` filters to limit scope

---

## Lessons Learned

### What Worked Well
✅ **Keyhash + Rowhash pattern**: Fast change detection without comparing all fields  
✅ **Temp table approach**: Atomic updates, easy rollback  
✅ **Partitioning by _valid_from**: Queries only scan relevant dates  
✅ **Truncated hourly timestamps**: Clean hour boundaries for reporting  
✅ **NULLIF chains**: Handles messy data from Python (nan, None, '<NA>')

### What We'd Do Differently
❌ **Initial design without partitioning**: Had to recreate table with 2M rows  
❌ **Too many fields**: 50+ fields make queries hard to read  
❌ **Not processing by country initially**: Hit 2-hour timeout on full runs  
❌ **Manual monitoring**: Should have automated alerts from day 1

### Production Tips
💡 **Always test on small dataset first** (100-1K records)  
💡 **Set up row count alerts immediately** - catches 95% of issues  
💡 **Keep temp tables for 1 day** after success - helps debugging  
💡 **Log keyhash + rowhash of changed records** - audit trail  
💡 **Materialize "current only" view** - faster dashboards

---

## Related Patterns

- **SCD Type 1**: Overwrite in place (simpler, no history)
- **SCD Type 3**: Track previous value only (1 historical version)
- **Snapshot Table**: Daily full snapshots (simpler, more storage)
- **Change Data Capture**: Event-based updates (real-time)

**When to use SCD Type 2** (this pattern):
- Need full audit trail
- Compliance requirements
- Point-in-time analysis needed
- Change history is valuable for analytics

---

## Credits

**Pattern by**: Vineeth Shyam (Head of Data Platform)  
**Production Use**: 4.5 years at enterprise hospitality tech company  
**Scale**: 500K active records, 1.5M historical records  
**SLA**: 99.5% on-time completion

---

## License

MIT License - See [LICENSE](../../../LICENSE) for details.

All code has been sanitized - company names, table names, credentials, and proprietary business logic have been removed or anonymized.

---

**Questions? Found a bug? Have an improvement?**

This is a reference implementation. Adapt it to your specific needs, and always test thoroughly in dev/staging before production deployment!
