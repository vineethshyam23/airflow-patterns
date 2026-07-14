# Data Flow: Matching Engine with SCD Type 2

## Overview

This document describes the end-to-end data flow for maintaining a historical record of establishment matches using SCD Type 2 pattern in BigQuery.

---

## Flow Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│  STEP 1: EXTRACT SOURCE DATA                                        │
│  ┌──────────┐  ┌──────────┐                                        │
│  │Source 1  │  │Source 2  │                                        │
│  │(SFDC)    │  │(Odoo)    │                                        │
│  └────┬─────┘  └────┬─────┘                                        │
│       │             │                                               │
│       ▼             ▼                                               │
│  [Raw data extracted daily]                                        │
└─────────────────────────────────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────────┐
│  STEP 2: RUN MATCHING ALGORITHM                                     │
│  ┌─────────────────────────────────┐                               │
│  │  Python Matching Service        │                               │
│  │                                 │                               │
│  │  For each pair (source1, source2):                              │
│  │    1. Normalize names/addresses                                 │
│  │    2. Calculate string similarity                               │
│  │    3. Calculate geographic distance                             │
│  │    4. Match contacts (email, phone)                             │
│  │    5. Compute weighted quality score                            │
│  │    6. Generate keyhash + rowhash                                │
│  └─────────────────────────────────┘                               │
│                                                                      │
│  Output: match_results_{run_id}.csv                                │
└─────────────────────────────────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────────┐
│  STEP 3: LOAD TO STAGING TABLE                                      │
│                                                                      │
│  BigQuery Load Job:                                                 │
│  dwh_trusted_staging.matching_results_stg                          │
│                                                                      │
│  Truncate-and-load (staging is transient)                          │
└─────────────────────────────────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────────┐
│  STEP 4: CREATE TEMPORARY TABLE                                     │
│                                                                      │
│  CREATE OR REPLACE TABLE dwh_trusted.tmp_matching_results AS       │
│  SELECT * FROM dwh_trusted.matching_results                        │
│  WHERE _valid_flag = True                                           │
│                                                                      │
│  → Copy only active records to temp table for processing           │
└─────────────────────────────────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────────┐
│  STEP 5: UPDATE EXISTING RECORDS (Set Valid Until)                  │
│                                                                      │
│  UPDATE dwh_trusted.tmp_matching_results                           │
│  SET _valid_until = CURRENT_TIMESTAMP - 1 second,                  │
│      _valid_flag = False                                            │
│  WHERE CONCAT(_keyhash, _rowhash) NOT IN (                        │
│      SELECT CONCAT(_keyhash, _rowhash)                             │
│      FROM dwh_trusted_staging.matching_results_stg                 │
│  )                                                                  │
│                                                                      │
│  → Marks records as historical if they:                            │
│    • Disappeared from new data (match no longer exists)            │
│    • Changed (same keyhash but different rowhash)                  │
└─────────────────────────────────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────────┐
│  STEP 6: INSERT NEW/CHANGED RECORDS                                 │
│                                                                      │
│  INSERT INTO dwh_trusted.matching_results                          │
│  SELECT                                                             │
│      *,  -- All fields from staging                                │
│      CURRENT_TIMESTAMP AS _valid_from,                             │
│      TIMESTAMP('2099-12-31') AS _valid_until,                      │
│      True AS _valid_flag                                            │
│  FROM dwh_trusted_staging.matching_results_stg                     │
│  WHERE CONCAT(_keyhash, _rowhash) NOT IN (                        │
│      SELECT CONCAT(_keyhash, _rowhash)                             │
│      FROM dwh_trusted.tmp_matching_results                         │
│      WHERE _valid_flag = True                                       │
│  )                                                                  │
│                                                                      │
│  → Inserts new records:                                            │
│    • Brand new matches (new keyhash)                               │
│    • Updated matches (same keyhash, new rowhash)                   │
└─────────────────────────────────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────────┐
│  STEP 7: MERGE TEMP TABLE BACK                                      │
│                                                                      │
│  DELETE FROM dwh_trusted.matching_results                          │
│  WHERE _valid_flag = True                                           │
│    AND _sourcesystem = 'matching-engine'                           │
│                                                                      │
│  INSERT INTO dwh_trusted.matching_results                          │
│  SELECT * FROM dwh_trusted.tmp_matching_results                    │
│                                                                      │
│  → Replace active records with updated versions from temp table    │
└─────────────────────────────────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────────┐
│  STEP 8: DATA VALIDATION                                            │
│                                                                      │
│  Row Count Check:                                                   │
│    SELECT COUNT(*) FROM staging                                     │
│    vs                                                               │
│    SELECT COUNT(*) FROM production WHERE _valid_flag = True        │
│                                                                      │
│  Quality Check:                                                     │
│    SELECT AVG(match_quality) FROM production                       │
│    → Alert if < threshold (e.g., 0.75)                             │
│                                                                      │
│  Duplicate Check:                                                   │
│    SELECT _keyhash, COUNT(*)                                       │
│    FROM production WHERE _valid_flag = True                        │
│    GROUP BY _keyhash HAVING COUNT(*) > 1                           │
│    → Should be 0 rows                                              │
└─────────────────────────────────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────────┐
│  STEP 9: CLEANUP                                                     │
│                                                                      │
│  DROP TABLE dwh_trusted.tmp_matching_results                       │
│  TRUNCATE TABLE dwh_trusted_staging.matching_results_stg           │
│                                                                      │
│  → Clean up temporary artifacts                                    │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Detailed Step-by-Step Flow

### Step 1: Extract Source Data
**Trigger**: Daily scheduled Airflow DAG (3 AM UTC)  
**Duration**: 5-10 minutes

**Actions**:
1. Query Salesforce API for all accounts (incremental: last 24 hours)
2. Query Odoo database for all partners (incremental: last 24 hours)
3. Extract relevant fields: name, address, city, zip, email, phone, etc.
4. Write to temporary CSV files in Cloud Storage

**Output**:
- `gs://bucket/matching/source1_{date}.csv`
- `gs://bucket/matching/source2_{date}.csv`

### Step 2: Run Matching Algorithm
**Trigger**: After Step 1 completes  
**Duration**: 20-40 minutes (depends on volume)

**Actions**:
1. Load CSV files into Pandas DataFrames
2. For each combination of (source1_record, source2_record):
   - Normalize strings (lowercase, remove punctuation, trim whitespace)
   - Calculate Levenshtein distance for name
   - Parse and compare addresses (street name, street number, zip, city)
   - Compare contact info (email domain, phone with country code)
   - Apply weighted formula to compute overall match_quality score
   - Generate keyhash: `MD5(source1|id1|source2|id2)`
   - Generate rowhash: `MD5(all_field_values)`
3. Filter matches below quality threshold (e.g., drop if score < 0.60)
4. Write results to CSV

**Matching Formula Example**:
```python
match_quality = (
    0.40 * name_similarity +
    0.30 * address_similarity +
    0.20 * contact_similarity +
    0.10 * legal_id_match
)
```

**Output**:
- `gs://bucket/matching/results_{run_id}.csv`

### Step 3: Load to Staging Table
**Trigger**: After Step 2 completes  
**Duration**: 2-5 minutes

**Actions**:
1. Truncate staging table (it's transient, not historical)
2. Load CSV from Cloud Storage to BigQuery staging table
3. Validate row counts (CSV rows = BQ rows loaded)

**BigQuery Load Job**:
```sql
LOAD DATA OVERWRITE dwh_trusted_staging.matching_results_stg
FROM FILES (
    format = 'CSV',
    uris = ['gs://bucket/matching/results_{run_id}.csv']
);
```

### Step 4: Create Temporary Table
**Trigger**: After Step 3 completes  
**Duration**: 30 seconds - 2 minutes

**Actions**:
1. Copy current active records to temp table for processing
2. This allows us to do bulk updates without locking the main table

**SQL**:
```sql
CREATE OR REPLACE TABLE dwh_trusted.tmp_matching_results AS
SELECT * 
FROM dwh_trusted.matching_results
WHERE _valid_flag = True
  AND _sourcesystem = 'matching-engine';
```

**Why temp table?**
- Production table remains queryable during updates
- Can rollback if validation fails
- Faster bulk operations

### Step 5: Update Existing Records
**Trigger**: After Step 4 completes  
**Duration**: 1-3 minutes

**Actions**:
1. Find records in temp table that are NOT present in new staging data
2. Set their `_valid_until` to current timestamp (marking as historical)
3. Set `_valid_flag` to False (no longer current)

**SQL**:
```sql
UPDATE dwh_trusted.tmp_matching_results
SET 
    _update_ts = CURRENT_TIMESTAMP,
    _valid_until = TIMESTAMP_SUB(CURRENT_TIMESTAMP, INTERVAL 1 SECOND),
    _valid_flag = False
WHERE 
    _valid_flag = True
    AND _sourcesystem = 'matching-engine'
    AND CONCAT(_keyhash, _rowhash) NOT IN (
        SELECT CONCAT(_keyhash, _rowhash)
        FROM dwh_trusted_staging.matching_results_stg
    )
    -- Ensure we only update records for countries in current run
    AND CONCAT(original_request, iso_code) IN (
        SELECT DISTINCT CONCAT(original_request, iso_code)
        FROM dwh_trusted_staging.matching_results_stg
    );
```

**Records Updated**:
- Matches that no longer exist (e.g., source record was deleted)
- Matches that changed (same keyhash, different rowhash)

**Records NOT Updated**:
- Matches unchanged (same keyhash + rowhash in both temp and staging)
- Matches for countries not in current run (preserves historical data)

### Step 6: Insert New/Changed Records
**Trigger**: After Step 5 completes  
**Duration**: 2-5 minutes

**Actions**:
1. Insert records from staging that don't exist in production (with _valid_flag = True)
2. Set `_valid_from` to current timestamp
3. Set `_valid_until` to far future (2099-12-31)
4. Set `_valid_flag` to True

**SQL**:
```sql
INSERT INTO dwh_trusted.matching_results
SELECT 
    run_id,
    iso_code,
    source_1,
    source_2,
    id_source_1,
    id_source_2,
    match_quality,
    match_quality_rule,
    -- ... all other fields ...
    CURRENT_TIMESTAMP AS _valid_from,
    TIMESTAMP('2099-12-31 00:00:00') AS _valid_until,
    True AS _valid_flag
FROM dwh_trusted_staging.matching_results_stg
WHERE CONCAT(_keyhash, _rowhash) NOT IN (
    SELECT CONCAT(_keyhash, _rowhash)
    FROM dwh_trusted.matching_results
    WHERE _valid_flag = True
      AND _sourcesystem = 'matching-engine'
);
```

**Records Inserted**:
- Brand new matches (new keyhash)
- Updated matches (same keyhash, new rowhash)

### Step 7: Merge Temp Table Back
**Trigger**: After Step 6 completes  
**Duration**: 1-2 minutes

**Actions**:
1. Delete current active records from production
2. Insert updated records from temp table (includes records with _valid_flag = False)

**SQL**:
```sql
-- Delete current active records
DELETE FROM dwh_trusted.matching_results
WHERE _valid_flag = True
  AND _sourcesystem = 'matching-engine';

-- Insert updated records from temp
INSERT INTO dwh_trusted.matching_results
SELECT * FROM dwh_trusted.tmp_matching_results;
```

**Result**: Production table now has updated active records + full history

### Step 8: Data Validation
**Trigger**: After Step 7 completes  
**Duration**: 30 seconds - 1 minute

**Validation Checks**:

**1. Row Count Reconciliation**:
```sql
-- Staging count
SELECT COUNT(*) AS staging_count
FROM dwh_trusted_staging.matching_results_stg;

-- Production active count
SELECT COUNT(*) AS production_count
FROM dwh_trusted.matching_results
WHERE _valid_flag = True
  AND _sourcesystem = 'matching-engine';

-- Must match (or be within tolerance if deletions expected)
```

**2. Quality Threshold Check**:
```sql
SELECT 
    AVG(match_quality) AS avg_quality,
    MIN(match_quality) AS min_quality,
    COUNT(CASE WHEN match_quality < 0.70 THEN 1 END) AS low_quality_count
FROM dwh_trusted.matching_results
WHERE _valid_flag = True;

-- Alert if avg_quality < 0.75 or low_quality_count > 5% of total
```

**3. Duplicate Check**:
```sql
SELECT 
    _keyhash,
    COUNT(*) AS duplicate_count
FROM dwh_trusted.matching_results
WHERE _valid_flag = True
GROUP BY _keyhash
HAVING COUNT(*) > 1;

-- Should return 0 rows (no duplicate active matches)
```

**4. Historical Integrity Check**:
```sql
SELECT 
    _keyhash,
    COUNT(*) AS version_count,
    MAX(_valid_until) AS latest_valid_until
FROM dwh_trusted.matching_results
GROUP BY _keyhash
HAVING MAX(_valid_until) < CURRENT_TIMESTAMP
  AND MAX(_valid_until) != TIMESTAMP('2099-12-31');

-- Find matches with no current version (gap in history)
```

**Actions on Failure**:
- Send Slack alert to #data-platform channel
- Mark Airflow task as FAILED
- Rollback: restore from previous day's snapshot if critical

### Step 9: Cleanup
**Trigger**: After Step 8 completes (success or failure)  
**Duration**: 10-20 seconds

**Actions**:
1. Drop temporary table
2. Truncate staging table
3. Delete CSV files from Cloud Storage (optional, keep for 7 days for debugging)

**SQL**:
```sql
DROP TABLE IF EXISTS dwh_trusted.tmp_matching_results;
TRUNCATE TABLE dwh_trusted_staging.matching_results_stg;
```

---

## Example: Tracking a Match Through Time

Let's follow a single match through multiple runs to see SCD Type 2 in action.

### Run 1 (2024-01-01 03:00:00)
**New match found**: Restaurant "Joe's Pizza" in Salesforce matches "Joey's Pizzeria" in Odoo

**Inserted Record**:
```
_keyhash: abc123
_rowhash: xyz789
source_1: salesforce
id_source_1: SF_12345
source_2: odoo
id_source_2: ODOO_67890
match_quality: 0.88
_valid_from: 2024-01-01 03:00:00
_valid_until: 2099-12-31 00:00:00
_valid_flag: True
```

### Run 2 (2024-01-02 03:00:00)
**No change**: Same match, same quality score

**Action**: None (keyhash + rowhash unchanged, so no update/insert)

**Record Remains**:
```
_keyhash: abc123
_rowhash: xyz789
_valid_from: 2024-01-01 03:00:00  ← unchanged
_valid_until: 2099-12-31 00:00:00
_valid_flag: True
```

### Run 3 (2024-01-03 03:00:00)
**Match updated**: Odoo record's phone number was updated, improving match quality from 0.88 → 0.92

**Step 5 (Update)**:
Old record is marked as historical:
```
_keyhash: abc123
_rowhash: xyz789  ← old hash
_valid_from: 2024-01-01 03:00:00
_valid_until: 2024-01-03 03:00:00  ← set to now
_valid_flag: False  ← marked historical
```

**Step 6 (Insert)**:
New version is inserted:
```
_keyhash: abc123  ← same (same match pair)
_rowhash: def456  ← new hash (quality changed)
match_quality: 0.92  ← improved
_valid_from: 2024-01-03 03:00:00  ← now
_valid_until: 2099-12-31 00:00:00
_valid_flag: True
```

**Result in Production**:
```
| _keyhash | _rowhash | match_quality | _valid_from         | _valid_until        | _valid_flag |
|----------|----------|---------------|---------------------|---------------------|-------------|
| abc123   | xyz789   | 0.88          | 2024-01-01 03:00:00 | 2024-01-03 03:00:00 | False       |
| abc123   | def456   | 0.92          | 2024-01-03 03:00:00 | 2099-12-31 00:00:00 | True        |
```

### Run 4 (2024-01-04 03:00:00)
**Match deleted**: Odoo record was merged with another record, match no longer valid

**Step 5 (Update)**:
Current record is marked as historical:
```
_keyhash: abc123
_rowhash: def456
_valid_from: 2024-01-03 03:00:00
_valid_until: 2024-01-04 03:00:00  ← set to now
_valid_flag: False  ← marked historical
```

**Step 6 (Insert)**:
No insert (match doesn't exist in staging anymore)

**Result in Production**:
```
| _keyhash | _rowhash | match_quality | _valid_from         | _valid_until        | _valid_flag |
|----------|----------|---------------|---------------------|---------------------|-------------|
| abc123   | xyz789   | 0.88          | 2024-01-01 03:00:00 | 2024-01-03 03:00:00 | False       |
| abc123   | def456   | 0.92          | 2024-01-03 03:00:00 | 2024-01-04 03:00:00 | False       |
```

**Full History Preserved**:
- We can see this match existed from Jan 1 - Jan 4
- Quality improved on Jan 3
- Match was deleted on Jan 4
- All timestamps are accurate for compliance/auditing

---

## Performance Metrics

### Typical Run Statistics (10K establishments, 50K match pairs)

| Step | Duration | Rows Processed | Notes |
|------|----------|----------------|-------|
| 1. Extract | 5-10 min | 20K | 10K from each source |
| 2. Matching | 30-40 min | 10K × 10K = 100M comparisons | Python CPU-intensive |
| 3. Load Staging | 2-3 min | 50K | BQ load job |
| 4. Create Temp | 1 min | 48K | Active records only |
| 5. Update | 2 min | 2K | ~4% change rate |
| 6. Insert | 3 min | 2.5K | New + updated |
| 7. Merge | 1 min | 48K | Replace active records |
| 8. Validate | 30 sec | - | Quality checks |
| 9. Cleanup | 10 sec | - | Drop temp |
| **Total** | **45-60 min** | **50K** | End-to-end |

### Scaling Considerations

**100K establishments** (10x scale):
- Matching step becomes bottleneck (3-4 hours)
- Solution: Parallel processing by country/region
- Split into 10 DAGs, each processing 1 country

**1M establishments** (100x scale):
- Matching: 20-30 hours (infeasible daily)
- Solution: Incremental matching (only new/changed records)
- Use change data capture from source systems
- Only re-match if source record fields changed

---

**This data flow demonstrates:**
- Production-grade SCD Type 2 implementation
- Comprehensive data validation
- Performance optimization strategies
- Real-world complexity handling
