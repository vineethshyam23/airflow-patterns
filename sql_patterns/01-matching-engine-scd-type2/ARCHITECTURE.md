# Architecture: Matching Engine with SCD Type 2

## High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          DATA SOURCES                                    │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐               │
│  │Salesforce│  │   Odoo   │  │ External │  │ Web Data │               │
│  │   CRM    │  │   ERP    │  │ Vendors  │  │ Scraping │               │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘               │
└───────┼─────────────┼─────────────┼──────────────┼──────────────────────┘
        │             │             │              │
        └─────────────┴─────────────┴──────────────┘
                      │
                      ▼
        ┌─────────────────────────────┐
        │   MATCHING ENGINE SERVICE   │
        │  (Python + Fuzzy Matching)  │
        │                             │
        │  • String similarity        │
        │  • Geographic proximity     │
        │  • Contact matching         │
        │  • Weighted scoring         │
        │  • Quality thresholds       │
        └──────────┬──────────────────┘
                   │
                   ▼
        ┌─────────────────────────────┐
        │    STAGING TABLE (BigQuery)  │
        │  dwh_trusted_staging.        │
        │  matching_results_stg        │
        │                              │
        │  • Raw match results         │
        │  • Keyhash + Rowhash         │
        │  • Quality scores            │
        │  • Source metadata           │
        └──────────┬───────────────────┘
                   │
                   ▼
        ┌─────────────────────────────┐
        │    SCD TYPE 2 LOGIC          │
        │  (SQL in BigQuery)           │
        │                              │
        │  1. Update existing records  │
        │     Set _valid_until,        │
        │     _valid_flag = False      │
        │                              │
        │  2. Insert new records       │
        │     With _valid_from = now   │
        │     _valid_until = 2099      │
        │     _valid_flag = True       │
        └──────────┬───────────────────┘
                   │
                   ▼
        ┌─────────────────────────────┐
        │   PRODUCTION TABLE (BigQuery)│
        │  dwh_trusted.                │
        │  matching_results            │
        │                              │
        │  • Full match history        │
        │  • SCD Type 2 fields         │
        │  • Current + historical      │
        │  • Audit trail               │
        └──────────┬───────────────────┘
                   │
                   ▼
        ┌─────────────────────────────┐
        │    DATA CONSUMERS            │
        │                              │
        │  • Analytics dashboards      │
        │  • Reporting (current view)  │
        │  • Audit/compliance          │
        │  • ML feature engineering    │
        └──────────────────────────────┘
```

## Component Details

### 1. Data Sources (Multiple Systems)
**Purpose**: Raw establishment data from various source systems

**Characteristics**:
- **Salesforce CRM**: Customer accounts, leads, opportunities
- **Odoo ERP**: Partners, contacts, addresses
- **External Vendors**: Third-party data providers
- **Web Scraping**: Business directories, public data

**Challenges**:
- Inconsistent naming conventions
- Different ID schemes per source
- Varying data quality (completeness, accuracy)
- No standard format

### 2. Matching Engine Service
**Purpose**: Calculate fuzzy matches between establishments from different sources

**Technology**: Python-based matching algorithm (separate service or module)

**Matching Dimensions**:

1. **Name Matching** (40% weight):
   - Levenshtein distance
   - Token-based matching (break into words)
   - Phonetic algorithms (Soundex, Metaphone)
   - Common abbreviation handling

2. **Address Matching** (30% weight):
   - Street name normalization
   - Street number extraction (handle "123A", "123-125")
   - Zip code matching (exact)
   - City matching (with fuzzy tolerance)

3. **Contact Matching** (20% weight):
   - Email domain matching
   - Phone number normalization and matching
   - Website URL comparison

4. **Legal ID Matching** (10% weight):
   - Tax ID / VAT number (exact match = high confidence)
   - Business registration numbers

**Output**:
- Match quality score (0.0 - 1.0)
- Match quality rule (which dimension matched best)
- Top match pair (source_1_id ↔ source_2_id)
- Detailed field-level scores (fm_mean_name, fm_mean_address, etc.)

### 3. Staging Table (Temporary Storage)
**Purpose**: Store raw matching results before applying SCD Type 2 logic

**Schema**:
```sql
CREATE TABLE dwh_trusted_staging.matching_results_stg (
    run_id STRING,                      -- Unique run identifier
    iso_code STRING,                    -- Country code (DE, NL, FR, etc.)
    source_1 STRING,                    -- Source system 1 (e.g., "salesforce")
    source_2 STRING,                    -- Source system 2 (e.g., "odoo")
    id_source_1 STRING,                 -- ID in source system 1
    id_source_2 STRING,                 -- ID in source system 2
    match_type STRING,                  -- "exact", "fuzzy", "manual"
    match_quality FLOAT64,              -- Overall score (0.0 - 1.0)
    match_quality_rule STRING,          -- Best matching dimension
    
    -- Field-level matching scores
    fm_mean FLOAT64,                    -- Average of all field scores
    fm_mean_name FLOAT64,               -- Name similarity score
    fm_mean_address FLOAT64,            -- Address similarity score
    fm_mean_zip FLOAT64,                -- Zip code match
    fm_mean_city FLOAT64,               -- City match
    fm_mean_email FLOAT64,              -- Email match
    fm_mean_phone FLOAT64,              -- Phone match
    
    -- Metadata
    _create_ts TIMESTAMP,
    _update_ts TIMESTAMP,
    _job_name STRING,
    _job_id STRING,
    _sourcesystem STRING,
    _keyhash STRING,                    -- Hash of (source_1, id_source_1, source_2, id_source_2)
    _rowhash STRING                     -- Hash of all data fields
);
```

**Keyhash Calculation**:
```sql
MD5(CONCAT(source_1, '|', id_source_1, '|', source_2, '|', id_source_2))
```
→ Uniquely identifies a match pair

**Rowhash Calculation**:
```sql
MD5(CONCAT(match_quality, '|', match_type, '|', fm_mean_name, '|', ...))
```
→ Detects if match details changed

### 4. SCD Type 2 Logic (SQL in BigQuery)
**Purpose**: Maintain full history of all matches with audit trail

**Two-Step Process**:

#### Step 1: Update Existing Records (Set Valid Until)
For records that exist in production but are missing from new staging data (or have changed):

```sql
UPDATE dwh_trusted.tmp_matching_results
SET 
    _update_ts = CURRENT_TIMESTAMP,
    _valid_until = TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 SECOND),
    _valid_flag = False
WHERE 
    _valid_flag = True
    AND _sourcesystem = 'matching-engine'
    AND CONCAT(_keyhash, _rowhash) NOT IN (
        SELECT CONCAT(_keyhash, _rowhash)
        FROM dwh_trusted_staging.matching_results_stg
    )
```

**What this does**:
- Finds existing active records (`_valid_flag = True`)
- That are **not present** in new staging data (either deleted or changed)
- Sets `_valid_until` to current time (marks as historical)
- Sets `_valid_flag = False` (no longer current)

#### Step 2: Insert New Records
For new matches or changed matches from staging:

```sql
INSERT INTO dwh_trusted.matching_results
SELECT 
    *,
    CURRENT_TIMESTAMP AS _valid_from,
    TIMESTAMP('2099-12-31 00:00:00') AS _valid_until,
    True AS _valid_flag
FROM dwh_trusted_staging.matching_results_stg
WHERE CONCAT(_keyhash, _rowhash) NOT IN (
    SELECT CONCAT(_keyhash, _rowhash)
    FROM dwh_trusted.matching_results
    WHERE _valid_flag = True
)
```

**What this does**:
- Inserts new records from staging
- Skips records that already exist in production (same keyhash + rowhash)
- Sets `_valid_from` to now
- Sets `_valid_until` to far future (2099-12-31)
- Sets `_valid_flag = True` (currently active)

### 5. Production Table (Final Storage)
**Purpose**: Store all match results with full SCD Type 2 history

**Schema**: Same as staging + SCD Type 2 fields:
```sql
_valid_from TIMESTAMP,          -- When this version became active
_valid_until TIMESTAMP,         -- When this version became inactive
_valid_flag BOOLEAN             -- True = current version, False = historical
```

**Querying Patterns**:

**Get Current Matches Only**:
```sql
SELECT * FROM dwh_trusted.matching_results
WHERE _valid_flag = True
```

**Get Matches at Specific Point in Time** (Time Travel):
```sql
SELECT * FROM dwh_trusted.matching_results
WHERE _valid_from <= '2024-01-15 10:00:00'
  AND _valid_until > '2024-01-15 10:00:00'
```

**Get Full History for a Match Pair**:
```sql
SELECT * FROM dwh_trusted.matching_results
WHERE _keyhash = 'abc123...'
ORDER BY _valid_from
```

## Design Decisions

### Why SCD Type 2 (Not Type 1 or Type 3)?

**Type 1 (Overwrite)** - ❌ Rejected:
- Loses history (can't audit changes)
- No compliance trail
- Can't answer "what was matched on X date?"

**Type 3 (Additional Columns)** - ❌ Rejected:
- Only tracks previous value, not full history
- Schema changes when adding more history
- Limited to 1-2 previous versions

**Type 2 (New Row Per Change)** - ✅ Chosen:
- ✅ Full audit trail (regulatory compliance)
- ✅ Time travel queries (point-in-time accuracy)
- ✅ Change analysis (see match quality evolution)
- ✅ Unlimited history
- ✅ No schema changes

### Why Keyhash + Rowhash?

**Keyhash Alone**:
- Can't detect if match details changed
- Would require comparing all fields

**Rowhash Addition**:
- ✅ Instant change detection (hash comparison)
- ✅ Performance (no need to compare 50+ fields)
- ✅ Handles null values gracefully

**Combined Approach**:
```
CONCAT(_keyhash, _rowhash) = unique identifier for this exact version
```
- Same keyhash + different rowhash = match pair exists but details changed
- Different keyhash = entirely new match pair

### Why Temporary Table Pattern?

Update-then-insert logic is split into two steps using a temp table (`tmp_matching_results`):

**Benefits**:
1. **Atomic operation**: Either both steps succeed or both fail
2. **Performance**: Bulk update + bulk insert faster than row-by-row
3. **Validation**: Can inspect temp table before final insert
4. **Rollback**: Easy to discard and retry if validation fails

## Performance Considerations

### Partitioning
```sql
-- Partition by date for faster queries
PARTITION BY DATE(_valid_from)
```
→ Query only relevant date ranges

### Clustering
```sql
-- Cluster by commonly filtered fields
CLUSTER BY iso_code, _valid_flag, _keyhash
```
→ Co-locate related data for faster scans

### Incremental Processing
- Only process new/changed source records (not full reload)
- Use `run_id` to identify which records belong to current run
- Filter by `iso_code` to process countries in parallel

### Query Optimization
- Use `_valid_flag = True` filter to avoid scanning historical records
- Index on `_keyhash` for fast lookups
- Materialize "current matches only" view for dashboards

## Data Quality & Monitoring

### Validation Checks
1. **Row count reconciliation**: staging count = insert count
2. **Duplicate detection**: No duplicate keyhash in active records
3. **Orphaned records**: All matches reference valid source IDs
4. **Quality threshold**: Alert if > 5% matches below quality threshold

### Monitoring Metrics
- Records processed per run
- New matches added
- Updated matches (changed scores)
- Deleted matches (no longer valid)
- Average match quality score
- Processing time per run

---

**This architecture demonstrates:**
- Production-grade SCD Type 2 implementation
- Performance optimization at scale
- Data quality and audit trail design
- BigQuery best practices
