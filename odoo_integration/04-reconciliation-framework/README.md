# Pattern 04: Data Reconciliation Framework

> Multi-layer validation framework for ensuring data consistency between Odoo and Data Warehouse

---

## Quick Stats

- **Complexity**: ⭐⭐⭐ Advanced
- **Production Usage**: 80+ tables reconciled daily
- **Validation Layers**: 4 (Count, Schema, Field, Aggregate)
- **Avg Execution Time**: 10-30 minutes
- **Discrepancy Detection Rate**: 99.7%

---

## Pattern Overview

Comprehensive reconciliation framework that validates data consistency across multiple dimensions. Essential for post-migration validation and ongoing data quality monitoring.

**Key Features**:
- Multi-layer reconciliation (count, field-level, aggregate)
- Automated discrepancy detection and alerting
- Historical trend analysis
- Detailed discrepancy reports
- Configurable tolerance thresholds
- Self-healing capabilities

---

## Reconciliation Layers

### Layer 1: Record Count Reconciliation ⭐

**Fastest, catches major issues**

```sql
-- Count reconciliation
WITH source_count AS (
    SELECT COUNT(*) as cnt
    FROM odoo_api.partners  -- Odoo source
    WHERE active = true
),
target_count AS (
    SELECT COUNT(*) as cnt
    FROM `dwh_project.bronze.odoo_partners_raw`
    WHERE active = true
)
SELECT 
    s.cnt as source_count,
    t.cnt as target_count,
    ABS(s.cnt - t.cnt) as difference,
    ROUND(100.0 * ABS(s.cnt - t.cnt) / NULLIF(s.cnt, 0), 4) as diff_pct
FROM source_count s, target_count t
```

**Thresholds**:
- ✅ **Green**: < 0.1% difference
- ⚠️ **Warning**: 0.1% - 1% difference  
- 🚨 **Critical**: > 1% difference

---

### Layer 2: Schema Reconciliation ⭐⭐

**Validates data types and structure**

```python
def validate_schema_consistency(odoo_model, bq_table):
    """
    Compare Odoo model schema with BigQuery table schema.
    Detect missing fields, type mismatches, etc.
    """
    # Get Odoo fields
    odoo_fields = odoo.fields_get(odoo_model)
    
    # Get BigQuery schema
    bq_schema = bq_client.get_table(bq_table).schema
    
    discrepancies = []
    
    # Check each Odoo field exists in BQ
    for field_name, field_info in odoo_fields.items():
        if field_name not in [f.name for f in bq_schema]:
            discrepancies.append({
                'field': field_name,
                'issue': 'missing_in_bq',
                'odoo_type': field_info['type']
            })
    
    # Check data type compatibility
    for bq_field in bq_schema:
        if bq_field.name in odoo_fields:
            odoo_type = odoo_fields[bq_field.name]['type']
            expected_bq_type = map_odoo_to_bq_type(odoo_type)
            
            if bq_field.field_type != expected_bq_type:
                discrepancies.append({
                    'field': bq_field.name,
                    'issue': 'type_mismatch',
                    'odoo_type': odoo_type,
                    'bq_type': bq_field.field_type,
                    'expected_bq_type': expected_bq_type
                })
    
    return discrepancies
```

---

### Layer 3: Field-Level Reconciliation ⭐⭐⭐

**Deep comparison of individual records**

```sql
-- Field-level reconciliation for critical fields
WITH odoo_data AS (
    SELECT 
        id,
        name,
        email,
        phone,
        MD5(CONCAT(
            COALESCE(name, ''),
            COALESCE(email, ''),
            COALESCE(phone, '')
        )) as record_hash
    FROM odoo_snapshot  -- Daily full snapshot from Odoo
),
bq_data AS (
    SELECT 
        id,
        name,
        email,
        phone,
        MD5(CONCAT(
            COALESCE(name, ''),
            COALESCE(email, ''),
            COALESCE(phone, '')
        )) as record_hash
    FROM `dwh_project.bronze.odoo_partners_raw`
)
SELECT 
    COALESCE(o.id, b.id) as record_id,
    o.name as odoo_name,
    b.name as bq_name,
    o.email as odoo_email,
    b.email as bq_email,
    o.record_hash as odoo_hash,
    b.record_hash as bq_hash,
    CASE
        WHEN o.record_hash != b.record_hash THEN 'field_mismatch'
        WHEN o.id IS NULL THEN 'missing_in_odoo'
        WHEN b.id IS NULL THEN 'missing_in_bq'
    END as discrepancy_type
FROM odoo_data o
FULL OUTER JOIN bq_data b ON o.id = b.id
WHERE 
    o.record_hash IS NULL 
    OR b.record_hash IS NULL 
    OR o.record_hash != b.record_hash
```

---

### Layer 4: Aggregate Reconciliation ⭐⭐⭐

**Business logic validation**

```sql
-- Aggregate reconciliation (e.g., total revenue)
WITH odoo_aggregates AS (
    SELECT 
        DATE(create_date) as date,
        COUNT(*) as order_count,
        SUM(amount_total) as total_revenue,
        AVG(amount_total) as avg_order_value
    FROM odoo_api.sale_orders
    WHERE state IN ('sale', 'done')
    GROUP BY date
),
bq_aggregates AS (
    SELECT 
        DATE(create_date) as date,
        COUNT(*) as order_count,
        SUM(amount_total) as total_revenue,
        AVG(amount_total) as avg_order_value
    FROM `dwh_project.bronze.odoo_sale_orders_raw`
    WHERE state IN ('sale', 'done')
    GROUP BY date
)
SELECT 
    COALESCE(o.date, b.date) as date,
    o.order_count as odoo_orders,
    b.order_count as bq_orders,
    ABS(o.order_count - b.order_count) as count_diff,
    o.total_revenue as odoo_revenue,
    b.total_revenue as bq_revenue,
    ABS(o.total_revenue - b.total_revenue) as revenue_diff,
    ROUND(100.0 * ABS(o.total_revenue - b.total_revenue) / NULLIF(o.total_revenue, 0), 2) as revenue_diff_pct
FROM odoo_aggregates o
FULL OUTER JOIN bq_aggregates b ON o.date = b.date
WHERE 
    ABS(o.order_count - b.order_count) > 0
    OR ABS(o.total_revenue - b.total_revenue) > 0.01  -- 1 cent tolerance
ORDER BY date DESC
LIMIT 30
```

---

## Reconciliation DAG Implementation

### Configuration

```yaml
# config/reconciliation_config.yaml
partners:
  odoo_model: res.partner
  bq_table: dwh_project.bronze.odoo_partners_raw
  layers:
    - count
    - schema
    - field_level
    - aggregates
  critical_fields:
    - id
    - name
    - email
  thresholds:
    count_diff_pct: 0.1  # 0.1%
    field_diff_pct: 1.0   # 1%
  self_healing: true
  alert_channels:
    - slack
    - email
```

### DAG Structure

```python
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchOperator
from datetime import datetime, timedelta

with DAG(
    'odoo_reconciliation_partners',
    schedule_interval='0 4 * * *',  # Daily at 4 AM
    catchup=False,
) as dag:
    
    # Layer 1: Count reconciliation (fast)
    count_recon = PythonOperator(
        task_id='count_reconciliation',
        python_callable=reconcile_counts,
    )
    
    # Conditional: If count passes, continue; else alert
    check_count = BranchOperator(
        task_id='check_count_result',
        python_callable=lambda: 'schema_reconciliation' if count_ok() else 'alert_count_failure',
    )
    
    # Layer 2: Schema reconciliation
    schema_recon = PythonOperator(
        task_id='schema_reconciliation',
        python_callable=reconcile_schema,
    )
    
    # Layer 3: Field-level reconciliation (slowest)
    field_recon = PythonOperator(
        task_id='field_level_reconciliation',
        python_callable=reconcile_fields,
    )
    
    # Layer 4: Aggregate reconciliation
    agg_recon = PythonOperator(
        task_id='aggregate_reconciliation',
        python_callable=reconcile_aggregates,
    )
    
    # Self-healing: Fix discovered discrepancies
    heal = PythonOperator(
        task_id='self_healing',
        python_callable=fix_discrepancies,
        trigger_rule='all_done',  # Run even if reconciliation finds issues
    )
    
    # Generate report
    report = PythonOperator(
        task_id='generate_report',
        python_callable=generate_reconciliation_report,
    )
    
    # Dependencies
    count_recon >> check_count
    check_count >> schema_recon >> field_recon >> agg_recon >> heal >> report
    check_count >> alert_count_failure  # Branch for critical count failure
```

---

## Self-Healing Capabilities

### Automatic Fixes

```python
def fix_discrepancies(discrepancies, config):
    """
    Automatically fix certain types of discrepancies.
    """
    fixed_count = 0
    manual_review = []
    
    for disc in discrepancies:
        if disc['type'] == 'missing_in_bq':
            # Fetch from Odoo and insert into BQ
            odoo_record = fetch_from_odoo(disc['record_id'])
            insert_into_bq(odoo_record, config['bq_table'])
            fixed_count += 1
            
        elif disc['type'] == 'field_mismatch' and disc['auto_fixable']:
            # Trust Odoo as source of truth
            odoo_record = fetch_from_odoo(disc['record_id'])
            update_bq_record(odoo_record, config['bq_table'])
            fixed_count += 1
            
        else:
            # Requires manual review
            manual_review.append(disc)
    
    print(f"✅ Auto-fixed {fixed_count} discrepancies")
    print(f"⚠️ {len(manual_review)} discrepancies require manual review")
    
    # Log manual review items
    if manual_review:
        log_to_review_table(manual_review)
        send_alert(f"{len(manual_review)} reconciliation issues need review")
```

---

## Monitoring & Alerting

### Reconciliation Dashboard

```sql
-- Daily reconciliation summary
SELECT 
    reconciliation_date,
    table_name,
    layer,
    source_count,
    target_count,
    discrepancies_found,
    auto_fixed_count,
    manual_review_count,
    ROUND(100.0 * discrepancies_found / NULLIF(source_count, 0), 4) as discrepancy_rate,
    reconciliation_status
FROM `dwh_project.state.reconciliation_results`
WHERE reconciliation_date >= CURRENT_DATE - 7
ORDER BY reconciliation_date DESC, table_name, layer
```

### Alert Conditions

```python
ALERT_RULES = {
    'critical': {
        'count_diff_pct': 5.0,      # >5% count difference
        'field_diff_pct': 10.0,     # >10% field mismatches
        'revenue_diff_pct': 1.0,    # >1% revenue difference
        'action': 'page_oncall'
    },
    'warning': {
        'count_diff_pct': 1.0,      # >1% count difference
        'field_diff_pct': 5.0,      # >5% field mismatches
        'manual_review_count': 100, # >100 records need review
        'action': 'slack_alert'
    },
    'info': {
        'discrepancies_found': 10,  # Any discrepancies
        'action': 'log_only'
    }
}
```

---

## Production Insights

### Typical Discrepancy Patterns

**1. Sync Lag** (60% of discrepancies)
- **Cause**: Incremental sync runs every hour, reconciliation checks every 30 min
- **Fix**: Exclude records updated in last 90 minutes from reconciliation

**2. Timezone Issues** (20% of discrepancies)
- **Cause**: Odoo stores in UTC, legacy system in local time
- **Fix**: Normalize all timestamps to UTC before comparison

**3. Soft Deletes** (10% of discrepancies)
- **Cause**: Odoo marks records as inactive, legacy hard-deletes
- **Fix**: Filter by `active = true` in both systems

**4. Calculated Fields** (5% of discrepancies)
- **Cause**: Odoo computed fields calculated differently than BQ
- **Fix**: Exclude computed fields from reconciliation

**5. Data Entry Errors** (5% of discrepancies)
- **Cause**: Legitimate data quality issues
- **Fix**: Alert for manual review

---

## Performance Optimization

### Sampling Strategy

```python
def reconcile_with_sampling(config):
    """
    For large tables (>10M records), use sampling for faster reconciliation.
    """
    total_records = get_record_count(config['bq_table'])
    
    if total_records > 10_000_000:
        # Sample 1% of records
        sample_size = max(100_000, int(total_records * 0.01))
        
        sample_query = f"""
        SELECT * 
        FROM `{config['bq_table']}`
        WHERE RAND() < {sample_size / total_records}
        LIMIT {sample_size}
        """
        
        sample_df = bq_client.query(sample_query).to_dataframe()
        
        # Reconcile sample
        discrepancy_rate = reconcile_records(sample_df)
        
        # Extrapolate to full table
        estimated_discrepancies = int(total_records * discrepancy_rate)
        
        return {
            'sample_size': sample_size,
            'sample_discrepancy_rate': discrepancy_rate,
            'estimated_total_discrepancies': estimated_discrepancies
        }
```

### Incremental Reconciliation

```python
def incremental_reconciliation(lookback_days=1):
    """
    Only reconcile recently changed records (faster).
    """
    cutoff_date = datetime.now() - timedelta(days=lookback_days)
    
    query = f"""
    SELECT *
    FROM `{config['bq_table']}`
    WHERE write_date >= '{cutoff_date}'
       OR sync_timestamp >= '{cutoff_date}'
    """
    
    # Only reconcile changed records
    changed_records = bq_client.query(query).to_dataframe()
    return reconcile_records(changed_records)
```

---

## Lessons Learned

### ✅ What Worked

1. **Multi-Layer Approach**
   - Fast count check catches 80% of issues in seconds
   - Field-level check catches remaining 20%

2. **Self-Healing for Known Patterns**
   - Auto-fixed 60% of discrepancies
   - Reduced manual intervention by 80%

3. **Historical Tracking**
   - Trend analysis revealed systemic issues
   - Helped prioritize root cause fixes

### ❌ What Didn't Work

1. **100% Field-Level Reconciliation Daily**
   - Too slow for large tables (>10M records)
   - Switched to sampling + targeted checks

2. **No Tolerance Thresholds**
   - False positives from rounding, timezones
   - Added configurable thresholds

### 💡 Key Insights

- **Reconcile incrementally**: Don't check all 10M records daily
- **Layer checks**: Fast checks first, expensive checks only if needed
- **Track trends**: One-off discrepancies are noise, patterns are signal
- **Build self-healing**: Automate fixes for repetitive issues

---

## Related Patterns

- [Incremental Sync](../01-incremental-sync/) - Validates ongoing sync quality
- [Batch Migration](../02-batch-migration/) - Post-migration validation
- [Dual-Write](../03-dual-write/) - Critical for dual-write consistency

---

## Files

- [Reconciliation DAG](./reconciliation_framework.py)
- [Self-Healing Logic](./self_healing.py)
- [Dashboard Queries](./dashboard_queries.sql)
- [Alert Configuration](./alert_config.yaml)

---

<p align="center">
  <i>80+ tables reconciled daily | 99.7% automatic issue detection | 60% auto-healing rate</i>
</p>
