# Pattern 03: Dual-Write Pattern

> Zero-downtime ERP cutover strategy with simultaneous writes to legacy and Odoo systems

---

## Quick Stats

- **Complexity**: ⭐⭐⭐⭐ Expert
- **Production Usage**: 3 major cutover events
- **Risk Level**: High (writes to production systems)
- **Cutover Duration**: 2-6 weeks (gradual transition)
- **Success Rate**: 100% (with careful planning)

---

## Pattern Overview

The Dual-Write pattern enables zero-downtime migration from a legacy system to Odoo by simultaneously writing changes to both systems during a transition period. This allows gradual traffic shifting with rollback capability.

**Key Features**:
- Writes go to both old and new systems
- Consistency validation between systems
- Conflict detection and resolution
- Gradual traffic shift (0% → 100% Odoo)
- Rollback capability at any stage
- Comprehensive audit trail

---

## When to Use This Pattern

✅ **Good For**:
- Mission-critical systems with no acceptable downtime
- Phased migration approaches
- High-risk migrations requiring safety net
- Complex business logic requiring gradual validation

❌ **Not Suitable For**:
- Read-only data migrations
- Low-risk systems where downtime is acceptable
- Simple data migrations without complex business logic

---

## Architecture

```
                    ┌─────────────────┐
                    │  Application    │
                    │  Layer          │
                    └────────┬────────┘
                             │
                    ┌────────▼─────────┐
                    │ Write Router     │
                    │ (Traffic Control)│
                    └────────┬─────────┘
                             │
                ┌────────────┴────────────┐
                │                         │
                ▼                         ▼
        ┌───────────────┐        ┌───────────────┐
        │ Legacy System │        │  Odoo System  │
        │               │        │               │
        │ (Old ERP)     │        │  (New ERP)    │
        └───────┬───────┘        └───────┬───────┘
                │                         │
                └────────────┬────────────┘
                             │
                             ▼
                  ┌────────────────────┐
                  │ Consistency        │
                  │ Validator          │
                  │ (Airflow DAG)      │
                  └────────────────────┘
                             │
                             ▼
                  ┌────────────────────┐
                  │ Discrepancy        │
                  │ Reports            │
                  │ (BigQuery)         │
                  └────────────────────┘
```

---

## Implementation Phases

### Phase 1: Setup (Week 0)
- Deploy dual-write infrastructure
- Set up consistency validation
- Create rollback procedures
- Test with synthetic data

### Phase 2: Shadow Mode (Weeks 1-2)
- **Primary**: Legacy system (100% reads/writes)
- **Secondary**: Odoo (writes only, no reads)
- **Goal**: Validate write logic without risk

```python
# Traffic split: 100% Legacy, 0% Odoo
TRAFFIC_CONFIG = {
    'primary': 'legacy',
    'secondary': 'odoo',
    'read_from': 'legacy',
    'write_to': ['legacy', 'odoo'],  # Dual write
}
```

### Phase 3: Read Testing (Weeks 3-4)
- **Primary**: Legacy (100% writes, 90% reads)
- **Secondary**: Odoo (10% reads for validation)
- **Goal**: Validate Odoo reads match legacy

```python
# Traffic split: 90% Legacy reads, 10% Odoo reads
TRAFFIC_CONFIG = {
    'read_split': {
        'legacy': 0.9,
        'odoo': 0.1,  # Compare results
    },
    'write_to': ['legacy', 'odoo'],
}
```

### Phase 4: Gradual Cutover (Weeks 5-6)
- **Gradual shift**: 10% → 50% → 100% Odoo
- **Writes**: Still dual-write to both systems
- **Rollback ready**: Can revert to legacy instantly

```python
# Week 5: 50-50 split
TRAFFIC_CONFIG = {
    'read_split': {'legacy': 0.5, 'odoo': 0.5},
    'write_to': ['legacy', 'odoo'],
}

# Week 6: 100% Odoo
TRAFFIC_CONFIG = {
    'read_split': {'legacy': 0, 'odoo': 1.0},
    'write_to': ['legacy', 'odoo'],  # Still dual-write (safety)
}
```

### Phase 5: Legacy Decommission (Week 7+)
- **Writes**: Odoo only
- **Legacy**: Read-only archive
- **Dual-write**: Disabled

---

## Consistency Validation

### Real-Time Validation

```python
def validate_write_consistency(entity_type, entity_id, write_timestamp):
    """
    Validate that a write to both systems resulted in consistent state.
    Run immediately after dual-write completes.
    """
    # Fetch from legacy
    legacy_data = fetch_from_legacy(entity_type, entity_id)
    
    # Fetch from Odoo
    odoo_data = fetch_from_odoo(entity_type, entity_id)
    
    # Compare critical fields
    discrepancies = compare_entities(legacy_data, odoo_data)
    
    if discrepancies:
        log_discrepancy(entity_type, entity_id, discrepancies, write_timestamp)
        alert_if_critical(discrepancies)
    
    return len(discrepancies) == 0
```

### Batch Reconciliation (Nightly)

```sql
-- Nightly consistency check across all entities
WITH legacy_snapshot AS (
    SELECT id, name, email, status, updated_at
    FROM legacy_db.customers
    WHERE updated_at >= CURRENT_DATE - 1
),
odoo_snapshot AS (
    SELECT id, name, email, status, write_date as updated_at
    FROM `dwh_project.bronze.odoo_partners_raw`
    WHERE DATE(sync_timestamp) = CURRENT_DATE
)
SELECT 
    COALESCE(l.id, o.id) as entity_id,
    l.name as legacy_name,
    o.name as odoo_name,
    l.email as legacy_email,
    o.email as odoo_email,
    CASE 
        WHEN l.name != o.name THEN 'name_mismatch'
        WHEN l.email != o.email THEN 'email_mismatch'
        WHEN l.id IS NULL THEN 'missing_in_legacy'
        WHEN o.id IS NULL THEN 'missing_in_odoo'
    END as discrepancy_type
FROM legacy_snapshot l
FULL OUTER JOIN odoo_snapshot o ON l.id = o.id
WHERE 
    l.name != o.name 
    OR l.email != o.email
    OR l.id IS NULL
    OR o.id IS NULL
```

---

## Conflict Resolution

### Write Conflicts

**Scenario**: Same entity updated in both systems simultaneously

**Resolution Strategy**:
```python
CONFLICT_RESOLUTION = {
    'strategy': 'last_write_wins',  # or 'legacy_wins' during transition
    'conflict_window': 60,  # seconds
}

def resolve_conflict(legacy_write, odoo_write):
    """Handle write conflicts."""
    time_diff = abs((legacy_write.timestamp - odoo_write.timestamp).total_seconds())
    
    if time_diff < CONFLICT_RESOLUTION['conflict_window']:
        # True conflict: writes within 60 seconds
        if CONFLICT_RESOLUTION['strategy'] == 'last_write_wins':
            winner = max([legacy_write, odoo_write], key=lambda w: w.timestamp)
        elif CONFLICT_RESOLUTION['strategy'] == 'legacy_wins':
            winner = legacy_write
        
        # Apply winner's values to both systems
        sync_to_legacy(winner.data)
        sync_to_odoo(winner.data)
        
        log_conflict_resolution(legacy_write, odoo_write, winner)
```

---

## Rollback Procedure

### Instant Rollback (Emergency)

```python
def emergency_rollback():
    """
    Instantly revert all traffic to legacy system.
    Use in case of critical Odoo issues.
    """
    # 1. Update traffic config
    update_traffic_config({
        'read_split': {'legacy': 1.0, 'odoo': 0},
        'write_to': ['legacy'],  # Stop writing to Odoo
    })
    
    # 2. Deploy config (takes ~30 seconds)
    deploy_config_change()
    
    # 3. Alert team
    send_alert("🚨 ROLLBACK: Traffic reverted to legacy system")
    
    # 4. Freeze Odoo writes
    set_odoo_readonly_mode(True)
    
    print("✅ Rollback complete. System running on legacy.")
```

### Data Resync After Rollback

```python
def resync_after_rollback(rollback_timestamp):
    """
    After rollback, resync Odoo with changes made to legacy.
    """
    # Get all legacy changes since rollback
    legacy_changes = fetch_legacy_changes_since(rollback_timestamp)
    
    # Apply to Odoo
    for change in legacy_changes:
        try:
            apply_change_to_odoo(change)
        except Exception as e:
            log_sync_failure(change, e)
    
    # Validate consistency
    run_consistency_check()
```

---

## Risk Mitigation

### Pre-Cutover Checklist

- [ ] Dual-write tested in staging for 2+ weeks
- [ ] Consistency validation shows <0.01% discrepancy rate
- [ ] Rollback procedure tested successfully
- [ ] Odoo performance validated under load
- [ ] Critical business flows validated
- [ ] 24/7 on-call coverage during cutover
- [ ] Communication plan for stakeholders

### During Cutover Monitoring

```python
# Key metrics to watch
CRITICAL_METRICS = {
    'write_success_rate': {
        'threshold': 0.999,  # 99.9%
        'alert': 'page_oncall'
    },
    'consistency_rate': {
        'threshold': 0.999,  # 99.9%
        'alert': 'slack_critical'
    },
    'odoo_response_time_p99': {
        'threshold': 500,  # ms
        'alert': 'slack_warning'
    },
    'discrepancy_rate': {
        'threshold': 0.001,  # 0.1%
        'alert': 'slack_warning'
    }
}
```

---

## Production War Stories

### Successful Cutover: Sales Orders (2022)

**Challenge**: 50K+ daily orders, zero tolerance for data loss

**Approach**:
- 4-week shadow mode (Odoo writes, legacy reads)
- 2-week gradual read shift (10% → 50% → 100%)
- 1-week full Odoo with dual-write safety net
- Final legacy decommission

**Result**:
- ✅ Zero downtime
- ✅ 99.998% consistency rate
- ✅ No rollback needed
- ✅ Detected and fixed 12 edge cases during shadow mode

### Near-Rollback: Invoicing System (2023)

**Challenge**: Odoo invoicing 30% slower than legacy at peak

**Issue Detected**: Week 3, after 50% traffic shift

**Response**:
- Immediately reduced Odoo traffic to 20%
- Optimized Odoo database queries
- Added caching layer
- Gradually increased back to 100% over 2 weeks

**Lesson**: Always monitor performance under real load, not just synthetic tests

---

## Lessons Learned

### ✅ What Worked

1. **Extended Shadow Mode**
   - Caught 90% of issues before user impact
   - Built confidence in Odoo system

2. **Gradual Traffic Shift**
   - Detected performance issues early
   - Easy rollback at low percentages

3. **Automated Consistency Checks**
   - Found issues humans would miss
   - Built trust with stakeholders

### ❌ What Didn't Work

1. **Too Aggressive Timeline**
   - Initial plan: 2 weeks total
   - Reality: 6 weeks needed
   - Lesson: Don't rush cutover

2. **Insufficient Load Testing**
   - Staging didn't replicate production volume
   - Found performance issues in production

### 💡 Key Insights

- **Shadow mode is non-negotiable**: Don't skip it
- **Monitor everything**: Metrics you don't track will hurt you
- **Have a rollback plan**: And test it before cutover
- **Communication is key**: Over-communicate status to stakeholders

---

## Related Patterns

- [Batch Migration](../02-batch-migration/) - Initial historical load before dual-write
- [Incremental Sync](../01-incremental-sync/) - Ongoing sync after cutover
- [Reconciliation Framework](../04-reconciliation-framework/) - Validate consistency

---

## Files

- [Dual-Write Router Implementation](./dual_write_router.py)
- [Consistency Validator DAG](./consistency_validator.py)
- [Rollback Procedures](./rollback_procedures.md)
- [Monitoring Dashboard](./monitoring_dashboard.sql)

---

<p align="center">
  <i>3 successful zero-downtime cutover events | 150K+ entities migrated | 0 rollbacks needed</i>
</p>
