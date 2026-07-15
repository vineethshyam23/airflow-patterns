# Odoo Integration Patterns

> Production-tested patterns from a 3-year, 116-DAG ERP migration and integration project

---

## Overview

This category contains battle-tested patterns for integrating with Odoo ERP systems. These patterns emerged from a large-scale enterprise migration project involving:

- **Duration**: 3 years (2021-2024)
- **Scale**: 116 production Airflow DAGs
- **Scope**: Complete ERP migration from legacy system to Odoo
- **Complexity**: Multi-country, multi-entity, zero-downtime cutover
- **Data Volume**: 10M+ records migrated, 500K+ daily incremental records

---

## Pattern Categories

### 1. Data Migration Patterns
Strategies for moving large datasets from legacy systems to Odoo:
- Batch processing with checkpointing
- Parallel processing for performance
- Data transformation and enrichment
- Foreign key resolution and mapping

### 2. Incremental Sync Patterns
Real-time and near-real-time data synchronization:
- Change data capture approaches
- State management and watermarking
- Deduplication strategies
- Conflict resolution

### 3. Dual-Write Patterns
Zero-downtime cutover strategies:
- Write to both old and new systems
- Consistency validation
- Rollback mechanisms
- Gradual migration approaches

### 4. Data Quality & Reconciliation
Ensuring data integrity throughout migration:
- Record count reconciliation
- Data validation frameworks
- Anomaly detection
- Drift monitoring

### 5. API Integration Patterns
Working with Odoo's XML-RPC API:
- Rate limiting and throttling
- Batch operations for performance
- Error handling and retries
- Authentication management

---

## Available Patterns

### 01 - Incremental Sync Pattern
**Use Case**: Daily synchronization of new/updated records from Odoo to Data Warehouse

**Key Features**:
- State-based change detection
- Configurable sync windows
- Idempotent execution
- Failed record tracking

**Best For**: 
- Daily/hourly data refreshes
- Moderate data volumes (1K-1M records/day)
- Systems with reliable update timestamps

[View Pattern →](./01-incremental-sync/)

---

### 02 - Batch Migration with Checkpointing
**Use Case**: One-time migration of large historical datasets with reliability

**Key Features**:
- Chunked processing (1K-10K records/batch)
- Automatic checkpointing
- Resume from failure
- Progress tracking and monitoring

**Best For**:
- Initial data migration
- Large datasets (1M+ records)
- Mission-critical migrations requiring reliability

[View Pattern →](./02-batch-migration/)

---

### 03 - Dual-Write Pattern
**Use Case**: Zero-downtime cutover during ERP migration

**Key Features**:
- Simultaneous writes to legacy and Odoo
- Consistency validation
- Conflict detection and resolution
- Rollback capability

**Best For**:
- Phased migrations
- High-availability requirements
- Risk mitigation during cutover

[View Pattern →](./03-dual-write/)

---

### 04 - Data Reconciliation Framework
**Use Case**: Continuous validation of data consistency between systems

**Key Features**:
- Multi-level reconciliation (record count, field-level, aggregate)
- Automated alerting on mismatches
- Detailed discrepancy reports
- Historical tracking

**Best For**:
- Post-migration validation
- Ongoing data quality monitoring
- Compliance and audit requirements

[View Pattern →](./04-reconciliation-framework/)

---

### 05 - API Rate Limit Handler
**Use Case**: Production-grade rate limiting for Odoo XML-RPC API

**Key Features**:
- Token bucket algorithm
- Automatic retry with exponential backoff
- Request queuing
- Performance metrics

**Best For**:
- High-volume API operations
- Multi-tenant environments
- API quota management

[View Pattern →](./05-rate-limit-handler/)

---

## Technology Stack

**ERP System**: Odoo 13/14/15  
**API Protocol**: XML-RPC  
**Orchestration**: Apache Airflow 2.x  
**Data Warehouse**: BigQuery  
**Languages**: Python 3.8+, SQL  
**Libraries**: xmlrpc.client, pandas, SQLAlchemy

---

## Project Context

### Migration Phases

**Phase 1: Assessment & Planning** (3 months)
- Data profiling and quality assessment
- Mapping legacy schema to Odoo data model
- Dependency analysis and sequencing
- Pilot migration testing

**Phase 2: Batch Migration** (9 months)
- Historical data migration (10M+ records)
- Entity-by-entity migration approach
- Extensive data validation
- Parallel old/new system operation

**Phase 3: Incremental Sync** (6 months)
- Real-time data synchronization
- Dual-write implementation
- Gradual traffic shift to Odoo
- Monitoring and optimization

**Phase 4: Cutover & Optimization** (6 months)
- Zero-downtime cutover
- Legacy system decommissioning
- Performance tuning
- Documentation and handover

### Key Challenges Solved

1. **Foreign Key Resolution**
   - Challenge: Legacy IDs don't exist in Odoo
   - Solution: ID mapping tables with reconciliation logic

2. **Data Quality Issues**
   - Challenge: Legacy data inconsistencies and duplicates
   - Solution: Multi-stage cleansing and validation

3. **API Rate Limits**
   - Challenge: Odoo API throttling with bulk operations
   - Solution: Token bucket rate limiter with intelligent batching

4. **Zero-Downtime Cutover**
   - Challenge: No acceptable maintenance window
   - Solution: Dual-write pattern with gradual traffic shift

5. **Complex Dependencies**
   - Challenge: Inter-entity relationships and ordering
   - Solution: Topological sorting with dependency DAGs

---

## Performance Benchmarks

From production experience with Odoo XML-RPC API:

| Operation | Records/Min | Notes |
|-----------|-------------|-------|
| Read (search_read) | 5,000-10,000 | With field filtering |
| Create (create) | 500-1,000 | Single record creates |
| Create (bulk) | 2,000-5,000 | Batch creates (50-100 records) |
| Update (write) | 1,000-2,000 | Single record updates |
| Update (bulk) | 3,000-6,000 | Batch updates (50-100 records) |

**Optimization Lessons**:
- Batch operations are 3-5x faster than individual calls
- Field filtering reduces response payload by 60-80%
- Parallel processing with 5-10 workers optimal for BigQuery writes
- Connection pooling reduces latency by 30-40%

---

## Common Gotchas & Solutions

### 1. Odoo API Session Timeouts
**Problem**: Long-running operations lose session authentication

**Solution**: Implement session refresh middleware
```python
def with_session_refresh(api_call):
    max_retries = 3
    for attempt in range(max_retries):
        try:
            return api_call()
        except xmlrpc.client.Fault as e:
            if 'session' in str(e).lower():
                re_authenticate()
            else:
                raise
```

### 2. Many2Many Field Handling
**Problem**: Odoo's special command syntax for relational fields

**Solution**: Use command tuples properly
```python
# Link existing records
partner_ids = [(6, 0, [id1, id2, id3])]  # Replace all

# Add records without unlinking
partner_ids = [(4, id1), (4, id2)]  # Add links
```

### 3. Date/Datetime Timezone Issues
**Problem**: Odoo stores in UTC, legacy system in local time

**Solution**: Explicit timezone conversion
```python
from datetime import datetime
import pytz

# Convert to UTC before sending to Odoo
local_tz = pytz.timezone('Europe/Berlin')
utc_dt = local_tz.localize(dt).astimezone(pytz.UTC)
```

### 4. Large Result Set Memory Issues
**Problem**: Reading 100K+ records exhausts memory

**Solution**: Cursor-based pagination
```python
offset = 0
limit = 1000
while True:
    records = odoo.search_read(
        domain, fields, offset=offset, limit=limit
    )
    if not records:
        break
    process_batch(records)
    offset += limit
```

### 5. Duplicate Detection
**Problem**: Re-running migration creates duplicates

**Solution**: Upsert pattern with external ID tracking
```python
# Store legacy ID in Odoo's external ID system
external_id = f"legacy_{model_name}_{legacy_id}"
existing = odoo.search([('id', '=', external_id)])
if existing:
    odoo.write(existing[0], values)
else:
    odoo.create(values)
```

---

## Best Practices

### Code Organization
- ✅ Separate DAGs for each Odoo model/entity
- ✅ Shared utility library for common operations
- ✅ Centralized connection management
- ✅ Configuration-driven (YAML/JSON for mappings)

### Error Handling
- ✅ Granular try-catch blocks per record
- ✅ Failed record logging to BigQuery error table
- ✅ Automatic retry with exponential backoff
- ✅ Alert on error rate thresholds

### Performance
- ✅ Batch API calls (50-100 records optimal)
- ✅ Parallel processing where dependencies allow
- ✅ Field filtering to reduce payload size
- ✅ Connection pooling for database writes

### Data Quality
- ✅ Pre-migration validation checks
- ✅ Post-write reconciliation
- ✅ Automated anomaly detection
- ✅ Record-level audit trail

### Monitoring
- ✅ Records processed per minute
- ✅ Success/failure rates
- ✅ API latency metrics
- ✅ Data drift alerts

---

## Related Patterns

- **API Integrations**: Generic REST/RPC patterns
- **Data Quality**: Validation and reconciliation frameworks
- **Custom Operators**: OdooOperator implementation
- **SQL Patterns**: Data transformation queries

---

## Pattern Selection Guide

| Scenario | Recommended Pattern |
|----------|-------------------|
| Initial historical data load | Batch Migration |
| Daily operational data sync | Incremental Sync |
| Migration with no downtime window | Dual-Write |
| Post-migration validation | Reconciliation Framework |
| High-volume API usage | Rate Limit Handler |
| All of the above | Use combination (typical) |

---

## Success Metrics

From our 3-year Odoo integration project:

- ✅ **10M+ records migrated** with 99.97% accuracy
- ✅ **Zero downtime** during cutover
- ✅ **116 production DAGs** deployed and maintained
- ✅ **500K+ daily records** synced with <1 hour SLA
- ✅ **99.8% DAG success rate** in production
- ✅ **60% reduction** in manual data entry
- ✅ **40% faster** month-end close processes

---

## Additional Resources

- [Odoo XML-RPC API Documentation](https://www.odoo.com/documentation/16.0/developer/api/external_api.html)
- [Airflow Best Practices](../docs/airflow-best-practices.md) *(coming soon)*
- [BigQuery Optimization Guide](../docs/bigquery-optimization.md) *(coming soon)*

---

<p align="center">
  <i>Battle-tested patterns from 116 production Odoo integration DAGs</i> 🚀
</p>
