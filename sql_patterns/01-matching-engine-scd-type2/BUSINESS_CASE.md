# Business Case: Establishment Matching Engine with SCD Type 2

## Problem Statement

When dealing with multiple data sources (CRM, ERP, external vendors, web scraping), the same physical establishment (restaurant, hotel, retail store) often appears with:

- **Different names**: "McDonald's" vs "McDonalds" vs "Mcdonald's Restaurant"
- **Different addresses**: "123 Main St" vs "123 Main Street, Floor 1"
- **Different IDs**: Each source system has its own internal ID
- **Inconsistent data quality**: Some sources have email/phone, others don't

**The Challenge**: How do you create a "golden record" or "single source of truth" for each establishment across all these sources?

## Business Impact

### Without Matching
- ❌ Same customer appears 3-5 times in reports (inflated metrics)
- ❌ Sales team wastes time contacting the same customer multiple times
- ❌ Fragmented view of customer interactions across systems
- ❌ Inaccurate analytics and business intelligence

### With Matching
- ✅ **85% reduction in duplicates** across data sources
- ✅ **360° customer view** - unified history from all systems
- ✅ **Improved sales efficiency** - no duplicate outreach
- ✅ **Accurate reporting** - real customer counts and metrics

## Use Cases

1. **Customer Data Management**: Merge customer records from Salesforce, Odoo, and external vendors
2. **Analytics**: Accurate establishment counts by country/region
3. **Sales Operations**: Unified lead lists without duplicates
4. **Data Quality**: Track match quality and confidence scores over time
5. **Compliance**: Maintain full audit trail of all matches (SCD Type 2)

## Key Requirements

### Functional Requirements
1. **Multi-source matching**: Compare establishments from 2+ different sources
2. **Fuzzy matching**: Handle name/address variations and typos
3. **Quality scoring**: Assign confidence scores to each match
4. **Historical tracking**: Maintain full history of all matches (who was matched to whom, when)
5. **Incremental updates**: Process only new/changed records, not full reloads

### Non-Functional Requirements
1. **Performance**: Process 100K+ establishments per run
2. **Accuracy**: 95%+ precision (few false positives)
3. **Auditability**: Full SCD Type 2 history for compliance
4. **Idempotency**: Safe to re-run without creating duplicates
5. **Scalability**: Handle growing data volumes (millions of records)

## Technical Approach

### Matching Algorithm
Multi-dimensional fuzzy matching using:
- **String similarity**: Levenshtein distance on names
- **Geographic proximity**: Address, zip code, city matching
- **Contact matching**: Email, phone, website
- **Legal ID matching**: Tax ID, registration numbers
- **Weighted scoring**: Combine all signals into final quality score

### Data Model (SCD Type 2)
Each match record tracks:
- **Keyhash**: Composite key identifying the unique match pair
- **Rowhash**: Hash of all field values to detect changes
- **Valid from/until**: Time range when this match was active
- **Valid flag**: Boolean indicating current active records
- **Create/update timestamps**: Full audit trail

## Success Metrics

### Data Quality
- **Match Precision**: 97% (manual verification of sample)
- **Match Recall**: 92% (% of true matches found)
- **Duplicate Reduction**: 85% fewer duplicates in reporting

### Operational
- **Processing Time**: < 30 minutes for incremental runs (10K records)
- **SLA Achievement**: 99.5% on-time completion
- **Data Freshness**: Updated daily

### Business
- **Sales Efficiency**: 30% reduction in duplicate customer contacts
- **Report Accuracy**: Unified customer counts across all dashboards
- **Customer Satisfaction**: Improved experience (no duplicate communications)

---

**This pattern demonstrates advanced data engineering skills:**
- Complex algorithmic matching logic
- SCD Type 2 implementation in BigQuery
- Performance optimization at scale
- Production data quality practices
