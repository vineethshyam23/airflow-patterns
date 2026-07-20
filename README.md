# Airflow Patterns - Production Data Engineering Reference

> Personal knowledge base of production-tested Airflow DAG patterns and utilities from 4.5 years of enterprise data platform work

---

## About This Repository

This repository contains **sanitized, anonymized implementations** of production Airflow DAGs I've built across various business domains. It serves as:

- **Personal reference** for patterns I'll reuse in future projects
- **Code samples** for technical interviews and discussions
- **Knowledge base** of production-tested solutions to common data engineering challenges

**All code has been sanitized:**
- Company names, project names, and table names replaced with generic equivalents
- Credentials, connection IDs, and emails removed
- Proprietary business logic anonymized where necessary
- Real production patterns and architectures preserved

---

## Repository Structure

```
airflow-patterns/
├── odoo_integration/            # ERP (Odoo) integration patterns
├── salesforce_integration/      # CRM (Salesforce) integration patterns
├── ml_pipelines/                # ML model deployment and scoring
├── scoring_analytics/           # Customer scoring and analytics
├── payment_processing/          # Payment system integrations
├── data_quality/                # Validation and reconciliation
├── custom_operators/            # Reusable Airflow operators
├── utilities/                   # Helper functions and decorators
├── sql_patterns/                # Complex SQL queries and patterns
└── docs/                        # Documentation and guides
```

GCP API platform work (Cloud Run, API Gateway, Apigee, API keys) lives in a separate repo: **[api-integrations](https://github.com/vineethshyam23/api-integrations)**.

---

## Pattern Categories

### ERP Integration (Odoo)
Patterns from a 3-year, 116-DAG ERP migration project:
- Batch data migration strategies
- Incremental sync patterns
- Dual-write for zero-downtime cutover
- Data reconciliation frameworks

### CRM Integration (Salesforce)
Salesforce API integration patterns:
- Bulk API for large data volumes
- Change data capture patterns
- Bi-directional sync strategies
- Asset and opportunity tracking

### ML Pipelines
End-to-end ML pipeline patterns:
- Vertex AI model deployment
- Feature engineering pipelines
- Batch scoring workflows
- Model monitoring and versioning

### Scoring & Analytics
Customer and establishment scoring algorithms:
- Multi-dimensional scoring logic
- Country-specific business rules
- Performance optimization for scale
- BigQuery advanced analytics

### Payment Processing
Financial data processing patterns:
- Transaction ingestion and validation
- Payment reconciliation
- Multi-currency handling
- Compliance and audit trails

### Data Quality
Production data quality frameworks:
- Multi-layer validation strategies
- Automated reconciliation
- Anomaly detection
- SLA monitoring and alerting

### Custom Operators
Reusable Airflow operators I've built:
- Database retry operators
- dbt integration operators
- Cloud SQL operators
- Generic API operators

### Utilities
Helper functions and decorators:
- BigQuery utilities
- Error handling decorators
- Retry mechanisms
- Logging utilities

### SQL Patterns
Complex SQL implementations:
- SCD Type 2 queries
- Fuzzy matching algorithms
- Window functions for analytics
- Performance-optimized CTEs

---

## Pattern Documentation

Each pattern includes:

1. **Business Case**: What problem does this solve?
2. **Architecture**: How is it structured?
3. **Data Flow**: How does data move through the pipeline?
4. **Code**: Fully documented, anonymized implementation
5. **Lessons Learned**: Production insights and gotchas

---

## Usage

These patterns are **reference implementations**, not production-ready out-of-the-box. You'll need to:

- Adapt connection IDs and credentials to your environment
- Adjust table names and schemas to your data warehouse
- Modify business logic for your specific use cases
- Test thoroughly in dev/staging before production

---

## Technology Stack

**Orchestration**: Apache Airflow 2.x on Cloud Composer  
**Cloud**: Google Cloud Platform (BigQuery, Cloud Storage, Vertex AI)  
**Languages**: Python 3.8+, SQL  
**Integrations**: Salesforce, Odoo, Adyen, Mailchimp, various APIs  
**Data Processing**: Pandas, NumPy, SQLAlchemy  
**ML**: scikit-learn, TensorFlow, Vertex AI  

---

## Related Repository

For business context, architecture diagrams, and impact metrics of these implementations, see:

📊 **[Data Platform Portfolio](https://github.com/vineethshyam23/data-platform-portfolio)**

---

## Pattern Count

Shipped so far (sanitized portfolio samples):

| # | Pattern | Folder |
|---|---------|--------|
| 01 | Matching Engine SCD Type 2 | `sql_patterns/01-matching-engine-scd-type2/` |
| 02 | POS product category prediction | `ml_pipelines/02-product-category-prediction/` |
| 03 | Adyen payment terminal integration | `payment_processing/03-adyen-payment-terminal/` |
| 04 | Multi-country FBO/NBO scoring export | `scoring_analytics/04-dana-scoring/` |
| 05 | Salesforce asset history delta export | `salesforce_integration/05-sfdc-asset-history-export/` |
| 06 | Odoo helpdesk tickets daily event export | `odoo_integration/06-helpdesk-tickets-export/` |
| — | Odoo integration set (01–05) | `odoo_integration/01`–`05` |

Backlog and phase tracking: [`docs/PATTERN_BACKLOG.md`](docs/PATTERN_BACKLOG.md).

Target over time: more domain DAGs, operators, utilities, and SQL patterns drawn from production Composer work — not a vanity count.

---

## Sanitization Approach

All code in this repository has been sanitized following these rules:

### What Was Changed
- **Company/Project Names**: `company_name` → `company`, `project_id` → `dwh_project`
- **Table Names**: `hd-dwh-stream-1.gold.customers` → `dwh_project.gold.customer_dim`
- **Connection IDs**: `bigquery_prod` → `bigquery_default`
- **Credentials**: All removed, using Airflow Variables/Connections instead
- **Emails**: `team@company.com` → `dataops@company.com`
- **Business Logic**: Proprietary scoring formulas generalized

### What Was Preserved
- **Architecture patterns**: The "how" of implementations
- **Error handling**: Production retry and logging patterns
- **Performance optimizations**: BigQuery partitioning, batching strategies
- **Data quality checks**: Validation and reconciliation logic
- **Code structure**: Organization, modularity, best practices

---

## License

MIT License - Feel free to use these patterns in your own projects.

**Note**: These are sanitized examples from production work. All sensitive data, credentials, and proprietary business logic have been removed or anonymized.

---

## Contact

**Vineeth Shyam** | Head of Data Platform  
**LinkedIn**: [linkedin.com/in/vineethshyam](https://www.linkedin.com/in/vineethshyam/)  
**Location**: Munich, Germany

---

<p align="center">
  <i>Production-tested patterns from 450+ Airflow DAGs</i> ✨
</p>
