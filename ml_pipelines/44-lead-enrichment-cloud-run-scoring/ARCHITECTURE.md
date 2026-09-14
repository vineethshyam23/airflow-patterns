# Architecture: Lead enrichment + Cloud Run scoring

Composer owns schedule, gate, and graph. dbt Cloud owns enrichment and
post-score models. Cloud Run owns the scoring binary. Pattern 02's
Odoo lead engine owns CRM writes (stubbed here for portfolio parse).

## Diagram

```mermaid
flowchart TB
  subgraph schedule [Schedule]
    CRON["Cron 30 5 * * * UTC"]
  end

  subgraph vars [Airflow Variables]
    DWH["dwh_project / dwh_project_dev"]
    SCOREP["lead_scoring_gcp_project"]
    JOBN["lead_scoring_job_name"]
    DBTIDS["lead_enrichment_dbt_job_id / lead_scoring_dbt_job_id"]
    MTBL["matching_engine_sam_leads_table"]
    FTBL["leads_enrichment_final_table"]
    SLACK["lead_enrichment_slack_*"]
  end

  subgraph upstream [Upstream]
    VERTEX[("matching_engine.sam_leads")]
  end

  subgraph compose [Composer DAG etl_leads_enrichment]
    START[start]
    GATE[check_matching_engine_results]
    SKIP[skip_enrichment]
    DBTE[dbt_lead_enrichment_run]
    CRUN[score_leads Cloud Run]
    DBTS[dbt_lead_scoring_run]
    ODOO[send_leads_enrichment_odoo]
    ALERT[slack_notification_complete]
    ENDN[end]
  end

  subgraph scoring [Scoring project]
    CRJOB["Cloud Run Job lead-scoring-score"]
  end

  subgraph warehouse [Warehouse]
    ENR[("crm_spot.leads_enrichment")]
    FIN[("crm_spot.leads_enrichment_final")]
  end

  subgraph crm [CRM]
    LEADS["Odoo crm.lead"]
  end

  CRON --> START --> GATE
  MTBL --> GATE
  VERTEX --> GATE
  GATE -->|rows today| DBTE
  GATE -->|none / error| SKIP --> ENDN
  DBTIDS --> DBTE
  DBTE --> ENR --> CRUN
  SCOREP --> CRUN
  JOBN --> CRUN
  CRUN --> CRJOB
  CRJOB --> DBTS --> FIN --> ODOO --> LEADS
  ODOO --> ALERT --> ENDN
  SLACK --> ALERT
  DWH --> GATE
  DWH --> ODOO
  FTBL --> ODOO
```

## Components

**matching_engine_gate.py**  
BigQuery `COUNT(*)` for `CURRENT_DATE()` on the matching-engine leads
table. Returns `dbt_lead_enrichment_run` or `skip_enrichment`. Query
exceptions take the skip path.

**dag_lead_enrichment_scoring.py**  
Branch + linear enrichment chain. Cloud Run Execute Job with CLI args
`(product, market, env, logical_date, mode, dry_run)`. dbt and Cloud
Run tasks become EmptyOperators when providers / job-id Variables are
missing so the reference checkout still imports.

**odoo_enrichment_push.py**  
Thin adapter. Production wires pattern 02
`load_data_leads_enrichment` (country/lang/channel refs, product codes,
address hash). Do not duplicate that mapper here.

## Design notes

Cloud Run `overrides.container_overrides[].args` is how product/market
and the logical date reach the scoring image without rebuilding. Keep
the arg order stable; the image CLI is the contract.

Gate task_ids must match BranchPythonOperator return values exactly
(`dbt_lead_enrichment_run`, `skip_enrichment`). Renaming a dbt task
without updating the gate module breaks the branch silently into a
missing downstream.
