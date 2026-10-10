# Architecture: Dining Guide DQ-gated publish

Composer owns the graph. `dining_guide_validation.py` owns Blake3
hashing, comparison SQL, and the fail-closed gate. Destination
projects are JSON in Airflow Variables so the same DAG definition
runs on Composer-dev (single target) and Composer-prod (four envs).

## Diagram

```mermaid
flowchart TB
  subgraph vars [Airflow Variables]
    JOB["dining_guide_dbt_job_id"]
    TGTS["dining_guide_publish_targets"]
    PROD["dining_guide_prod_project"]
    SLACK["dining_guide_slack_conn"]
  end

  subgraph compose [Composer DAG]
    START[start_task]
    DBT[dbt_dining_guide deferrable]
    RUNIDS[get_runids_task]
    HASH[hash_uid Blake3]
    CMP[comparison_results_dining_guide]
    CHK[check_query_task]
    BR[branch_task]
    FAIL[slack_dq_fail_task]
    OK[approve_publish]
    BAK[backup_trusted_previous_push]
    PUB["publish_dining_guide_{dev,acc,stg,prod}"]
    RAT[publish_with_ratings_dev]
  end

  subgraph dwh [DWH project]
    TRUST[("trusted.dining_guide_data_base")]
    HID[("trusted.dining_guide_establishment_id_hash")]
    MON[("monitoring.comparison_results_dining_guide")]
    BAKTBL[("backup_trusted.*_previous_push")]
  end

  subgraph apps [Dining Guide BQ projects]
    DEV[("dining-guide-dev.app_data")]
    ACC[("dining-guide-acc.app_data")]
    STG[("dining-guide-stg.app_data")]
    PRJ[("dining-guide-prod.app_data")]
  end

  JOB --> DBT
  TGTS --> PUB
  PROD --> CMP
  SLACK --> FAIL

  START --> DBT
  DBT --> RUNIDS
  DBT --> HASH --> CMP --> CHK --> BR
  BR -->|fail| FAIL
  BR -->|pass| OK --> BAK
  BAK --> PUB
  BAK --> RAT

  DBT --> TRUST
  HASH --> HID
  TRUST --> CMP
  PRJ --> CMP
  CMP --> MON
  TRUST --> PUB
  HID --> PUB
  BAK --> BAKTBL
  PUB --> DEV
  PUB --> ACC
  PUB --> STG
  PUB --> PRJ
  RAT --> DEV
```

## Components

**dag_dining_guide_dq_gated_publish.py**  
Schedule, dbt operator, branch, backup, and per-env
`BigQueryInsertJobOperator` fan-out. `max_active_runs=1` so a long
dbt+validate cycle cannot overlap the next day.

**dining_guide_validation.py**  
`UidShortener`, hash reload, comparison SQL builder, `any_flag_true`
(fail closed), branch helper, publish SELECT shape.

**sql/dining_guide_with_ratings.sql**  
Stub for the dev-only ratings-enriched table. Production substitutes a
large self-contained establishment query; the stub preserves the load
path without shipping proprietary SQL.

## Design notes

- Comparison always reads **yesterday's prod app project**, not the
  DWH backup. That is the user-visible contract.
- `hash_uid` uses `TriggerRule.ALL_DONE` so run-id tracking failures
  do not block the gate (same as production).
- Env loads stay sequential after backup in this reference; they are
  independent and can be parallelized when runtime matters.
