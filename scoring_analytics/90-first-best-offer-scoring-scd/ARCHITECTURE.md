# Architecture: First Best Offer scoring SCD

Weekly Composer DAG that boots a scoring VM, runs two predict jobs in
parallel over SSH, then historises each population into refined with
hash-based SCD Type 2.

## Diagram

```mermaid
flowchart TB
  subgraph composer [Cloud Composer — Monday 02:00 UTC]
    START[ComputeEngineStartInstanceOperator<br/>start_vm]
    PRED_W[BashOperator SSH<br/>predict wholesale]
    PRED_I[BashOperator SSH<br/>predict independent]
    STOP[ComputeEngineStopInstanceOperator<br/>stop_vm all_done]

    STG_W[BQ stage wholesale<br/>WRITE_TRUNCATE staging]
    EXP_W[BQ expire wholesale hist]
    INS_W[BQ insert wholesale hist<br/>WRITE_APPEND]

    STG_I[BQ stage independent<br/>WRITE_TRUNCATE staging]
    EXP_I[BQ expire independent hist]
    INS_I[BQ insert independent hist<br/>WRITE_APPEND]
  end

  subgraph gce [GCE scoring VM]
    VM[(first-best-offer VM)]
    MODEL[predict_fbo.py<br/>wholesale + independent models]
  end

  subgraph bq [BigQuery]
    SRC_W[(refined.fbo_wholesale_score)]
    SRC_I[(refined.fbo_independent_score)]
    TMP_W[(trusted_staging.tmp_fbo_wholesale_score)]
    TMP_I[(trusted_staging.tmp_fbo_independent_score)]
    HIST_W[(refined.fbo_wholesale_score_historized)]
    HIST_I[(refined.fbo_independent_score_historized)]
  end

  subgraph down [Downstream]
    P04[Pattern 04 — FBO/NBO partner export]
    BI[Account recommendation reports]
  end

  START --> VM
  START --> PRED_W
  START --> PRED_I
  PRED_W --> MODEL
  PRED_I --> MODEL
  MODEL --> SRC_W
  MODEL --> SRC_I
  PRED_W --> STOP
  PRED_I --> STOP
  PRED_W --> STG_W --> EXP_W --> INS_W
  PRED_I --> STG_I --> EXP_I --> INS_I
  SRC_W --> STG_W
  SRC_I --> STG_I
  STG_W --> TMP_W
  STG_I --> TMP_I
  EXP_W --> HIST_W
  INS_W --> HIST_W
  EXP_I --> HIST_I
  INS_I --> HIST_I
  HIST_W --> P04
  HIST_I --> P04
  HIST_W --> BI
  HIST_I --> BI
```

## Components

**VM lifecycle**  
`ComputeEngineStartInstanceOperator` / `StopInstanceOperator` around
the predict window. Zone, project, and VM name are Airflow Variables
(`fbo_gce_*`). Stop uses `TriggerRule.ALL_DONE`.

**Predict (BashOperator + SSH)**  
Composer copies an SSH private key from the Composer data bucket, then
SSHs into the VM and runs `predict_fbo.py` with `--model=wholesale` or
`--model=independent`. PYTHONPATH + venv activation happen on the
remote side. The model code itself is not shipped in this folder.

**SCD SQL (`fbo_scd_queries.py`)**  
Three builders per arm: `stage_sql`, `expire_sql`, `insert_sql`. Arms
differ only by key column and table names (`FboScoreArm` dataclass).

| Column | Role |
|--------|------|
| `_keyhash` | MD5 of business key (account or establishment id) |
| `_rowhash` | MD5 of full row JSON — content identity |
| `_valid_from` / `_valid_until` | Version window; open end = `2099-12-31` |
| `_valid_flag` | Fast filter for current version |
| `_job_name` / `_sourcesystem` | Lineage (`etl_first_best_offer` / `scoring`) |

**Config**  
No partner OAuth, no GCS land, no dbt. BigQuery via `bigquery_default`
(Variable `dwh_gcp_conn`). Datasets default to `refined` /
`trusted_staging`.

## Failure modes

| Mode | Behaviour |
|------|-----------|
| VM start fails | Both predicts skip; stop still runs (`all_done`); no SCD |
| One predict fails | Other arm can still historize; stop still runs |
| Stage `all_done` after failed predict | May historize stale score table — known footgun |
| SSH key missing in GCS | Predict BashOperator fails |
| Schema drift on score table | Mass expire+insert (rowhash covers whole row) |
| Overlapping Monday runs | Blocked by `max_active_runs=1` (added in sanitization) |
