# Architecture: Jira Service Desk ingest

Composer owns the graph. `jira_client.py` owns JQL, ADF flattening,
pagination, and rate-limit backoff. Stock GCS / BigQuery operators move
bytes. dbt Cloud owns trusted models and dedupe.

## Diagram

```mermaid
flowchart TB
  subgraph vars [Airflow Variables]
    CREDS["jira_service_desk_creds"]
    KEYS["jira_project_keys"]
    DBT["jira_dbt_job_id"]
    ENV["env / composer_bucket / gcp project"]
  end

  subgraph jira [Jira Cloud REST]
    JQL["/rest/api/3/search/jql"]
    COUNT["/search/approximate-count"]
    PROBE["date-range probe ORDER BY created/updated"]
  end

  subgraph compose [Composer DAG etl_jira_import]
    START[start]
    INC["extract_jira_incremental_{project}"]
    TG["TaskGroup extract_jira_monthly_{project}"]
    MERGE["merge_monthly_files_{project}"]
    UP["upload_storage_{project}"]
    LOAD["load_staging_{project}"]
    PAUSE[pause]
    DBTJOB[jira_dbt_job]
    RUNIDS[get_run_ids]
    ENDN[end]
  end

  subgraph storage [Storage]
    LOCAL["Composer data/jira_{project}/*.jsonl"]
    RAW["rawzone jira_{project}/*.jsonl"]
  end

  subgraph warehouse [Warehouse]
    STG[("trusted_staging.jira_{project} JSON col")]
    TRUSTED[("trusted jira models via dbt")]
  end

  CREDS --> INC
  CREDS --> TG
  KEYS --> START
  START --> INC
  START --> TG
  PROBE --> TG
  TG --> MERGE
  JQL --> INC
  JQL --> TG
  COUNT --> INC
  INC --> LOCAL
  MERGE --> LOCAL
  LOCAL --> UP --> RAW --> LOAD --> STG
  LOAD --> PAUSE --> DBTJOB --> RUNIDS --> ENDN
  DBT --> DBTJOB
  STG --> TRUSTED
```

## Components

**jira_client.py**  
Runtime Variable for username/token. Recursive ADF walker for
description + comments. `nextPageToken` pagination with 429/5xx
backoff. Checkpoint JSONL every 10k issues on long pulls.
`get_jira_project_date_range` is the parse-time probe used only when
`FULL_LOAD_MODE` is True.

**dag_jira_ingest.py**  
Loops configured project keys. Incremental tasks bind
`data_interval_start` → `data_interval_end`. Full-load builds monthly
PythonOperators inside a TaskGroup, then merges shards before the
shared upload/load tail. dbt job id comes from a Variable; missing id
renders an EmptyOperator so the graph still opens in a reference
checkout.

**Staging contract**  
JSONL → GCS → BigQuery CSV load with tab delimiter and a single JSON
`value` column, WRITE_APPEND. dbt is responsible for exploding fields
and deduping on issue key + updated timestamp.

## Design notes

Parse-time date probes mean FULL_LOAD_MODE can fail DAG import if Jira
is down or the token is expired — that is intentional for a backfill
switch you flip by hand, not for the default schedule. Keep production
on `FULL_LOAD_MODE = False`.

Landing `*all` fields is deliberate schema debt. Support projects add
custom fields monthly; a rigid BQ schema would break the load more
often than JSON staging costs us in bytes.
