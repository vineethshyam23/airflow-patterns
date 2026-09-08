# Architecture: POS vendor GA4 rolling event ingest

Composer fans out seven day-scoped BigQuery jobs against the native
GA4 export dataset, then sequences a Data Transfer kick, one dbt Cloud
job, and run-id capture. Staging is a dedicated temp dataset next to
the GA4 property export — not the main trusted zone.

## Diagram

```mermaid
flowchart TB
  subgraph vars [Airflow Variables]
    PROP["booq_ga4_property_id"]
    XFER["booq_ga4_transfer_config"]
    WAIT["booq_ga4_transfer_wait_seconds"]
    DBT["booq_ga4_dbt_job_id"]
    ENV["env / GCP project"]
  end

  subgraph ga4 [GA4 BigQuery export]
    SHARDS["analytics_PROPERTY.events_YYYYMMDD"]
  end

  subgraph compose [Composer DAG etl_booq_google_analytics]
    D1[booq_ga_events_1]
    D2[booq_ga_events_2]
    D3[booq_ga_events_3]
    D4[booq_ga_events_4]
    D5[booq_ga_events_5]
    D6[booq_ga_events_6]
    D7[booq_ga_events_7]
    XFER_TASK[run_Data_transfers]
    DBTJOB[dbt_ga4]
    RUNIDS[get_runids_task]
  end

  subgraph bqts [BigQuery Data Transfer]
    CFG["manual transfer config Europe"]
  end

  subgraph warehouse [Warehouse]
    STG[("analytics_PROPERTY_temp.pos_vendor_ga_events")]
    TRUSTED[("trusted GA4 models via dbt")]
  end

  PROP --> SHARDS
  PROP --> STG
  ENV --> D1
  ENV --> D2
  SHARDS --> D1 & D2 & D3 & D4 & D5 & D6 & D7
  D1 & D2 & D3 & D4 & D5 & D6 & D7 --> XFER_TASK
  XFER --> XFER_TASK
  WAIT --> XFER_TASK
  XFER_TASK --> CFG
  XFER_TASK --> DBTJOB
  DBT --> DBTJOB
  DBTJOB --> RUNIDS
  D1 & D2 & D3 & D4 & D5 & D6 & D7 --> STG
  STG --> TRUSTED
  CFG --> TRUSTED
```

## Components

**dag_booq_ga4.py**  
Builds the seven `BigQueryInsertJobOperator` tasks in a loop
(`day_offset` 1..7), wires them into transfer → dbt → runids, and
switches GCP project / connection on DEV vs PROD. dbt job id and GA4
property id come from Variables; missing dbt provider or job id
renders an EmptyOperator so the graph still imports in a reference
checkout.

**ga4_transfer.py**  
Starts a manual BigQuery Data Transfer run from a Variable-held
config resource name, sleeps a configurable wait (default 360s), and
logs the start response without asserting SUCCESS. Also holds the
shared dbt run-id collector used by `get_runids_task`.

**Staging contract**  
Per day: DELETE rows for that `event_date`, then INSERT the full GA4
event column set from `events_*` filtered on `_TABLE_SUFFIX`. Nested
structs (`event_params`, `user_properties`, `ecommerce`, `items`) are
preserved as exported.

## Design notes

Seven tasks instead of one BETWEEN window matches what ran in
production and keeps per-day retries isolated. Consolidating to a
single query is a reasonable rewrite if you also move dates to
`{{ ds }}` macros.

The transfer sleep is intentional technical debt. Polling
`TransferRun.state` would remove the magic number and catch failures
before dbt starts; do that before you shorten `dagrun_timeout` further.
