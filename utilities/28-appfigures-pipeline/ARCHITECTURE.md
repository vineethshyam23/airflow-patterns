# Architecture: AppFigures weekly ingest

Composer owns the graph. `fetch_appfigures_data` owns the HTTP + local
CSV write. Stock GCS / BigQuery operators move bytes and rows. dbt
Cloud runs after a fan-in barrier.

## Diagram

```mermaid
flowchart TB
  subgraph api [AppFigures API v2]
    SALES["/reports/sales"]
    RAT["/reports/ratings"]
  end

  subgraph compose [Composer DAG etl_appfigures_pipeline]
    START[start]
    F1[data_fetch_sales]
    F2[data_fetch_ratings]
    F3[data_fetch_ratings_product]
    F4[data_fetch_ratings_country]
    U1[upload_storage_*]
    L1[load_staging_*]
    C1[copy_table_trusted_*]
    STAGE[stage barrier]
    DBT[appfigures_dbt]
    ENDN[end]
  end

  subgraph storage [Storage]
    LOCAL["Composer data/appfigures/*.csv"]
    RAW["rawzone appfigures/{report}/{end_date}/*.csv"]
  end

  subgraph warehouse [Warehouse]
    STG[(trusted_staging.appfigures_*)]
    TRU[(trusted.appfigures_*)]
    DBTJ[dbt Cloud job]
  end

  START --> F1 & F2 & F3 & F4
  SALES --> F1
  RAT --> F2 & F3 & F4
  F1 & F2 & F3 & F4 --> LOCAL
  LOCAL --> U1 --> RAW
  RAW --> L1 --> STG
  STG --> C1 --> TRU
  C1 --> STAGE
  STAGE --> DBT --> DBTJ
  DBT --> ENDN
```

## Components

**fetch_appfigures_data**  
Maps `file_name` → report type + `group_by`, calls AppFigures with a
Bearer token from Airflow Variable, writes CSV under the Composer
data volume. Sanitized version raises on non-200.

**DAG chains**  
For each of four filenames: fetch → GCSToGCS → GCSToBigQuery
(TRUNCATE staging) → BigQueryToBigQuery (APPEND trusted) → `stage`.
`chain(start, …, stage)` fans out from start and fans in at stage.

**dbt step**  
Production used a fixed Cloud job id. Sample reads
`appfigures_dbt_job_id` and falls back to EmptyOperator so the graph
renders without credentials.

## Why four chains instead of one looped load?

Each report has a different schema and `group_by`. Parallel chains
give independent retries and clearer task failure signals in the UI.
A single dynamic-mapped task would be cleaner in Airflow 2.3+, but
the production DAG predated that habit and the parallel layout is
still easy to operate.
