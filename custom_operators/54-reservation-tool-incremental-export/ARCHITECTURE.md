# Architecture: Reservation Tool incremental Cloud SQL export

Composer reads high-water marks from BigQuery staging, exports each
MySQL table serially through Cloud SQL Admin, loads CSV into
`dwh_trusted_staging.rt_*`, then runs one dbt Cloud job. Incremental
tables APPEND; full-load tables TRUNCATE. Sunday forces a full
baseline for the incremental set.

## Diagram

```mermaid
flowchart TB
  subgraph vars [Airflow Variables]
    SQLPROJ["rt_cloudsql_project"]
    SQLINST["rt_cloudsql_instance"]
    SQLDB["rt_cloudsql_database"]
    BUCKET["rt_raw_bucket"]
    DBT["rt_dbt_job_id"]
    DWH["rt_dwh_project"]
  end

  subgraph mysql [Cloud SQL MySQL]
    INC["INCREMENTAL: reservations, customers, …"]
    FULL["FULL_LOAD: junctions, tenants, …"]
  end

  subgraph compose [Composer DAG etl_reservationtool_v2]
    START[start]
    MAXIDS["get_max_ids\nMAX id or Sunday truncate"]
    E1[export_A]
    L1[load_A]
    E2[export_B]
    L2[load_B]
    EN[export_N]
    LN[load_N]
    GATE[all_loaded]
    DBTJOB[dbt_rt_run]
    ENDN[end]
  end

  subgraph gcs [GCS rawzone]
    CSV["reservationtool/table/ds/000000/table.csv"]
    SCHEMA["schema_json/rt_table.json"]
  end

  subgraph warehouse [Warehouse]
    STG[("dwh_trusted_staging.rt_*")]
    TRUST[("dbt: mask + SCD + restricted views")]
  end

  DWH --> MAXIDS
  SQLPROJ --> E1
  SQLINST --> E1
  SQLDB --> E1
  BUCKET --> E1
  BUCKET --> L1
  SCHEMA --> L1
  DBT --> DBTJOB

  INC --> E1
  FULL --> EN
  MAXIDS -->|"xcom max_id"| E1

  START --> MAXIDS --> E1 --> L1
  E1 --> E2 --> EN
  E2 --> L2
  EN --> LN
  L1 --> GATE
  L2 --> GATE
  LN --> GATE
  GATE --> DBTJOB --> ENDN

  E1 --> CSV --> L1 --> STG
  STG --> TRUST
  DBTJOB --> TRUST
```

## Components

**rt_table_config.py**  
Splits the catalog into `INCREMENTAL_TABLES` and `FULL_LOAD_TABLES`,
builds CSV-safe SELECTs (`IFNULL`, CAST UNSIGNED for bit flags,
newline/quote sanitisation for free text), and keeps credential
columns in `EXCLUDE_COLUMNS` out of the export list.

**cloudsql_export_operator.py**  
`CloudSqlExportOperatorWithRetry` retries HTTP 409
`operationInProgress` with linear backoff.  
`CloudSqlExportOperatorWithScheduleAware` doubles that delay inside
a configured UTC backup window. Same family as pattern 39.

**dag_reservationtool_v2.py**  
`get_max_ids` pushes per-table watermarks (or Sunday truncate +
`max_id=0`). Builds the serial export chain with parallel loads,
gates on `all_loaded`, then runs dbt (or an EmptyOperator stub when
the job Variable / provider is missing).

## Design notes

Exports are **serial by design**. Parallelizing Cloud SQL Admin
exports on one instance recreates the 409 storm the operator exists
to absorb. Loads are the only fan-out.

Historization is **not** in the export. Daily APPEND assumes
auto_increment growth; Sunday TRUNCATE+reload plus dbt snapshots
cover deletes and late corrections. That is the opposite tradeoff
from Hydra (always full) and from Offer Tool SCD (merge in-DAG).

Jinja in the export `selectQuery` pulls the XCom watermark:

```text
… WHERE id > {{ (ti.xcom_pull(task_ids='get_max_ids', key='<table>_max_id') or 0) | int }}
```

so the operator body stays templated without a custom render hook.
