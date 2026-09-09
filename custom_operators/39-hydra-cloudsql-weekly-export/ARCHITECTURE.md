# Architecture: Hydra Cloud SQL weekly full export

Composer walks `HYDRA_RAW_TABLES` in order. Each table gets a Cloud
SQL Admin CSV export into the raw bucket, then a GCS→BigQuery load
into `dwh_trusted_staging.hyd_v2_<table>` with WRITE_TRUNCATE. After
the last load succeeds, one dbt Cloud job builds `tag:hydra_v2`.

## Diagram

```mermaid
flowchart TB
  subgraph vars [Airflow Variables]
    SQLPROJ["hydra_cloudsql_project"]
    SQLINST["hydra_cloudsql_instance"]
    SQLDB["hydra_cloudsql_database"]
    BUCKET["hydra_raw_bucket"]
    DBT["hydra_v2_dbt_job_id"]
    BACKUP["backup window hours"]
  end

  subgraph mysql [Cloud SQL MySQL]
    T1["countries"]
    T2["users"]
    T3["establishments"]
    TN["… HYDRA_RAW_TABLES"]
  end

  subgraph compose [Composer DAG etl_hydra_job_v2]
    START[start]
    subgraph tg [TaskGroup weekly_full_exports]
      E1[export_countries]
      L1[load_hyd_v2_countries]
      E2[export_users]
      L2[load_hyd_v2_users]
      EN[export_N]
      LN[load_hyd_v2_N]
    end
    DBTJOB[hydra_v2_dbt_job]
    ENDN[end]
  end

  subgraph gcs [GCS rawzone]
    CSV["hydra_raw/table/ds/000000/table.csv"]
    SCHEMA["schema_json/hyd_v2_table.json"]
  end

  subgraph warehouse [Warehouse]
    STG[("dwh_trusted_staging.hyd_v2_*")]
    SNAP[("dbt snapshots / trusted models")]
  end

  SQLPROJ --> E1
  SQLINST --> E1
  SQLDB --> E1
  BACKUP --> E1
  BUCKET --> E1
  BUCKET --> L1
  SCHEMA --> L1

  T1 --> E1 --> CSV --> L1 --> STG
  T2 --> E2
  T3 --> EN
  TN --> EN

  START --> E1 --> L1 --> E2 --> L2 --> EN --> LN
  LN --> DBTJOB --> ENDN
  DBT --> DBTJOB
  STG --> SNAP
  DBTJOB --> SNAP
```

## Components

**cloudsql_export_operator.py**  
`CloudSqlExportOperatorWithRetry` wraps the contrib Cloud SQL export
operator and retries HTTP 409 `operationInProgress` with linear
backoff (`delay * attempt`).  
`CloudSqlExportOperatorWithScheduleAware` doubles that delay when UTC
hour falls in a configured backup window so dumps do not thrash
against automated backups.

**hydra_export_queries.py**  
Table catalog (`HydraRawTable` + `ColumnSpec`), CSV-safe SELECT
generation (`bit` → CAST UNSIGNED, string NULL/quote/newline fixes),
MySQL→BQ load type map, schema JSON writer, and thin dbt staging /
snapshot generators. Sensitive and OAuth table key sets are asserted
out of the active catalog.

**dag_hydra_v2.py**  
Builds the sequential export→load chain inside a TaskGroup, then
fans into dbt. Missing dbt provider or job Variable renders an
EmptyOperator so the reference DAG still imports.

## Design notes

Exports are **serial by design**. Parallelizing Cloud SQL Admin
exports on one instance is how you recreate the 409 storm this
operator exists to absorb.

Historization is **not** in the export. Staging is a full replace;
dbt snapshots own unique keys (MySQL PK, or all columns when no PK).
That keeps the DAG dumb and the warehouse smart.
