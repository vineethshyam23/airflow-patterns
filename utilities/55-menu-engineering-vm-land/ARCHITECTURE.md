# Architecture: Menu Engineering VM Postgres land

Composer SSHs onto a product GCE VM, exports Dockerised Postgres
tables to CSV, uploads into the product GCS bucket, cleans the VM,
then copies into the DWH rawzone and TRUNCATEs BigQuery staging
before one dbt Cloud job.

## Diagram

```mermaid
flowchart TB
  subgraph vars [Airflow Variables]
    SSH["me_vm_ssh_conn_id"]
    PBUCKET["me_product_bucket"]
    RBUCKET["me_raw_bucket"]
    DWH["me_dwh_project"]
    DBT["me_dbt_job_id"]
  end

  subgraph vm [Product GCE VM]
    DOCKER["Docker: Postgres menu_engineering"]
    CSVDIR["/home/postgres_csv_files/*.csv"]
  end

  subgraph compose [Composer DAG etl_menu_engineering_vm_land]
    START[start]
    PREP[prepare_vm]
    EXP["export_* parallel"]
    UP["upload_product_* parallel"]
    CLEAN[cleanup_vm_after_upload]
    COPY["copy_rawzone_* parallel"]
    LOAD["load_staging_* parallel"]
    DBTJOB[dbt_me_run]
    ENDN[end]
  end

  subgraph productgcs [Product GCS]
    PCSV["menu_engineering/table.csv"]
  end

  subgraph rawzone [DWH rawzone]
    RCSV["menu_engineering/menu_engineering/ds/table.csv"]
    SCHEMA["schema_json/table.json"]
  end

  subgraph warehouse [Warehouse]
    STG[("dwh_trusted_staging.me_*_tbl")]
    TRUST[("dbt trusted models")]
  end

  SSH --> PREP
  SSH --> EXP
  SSH --> UP
  SSH --> CLEAN
  PBUCKET --> UP
  PBUCKET --> COPY
  RBUCKET --> COPY
  RBUCKET --> LOAD
  DWH --> LOAD
  DBT --> DBTJOB
  SCHEMA --> LOAD

  DOCKER -->|"COPY CSV HEADER"| CSVDIR
  CSVDIR --> UP --> PCSV
  PCSV --> COPY --> RCSV --> LOAD --> STG
  STG --> TRUST
  DBTJOB --> TRUST

  START --> PREP --> EXP --> UP --> CLEAN --> COPY --> LOAD --> DBTJOB --> ENDN
```

## Components

**table_catalog.py**  
Trimmed list of Postgres tables plus schema / staging-prefix
constants. Production carries ~30 tables (per-country address
slices, import ledgers, shifts). The catalog is the only place to
add or remove a land without editing SSH command templates.

**dag_menu_engineering_vm_land.py**  
Stage markers + `chain()` fan-out/fan-in. SSHOperator for prepare /
export / product upload / cleanup. GCSToGCS for the IAM boundary.
GCSToBigQuery with explicit schema objects and WRITE_TRUNCATE.
dbt Cloud job or EmptyOperator stub when the Variable / provider is
missing.

## Design notes

**Why dual-bucket instead of one hop.** The VM service account
already writes the product bucket. Composer already writes the DWH
rawzone. Crossing those two concerns on one SA creates a permanent
exception in both teams' IAM reviews. The extra GCS copy is cheap
compared to that argument.

**Why cleanup sits after product upload, before rawzone copy.**
CSV deletion is safe once the product bucket has the object. If the
rawzone copy fails, Composer retries from GCS — it does not need
the VM disk. Putting cleanup later would leave large CSVs on the VM
whenever BQ load flakes.

**Parallelism.** Exports and uploads fan out per table. That is
fine on SSH+docker for this workload size; Cloud SQL Admin 409
storms do not apply. Still keep `max_active_runs=1` so two DAG runs
do not share the same CSV filenames.

**Schema JSON.** Autodetect is off. Menu-engineering free-text and
quoted newlines need `allow_quoted_newlines=True` and a reviewed
schema object; autodetection has burned us on type flips.
