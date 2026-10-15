# Architecture: Midday POS customer-master refresh

Composer owns schedule and graph. `customer_master_load.py` owns
blob selection and staging load. dbt Cloud owns trusted/refined
models after staging lands. A final BigQuery job materializes the
customer-base view for BI extracts.

## Diagram

```mermaid
flowchart TB
  subgraph schedule [Schedule]
    CRON["CronTriggerTimetable 0 13 Europe/Amsterdam"]
  end

  subgraph vars [Airflow Variables]
    BUCKET["pos_vendor_drop_bucket"]
    PROJ["dwh_project"]
    DBTIDS["pos_afternoon_dbt_*_job_id"]
    SLACK["pos_afternoon_slack_*"]
  end

  subgraph dropzone [Vendor GCS drop zone]
    DEBT["Vendor-Debtor/*_YYYYMMDDT*"]
    LOC["Vendor-DebLoc/*_YYYYMMDDT*"]
  end

  subgraph compose [Composer DAG etl_pos_afternoon_customer_refresh]
    L1[load_vendor_debtor]
    L2[load_vendor_location]
    D1[dbt_vendor_customer]
    D2[dbt_pos_matching_ids]
    D3[dbt_pos]
    D4[dbt_pos_tableau]
    MAT[materialize_customer_base]
    ENDN[end_task]
  end

  subgraph warehouse [Warehouse]
    STG1[("trusted_staging.vendor_debtor_stg")]
    STG2[("trusted_staging.vendor_location_stg")]
    VIEW["refined.vw_vendor_customer_base"]
    TBL[("refined.vendor_customer_base")]
  end

  CRON --> L1
  CRON --> L2
  BUCKET --> L1
  BUCKET --> L2
  DEBT --> L1 --> STG1
  LOC --> L2 --> STG2
  STG1 --> D1
  STG2 --> D1
  DBTIDS --> D1
  D1 --> D2 --> D3 --> D4 --> MAT --> ENDN
  VIEW --> MAT --> TBL
  SLACK -.->|on_failure_callback| L1
  SLACK -.->|on_failure_callback| D1
  PROJ --> MAT
```

## Components

**customer_master_load.py**  
Resolves the newest same-day blob per table prefix, then TRUNCATE-loads
semicolon CSV into `*_stg` with an explicit schema. Soft-skips when no
matching object exists.

**dag_pos_afternoon_refresh.py**  
Timezone-aware timetable, two parallel load tasks fanning into one dbt
chain, view → table materialize, Slack failure callback. dbt tasks
become EmptyOperators when the Cloud provider or job-id Variable is
missing so the reference checkout still imports.

**Overnight boundary**  
Full-table SCD / hash merge lives in the overnight POS DAG and dbt
models. Afternoon does not re-implement Type 2 SQL; it refreshes the
two customer-master staging tables the shared models already read.

## Design notes

Both load tasks wire `>> dbt_vendor_customer`. Airflow treats that as
a fan-in: dbt starts once after both loads finish (or soft-skip). Do
not put the dbt chain inside the for-loop as a duplicated linear
edge set if you rewrite — keep one shared chain.

`DATE_TOKEN` is evaluated at DAG parse time, same as production. That
is fine for a daily Composer environment that re-parses overnight; it
is wrong for historical backfills. If you need backfills, thread
`{{ ds_nodash }}` into the callable and align the vendor object-name
convention.
