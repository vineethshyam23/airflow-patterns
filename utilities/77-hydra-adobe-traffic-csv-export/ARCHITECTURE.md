# Architecture: Hydra Adobe traffic CSV publish

Composer reads five refined site-traffic tables from BigQuery for a
two-day window, stages CSVs on the Composer bucket, overwrites the
publish bucket for those dates, then cleans staging and applies
60-day retention on the consumer side.

## Diagram

```mermaid
flowchart TB
  subgraph bq [BigQuery refined]
    T1[(site_traffic_mailphoneclicks)]
    T2[(site_traffic_openingtimes)]
    T3[(site_traffic_traffic)]
    T4[(site_traffic_visitsbydevice)]
    T5[(site_traffic_visitsbylocale)]
  end

  subgraph airflow [Cloud Composer]
    START[EmptyOperator start]
    RET[PythonOperator delete_old_files]
    EXT[PythonOperator extract_and_upload_csv_files]
    DEL[PythonOperator delete_target_files]
    CPY[PythonOperator copy_files_to_targets]
    SUM1[PythonOperator generate_copy_summary]
    SUM2[PythonOperator generate_summary_report]
    CLN[PythonOperator cleanup_source_files]
    ENDN[EmptyOperator end ALL_DONE]
  end

  subgraph stage [Composer GCS staging]
    STG["data/site-traffic/{metric}_{YYYY-MM-DD}.csv"]
  end

  subgraph pub [Publish bucket root]
    OUT["{metric}_{YYYY-MM-DD}.csv"]
  end

  START --> RET
  RET --> EXT
  T1 --> EXT
  T2 --> EXT
  T3 --> EXT
  T4 --> EXT
  T5 --> EXT
  EXT --> STG
  EXT --> DEL
  DEL --> CPY
  STG --> CPY
  CPY --> OUT
  CPY --> SUM1
  SUM1 --> SUM2
  SUM2 --> CLN
  CLN --> ENDN
```

## Components

**DAG (`dag_hydra_adobe_traffic_csv.py`)**  
Linear chain. Schedule `0 7 * * *`. `dagrun_timeout=12h`. Extract
timeout 6h; copy 2h; retention/overwrite 1h each. `end` uses
`ALL_DONE` so a mid-chain failure still closes the run for scheduling.

**Helpers (`traffic_csv_export.py`)**  
Table list + short-name mapping, date window, pandas extract with
`d-m-yyyy` date formatting and device-type remaps for visits-by-device,
target delete for the window, GCS copy to bucket root, staging cleanup
gated on successful copy, retention by parsing dates from filenames.

**Config**  
Project, dataset, Composer bucket, staging folder, and publish bucket
come from Airflow Variables with sanitized defaults
(`dwh_project_id`, `refined_dataset`, `composer_bucket`,
`site_traffic_staging_folder`, `site_traffic_publish_bucket`).

## Failure modes

| Mode | Behaviour |
|------|-----------|
| Empty table/date | Logged as `no_data`; other extracts continue |
| Single extract exception | Logged as `error` in results; loop continues |
| Copy failure | Staging file kept; cleanup skips that object |
| Target list/delete flake | Task retries (1 × 5 min); may leave stale objects until next run |
| Publish bucket growth | Full-object list for retention/overwrite slows; monitor object count |
| Module-level date window | Backfill / reparse can miss intended logical dates — known debt |
