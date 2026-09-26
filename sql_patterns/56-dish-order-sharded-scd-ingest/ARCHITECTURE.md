# Architecture: multi-shard food-ordering Cloud SQL → BigQuery SCD2

Composer owns schedule and the task graph. Bash scripts on the
Composer data folder own Cloud SQL CSV export and shard merge. GCS
holds export staging + raw-zone objects. BigQuery owns staging
truncate, tmp snapshot, insert-new / expire-old SCD steps, and the
final trusted promote — pinned to the night-ETL reservation.

## Diagram

```mermaid
flowchart TB
  subgraph source [Food-ordering Cloud SQL]
    MASTER[(Master MySQL<br/>clients / countries / users / flavours)]
    S1[(Shard A<br/>tenant DBs)]
    S2[(Shard B<br/>tenant DBs)]
    SN[(Shard N<br/>tenant DBs)]
  end

  subgraph composer [Cloud Composer daily 00:30 UTC]
    GET[getdbs<br/>tenant→shard CSV]
    F1[getdbs_shard_A]
    F2[getdbs_shard_B]
    FN[getdbs_shard_N]
    E1[export_shard_A]
    E2[export_shard_B]
    EN[export_shard_N]
    EM[export_master]
    MERGE[mergefiles<br/>concat shard CSVs]
    GET --> F1 --> E1
    GET --> F2 --> E2
    GET --> FN --> EN
    E1 --> EM
    E2 --> EM
    EN --> EM
    EM --> MERGE
  end

  subgraph gcs [Object storage]
    EXP[export bucket<br/>master CSVs + shard fragments]
    RAW[raw zone<br/>foodorder/table/ds/table.csv]
  end

  subgraph bq [BigQuery per table]
    TMP[(trusted_staging.tmp_order_*)]
    STG[(trusted_staging.order_*)]
    INS[INSERT new hash pairs]
    EXP_ROW[UPDATE expire missing hashes]
    TRU[(trusted.order_*)]
  end

  MASTER --> GET
  MASTER --> EM
  S1 --> E1
  S2 --> E2
  SN --> EN
  E1 --> EXP
  E2 --> EXP
  EN --> EXP
  EM --> EXP
  MERGE --> RAW
  EXP --> RAW
  RAW --> STG
  TRU --> TMP
  STG --> INS
  TMP --> INS
  INS --> EXP_ROW
  STG --> EXP_ROW
  EXP_ROW --> TRU
```

Per-table chain after merge:

`download_or_skip → snapshot_tmp → load_staging → insert → expire → promote → wait_for_files → clean`

Master tables take a `GCSToGCS` hop from the export bucket into the
raw zone. Sharded tables are already written there by `mergefiles`,
so the download task is an `EmptyOperator`.

## Components

**shard_config.SHARD_INSTANCES**  
Instance name → private IP. Production held ~44 entries; this sample
keeps four so the fan-out is obvious without a wall of task ids.

**getdbs.sh + getdbs_{instance}**  
Master export of `ti_clients ⨝ ti_server_instances` →
`foodorder_dbs.csv`. Each shard task greps its IP and writes a
per-instance tenant list the export script consumes.

**dishordersplitted.sh / dishorder.sh**  
`gcloud sql export csv` with SELECTs that stamp `_keyhash`,
`_rowhash`, `_create_ts`, `_job_name`, `_sourcesystem`. Shard script
walks tenant DBs; master script exports the four reference tables
once.

**dishordermerge.sh**  
Concatenates shard fragments (header once) into
`gs://{raw}/foodorder/{table}/{ds}/{table}.csv` via `gsutil -m`.

**ReservedBigQueryInsertJobOperator**  
Same SCD insert / expire / promote shape as Offer Tool (#27), but
query jobs are pinned to the DWH reservation so the overnight wave
does not burn on-demand slot budget.

## Why bash fan-out instead of one giant operator?

Cloud SQL Admin export is per-instance. Parallel BashOperators map
1:1 onto shards, give you clear retry boundaries when one shard
blows a 409, and keep the merge step as a single barrier before any
BigQuery load starts. A TaskGroup rewrite would clean the UI; it
would not change the export physics.
