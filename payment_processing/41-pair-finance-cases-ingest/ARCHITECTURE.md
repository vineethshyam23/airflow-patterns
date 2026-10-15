# Architecture: Collections partner case ingest

Composer DAG that fans out five market TaskGroups against a partner
collections API, lands NDJSON on the Composer bucket, loads a typed
BigQuery staging table, and optionally triggers dbt Cloud.

## Diagram

```mermaid
flowchart TB
  subgraph partner [Collections partner API]
    AT_API[AT case_files]
    DE_API[DE case_files]
    FR_API[FR case_files]
    ES_API[ES case_files]
    IT_API[IT case_files]
  end

  subgraph secrets [Secret Manager]
    KEYS["collections-{market}-api-key"]
  end

  subgraph airflow [Cloud Composer]
    TG[TaskGroup per market]
    EXT[extract_cases]
    GCS[load_gcs]
    GATE{ShortCircuit has_records}
    STAGE[GCSToBigQuery stage_bq]
    ALL[all_markets_loaded ALL_DONE]
    DBT_GATE{ShortCircuit dbt_job_configured}
    DBT[DbtCloudRunJobOperator]
  end

  subgraph gcp [DWH project]
    BUCKET[(composer-data / pair-finance/ds/MARKET)]
    STG[(trusted_staging.pair_finance_cases_raw)]
    REF[(refined.pair_finance_cases)]
  end

  KEYS --> EXT
  AT_API --> EXT
  DE_API --> EXT
  FR_API --> EXT
  ES_API --> EXT
  IT_API --> EXT
  EXT --> GCS
  GCS --> GATE
  GATE -->|records| STAGE
  EXT --> BUCKET
  STAGE --> STG
  TG --> ALL
  ALL --> DBT_GATE
  DBT_GATE --> DBT
  DBT --> REF
  STG --> REF
```

## Components

**API connector (`pair_finance_api.py`)**  
Bearer auth, paginated `list_cases` (from/amount, 500/page), optional
`get_case` detail fetch, `flatten_case` to STRING-safe rows,
`cases_to_ndjson`. Retries on 429/5xx with exponential backoff.
`MARKET_CONFIG` holds base URL and merchant per market.

**Pipeline helpers (`pair_finance_pipeline.py`)**  
`resolve_env`, Secret Manager + Variable key resolution,
`gcs_object_name`, idempotent `extract_cases`, optional GCS copy
`load_gcs`, `has_records` gate, explicit `STAGING_SCHEMA_FIELDS`,
`raw_bucket_name` (default `composer-data` in PROD).

**DAG (`dag_pair_finance_cases.py`)**  
Five TaskGroups driven by `MARKET_CONFIG` keys. `all_markets_loaded`
uses `TriggerRule.ALL_DONE` so skipped markets do not block the join.
dbt job id defaults empty; ShortCircuit skips the operator until a
Variable is set. `dbt_poll_interval` is inlined (no package import).

## Failure modes

| Mode | Behaviour |
|------|-----------|
| Missing API key | Skip extract for that market |
| Object already landed | Skip extract unless full load |
| Zero cases | `has_records` ShortCircuit skips `stage_bq` only |
| Empty dbt job id | ShortCircuit skips dbt; `end` still runs via ALL_DONE |
| Schema drift | Raw JSON retained; fix in dbt staging model |
