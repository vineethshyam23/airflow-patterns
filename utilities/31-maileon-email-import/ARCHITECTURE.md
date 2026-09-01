# Architecture: Maileon email marketing import

Composer orchestrates eight parallel extract→branch→copy|skip→load
chains, then a metadata + dbt tail. The Python client owns XML
pagination and JSONL landing; stock GCS / BigQuery operators move
bytes; dbt Cloud owns trusted transforms.

## Diagram

```mermaid
flowchart TB
  subgraph api [Maileon REST API]
    COUNT["GET /reports/{type}/count"]
    PAGE["GET /reports/{type} XML pages"]
    NAME["GET /mailings/{id}/name"]
    TAGS["GET /mailings/{id}/settings/tags"]
  end

  subgraph compose [Composer DAG etl_maileon_import]
    EXT["get_{report}_report ×8"]
    BR["branch_{report}_file_check"]
    COPY["copy_{report}_to_rawzone"]
    SKIP["skip_{report}_empty_file"]
    LOAD["load_{report}_to_bq"]
    DBT1["dbt_transform_maileon"]
    FN["fetch_maileon_names"]
    LN["load_data_to_bq_names"]
    FT["fetch_maileon_tags"]
    LT["load_data_to_bq_tags"]
    STAGE[stage]
    DBT2["dbt_get_maileon"]
    DBT3["dbt_maileon_api"]
  end

  subgraph storage [Storage]
    LOCAL["Composer data/maileon/{report}/*.jsonl"]
    RAW["rawzone maileon/{date}/{report}.jsonl"]
  end

  subgraph warehouse [Warehouse]
    STG[("trusted_staging.maileon_*")]
    INT[("trusted.int_maileon_*")]
    META[("trusted_staging.maileon_{names,tags}_tbl")]
    REF[("trusted marketing models")]
  end

  COUNT --> PAGE --> EXT
  EXT --> LOCAL --> BR
  BR --> COPY --> RAW --> LOAD --> STG
  BR --> SKIP --> LOAD
  LOAD --> DBT1 --> INT
  DBT1 --> FN
  NAME --> FN --> LN --> META
  LN --> FT
  TAGS --> FT --> LT --> META
  LT --> STAGE --> DBT2 --> DBT3 --> REF
```

## Components

**MaileonAPI (`maileon_api.py`)**  
Basic-auth client. Prefetches `/count`, pages at 1000 rows, parses
XML with xmltodict, maps contact + event fields, writes JSONL under
the Composer data path (synced to the Composer bucket). Prefers the
normalized `records` list from the XML cleaner when present.

**Empty-file branch**  
`BranchPythonOperator` checks Composer blob size. Non-empty →
GCSToGCS into rawzone → GCSToBigQuery. Empty/missing → EmptyOperator
skip, then join on the load task (`none_failed_or_skipped`).

**Metadata enrichment (`get_maileon_metadata.py`)**  
After the first dbt job, UNION distinct `mailing_id` from
`int_maileon_*`, then sequential name lookups. Tags read from the
names staging table. Retries 429/500 with exponential backoff.

**dbt Cloud**  
Three jobs: report transforms, names/tags models, API integration
models. Job IDs come from Variables; missing IDs fall back to
EmptyOperator so the graph still renders in a reference checkout.

## Design notes

Parallel report branches isolate vendor flakiness. WRITE_TRUNCATE
keeps daily staging idempotent for re-runs. Parse-time
`datetime.now()` for report dates is an inherited footgun — prefer
`{{ ds }}` in a real deploy. The skip→load join can truncate staging
on empty days; call that out in ops runbooks if you copy the graph.
