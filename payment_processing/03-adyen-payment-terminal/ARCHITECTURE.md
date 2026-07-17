# Architecture: Adyen payment terminal integration

Two halves in one DAG: a read path that materializes Adyen Management API
entities into BigQuery staging, and a write path that reassigns / patches
terminals using a dbt-built match table.

## Diagram

```mermaid
flowchart TB
  subgraph adyen [Adyen Management API v3]
    M["GET /merchants"]
    S["GET /merchants/{id}/stores"]
    T["GET /terminals"]
    TS["GET /terminals/{id}/terminalSettings"]
    RA["POST /terminals/{id}/reassign"]
    PT["PATCH /terminals/{id}/terminalSettings"]
  end

  subgraph composer [Cloud Composer]
    F1[merchant_data_fetch]
    F2[store_data_fetch]
    F3[terminal_data_fetch]
    F4[terminal_settings_data_fetch]
    UP[GCSToGCS per entity]
    LD[GCSToBigQuery staging]
    DBT[dbt Cloud job - match models]
    TG[TaskGroup terminal_management_api]
  end

  subgraph storage [Storage / Warehouse]
    LOCAL["Composer data/adyen/payment_terminal/*.json"]
    RAW[(raw GCS dated prefix)]
    STG[(trusted_staging.adyen_payment_terminal_*)]
    MATCH[(trusted.int_payment_terminal_erp_serial_store_match)]
  end

  M --> F1 --> LOCAL
  F1 --> F2
  S --> F2 --> LOCAL
  T --> F3 --> LOCAL
  F3 --> F4
  TS --> F4 --> LOCAL
  LOCAL --> UP --> RAW --> LD --> STG
  STG --> DBT --> MATCH
  MATCH --> TG
  TG --> RA
  TG --> PT
```

## Components

**AdyenConfig / AdyenManagementClient**  
API key + Basic auth headers, environment-specific base URL
(`management-{test|live}.adyen.com/v3`). One client shared by endpoint helpers.

**Endpoint classes**  
Merchants, stores, terminals, terminal settings, terminal reassign. Each
normalizes the Adyen payload into flat dicts we can JSONL and load without
nested schema pain in staging.

**Extract callables**  
`fetch_*_and_save` write JSONL under the Composer data directory. Merchants
feed stores via XCom. Terminals feed settings. Inventory-assigned terminals
are skipped on settings GET.

**Landing + load**  
Copy Composer objects into the raw zone under a date partition, then
`WRITE_TRUNCATE` into staging tables using schema JSON from the same bucket.

**dbt step**  
Builds the serial↔store match (and related flags like
`to_disable_standalone_tip`). The portfolio DAG stubs the custom Composer
dbt operator; production used `DbtCloudRunJobOperator` with a fixed job id.

**terminal_management_api TaskGroup**  
1. Reassign from BQ rows (`terminal_id`, `store_id`)
2. PATCH default lite settings for flagged terminals (`ALL_DONE` after reassign)

## Why Management API and not webhooks?

We needed a full inventory snapshot for analytics and a deterministic write
path driven by warehouse logic. Webhooks are great for event-driven updates;
they are a poor fit when dbt decides *which* terminals to move after joining
ERP serials. Nightly pull + targeted POST/PATCH kept the control plane in
one place.
