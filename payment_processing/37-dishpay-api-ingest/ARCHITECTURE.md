# Architecture: Payment wallet API ingest

Composer owns the graph. `payment_api.py` owns OAuth, pagination, and
NDJSON write. Stock GCS / BigQuery operators move bytes. Two dbt Cloud
jobs own trusted models after staging lands (VOP models share the KYC
job tag).

## Diagram

```mermaid
flowchart TB
  subgraph vars [Airflow Variables]
    CREDS["payment_wallet_creds"]
    KYCJOB["payment_wallet_dbt_kyc_job_id"]
    TXNJOB["payment_wallet_dbt_transactions_job_id"]
    VOPBF["payment_vop_backfill_start/end"]
    ENV["env / composer_bucket / gcp project"]
  end

  subgraph wallet [Payment wallet DWH API]
    OAUTH["POST /auth/oauth/token"]
    KYC["POST /api/v1/dwh/KYC"]
    TXN["POST /api/v1/dwh/transaction"]
    CNT["POST /api/v1/dwh/transaction/count"]
    VOP["POST /api/v1/dwh/VOP/performance"]
  end

  subgraph compose [Composer DAG etl_payment_wallet_dbt]
    START[start]
    FETCH_K[data_fetch_payment_kyc]
    FETCH_T[data_fetch_payment_transactions]
    FETCH_V[data_fetch_payment_vop_performance]
    UP[upload_storage_*]
    BR[is_file_empty_*]
    LOAD[load_staging_*]
    S1[stage_1]
    DBTK[payment_kyc_dbt]
    DBTT[payment_transactions_dbt]
    S2[stage_2]
    CHK[check_all_tasks]
    CNTQ[get_loaded_data_count]
    NTF[notify_kyc / transactions / vop]
    ENDN[end]
  end

  subgraph storage [Storage]
    LOCAL["Composer data/payment_wallet/*.json"]
    RAW["rawzone payment-wallet/{feed}/{date}/*.json"]
  end

  subgraph warehouse [Warehouse]
    STG_K[("trusted_staging.payment_kyc TRUNCATE")]
    STG_T[("trusted_staging.payment_transactions APPEND")]
    STG_V[("trusted_staging.payment_vop_performance APPEND")]
    TRUSTED[("trusted via dbt")]
  end

  CREDS --> OAUTH
  OAUTH --> KYC
  OAUTH --> TXN
  OAUTH --> VOP
  CNT --> TXN
  VOPBF --> FETCH_V

  START --> FETCH_K
  START --> FETCH_T
  START --> FETCH_V
  KYC --> FETCH_K
  TXN --> FETCH_T
  VOP --> FETCH_V

  FETCH_K --> LOCAL
  FETCH_T --> LOCAL
  FETCH_V --> LOCAL
  LOCAL --> UP --> RAW --> BR
  BR -->|has data| LOAD
  BR -->|empty| ENDN
  LOAD --> STG_K
  LOAD --> STG_T
  LOAD --> STG_V
  LOAD --> S1
  S1 --> DBTK --> S2
  S1 --> DBTT --> S2
  KYCJOB --> DBTK
  TXNJOB --> DBTT
  ENV --> UP
  ENV --> LOAD
  S2 --> CHK --> CNTQ --> NTF --> ENDN
  STG_K --> TRUSTED
  STG_T --> TRUSTED
  STG_V --> TRUSTED
```

## Components

**payment_api.py**  
`PaymentWalletAPI` handles token fetch, 401/403 refresh, and three
pagination styles. `get_payment_wallet_data` is the Airflow callable
that writes NDJSON and returns `{api_records_count, ...}` for XCom.
`get_loaded_data_count` queries the trusted intermediate for today's
transaction row count (ops reconciliation).

**dag_dishpay_api_ingest.py**  
Fan-out loop over three feeds with shared OAuth kwargs. Branch on empty
rawzone blob before staging load. Parallel dbt jobs after `stage_1`.
Notification tasks run `ALL_DONE` so a single feed failure still
surfaces a Slack summary.

**Staging contract**  
NDJSON lines → GCS → BigQuery single `JSON` column. KYC truncates;
transactions and VOP append. dbt owns the trusted / refined layer
(including VOP models tagged with the KYC job).

## Design notes

Three pagination contracts in one client is intentional. The wallet
API did not ship a uniform list contract; wrapping each feed in its
own DAG would triple OAuth Variable sprawl and notification noise.

Empty-file branching exists because weekends and low-volume markets
legitimately return zero rows. Failing the DAG on empty is worse than
skipping load and still running dbt for the feeds that did land —
though today `no_data_*` goes straight to `end`, so a zero-KYC day
does not block transactions dbt if that feed had data. Read the graph
carefully before changing trigger rules.
