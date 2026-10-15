# Architecture: Wholesale NL dual-source land

Composer owns scheduling and fan-out. `makro_customers_api.py` owns
OAuth2, pagination, and CHD CSV clean/validate. Stock GCS / BigQuery
operators move bytes. Two dbt Cloud jobs (customer + CHD) own trusted /
discovery models after staging lands.

Three chains share a DAG id but have no cross-branch edges. That is the
point: optional market files must not block the daily customer mutation
pull, and the reverse.

## Diagram

```mermaid
flowchart TB
  subgraph vars [Airflow Variables]
    AUTH["wholesale_nl_mcc_user / password + OAuth client"]
    URLS["customer + merge-request URLs"]
    DBTIDS["customer dbt job + CHD dbt job"]
    ENV["env / rawzone / composer_bucket / chd_landing"]
  end

  subgraph partner [Partner MCC API]
    OAUTH["OAuth2 password grant"]
    CUST["GET customers last_mutday_from + offset"]
    MERGE["GET merge-requests"]
  end

  subgraph landing [CHD landing zone]
    CSV["WHOLESALE_NL_*.csv"]
    PROC["processed/ archive"]
  end

  subgraph compose [Composer DAG etl_wholesale_nl_dual_source]
    F1[data_fetch_customer_base]
    U1[upload_storage_customer_base]
    L1[load_data_wholesale_customers]
    D1[wholesale_customer_dbt]
    R1[get_runids_task_wholesale_nl]

    F2[data_fetch_merge_requests]
    U2[upload_storage_merge_requests]
    L2[load_data_merge_requests]

    SC[check_file ShortCircuit]
    L3[load_gcs_to_bq]
    MV[move_files_chd]
    D3[chd_market_data_NL_dbt]
    R3[get_runids_task_chd]
  end

  subgraph storage [Storage]
    LOCAL["Composer data/wholesale/customer_NL/"]
    RAW["rawzone wholesale/customer_base_NL/ds/"]
  end

  subgraph warehouse [Warehouse staging]
    STG1[("trusted_staging.wholesale_customer_NL JSON value")]
    STG2[("trusted_staging.wholesale_customer_NL_merge_requests")]
    STG3[("trusted_staging.chd_market_data_NL ~90 cols")]
  end

  AUTH --> OAUTH
  OAUTH --> CUST
  OAUTH --> MERGE
  URLS --> F1
  URLS --> F2
  CUST --> F1 --> LOCAL --> U1 --> RAW --> L1 --> STG1 --> D1 --> R1
  MERGE --> F2 --> LOCAL --> U2 --> RAW --> L2 --> STG2
  CSV --> SC --> L3 --> STG3 --> MV --> PROC
  L3 --> D3 --> R3
  DBTIDS --> D1
  DBTIDS --> D3
  ENV --> U1
  ENV --> L1
  ENV --> L3
```

## Components

**makro_customers_api.py**  
Inbound helpers only: `callAPI` (Bearer + 401 retry), `getdata`
(10k-row offset pagination to JSONL), `getdata_merge_requests`,
`check_file` (schema-driven numeric clean + ShortCircuit bool), and
`schema_fields` for the CHD load. Reverse HubSpot POST helpers are
deliberately absent.

**dag_makro_nl_dual_source.py**  
DEV/PROD switch for project, rawzone, schedule. Customer JSON loads as
CSV/tab into a single JSON column (production contract). CHD uses
explicit schema, `max_bad_records=0`, jagged rows + quoted newlines.
dbt operators degrade to EmptyOperator when the provider or job id is
missing so the reference checkout still imports.

## Design notes

Opaque JSON staging for the customer base is a deliberate tradeoff.
Partner payloads change fields without notice; failing the load on
schema drift would page onboarding more often than it protects
downstream. CHD is the opposite contract — wide typed CSV with zero
bad records — so we clean integers in place first.

`trigger_rule=all_done` on the customer BigQuery load lets the customer
→ dbt chain proceed even if the merge-request branch failed. Watch both
branches in alerting; a single end marker waiting on all three chains
would be a useful follow-up.
