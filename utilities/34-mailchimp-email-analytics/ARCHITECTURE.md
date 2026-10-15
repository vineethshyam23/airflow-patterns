# Architecture: Mailchimp email analytics ingest

Composer runs a two-phase DAG. Phase 1 is a serial extract chain on
the Mailchimp Marketing SDK. Phase 2 fans six land+load chains from
a pause barrier into GCS rawzone and BigQuery.

## Diagram

```mermaid
flowchart TB
  subgraph api [Mailchimp Marketing API]
    CL["GET /campaigns paginated"]
    CR["GET /reports/{id}"]
    CK["GET /reports/{id}/click-details"]
    US["GET /reports/{id}/unsubscribed"]
    EA["GET /reports/{id}/email-activity"]
    RC["GET /reports/{id}/sent-to"]
  end

  subgraph compose [Composer DAG etl_mailchimp]
    FCL[campaign_list_fetch]
    FCR[campaign_report_fetch]
    FCK[click_report_fetch]
    FUS[unsubscribes_fetch]
    FEA[email_activity_fetch]
    FRC[recipients_fetch]
    PAUSE[pause]
    UP["upload_storage_{entity} ×6"]
    LD["load_staging_{entity} ×6"]
    CP["copy_table_to_trusted ×6"]
  end

  subgraph storage [Storage]
    LOCAL["Composer data/mailchimp/{entity}/*.json"]
    RAW["rawzone mailchimp/{entity}/{date}/"]
  end

  subgraph warehouse [Warehouse]
    STG[("trusted_staging.mailchimp_* APPEND")]
    TRU[("trusted.mailchimp_* TRUNCATE")]
    LOOKUP["90-day campaign_id lookup"]
  end

  CL --> FCL --> LOCAL
  FCL --> FCR
  LOOKUP --> FCR
  LOOKUP --> FCK
  LOOKUP --> FUS
  LOOKUP --> FEA
  LOOKUP --> FRC
  CR --> FCR
  CK --> FCK
  US --> FUS
  EA --> FEA
  RC --> FRC
  FCR --> FCK --> FUS --> FEA --> FRC --> PAUSE
  PAUSE --> UP --> RAW --> LD --> STG --> CP --> TRU
  STG -.-> LOOKUP
```

## Components

**MailChimp base (`mailchimp_api.py`)**  
Configures the official `mailchimp_marketing` client (api_key +
server prefix). `_query_results` selects distinct campaign IDs from
staging with `DATE(sent_time) >= CURRENT_DATE() - 90`.

**Entity extractors**  
Six subclasses flatten nested API payloads into one JSON object per
row and write JSONL. Campaign list pages at 100; click / unsubscribe
/ activity / recipients page at 1000. Campaign-scoped fetchers wrap
each ID in a 10-attempt retry loop so one flaky campaign does not
abort the whole grain.

**DAG (`dag_mailchimp_pipeline.py`)**  
Serial PythonOperators into `pause`, then a Python `for` loop builds
six parallel GCSToGCS → GCSToBigQuery (APPEND, day partition) →
BigQueryToBigQuery (TRUNCATE into trusted) chains that all join on
`end`.

## Design notes

Serial extracts keep API pressure predictable and match production
ordering. Parallel land/load after pause isolates GCS/BQ failures
per grain without re-pulling the Marketing API. Staging APPEND plus
trusted TRUNCATE means trusted always mirrors the full staging
history for that table — plan a retention / partition expiry job if
you copy this graph, or switch staging to TRUNCATE per day like
pattern 31.
