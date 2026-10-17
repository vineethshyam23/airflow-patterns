# Architecture: Mailchimp campaign analytics integration

Read-only extract from the Mailchimp Marketing API into BigQuery via
Composer local disk and GCS raw zone. No write-back to Mailchimp.

## Diagram

```mermaid
flowchart TB
  subgraph mc [Mailchimp Marketing API]
    CL[campaigns.list]
    CR[reports.get_campaign_report]
    CLK[reports.get_campaign_click_details]
    UNS[reports.get_unsubscribed_list]
    EA[reports.get_email_activity]
    REC[reports.get_campaign_recipients]
  end

  subgraph composer [Cloud Composer DAG etl_mailchimp]
    FETCH[Sequential PythonOperator fetch chain]
    PAUSE[pause]
    LOOP[Per-entity GCS upload + BQ load + trusted copy]
  end

  subgraph storage [Landing and warehouse]
    LOCAL["Composer data/mailchimp/{entity}/*.json"]
    RAW[(raw GCS mailchimp/{entity}/{date}/)]
    STG[(trusted_staging.mailchimp_{entity})]
    TRU[(trusted.mailchimp_{entity})]
  end

  CL --> FETCH
  CR --> FETCH
  CLK --> FETCH
  UNS --> FETCH
  EA --> FETCH
  REC --> FETCH
  FETCH --> LOCAL --> PAUSE --> LOOP
  LOOP --> RAW --> STG --> TRU
```

## Components

| Layer | Responsibility |
|-------|----------------|
| `mailchimp_client.py` | `mailchimp_marketing` SDK wrapper, pagination, JSONL write |
| `dag_mailchimp.py` | Orchestration, env config, operator wiring |
| Composer local disk | Ephemeral JSONL landing before GCS copy |
| Raw GCS | Dated prefix per entity for audit and reprocessing |
| `trusted_staging` | Partitioned append tables (daily load_date) |
| `trusted` | Analyst-facing snapshot (truncate copy from staging) |

## Entity classes

| Class | API endpoint | Output file pattern |
|-------|-------------|---------------------|
| `CampaignList` | `campaigns.list` | `campaign_list.json` |
| `CampaignReports` | `reports.get_campaign_report` | `campaign_reports_{id}.json` |
| `ClickReport` | `reports.get_campaign_click_details` | `click_report_{id}.json` |
| `Unsubscribes` | `reports.get_unsubscribed_list_for_campaign` | `unsubscribes_{id}.json` |
| `EmailActivity` | `reports.get_email_activity_for_campaign` | `email_activity_{id}.json` |
| `Recipients` | `reports.get_campaign_recipients` | `recipients_{id}.json` |

## Configuration

| Variable | Purpose |
|----------|---------|
| `env` | `DEV` or `PROD` — selects project, bucket, conn id |
| `mailchimp_apikey` | Mailchimp API key (never hardcode) |
| `mailchimp_server` | Data center prefix, e.g. `us1`, `us3` |
| `composer_bucket` | Source bucket for GCSToGCS copy |

## Dependencies

- `mailchimp-marketing` Python SDK
- `google-cloud-bigquery` for campaign ID lookup
- Airflow GCP provider operators (GCSToGCS, GCSToBigQuery, BigQueryToBigQuery)
