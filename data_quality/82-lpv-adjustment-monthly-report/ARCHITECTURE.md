# Architecture: LPV Adjustment monthly report

Composer owns the graph. `lpv_report.py` owns the SQL, country zip,
and HTML. `email_delivery.py` owns transport. `email_tasks.py`
adds a SendGrid size guard that offloads oversized zips to GCS
before send. Airflow Variables map into environment variables so
the notebook-shaped entrypoint stays runnable locally.

## Diagram

```mermaid
flowchart TB
  subgraph vars [Airflow Variables]
    CFG["lpv_report_config JSON"]
    SECRET["sendgrid_api_key / smtp password"]
    ENV["env = DEV|PROD"]
    BUCKET["composer_bucket"]
  end

  subgraph compose [Composer DAG etl_lpv_report]
    START[start]
    GEN[generate_lpv_reports]
    SEND[send_lpv_report_emails]
    ENDN[end]
  end

  subgraph report [lpv_report.py]
    SQL["previous calendar month + Recurring/Onetime CASE"]
    ZIP["per-country UTF-8 CSV → DEFLATE zip"]
    HTML["inline HTML summary"]
  end

  subgraph bq [BigQuery]
    LPV[("discovery.lpv_adjusted")]
  end

  subgraph out [Delivery]
    STAGE["worker staging dir / run_id"]
    OFFLOAD{"zip > 20 MB?"}
    GCS[("gs://composer_bucket/lpv_report/...")]
    MAIL["SendGrid or SMTP"]
  end

  CFG --> GEN
  SECRET --> SEND
  ENV --> GEN
  BUCKET --> SEND
  START --> GEN --> SEND --> ENDN
  LPV --> SQL --> ZIP --> STAGE
  ZIP --> HTML
  STAGE --> OFFLOAD
  OFFLOAD -->|no| MAIL
  OFFLOAD -->|yes| GCS
  GCS --> MAIL
  HTML --> SEND
```

## Components

**config.py**  
Reads `lpv_report_config`, resolves project/dataset/table, writes
`BQ_*` / `EMAIL_*` / `SMTP_*` into `os.environ`. Always overwrites
`EMAIL_TO` with `REPORT_RECIPIENTS`.

**report_generator.py**  
Resolves the report module (Composer sync path, else this folder),
importlib-loads `lpv_report.py`, stages under a per-`run_id`
directory, returns the email payload list for XCom.

**lpv_report.py**  
Previous-month filter, revenue-model CASE, country zip, HTML body.
Empty extract returns `[]` so send becomes a no-op success.

**email_tasks.py + email_delivery.py**  
XCom pull → size check → optional GCS offload → `EmailMessage` →
SendGrid API or SMTP with TLS. Zip MIME type is set from filename.

## Design notes

Dynamic import keeps the notebook entrypoint (`generate_reports`)
callable without packaging it as an Airflow plugin. The cost is a
path convention on Composer.

The 20 MB guard is the production lesson: multi-country months can
blow past SendGrid after base64. Prefer a GCS URI in the HTML over
a hard failure — finance still gets the file, ops still gets a green
DAG when the only problem was attachment size.
