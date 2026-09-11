###################################################################
# DAG: etl_pair_finance_cases_daily                               #
#                                                                 #
# Daily collections partner case-file ingest for AT/DE/FR/ES/IT.  #
###################################################################

"""
# Collections partner case files

## Overview
Daily batch ingest of collections partner case files for AT, DE, FR, ES, IT.
Partnership Management uses the refined table for performance tracking.

## Flow
```
etl_pair_finance_cases_daily
├── task_group_AT: extract_cases → load_gcs → stage_bq
├── task_group_DE: extract_cases → load_gcs → stage_bq
├── task_group_FR: extract_cases → load_gcs → stage_bq
├── task_group_ES: extract_cases → load_gcs → stage_bq
├── task_group_IT: extract_cases → load_gcs → stage_bq
└── trigger_dbt_job (waits for all markets)
```

1. **Extract** — GET /api/v2/case_files. Daily: `updated_from`/`updated_to` = logical date.
   First seed: trigger with conf `{"full_load": true}` (no date filter).
2. **GCS** — `gs://<composer_bucket>/pair-finance/YYYY-MM-DD/{MARKET}/cases.ndjson`
3. **Staging BQ** — `{project}.trusted_staging.pair_finance_cases_raw`
4. **dbt** — `refined.pair_finance_cases`

## Config (Airflow Variables)
| Variable | Purpose |
|----------|---------|
| `pair_finance_secret_project` | Secret Manager project (DEV `dwh_project_dev`, PROD `dwh_project`) |
| `pair_finance_api_keys` | Optional JSON fallback if a market secret is missing |
| `pair_finance_raw_bucket` | Optional override; default is Airflow Variable `composer_bucket` |
| `pair_finance_dbt_job_id` | dbt Cloud job id (default empty — ShortCircuit skips dbt) |
| `pair_finance_full_load` | `true` = seed all case files (or pass conf `full_load`) |
| `composer_bucket` | Landing bucket if different from raw bucket |
| `env` | `DEV` or `PROD` (Composer env var also works) |

Keys: Secret Manager `collections-{market}-api-key` in the Composer GCP project
(`dwh_project` in PROD). Markets without a key are skipped.

## Schedule
Daily 03:00 UTC. SLA is only on `end` (30h from logical date ≈ 09:00 UTC the
morning the run actually starts). Airflow SLA is relative to logical date, so a
6h default on every task emails skipped/`load_gcs` tasks every night. Real
failures still email via `email_on_failure`. Idempotent: skips a market/date if
the GCS object already exists.

## Runbook
| Symptom | What to do |
|---------|------------|
| API down / 5xx | Task retries 3x with exponential backoff. If still failing, check partner status page / contact. Re-run DAG for that date. |
| HTTP 429 | Connector backs off (1s, 2s, 4s). If persistent, reduce parallelism or wait; do not disable retries. |
| HTTP 401 | Key expired or IP not allowlisted. Confirm Secret `collections-{market}-api-key` and Composer egress IP. |
| Schema change | Raw JSON is stored in `raw_json`. Update `stg_pair_finance__cases` + schema.yml, then re-run dbt. |
| BQ `case_id` INTEGER vs STRING | Autodetect is off; load uses `STAGING_SCHEMA_FIELDS`. Re-run `stage_bq` after DAG parse. |
| `stage_bq` skipped / no logs | `has_records` was False — extract listed 0 cases. Check extract log, not BQ. |
| `all_markets_loaded` / dbt skipped | One market had 0 cases; ShortCircuit used to skip the rest of the DAG. Clear those three tasks, or wait for DAG parse after the `ignore_downstream_trigger_rules=False` fix. |
| SLA mail for `load_gcs` / skipped tasks | False alarm if extract already wrote GCS. SLA used to sit on every task and is counted from logical date (D-1). Check Grid: green extract + BQ `load_date` = that `ds` means the run worked. |
| DQ test failure | Check `stg_pair_finance__cases` for null `case_id`/`status`/`amount`/`market`, invalid market, or <80% of previous-day row count. |
| dbt job missing | Default job id is empty. Set Variable `pair_finance_dbt_job_id` to enable the dbt ShortCircuit branch. |
| Already-loaded skip | Delete `gs://…/pair-finance/YYYY-MM-DD/{MARKET}/cases.ndjson` and clear that day's staging rows, then clear-and-re-run. |
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

from pair_finance_api import MARKET_CONFIG
from pair_finance_pipeline import (
    STAGING_SCHEMA_FIELDS,
    extract_cases,
    gcs_object_name,
    has_records,
    load_gcs,
    raw_bucket_name,
    resolve_env,
)

try:
    from airflow.operators.empty import EmptyOperator
except ModuleNotFoundError:  # pragma: no cover
    from airflow.operators.dummy import DummyOperator as EmptyOperator  # type: ignore

try:
    from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
except ImportError:  # pragma: no cover - reference stub for checkouts without the provider
    DbtCloudRunJobOperator = None

# Inlined dbt poll helper (originally a shared utils import) — keep poll count bounded.
_TARGET_POLLS = 15
_MIN_POLL_SECONDS = 60
_MAX_POLL_SECONDS = 120


def dbt_poll_interval(timeout_seconds: int) -> int:
    """Amortized poll budget: timeout / target_polls, clamped to [60, 120]."""
    if timeout_seconds <= 0:
        return _MIN_POLL_SECONDS
    computed = timeout_seconds / _TARGET_POLLS
    return int(min(_MAX_POLL_SECONDS, max(_MIN_POLL_SECONDS, computed)))


ENV = resolve_env()
PROJECT_CONFIG = {
    "DEV": {"project_id": "dwh_project_dev", "gcp_conn_id": "bigquery_default_dev"},
    "PROD": {"project_id": "dwh_project", "gcp_conn_id": "bigquery_default"},
}
GCP_PROJECT = PROJECT_CONFIG.get(ENV, PROJECT_CONFIG["DEV"])["project_id"]
GCP_CONN_ID = PROJECT_CONFIG.get(ENV, PROJECT_CONFIG["DEV"])["gcp_conn_id"]

STAGING_DATASET = Variable.get("pair_finance_staging_dataset", default_var="trusted_staging")
RAW_BUCKET = raw_bucket_name()
DBT_JOB_ID = Variable.get("pair_finance_dbt_job_id", default_var="")
_JOB_TIMEOUT = 3600

MARKETS = tuple(MARKET_CONFIG.keys())

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "start_date": datetime(2026, 8, 1),
    "email": [
        "dataops@example.com",
    ],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "dbt_cloud_conn_id": "dbt_conn",
    "account_id": 1,
}


def _dbt_job_configured() -> bool:
    return str(DBT_JOB_ID).isdigit() and int(DBT_JOB_ID) > 0


with DAG(
    dag_id="etl_pair_finance_cases_daily",
    default_args=default_args,
    schedule_interval="0 3 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["pair-finance", "api-integration", "partner-data"],
    doc_md=__doc__,
) as dag:
    start = EmptyOperator(task_id="start")
    markets_done = EmptyOperator(
        task_id="all_markets_loaded",
        trigger_rule=TriggerRule.ALL_DONE,
    )
    # Logical date is D-1 03:00; run starts D 03:00. 30h → miss only if still
    # unfinished at D 09:00 UTC.
    end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.ALL_DONE,
        sla=timedelta(hours=30),
    )

    for market in MARKETS:
        with TaskGroup(group_id=f"task_group_{market}") as tg:
            extract = PythonOperator(
                task_id="extract_cases",
                python_callable=extract_cases,
                op_kwargs={"market": market},
                execution_timeout=timedelta(hours=2),
            )
            copy_gcs = PythonOperator(
                task_id="load_gcs",
                python_callable=load_gcs,
                op_kwargs={"market": market},
                execution_timeout=timedelta(minutes=30),
            )
            gate = ShortCircuitOperator(
                task_id="has_records",
                python_callable=has_records,
                op_kwargs={"market": market},
                ignore_downstream_trigger_rules=False,
            )
            stage = GCSToBigQueryOperator(
                task_id="stage_bq",
                gcp_conn_id=GCP_CONN_ID,
                bucket=RAW_BUCKET,
                source_objects=[gcs_object_name("{{ ds }}", market)],
                source_format="NEWLINE_DELIMITED_JSON",
                destination_project_dataset_table=(
                    f"{GCP_PROJECT}.{STAGING_DATASET}.pair_finance_cases_raw"
                ),
                schema_fields=STAGING_SCHEMA_FIELDS,
                create_disposition="CREATE_IF_NEEDED",
                write_disposition="WRITE_APPEND",
                autodetect=False,
                ignore_unknown_values=True,
            )
            extract >> copy_gcs >> gate >> stage
        start >> tg >> markets_done

    check_dbt = ShortCircuitOperator(
        task_id="dbt_job_configured",
        python_callable=_dbt_job_configured,
        ignore_downstream_trigger_rules=False,
    )
    if DbtCloudRunJobOperator is not None and _dbt_job_configured():
        trigger_dbt_job = DbtCloudRunJobOperator(
            task_id="trigger_dbt_job",
            job_id=int(DBT_JOB_ID),
            check_interval=dbt_poll_interval(_JOB_TIMEOUT),
            timeout=_JOB_TIMEOUT,
            do_xcom_push=False,
        )
    else:
        trigger_dbt_job = EmptyOperator(task_id="trigger_dbt_job")

    markets_done >> check_dbt >> trigger_dbt_job >> end
    markets_done >> end
