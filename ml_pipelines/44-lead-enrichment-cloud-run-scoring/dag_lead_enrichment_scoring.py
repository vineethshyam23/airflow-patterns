"""Airflow DAG: lead enrichment + Cloud Run scoring → Odoo.

Daily after Vertex matching-engine output lands. Soft-skips when no
same-day rows exist, otherwise: dbt enrich → Cloud Run score (wait) →
dbt post-score → Odoo push → Slack.

Distinct from pattern 02 (POS text classification / Odoo lead class)
and pattern 42 (field-sales API → CRM). This DAG owns the enrichment
and batch-scoring handoff between matching engine, Cloud Run, and CRM.

Source (read-only):
  dags/etl_leads_enrichment.py
  uses horeca_digital/lead_engine_odoo.py (covered by pattern 02)
"""

from __future__ import annotations

import functools
import os
from datetime import datetime, timedelta
from typing import Any, Dict

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.utils.helpers import chain

from matching_engine_gate import check_matching_engine_data
from odoo_enrichment_push import OdooEnrichmentPush

try:
    from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
except ImportError:  # pragma: no cover - reference stub
    DbtCloudRunJobOperator = None  # type: ignore[misc, assignment]

try:
    from airflow.providers.google.cloud.operators.cloud_run import (
        CloudRunExecuteJobOperator,
    )
except ImportError:  # pragma: no cover - reference stub
    CloudRunExecuteJobOperator = None  # type: ignore[misc, assignment]


# ── Cloud Run scoring job (sanitized defaults; override via Variables) ────────
SCORING_PROJECT = Variable.get(
    "lead_scoring_gcp_project",
    default_var="ml_scoring_project",
)
SCORING_REGION = Variable.get(
    "lead_scoring_region",
    default_var="europe-west3",
)
SCORING_JOB_NAME = Variable.get(
    "lead_scoring_job_name",
    default_var="lead-scoring-score",
)
# Must match the env config pack inside the scoring image (e.g. prod.yaml).
SCORING_JOB_ENV = Variable.get(
    "lead_scoring_job_env",
    default_var="prod",
)
# Product + market args the scoring CLI expects (example: POS / IT).
SCORING_PRODUCT = Variable.get("lead_scoring_product", default_var="pos")
SCORING_MARKET = Variable.get("lead_scoring_market", default_var="IT")
SCORING_MODE = Variable.get("lead_scoring_mode", default_var="both")
SCORING_DRY_RUN = Variable.get("lead_scoring_dry_run", default_var="false")

MATCHING_TABLE = Variable.get(
    "matching_engine_sam_leads_table",
    default_var="ml_scoring_project.matching_engine.sam_leads",
)
LEADS_FINAL_TABLE = Variable.get(
    "leads_enrichment_final_table",
    default_var="crm_spot.leads_enrichment_final",
)

DBT_JOB_ENRICHMENT = Variable.get(
    "lead_enrichment_dbt_job_id",
    default_var="",
)
DBT_JOB_SCORING = Variable.get(
    "lead_scoring_dbt_job_id",
    default_var="",
)

ENV = os.environ.get("env", Variable.get("env", default_var="DEV"))

if ENV == "DEV":
    PROJECT_ID = Variable.get("dwh_project_dev", default_var="dwh_project_dev")
    SLACK_CHANNEL = Variable.get(
        "lead_enrichment_slack_channel_dev",
        default_var="#data-pipeline-test",
    )
    SLACK_CONN = Variable.get(
        "lead_enrichment_slack_conn_dev",
        default_var="slack_conn_test",
    )
    GCP_CONN = "google_cloud_dev"
    ODOO_CREDS_KEY = "odoo_pp_creds"
else:
    PROJECT_ID = Variable.get("dwh_project", default_var="dwh_project")
    SLACK_CHANNEL = Variable.get(
        "lead_enrichment_slack_channel",
        default_var="#crm-data-ops",
    )
    SLACK_CONN = Variable.get(
        "lead_enrichment_slack_conn",
        default_var="slack_conn",
    )
    GCP_CONN = "google_cloud_default"
    ODOO_CREDS_KEY = "odoo_prod_creds"

odoo_push = OdooEnrichmentPush()
odoo_creds: Dict[str, Any] = Variable.get(
    ODOO_CREDS_KEY,
    deserialize_json=True,
    default_var={},
)

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "start_date": datetime(2026, 7, 14),
    "email": ["dataops@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "dbt_cloud_conn_id": "dbt_cloud_default",
    "account_id": 1,
}


def slack_notification_complete(**_: Any) -> None:
    """Success path alert — keep it short; logs have the detail."""
    message = (
        "*Lead enrichment + scoring completed*\n"
        "Matching engine data found\n"
        "dbt enrichment finished\n"
        "Cloud Run scoring finished\n"
        "dbt post-score finished\n"
        "Leads pushed to Odoo"
    )
    SlackWebhookOperator(
        task_id="lead_enrichment_slack_alert",
        slack_webhook_conn_id=SLACK_CONN,
        message=message,
        channel=SLACK_CHANNEL,
        username="airflow-lead-enrichment",
    ).execute({})


dag = DAG(
    dag_id="etl_leads_enrichment",
    default_args=default_args,
    schedule="30 5 * * *",  # daily 05:30 UTC — after matching-engine land
    catchup=False,
    description=(
        "Lead enrichment + Cloud Run scoring — gate → dbt → score → "
        "dbt → Odoo"
    ),
    tags=[
        "enrichment",
        "odoo",
        "dbt",
        "matching-engine",
        "lead-scoring",
        "cloud-run",
    ],
    max_active_runs=1,
    render_template_as_native_obj=True,
    doc_md=__doc__,
)


def _dbt_or_stub(task_id: str, job_id: str, timeout: int):
    if DbtCloudRunJobOperator is not None and job_id:
        return DbtCloudRunJobOperator(
            task_id=task_id,
            job_id=int(job_id),
            check_interval=10,
            do_xcom_push=True,
            dag=dag,
            timeout=timeout,
        )
    return EmptyOperator(
        task_id=task_id,
        dag=dag,
        doc_md="Stub: set Variable and install dbt Cloud provider to run.",
    )


def _score_or_stub():
    if CloudRunExecuteJobOperator is not None:
        return CloudRunExecuteJobOperator(
            task_id="score_leads",
            project_id=SCORING_PROJECT,
            region=SCORING_REGION,
            job_name=SCORING_JOB_NAME,
            gcp_conn_id=GCP_CONN,
            overrides={
                "container_overrides": [
                    {
                        "args": [
                            SCORING_PRODUCT,
                            SCORING_MARKET,
                            SCORING_JOB_ENV,
                            "{{ data_interval_end | ds }}",
                            SCORING_MODE,
                            SCORING_DRY_RUN,
                        ]
                    }
                ]
            },
            deferrable=False,
            execution_timeout=timedelta(hours=1),
            dag=dag,
        )
    return EmptyOperator(
        task_id="score_leads",
        dag=dag,
        doc_md=(
            "Stub: install google Cloud Run provider and set "
            "lead_scoring_* Variables to execute the scoring job."
        ),
    )


start = EmptyOperator(task_id="start", dag=dag)
end = EmptyOperator(task_id="end", dag=dag)
skip_enrichment = EmptyOperator(task_id="skip_enrichment", dag=dag)

check_matching_engine_results = BranchPythonOperator(
    task_id="check_matching_engine_results",
    python_callable=functools.partial(
        check_matching_engine_data,
        client_project=PROJECT_ID,
        matching_table=MATCHING_TABLE,
    ),
    dag=dag,
)

dbt_lead_enrichment_run = _dbt_or_stub(
    "dbt_lead_enrichment_run",
    DBT_JOB_ENRICHMENT,
    600,
)
score_leads = _score_or_stub()
dbt_lead_scoring_run = _dbt_or_stub(
    "dbt_lead_scoring_run",
    DBT_JOB_SCORING,
    600,
)

send_leads_to_odoo = PythonOperator(
    task_id="send_leads_enrichment_odoo",
    python_callable=odoo_push.load_data_leads_enrichment,
    execution_timeout=timedelta(hours=2),
    trigger_rule="all_success",
    op_kwargs={
        "odoo_creds": odoo_creds,
        "project_name": PROJECT_ID,
        "table_name": LEADS_FINAL_TABLE,
    },
    dag=dag,
)

slack_alert = PythonOperator(
    task_id="slack_notification_complete",
    python_callable=slack_notification_complete,
    execution_timeout=timedelta(minutes=5),
    dag=dag,
)

chain(start, check_matching_engine_results)

chain(
    check_matching_engine_results,
    dbt_lead_enrichment_run,
    score_leads,
    dbt_lead_scoring_run,
    send_leads_to_odoo,
    slack_alert,
    end,
)

chain(check_matching_engine_results, skip_enrichment, end)
