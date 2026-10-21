###################################################################
# DAG: etl_lpv_report
#
# Monthly LPV Adjustment report — queries discovery.lpv_adjusted
# for the previous calendar month and emails a per-country CSV zip
# via SendGrid or SMTP (with GCS offload when the zip is too large).
#
# Sanitized portfolio sample from a Cloud Composer production DAG.
# Sibling of Invoice Radar (pattern 32): shared generate→XCom→send
# split, different extract contract and attachment shape.
###################################################################

"""
# LPV Adjustment Report

## Overview
Composer DAG: load config from Airflow Variables, generate a
per-country CSV zip in one Python task, then send email via the
shared ``email_delivery`` module. Empty previous months succeed
without sending mail.

## Dev setup

1. Sync this folder (or at least ``lpv_report.py``) to Composer under
   ``dags/lpv_report/`` — or keep the file next to the DAG (portfolio
   layout).

2. Airflow Variables:
   - ``lpv_report_config`` — JSON, no secrets (see example)
   - SMTP: ``lpv_report_smtp_password``
   - SendGrid: ``sendgrid_api_key`` and
     ``"email_provider": "sendgrid"`` in the JSON
   - Oversized attachments: ``composer_bucket`` (GCS offload)

3. DEV schedule is ``None`` (manual). Recipients are always forced
   from ``config.REPORT_RECIPIENTS``.

4. PROD: ``env=PROD``, then 09:00 Europe/Amsterdam on the 5th.

### Example ``lpv_report_config``
```json
{
  "email_provider": "sendgrid",
  "smtp_host": "smtp.example.com",
  "smtp_port": 587,
  "smtp_use_tls": true,
  "smtp_user": "smtp-user@example.com",
  "smtp_timeout": 60,
  "email_from": "analytics@example.com",
  "email_from_name": "Platform Reports",
  "email_to": "finance-lpv@example.com",
  "email_cc": "",
  "gcp_project_id": "dwh_project_dev",
  "bq_dataset": "discovery",
  "bq_table": "lpv_adjusted"
}
```

## Tasks
```
start → generate_lpv_reports → send_lpv_report_emails → end
```
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.timetables.trigger import CronTriggerTimetable

from email_tasks import send_lpv_report_emails
from report_generator import generate_lpv_reports

ENV_VAR_NAME = "env"
env = os.environ.get(ENV_VAR_NAME, Variable.get(ENV_VAR_NAME, default_var="DEV"))

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 5),
    "email": ["dataops@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}

# Manual in DEV. 09:00 Europe/Amsterdam on the 5th in PROD — gives
# discovery.lpv_adjusted a few days after month-end to settle.
if env == "DEV":
    schedule = None
else:
    schedule = CronTriggerTimetable(
        cron="0 9 5 * *",
        timezone="Europe/Amsterdam",
    )

with DAG(
    dag_id="etl_lpv_report",
    default_args=default_args,
    schedule=schedule,
    catchup=False,
    max_active_runs=1,
    description="Monthly LPV Adjustment report — SendGrid/SMTP delivery",
    tags=["lpv", "email_report", "data_quality"],
) as dag:
    start = EmptyOperator(task_id="start")

    generate_reports = PythonOperator(
        task_id="generate_lpv_reports",
        python_callable=generate_lpv_reports,
    )

    send_emails = PythonOperator(
        task_id="send_lpv_report_emails",
        python_callable=send_lpv_report_emails,
    )

    end = EmptyOperator(task_id="end")

    start >> generate_reports >> send_emails >> end
