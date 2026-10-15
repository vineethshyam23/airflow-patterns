###################################################################
# DAG: etl_invoice_radar
#
# Daily Missing Invoices / Invoice Radar — LPV vs invoice
# discrepancy report. Writes bi.all_invoices (optional) and emails
# a 4-sheet Excel via SendGrid or SMTP.
#
# Sanitized portfolio sample from a Cloud Composer production DAG.
###################################################################

"""
# Invoice Radar / Missing Invoices

## Overview
Composer DAG: load config from Airflow Variables, generate the Excel
report in one Python task, then send email via the shared
``email_delivery`` module.

## Dev setup

1. Sync this folder (or at least ``invoice_radar.py`` +
   ``invoice_radar_alert.html``) to Composer under
   ``dags/invoice_radar/`` — or keep them on ``sys.path`` next to
   the DAG file (portfolio layout).

2. Airflow Variables:
   - ``invoice_radar_config`` — JSON, no secrets (see example)
   - SMTP: ``invoice_radar_smtp_password``
   - SendGrid: ``sendgrid_api_key`` and
     ``"email_provider": "sendgrid"`` in the JSON

3. DEV schedule is ``None`` (manual). Recipients forced to
   ``config.DEV_TEST_RECIPIENTS``.

4. PROD: ``env=PROD``, then daily 09:00 Europe/Amsterdam.

### Example ``invoice_radar_config`` (DEV)
```json
{
  "email_provider": "sendgrid",
  "smtp_host": "smtp.example.com",
  "smtp_port": 587,
  "smtp_use_tls": true,
  "smtp_user": "smtp-user@example.com",
  "smtp_timeout": 60,
  "email_from": "analytics@example.com",
  "email_from_name": "Invoice Radar",
  "email_recipients": "dataops@example.com",
  "email_cc": "",
  "gcp_project_id": "dwh_project_dev",
  "bq_dataset_discovery": "discovery",
  "bq_table_lpv_adj": "lpv_adjusted",
  "bq_dataset_product_spot": "product_spot",
  "bq_table_asset": "erp_asset",
  "bq_dataset_refined_sales": "refined_sales",
  "bq_table_pricing": "ic_pricing_table",
  "bq_dest_dataset": "bi",
  "bq_dest_table": "all_invoices",
  "write_all_invoices": true
}
```

## Tasks
```
start → generate_invoice_radar_reports → send_invoice_radar_emails → end
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

from email_tasks import send_invoice_radar_emails
from report_generator import generate_invoice_radar_reports

ENV_VAR_NAME = "env"
env = os.environ.get(ENV_VAR_NAME, Variable.get(ENV_VAR_NAME, default_var="DEV"))

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email": ["dataops@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

if env == "DEV":
    schedule = None
else:
    schedule = CronTriggerTimetable(
        cron="0 9 * * *",
        timezone="Europe/Amsterdam",
    )

with DAG(
    dag_id="etl_invoice_radar",
    default_args=default_args,
    schedule=schedule,
    catchup=False,
    max_active_runs=1,
    description="Invoice Radar missing-invoices report — SendGrid/SMTP delivery",
    tags=["invoice_radar", "email_report", "missing_invoices", "data_quality"],
) as dag:
    start = EmptyOperator(task_id="start")

    generate_reports = PythonOperator(
        task_id="generate_invoice_radar_reports",
        python_callable=generate_invoice_radar_reports,
    )

    send_emails = PythonOperator(
        task_id="send_invoice_radar_emails",
        python_callable=send_invoice_radar_emails,
    )

    end = EmptyOperator(task_id="end")

    start >> generate_reports >> send_emails >> end
