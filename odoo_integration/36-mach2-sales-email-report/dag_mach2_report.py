###################################################################
# DAG: etl_mach2_report
#
# Daily Mach2 Odoo sales Excel reports — five stakeholder workbooks
# emailed via SendGrid or SMTP. Generate and send are separate tasks
# so a mail-provider blip does not re-query BigQuery.
#
# Sanitized portfolio sample from a Cloud Composer production DAG.
###################################################################

"""
# Mach2 Odoo Sales Email Report

## Overview
Composer DAG: load config from Airflow Variables, generate Excel
reports in one Python task, then send email via the shared
``email_delivery`` module.

## Dev setup

1. Sync this folder (or at least ``report.py`` + ``templates/``) to
   Composer under ``dags/mach2_report/`` — or keep them on ``sys.path``
   next to the DAG file (portfolio layout).

2. Airflow Variables:
   - ``mach2_report_config`` — JSON, no secrets (see example)
   - SMTP: ``mach2_report_smtp_password``
   - SendGrid: ``sendgrid_api_key`` and
     ``"email_provider": "sendgrid"`` in the JSON

3. DEV schedule is ``None`` (manual). Recipients forced to
   ``config.DEV_TEST_RECIPIENTS``.

4. PROD: ``env=PROD``, then daily 07:00 Europe/Amsterdam.

### Example ``mach2_report_config`` (DEV)
```json
{
  "email_provider": "sendgrid",
  "smtp_host": "smtp.example.com",
  "smtp_port": 587,
  "smtp_use_tls": true,
  "smtp_user": "smtp-user@example.com",
  "smtp_timeout": 60,
  "email_from": "analytics@example.com",
  "email_from_name": "Platform Analytics",
  "email_to_report_1": "dataops@example.com",
  "email_to_report_2": "dataops@example.com",
  "email_to_report_3": "dataops@example.com",
  "email_cc": "",
  "gcp_project_id": "dwh_project_dev",
  "bq_dataset_refined_sales": "refined_sales",
  "bq_dataset_refined": "refined",
  "bq_table_res_partner": "odoo_res_partner",
  "bq_table_sale_order": "odoo_sale_order",
  "bq_table_sale_order_line": "odoo_sale_order_line",
  "bq_table_res_country": "odoo_res_country",
  "bq_table_product_product": "odoo_product_product",
  "bq_table_product_template": "odoo_product_template",
  "bq_table_sale_order_close_reason": "odoo_sale_order_close_reason",
  "bq_table_partner_matching": "partner_crm_matching_ids"
}
```

## Tasks
```
start → generate_mach2_reports → send_mach2_report_emails → end
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

from email_tasks import send_mach2_report_emails
from report_generator import generate_mach2_reports

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
        cron="0 7 * * *",
        timezone="Europe/Amsterdam",
    )

with DAG(
    dag_id="etl_mach2_report",
    default_args=default_args,
    schedule=schedule,
    catchup=False,
    max_active_runs=1,
    description="Mach2 Odoo sales reports — SendGrid/SMTP delivery",
    tags=["mach2", "email_report", "odoo", "sales"],
) as dag:
    start = EmptyOperator(task_id="start")

    generate_reports = PythonOperator(
        task_id="generate_mach2_reports",
        python_callable=generate_mach2_reports,
    )

    send_emails = PythonOperator(
        task_id="send_mach2_report_emails",
        python_callable=send_mach2_report_emails,
    )

    end = EmptyOperator(task_id="end")

    start >> generate_reports >> send_emails >> end
