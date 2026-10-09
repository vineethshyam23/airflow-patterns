"""Wholesale NL enrichment → partner HubSpot reverse export.

Manual-only DAG: three independent PythonOperators push discovery
enrichment tables (prospects, matched, dedupe pairs) through the partner
MCC OAuth2 HubSpot endpoints in 5k-record chunks.

Distinct from inbound dual-source land (pattern 69) and from partner
event-bus Avro exports (patterns 10/11/53): this is a full-table CRM
push with session-scoped batches, not an incremental land or Avro bus.

Source (read-only):
  dags/etl_makro_hubspot_export.py
  dags/horeca_digital/makro_customers_api.py (export_* only)
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

import wholesale_nl_hubspot_export as dg

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
    "email": ["dataops@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}

# schedule=None: ops triggers after discovery enrichment refresh.
# Production left unused bucket/conn vars in the DAG file; dropped here.
with DAG(
    dag_id="etl_wholesale_nl_hubspot_export",
    default_args=default_args,
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["wholesale", "hubspot", "nl", "reverse-etl"],
) as dag:
    export_prospects = PythonOperator(
        task_id="export_data_prospects",
        python_callable=dg.export_data_prospects,
    )
    export_matched = PythonOperator(
        task_id="export_data_matched",
        python_callable=dg.export_data_matched,
    )
    export_dedupe = PythonOperator(
        task_id="export_data_deduplication",
        python_callable=dg.export_data_deduplication,
    )

# No edges — all three start when the DAG is triggered. Deduplication
# may need to follow matched in some CRM workflows; production runs
# them in parallel. Add >> only if the partner requires ordering.
_ = (export_prospects, export_matched, export_dedupe)
