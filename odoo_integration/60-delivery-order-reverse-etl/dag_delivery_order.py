"""
DAG: etl_delivery_order

Orchestrates BigQuery → Odoo stock.picking reverse ETL for logistics
delivery orders. One Python task pulls the curated trusted query and
runs insert/update against Odoo.

Source (read-only):
- dags/horeca_digital/archived/etl_delivery_order.py
- dags/horeca_digital/archived/odoo_migration/product_installation_odoo.py
  (OdooProductInstallation delivery-order path only)
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.helpers import chain

from odoo_delivery_order import OdooDeliveryOrderSync

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "start_date": datetime(2022, 10, 23),
    "email": ["dataops@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=10),
}

env = os.environ.get("env", Variable.get("env", default_var="DEV"))

if env == "DEV":
    project_id = Variable.get("dwh_project_id_dev", default_var="dwh_project_dev")
    odoo_creds = Variable.get("odoo_dev_creds", deserialize_json=True)
else:
    project_id = Variable.get("dwh_project_id", default_var="dwh_project")
    odoo_creds = Variable.get("odoo_prod_creds", deserialize_json=True)

odoo = OdooDeliveryOrderSync(credentials=odoo_creds, bq_project=project_id)

# Exclude already-exported external identifiers held in a control table
DELIVERY_ORDER_QUERY = f"""
SELECT
  ROW_NUMBER() OVER (
    ORDER BY carrier_tracking_ref DESC NULLS FIRST,
             DATE(_PARTITIONTIME) DESC
  ) AS rnk,
  *
FROM `{project_id}.trusted.odoo_stock_picking_delivery_order`
WHERE carrier_tracking_ref NOT IN (
  SELECT DISTINCT external_id
  FROM `{project_id}.ops.delivery_order_exported_ids`
  WHERE external_id <> 'External Identifier'
)
"""

with DAG(
    dag_id="etl_delivery_order",
    default_args=default_args,
    schedule_interval=None,
    render_template_as_native_obj=True,
    max_active_runs=1,
    catchup=False,
    tags=["odoo", "reverse-etl", "stock-picking", "logistics"],
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    odoo_delivery_order_ingestion = PythonOperator(
        task_id="odoo_delivery_order_ingestion",
        python_callable=odoo.load_delivery_order_data,
        execution_timeout=timedelta(hours=1),
        op_kwargs={"query": DELIVERY_ORDER_QUERY},
    )

    chain(start, odoo_delivery_order_ingestion, end)
