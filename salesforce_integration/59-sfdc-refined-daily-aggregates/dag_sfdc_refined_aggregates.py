"""Composer DAG: Salesforce-facing refined daily aggregates.

Eight BigQuery WRITE_TRUNCATE snapshots (orders, reservations,
subscription billing, vouchers, Odoo→SFDC revenue, invoice copy,
app logins, multi-country POS), staged with EmptyOperator fan-in /
fan-out, then a completion email.

Distinct from pattern 05 (asset-history hash-delta → Avro event bus):
this DAG materializes CRM-consumed warehouse tables, it does not push
to Salesforce APIs.

Source (read-only): ``dags/etl_refined_salesforce.py``.

Production quirks kept visible:
- Most BQ tasks use ``TriggerRule.ALL_DONE`` so a failed sibling still
  lets the stage markers and email proceed — partial refresh risk.
- Full daily truncate on all eight tables; no incremental MERGE.
- Destination project was hardcoded in source; sample keeps a single
  ``project_id`` Variable with a safe default.
- Dead commented POS-vendor matching-id task removed (not active).
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.models import Variable
from airflow.operators.email import EmailOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.helpers import chain
from airflow.utils.trigger_rule import TriggerRule

from sfdc_refined_queries import SfdcRefinedQueries

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "start_date": datetime(2018, 10, 24),
    "email": ["dataops@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

project_id = Variable.get("dwh_project_id", default_var="dwh_project")
dataset_id = "refined_salesforce"
gcp_conn_id = "bigquery_default"

dag = DAG(
    dag_id="etl_sfdc_refined_aggregates",
    default_args=default_args,
    schedule_interval="0 7 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["salesforce", "refined", "aggregates"],
)


def _truncate_job(
    task_id: str,
    query: str,
    table_id: str,
    *,
    time_partitioning: dict[str, Any] | None = None,
    clustering: dict[str, Any] | None = None,
    trigger_rule: str = TriggerRule.ALL_DONE,
) -> BigQueryInsertJobOperator:
    query_cfg: dict[str, Any] = {
        "query": query,
        "useLegacySql": False,
        "writeDisposition": "WRITE_TRUNCATE",
        "createDisposition": "CREATE_IF_NEEDED",
        "allowLargeResults": True,
        "destinationTable": {
            "projectId": project_id,
            "datasetId": dataset_id,
            "tableId": table_id,
        },
    }
    if time_partitioning:
        query_cfg["timePartitioning"] = time_partitioning
    if clustering:
        query_cfg["clustering"] = clustering
    return BigQueryInsertJobOperator(
        task_id=task_id,
        configuration={"query": query_cfg},
        gcp_conn_id=gcp_conn_id,
        trigger_rule=trigger_rule,
        dag=dag,
    )


q = SfdcRefinedQueries

create_orders = _truncate_job(
    "create_orders_aggregated",
    q.orders_aggregated(),
    "orders_aggregated",
    time_partitioning={"type": "DAY", "field": "order_date"},
)

create_reservations = _truncate_job(
    "create_reservations_aggregated",
    q.reservations_aggregated(project_id),
    "reservations_aggregated",
    time_partitioning={"type": "DAY", "field": "date"},
    clustering={"fields": ["country_code", "salesforce_id"]},
)

create_billing = _truncate_job(
    "create_subscription_billing_info",
    q.subscription_billing_info(project_id),
    "subscription_billing_info",
)

create_vouchers = _truncate_job(
    "create_voucher_info",
    q.voucher_info(project_id),
    "voucher_info",
)

create_odoo_export = _truncate_job(
    "create_sfdc_odoo_export",
    q.sfdc_odoo_export(project_id),
    "sfdc_odoo_export",
)

odoo_invoice_copy = _truncate_job(
    "odoo_wsl_invoice_lines_copy",
    q.odoo_wsl_invoice_lines_copy(project_id),
    "odoo_wsl_invoice_lines",
    trigger_rule=TriggerRule.ALL_SUCCESS,
)

app_login = _truncate_job(
    "app_login",
    q.app_login(project_id),
    "app_login",
)

pos_txn_aggregated = _truncate_job(
    "pos_transactions_aggregated",
    q.pos_transactions_aggregated(project_id),
    "pos_transactions_aggregated",
)

email = EmailOperator(
    task_id="email",
    to=["dataops@example.com"],
    subject="SFDC refined aggregates refreshed",
    html_content=(
        "Order, reservation, billing, voucher, Odoo, app-login, and POS "
        "aggregates refreshed at {{ ts }} (UTC)."
    ),
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
)

start = EmptyOperator(task_id="start", dag=dag)
stage_1 = EmptyOperator(
    task_id="stage_1", trigger_rule=TriggerRule.ALL_DONE, dag=dag
)
stage_2 = EmptyOperator(
    task_id="stage_2", trigger_rule=TriggerRule.ALL_DONE, dag=dag
)
end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE, dag=dag)

chain(
    start,
    [create_reservations, create_orders],
    stage_1,
    [
        create_billing,
        create_vouchers,
        create_odoo_export,
        odoo_invoice_copy,
        app_login,
    ],
    stage_2,
    [pos_txn_aggregated],
    [email],
    end,
)
