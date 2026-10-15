"""
Airflow DAG: Odoo helpdesk Postgres pull → GCS → BigQuery staging → dbt.

Opposite direction from pattern 06. Pattern 06 pushes refined helpdesk
rows out to an event bus. This DAG pulls raw helpdesk entities from
Odoo Postgres into the warehouse landing zone.

Per table: fetch NDJSON on the Composer data volume → copy to raw
bucket dated path → load staging (WRITE_TRUNCATE for dims / delta
tickets) → then one dbt Cloud job rebuilds trusted helpdesk models.

Source (read-only):
  dags/horeca_digital/archived/odoo_migration/etl_odoo_helpdesk_import.py
  dags/horeca_digital/helpdesk_odoo_import.py
"""

from __future__ import annotations

import os
from datetime import date, datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.utils.helpers import chain
from airflow.utils.trigger_rule import TriggerRule

from helpdesk_postgres_pull import HelpdeskPostgresPull

try:
    from airflow.operators.empty import EmptyOperator
except ModuleNotFoundError:
    from airflow.operators.dummy import DummyOperator as EmptyOperator  # type: ignore


default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
    "email": ["dataops@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "dbt_cloud_conn_id": "dbt_conn",
    "account_id": Variable.get("dbt_cloud_account_id", default_var=1),
}

ENV = os.environ.get("env", Variable.get("env", default_var="DEV"))

if ENV == "DEV":
    odoo_env = "DM"
    project_id = "dwh_project_dev"
    bucket_name = "data-platform-dev-rawzone"
    gcp_conn_id = "google_cloud_dev"
else:
    odoo_env = "PRD"
    project_id = "dwh_project"
    bucket_name = "data-platform-rawzone"
    gcp_conn_id = "google_cloud_default"

# Tickets are incremental (2-day create/write window). Everything else
# is a full dim refresh — small tables, truncate is cheaper than
# inventing SCD for team/stage/tag.
TABLE_LIST = [
    "helpdesk_ticket",
    "helpdesk_team",
    "helpdesk_ticket_type",
    "helpdesk_ticket_medium",
    "helpdesk_stage",
    "helpdesk_tag",
    "helpdesk_tag_helpdesk_ticket_rel",
    # "mail_message",  # optional; enable when conversation bodies are needed
]

TMP_LOC = "/home/airflow/gcs/data/odoo/"
LOAD_DATE = date.today().strftime("%Y-%m-%d")

EXTRACTORS = {
    "helpdesk_ticket": "helpdesk_ticket",
    "helpdesk_team": "helpdesk_team",
    "helpdesk_ticket_type": "helpdesk_ticket_type",
    "helpdesk_ticket_medium": "helpdesk_ticket_medium",
    "helpdesk_stage": "helpdesk_stage",
    "helpdesk_tag": "helpdesk_tag",
    "helpdesk_tag_helpdesk_ticket_rel": "helpdesk_tag_helpdesk_ticket_rel",
    "mail_message": "mail_messages",
}


def _make_fetch(table: str):
    """Build a task callable that constructs a fresh puller per run."""

    method_name = EXTRACTORS[table]

    def _fetch(table_name: str, execution_date=None, **_):
        puller = HelpdeskPostgresPull(
            env=odoo_env, project_id=project_id, tmp_loc=TMP_LOC
        )
        getattr(puller, method_name)(
            table_name=table_name, execution_date=execution_date
        )

    return _fetch


dag = DAG(
    dag_id="etl_odoo_helpdesk_postgres_pull",
    default_args=default_args,
    # Source shipped schedule_interval=None (manual / trigger). Keep that
    # contract — ops triggered after Odoo cutovers and nightly via
    # external scheduler when needed.
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    render_template_as_native_obj=True,
    description=(
        "[ON-DEMAND] Pull Odoo helpdesk tables from Postgres into "
        "GCS/BigQuery staging, then refresh trusted models via dbt Cloud"
    ),
    tags=["odoo", "helpdesk", "postgres-pull", "staging", "dbt"],
    doc_md=(
        "Odoo Postgres → NDJSON → GCS → BQ staging → dbt. "
        "See odoo_integration/15-helpdesk-postgres-pull/."
    ),
)

start = EmptyOperator(task_id="start", dag=dag)
end = EmptyOperator(
    task_id="end", trigger_rule=TriggerRule.ALL_DONE, dag=dag
)

task_list = []
for table in TABLE_LIST:
    data_fetch = PythonOperator(
        task_id=f"data_fetch_{table}",
        python_callable=_make_fetch(table),
        op_kwargs={
            "table_name": table,
            "execution_date": "{{ execution_date }}",
        },
        dag=dag,
    )

    upload_storage = GCSToGCSOperator(
        task_id=f"upload_storage_{table}",
        gcp_conn_id=gcp_conn_id,
        source_bucket=Variable.get("composer_bucket"),
        source_object=f"data/odoo/{table}.json",
        destination_bucket=bucket_name,
        destination_object=f"odoo/{table}/{LOAD_DATE}/",
        dag=dag,
    )

    data_load_staging = GCSToBigQueryOperator(
        task_id=f"load_staging_{table}",
        gcp_conn_id=gcp_conn_id,
        bucket=bucket_name,
        source_format="NEWLINE_DELIMITED_JSON",
        source_objects=[f"odoo/{table}/{LOAD_DATE}/{table}.json"],
        destination_project_dataset_table=f"staging.odoo_{table}",
        schema_object=f"schema_json/odoo_{table}.json",
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        allow_quoted_newlines=True,
        dag=dag,
    )

    task_list.extend([data_fetch, upload_storage, data_load_staging])

dbt_odoo_helpdesk = DbtCloudRunJobOperator(
    task_id="dbt_odoo_helpdesk_run",
    job_id=int(Variable.get("dbt_odoo_helpdesk_job_id", default_var="0")),
    check_interval=10,
    timeout=300,
    do_xcom_push=True,
    dag=dag,
)

# Sequential per-table triples, then dbt. Source used chain(start,
# *task_list, dbt, end) — same shape. Parallelizing tables is the
# obvious next step once Odoo connection limits allow it.
chain(start, *task_list, dbt_odoo_helpdesk, end)
