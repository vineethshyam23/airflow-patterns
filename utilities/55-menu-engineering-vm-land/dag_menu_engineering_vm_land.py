"""
DAG: Menu Engineering VM → dual GCS → BigQuery staging

Lands Postgres tables that live only on a product GCE VM (Dockerised
Postgres, no Cloud SQL Admin path). For each table:

1. SSH onto the VM and ``COPY … TO STDOUT WITH CSV HEADER`` via
   ``docker exec`` into a local CSV directory.
2. ``gcloud storage cp`` into the product team's GCS bucket (VM SA
   already has write there; Composer does not need VM-local IAM).
3. After all uploads: vacuum journald, drop old product logs, delete
   the CSVs so the VM disk does not grow forever.
4. Composer ``GCSToGCSOperator`` copies product bucket → DWH rawzone
   under a dated prefix (keeps product and platform IAM separate).
5. ``GCSToBigQueryOperator`` TRUNCATEs staging ``me_<table>_tbl``
   from schema JSON, then one dbt Cloud job builds trusted models.

One-time SSH key bootstrap lives as commented tasks in the source
DAG; keep that in runbooks, not in the daily graph.

Distinct from patterns 20–22 (partner Avro exports of enrichment
attributes): this is the OLTP land of the menu-engineering product
database itself. Distinct from Hydra / Reservation Tool Cloud SQL
exports (#39 / #54): there is no Cloud SQL Admin API here — the
source is a VM with Dockerised Postgres reached over SSH.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.utils.helpers import chain
from airflow.utils.trigger_rule import TriggerRule

from table_catalog import BQ_STAGING_PREFIX, PG_SCHEMA, POSTGRES_TABLES

try:
    from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
except ImportError:  # pragma: no cover - reference stub
    DbtCloudRunJobOperator = None

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration — real project / VM / bucket names stay in Variables
# ---------------------------------------------------------------------------

try:
    SSH_CONN_ID = Variable.get("me_vm_ssh_conn_id")
except KeyError:
    SSH_CONN_ID = "me_psql_ssh"

try:
    CSV_DIR = Variable.get("me_vm_csv_dir")
except KeyError:
    CSV_DIR = "/home/postgres_csv_files"

try:
    DOCKER_CONTAINER = Variable.get("me_pg_container")
except KeyError:
    DOCKER_CONTAINER = "me_db_prod"

try:
    PG_USER = Variable.get("me_pg_user")
except KeyError:
    PG_USER = "postgres"

try:
    PG_DATABASE = Variable.get("me_pg_database")
except KeyError:
    PG_DATABASE = "menu_engineering"

try:
    PRODUCT_BUCKET = Variable.get("me_product_bucket")
except KeyError:
    PRODUCT_BUCKET = "me-pipeline-prod"

try:
    PRODUCT_PREFIX = Variable.get("me_product_prefix")
except KeyError:
    PRODUCT_PREFIX = "menu_engineering"

try:
    RAW_BUCKET = Variable.get("me_raw_bucket")
except KeyError:
    RAW_BUCKET = "dwh-rawzone"

try:
    RAW_PREFIX = Variable.get("me_raw_prefix")
except KeyError:
    RAW_PREFIX = "menu_engineering"

try:
    DWH_PROJECT = Variable.get("me_dwh_project")
except KeyError:
    DWH_PROJECT = "dwh_project"

try:
    STAGING_DATASET = Variable.get("me_staging_dataset")
except KeyError:
    STAGING_DATASET = "dwh_trusted_staging"

try:
    GCP_CONN_ID = Variable.get("me_gcp_conn_id")
except KeyError:
    GCP_CONN_ID = "google_cloud_default"

try:
    DBT_JOB_ID = Variable.get("me_dbt_job_id")
except KeyError:
    DBT_JOB_ID = None

# Logical date baked into the rawzone path (Composer render).
LOADED_DATE = "{{ ds }}"

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "email": ["dataops@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
}

dag = DAG(
    dag_id="etl_menu_engineering_vm_land",
    default_args=default_args,
    description=(
        "SSH COPY Postgres on product VM → product GCS → DWH rawzone "
        "→ BQ staging → dbt"
    ),
    schedule_interval="0 5 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    doc_md=__doc__,
    tags=["menu-engineering", "vm-export", "postgres", "gcs"],
)

start = EmptyOperator(task_id="start", dag=dag)
stage_export = EmptyOperator(
    task_id="stage_export", trigger_rule=TriggerRule.ALL_DONE, dag=dag
)
stage_product_gcs = EmptyOperator(
    task_id="stage_product_gcs", trigger_rule=TriggerRule.ALL_DONE, dag=dag
)
stage_rawzone = EmptyOperator(
    task_id="stage_rawzone", trigger_rule=TriggerRule.ALL_DONE, dag=dag
)
stage_bq = EmptyOperator(
    task_id="stage_bq", trigger_rule=TriggerRule.ALL_DONE, dag=dag
)
end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE, dag=dag)

# Prepare CSV dir + enable parallel composite upload on the VM.
# Tracker-file wipe avoids gcloud storage JSON decode errors after a
# previously interrupted composite upload.
prepare_vm = SSHOperator(
    task_id="prepare_vm",
    ssh_conn_id=SSH_CONN_ID,
    command=f"""
        set -e
        sudo mkdir -p {CSV_DIR}
        sudo chmod 777 {CSV_DIR}
        gcloud config set storage/parallel_composite_upload_enabled True
        rm -rf "$HOME/.config/gcloud/surface_data/storage/tracker_files/"* || true
    """,
    conn_timeout=3600,
    cmd_timeout=600,
    dag=dag,
)

export_tasks = []
upload_product_tasks = []
copy_rawzone_tasks = []
load_bq_tasks = []

for table in POSTGRES_TABLES:
    export_tasks.append(
        SSHOperator(
            task_id=f"export_{table}",
            ssh_conn_id=SSH_CONN_ID,
            command=f"""
                set -e
                sudo docker exec -i {DOCKER_CONTAINER} psql \\
                    -U {PG_USER} \\
                    -d {PG_DATABASE} \\
                    -c "COPY {PG_SCHEMA}.{table} TO STDOUT WITH CSV HEADER" \\
                    > {CSV_DIR}/{table}.csv
                if [ ! -f {CSV_DIR}/{table}.csv ]; then
                    echo "Export failed for {table}: file not created"
                    exit 1
                fi
                echo "Exported {table}"
            """,
            conn_timeout=3600,
            cmd_timeout=3600,
            dag=dag,
        )
    )

    upload_product_tasks.append(
        SSHOperator(
            task_id=f"upload_product_{table}",
            ssh_conn_id=SSH_CONN_ID,
            command=f"""
                set -e
                gcloud storage cp \\
                    "{CSV_DIR}/{table}.csv" \\
                    "gs://{PRODUCT_BUCKET}/{PRODUCT_PREFIX}/{table}.csv"
                echo "Uploaded {table} to product bucket"
            """,
            conn_timeout=3600,
            cmd_timeout=3600,
            dag=dag,
        )
    )

    copy_rawzone_tasks.append(
        GCSToGCSOperator(
            task_id=f"copy_rawzone_{table}",
            gcp_conn_id=GCP_CONN_ID,
            source_bucket=PRODUCT_BUCKET,
            source_object=f"{PRODUCT_PREFIX}/{table}.csv",
            destination_bucket=RAW_BUCKET,
            destination_object=(
                f"{RAW_PREFIX}/menu_engineering/{LOADED_DATE}/{table}.csv"
            ),
            dag=dag,
        )
    )

    load_bq_tasks.append(
        GCSToBigQueryOperator(
            task_id=f"load_staging_{table}",
            gcp_conn_id=GCP_CONN_ID,
            bucket=RAW_BUCKET,
            source_format="CSV",
            source_objects=[
                f"{RAW_PREFIX}/menu_engineering/{LOADED_DATE}/{table}.csv"
            ],
            destination_project_dataset_table=(
                f"{DWH_PROJECT}.{STAGING_DATASET}."
                f"{BQ_STAGING_PREFIX}{table}_tbl"
            ),
            skip_leading_rows=1,
            write_disposition="WRITE_TRUNCATE",
            autodetect=False,
            schema_object=f"schema_json/{table}.json",
            create_disposition="CREATE_IF_NEEDED",
            field_delimiter=",",
            allow_quoted_newlines=True,
            dag=dag,
        )
    )

# Disk hygiene after product-bucket upload succeeds for every table.
cleanup_vm = SSHOperator(
    task_id="cleanup_vm_after_upload",
    ssh_conn_id=SSH_CONN_ID,
    command=f"""
        set -e
        echo "=== VM cleanup started at $(date) ==="
        sudo journalctl --vacuum-time=3d || true
        sudo find /var/log/menu-engineering/prod -type f -name "*.log" \\
            -mtime +2 -delete 2>/dev/null || true
        sudo find /var/log -maxdepth 1 -type f \\( \\
          -name "*.gz" -o -name "*.1" -o -name "*.2" -o -name "*.3" \\
          -o -name "*.4" -o -name "*.5" -o -name "*.6" -o -name "*.7" \\
          -o -name "*.8" -o -name "*.9" -o -name "*.10" -o -name "*.11" \\
          -o -name "*.12" \\
        \\) -delete 2>/dev/null || true
        find "{CSV_DIR}" -maxdepth 1 -type f -name "*.csv" -delete 2>/dev/null || true
        echo "=== VM cleanup finished at $(date) ==="
    """,
    conn_timeout=3600,
    cmd_timeout=600,
    dag=dag,
)

if DbtCloudRunJobOperator is not None and DBT_JOB_ID:
    dbt_me_run = DbtCloudRunJobOperator(
        task_id="dbt_me_run",
        job_id=int(DBT_JOB_ID),
        check_interval=10,
        timeout=300,
        do_xcom_push=True,
        reuse_existing_run=True,
        retry_from_failure=True,
        dag=dag,
    )
else:
    dbt_me_run = EmptyOperator(task_id="dbt_me_run", dag=dag)
    logger.info(
        "dbt provider missing or me_dbt_job_id unset — using EmptyOperator stub"
    )

# Lists inside chain() fan out in parallel and fan in to the next
# marker — same shape as the production DAG.
chain(
    start,
    prepare_vm,
    stage_export,
    export_tasks,
    stage_product_gcs,
    upload_product_tasks,
    cleanup_vm,
    stage_rawzone,
    copy_rawzone_tasks,
    stage_bq,
    load_bq_tasks,
    dbt_me_run,
    end,
)
