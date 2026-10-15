"""Jira Service Desk / support-project ingest — incremental or monthly full load.

Twice-daily Composer DAG. Default path: for each configured project, extract
issues updated between data_interval_start and data_interval_end via JQL,
land JSONL on the Composer data volume, copy into the rawzone bucket, append
into a single-column JSON BigQuery staging table, then run one dbt Cloud job
that normalizes + dedupes across projects.

Flip FULL_LOAD_MODE to True for a backfill. At parse time we ask Jira for
each project's real created→updated span, fan out one TaskGroup of monthly
PythonOperators, merge the monthly JSONL files, then reuse the same
upload → BQ → dbt tail. Same idea as the Odoo EDI rank-split pattern:
size the fan-out from live metadata, not a hardcoded year list.

Source (read-only):
  dags/etl_jira_HDSD.py
  dags/horeca_digital/jira_hdsd.py
"""

from __future__ import annotations

import glob
import os
from calendar import monthrange
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.utils.task_group import TaskGroup

from jira_client import (
    get_jira_issues_by_date_range,
    get_jira_project_date_range,
)

try:
    from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
except ImportError:  # pragma: no cover - reference stub
    DbtCloudRunJobOperator = None

# ---------------------------------------------------------------------------
# Load mode
# FULL_LOAD_MODE=False → incremental window from the schedule interval
# FULL_LOAD_MODE=True  → monthly TaskGroups covering the project's real span
# ---------------------------------------------------------------------------
FULL_LOAD_MODE = False

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "start_date": datetime(2024, 9, 25),
    "email": ["dataops@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 4,
    "retry_delay": timedelta(minutes=10),
    "dbt_cloud_conn_id": "dbt_conn",
    "account_id": 1,
}

environment = "env"
env = os.environ.get(environment, Variable.get(environment, default_var="DEV"))

if env == "DEV":
    PROJECT_ID = Variable.get("jira_gcp_project", default_var="dwh_project_dev")
    BUCKET_NAME = Variable.get("jira_rawzone_bucket", default_var="rawzone_dev")
    GCP_CONN_ID = "google_cloud_dev"
else:
    PROJECT_ID = Variable.get("jira_gcp_project", default_var="dwh_project")
    BUCKET_NAME = Variable.get("jira_rawzone_bucket", default_var="rawzone")
    GCP_CONN_ID = "google_cloud_default"

COMPOSER_BUCKET = Variable.get("composer_bucket", default_var="composer-data")

try:
    DBT_JOB_ID = Variable.get("jira_dbt_job_id")
except KeyError:
    DBT_JOB_ID = None

# Sanitized project keys — production used internal support + POS keys.
JIRA_PROJECTS = [
    p.strip()
    for p in Variable.get("jira_project_keys", default_var="SUP,POSAPP").split(",")
    if p.strip()
]


def generate_monthly_ranges(start_date_str: str, end_date_str: str):
    """Month windows from an inclusive YYYY-MM-DD span (parse-time fan-out)."""
    start = datetime.strptime(start_date_str, "%Y-%m-%d")
    end = datetime.strptime(end_date_str, "%Y-%m-%d")
    ranges = []
    current = start.replace(day=1)

    while current <= end:
        last_day = monthrange(current.year, current.month)[1]
        month_end = current.replace(day=last_day)
        if month_end > end:
            month_end = end
        ranges.append(
            (
                f"{current.year}_{current.month:02d}",
                current.strftime("%Y-%m-%d"),
                month_end.strftime("%Y-%m-%d"),
            )
        )
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)

    return ranges


def merge_monthly_files(project_key: str, clean_execution_date: str) -> int:
    """Concatenate per-month JSONL into one file; delete the month shards."""
    small = project_key.lower()
    if FULL_LOAD_MODE:
        final_file = (
            f"/home/airflow/gcs/data/jira_{small}/"
            f"{project_key}_full_load_{clean_execution_date}.jsonl"
        )
        pattern = (
            f"/home/airflow/gcs/data/jira_{small}/"
            f"{project_key}_full_load_*_*.jsonl"
        )
    else:
        final_file = (
            f"/home/airflow/gcs/data/jira_{small}/"
            f"{project_key}_{clean_execution_date}.jsonl"
        )
        pattern = (
            f"/home/airflow/gcs/data/jira_{small}/"
            f"{project_key}_????_??_*.jsonl"
        )

    month_files = [f for f in sorted(glob.glob(pattern)) if f != final_file]

    total = 0
    with open(final_file, "w", encoding="utf-8") as out:
        for path in month_files:
            with open(path, "r", encoding="utf-8") as inp:
                for line in inp:
                    out.write(line)
                    total += 1
            os.remove(path)

    print(f"Merged {total} records into {final_file} from {len(month_files)} shards")
    return total


def persist_dbt_run_id(ti):
    """Best-effort capture of the dbt Cloud run id into an Airflow Variable."""
    run_ids = []
    try:
        run_ids.append(ti.xcom_pull(task_ids=["jira_dbt_job"], key="return_value")[0])
    except (IndexError, TypeError):
        url = ti.xcom_pull(task_ids=["jira_dbt_job"], key="job_run_url")
        if url and url[0]:
            run_ids.append(int(str(url[0]).rstrip("/").split("/")[-1]))
    if run_ids:
        Variable.set(key="jira_dbt_job_run_ids", value=run_ids)
    return run_ids


dag = DAG(
    dag_id="etl_jira_import",
    default_args=default_args,
    schedule_interval="0 4,12 * * *",
    catchup=False,
    max_active_runs=1,
    max_active_tasks=20,
    tags=["jira", "service-desk", "support", "import"],
    doc_md=__doc__,
)

start = EmptyOperator(task_id="start", dag=dag)
pause = EmptyOperator(task_id="pause", dag=dag)
end = EmptyOperator(task_id="end", dag=dag)

execution_date = "{{ data_interval_end.strftime('%Y-%m-%d %H:%M') }}"
previous_execution_date = "{{ data_interval_start.strftime('%Y-%m-%d %H:%M') }}"
clean_execution_date = (
    "{{ data_interval_end.strftime('%Y-%m-%d %H:%M')"
    ".replace(':', '-').replace(' ', '_') }}"
)

extraction_tasks = []
load_tasks = []

for jira_project in JIRA_PROJECTS:
    small = jira_project.lower()

    if FULL_LOAD_MODE:
        date_range = get_jira_project_date_range(jira_project)
        if date_range:
            start_date_str, end_date_str = date_range
            monthly_ranges = generate_monthly_ranges(start_date_str, end_date_str)

            with TaskGroup(
                group_id=f"extract_jira_monthly_{small}", dag=dag
            ) as extract_monthly_group:
                for year_month, month_start, month_end in monthly_ranges:
                    monthly_file = (
                        f"/home/airflow/gcs/data/jira_{small}/"
                        f"{jira_project}_full_load_{year_month}_"
                        f"{clean_execution_date}.jsonl"
                    )
                    PythonOperator(
                        task_id=f"extract_monthly_{year_month}",
                        python_callable=get_jira_issues_by_date_range,
                        op_kwargs={
                            "project_key": jira_project,
                            "start_date": month_start,
                            "end_date": month_end,
                            "destination_path": monthly_file,
                        },
                        execution_timeout=timedelta(hours=3),
                        dag=dag,
                    )

            merge_task = PythonOperator(
                task_id=f"merge_monthly_files_{small}",
                python_callable=merge_monthly_files,
                op_kwargs={
                    "project_key": jira_project,
                    "clean_execution_date": clean_execution_date,
                },
                execution_timeout=timedelta(hours=2),
                dag=dag,
            )
            extract_monthly_group >> merge_task
            final_extract = merge_task
        else:
            final_extract = EmptyOperator(
                task_id=f"skip_full_load_{small}", dag=dag
            )
    else:
        final_extract = PythonOperator(
            task_id=f"extract_jira_incremental_{small}",
            python_callable=get_jira_issues_by_date_range,
            op_kwargs={
                "project_key": jira_project,
                "start_date": previous_execution_date,
                "end_date": execution_date,
                "destination_path": (
                    f"/home/airflow/gcs/data/jira_{small}/"
                    f"{jira_project}_{clean_execution_date}.jsonl"
                ),
            },
            execution_timeout=timedelta(hours=2),
            dag=dag,
        )

    if FULL_LOAD_MODE:
        source_object = (
            f"data/jira_{small}/{jira_project}_full_load_{clean_execution_date}.jsonl"
        )
        dest_object = (
            f"jira_{small}/{jira_project}_full_load_{clean_execution_date}.jsonl"
        )
    else:
        source_object = (
            f"data/jira_{small}/{jira_project}_{clean_execution_date}.jsonl"
        )
        dest_object = f"jira_{small}/{jira_project}_{clean_execution_date}.jsonl"

    upload = GCSToGCSOperator(
        task_id=f"upload_storage_{small}",
        source_bucket=COMPOSER_BUCKET,
        source_object=source_object,
        destination_bucket=BUCKET_NAME,
        destination_object=dest_object,
        gcp_conn_id=GCP_CONN_ID,
        retries=5,
        retry_delay=timedelta(minutes=2),
        dag=dag,
    )

    # JSONL loaded as one JSON column (tab delimiter so commas inside
    # payloads do not split). dbt owns field extraction + dedupe.
    load = GCSToBigQueryOperator(
        task_id=f"load_staging_{small}",
        bucket=BUCKET_NAME,
        source_objects=[dest_object],
        source_format="CSV",
        destination_project_dataset_table=(
            f"{PROJECT_ID}.trusted_staging.jira_{small}"
        ),
        skip_leading_rows=0,
        schema_fields=[{"name": "value", "type": "JSON", "mode": "NULLABLE"}],
        write_disposition="WRITE_APPEND",
        create_disposition="CREATE_IF_NEEDED",
        autodetect=False,
        field_delimiter="\t",
        gcp_conn_id=GCP_CONN_ID,
        dag=dag,
    )

    extraction_tasks.append(final_extract)
    load_tasks.append(load)
    final_extract >> upload >> load


if DBT_JOB_ID is not None and DbtCloudRunJobOperator is not None:
    jira_dbt_job = DbtCloudRunJobOperator(
        task_id="jira_dbt_job",
        job_id=int(DBT_JOB_ID),
        check_interval=10,
        timeout=600,
        dag=dag,
    )
else:
    jira_dbt_job = EmptyOperator(task_id="jira_dbt_job", dag=dag)

get_run_ids = PythonOperator(
    task_id="get_run_ids",
    python_callable=persist_dbt_run_id,
    dag=dag,
)

start >> extraction_tasks
load_tasks >> pause >> jira_dbt_job >> get_run_ids >> end
