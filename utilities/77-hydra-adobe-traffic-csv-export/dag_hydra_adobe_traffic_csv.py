"""DAG: Hydra / Adobe site-traffic refined tables → CSV publish bucket.

Daily job that extracts five refined traffic metric tables for a
two-day rolling window, stages CSVs on the Composer bucket, then
publishes them to a consumer bucket with overwrite + retention.

Source (read-only): dags/etl_adobe_traffic_data.py
"""

from datetime import timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

from traffic_csv_export import (
    cleanup_source_files,
    copy_files_to_target_bucket,
    delete_old_files_from_target_bucket,
    delete_target_files_for_date_range,
    generate_copy_summary_report,
    generate_summary_report,
    process_all_tables_and_dates,
)

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "email": ["dataops@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=10),
}

with DAG(
    dag_id="etl_site_traffic_csv_publish",
    default_args=default_args,
    description=(
        "Publish refined site-traffic metrics as dated CSVs "
        "(staging → overwrite → retention)"
    ),
    schedule_interval="0 7 * * *",
    start_date=days_ago(1),
    catchup=False,
    dagrun_timeout=timedelta(hours=12),
    tags=["file-processing", "bigquery", "adobe", "site-traffic"],
) as dag:
    start = EmptyOperator(task_id="start")

    delete_old_files = PythonOperator(
        task_id="delete_old_files",
        python_callable=delete_old_files_from_target_bucket,
        execution_timeout=timedelta(hours=1),
    )

    extract_and_upload = PythonOperator(
        task_id="extract_and_upload_csv_files",
        python_callable=process_all_tables_and_dates,
        execution_timeout=timedelta(hours=6),
    )

    delete_target_files = PythonOperator(
        task_id="delete_target_files",
        python_callable=delete_target_files_for_date_range,
        execution_timeout=timedelta(hours=1),
    )

    copy_files = PythonOperator(
        task_id="copy_files_to_targets",
        python_callable=copy_files_to_target_bucket,
        execution_timeout=timedelta(hours=2),
    )

    copy_summary = PythonOperator(
        task_id="generate_copy_summary",
        python_callable=generate_copy_summary_report,
        execution_timeout=timedelta(minutes=15),
    )

    extract_summary = PythonOperator(
        task_id="generate_summary_report",
        python_callable=generate_summary_report,
        execution_timeout=timedelta(minutes=30),
    )

    cleanup = PythonOperator(
        task_id="cleanup_source_files",
        python_callable=cleanup_source_files,
        execution_timeout=timedelta(minutes=30),
    )

    end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    (
        start
        >> delete_old_files
        >> extract_and_upload
        >> delete_target_files
        >> copy_files
        >> copy_summary
        >> extract_summary
        >> cleanup
        >> end
    )
