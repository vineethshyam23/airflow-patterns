"""
Airflow DAG: monthly absolute (multi-channel) establishment activity scores.

Layered BigQuery pipeline:
  1. Base extracts (events, reservations, Adobe / analytics logins)
  2. Per-channel rolling activity tables (WRITE_APPEND, month-partitioned)
  3. Fan-in score (0–3) against external KPI thresholds
  4. MoM upscore / downscore / unchanged transition flags

Source (read-only):
  dags/absolute_activityscores.py

Production later replaced the Odoo-adjusted sibling with a dbt Cloud job
(`etl_activity_score_job`). This sample keeps the Composer + SQL shape —
useful when you need to reason about fan-in ordering and partition
contracts before moving logic into dbt.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

import activity_score_queries as q

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "start_date": datetime(2022, 1, 1),
    "email": ["dataops@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

project = Variable.get("activity_score_bq_project", default_var=q.DEFAULT_PROJECT)
bq_conn = Variable.get("activity_score_bq_conn", default_var="bigquery_default")


def _insert_job(
    *,
    task_id: str,
    sql: str,
    dataset: str,
    table: str,
    write_disposition: str,
    partition_field: str | None = None,
) -> BigQueryInsertJobOperator:
    query_cfg: dict = {
        "query": sql,
        "useLegacySql": False,
        "destinationTable": {
            "projectId": project,
            "datasetId": dataset,
            "tableId": table,
        },
        "writeDisposition": write_disposition,
        "createDisposition": "CREATE_IF_NEEDED",
        "allowLargeResults": True,
    }
    if partition_field:
        query_cfg["timePartitioning"] = {"type": "DAY", "field": partition_field}

    return BigQueryInsertJobOperator(
        task_id=task_id,
        configuration={"query": query_cfg},
        gcp_conn_id=bq_conn,
        dag=dag,
    )


dag = DAG(
    dag_id="absolute_activity_scores",
    default_args=default_args,
    schedule_interval="15 7 1 * *",
    catchup=False,
    max_active_runs=1,
    tags=["scoring", "activity-score", "monthly", "bigquery"],
    doc_md=(
        "Monthly multi-channel activity score (0–3) with MoM transitions. "
        "See scoring_analytics/47-absolute-activity-scores/."
    ),
)

# --- base extracts (truncate) ---
as_absolute_base_wb_rt_event = _insert_job(
    task_id="as_absolute_base_wb_rt_event",
    sql=q.query_as_absolute_base_wb_rt_event(project),
    dataset=q.REFINED,
    table="as_absolute_base_wb_rt_event",
    write_disposition="WRITE_TRUNCATE",
    partition_field="event_date",
)
as_absolute_base_rt_reservation = _insert_job(
    task_id="as_absolute_base_rt_reservation",
    sql=q.query_as_absolute_base_rt_reservation(project),
    dataset=q.REFINED,
    table="as_absolute_base_rt_reservation",
    write_disposition="WRITE_TRUNCATE",
    partition_field="reservation_month",
)
as_absolute_base_adobe = _insert_job(
    task_id="as_absolute_base_adobe",
    sql=q.query_as_absolute_base_adobe(project),
    dataset=q.REFINED,
    table="as_absolute_base_adobe",
    write_disposition="WRITE_TRUNCATE",
    partition_field="month",
)
as_absolute_base_adobe_mk = _insert_job(
    task_id="as_absolute_base_adobe_mk",
    sql=q.query_as_absolute_base_adobe_mk(project),
    dataset=q.REFINED,
    table="as_absolute_base_adobe_mk",
    write_disposition="WRITE_TRUNCATE",
    partition_field="last_date_mk_login",
)

# --- per-channel activity (append, month-partitioned) ---
as_absolute_wb_login_visitor = _insert_job(
    task_id="as_absolute_wb_login_visitor",
    sql=q.query_as_absolute_wb_login_visitor(project),
    dataset=q.REFINED,
    table="as_absolute_wb_login_visitor",
    write_disposition="WRITE_APPEND",
    partition_field="as_month",
)
as_absolute_wb_event = _insert_job(
    task_id="as_absolute_wb_event",
    sql=q.query_as_absolute_wb_event(project),
    dataset=q.REFINED,
    table="as_absolute_wb_event",
    write_disposition="WRITE_APPEND",
    partition_field="as_month",
)
as_absolute_rt_reservation = _insert_job(
    task_id="as_absolute_rt_reservation",
    sql=q.query_as_absolute_rt_reservation(project),
    dataset=q.REFINED,
    table="as_absolute_rt_reservation",
    write_disposition="WRITE_APPEND",
    partition_field="as_month",
)
as_absolute_rt_event = _insert_job(
    task_id="as_absolute_rt_event",
    sql=q.query_as_absolute_rt_event(project),
    dataset=q.REFINED,
    table="as_absolute_rt_event",
    write_disposition="WRITE_APPEND",
    partition_field="as_month",
)
as_absolute_rt_login = _insert_job(
    task_id="as_absolute_rt_login",
    sql=q.query_as_absolute_rt_login(project),
    dataset=q.REFINED,
    table="as_absolute_rt_login",
    write_disposition="WRITE_APPEND",
    partition_field="as_month",
)
as_absolute_wl_login = _insert_job(
    task_id="as_absolute_wl_login",
    sql=q.query_as_absolute_wl_login(project),
    dataset=q.REFINED,
    table="as_absolute_wl_login",
    write_disposition="WRITE_APPEND",
    partition_field="as_month",
)
as_absolute_mk_login = _insert_job(
    task_id="as_absolute_mk_login",
    sql=q.query_as_absolute_mk_login(project),
    dataset=q.REFINED,
    table="as_absolute_mk_login",
    write_disposition="WRITE_APPEND",
    partition_field="as_month",
)
as_absolute_sfdc_call = _insert_job(
    task_id="as_absolute_sfdc_call",
    sql=q.query_as_absolute_sfdc_call(project),
    dataset=q.REFINED,
    table="as_absolute_sfdc_call",
    write_disposition="WRITE_APPEND",
    partition_field="as_month",
)
as_absolute_do_login_event = _insert_job(
    task_id="as_absolute_do_login_event",
    sql=q.query_as_absolute_do_login_event(project),
    dataset=q.REFINED,
    table="as_absolute_do_login_event",
    write_disposition="WRITE_APPEND",
    partition_field="as_month",
)
as_absolute_do_order = _insert_job(
    task_id="as_absolute_do_order",
    sql=q.query_as_absolute_do_order(project),
    dataset=q.REFINED,
    table="as_absolute_do_order",
    write_disposition="WRITE_APPEND",
    partition_field="as_month",
)
as_absolute_portal = _insert_job(
    task_id="as_absolute_portal",
    sql=q.query_as_absolute_portal(project),
    dataset=q.REFINED,
    table="as_absolute_portal",
    write_disposition="WRITE_APPEND",
    partition_field="as_month",
)
as_absolute_mobile_app = _insert_job(
    task_id="as_absolute_mobile_app",
    sql=q.query_as_absolute_mobile_app(project),
    dataset=q.REFINED,
    table="as_absolute_mobile_app",
    write_disposition="WRITE_APPEND",
    partition_field="as_month",
)

# --- fan-in score + MoM transitions ---
as_absolute_activity_score = _insert_job(
    task_id="as_absolute_activity_score",
    sql=q.query_as_absolute_activity_score(project),
    dataset=q.REFINED,
    table="as_absolute_activity_score",
    write_disposition="WRITE_APPEND",
    partition_field="as_month",
)
ce_activity_score_transitions = _insert_job(
    task_id="ce_activity_score_transitions",
    sql=q.query_ce_activity_score_transitions(project),
    dataset=q.TRUSTED,
    table="ce_activity_score_transitions",
    write_disposition="WRITE_TRUNCATE",
)

# Dependency graph mirrors production: bases fan into channels, all
# channels fan into the score, score feeds MoM transition flags.
as_absolute_base_adobe >> [
    as_absolute_wb_login_visitor,
    as_absolute_rt_login,
    as_absolute_wl_login,
    as_absolute_do_login_event,
] >> as_absolute_activity_score

as_absolute_base_wb_rt_event >> as_absolute_wb_event >> as_absolute_activity_score
as_absolute_base_rt_reservation >> as_absolute_rt_reservation >> as_absolute_activity_score
as_absolute_base_wb_rt_event >> as_absolute_rt_event >> as_absolute_activity_score
as_absolute_base_adobe_mk >> as_absolute_mk_login >> as_absolute_activity_score

[
    as_absolute_do_order,
    as_absolute_sfdc_call,
    as_absolute_portal,
    as_absolute_mobile_app,
] >> as_absolute_activity_score

as_absolute_activity_score >> ce_activity_score_transitions
