"""Daily derived-events change-detection DAG (representative subset).

Production is a ~58-task sequential monolith that appends CMS, Adobe,
and Reservation Tool change events into trusted.derived_events. This
file keeps six tasks — two CMS, one Adobe, three Reservation — that
cover every detection style used in that chain.

Downstream consumers (activity scores, engagement MVs) read the shared
append-only event store; they do not care which task wrote the row.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable

from bq_reservation import ReservedBigQueryInsertJobOperator
from event_queries import (
    query_adobe_datafeed,
    query_cms_loc_name_change,
    query_cms_modification_date,
    query_reservation_auto_arrivals,
    query_reservation_channels_change,
    query_reservation_user_logindate,
)

BQ_PROJECT = Variable.get("derived_events_bq_project", default_var="dwh_project")
BQ_CONN = Variable.get("derived_events_bq_conn", default_var="bigquery_default")
DESTINATION = "trusted.derived_events"
DEST_DATASET, DEST_TABLE = DESTINATION.split(".", 1)

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "start_date": datetime(2018, 10, 25),
    "email": ["dataops@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}


def _append_job(sql: str) -> dict:
    return {
        "query": {
            "query": sql,
            "useLegacySql": False,
            "destinationTable": {
                "projectId": BQ_PROJECT,
                "datasetId": DEST_DATASET,
                "tableId": DEST_TABLE,
            },
            "writeDisposition": "WRITE_APPEND",
            "allowLargeResults": True,
        }
    }


with DAG(
    dag_id="etl_derived_events",
    default_args=default_args,
    schedule_interval="30 5 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["derived_events", "cms", "adobe", "reservation", "change_detection"],
    description=(
        "Append SCD / hit-derived business events into trusted.derived_events "
        "(representative CMS + Adobe + Reservation subset)."
    ),
) as dag:

    event_cms_modification = ReservedBigQueryInsertJobOperator(
        task_id="event_establishment_modification_date",
        configuration=_append_job(query_cms_modification_date()),
        gcp_conn_id=BQ_CONN,
    )

    event_cms_loc_name = ReservedBigQueryInsertJobOperator(
        task_id="event_establishment_loc_name_change",
        configuration=_append_job(query_cms_loc_name_change(DESTINATION)),
        gcp_conn_id=BQ_CONN,
    )

    event_adobe = ReservedBigQueryInsertJobOperator(
        task_id="event_adobe_datafeed",
        configuration=_append_job(query_adobe_datafeed(DESTINATION)),
        gcp_conn_id=BQ_CONN,
    )

    event_rt_login = ReservedBigQueryInsertJobOperator(
        task_id="event_rt_user_logindate",
        configuration=_append_job(query_reservation_user_logindate(DESTINATION)),
        gcp_conn_id=BQ_CONN,
    )

    event_rt_auto_arrivals = ReservedBigQueryInsertJobOperator(
        task_id="event_rt_establishment_auto_arrivals",
        configuration=_append_job(query_reservation_auto_arrivals(DESTINATION)),
        gcp_conn_id=BQ_CONN,
    )

    event_rt_channels = ReservedBigQueryInsertJobOperator(
        task_id="event_rt_reservationchannels_change",
        configuration=_append_job(query_reservation_channels_change(DESTINATION)),
        gcp_conn_id=BQ_CONN,
    )

    # Production chains all ~58 tasks sequentially. Kept here so the
    # failure-blocks-downstream behaviour stays visible; in a rewrite I
    # would TaskGroup CMS / Adobe / Reservation and run the groups in
    # parallel.
    (
        event_cms_modification
        >> event_cms_loc_name
        >> event_adobe
        >> event_rt_login
        >> event_rt_auto_arrivals
        >> event_rt_channels
    )
