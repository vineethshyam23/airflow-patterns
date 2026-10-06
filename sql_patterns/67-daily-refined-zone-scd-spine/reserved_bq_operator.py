"""Pin night-ETL BigQuery jobs onto a dedicated reservation.

Use ``ReservedBigQueryInsertJobOperator`` in Composer DAGs, and
``night_etl_query_job_config()`` for ``google.cloud.bigquery.Client.query()``.

Do not assign the whole GCP project to this reservation — console,
Tableau, and analyst jobs should stay on-demand. Rollback: stop using
these helpers and jobs fall back to the project default.

Source (read-only): ``dags/horeca_digital/bq_reservation.py``
"""

from __future__ import annotations

from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
)
from google.cloud.bigquery.job import QueryJobConfig

# Production used a hard-coded projects/.../reservations/... path.
# Prefer a Variable so env forks (dev Composer) do not share prod slots.
BQ_DWH_RESERVATION = Variable.get(
    "bq_night_etl_reservation",
    default_var=(
        "projects/dwh_project/locations/EU/reservations/bigquery-dwh-reservation"
    ),
)


def with_reservation(configuration: dict) -> dict:
    """Return a jobs.insert configuration that uses the night-ETL reservation."""
    config = dict(configuration)
    config["reservation"] = BQ_DWH_RESERVATION
    return config


def night_etl_query_job_config(**kwargs) -> QueryJobConfig:
    """QueryJobConfig for Client.query() calls from PythonOperators."""
    return QueryJobConfig(reservation=BQ_DWH_RESERVATION, **kwargs)


class ReservedBigQueryInsertJobOperator(BigQueryInsertJobOperator):
    """BigQueryInsertJobOperator pinned to the night-ETL reservation."""

    def __init__(self, *args, **kwargs):
        kwargs["configuration"] = with_reservation(kwargs["configuration"])
        super().__init__(*args, **kwargs)
