"""Pin BigQuery insert jobs onto a capacity reservation.

Production wrapped BigQueryInsertJobOperator so the long sequential
change-detection chain hits the DWH reservation instead of on-demand
slots. Do not assign the whole GCP project to that reservation —
console and analyst jobs stay on-demand.
"""

from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# Example reservation path — replace with your project/location/name.
BQ_DWH_RESERVATION = (
    "projects/dwh_project/locations/EU/reservations/bigquery-dwh-reservation"
)


def with_reservation(configuration: dict) -> dict:
    """Return a jobs.insert configuration pinned to the night-ETL reservation."""
    config = dict(configuration)
    config["reservation"] = BQ_DWH_RESERVATION
    return config


class ReservedBigQueryInsertJobOperator(BigQueryInsertJobOperator):
    """BigQueryInsertJobOperator pinned to the night-ETL reservation."""

    def __init__(self, *args, **kwargs):
        kwargs["configuration"] = with_reservation(kwargs["configuration"])
        super().__init__(*args, **kwargs)
