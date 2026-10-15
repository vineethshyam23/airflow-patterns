"""Manual BigQuery Data Transfer kick + fixed wait.

Production slept a fixed 360s after start_manual_transfer_runs and
did not poll run state. That is cheap and usually enough for this
config; it is also a silent failure mode if the transfer is slow or
errors. Prefer polling TransferRun.state in a rewrite.

Config name comes from an Airflow Variable so project numbers and
transfer UUIDs never live in source.
"""

from __future__ import annotations

import logging
import time

from airflow.models import Variable

try:
    from google.cloud import bigquery_datatransfer_v1
    from google.protobuf.timestamp_pb2 import Timestamp
except ImportError:  # pragma: no cover - reference checkout without GCP libs
    bigquery_datatransfer_v1 = None
    Timestamp = None

logger = logging.getLogger(__name__)

DEFAULT_WAIT_SECONDS = 360


def run_data_transfer(**_context) -> str:
    """Start one manual transfer run and block for DEFAULT_WAIT_SECONDS."""
    if bigquery_datatransfer_v1 is None or Timestamp is None:
        raise ImportError(
            "google-cloud-bigquery-datatransfer is required to run the transfer task"
        )

    transfer_config_name = Variable.get("booq_ga4_transfer_config")
    wait_seconds = int(
        Variable.get("booq_ga4_transfer_wait_seconds", default_var=str(DEFAULT_WAIT_SECONDS))
    )

    client = bigquery_datatransfer_v1.DataTransferServiceClient()
    start_time = Timestamp(seconds=int(time.time()))
    response = client.start_manual_transfer_runs(
        {
            "parent": transfer_config_name,
            "requested_run_time": start_time,
        }
    )
    logger.info(
        "Started GA4 transfer; sleeping %ss (not polling run state)",
        wait_seconds,
    )
    time.sleep(wait_seconds)
    # Response is logged for ops triage; production did not assert
    # SUCCESS. Keep that behaviour so the sample matches what ran.
    logger.info("Transfer start response: %s", response)
    return str(response)


def collect_dbt_run_ids(ti, task_ids: list[str], variable_key: str) -> list:
    """Pull dbt Cloud run ids from XCom and stash them in a Variable."""
    runids = []
    for task_id in task_ids:
        try:
            runids.append(ti.xcom_pull(task_ids=[task_id], key="return_value")[0])
        except (IndexError, TypeError):
            job_url = ti.xcom_pull(task_ids=[task_id], key="job_run_url")
            if not job_url:
                continue
            parts = list(filter(None, job_url[0].split("/")))
            runids.append(int(parts[-1]))

    Variable.set(key=variable_key, value=runids)
    return runids
