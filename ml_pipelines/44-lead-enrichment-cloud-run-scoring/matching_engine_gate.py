"""Gate: proceed only when Vertex matching-engine output exists for today.

Used by the lead enrichment DAG as a BranchPythonOperator callable.
Fails soft (skip path) on empty results or query errors so a quiet day
does not page on_failure for a missing upstream batch.
"""

from __future__ import annotations

import logging
from typing import Any

from google.cloud import bigquery

logger = logging.getLogger(__name__)

PROCEED_TASK = "dbt_lead_enrichment_run"
SKIP_TASK = "skip_enrichment"


def check_matching_engine_data(
    *,
    client_project: str,
    matching_table: str,
    **_: Any,
) -> str:
    """Return the next task_id based on today's matching-engine row count.

    Args:
        client_project: BigQuery client / billing project.
        matching_table: Fully-qualified `project.dataset.table` for
            matching-engine SAM leads (or equivalent).
    """
    query = f"""
        SELECT COUNT(*) AS row_count
        FROM `{matching_table}`
        WHERE DATE(_create_ts) = CURRENT_DATE()
    """
    try:
        client = bigquery.Client(project=client_project)
        result = client.query(query).result()
        row_count = next(iter(result))["row_count"]
        if row_count > 0:
            logger.info(
                "Matching engine data found for today: %s rows — proceeding.",
                row_count,
            )
            return PROCEED_TASK
        logger.info("No matching engine data for today — skipping enrichment.")
        return SKIP_TASK
    except Exception as exc:  # noqa: BLE001 — gate must not fail the DAG hard
        logger.warning(
            "BQ matching-engine check failed; skipping enrichment. Error: %s",
            exc,
        )
        return SKIP_TASK
