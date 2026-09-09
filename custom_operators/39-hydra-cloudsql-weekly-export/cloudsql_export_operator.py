"""Schedule-aware Cloud SQL export operators.

Cloud SQL Admin allows only one long-running operation per instance.
Weekly multi-table dumps collide with automated backups and with each
other. These subclasses retry 409 ``operationInProgress`` and stretch
backoff when the wall clock sits in a known backup window.

Source (read-only):
  dags/horeca_digital/operators/cloudsql_retry_operator.py
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any

from airflow.contrib.operators.gcp_sql_operator import CloudSqlInstanceExportOperator
from googleapiclient.errors import HttpError

logger = logging.getLogger(__name__)


class CloudSqlExportOperatorWithRetry(CloudSqlInstanceExportOperator):
    """Retry Cloud SQL export on 409 operationInProgress conflicts."""

    template_fields = tuple(
        getattr(CloudSqlInstanceExportOperator, "template_fields", ())
    ) + ("body",)

    def __init__(
        self,
        max_operation_retries: int = 10,
        operation_retry_delay: int = 300,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.max_operation_retries = max_operation_retries
        self.operation_retry_delay = operation_retry_delay

    def execute(self, context):
        for attempt in range(1, self.max_operation_retries + 1):
            try:
                logger.info(
                    "Cloud SQL export attempt %s/%s",
                    attempt,
                    self.max_operation_retries,
                )
                return super().execute(context)
            except HttpError as exc:
                if exc.resp.status == 409 and "operationInProgress" in str(exc):
                    if attempt < self.max_operation_retries:
                        wait_time = self.operation_retry_delay * attempt
                        logger.warning(
                            "operationInProgress (attempt %s); sleeping %ss",
                            attempt,
                            wait_time,
                        )
                        time.sleep(wait_time)
                        continue
                    logger.error(
                        "Gave up after %s operationInProgress retries",
                        self.max_operation_retries,
                    )
                    raise
                logger.error("Non-retryable HTTP error during Cloud SQL export")
                raise
            except Exception:
                logger.exception("Non-HTTP error during Cloud SQL export")
                raise

        raise RuntimeError(
            f"Export failed after {self.max_operation_retries} attempts"
        )


class CloudSqlExportOperatorWithScheduleAware(CloudSqlExportOperatorWithRetry):
    """Double retry delay when wall clock falls in a backup window."""

    def __init__(
        self,
        backup_window_start_hour: int = 3,
        backup_window_duration_hours: int = 2,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.backup_window_start_hour = backup_window_start_hour
        self.backup_window_duration_hours = backup_window_duration_hours

    def _is_likely_backup_time(self) -> bool:
        current_hour = datetime.now(timezone.utc).hour
        start = self.backup_window_start_hour
        end = (start + self.backup_window_duration_hours) % 24
        if end > start:
            return start <= current_hour < end
        return current_hour >= start or current_hour < end

    def execute(self, context):
        if not self._is_likely_backup_time():
            return super().execute(context)

        logger.info(
            "Inside typical backup window; doubling operation_retry_delay"
        )
        original_delay = self.operation_retry_delay
        self.operation_retry_delay = original_delay * 2
        try:
            return super().execute(context)
        finally:
            self.operation_retry_delay = original_delay
