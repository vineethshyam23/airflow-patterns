"""
Thin lazy Postgres client for Odoo ID extracts.

Production wired this DAG through a large `OdooDataPull` module that
also owns full-table pulls. For snapshot cleanup we only need a
connection + cursor, so the reusable bit is isolated here.

Connection opens on first access (not at DAG parse) — Composer
schedulers used to time out when import-time connects piled up.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Dict, Optional

import psycopg2
from airflow.models import Variable


class OdooPostgresClient:
    """Lazy Odoo Postgres connection with short connect retries."""

    def __init__(
        self,
        env: str = "DM",
        project_id: str = "dwh_project_dev",
        creds: Optional[Dict[str, Any]] = None,
    ):
        self.env = env
        self.project_id = project_id
        if creds is None:
            var_name = "odoo_dm_creds" if env == "DM" else "odoo_prod_creds"
            creds = Variable.get(var_name, deserialize_json=True)
        self._endpoint = creds["hostname"]
        self._db_name = creds["database"]
        self._db_user = creds["db_user"]
        self._db_pwd = creds["db_pwd"]
        self._conn = None

    @property
    def conn(self):
        if self._conn is not None and not self._conn.closed:
            return self._conn

        last_error = None
        for attempt in range(1, 4):
            logging.info(
                "Opening Odoo PostgreSQL connection host=%s attempt=%s/3",
                self._endpoint,
                attempt,
            )
            try:
                # Composer workers sometimes leave a wait callback that
                # hangs SSL handshakes on long pulls — disable it.
                psycopg2.extensions.set_wait_callback(None)
                self._conn = psycopg2.connect(
                    host=self._endpoint,
                    database=self._db_name,
                    user=self._db_user,
                    password=self._db_pwd,
                    sslmode="require",
                )
                logging.info(
                    "Opened Odoo PostgreSQL connection host=%s", self._endpoint
                )
                return self._conn
            except psycopg2.OperationalError as exc:
                last_error = exc
                logging.warning(
                    "Odoo Postgres connect attempt %s/3 failed host=%s: %s",
                    attempt,
                    self._endpoint,
                    exc,
                )
                if attempt < 3:
                    time.sleep(10 * attempt)
        raise last_error

    def close(self) -> None:
        if self._conn is not None and not self._conn.closed:
            self._conn.close()
            logging.info("Closed Odoo PostgreSQL connection")
        self._conn = None
