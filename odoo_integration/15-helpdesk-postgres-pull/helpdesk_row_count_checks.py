"""
Lightweight row-count checks for Odoo helpdesk vs warehouse views.

Used after a pull (or independently) to compare create/update volumes
for a given execution date. Returns plain strings for Slack/email
templates — not a hard gate.

Source (read-only): dags/horeca_digital/helpdesk_odoo.py
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict

import psycopg2
from google.cloud import bigquery


class HelpdeskRowCountChecks:
    @staticmethod
    def _bq_count(project: str, sql: str) -> Any:
        try:
            client = bigquery.Client(project=project)
            return client.query(sql).result().total_rows
        except Exception as exc:  # keep ops-friendly; caller logs
            return exc

    @staticmethod
    def warehouse_counts(project: str, execution_date: str) -> Dict[str, str]:
        """
        Counts from the trusted helpdesk view for creates vs updates
        on execution_date.
        """
        inserts = HelpdeskRowCountChecks._bq_count(
            project,
            f"""
            SELECT DISTINCT COUNT(external_id)
            FROM `{project}.trusted_views.odoo_helpdesk`
            WHERE DATE(create_date) = '{execution_date}'
            """,
        )
        updates = HelpdeskRowCountChecks._bq_count(
            project,
            f"""
            SELECT DISTINCT COUNT(external_id)
            FROM `{project}.trusted_views.odoo_helpdesk`
            WHERE DATE(update_date) = '{execution_date}'
              AND DATE(create_date) <> '{execution_date}'
            """,
        )
        return {
            "insert": f"Warehouse helpdesk creates on {execution_date}: {inserts}",
            "update": f"Warehouse helpdesk updates on {execution_date}: {updates}",
        }

    @staticmethod
    def odoo_counts(**kwargs) -> Dict[str, str]:
        """
        Same create/update split against live Odoo Postgres.
        Pass host/db_name/db_user/db_pwd/execution_date via kwargs.
        """
        psycopg2.extensions.set_wait_callback(None)
        conn = psycopg2.connect(
            host=kwargs["host"],
            database=kwargs["db_name"],
            user=kwargs["db_user"],
            password=kwargs["db_pwd"],
            sslmode="require",
        )
        execution_date = datetime.fromisoformat(kwargs["execution_date"]).date()
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(
            "SELECT count(*) FROM helpdesk_ticket "
            f"WHERE date(create_date) = '{execution_date}'"
        )
        insert_count = cur.fetchone()[0]
        cur.execute(
            "SELECT count(*) FROM helpdesk_ticket "
            f"WHERE date(write_date) = '{execution_date}' "
            f"AND date(create_date) <> '{execution_date}'"
        )
        update_count = cur.fetchone()[0]
        cur.close()
        conn.close()
        return {
            "insert": (
                f"Odoo helpdesk creates on {execution_date}: {insert_count}"
            ),
            "update": (
                f"Odoo helpdesk updates on {execution_date}: {update_count}"
            ),
        }
