"""
Odoo.sh Postgres (PgBouncer) connect helper.

Production learned the hard way that Composer workers + PgBouncer need
sslmode=require, gssencmode=disable, and channel_binding=disable, plus
autocommit so statement pooling accepts SELECT. This is the thin helper
the DWH extract class uses — no OdooRPC, no parquet path.

Source (read-only):
  dags/horeca_digital/utils/odoo_utils.py (connect_odoo_postgres only)
"""

from __future__ import annotations

import psycopg2


def connect_odoo_postgres(
    host: str,
    database: str,
    user: str,
    password: str,
    port: int = 5432,
    connect_timeout: int = 45,
    autocommit: bool = True,
):
    """Open a Postgres connection compatible with Odoo.sh PgBouncer."""
    connect_kwargs = {
        "host": host,
        "database": database,
        "user": user,
        "password": password,
        "port": port or 5432,
        "sslmode": "require",
        "gssencmode": "disable",
        "connect_timeout": connect_timeout,
        "channel_binding": "disable",
    }
    try:
        conn = psycopg2.connect(**connect_kwargs)
    except TypeError:
        # Older psycopg2 builds reject channel_binding.
        connect_kwargs.pop("channel_binding", None)
        conn = psycopg2.connect(**connect_kwargs)
    if autocommit:
        conn.autocommit = True
    return conn
