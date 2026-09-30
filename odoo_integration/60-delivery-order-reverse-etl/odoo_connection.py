"""
Thin OdooRPC connection helper used by the delivery-order reverse ETL.

Production wires the shared Connection class from pattern 05
(`05-connection-management/`). This stub keeps the sample importable.
"""

from __future__ import annotations

import logging
from typing import Any, Dict

logger = logging.getLogger(__name__)


class Connection:
    """Connect via OdooRPC (JSON-RPC + SSL). Wire pattern 05 in production."""

    def connect(self, odoo_creds: Dict[str, Any]) -> Any:
        try:
            import odoorpc
        except ImportError as exc:
            raise ImportError(
                "odoorpc is required for live Odoo writes; "
                "install it or wire pattern 05 Connection."
            ) from exc

        hostname = odoo_creds["hostname"]
        port = int(odoo_creds.get("port", 443))
        database = odoo_creds["database"]
        user = odoo_creds["user"]
        password = odoo_creds["password"]

        logger.info("Connecting to Odoo host=%s db=%s", hostname, database)
        odoo = odoorpc.ODOO(hostname, protocol="jsonrpc+ssl", port=port)
        odoo.login(database, user, password)
        return odoo
