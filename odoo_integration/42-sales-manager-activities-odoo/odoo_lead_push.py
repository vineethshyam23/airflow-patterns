"""
Thin Odoo CRM write-back adapter used by the SAM activities DAG.

Full lead mapping lives in pattern 02 (`02-leads-ingestion/`).
This module keeps the DAG importable as a portfolio sample.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


class OdooLeadPush:
    """Contract the DAG expects; wire pattern 02 implementation for production."""

    def load_data(self, odoo_creds: Dict[str, Any], project_name: str) -> None:
        logger.info(
            "OdooLeadPush.load_data stub — project=%s host=%s. "
            "Wire pattern 02 lead engine before production use.",
            project_name,
            odoo_creds.get("hostname"),
        )

    def lead_engine_monitoring(
        self,
        odoo_creds: Dict[str, Any],
        project: str,
        **_: Any,
    ) -> Dict[str, Optional[str]]:
        logger.info("OdooLeadPush.lead_engine_monitoring stub — project=%s", project)
        return {
            "status": "STUB",
            "sam": "not_checked",
            "odoo": "not_checked",
        }
