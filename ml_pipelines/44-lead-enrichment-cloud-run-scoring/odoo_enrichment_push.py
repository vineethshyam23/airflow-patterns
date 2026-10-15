"""
Thin Odoo CRM write-back adapter for enriched + scored leads.

Full mapping (country/lang/channel refs, product code resolution,
address hash checks) lives in pattern 02 (`02-leads-ingestion/`) and
the production `Odoo.load_data_leads_enrichment` method. This stub
keeps the DAG importable as a portfolio sample.
"""

from __future__ import annotations

import logging
from typing import Any, Dict

logger = logging.getLogger(__name__)


class OdooEnrichmentPush:
    """Contract the DAG expects; wire pattern 02 engine for production."""

    def load_data_leads_enrichment(
        self,
        odoo_creds: Dict[str, Any],
        project_name: str,
        table_name: str,
    ) -> None:
        logger.info(
            "OdooEnrichmentPush.load_data_leads_enrichment stub — "
            "project=%s table=%s host=%s. "
            "Wire pattern 02 lead engine before production use.",
            project_name,
            table_name,
            odoo_creds.get("hostname"),
        )
