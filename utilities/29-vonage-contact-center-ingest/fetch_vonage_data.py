"""Vonage Contact Center stats API → NDJSON on the Composer data volume.

Pulls five report grains (agent activities / presence / status,
interactions, queue times) via OAuth2 client-credentials, paginates
with limit/page, and writes newline-delimited JSON so a later
GCSToGCSOperator can promote the file into the raw zone.

Credentials belong in an Airflow Variable (`vonage_creds` JSON with
client_id / client_secret). Never hardcode them — production once
had a working pair in a `__main__` block; that is gone here.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union
from urllib.parse import urlencode

import requests
from google.cloud import bigquery

LOG = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://emea.api.cc.vonage.com"
DEFAULT_AUTH_URL = "https://emea.cc.vonage.com/Auth/connect/token"
DEFAULT_OUTPUT_PATH = Path("/home/airflow/gcs/data/vonage")
DEFAULT_LIMIT = 500

# Logical file name → Contact Center stats path
VONAGE_ENDPOINTS = {
    "vonage_agent_activities": "/stats/agent-activities",
    "vonage_agent_presence": "/stats/agent-activities/presence",
    "vonage_agent_status": "/stats/agent-status",
    "vonage_interactions": "/stats/interactions",
    "vonage_queue_times": "/stats/queue-times",
}

# Refined dataset used by the post-dbt row-count check
REFINED_DATASET = "refined_sales"
DEFAULT_PROJECT = "dwh_project"


class VonageAPIError(Exception):
    """Raised for Vonage auth / fetch failures that should fail the task."""


class VonageAPI:
    """Thin client: token, paginated GET, NDJSON write."""

    def __init__(
        self,
        bearer_token: Optional[str] = None,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        base_url: str = DEFAULT_BASE_URL,
        auth_url: str = DEFAULT_AUTH_URL,
    ) -> None:
        if not bearer_token and not (client_id and client_secret):
            raise ValueError(
                "Provide bearer_token or both client_id and client_secret"
            )

        self.bearer_token = bearer_token
        self.client_id = client_id
        self.client_secret = client_secret
        self.base_url = base_url
        self.auth_url = auth_url
        self.start_date = start_date
        self.end_date = end_date

    def get_token(self) -> str:
        """OAuth2 client-credentials with scope=stats."""
        if not self.client_id or not self.client_secret:
            raise ValueError("client_id and client_secret required for token")

        encoded = urlencode(
            {
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "scope": "stats",
            }
        )
        response = requests.post(
            self.auth_url,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data=encoded,
            timeout=60,
        )
        if response.status_code != 200:
            raise VonageAPIError(
                f"Token HTTP {response.status_code}: {response.text[:300]}"
            )

        access = response.json()["access_token"]
        self.bearer_token = f"Bearer {access}"
        LOG.info("Obtained Vonage bearer token")
        return self.bearer_token

    def _default_yesterday_window(self) -> Tuple[str, str]:
        yesterday = datetime.now() - timedelta(days=1)
        start = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
        end = yesterday.replace(hour=23, minute=59, second=59, microsecond=999000)
        return (
            start.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            end.strftime("%Y-%m-%dT%H:%M:%S.999Z"),
        )

    def _fetch_pages(
        self,
        endpoint: str,
        params: Dict[str, Union[str, int, None]],
        limit: int = DEFAULT_LIMIT,
    ) -> Tuple[List[Dict[str, Any]], int]:
        if not self.bearer_token:
            self.get_token()

        clean = {k: v for k, v in params.items() if v is not None}
        clean.update({"limit": limit, "page": 1})
        headers = {
            "Accept": "application/vnd.newvoicemedia.v3+json",
            "Authorization": self.bearer_token,
        }

        all_items: List[Dict[str, Any]] = []
        total_count = 0
        page = 1

        while True:
            url = f"{self.base_url}{endpoint}?{urlencode(clean)}"
            response = requests.get(url, headers=headers, timeout=60)

            # Token can expire mid-pagination; refresh once and retry.
            if response.status_code in (401, 403):
                LOG.warning("Token rejected (%s); refreshing", response.status_code)
                self.bearer_token = None
                self.get_token()
                headers["Authorization"] = self.bearer_token
                response = requests.get(url, headers=headers, timeout=60)

            if response.status_code != 200:
                raise VonageAPIError(
                    f"GET {endpoint} HTTP {response.status_code}: "
                    f"{response.text[:300]}"
                )

            payload = response.json()
            items = payload.get("items") or []
            all_items.extend(items)

            if page == 1:
                total_count = int(payload.get("meta", {}).get("totalCount", 0))
                LOG.info("Endpoint %s reports totalCount=%s", endpoint, total_count)

            LOG.info("Page %s: %s records", page, len(items))

            if not items or len(all_items) >= total_count:
                break

            page += 1
            clean["page"] = page

        return all_items, total_count

    def get_data(
        self, file_name: str, include_processed: bool = True, limit: int = DEFAULT_LIMIT
    ) -> Tuple[List[Dict[str, Any]], int]:
        if file_name not in VONAGE_ENDPOINTS:
            raise ValueError(f"Unknown Vonage file_name: {file_name}")

        endpoint = VONAGE_ENDPOINTS[file_name]

        # Agent status is a point-in-time snapshot; no start/end filter.
        if file_name == "vonage_agent_status":
            params: Dict[str, Union[str, int, None]] = {}
        else:
            start, end = (
                self._default_yesterday_window()
                if not (self.start_date and self.end_date)
                else (self.start_date, self.end_date)
            )
            params = {
                "start": start,
                "end": end,
                "include": "Processed" if include_processed else None,
            }

        return self._fetch_pages(endpoint, params, limit)

    @staticmethod
    def save_ndjson(data: List[Dict[str, Any]], file_path: Path) -> None:
        file_path.parent.mkdir(parents=True, exist_ok=True)
        with file_path.open("w", encoding="utf-8") as handle:
            for item in data:
                json.dump(item, handle, ensure_ascii=False, separators=(",", ":"))
                handle.write("\n")
        LOG.info("Wrote %s records to %s", len(data), file_path)


def get_vonage_data(**kwargs) -> Dict[str, Union[str, int]]:
    """Extract one Vonage grain and write NDJSON. Used by PythonOperator."""
    bearer_token = kwargs.get("bearer_token")
    client_id = kwargs.get("client_id")
    client_secret = kwargs.get("client_secret")
    start_date = kwargs.get("start_date")
    end_date = kwargs.get("end_date")
    file_name = kwargs.get("file_name")
    output_path = Path(kwargs.get("output_path", DEFAULT_OUTPUT_PATH))

    if not file_name:
        raise ValueError("file_name is required")

    api = VonageAPI(
        bearer_token=bearer_token,
        client_id=client_id,
        client_secret=client_secret,
        start_date=start_date,
        end_date=end_date,
    )
    data, total_count = api.get_data(file_name)
    out = output_path / f"{file_name}.ndjson"
    api.save_ndjson(data, out)

    return {
        "api_records_count": total_count,
        "extracted_records_count": len(data),
        "file_name": file_name,
        "start_date": start_date or "default (yesterday)",
        "end_date": end_date or "default (yesterday)",
        "file_path": str(out),
        "status": "success",
    }


def get_loaded_data_count(**kwargs) -> int:
    """Count refined rows loaded today for Slack / ops reconciliation.

    Production queried a fixed project. Sample reads project from kwargs
    or falls back to DEFAULT_PROJECT so the helper stays portable.
    """
    table_name = kwargs.get("table_name")
    project = kwargs.get("project_id", DEFAULT_PROJECT)
    if not table_name:
        raise ValueError("table_name is required")

    client = bigquery.Client()
    query = f"""
        SELECT COUNT(1) AS record_count
        FROM `{project}.{REFINED_DATASET}.{table_name}`
        WHERE loaded_date = CURRENT_DATE()
    """
    LOG.info("Counting loaded rows for %s.%s.%s", project, REFINED_DATASET, table_name)
    for row in client.query(query).result():
        return int(row.record_count)
    return 0
