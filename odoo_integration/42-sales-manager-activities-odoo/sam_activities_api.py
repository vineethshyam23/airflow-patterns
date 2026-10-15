"""
Field-sales activities API client.

OAuth2 password grant + paginated country activities fetch.
Writes newline-delimited JSON for GCS → BigQuery load.

Sanitized from production Composer module. No live credentials.
"""

from __future__ import annotations

import base64
import json
import logging
import os
import time
from datetime import date, datetime, timedelta
from typing import Any, Callable, Dict, Final, Optional

import requests

logger = logging.getLogger(__name__)


class SalesManagerActivitiesAPI:
    """Fetch completed field-sales activities for one country / date window."""

    def __init__(
        self,
        oauth2_url: str,
        client_id: str,
        client_secret: str,
        username: str,
        password: str,
        base_url: str,
        category_name: str = "Hospitality Digital",
        max_retries: int = 3,
        retry_wait_sec: int = 60,
        request_timeout_sec: int = 60,
    ) -> None:
        self.oauth2_url = oauth2_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.username = username
        self.password = password
        self.base_url = base_url.rstrip("/") + "/"
        self.category_name = category_name
        self.max_retries = max_retries
        self.retry_wait_sec = retry_wait_sec
        self.request_timeout_sec = request_timeout_sec
        self.token: Optional[str] = None

    @staticmethod
    def _b64(value: str) -> str:
        return base64.b64encode(value.encode()).decode()

    def get_token(self) -> str:
        headers = {
            "Authorization": "Basic " + self._b64(f"{self.client_id}:{self.client_secret}"),
            "Content-Type": "application/x-www-form-urlencoded",
        }
        params = {
            "grant_type": "password",
            "username": self.username,
            "password": self.password,
        }
        response = requests.post(
            self.oauth2_url,
            headers=headers,
            params=params,
            timeout=self.request_timeout_sec,
        )
        response.raise_for_status()
        self.token = response.json()["access_token"]
        logger.info("OAuth2 token acquired")
        return self.token

    def _request_json(self, url: str) -> Dict[str, Any]:
        if self.token is None:
            self.get_token()

        headers = {"Authorization": f"Bearer {self.token}"}
        response = requests.get(url, headers=headers, timeout=self.request_timeout_sec)

        if response.status_code == 401:
            logger.warning("Bearer expired; refreshing token")
            self.token = None
            return self._request_json(url)

        response.raise_for_status()
        return response.json()

    def _with_retries(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        last_error: Optional[Exception] = None
        for attempt in range(1, self.max_retries + 1):
            try:
                return func(*args, **kwargs)
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                logger.error(
                    "%s attempt %s/%s failed: %s",
                    func.__name__,
                    attempt,
                    self.max_retries,
                    exc,
                )
                if attempt < self.max_retries:
                    time.sleep(self.retry_wait_sec)
        raise RuntimeError(
            f"{func.__name__} failed after {self.max_retries} attempts"
        ) from last_error

    @staticmethod
    def _load_date_from_last_modified(last_modified: Optional[str]) -> str:
        """
        Align activity load_date with the warehouse convention:

        - lastModified on the calendar day of the run → that day
        - otherwise → lastModified date + 1 day (catch-up window)
        """
        today = date.today()
        if not last_modified:
            return today.isoformat()

        try:
            cleaned = last_modified.strip()
            if cleaned.endswith(" UTC"):
                cleaned = cleaned[:-4]
            modified_day = datetime.fromisoformat(cleaned).date()
            if modified_day == today:
                return today.isoformat()
            return (modified_day + timedelta(days=1)).isoformat()
        except (ValueError, TypeError, AttributeError) as exc:
            logger.warning(
                "Could not parse lastModified=%r (%s); using today",
                last_modified,
                exc,
            )
            return today.isoformat()

    def _normalize_record(self, row: Dict[str, Any]) -> Dict[str, Any]:
        planned = [
            obj.get("subCategoryName")
            for obj in row.get("plannedObjectives", [])
            if obj.get("subCategoryName")
        ]
        achieved = [
            obj.get("subCategoryName")
            for obj in row.get("achievedObjectives", [])
            if obj.get("subCategoryName")
        ]

        return {
            "sam_id": row.get("id"),
            "account_id": row.get("accountIdentifier"),
            "customer_upn": row.get("customerUpn"),
            "sales_manager_id": row.get("salesManagerId"),
            "planned_objectives": ";".join(planned),
            "achieved_objectives": ";".join(achieved),
            "last_modified": row.get("lastModified"),
            "status": row.get("status"),
            "mcrm_activity_id": row.get("mcrmActivityId"),
            "activity_type": row.get("activityType"),
            "_valid_flag": True,
            "creation_date": row.get("creationDate"),
            "planned_datetime": row.get("plannedDateTime"),
            "documented_datetime": row.get("documentedDateTime"),
            "start_datetime": row.get("startDateTime"),
            "end_datetime": row.get("endDateTime"),
            "anytime": row.get("anytime"),
            "load_date": self._load_date_from_last_modified(row.get("lastModified")),
        }

    def _activities_url(
        self,
        country: str,
        from_date: str,
        to_date: str,
        page: Optional[int] = None,
    ) -> str:
        category = self.category_name.replace(" ", "%20")
        url = (
            f"{self.base_url}v1/countries/{country.lower()}/activities"
            f"?fromDate={from_date}&toDate={to_date}"
            f"&categoryName={category}&status=Completed"
        )
        if page is not None:
            url = f"{url}&page={page}"
        return url

    def fetch_activities_data(
        self,
        country: str,
        from_date: str,
        to_date: str,
        output_dir: Optional[str] = None,
        filename: str = "sam_response.json",
    ) -> str:
        """Page through completed activities and write NDJSON. Returns local path."""
        output_dir = output_dir or f"/tmp/sam/{country.lower()}/"
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, filename)

        first_url = self._activities_url(country, from_date, to_date)
        logger.info("Fetching %s activities %s → %s", country, from_date, to_date)

        first_page = self._with_retries(self._request_json, first_url)
        total_pages: Final[int] = int(first_page.get("totalPages", 1) or 1)
        total_elements = int(first_page.get("totalElements", 0) or 0)
        logger.info("%s: %s elements across %s pages", country, total_elements, total_pages)

        written = 0
        with open(output_path, "w", encoding="utf-8") as handle:
            for page_num in range(total_pages):
                page_url = self._activities_url(country, from_date, to_date, page=page_num)
                page_body = (
                    first_page
                    if page_num == 0
                    else self._with_retries(self._request_json, page_url)
                )
                for row in page_body.get("content", []):
                    try:
                        handle.write(
                            json.dumps(self._normalize_record(row), default=str) + "\n"
                        )
                        written += 1
                    except Exception as exc:  # noqa: BLE001
                        logger.error("Skip bad activity row: %s", exc)
                        continue

        logger.info("Wrote %s records to %s", written, output_path)
        return output_path
