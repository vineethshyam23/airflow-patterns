"""Payment wallet DWH API client: OAuth, paginated pulls, NDJSON land.

Pulls three product feeds from a payment wallet DWH API:

- KYC onboarding status (paginate until empty; no count endpoint)
- Transactions (count endpoint drives page math)
- VOP (Verification of Pay) performance (single reportDate per call)

Writes newline-delimited JSON under a Composer data path for a later
GCS → BigQuery staging load. Returns an XCom-friendly dict with
``api_records_count`` for ops Slack / email summaries.

Distinct from pattern 11 (outbound KYC Avro export to a partner event
bus). This module is the *inbound* land-and-stage half.

Source (read-only):
  dags/horeca_digital/get_dish_pay_data.py
"""

from __future__ import annotations

import json
import logging
import math
import os
import time
from datetime import datetime, timedelta

import requests

try:
    from google.cloud import bigquery
except ImportError:  # pragma: no cover - optional in reference checkouts
    bigquery = None

logger = logging.getLogger(__name__)

# Composer data volume path. Override via PAYMENT_WALLET_DATA_DIR for
# local smoke tests.
DEFAULT_DATA_DIR = "/home/airflow/gcs/data/payment_wallet"


class PaymentWalletAPI:
    """OAuth client with 401/403 token refresh and paginated POSTs."""

    def __init__(
        self,
        token_url: str,
        client_id: str,
        client_secret: str,
        start_date: str | None = None,
        end_date: str | None = None,
    ):
        self.token_url = token_url
        self.token: str | None = None
        self.client_id = client_id
        self.client_secret = client_secret
        self.content_type = "application/json"
        self.start_date = start_date
        self.end_date = end_date
        self.api_records_count = 0

    def get_token(self) -> str:
        headers = {
            "accept": self.content_type,
            "Content-Type": self.content_type,
        }
        payload = {
            "ClientID": self.client_id,
            "ClientSecret": self.client_secret,
        }
        try:
            response = requests.post(
                url=self.token_url, headers=headers, json=payload, timeout=60
            )
            response.raise_for_status()
            body = response.json()
            self.token = f"Bearer {body['accessToken']}"
            return self.token
        except requests.exceptions.RequestException as exc:
            raise RuntimeError(f"Error getting token: {exc}") from exc

    @staticmethod
    def is_token_valid(response: requests.Response) -> bool:
        return response.status_code not in (401, 403)

    def make_authenticated_request(
        self, url: str, json_data: dict, max_retries: int = 3
    ):
        """POST with automatic token refresh. Returns JSON or None on HTTP 400."""
        for attempt in range(max_retries):
            if self.token is None:
                self.get_token()

            headers = {
                "accept": self.content_type,
                "Authorization": self.token,
                "Content-Type": self.content_type,
            }

            try:
                response = requests.post(
                    url=url, headers=headers, json=json_data, timeout=120
                )

                if not self.is_token_valid(response):
                    logger.info("Token expired, refreshing")
                    self.token = None
                    self.get_token()
                    headers["Authorization"] = self.token
                    response = requests.post(
                        url=url, headers=headers, json=json_data, timeout=120
                    )

                if response.status_code == 200:
                    return response.json()
                if response.status_code == 400:
                    # Some pages return 400 for empty windows; skip and continue.
                    logger.warning(
                        "Request failed with status %s: %s",
                        response.status_code,
                        response.text[:500],
                    )
                    return None

                logger.error(
                    "Request failed with status %s: %s",
                    response.status_code,
                    response.text[:500],
                )
                if attempt < max_retries - 1:
                    time.sleep(120)
                    continue
                raise RuntimeError(
                    f"Request failed after {max_retries} attempts "
                    f"(status {response.status_code})"
                )

            except requests.exceptions.RequestException as exc:
                logger.error("Request exception on attempt %s: %s", attempt + 1, exc)
                if attempt < max_retries - 1:
                    time.sleep(120)
                    continue
                raise RuntimeError(
                    f"Request failed after {max_retries} attempts: {exc}"
                ) from exc

        raise RuntimeError("Failed to make authenticated request")

    def _ensure_date_window(self) -> None:
        if not self.start_date:
            yesterday = datetime.now() - timedelta(days=1)
            self.start_date = yesterday.replace(
                hour=0, minute=0, second=0, microsecond=0
            ).strftime("%Y-%m-%dT%H:%M:%S.000Z")
        if not self.end_date:
            yesterday = datetime.now() - timedelta(days=1)
            self.end_date = datetime(
                yesterday.year, yesterday.month, yesterday.day, 23, 59, 59
            ).strftime("%Y-%m-%dT%H:%M:%S.000Z")

    def get_transaction_count(self, base_url: str) -> int:
        self._ensure_date_window()
        count_url = f"{base_url}/api/v1/dwh/transaction/count"
        json_data = {
            "page": 1,
            "size": 50,
            "filters": {
                "startDate": self.start_date,
                "endDate": self.end_date,
            },
        }
        try:
            response_data = self.make_authenticated_request(count_url, json_data)
            if response_data is None:
                logger.warning("Count endpoint returned 400; treating count as 0")
                self.api_records_count = 0
                return 0

            total_count = (
                response_data.get("count", 0)
                if isinstance(response_data, dict)
                else 0
            )
            self.api_records_count = total_count
            logger.info("Total transaction count: %s", total_count)
            return total_count
        except Exception as exc:
            logger.error("Error getting transaction count: %s", exc)
            raise

    def endpoint(
        self,
        url: str,
        use_count_endpoint: bool = False,
        base_url: str | None = None,
        page_size: int = 1000,
    ):
        """Paginate a list endpoint. Returns (rows, reported_count)."""
        json_arr: list = []
        self._ensure_date_window()

        total_count = 0
        if use_count_endpoint and base_url:
            total_count = self.get_transaction_count(base_url)
            if total_count == 0:
                logger.info("No records found (or count error); returning empty")
                return [], 0
            total_pages = math.ceil(total_count / page_size)
            logger.info(
                "Expecting %s records across ~%s pages", total_count, total_pages
            )
        else:
            logger.info("No count endpoint — iterating until empty page")

        page = 1
        while True:
            json_data = {
                "page": page,
                "size": page_size,
                "filters": {
                    "startDate": self.start_date,
                    "endDate": self.end_date,
                },
            }
            logger.info("Fetching page %s", page)

            try:
                response_data = self.make_authenticated_request(url, json_data)
                if response_data is None:
                    logger.warning("Page %s: 400 — skipping to next page", page)
                    page += 1
                    continue

                if len(response_data) > 0:
                    json_arr.append(response_data)
                    logger.info(
                        "Page %s: retrieved %s records", page, len(response_data)
                    )
                    if use_count_endpoint and total_count > 0:
                        records_retrieved = sum(len(batch) for batch in json_arr)
                        if records_retrieved >= total_count:
                            break
                    page += 1
                else:
                    logger.info("Page %s: empty — stopping pagination", page)
                    break
            except Exception as exc:
                logger.error("Error fetching page %s: %s", page, exc)
                raise

        total_records = sum(len(batch) for batch in json_arr)
        logger.info("Retrieved %s records for window %s", total_records, self.start_date)
        if not use_count_endpoint:
            self.api_records_count = total_records
        return sum(json_arr, []), self.api_records_count

    def vop_performance_endpoint(
        self, url: str, report_date: str, page_size: int = 50, max_pages: int = 100
    ):
        """Fetch VOP performance for one calendar day (reportDate filter)."""
        json_arr: list = []
        page = 1

        while page <= max_pages:
            json_data = {
                "page": page,
                "size": page_size,
                "filters": {"reportDate": report_date},
            }
            logger.info("Fetching VOP performance page %s for %s", page, report_date)
            response_data = self.make_authenticated_request(url, json_data)

            if response_data is None:
                logger.warning("VOP page %s: 400 — stopping pagination", page)
                break

            if len(response_data) > 0:
                json_arr.append(response_data)
                page += 1
            else:
                break

        report_objects = sum(json_arr, [])
        performance_rows = sum(
            len(obj.get("vopPerformance", [])) for obj in report_objects
        )
        self.api_records_count = performance_rows
        logger.info(
            "Total VOP performance rows for %s: %s", report_date, performance_rows
        )
        return report_objects, self.api_records_count


def _data_dir() -> str:
    path = os.environ.get("PAYMENT_WALLET_DATA_DIR", DEFAULT_DATA_DIR)
    os.makedirs(path, exist_ok=True)
    return path


def get_payment_wallet_data(**kwargs):
    """Airflow callable: fetch one feed and write NDJSON. Returns XCom dict."""
    token_url = kwargs["token_url"]
    client_id = kwargs["client_id"]
    client_secret = kwargs["client_secret"]
    start_date = kwargs["start_date"]
    end_date = kwargs["end_date"]
    base_url = kwargs["base_url"]
    file_name = kwargs["file_name"]
    tmp_loc = _data_dir()
    out_path = f"{tmp_loc}/{file_name}.json"

    api = PaymentWalletAPI(
        token_url,
        client_id,
        client_secret,
        start_date=start_date,
        end_date=end_date,
    )

    if file_name == "payment_kyc":
        url = f"{base_url}/api/v1/dwh/KYC"
        try:
            rows, api_records_count = api.endpoint(url=url, use_count_endpoint=False)
            with open(out_path, "w", encoding="utf-8") as handle:
                for row in rows:
                    handle.write(json.dumps(row) + "\n")
            return {
                "api_records_count": api_records_count,
                "file_name": file_name,
                "start_date": start_date,
                "end_date": end_date,
            }
        except Exception as exc:
            raise RuntimeError(f"Error fetching payment KYC: {exc}") from exc

    if file_name == "payment_transactions":
        url = f"{base_url}/api/v1/dwh/transaction"
        try:
            rows, api_records_count = api.endpoint(
                url=url, use_count_endpoint=True, base_url=base_url
            )
            with open(out_path, "w", encoding="utf-8") as handle:
                for row in rows:
                    handle.write(json.dumps(row) + "\n")
            return {
                "api_records_count": api_records_count,
                "file_name": file_name,
                "start_date": start_date,
                "end_date": end_date,
            }
        except Exception as exc:
            raise RuntimeError(f"Error fetching payment transactions: {exc}") from exc

    if file_name == "payment_vop_performance":
        report_dates = kwargs.get("report_dates")
        if not report_dates:
            report_date = kwargs.get("report_date") or start_date[:10]
            report_dates = [report_date]

        vop_url = f"{base_url}/api/v1/dwh/VOP/performance"
        total_records = 0
        with open(out_path, "w", encoding="utf-8") as handle:
            for report_date in report_dates:
                try:
                    report_objects, day_records = api.vop_performance_endpoint(
                        url=vop_url, report_date=report_date
                    )
                    for row in report_objects:
                        handle.write(json.dumps(row) + "\n")
                    total_records += day_records
                except Exception as exc:
                    raise RuntimeError(
                        f"Error fetching VOP performance for {report_date}: {exc}"
                    ) from exc
        return {
            "api_records_count": total_records,
            "file_name": file_name,
            "report_dates": report_dates,
            "start_date": start_date,
            "end_date": end_date,
        }

    raise ValueError(f"Unknown file_name: {file_name}")


def get_loaded_data_count(
    project_id: str = "dwh_project",
    table: str = "trusted.int_payment_transactions",
):
    """Count rows loaded today in the trusted transactions intermediate."""
    if bigquery is None:
        raise RuntimeError("google-cloud-bigquery is required for get_loaded_data_count")

    client = bigquery.Client()
    query = f"""
        SELECT COUNT(1) AS record_count
        FROM `{project_id}.{table}`
        WHERE loaded_date = CURRENT_DATE()
    """
    logger.info("Counting rows loaded today in %s.%s", project_id, table)
    for row in client.query(query).result():
        return row.record_count
    return 0


if __name__ == "__main__":
    # Syntax / import smoke only — no live credentials.
    print("payment_api module OK")
    print("feeds: payment_kyc | payment_transactions | payment_vop_performance")
