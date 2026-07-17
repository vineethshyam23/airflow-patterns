from __future__ import annotations

"""
Adyen Management API client for payment terminal inventory.

Pulls merchants, stores, terminals, and terminal settings; can also reassign
terminals and PATCH default settings driven by a BigQuery match table.

Sanitized portfolio sample — credentials come from Airflow Variables, not code.
"""


import base64
import copy
import csv
import json
import logging
import time
from dataclasses import dataclass
from io import StringIO
from typing import Any, Dict, List, Optional, Tuple

import requests
from google.cloud import bigquery
from requests.exceptions import RequestException
from urllib.parse import quote

from airflow.exceptions import AirflowException

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PAGE_SIZE = 100
FETCH_MAX_RETRIES = 2
FETCH_RETRY_DELAY_SECONDS = 60
BASE_URL_TEMPLATE = "https://management-{environment}.adyen.com/v3"
DEFAULT_ENVIRONMENT = "test"
TMP_LOC = "/home/airflow/gcs/data/adyen/payment_terminal"

REASSIGN_MAX_RETRIES = 2
REASSIGN_RETRY_DELAY_SECONDS = 60
REASSIGN_INVENTORY_DEFAULT = False

PATCH_MAX_RETRIES = 2
PATCH_RETRY_DELAY_SECONDS = 60

SERIAL_STORE_MATCH_DATASET = "trusted"
SERIAL_STORE_MATCH_TABLE = "int_payment_terminal_erp_serial_store_match"

TERMINAL_SETTINGS_INTER_REQUEST_SLEEP_SECONDS = 0.05
ALLOWED_TERMINAL_SETTINGS_TOP_LEVEL_KEYS = frozenset(
    {
        "cardholderReceipt",
        "gratuities",
        "opi",
        "receiptPrinting",
        "refunds",
        "signature",
        "hardware",
        "connectivity",
        "offlineProcessing",
        "standalone",
        "payment",
        "localization",
        "terminalInstructions",
        "homeScreen",
    }
)

POS_LITE_DEFAULT_PATCH: Dict[str, Any] = {
    "standalone": {
        "enableStandalone": False,
        "enableGratuities": False,
    }
}


def _serial_store_match_table_fqn(project_id: str) -> str:
    return (
        f"`{project_id}.{SERIAL_STORE_MATCH_DATASET}."
        f"{SERIAL_STORE_MATCH_TABLE}`"
    )


def _log_adyen_api_error_response(response: requests.Response, prefix: str) -> None:
    """Log full response.text plus parsed JSON fields when present."""
    text = response.text or ""
    logger.error(
        "%s HTTP status=%s response.body.full=%r", prefix, response.status_code, text
    )
    try:
        data = response.json()
    except (ValueError, TypeError, json.JSONDecodeError):
        logger.error("%s body is not JSON; raw text already logged above", prefix)
        return
    if not isinstance(data, dict):
        logger.error("%s parsed JSON is not an object: %r", prefix, data)
        return
    for key in ("errorCode", "message", "errorType", "status"):
        if key in data:
            logger.error("%s parsed.%s=%r", prefix, key, data[key])
    for key in ("title", "detail", "requestId", "instance", "type"):
        if key in data:
            logger.error("%s parsed.%s=%r", prefix, key, data[key])
    invalid_fields = data.get("invalidFields")
    if invalid_fields:
        logger.error("%s parsed.invalidFields=%s", prefix, json.dumps(invalid_fields))


@dataclass
class AdyenConfig:
    """Configuration class for Adyen API credentials."""

    api_key: str
    username: str
    password: str
    environment: str = DEFAULT_ENVIRONMENT

    @property
    def basic_auth(self) -> str:
        credentials = f"{self.username}:{self.password}"
        return base64.b64encode(credentials.encode()).decode()

    @property
    def headers(self) -> Dict[str, str]:
        return {
            "X-API-Key": self.api_key,
            "Content-Type": "application/json",
            "Authorization": f"Basic {self.basic_auth}",
        }


class AdyenJsonStorage:
    @staticmethod
    def save_jsonl(data: List[Dict[Any, Any]], output_path: str) -> None:
        try:
            with open(output_path, "w") as f:
                for item in data:
                    json.dump(item, f)
                    f.write("\n")
            logger.info("Data successfully saved to %s", output_path)
            logger.info("Total items saved: %s", len(data))
        except IOError as e:
            logger.error("Error saving data to %s: %s", output_path, e)
            raise


class AdyenManagementClient:
    def __init__(self, config: AdyenConfig) -> None:
        self._config = config

    @property
    def config(self) -> AdyenConfig:
        return self._config

    def _url(self, path: str) -> str:
        if not path.startswith("/"):
            path = f"/{path}"
        return f"{BASE_URL_TEMPLATE.format(environment=self._config.environment)}{path}"

    def _request(
        self,
        method: str,
        path: str,
        *,
        json_body: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> requests.Response:
        return requests.request(
            method,
            self._url(path),
            headers=self._config.headers,
            json=json_body,
            params=params,
            timeout=120,
        )

    def _fetch_paginated_page(
        self, path: str, page_number: int, *, quiet: bool = False
    ) -> Dict[str, Any]:
        log = logger.debug if quiet else logger.info
        max_attempts = 1 + FETCH_MAX_RETRIES
        last_error: Optional[RequestException] = None
        for attempt in range(1, max_attempts + 1):
            try:
                response = self._request(
                    "GET",
                    path,
                    params={"pageSize": PAGE_SIZE, "pageNumber": page_number},
                )
                response.raise_for_status()
                return response.json()
            except RequestException as e:
                last_error = e
                logger.error(
                    "Error fetching page %s (attempt %s/%s): %s",
                    page_number,
                    attempt,
                    max_attempts,
                    e,
                )
                if attempt < max_attempts:
                    delay = FETCH_RETRY_DELAY_SECONDS * attempt
                    log("Retrying page %s in %s seconds", page_number, delay)
                    time.sleep(delay)
        assert last_error is not None
        raise last_error

    def paginated_get(self, path: str, *, quiet: bool = False) -> List[Dict[Any, Any]]:
        all_items: List[Dict[Any, Any]] = []
        page_number = 1
        while True:
            data = self._fetch_paginated_page(path, page_number, quiet=quiet)
            all_items.extend(data.get("data", []))
            if page_number >= data.get("pagesTotal", 1):
                return all_items
            page_number += 1


class MerchantsEndpoint:
    PATH = "/merchants"

    def __init__(self, client: AdyenManagementClient) -> None:
        self._client = client

    @staticmethod
    def normalize(merchant: Dict[Any, Any]) -> Dict[str, Any]:
        links = merchant.get("_links", {})
        return {
            "merchant_id": merchant.get("id"),
            "merchant_name": merchant.get("name"),
            "company_id": merchant.get("companyId"),
            "capture_delay": merchant.get("captureDelay"),
            "shopper_interaction": merchant.get("defaultShopperInteraction"),
            "status": merchant.get("status"),
            "web_address": merchant.get("shopWebAddress"),
            "city": merchant.get("merchantCity"),
            "settlement_currency": merchant.get("primarySettlementCurrency"),
            "reference": merchant.get("reference"),
            "data_centers": [
                {"name": dc.get("name"), "live_prefix": dc.get("livePrefix")}
                for dc in merchant.get("dataCenters", [])
            ],
            "self_url": links.get("self", {}).get("href"),
            "api_credentials_url": links.get("apiCredentials", {}).get("href"),
            "users_url": links.get("users", {}).get("href"),
            "webhooks_url": links.get("webhooks", {}).get("href"),
        }

    def fetch_all(self) -> List[Dict[Any, Any]]:
        logger.info("Fetching merchant data from: %s", self._client._url(self.PATH))
        merchants = self._client.paginated_get(self.PATH)
        return [self.normalize(m) for m in merchants]

    def fetch_and_save(self) -> List[Dict[Any, Any]]:
        merchant_data = self.fetch_all()
        logger.info("Total merchants retrieved: %s", len(merchant_data))
        AdyenJsonStorage.save_jsonl(merchant_data, f"{TMP_LOC}/merchant_data.json")
        logger.info("Merchant data saved to merchant_data.json")
        return merchant_data


class MerchantStoresEndpoint:
    def __init__(self, client: AdyenManagementClient) -> None:
        self._client = client

    @staticmethod
    def normalize(store: Dict[Any, Any], merchant_id: str) -> Dict[str, Any]:
        address = store.get("address", {})
        return {
            "store_id": store.get("id"),
            "store_reference": store.get("reference"),
            "store_description": store.get("description"),
            "store_status": store.get("status"),
            "store_merchant_id": store.get("merchantId"),
            "store_phone_number": store.get("phoneNumber"),
            "store_shopper_statement": store.get("shopperStatement"),
            "store_external_reference_id": store.get("externalReferenceId"),
            "store_address_line1": address.get("line1"),
            "store_city": address.get("city"),
            "store_country": address.get("country"),
            "store_postal_code": address.get("postalCode"),
            "store_self_url": store.get("_links", {}).get("self", {}).get("href"),
        }

    def fetch_for_merchants(
        self, merchant_data: List[Dict[Any, Any]]
    ) -> List[Dict[Any, Any]]:
        if not merchant_data:
            logger.warning("Merchant data is empty, skipping store data retrieval")
            return []

        all_store_data: List[Dict[Any, Any]] = []
        for merchant in merchant_data:
            merchant_id = merchant.get("merchant_id", "")
            encoded_merchant = quote(merchant_id, safe="")
            path = f"/merchants/{encoded_merchant}/stores"
            try:
                stores = self._client.paginated_get(path, quiet=True)
                all_store_data.extend(
                    self.normalize(store, merchant_id) for store in stores
                )
            except Exception as e:
                logger.error(
                    "Error fetching store data for merchant_id=%s: %s",
                    merchant_id,
                    e,
                )
                return []
        return all_store_data

    def fetch_and_save(
        self, merchant_data: List[Dict[Any, Any]]
    ) -> List[Dict[Any, Any]]:
        all_store_data = self.fetch_for_merchants(merchant_data)
        logger.info("Total stores retrieved: %s", len(all_store_data))
        AdyenJsonStorage.save_jsonl(all_store_data, f"{TMP_LOC}/store_data.json")
        logger.info("Store data saved to store_data.json")
        return all_store_data


class TerminalsEndpoint:
    PATH = "/terminals"

    def __init__(self, client: AdyenManagementClient) -> None:
        self._client = client

    @staticmethod
    def normalize(terminal: Dict[Any, Any]) -> Dict[str, Any]:
        assignment = terminal.get("assignment", {})
        connectivity = terminal.get("connectivity", {})
        wifi = connectivity.get("wifi", {})
        cellular = connectivity.get("cellular", {})
        bluetooth = connectivity.get("bluetooth", {})
        ethernet = connectivity.get("ethernet", {})
        return {
            "terminal_id": terminal.get("id"),
            "model": terminal.get("model"),
            "serial_number": terminal.get("serialNumber"),
            "firmware_version": terminal.get("firmwareVersion"),
            "country_code": terminal.get("countryCode"),
            "cloud_device_api_endpoint": terminal.get("cloudDeviceApiEndpoint"),
            "restart_local_time": terminal.get("restartLocalTime"),
            "company_id": assignment.get("companyId"),
            "merchant_id": assignment.get("merchantId"),
            "store_id": assignment.get("storeId"),
            "assignment_status": assignment.get("status"),
            "wifi_ip_address": wifi.get("ipAddress"),
            "wifi_mac_address": wifi.get("macAddress"),
            "wifi_ssid": wifi.get("ssid"),
            "cellular_iccid": cellular.get("iccid"),
            "cellular_status": cellular.get("status"),
            "bluetooth_mac_address": bluetooth.get("macAddress"),
            "ethernet_mac_address": ethernet.get("macAddress"),
        }

    def fetch_all(self) -> List[Dict[str, Any]]:
        logger.info("Fetching terminal data from: %s", self._client._url(self.PATH))
        terminals = self._client.paginated_get(self.PATH)
        return [self.normalize(t) for t in terminals]

    def fetch_and_save(self) -> List[Dict[str, Any]]:
        terminal_data = self.fetch_all()
        logger.info("Total terminals retrieved: %s", len(terminal_data))
        AdyenJsonStorage.save_jsonl(terminal_data, f"{TMP_LOC}/terminal_data.json")
        logger.info("Terminal data saved to terminal_data.json")
        return terminal_data


class TerminalSettingsEndpoint:
    """GET + PATCH /terminals/{terminalId}/terminalSettings."""

    def __init__(self, client: AdyenManagementClient) -> None:
        self._client = client

    def _settings_path(self, terminal_id: str) -> str:
        encoded_tid = quote(terminal_id, safe="")
        return f"/terminals/{encoded_tid}/terminalSettings"

    def _get_once(self, terminal_id: str) -> requests.Response:
        return self._client._request("GET", self._settings_path(terminal_id))

    def fetch_with_retries(
        self,
        terminal_id: str,
        *,
        max_retries: int = FETCH_MAX_RETRIES,
        retry_delay_seconds: int = FETCH_RETRY_DELAY_SECONDS,
    ) -> Tuple[Optional[Dict[str, Any]], Optional[int]]:
        max_attempts = 1 + max_retries
        last_status: Optional[int] = None
        for attempt in range(1, max_attempts + 1):
            try:
                response = self._get_once(terminal_id)
                last_status = response.status_code
                if response.status_code == 200:
                    data = response.json()
                    if isinstance(data, dict):
                        return data, last_status
                    logger.error(
                        "terminal settings non-object JSON terminal_id=%r "
                        "attempt=%s/%s",
                        terminal_id,
                        attempt,
                        max_attempts,
                    )
                else:
                    logger.error(
                        "terminal settings GET failed terminal_id=%r HTTP=%s "
                        "attempt=%s/%s",
                        terminal_id,
                        response.status_code,
                        attempt,
                        max_attempts,
                    )
            except RequestException as exc:
                logger.error(
                    "terminal settings GET RequestException terminal_id=%r "
                    "attempt=%s/%s: %s",
                    terminal_id,
                    attempt,
                    max_attempts,
                    exc,
                )
                err_resp = getattr(exc, "response", None)
                if err_resp is not None:
                    last_status = err_resp.status_code
            if attempt < max_attempts:
                time.sleep(retry_delay_seconds)
        return None, last_status

    @classmethod
    def filter_for_storage(cls, settings: Dict[str, Any]) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        for key in ALLOWED_TERMINAL_SETTINGS_TOP_LEVEL_KEYS:
            if key not in settings:
                continue
            out[key] = copy.deepcopy(settings[key])
        return out

    @classmethod
    def normalize_row(
        cls, terminal_id: str, settings: Dict[str, Any]
    ) -> Dict[str, Any]:
        return {
            "terminal_id": terminal_id,
            "settings": cls.filter_for_storage(settings),
        }

    def _log_fetch_summary(
        self, *, total: int, success: int, failed: int, skipped_empty: int
    ) -> None:
        logger.info(
            "Adyen terminal settings summary: total=%s success=%s failed=%s "
            "skipped_empty=%s",
            total,
            success,
            failed,
            skipped_empty,
        )

    def fetch_for_terminals(
        self, terminal_data: List[Dict[Any, Any]]
    ) -> List[Dict[str, Any]]:
        terminal_ids: List[str] = []
        skipped_inventory = 0
        for row in terminal_data:
            if not isinstance(row, dict):
                continue
            if str(row.get("assignment_status") or "").lower() == "inventory":
                skipped_inventory += 1
                continue
            tid = str(row.get("terminal_id") or "").strip()
            if tid:
                terminal_ids.append(tid)

        if skipped_inventory:
            logger.info(
                "Skipped terminal settings fetch for %s terminal(s) with "
                "assignment_status=inventory",
                skipped_inventory,
            )

        if not terminal_ids:
            logger.info("No terminal_id values for terminal settings fetch; exiting")
            return []

        results: List[Dict[str, Any]] = []
        failed = 0
        skipped_empty = 0
        for terminal_id in terminal_ids:
            settings, last_status = self.fetch_with_retries(terminal_id)
            if settings is None:
                logger.error(
                    "terminal settings skipped terminal_id=%r last_http_status=%s",
                    terminal_id,
                    last_status,
                )
                failed += 1
            else:
                if not self.filter_for_storage(settings):
                    logger.warning(
                        "terminal settings empty after whitelist terminal_id=%r",
                        terminal_id,
                    )
                    skipped_empty += 1
                else:
                    results.append(self.normalize_row(terminal_id, settings))
            if TERMINAL_SETTINGS_INTER_REQUEST_SLEEP_SECONDS > 0:
                time.sleep(TERMINAL_SETTINGS_INTER_REQUEST_SLEEP_SECONDS)

        self._log_fetch_summary(
            total=len(terminal_ids),
            success=len(results),
            failed=failed,
            skipped_empty=skipped_empty,
        )
        if not results:
            raise AirflowException(
                f"terminal settings fetch saved zero rows for "
                f"{len(terminal_ids)} terminal(s)"
            )
        return results

    def fetch_and_save(
        self, terminal_data: List[Dict[Any, Any]]
    ) -> List[Dict[str, Any]]:
        results = self.fetch_for_terminals(terminal_data)
        AdyenJsonStorage.save_jsonl(
            results, f"{TMP_LOC}/terminal_settings_data.json"
        )
        logger.info("Terminal settings data saved to terminal_settings_data.json")
        return results

    def _patch_once(
        self, terminal_id: str, patch_body: Dict[str, Any]
    ) -> requests.Response:
        return self._client._request(
            "PATCH", self._settings_path(terminal_id), json_body=patch_body
        )

    def patch_with_retries(
        self,
        terminal_id: str,
        patch_body: Dict[str, Any],
        *,
        max_retries: int = PATCH_MAX_RETRIES,
        retry_delay_seconds: int = PATCH_RETRY_DELAY_SECONDS,
    ) -> Dict[str, Any]:
        max_attempts = 1 + max_retries
        last_status: Optional[int] = None
        last_text = ""
        attempts_used = 0
        for attempt in range(1, max_attempts + 1):
            attempts_used = attempt
            try:
                response = self._patch_once(terminal_id, patch_body)
                last_status = response.status_code
                last_text = response.text or ""
                if response.status_code == 200:
                    logger.info(
                        "Adyen settings PATCH success terminal_id=%r HTTP=200 "
                        "body_len=%s",
                        terminal_id,
                        len(last_text),
                    )
                    return {
                        "terminal_id": terminal_id,
                        "success": True,
                        "attempts_used": attempts_used,
                        "last_status": last_status,
                        "last_response_text": last_text,
                    }
                _log_adyen_api_error_response(
                    response,
                    f"Adyen settings PATCH attempt {attempt}/{max_attempts} "
                    f"terminal_id={terminal_id!r}",
                )
            except RequestException as exc:
                logger.error(
                    "Adyen settings PATCH attempt %s/%s RequestException "
                    "terminal_id=%r: %s",
                    attempt,
                    max_attempts,
                    terminal_id,
                    exc,
                )
                err_resp = getattr(exc, "response", None)
                if err_resp is not None:
                    last_status = err_resp.status_code
                    last_text = err_resp.text or ""
                    _log_adyen_api_error_response(
                        err_resp,
                        f"Adyen settings PATCH attempt {attempt}/{max_attempts} "
                        f"terminal_id={terminal_id!r} (exception response)",
                    )
                else:
                    last_text = str(exc)
            if attempt < max_attempts:
                time.sleep(retry_delay_seconds)

        logger.error(
            "Adyen settings PATCH FAILED after %s attempts terminal_id=%r "
            "last_status=%s last_response_text=%r",
            attempts_used,
            terminal_id,
            last_status,
            last_text,
        )
        return {
            "terminal_id": terminal_id,
            "success": False,
            "attempts_used": attempts_used,
            "last_status": last_status,
            "last_response_text": last_text,
        }

    @staticmethod
    def fetch_patch_rows_from_bigquery(project_id: str) -> List[str]:
        client = bigquery.Client(project=project_id)
        table_fqn = _serial_store_match_table_fqn(project_id)
        query = f"""
            SELECT terminal_id
            FROM {table_fqn}
            WHERE terminal_id IS NOT NULL
              AND to_disable_standalone_tip IS TRUE
        """
        rows_iter = client.query(query).result()
        out = [str(row["terminal_id"]) for row in rows_iter]
        logger.info(
            "BQ settings PATCH rows fetched: count=%s project=%s table=%s.%s",
            len(out),
            project_id,
            SERIAL_STORE_MATCH_DATASET,
            SERIAL_STORE_MATCH_TABLE,
        )
        return out

    @staticmethod
    def _log_patch_summary_csv(
        results: List[Dict[str, Any]], *, skipped_rows: int
    ) -> None:
        ok = sum(1 for r in results if r.get("success"))
        fail = len(results) - ok
        buf = StringIO()
        writer = csv.writer(buf, lineterminator="\n")
        writer.writerow(["terminal_id", "outcome", "last_http_status"])
        for r in results:
            st = r.get("last_status")
            writer.writerow(
                [
                    str(r.get("terminal_id", "")),
                    "success" if r.get("success") else "failed",
                    "" if st is None else str(st),
                ]
            )
        logger.info(
            "Adyen terminal settings PATCH summary: skipped=%s processed=%s "
            "success=%s failed=%s\n%s",
            skipped_rows,
            len(results),
            ok,
            fail,
            buf.getvalue().rstrip("\n"),
        )

    def run_patches_from_bigquery(self, project_id: str) -> None:
        terminal_ids = self.fetch_patch_rows_from_bigquery(project_id)
        if not terminal_ids:
            logger.info("No rows from BQ for terminal settings PATCH; exiting")
            return

        results: List[Dict[str, Any]] = []
        skipped_rows = 0
        for terminal_id in terminal_ids:
            tid = terminal_id.strip()
            if not tid:
                logger.warning("Skipping empty terminal_id from BQ patch rows")
                skipped_rows += 1
                continue
            results.append(
                self.patch_with_retries(tid, POS_LITE_DEFAULT_PATCH)
            )

        self._log_patch_summary_csv(results, skipped_rows=skipped_rows)
        failures = [r for r in results if not r["success"]]
        if failures:
            preview = failures[:5]
            raise AirflowException(
                f"terminal settings PATCH failed for {len(failures)} row(s). "
                f"First failures (up to 5): {preview}"
            )


class TerminalReassignEndpoint:
    """POST /terminals/{terminalId}/reassign + BigQuery orchestration."""

    def __init__(self, client: AdyenManagementClient) -> None:
        self._client = client

    def _reassign_path(self, terminal_id: str) -> str:
        encoded_tid = quote(terminal_id, safe="")
        return f"/terminals/{encoded_tid}/reassign"

    def _post_once(
        self, terminal_id: str, store_id: str, inventory: bool
    ) -> requests.Response:
        body: Dict[str, Any] = {"inventory": inventory, "storeId": store_id}
        return self._client._request(
            "POST", self._reassign_path(terminal_id), json_body=body
        )

    def reassign_with_retries(
        self,
        terminal_id: str,
        store_id: str,
        *,
        inventory: bool = REASSIGN_INVENTORY_DEFAULT,
        max_retries: int = REASSIGN_MAX_RETRIES,
        retry_delay_seconds: int = REASSIGN_RETRY_DELAY_SECONDS,
    ) -> Dict[str, Any]:
        max_attempts = 1 + max_retries
        logger.info(
            "Adyen reassign pair: terminal_id=%r store_id=%r inventory=%s",
            terminal_id,
            store_id,
            inventory,
        )
        last_status: Optional[int] = None
        last_text = ""
        attempts_used = 0
        for attempt in range(1, max_attempts + 1):
            attempts_used = attempt
            try:
                response = self._post_once(terminal_id, store_id, inventory)
                last_status = response.status_code
                last_text = response.text or ""
                if response.status_code == 200:
                    logger.info(
                        "Adyen reassign success terminal_id=%r store_id=%r HTTP=200 "
                        "body_len=%s",
                        terminal_id,
                        store_id,
                        len(last_text),
                    )
                    return {
                        "terminal_id": terminal_id,
                        "store_id": store_id,
                        "success": True,
                        "attempts_used": attempts_used,
                        "last_status": last_status,
                        "last_response_text": last_text,
                    }
                _log_adyen_api_error_response(
                    response,
                    f"Adyen reassign attempt {attempt}/{max_attempts} "
                    f"terminal_id={terminal_id!r}",
                )
            except RequestException as exc:
                logger.error(
                    "Adyen reassign attempt %s/%s RequestException terminal_id=%r: %s",
                    attempt,
                    max_attempts,
                    terminal_id,
                    exc,
                )
                err_resp = getattr(exc, "response", None)
                if err_resp is not None:
                    last_status = err_resp.status_code
                    last_text = err_resp.text or ""
                    _log_adyen_api_error_response(
                        err_resp,
                        f"Adyen reassign attempt {attempt}/{max_attempts} "
                        f"terminal_id={terminal_id!r} (exception response)",
                    )
                else:
                    last_text = str(exc)
            if attempt < max_attempts:
                time.sleep(retry_delay_seconds)

        logger.error(
            "Adyen reassign FAILED after %s attempts terminal_id=%r store_id=%r "
            "last_status=%s last_response_text=%r",
            attempts_used,
            terminal_id,
            store_id,
            last_status,
            last_text,
        )
        return {
            "terminal_id": terminal_id,
            "store_id": store_id,
            "success": False,
            "attempts_used": attempts_used,
            "last_status": last_status,
            "last_response_text": last_text,
        }

    @staticmethod
    def fetch_rows_from_bigquery(project_id: str) -> List[Dict[str, str]]:
        client = bigquery.Client(project=project_id)
        table_fqn = _serial_store_match_table_fqn(project_id)
        query = f"""
            SELECT terminal_id, store_id
            FROM {table_fqn}
            WHERE terminal_id IS NOT NULL
              AND store_id IS NOT NULL
        """
        rows_iter = client.query(query).result()
        out = [
            {
                "terminal_id": str(row["terminal_id"]),
                "store_id": str(row["store_id"]),
            }
            for row in rows_iter
        ]
        logger.info(
            "BQ reassign rows fetched: count=%s project=%s table=%s.%s",
            len(out),
            project_id,
            SERIAL_STORE_MATCH_DATASET,
            SERIAL_STORE_MATCH_TABLE,
        )
        return out

    @staticmethod
    def _log_summary_csv(
        results: List[Dict[str, Any]], *, skipped_rows: int
    ) -> None:
        ok = sum(1 for r in results if r.get("success"))
        fail = len(results) - ok
        buf = StringIO()
        writer = csv.writer(buf, lineterminator="\n")
        writer.writerow(["terminal_id", "store_id", "outcome", "last_http_status"])
        for r in results:
            st = r.get("last_status")
            writer.writerow(
                [
                    str(r.get("terminal_id", "")),
                    str(r.get("store_id", "")),
                    "success" if r.get("success") else "failed",
                    "" if st is None else str(st),
                ]
            )
        logger.info(
            "Adyen terminal reassign summary: skipped_bq_rows=%s processed=%s "
            "success=%s failed=%s\n%s",
            skipped_rows,
            len(results),
            ok,
            fail,
            buf.getvalue().rstrip("\n"),
        )

    def run_from_bigquery(self, project_id: str) -> None:
        rows = self.fetch_rows_from_bigquery(project_id)
        if not rows:
            logger.info("No rows from BQ for terminal reassign; exiting")
            return

        results: List[Dict[str, Any]] = []
        skipped_rows = 0
        for row in rows:
            terminal_id = row["terminal_id"].strip()
            store_id = row["store_id"].strip()
            if not terminal_id or not store_id:
                logger.warning(
                    "Skipping row with empty terminal_id or store_id: %r", row
                )
                skipped_rows += 1
                continue
            results.append(
                self.reassign_with_retries(
                    terminal_id,
                    store_id,
                    inventory=REASSIGN_INVENTORY_DEFAULT,
                )
            )

        self._log_summary_csv(results, skipped_rows=skipped_rows)
        failures = [r for r in results if not r["success"]]
        if failures:
            preview = failures[:5]
            raise AirflowException(
                f"terminal reassign failed for {len(failures)} row(s). "
                f"First failures (up to 5): {preview}"
            )


class AdyenPaymentTerminalIntegration:
    def __init__(self, config: AdyenConfig) -> None:
        client = AdyenManagementClient(config)
        self.merchants = MerchantsEndpoint(client)
        self.stores = MerchantStoresEndpoint(client)
        self.terminals = TerminalsEndpoint(client)
        self.terminal_settings = TerminalSettingsEndpoint(client)
        self.terminal_reassign = TerminalReassignEndpoint(client)


def fetch_merchant_data(**kwargs: Any) -> List[Dict[Any, Any]]:
    return AdyenPaymentTerminalIntegration(kwargs["config"]).merchants.fetch_and_save()


def fetch_store_data(**kwargs: Any) -> List[Dict[Any, Any]]:
    return (
        AdyenPaymentTerminalIntegration(kwargs["config"])
        .stores.fetch_and_save(kwargs["merchant_data"])
    )


def fetch_terminal_data(**kwargs: Any) -> List[Dict[str, Any]]:
    return AdyenPaymentTerminalIntegration(kwargs["config"]).terminals.fetch_and_save()


def fetch_terminal_settings_data(**kwargs: Any) -> List[Dict[str, Any]]:
    return (
        AdyenPaymentTerminalIntegration(kwargs["config"])
        .terminal_settings.fetch_and_save(kwargs["terminal_data"])
    )


def run_terminal_reassignments_from_bigquery(**kwargs: Any) -> None:
    AdyenPaymentTerminalIntegration(kwargs["config"]).terminal_reassign.run_from_bigquery(
        kwargs["project_id"]
    )


def run_terminal_default_settings_patches_from_bigquery(**kwargs: Any) -> None:
    AdyenPaymentTerminalIntegration(
        kwargs["config"]
    ).terminal_settings.run_patches_from_bigquery(kwargs["project_id"])
