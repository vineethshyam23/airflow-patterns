"""Collections partner API v2 connector.

Official docs: partner developer portal (collections API v2).
Auth: Authorization: Bearer <api_key>
List: GET /api/v2/case_files  (pagination: from + amount)
Detail: GET /api/v2/case_files/{case_file_id}

Do not hardcode API keys in this module.
"""

from __future__ import annotations

import json
import logging
import socket
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, Iterable, List, Optional

logger = logging.getLogger(__name__)

PAGE_SIZE = 500
DEFAULT_TIMEOUT_SECONDS = 60
DEFAULT_MAX_ATTEMPTS = 3
MAX_PAGES = 20_000

MARKET_CONFIG: Dict[str, Dict[str, str]] = {
    "AT": {
        "base_url": "https://at.collections.example.com/api/v2",
        "merchant": "partner_at",
    },
    "DE": {
        "base_url": "https://app.collections.example.com/api/v2",
        "merchant": "partner_de",
    },
    "FR": {
        "base_url": "https://app.collections.example.com/api/v2",
        "merchant": "partner_fr",
    },
    "ES": {
        "base_url": "https://app.collections.example.com/api/v2",
        "merchant": "partner_es",
    },
    "IT": {
        "base_url": "https://app.collections.example.com/api/v2",
        "merchant": "partner_it",
    },
}

RETRYABLE_STATUS = {429, 500, 502, 503, 504}


class PairFinanceApiError(Exception):
    """Base error for collections partner API calls."""


class PairFinanceRateLimitError(PairFinanceApiError):
    """HTTP 429."""


class PairFinanceServerError(PairFinanceApiError):
    """HTTP 5xx."""


def normalize_market(market: str) -> str:
    code = (market or "").strip().upper()
    if code not in MARKET_CONFIG:
        raise ValueError(f"Unsupported market '{market}'. Expected one of {sorted(MARKET_CONFIG)}")
    return code


def market_base_url(market: str) -> str:
    return MARKET_CONFIG[normalize_market(market)]["base_url"].rstrip("/")


def market_merchant(market: str) -> str:
    return MARKET_CONFIG[normalize_market(market)]["merchant"]


def _headers(api_key: str) -> Dict[str, str]:
    if not api_key:
        raise PairFinanceApiError("API key is empty")
    return {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def _http_request(
    method: str,
    url: str,
    headers: Dict[str, str],
    timeout: int = DEFAULT_TIMEOUT_SECONDS,
) -> tuple[int, str]:
    request = urllib.request.Request(url, method=method, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.status, response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        return exc.code, body
    except socket.timeout as exc:
        raise TimeoutError(f"Collections partner request timed out: {url}") from exc
    except urllib.error.URLError as exc:
        reason = exc.reason
        if isinstance(reason, socket.timeout) or "timed out" in str(reason).lower():
            raise TimeoutError(f"Collections partner request timed out: {url}") from exc
        raise PairFinanceApiError(f"Collections partner network error: {reason}") from exc


def _request_with_retry(
    method: str,
    url: str,
    api_key: str,
    *,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    timeout: int = DEFAULT_TIMEOUT_SECONDS,
    sleep_fn=time.sleep,
) -> Any:
    delay = 1.0
    last_error: Optional[BaseException] = None
    for attempt in range(1, max_attempts + 1):
        try:
            status, body = _http_request(method, url, _headers(api_key), timeout=timeout)
            if status == 429:
                raise PairFinanceRateLimitError(f"HTTP 429 rate limit: {url}")
            if status >= 500:
                raise PairFinanceServerError(f"HTTP {status} server error: {url}")
            if status >= 400:
                raise PairFinanceApiError(f"HTTP {status} from collections partner: {body[:300]}")
            if not body:
                return {}
            return json.loads(body)
        except (TimeoutError, PairFinanceRateLimitError, PairFinanceServerError) as exc:
            last_error = exc
            if attempt == max_attempts:
                logger.warning(
                    "Collections partner attempt %s/%s failed (%s). No retries left.",
                    attempt,
                    max_attempts,
                    exc,
                )
                raise
            logger.warning(
                "Collections partner attempt %s/%s failed (%s). Retrying in %.1fs",
                attempt,
                max_attempts,
                exc,
                delay,
            )
            sleep_fn(delay)
            delay *= 2
    raise PairFinanceApiError(f"Collections partner request failed: {last_error}")


def _extract_cases(payload: Any) -> List[Dict[str, Any]]:
    if payload is None:
        return []
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    if isinstance(payload, dict):
        for key in ("cases", "data", "case_files"):
            value = payload.get(key)
            if isinstance(value, list):
                return [row for row in value if isinstance(row, dict)]
    return []


def list_cases(
    market: str,
    api_key: str,
    *,
    amount: int = PAGE_SIZE,
    updated_from: Optional[int] = None,
    updated_to: Optional[int] = None,
    merchant: Optional[str] = None,
    extra_params: Optional[Dict[str, Any]] = None,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    sleep_fn=time.sleep,
) -> List[Dict[str, Any]]:
    """List all case files for a market, following pagination (500/page)."""
    code = normalize_market(market)
    base = market_base_url(code)
    page_size = amount or PAGE_SIZE
    offset = 0
    collected: List[Dict[str, Any]] = []

    for _page in range(MAX_PAGES):
        params: Dict[str, Any] = {
            "from": offset,
            "amount": page_size,
        }
        if merchant:
            params["merchant"] = merchant
        if updated_from is not None:
            params["updated_from"] = updated_from
        if updated_to is not None:
            params["updated_to"] = updated_to
        if extra_params:
            params.update(extra_params)
        query = urllib.parse.urlencode({k: v for k, v in params.items() if v is not None})
        url = f"{base}/case_files?{query}"
        payload = _request_with_retry(
            "GET",
            url,
            api_key,
            max_attempts=max_attempts,
            sleep_fn=sleep_fn,
        )
        batch = _extract_cases(payload)
        collected.extend(batch)

        total = None
        if isinstance(payload, dict):
            meta = payload.get("meta") or {}
            total = meta.get("total_cases")

        if not batch or len(batch) < page_size:
            break
        offset += page_size
        if total is not None and offset >= int(total):
            break
    else:
        logger.warning("list_cases hit MAX_PAGES=%s for market=%s", MAX_PAGES, code)

    logger.info("Listed %s cases for market=%s", len(collected), code)
    return collected


def get_case(
    case_id: str,
    market: str,
    api_key: str,
    *,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    sleep_fn=time.sleep,
) -> Dict[str, Any]:
    """Fetch one case file by id."""
    if not case_id:
        raise PairFinanceApiError("case_id is required")
    code = normalize_market(market)
    url = f"{market_base_url(code)}/case_files/{urllib.parse.quote(str(case_id), safe='')}"
    payload = _request_with_retry(
        "GET",
        url,
        api_key,
        max_attempts=max_attempts,
        sleep_fn=sleep_fn,
    )
    if isinstance(payload, dict) and "id" not in payload:
        inner = payload.get("case") or payload.get("data")
        if isinstance(inner, dict):
            return inner
    if not isinstance(payload, dict):
        raise PairFinanceApiError(f"Unexpected get_case payload type: {type(payload)}")
    return payload


def _as_str(value: Any) -> Optional[str]:
    """Keep landing types aligned with the STRING staging table (avoid BQ INTEGER autodetect)."""
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return str(value)


def company_display_name(value: Any) -> Optional[str]:
    """Company name column is the display name only, not the API object/JSON."""
    if value is None:
        return None
    if isinstance(value, dict):
        inner = value.get("company_name")
        return _as_str(inner) if inner not in (None, "") else None
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.startswith("{"):
            try:
                parsed = json.loads(stripped)
            except json.JSONDecodeError:
                return value
            if isinstance(parsed, dict):
                return company_display_name(parsed)
        return value
    return _as_str(value)


def flatten_case(
    case: Dict[str, Any],
    market: str,
    load_date: str,
    ingested_at: str,
) -> Dict[str, Any]:
    """Normalize a case file for NDJSON / staging BQ."""
    company = case.get("company_name")
    return {
        "case_id": _as_str(case.get("id")),
        "reference_id": _as_str(case.get("reference_id")),
        "merchant": _as_str(case.get("merchant")),
        "status": _as_str(case.get("state")),
        "phase": _as_str(case.get("phase")),
        "amount": _as_str(case.get("debt_total")),
        "currency": _as_str(case.get("currency")),
        "market": normalize_market(market),
        "created_at": _as_str(case.get("created_at")),
        "updated_at": _as_str(case.get("updated_at")),
        "customer_number": _as_str(case.get("customer_number")),
        "first_name": _as_str(case.get("first_name")),
        "last_name": _as_str(case.get("last_name")),
        "company_name": company_display_name(company),
        "raw_json": json.dumps(case, ensure_ascii=False),
        "load_date": load_date,
        "ingested_at": ingested_at,
    }


def cases_to_ndjson(rows: Iterable[Dict[str, Any]]) -> str:
    return "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows)
