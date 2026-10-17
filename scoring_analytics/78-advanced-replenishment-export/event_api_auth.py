"""Token helpers for parallel event-ingest export tasks.

Identity providers often return 403 when many Composer tasks request
tokens at the same wall-clock. Retry client_credentials with long
backoff + jitter; do not immediately fall through to password grant
on a throttle (that doubles IdP load).

Stagger concurrent tasks so ten hash-partition workers do not all
hit the token endpoint together.
"""

from __future__ import annotations

import logging
import random
import time
import zlib

import requests

RETRYABLE_TOKEN_STATUS_CODES = (403, 429, 500, 502, 503, 504)


def is_retryable_token_error(exc: BaseException) -> bool:
    """True when a token request should be retried (throttle / transient)."""
    response = getattr(exc, "response", None)
    status = getattr(response, "status_code", None)
    if status in RETRYABLE_TOKEN_STATUS_CODES:
        return True
    return isinstance(
        exc, (requests.exceptions.Timeout, requests.exceptions.ConnectionError)
    )


def raise_retryable_token_error(exc: BaseException) -> None:
    """Re-raise throttle errors so callers retry client_credentials only."""
    if is_retryable_token_error(exc):
        raise


def stagger_parallel_token_request(
    slot_key: str | None = None, max_delay_seconds: float = 8.0
) -> None:
    """Spread concurrent batch tasks so they do not stampede the IdP.

    ``slot_key`` (usually a batch label) adds a stable extra delay so
    retries of many failed tasks do not stay synchronized.
    """
    delay = random.uniform(0.0, max_delay_seconds)
    if slot_key is not None:
        delay += zlib.crc32(str(slot_key).lower().encode("utf-8")) % 60
    time.sleep(delay)


def acquire_token_with_retry(acquire_once, max_retries: int = 8):
    """Call ``acquire_once`` until it returns a token or retries are exhausted."""
    last_exc = None
    for attempt in range(max_retries):
        try:
            return acquire_once()
        except Exception as e:
            last_exc = e
            if not is_retryable_token_error(e) or attempt == max_retries - 1:
                break
            base = min(20 * (2**attempt), 120)
            wait = base + random.uniform(5.0, 40.0)
            logging.warning(
                "Token request failed (attempt %d/%d): %s — retrying in %.1fs",
                attempt + 1,
                max_retries,
                e,
                wait,
            )
            time.sleep(wait)

    logging.error("All token authentication options exhausted.")
    if last_exc:
        raise last_exc
    raise RuntimeError("Failed to obtain access token")
