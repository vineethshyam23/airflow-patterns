"""Call the Cloud Run menu renderer from a Composer task.

HTTP fallback stays here so a dead renderer does not fail the whole batch.
The service URL comes from Airflow Variable `playwright_scraper_url`
(base URL, or a full .../fetch URL). Never hardcode a run.app host.
"""

from __future__ import annotations

import logging

import requests

logger = logging.getLogger(__name__)

_PLAYWRIGHT_TRIGGERS = (
    "http_403",
    "http_429",
    "http_503",
    "http_520",
    "request_error:ReadTimeout",
    "request_error:ConnectionError",
)


def _fetch_url(scraper_base_url: str) -> str:
    base = scraper_base_url.rstrip("/")
    if base.endswith("/fetch"):
        base = base[: -len("/fetch")]
    return base + "/fetch"


def http_get(url: str, timeout_sec: int = 20) -> tuple[str | None, str, str]:
    """Return (html, status, final_url). html is None on failure."""
    try:
        response = requests.get(
            url,
            timeout=timeout_sec,
            allow_redirects=True,
            headers={"User-Agent": "menu-extract/1.0"},
        )
    except requests.RequestException as exc:
        return None, f"request_error:{type(exc).__name__}", url
    if response.status_code >= 400:
        return None, f"http_{response.status_code}", response.url
    return response.text, "ok", response.url


def fetch_rendered_page(
    url: str,
    scraper_base_url: str,
    timeout_sec: int = 35,
) -> tuple[str | None, str, bool]:
    """POST /fetch. Returns (html, final_url, playwright_used).

    playwright_used is True only when Cloud Run returned HTML. On any
    service error we fall back to a plain GET and report False.
    """
    if not scraper_base_url:
        logger.warning("playwright_scraper_url is empty; HTTP only")
        html, _status, final = http_get(url)
        return html, final, False

    try:
        import google.auth.transport.requests
        import google.oauth2.id_token

        audience = scraper_base_url.rstrip("/")
        if audience.endswith("/fetch"):
            audience = audience[: -len("/fetch")]
        auth_req = google.auth.transport.requests.Request()
        token = google.oauth2.id_token.fetch_id_token(auth_req, audience)
        response = requests.post(
            _fetch_url(scraper_base_url),
            json={"url": url},
            headers={"Authorization": f"Bearer {token}"},
            timeout=(3.0, timeout_sec),
        )
        response.raise_for_status()
        body = response.json()
        html = body.get("html")
        if not html:
            raise ValueError("renderer response missing html")
        return html, body.get("final_url", url), True
    except Exception as exc:
        logger.warning("renderer failed (%s); HTTP fallback", exc)
        html, _status, final = http_get(url)
        return html, final, False


def fetch_menu_page(
    url: str,
    scraper_base_url: str,
    js_heavy: bool = False,
) -> tuple[str | None, str, str, bool]:
    """HTTP first, Cloud Run when the status or the host says so.

    Returns (html, status, final_url, playwright_used).
    """
    html, status, final = http_get(url)
    if html is not None and not js_heavy:
        return html, status, final, False
    if html is None and status not in _PLAYWRIGHT_TRIGGERS and not js_heavy:
        return html, status, final, False
    rendered, rendered_url, used = fetch_rendered_page(url, scraper_base_url)
    if rendered is None:
        return None, status if not used else "renderer_failed", final, used
    return rendered, "ok", rendered_url, used
