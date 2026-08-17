"""
Shared URL normalization for menu / HoReCa link discovery.

Used by ``menu_url_discovery`` (anchors, JSON-LD, embedded JSON, data-*) and
the BigQuery extractor so all paths resolve and filter links the same way.
Pure ``urllib.parse`` rules — no LLM.
"""

from __future__ import annotations

from typing import Optional
from urllib.parse import urljoin, urlparse, urlunparse

# Path keywords that indicate a menu-like page (also gate query-string stripping)
_MENU_PATH_KEYWORDS = ("menu", "carta", "speisekarte", "karte", "getranke", "food", "drink")


def _path_has_menu_keyword(path: str) -> bool:
    low = path.lower()
    return any(kw in low for kw in _MENU_PATH_KEYWORDS)


def normalize_menu_url(base_url: str, href: str) -> Optional[str]:
    """
    Resolve relative links, keep http(s) only, strip fragments.
    Strip query params only when the path already contains a menu keyword
    (e.g. /menu?product=72 → /menu) so /?page=menu is not destroyed.
    """
    if not href:
        return None
    href = href.strip()
    low = href.lower()
    if low.startswith(("#", "javascript:", "mailto:", "tel:", "data:")):
        return None
    absolute = urljoin(base_url, href)
    parsed = urlparse(absolute)
    if parsed.scheme not in ("http", "https"):
        return None

    keep_query = "" if _path_has_menu_keyword(parsed.path) else parsed.query

    cleaned = urlunparse(
        (
            parsed.scheme.lower(),
            parsed.netloc.lower(),
            parsed.path or "/",
            "",
            keep_query,
            "",
        )
    )
    if cleaned.rstrip("/") == base_url.rstrip("/"):
        return None
    return cleaned
