"""
Shared URL normalization for menu / HoReCa link discovery.

Used by ``menu_url_discovery`` (anchors, JSON-LD, embedded JSON, data-*) and
re-exported via ``gbq_menu_url_extractor`` so all paths resolve and filter
links the same way without duplicating logic. Pure ``urllib.parse`` rules only — no AI/LLM.
"""

from __future__ import annotations

from typing import Optional
from urllib.parse import urljoin, urlparse, urlunparse


def normalize_menu_url(base_url: str, href: str) -> Optional[str]:
    """Resolve relative links, keep http(s) only, strip fragments."""
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
    cleaned = urlunparse(
        (
            parsed.scheme.lower(),
            parsed.netloc.lower(),
            parsed.path or "/",
            parsed.params,
            parsed.query,
            "",
        )
    )
    if cleaned.rstrip("/") == base_url.rstrip("/"):
        return None
    return cleaned
