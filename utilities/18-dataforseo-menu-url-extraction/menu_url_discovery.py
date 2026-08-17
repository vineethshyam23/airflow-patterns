"""
Discover restaurant / menu-like URLs from HTML without LLMs.

Covers:
  - Anchor tags (keyword heuristics on href + visible text)
  - JSON-LD (schema.org Menu, Restaurant, etc.)
  - Embedded SPA JSON (__NEXT_DATA__, Nuxt payloads)
  - data-* attributes that often hold router targets

Plain HTTP HTML is enough when JSON is in the initial response.
For DOM that only exists after JS, the caller should fetch via a
Playwright / headless service (see MENU_URL_FETCH_MODE in the extractor).
"""

from __future__ import annotations

import json
import logging
import re
from html import unescape
from typing import Any, Iterable, List, Sequence, Set
from urllib.parse import urlparse

from menu_url_utils import normalize_menu_url

try:
    from selectolax.parser import HTMLParser
except ImportError:  # pragma: no cover
    HTMLParser = None  # type: ignore

logger = logging.getLogger(__name__)

_JSONLD_HORECA_TYPES = frozenset(
    {
        "Menu",
        "Restaurant",
        "FoodEstablishment",
        "FoodService",
        "OfferCatalog",
        "CafeOrCoffeeShop",
        "BarOrPub",
        "Hotel",
        "LodgingBusiness",
    }
)

_RX_LD_JSON = re.compile(
    r'<script[^>]+type\s*=\s*["\']application/ld\+json["\'][^>]*>(.*?)</script>',
    re.IGNORECASE | re.DOTALL,
)

_RX_EMBEDDED = (
    re.compile(
        r'<script[^>]*id=["\']__NEXT_DATA__["\'][^>]*>(.*?)</script>',
        re.I | re.DOTALL,
    ),
    re.compile(
        r'<script[^>]*id=["\']__NUXT_DATA__["\'][^>]*>(.*?)</script>',
        re.I | re.DOTALL,
    ),
)

_DATA_ATTRS = (
    "data-url",
    "data-href",
    "data-link",
    "data-menu",
    "data-menu-url",
    "data-to",
    "data-path",
)
_DATA_ATTRS_SELECTOR = ",".join(f"[{a}]" for a in _DATA_ATTRS)

_MEDIA_EXTENSIONS = frozenset(
    {
        ".jpg",
        ".jpeg",
        ".png",
        ".gif",
        ".webp",
        ".svg",
        ".ico",
        ".mp4",
        ".mp3",
        ".zip",
        ".woff",
        ".woff2",
        ".ttf",
        ".eot",
        ".css",
        ".js",
        ".map",
    }
)


def _to_normalized_urls(raw: Iterable[str], base_url: str) -> List[str]:
    normalized = (normalize_menu_url(base_url, s) for s in raw)
    return list(dict.fromkeys(n for n in normalized if n))


def _type_tokens(node: dict) -> Set[str]:
    raw = node.get("@type")
    if raw is None:
        return set()
    items = raw if isinstance(raw, list) else [raw]
    return {x.rsplit("/", 1)[-1].split("#")[-1] for x in items if isinstance(x, str)}


def _jsonld_tree_has_horeca(obj: Any) -> bool:
    if isinstance(obj, dict):
        if _type_tokens(obj) & _JSONLD_HORECA_TYPES:
            return True
        return any(_jsonld_tree_has_horeca(v) for v in obj.values())
    if isinstance(obj, list):
        return any(_jsonld_tree_has_horeca(x) for x in obj)
    return False


def _is_media_url(url: str) -> bool:
    try:
        path = urlparse(url.strip()).path.lower()
    except Exception:
        return False
    dot = path.rfind(".")
    if dot == -1:
        return False
    return path[dot:].split("?")[0] in _MEDIA_EXTENSIONS


def _is_schema_or_ontology_url(url: str) -> bool:
    """Drop vocabulary IRIs (schema.org/Tuesday, w3.org/...) — not navigable pages."""
    try:
        host = (urlparse(url.strip()).netloc or "").lower()
    except Exception:
        return False
    if not host:
        return False
    if host == "schema.org" or host.endswith(".schema.org"):
        return True
    if host in ("www.w3.org", "w3.org"):
        return True
    return False


def _collect_http_strings(obj: Any, bucket: Set[str]) -> None:
    if isinstance(obj, str):
        s = obj.strip()
        if s.startswith("http://") or s.startswith("https://"):
            if not _is_schema_or_ontology_url(s) and not _is_media_url(s):
                bucket.add(s)
    elif isinstance(obj, dict):
        for v in obj.values():
            _collect_http_strings(v, bucket)
    elif isinstance(obj, list):
        for x in obj:
            _collect_http_strings(x, bucket)


def extract_urls_from_json_ld(html: str, base_url: str) -> List[str]:
    found: Set[str] = set()
    for m in _RX_LD_JSON.finditer(html or ""):
        raw = unescape(m.group(1)).strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            logger.debug("Skipping invalid JSON-LD chunk")
            continue
        if not _jsonld_tree_has_horeca(data):
            continue
        _collect_http_strings(data, found)
    return _to_normalized_urls(found, base_url)


def extract_urls_from_embedded_json(html: str, base_url: str) -> List[str]:
    found: Set[str] = set()
    for rx in _RX_EMBEDDED:
        for m in rx.finditer(html or ""):
            raw = unescape(m.group(1)).strip()
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                continue
            _collect_http_strings(data, found)
    return _to_normalized_urls(found, base_url)


def extract_urls_from_data_attributes(html: str, base_url: str) -> List[str]:
    if not html or HTMLParser is None:
        return []
    tree = HTMLParser(html)
    raw: List[str] = []
    for node in tree.css(_DATA_ATTRS_SELECTOR):
        try:
            attrs = node.attributes
        except Exception:
            continue
        for attr in _DATA_ATTRS:
            val = attrs.get(attr)
            if val and str(val).strip():
                raw.append(str(val).strip())
    return _to_normalized_urls(raw, base_url)


def extract_urls_from_anchors(
    html: str,
    base_url: str,
    href_substrings: Sequence[str],
    text_substrings: Sequence[str],
) -> List[str]:
    if not html or HTMLParser is None:
        return []
    tree = HTMLParser(html)
    hrefs: List[str] = []
    for node in tree.css("a[href]"):
        try:
            href = node.attributes.get("href") or ""
        except Exception:
            continue
        href_l = href.lower()
        text = (node.text() or "").lower()
        if any(k in href_l for k in href_substrings) or any(
            k in text for k in text_substrings
        ):
            hrefs.append(href)
    return _to_normalized_urls(hrefs, base_url)


def _href_heuristic(url: str, href_substrings: Sequence[str]) -> bool:
    u = url.lower()
    return any(k in u for k in href_substrings)


def discover_menu_urls(
    html: str,
    base_url: str,
    href_substrings: Sequence[str],
    text_substrings: Sequence[str],
) -> List[str]:
    """
    Merge anchors, JSON-LD, embedded JSON, and data-* attributes.

    Anchor matches are unchanged vs. an anchor-only extractor; extra sources
    only add candidates. Order: JSON-LD (href-filtered), anchors, then
    embedded/data with href heuristics.
    """
    json_ld = extract_urls_from_json_ld(html, base_url)
    anchors = extract_urls_from_anchors(
        html, base_url, href_substrings, text_substrings
    )
    embedded = extract_urls_from_embedded_json(html, base_url)
    data_attrs = extract_urls_from_data_attributes(html, base_url)

    candidates = (
        [u for u in json_ld if _href_heuristic(u, href_substrings)]
        + anchors
        + [u for u in embedded if _href_heuristic(u, href_substrings)]
        + [u for u in data_attrs if _href_heuristic(u, href_substrings)]
    )
    return list(dict.fromkeys(candidates))
