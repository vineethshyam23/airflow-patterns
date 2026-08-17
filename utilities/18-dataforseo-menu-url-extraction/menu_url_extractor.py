"""
Read establishment websites from BigQuery, fetch HTML, discover menu URLs,
validate them, and append results. No LLM / embedding / generative calls.

Designed for Airflow PythonOperators. The DAG plans per-country NTILE
partitions; each mapped task calls ``run_extraction_partition``.

Idempotency: skips websites already marked ``_extraction_complete`` and
dedupes (website, fetched_menu_url) pairs already in the destination.

Priority rules per record (P1–P6):
  P1 — source menu_url reachable → keep as-is, skip crawl
  P2 — website path itself looks like a menu → keep website URL
  P3 — HTTP fetch + discover_menu_urls + HEAD/GET validation
  P4 — HTTP blocked / timed out → Cloud Run Playwright scraper
  P5 — discovery returned nothing → write no_menu_links_found
  P6 — all candidates unreachable → write final status row
"""

from __future__ import annotations

import hashlib
import logging
import os
import threading
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Dict, Iterable, Optional, Set, Tuple

import pandas as pd
import requests
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from menu_url_discovery import discover_menu_urls

logger = logging.getLogger(__name__)

DEFAULT_MENU_URL_COLUMN = "website"
DEFAULT_SOURCE_MENU_URL_COLUMN = "menu_url"
DEFAULT_MENU_URL_BATCH_LIMIT = 10000

# Inclusion keywords — DE / ES / FR / IT + English generics
MENU_HREF_SUBSTRINGS = (
    "menu",
    "/food",
    "order-online",
    "takeaway",
    "delivery",
    "speisekarte",
    "menue",
    "menükarte",
    "menü",
    "karte",
    "bestellen",
    "lieferung",
    "bestellung",
    "speisen",
    "menú",
    "carta",
    "comida",
    "platillos",
    "pedir",
    "pedidos",
    "entrega",
    "carte",
    "plats",
    "commander",
    "commande",
    "livraison",
    "piatti",
    "ordinare",
    "ordini",
    "consegna",
)
MENU_TEXT_SUBSTRINGS = (
    "menu",
    "food",
    "order",
    "speisekarte",
    "menü",
    "menue",
    "karte",
    "bestellen",
    "speisen",
    "menú",
    "carta",
    "comida",
    "pedir",
    "carte",
    "plats",
    "commander",
    "livraison",
    "piatti",
    "ordinare",
)

_SKIP_VALIDATION_HOSTS = frozenset(
    {
        "instagram.com",
        "twitter.com",
        "x.com",
        "facebook.com",
        "fb.com",
        "youtube.com",
    }
)

_HTTP_GET_MAX_ATTEMPTS = 2
_HTTP_RETRY_5XX = frozenset((502, 503, 504))
_HTTP_RETRY_TRANSPORT_BACKOFF_SEC = 3.0
_HTTP_RETRY_5XX_BACKOFF_SEC = 5.0
_HTTP_RETRY_403_BACKOFF_SEC = 1.5


@dataclass(frozen=True)
class ExtractorConfig:
    project_id: str
    source_table: str
    dest_table: str
    url_column: str = DEFAULT_MENU_URL_COLUMN
    batch_limit: int = DEFAULT_MENU_URL_BATCH_LIMIT
    request_timeout_sec: float = 10.0
    request_delay_sec: float = 0.3
    gcp_conn_id: str = "bigquery_default"
    http_user_agent: str = (
        "Mozilla/5.0 (compatible; MenuUrlBot/1.0; +https://example.com/bot)"
    )
    fetch_mode: str = "http"  # http | playwright
    request_proxies: Optional[Dict[str, str]] = None
    proxy_on_403_retry: Optional[str] = None


MENU_URL_DEST_SCHEMA = (
    bigquery.SchemaField("menu_url_id", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("restaurant_name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("places_id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("country", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("first_seen", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("establishment_id", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("website", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("menu_url", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("fetched_menu_url", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("fetched_menu_url_status", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("menus_on_page", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("playwright_used", "BOOL", mode="NULLABLE"),
    bigquery.SchemaField("_extraction_complete", "BOOL", mode="NULLABLE"),
    bigquery.SchemaField("_extracted_ts", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("_create_ts", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("_update_ts", "TIMESTAMP", mode="NULLABLE"),
)


def get_bigquery_client(project_id: str, gcp_conn_id: str) -> Tuple[bigquery.Client, str]:
    pid = (project_id or "").strip()
    try:
        from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

        hook = BigQueryHook(gcp_conn_id=gcp_conn_id)
        resolved = pid or (hook.project_id or "")
        if not resolved:
            raise ValueError("Could not resolve GCP project from arguments or hook.")
        return hook.get_client(project_id=resolved), resolved
    except Exception as e:
        logger.warning("BigQueryHook unavailable (%s); using ADC.", e)
        if not pid:
            pid = "dwh_project"
        return bigquery.Client(project=pid), pid


def _airflow_variable(name: str, default: str) -> str:
    try:
        from airflow.models import Variable

        got = Variable.get(name, default_var=default)
        s = default if got is None else str(got).strip()
        return s if s else default
    except Exception:
        return default


def _setting_str(name: str, default: str) -> str:
    env_val = os.environ.get(name, "").strip()
    if env_val:
        return env_val
    return _airflow_variable(name, default)


def _setting_int(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip() or _airflow_variable(name, str(default))
    try:
        return int(raw)
    except ValueError:
        return default


def _setting_float(name: str, default: float) -> float:
    raw = os.environ.get(name, "").strip() or _airflow_variable(name, str(default))
    try:
        return float(raw)
    except ValueError:
        return default


def _optional_nonempty_str(name: str) -> str:
    v = os.environ.get(name, "").strip()
    if v:
        return v
    try:
        from airflow.models import Variable

        raw = Variable.get(name, default_var="")
        return str(raw).strip() if raw is not None else ""
    except Exception:
        return ""


def _build_request_proxies() -> Optional[Dict[str, str]]:
    single = _optional_nonempty_str("MENU_URL_PROXY")
    http_u = _optional_nonempty_str("MENU_URL_PROXY_HTTP")
    https_u = _optional_nonempty_str("MENU_URL_PROXY_HTTPS")
    if single:
        return {"http": single, "https": single}
    out: Dict[str, str] = {}
    if http_u:
        out["http"] = http_u
    if https_u:
        out["https"] = https_u
    return out if out else None


def _normalize_bq_table_id(table_id: str) -> str:
    return table_id.replace("`", "").strip().lower().replace(":", ".")


def _reject_if_dest_is_source(source_table: str, dest_table: str) -> None:
    if _normalize_bq_table_id(source_table) == _normalize_bq_table_id(dest_table):
        raise ValueError(
            "DEST_TABLE cannot equal SOURCE_TABLE; input must stay read-only."
        )


def _parse_bq_table_triplet(full_table_id: str) -> Tuple[str, str, str]:
    parts = full_table_id.replace("`", "").strip().split(".")
    if len(parts) != 3:
        raise ValueError(f"Expected project.dataset.table, got {full_table_id!r}")
    return parts[0], parts[1], parts[2]


def new_menu_url_id(src_url: str, menu_url: str) -> int:
    """Deterministic INT64 from SHA-256 — parallel-safe, no shared counter."""
    digest = hashlib.sha256(f"{src_url}\x00{menu_url}".encode()).digest()
    return int.from_bytes(digest[:6], "big") or 1


def ensure_destination_table_exists(
    client: bigquery.Client, dest_table_full_id: str
) -> None:
    project_id, dataset_id, table_id = _parse_bq_table_triplet(dest_table_full_id)
    ds_ref = bigquery.DatasetReference(project_id, dataset_id)
    table_ref = ds_ref.table(table_id)
    try:
        client.get_table(table_ref)
        return
    except NotFound:
        pass
    try:
        client.get_dataset(ds_ref)
    except NotFound:
        client.create_dataset(bigquery.Dataset(ds_ref), exists_ok=True)
    table = bigquery.Table(table_ref, schema=MENU_URL_DEST_SCHEMA)
    client.create_table(table, exists_ok=True)


def _default_gcp_settings_from_env() -> Tuple[str, str]:
    raw = os.environ.get("env", "").strip()
    if not raw:
        try:
            from airflow.models import Variable

            raw = Variable.get("env", default_var="").strip()
        except Exception:
            raw = ""
    if raw.upper() == "DEV":
        return "dwh_project_dev", "bigquery_default_dev"
    return "dwh_project", "bigquery_default"


def load_config(source_table: str, dest_table: str) -> ExtractorConfig:
    if not source_table or not dest_table:
        raise ValueError("source_table and dest_table must be non-empty.")
    default_project_id, default_gcp_conn_id = _default_gcp_settings_from_env()
    _reject_if_dest_is_source(source_table, dest_table)
    return ExtractorConfig(
        project_id=_setting_str("GCP_PROJECT", default_project_id),
        source_table=source_table,
        dest_table=dest_table,
        url_column=_setting_str("MENU_URL_COLUMN", DEFAULT_MENU_URL_COLUMN)
        or DEFAULT_MENU_URL_COLUMN,
        batch_limit=_setting_int("MENU_URL_BATCH_LIMIT", DEFAULT_MENU_URL_BATCH_LIMIT),
        request_timeout_sec=_setting_float("MENU_URL_REQUEST_TIMEOUT_SEC", 10.0),
        request_delay_sec=_setting_float("MENU_URL_REQUEST_DELAY_SEC", 0.3),
        gcp_conn_id=_setting_str("GCP_CONN_ID", default_gcp_conn_id),
        fetch_mode=_setting_str("menu_url_fetch_mode", "http").strip().lower() or "http",
        request_proxies=_build_request_proxies(),
        proxy_on_403_retry=_optional_nonempty_str("MENU_URL_PROXY_ON_403_RETRY") or None,
    )


def fetch_urls_ntile_partition(
    client: bigquery.Client,
    cfg: ExtractorConfig,
    batch_index: int,
    num_batches: int,
    country_filter: Optional[str] = None,
) -> list[dict]:
    """
    Distinct websites in ``batch_index`` via NTILE(num_batches) OVER (ORDER BY url).
    Skips sites already marked complete in the destination.
    """
    if batch_index < 1 or batch_index > num_batches or num_batches < 1:
        raise ValueError(
            f"Invalid partition batch_index={batch_index!r} num_batches={num_batches!r}"
        )
    col = cfg.url_column.replace("`", "")
    src_menu_col = DEFAULT_SOURCE_MENU_URL_COLUMN.replace("`", "")
    params = [
        bigquery.ScalarQueryParameter("batch_index", "INT64", int(batch_index)),
        bigquery.ScalarQueryParameter("num_batches", "INT64", int(num_batches)),
    ]
    country_where = ""
    if country_filter:
        params.append(
            bigquery.ScalarQueryParameter("country_filter", "STRING", country_filter)
        )
        country_where = "AND CAST(country AS STRING) = @country_filter"

    job_config = bigquery.QueryJobConfig(query_parameters=params)
    q = f"""
    WITH distinct_urls AS (
      SELECT
        TRIM(CAST(`{col}` AS STRING)) AS url,
        ANY_VALUE(CAST(places_id AS STRING)) AS places_id,
        ANY_VALUE(CAST(restaurant_name AS STRING)) AS restaurant_name,
        ANY_VALUE(CAST(establishment_id AS INT64)) AS establishment_id,
        ANY_VALUE(CAST(country AS STRING)) AS country,
        ANY_VALUE(CAST(first_seen AS TIMESTAMP)) AS first_seen,
        ANY_VALUE(CAST(`{src_menu_col}` AS STRING)) AS menu_url
      FROM `{cfg.source_table}`
      WHERE `{col}` IS NOT NULL
        AND TRIM(CAST(`{col}` AS STRING)) != ''
        {country_where}
      GROUP BY url
    ),
    bucketed AS (
      SELECT
        url, places_id, restaurant_name, establishment_id, country, first_seen, menu_url,
        NTILE(@num_batches) OVER (ORDER BY url) AS batch_id
      FROM distinct_urls
    )
    SELECT url, places_id, restaurant_name, establishment_id, country, first_seen, menu_url
    FROM bucketed b
    WHERE batch_id = @batch_index
      AND NOT EXISTS (
        SELECT 1 FROM `{cfg.dest_table}` d
        WHERE d.website = b.url AND COALESCE(d._extraction_complete, FALSE) = TRUE
      )
    ORDER BY url
    """
    rows = list(client.query(q, job_config=job_config).result())
    return [
        {
            "url": r["url"].strip(),
            "places_id": r["places_id"],
            "restaurant_name": r["restaurant_name"],
            "establishment_id": r.get("establishment_id"),
            "country": r.get("country"),
            "first_seen": r["first_seen"],
            "menu_url": r.get("menu_url"),
        }
        for r in rows
        if r.get("url")
    ]


def existing_menu_pairs(
    client: bigquery.Client, dest_table: str, source_urls: Iterable[str]
) -> Set[Tuple[str, str]]:
    if not source_urls:
        return set()
    urls = list(dict.fromkeys(source_urls))
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ArrayQueryParameter("src_urls", "STRING", urls)]
    )
    q = f"""
    SELECT website, fetched_menu_url
    FROM `{dest_table}`
    WHERE website IN UNNEST(@src_urls)
    """
    try:
        result = client.query(q, job_config=job_config).result()
    except Exception as e:
        logger.debug("Destination read failed (table may not exist yet): %s", e)
        return set()
    return {(r["website"], r["fetched_menu_url"]) for r in result}


def _is_third_party_host(url: str) -> bool:
    try:
        host = urllib.parse.urlparse(url).netloc.lower().lstrip("www.")
        return any(host == h or host.endswith("." + h) for h in _SKIP_VALIDATION_HOSTS)
    except Exception:
        return False


def _same_host(url_a: str, url_b: str) -> bool:
    try:
        host_a = urllib.parse.urlparse(url_a).netloc.lower().lstrip("www.")
        host_b = urllib.parse.urlparse(url_b).netloc.lower().lstrip("www.")
        return bool(host_a) and host_a == host_b
    except Exception:
        return False


def _get_js_heavy_domains() -> frozenset:
    raw = _optional_nonempty_str("MENU_URL_JS_HEAVY_DOMAINS") or ""
    if not raw:
        return frozenset()
    return frozenset(
        d.strip().lower().lstrip("www.") for d in raw.split(",") if d.strip()
    )


def _is_js_heavy_domain(url: str) -> bool:
    try:
        domain = urllib.parse.urlparse(url).netloc.lower().lstrip("www.")
        return domain in _get_js_heavy_domains()
    except Exception:
        return False


def _request_get_with_retry(requester, url: str, cfg: ExtractorConfig):
    pending_403_second = False
    for attempt in range(_HTTP_GET_MAX_ATTEMPTS):
        if attempt == 1 and pending_403_second and cfg.proxy_on_403_retry:
            proxies: Optional[Dict[str, str]] = {
                "http": cfg.proxy_on_403_retry,
                "https": cfg.proxy_on_403_retry,
            }
        else:
            proxies = cfg.request_proxies
        try:
            r = requester.get(
                url,
                timeout=(3.0, cfg.request_timeout_sec),
                headers={"User-Agent": cfg.http_user_agent},
                allow_redirects=True,
                proxies=proxies,
            )
            if attempt < _HTTP_GET_MAX_ATTEMPTS - 1:
                if r.status_code == 403:
                    pending_403_second = True
                    time.sleep(_HTTP_RETRY_403_BACKOFF_SEC)
                    continue
                pending_403_second = False
                if r.status_code in _HTTP_RETRY_5XX:
                    time.sleep(_HTTP_RETRY_5XX_BACKOFF_SEC)
                    continue
            return r
        except requests.RequestException:
            if attempt < _HTTP_GET_MAX_ATTEMPTS - 1:
                pending_403_second = False
                time.sleep(_HTTP_RETRY_TRANSPORT_BACKOFF_SEC)
                continue
            raise


def http_get(
    url: str,
    cfg: ExtractorConfig,
    session: Optional[requests.Session] = None,
) -> Tuple[Optional[str], str, str]:
    requester = session or requests
    try:
        r = _request_get_with_retry(requester, url, cfg)
        enc = r.encoding or "utf-8"
        final = r.url or url
        if r.status_code >= 400:
            return None, f"http_{r.status_code}", final
        try:
            return r.content.decode(enc, errors="replace"), "ok", final
        except LookupError:
            return r.content.decode("utf-8", errors="replace"), "ok", final
    except requests.RequestException as e:
        return None, f"request_error:{type(e).__name__}", url


def _fetch_via_playwright(
    url: str, cfg: ExtractorConfig
) -> Tuple[Optional[str], str, str, bool]:
    """Cloud Run scraper with identity-token auth; falls back to plain HTTP."""
    scraper_base_url = _optional_nonempty_str("playwright_scraper_url").rstrip("/")
    if not scraper_base_url:
        logger.warning("playwright_scraper_url not set; falling back to HTTP GET")
        html, status, final = http_get(url, cfg)
        return html, status, final, False

    if scraper_base_url.endswith("/fetch"):
        scraper_base_url = scraper_base_url[: -len("/fetch")]

    try:
        import google.auth.transport.requests
        import google.oauth2.id_token

        auth_req = google.auth.transport.requests.Request()
        id_token = google.oauth2.id_token.fetch_id_token(auth_req, scraper_base_url)
        r = requests.post(
            scraper_base_url + "/fetch",
            json={"url": url},
            headers={"Authorization": f"Bearer {id_token}"},
            timeout=(3.0, cfg.request_timeout_sec + 15),
        )
        r.raise_for_status()
        data = r.json()
        html = data.get("html")
        final_url = data.get("final_url", url)
        if not html:
            raise ValueError("Scraper service response missing html")
        return html, "ok", final_url, True
    except Exception as e:
        logger.warning("Scraper service failed (%s); HTTP fallback", e)
        html, status, final = http_get(url, cfg)
        return html, status, final, False


def fetch_html(
    url: str,
    cfg: ExtractorConfig,
    session: Optional[requests.Session] = None,
) -> Tuple[Optional[str], str, str, bool]:
    mode = (cfg.fetch_mode or "http").lower()
    if mode == "playwright":
        return _fetch_via_playwright(url, cfg)

    html, status, final = http_get(url, cfg, session=session)
    triggers = (
        "http_403",
        "http_429",
        "http_520",
        "http_503",
        "request_error:ReadTimeout",
        "request_error:ConnectionError",
    )
    if html is None and status in triggers:
        return _fetch_via_playwright(url, cfg)
    if html is None and _is_js_heavy_domain(url):
        return _fetch_via_playwright(url, cfg)
    return html, status, final, False


def validate_url(
    url: str,
    cfg: ExtractorConfig,
    session: Optional[requests.Session] = None,
    src_url: str = "",
) -> Tuple[bool, str]:
    from http import HTTPStatus

    def _status_label(code: int) -> str:
        try:
            reason = HTTPStatus(code).phrase
        except ValueError:
            reason = "Unknown"
        return f"{code}-{reason}"

    if _is_third_party_host(url):
        return False, "skipped:third_party"

    try:
        parsed = urllib.parse.urlparse(url)
        if parsed.scheme not in ("http", "https") or not parsed.netloc:
            return False, "invalid_url:bad_scheme_or_host"
    except Exception:
        return False, "invalid_url:parse_error"

    requester = session or requests
    try:
        r = requester.head(
            url,
            timeout=(3.0, cfg.request_timeout_sec),
            headers={"User-Agent": cfg.http_user_agent},
            allow_redirects=True,
            proxies=cfg.request_proxies,
        )
        if r.status_code in (405, 501):
            r = _request_get_with_retry(requester, url, cfg)
        # Own-domain 403: page exists, bots blocked — keep for later Playwright.
        if r.status_code == 403 and src_url and _same_host(url, src_url):
            return True, _status_label(r.status_code)
        return r.status_code < 400, _status_label(r.status_code)
    except requests.RequestException as e:
        return False, f"request_error:{type(e).__name__}"


def _row(
    src: str,
    candidate: Optional[str],
    source_menu_url: Optional[str],
    status: str,
    menus_on_page: int,
    src_rec: dict,
    playwright_used: bool,
    fetched: Optional[str] = None,
) -> dict:
    return {
        "menu_url_id": new_menu_url_id(src, candidate or source_menu_url or ""),
        "website": src,
        "menu_url": source_menu_url,
        "fetched_menu_url": fetched if fetched is not None else candidate,
        "fetched_menu_url_status": status,
        "menus_on_page": menus_on_page,
        "places_id": src_rec.get("places_id"),
        "restaurant_name": src_rec.get("restaurant_name"),
        "establishment_id": src_rec.get("establishment_id"),
        "country": src_rec.get("country"),
        "first_seen": src_rec.get("first_seen"),
        "playwright_used": playwright_used,
        "_extraction_complete": True,
    }


def process_source_record(
    src_rec: dict,
    cfg: ExtractorConfig,
    existing: Set[Tuple[str, str]],
    http_session: requests.Session,
) -> list[dict]:
    rows: list[dict] = []
    src = src_rec["url"]
    source_menu_url = src_rec.get("menu_url")

    # P1 — trust a reachable source menu_url
    if source_menu_url and str(source_menu_url).strip():
        candidate = str(source_menu_url).strip()
        if (src, candidate) in existing:
            return rows
        ok, url_status = validate_url(candidate, cfg, session=http_session, src_url=src)
        if ok:
            rows.append(
                _row(
                    src,
                    candidate,
                    source_menu_url,
                    url_status,
                    1,
                    src_rec,
                    False,
                    fetched=None,
                )
            )
            return rows

    # P2 — website path itself looks like a menu
    if not source_menu_url or not str(source_menu_url).strip():
        if any(k in src.lower() for k in MENU_HREF_SUBSTRINGS):
            if (src, src) in existing:
                return rows
            ok, url_status = validate_url(src, cfg, session=http_session, src_url=src)
            if ok:
                rows.append(
                    _row(src, src, source_menu_url, url_status, 1, src_rec, False, fetched=None)
                )
                return rows

    # P3 / P4 — fetch + discover
    time.sleep(cfg.request_delay_sec)
    html, status, page_base, playwright_used = fetch_html(
        src, cfg, session=http_session
    )

    if html is None:
        if (src, None) not in existing:
            rows.append(
                _row(src, None, source_menu_url, status, 0, src_rec, bool(playwright_used))
            )
        return rows

    menus = discover_menu_urls(
        html, page_base, MENU_HREF_SUBSTRINGS, MENU_TEXT_SUBSTRINGS
    )
    if not menus:
        if (src, None) not in existing:
            rows.append(
                _row(
                    src,
                    None,
                    source_menu_url,
                    "no_menu_links_found",
                    0,
                    src_rec,
                    bool(playwright_used),
                )
            )
        return rows

    strong = [u for u in menus if any(k in u.lower() for k in MENU_HREF_SUBSTRINGS)]
    if strong:
        menus = strong

    found_valid = False
    for m in menus:
        if (src, m) in existing:
            continue
        ok, url_status = validate_url(m, cfg, session=http_session, src_url=src)
        if not ok:
            continue
        found_valid = True
        rows.append(
            _row(
                src,
                m,
                source_menu_url,
                url_status,
                len(menus),
                src_rec,
                bool(playwright_used),
                fetched=m,
            )
        )

    if not found_valid and menus and (src, None) not in existing:
        rows.append(
            _row(
                src,
                None,
                source_menu_url,
                "all_candidates_unreachable",
                len(menus),
                src_rec,
                bool(playwright_used),
            )
        )
    return rows


def run_extraction_partition(
    batch_index: int,
    num_batches: int,
    cfg: Optional[ExtractorConfig] = None,
    country_filter: Optional[str] = None,
) -> int:
    """Process one NTILE partition. Returns rows appended."""
    if cfg is None:
        raise ValueError("cfg is required (pass load_config(...) from the DAG)")

    client, _ = get_bigquery_client(cfg.project_id, cfg.gcp_conn_id)
    try:
        import pandas_gbq
    except ImportError as e:  # pragma: no cover
        raise RuntimeError("pandas_gbq is required to load into BigQuery.") from e

    _reject_if_dest_is_source(cfg.source_table, cfg.dest_table)
    ensure_destination_table_exists(client, cfg.dest_table)
    dest_project, _, _ = _parse_bq_table_triplet(cfg.dest_table)

    from requests.adapters import HTTPAdapter

    http_session = requests.Session()
    http_session.headers.update({"User-Agent": cfg.http_user_agent})
    adapter = HTTPAdapter(pool_connections=10, pool_maxsize=10, max_retries=0)
    http_session.mount("https://", adapter)
    http_session.mount("http://", adapter)

    try:
        sources = fetch_urls_ntile_partition(
            client, cfg, int(batch_index), int(num_batches), country_filter=country_filter
        )
        if not sources:
            return 0

        existing = existing_menu_pairs(
            client, cfg.dest_table, [s["url"] for s in sources]
        )

        bq_mini_batch = 250
        workers = 5
        rows: list[dict] = []
        total_appended = 0
        rows_lock = threading.Lock()
        seen_ids: set[int] = set()

        def _flush(buffer: list[dict]) -> int:
            if not buffer:
                return 0
            df = pd.DataFrame(buffer)
            df["_extracted_ts"] = pd.Timestamp.utcnow()
            if "establishment_id" in df.columns:
                df["establishment_id"] = pd.array(
                    df["establishment_id"], dtype=pd.Int64Dtype()
                )
            pandas_gbq.to_gbq(
                df,
                cfg.dest_table,
                project_id=dest_project,
                if_exists="append",
                progress_bar=False,
            )
            return len(df)

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(
                    process_source_record, rec, cfg, existing, http_session
                ): rec
                for rec in sources
            }
            for future in as_completed(futures):
                try:
                    output_rows = future.result()
                    with rows_lock:
                        for row in output_rows:
                            row_id = row.get("menu_url_id")
                            if row_id and row_id not in seen_ids:
                                seen_ids.add(row_id)
                                rows.append(row)
                        if len(rows) >= bq_mini_batch:
                            total_appended += _flush(rows)
                            rows = []
                except Exception as e:
                    src_rec = futures[future]
                    logger.warning("Error processing %s: %s", src_rec.get("url", "?"), e)

        total_appended += _flush(rows)
        return total_appended
    finally:
        try:
            client.close()
        except Exception:
            pass
        try:
            http_session.close()
        except Exception:
            pass
