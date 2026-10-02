#!/usr/bin/env python3
"""
Read page URLs from BigQuery, fetch HTML at runtime, extract likely menu URLs,
normalize them, and write results back to BigQuery. This module does not call any LLM,
embedding API, or generative AI service.

Intended to run inside Airflow (PythonOperator), The DAG file’s
``project_id`` / ``gcp_conn_id`` are not passed into the task callable; ``load_config()`` uses
the same fallback strings as the DAG only when ``GCP_PROJECT`` / ``GCP_CONN_ID`` are unset
(Variables or environment).

Idempotency: only rows with a non-null ``menu_url`` are written; skips ``(source_url, menu_url)``
pairs already present in the destination table (safe to re-run).

Each row gets a non-null random ``menu_url_id`` (INT64): unique across parallel tasks without
shared counters or ``MAX(id)+1`` (which would race). Not monotonic. A PRIMARY KEY on ``menu_url_id``
is declared on new tables (metadata for the optimizer; not enforced by BigQuery).

**Input table is never modified:** read-only ``SELECT``s pull distinct URLs from ``MENU_URL_SOURCE_TABLE``.
For Airflow parallel runs, the DAG's planning task counts distinct URLs and divides them into NTILE
partitions; each mapped task calls ``run_extraction_partition(batch_index, num_batches)`` to process
its slice. All writes use ``pandas_gbq.to_gbq`` against ``MENU_URL_DEST_TABLE`` only.
If source and destination are the same table, the job raises an error.

**Destination:** default ``MENU_URL_DEST_TABLE`` is always under project ``source_project``
(regardless of ``GCP_PROJECT``, which may point elsewhere for API / cross-project reads). Override
with Variable or env if needed. Missing table/dataset is created once; loads use append only.

**Discovery (``menu_url_discovery``):** anchor keywords, JSON-LD (HoReCa schema), embedded SPA JSON
(Next/Nuxt), and ``data-*`` URL attributes. For pages that only render after JS, set
``MENU_URL_FETCH_MODE=playwright`` (requires ``playwright`` + browser install on the worker).

**HTTP proxies (optional, Airflow Variable or env):** ``MENU_URL_PROXY`` (http+https),
or ``MENU_URL_PROXY_HTTP`` / ``MENU_URL_PROXY_HTTPS``. Use ``MENU_URL_PROXY_ON_403_RETRY`` for a
proxy URL applied **only** on the second GET after a **403** (direct request first), which can
help with IP-based blocks without sending all traffic via the proxy.
"""

from __future__ import annotations

import hashlib
import logging
import os
import time
import urllib.parse
from dataclasses import dataclass
from typing import Dict, Iterable, Optional, Set, Tuple

import pandas as pd
import requests
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

try:
    from .db_connections import initialize_db_connection
except ImportError:  # pragma: no cover
    from modules.db_connections import initialize_db_connection


def _import_discover_and_normalize():
    """
    Import ``discover_menu_urls`` and ``normalize_menu_url`` with two fallbacks:
      1. Relative import — works when this file is loaded inside an installed package.
      2. Patch sys.path with the DAGs root, retry absolute import, then load sibling
         .py files directly via importlib as a last resort.
    """
    import sys

    # 1. Relative import (normal package / local-dev context).
    try:
        from .menu_url_discovery import discover_menu_urls
        from .menu_url_utils import normalize_menu_url
        return discover_menu_urls, normalize_menu_url
    except ImportError:
        pass

    # 2. Ensure DAGs root is on sys.path, retry absolute import, then load files directly.
    import importlib.util
    import types

    mod_dir = os.path.dirname(os.path.abspath(__file__))
    dags_root = os.path.abspath(os.path.join(mod_dir, ".."))
    if dags_root not in sys.path:
        sys.path.insert(0, dags_root)

    try:
        from modules.menu_url_discovery import discover_menu_urls
        from modules.menu_url_utils import normalize_menu_url
        return discover_menu_urls, normalize_menu_url
    except ImportError:
        pass

    # Last resort: register a stub ``modules`` package and exec sibling files directly.
    if "modules" not in sys.modules:
        pkg = types.ModuleType("modules")
        pkg.__path__ = [mod_dir]  # type: ignore[attr-defined]
        sys.modules["modules"] = pkg

    def _load_sibling(fullname: str, filename: str):
        path = os.path.join(mod_dir, filename)
        if not os.path.isfile(path):
            raise ModuleNotFoundError(
                f"Missing {filename} next to gbq_menu_url_extractor.py ({path}). "
                "Sync menu_url_discovery.py and menu_url_utils.py into the same GCS modules/ folder."
            )
        spec = importlib.util.spec_from_file_location(fullname, path)
        if spec is None or spec.loader is None:
            raise ImportError(f"Cannot create module spec for {fullname!r}")
        mod = importlib.util.module_from_spec(spec)
        sys.modules[fullname] = mod
        spec.loader.exec_module(mod)
        return mod

    _load_sibling("modules.menu_url_utils", "menu_url_utils.py")
    disc = _load_sibling("modules.menu_url_discovery", "menu_url_discovery.py")
    return disc.discover_menu_urls, sys.modules["modules.menu_url_utils"].normalize_menu_url


discover_menu_urls, normalize_menu_url = _import_discover_and_normalize()

logger = logging.getLogger(__name__)

__all__ = [
    "get_bigquery_client",
    "load_config",
    "main_airflow",
    "normalize_menu_url",
    "run_extraction",
    "run_extraction_partition",
]

# Default input (override with Airflow Variable MENU_URL_SOURCE_TABLE or env)
DEFAULT_MENU_URL_SOURCE_TABLE = (
    #"dwh_project.dwh_discovery.data_for_seo_20260128"
    #"source_project.dwh_de_test.data_for_seo_dump" #dump of above table
    "source_project.dwh_de_test.data_for_seo_sample"
    #"dwh_project.dwh_de.refined_dataforseo_business_listing"
)
# Column on the source table that holds page URLs to crawl
DEFAULT_MENU_URL_COLUMN = "url"

# Menu extract output always defaults to this project (not tied to GCP_PROJECT)
DEFAULT_MENU_URL_DEST_TABLE = "source_project.dwh_de_test.menu_url_extracted_ch1test"

# Distinct URLs per source read chunk; the job loops until a chunk is empty (MENU_URL_BATCH_LIMIT).
DEFAULT_MENU_URL_BATCH_LIMIT = 20000  # production
# DEFAULT_MENU_URL_BATCH_LIMIT = 20  


def get_bigquery_client(project_id: str, gcp_conn_id: str) -> Tuple[bigquery.Client, str]:
    """BigQuery client via Airflow BigQueryHook; ADC fallback if hook unavailable."""
    pid = (project_id or "").strip()    
    try:
        from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

        hook = BigQueryHook(gcp_conn_id=gcp_conn_id)
        resolved = pid or (hook.project_id or "")
        if not resolved:
            raise ValueError("Could not resolve GCP project from arguments or hook.")
        client = hook.get_client(project_id=resolved)
        return client, resolved
    except Exception as e:
        logger.warning(
            "BigQueryHook unavailable (%s); using Application Default Credentials.",
            e,
        )
        if not pid:
            pid = "source_project"
        return bigquery.Client(project=pid), pid


# Heuristics: extend or tune for your domains
MENU_HREF_SUBSTRINGS = (
    # generic menu / ordering terms
    "menu",
    "speisekarte",
    "menue",
    "menükarte",
    "karte",
    "carte",
    "/food",
    "order-online",
    "bestellen",
    "takeaway",
    "delivery",
    "lieferung",
    # # keywords from DataForSEO Food & Drink categories
    # "diner",
    # "buffet",
    # "pizza",
    # "sushi",
    # "burger",
    # "barbecue",
    # "bbq",
    # "seafood",
    # "steak",
    # "grill",
    # "coffee",
    # "brunch",
    # "tapas",
    # "kebab",
    # "noodle",
    # "sandwich",
    # "dessert",
    # "wine",
    # "cocktail",
    # "vegan",
    # "vegetarian",
)
MENU_TEXT_SUBSTRINGS = (
    # generic menu / ordering terms
    "menu",
    "speisekarte",
    "menü",
    "menue",
    "karte",
    "food",
    "order",
    "bestellen",
    #keywords from DataForSEO Food & Drink categories
    # "pizza",
    # "sushi",
    # "burger",
    # "grill",
    # "coffee",
    # "brunch",
    # "tapas",
    # "kebab",
    # "sandwich",
    # "dessert",
    # "wine",
    # "vegan",
    # "vegetarian",
)


@dataclass(frozen=True)
class ExtractorConfig:
    project_id: str
    source_table: str
    dest_table: str
    url_column: str = DEFAULT_MENU_URL_COLUMN
    batch_limit: int = DEFAULT_MENU_URL_BATCH_LIMIT  # chunk size for each BigQuery distinct-url read
    request_timeout_sec: float = 10.0
    request_delay_sec: float = 0.3
    gcp_conn_id: str = "google_cloud_default"
    http_user_agent: str = (
        "Mozilla/5.0 (compatible; DishMenuUrlBot/1.0; +https://dish.digital)"
    )
    # http = requests only; playwright = Chromium render (then same discovery on final DOM)
    fetch_mode: str = "http"
    # requests "proxies" mapping when set (see _build_request_proxies / MENU_URL_PROXY*)
    request_proxies: Optional[Dict[str, str]] = None
    # If set, second GET after 403 uses this URL for both schemes (direct fetch first).
    proxy_on_403_retry: Optional[str] = None


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
    env_val = os.environ.get(name, "").strip()
    if env_val:
        try:
            return int(env_val)
        except ValueError:
            pass
    try:
        from airflow.models import Variable

        raw = str(Variable.get(name, default_var=str(default))).strip()
        return int(raw)
    except Exception:
        return default


def _setting_float(name: str, default: float) -> float:
    env_val = os.environ.get(name, "").strip()
    if env_val:
        try:
            return float(env_val)
        except ValueError:
            pass
    try:
        from airflow.models import Variable

        raw = str(Variable.get(name, default_var=str(default))).strip()
        return float(raw)
    except Exception:
        return default


def _optional_nonempty_str(name: str) -> str:
    """Read env or Airflow Variable; return empty string if unset."""
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
    """
    Build optional ``requests`` ``proxies=`` dict.

    - ``MENU_URL_PROXY``: one URL for both ``http`` and ``https`` schemes.
    - Or ``MENU_URL_PROXY_HTTP`` / ``MENU_URL_PROXY_HTTPS`` separately.
    """
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
    """Normalize ``project.dataset.table`` for equality checks (no backticks, lower case)."""
    return table_id.replace("`", "").strip().lower().replace(":", ".")


def _reject_if_dest_is_source(source_table: str, dest_table: str) -> None:
    """Prevent any write path from targeting the read-only input table."""
    if _normalize_bq_table_id(source_table) == _normalize_bq_table_id(dest_table):
        raise ValueError(
            "MENU_URL_DEST_TABLE cannot equal MENU_URL_SOURCE_TABLE; "
            "the input table must stay read-only."
        )


def _parse_bq_table_triplet(full_table_id: str) -> Tuple[str, str, str]:
    raw = full_table_id.replace("`", "").strip()
    parts = raw.split(".")
    if len(parts) != 3:
        raise ValueError(
            "MENU_URL_DEST_TABLE must be project.dataset.table "
            f"(got {full_table_id!r})"
        )
    return parts[0], parts[1], parts[2]


def new_menu_url_id(src_url: str, menu_url: str) -> int:
    """
    Deterministic INT64 key derived from (src_url, menu_url).
    Uses first 6 bytes of SHA-256 → values ≤ 281_474_976_710_655 (15 digits).
    Parallel-safe and idempotent: same URL pair always produces the same ID.
    """
    digest = hashlib.sha256(f"{src_url}\x00{menu_url}".encode()).digest()
    return int.from_bytes(digest[:6], "big") or 1  # or 1 guards the theoretical all-zero case


def _attach_menu_url_id_primary_key(table: bigquery.Table) -> None:
    """Declare PRIMARY KEY(menu_url_id) when the client library supports it (not enforced in BQ)."""
    try:
        from google.cloud.bigquery.table import PrimaryKey, TableConstraints

        table.table_constraints = TableConstraints(
            primary_key=PrimaryKey(["menu_url_id"])
        )
    except Exception as e:  # pragma: no cover - older google-cloud-bigquery
        logger.debug("Skipping PRIMARY KEY metadata (unsupported or error): %s", e)


# Created automatically when missing; must match columns appended via pandas_gbq
MENU_URL_DEST_SCHEMA = (
    bigquery.SchemaField("menu_url_id",    "INTEGER",   mode="REQUIRED"),
    bigquery.SchemaField("src_title",      "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("src_place_id",   "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("src_country",    "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("src_first_seen", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("src_est_url",    "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("fetched_menu_url", "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("menu_url_fetch_status", "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("menus_on_page",  "INTEGER",   mode="NULLABLE"),
    bigquery.SchemaField("playwright_used","BOOL",      mode="NULLABLE"),
    bigquery.SchemaField("extracted_at",   "TIMESTAMP", mode="NULLABLE"),
)


def ensure_destination_table_exists(
    client: bigquery.Client, dest_table_full_id: str
) -> None:
    """
    Create destination dataset and empty table if they do not exist.
    Later loads always use append (see ``pandas_gbq.to_gbq(..., if_exists='append')``).
    """
    project_id, dataset_id, table_id = _parse_bq_table_triplet(dest_table_full_id)
    raw = dest_table_full_id.replace("`", "").strip()
    ds_ref = bigquery.DatasetReference(project_id, dataset_id)
    table_ref = ds_ref.table(table_id)
    try:
        table = client.get_table(table_ref)
        # Patch schema for columns added or renamed after table creation.
        existing_names = {f.name for f in table.schema}
        extra_fields = []
        if "playwright_used" not in existing_names:
            extra_fields.append(bigquery.SchemaField("playwright_used", "BOOL", mode="NULLABLE"))
        if "fetched_menu_url" not in existing_names:
            extra_fields.append(bigquery.SchemaField("fetched_menu_url", "STRING", mode="NULLABLE"))
        if "src_country" not in existing_names:
            extra_fields.append(bigquery.SchemaField("src_country", "STRING", mode="NULLABLE"))
        if extra_fields:
            table.schema = list(table.schema) + extra_fields
            client.update_table(table, ["schema"])
            logger.info(
                "Added column(s) %s to existing table: %s",
                [f.name for f in extra_fields], raw,
            )
        logger.debug("Destination table exists: %s", raw)
        return
    except NotFound:
        logger.info("Destination table not found, creating: %s", raw)
    try:
        client.get_dataset(ds_ref)
    except NotFound:
        client.create_dataset(bigquery.Dataset(ds_ref), exists_ok=True)
        logger.info("Created dataset %s.%s", project_id, dataset_id)
    table = bigquery.Table(table_ref, schema=MENU_URL_DEST_SCHEMA)
    _attach_menu_url_id_primary_key(table)
    client.create_table(table, exists_ok=True)
    logger.info("Ensured destination table exists: %s", raw)


def load_config() -> ExtractorConfig:
    """
    ``GCP_PROJECT`` / ``GCP_CONN_ID``: hook client defaults (often ``source_project``).
    ``MENU_URL_DEST_TABLE`` defaults to ``DEFAULT_MENU_URL_DEST_TABLE`` (always ``source_project``).

    Optional HTTP proxies: ``MENU_URL_PROXY`` or ``MENU_URL_PROXY_HTTP`` / ``MENU_URL_PROXY_HTTPS``;
    ``MENU_URL_PROXY_ON_403_RETRY`` for a proxy used only on the second GET after 403.
    """
    project_id = _setting_str("GCP_PROJECT", "source_project")
    gcp_conn_id = _setting_str("GCP_CONN_ID", "google_cloud_default")
    source = _setting_str("MENU_URL_SOURCE_TABLE", DEFAULT_MENU_URL_SOURCE_TABLE)
    dest = _setting_str("MENU_URL_DEST_TABLE", DEFAULT_MENU_URL_DEST_TABLE)
    if not source or not dest:
        raise ValueError(
            "Set MENU_URL_SOURCE_TABLE and MENU_URL_DEST_TABLE "
            "(Airflow Variables or environment; full project.dataset.table ids)."
        )
    url_col = _setting_str("MENU_URL_COLUMN", DEFAULT_MENU_URL_COLUMN) or DEFAULT_MENU_URL_COLUMN
    _reject_if_dest_is_source(source, dest)
    return ExtractorConfig(
        project_id=project_id,
        source_table=source,
        dest_table=dest,
        url_column=url_col,
        batch_limit=_setting_int(
            "MENU_URL_BATCH_LIMIT", DEFAULT_MENU_URL_BATCH_LIMIT
        ),
        request_timeout_sec=_setting_float("MENU_URL_REQUEST_TIMEOUT_SEC", 10.0),
        request_delay_sec=_setting_float("MENU_URL_REQUEST_DELAY_SEC", 0.3),
        gcp_conn_id=gcp_conn_id,
        fetch_mode=_setting_str("MENU_URL_FETCH_MODE", "http").strip().lower() or "http",
        request_proxies=_build_request_proxies(),
        proxy_on_403_retry=(
            _optional_nonempty_str("MENU_URL_PROXY_ON_403_RETRY") or None
        ),
    )


def fetch_source_url_batch(
    client: bigquery.Client, cfg: ExtractorConfig, after_url: str
) -> list[str]:
    """
    Next chunk of distinct URLs from the source (read-only ``SELECT``). Ordered by URL;
    ``after_url`` is exclusive—use ``""`` for the first chunk. At most ``cfg.batch_limit`` rows;
    empty list means the distinct URL set is exhausted.
    """
    col = cfg.url_column.replace("`", "")
    lim = int(cfg.batch_limit)
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("after_url", "STRING", after_url),
        ],
        labels={"gbq_menu_extractor": "source_select_only"},
    )
    q = f"""
    WITH distinct_urls AS (
      SELECT DISTINCT TRIM(CAST(`{col}` AS STRING)) AS url
      FROM `{cfg.source_table}`
      WHERE `{col}` IS NOT NULL
        AND TRIM(CAST(`{col}` AS STRING)) != ''
    )
    SELECT url
    FROM distinct_urls
    WHERE url > @after_url
    ORDER BY url
    LIMIT {lim}
    """
    rows = list(client.query(q, job_config=job_config).result())
    return [r["url"].strip() for r in rows if r.get("url")]


def fetch_urls_ntile_partition(
    client: bigquery.Client,
    cfg: ExtractorConfig,
    batch_index: int,
    num_batches: int,
) -> list[dict]:
    """
    Distinct URLs assigned to ``batch_index`` by ``NTILE(num_batches) OVER (ORDER BY url)``.
    ``batch_index`` is 1-based in ``[1, num_batches]``.
    Returns list of dicts with keys: url, src_place_id, src_title, src_country, src_first_seen.
    """
    if batch_index < 1 or batch_index > num_batches or num_batches < 1:
        raise ValueError(
            f"Invalid partition batch_index={batch_index!r} num_batches={num_batches!r}"
        )
    col = cfg.url_column.replace("`", "")
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("batch_index", "INT64", int(batch_index)),
            bigquery.ScalarQueryParameter("num_batches", "INT64", int(num_batches)),
        ],
        labels={"gbq_menu_extractor": "source_select_partition"},
    )
    q = f"""
    WITH distinct_urls AS (
      SELECT
        TRIM(CAST(`{col}` AS STRING))            AS url,
        CAST(place_id    AS STRING)              AS place_id,
        CAST(title       AS STRING)              AS title,
        CAST(country     AS STRING)              AS src_country,
        CAST(first_seen  AS TIMESTAMP)           AS first_seen
      FROM `{cfg.source_table}`
      WHERE `{col}` IS NOT NULL
        AND TRIM(CAST(`{col}` AS STRING)) != ''
      LIMIT 100
    ),
    bucketed AS (
      SELECT
        url, place_id, title, src_country, first_seen,
        NTILE(@num_batches) OVER (ORDER BY url) AS batch_id
      FROM distinct_urls
    )
    SELECT url, place_id, title, src_country, first_seen
    FROM bucketed
    WHERE batch_id = @batch_index
    ORDER BY url
    """
    rows = list(client.query(q, job_config=job_config).result())
    return [
        {
            "url":        r["url"].strip(),
            "place_id":   r["place_id"],
            "title":      r["title"],
            "src_country": r.get("src_country"),
            "first_seen": r["first_seen"],
        }
        for r in rows if r.get("url")
    ]


def run_extraction_partition(
    batch_index: int,
    num_batches: int,
    cfg: Optional[ExtractorConfig] = None,
) -> int:
    """
    Process one NTILE partition of distinct source URLs (for a mapped Airflow task).
    Returns the number of new rows appended for this partition.
    """
    cfg = cfg or load_config()
    db_conn = initialize_db_connection()
    try:
        logger.info(
            "db_connections.initialize_db_connection OK (partition %s/%s)",
            batch_index,
            num_batches,
        )
    finally:
        db_conn.close()

    client, _ = get_bigquery_client(cfg.project_id, cfg.gcp_conn_id)

    try:
        import pandas_gbq
    except ImportError as e:  # pragma: no cover
        raise RuntimeError("pandas_gbq is required to load into BigQuery.") from e

    _reject_if_dest_is_source(cfg.source_table, cfg.dest_table)
    ensure_destination_table_exists(client, cfg.dest_table)
    dest_project, _, _ = _parse_bq_table_triplet(cfg.dest_table)

    # Persistent HTTP session: reuses TCP connections across all URL fetches in
    # this partition, reducing TLS handshake overhead for repeated domains.
    from requests.adapters import HTTPAdapter
    http_session = requests.Session()
    http_session.headers.update({"User-Agent": cfg.http_user_agent})
    _adapter = HTTPAdapter(
        pool_connections=10,   # number of distinct host connection pools
        pool_maxsize=20,       # max open connections per pool
        max_retries=0,         # per-URL GET retries are in http_get / _request_get_with_retry
    )
    http_session.mount("https://", _adapter)
    http_session.mount("http://",  _adapter)

    try:
        sources = fetch_urls_ntile_partition(
            client, cfg, int(batch_index), int(num_batches)
        )
        if not sources:
            logger.info(
                "Partition %s/%s: no source URLs", int(batch_index), int(num_batches)
            )
            return 0

        logger.info(
            "Partition %s/%s: %s distinct URLs",
            int(batch_index),
            int(num_batches),
            len(sources),
        )

        existing = existing_menu_pairs(client, cfg.dest_table, [s["url"] for s in sources])

        BQ_MINI_BATCH_SIZE = 500  # flush to BQ every N rows to preserve progress on timeout
        rows: list[dict] = []
        total_appended = 0

        def _flush(buffer: list[dict]) -> int:
            """Write buffer to BQ and return number of rows written."""
            if not buffer:
                return 0
            df = pd.DataFrame(buffer)
            df["extracted_at"] = pd.Timestamp.utcnow()
            pandas_gbq.to_gbq(
                df,
                cfg.dest_table,
                project_id=dest_project,
                if_exists="append",
                progress_bar=False,
            )
            logger.info(
                "Partition %s/%s: flushed %s rows to %s (running total: %s)",
                int(batch_index),
                int(num_batches),
                len(df),
                cfg.dest_table,
                total_appended + len(df),
            )
            return len(df)

        for src_rec in sources:
            src        = src_rec["url"]
            place_id   = src_rec.get("place_id")
            title      = src_rec.get("title")
            src_country = src_rec.get("src_country")
            first_seen = src_rec.get("first_seen")

            time.sleep(cfg.request_delay_sec)
            html, status, page_base, playwright_used = fetch_html(src, cfg, session=http_session)
            if html is None:
                logger.warning("Skip %s — %s", src, status)
                continue

            menus = discover_menu_urls(
                html, page_base, MENU_HREF_SUBSTRINGS, MENU_TEXT_SUBSTRINGS
            )
            if not menus:
                logger.info("No menu-like links for %s", src)
                continue

            for m in menus:
                if (src, m) in existing:
                    continue
                is_reachable, url_status = validate_url(m, cfg, session=http_session, src_url=src)
                if not is_reachable:
                    logger.warning(
                        "Menu URL unreachable [%s] %s (source: %s) — skipped",
                        url_status, m, src,
                    )
                    continue
                rows.append(
                    {
                        "menu_url_id":           new_menu_url_id(src, m),
                        "src_est_url":           src,
                        "fetched_menu_url":       m,
                        "menu_url_fetch_status":  url_status,
                        "menus_on_page":          len(menus),
                        "src_place_id":           place_id,
                        "src_title":              title,
                        "src_country":            src_country,
                        "src_first_seen":         first_seen,
                        "playwright_used":        playwright_used,
                    }
                )

            # flush mini-batch when threshold reached
            if len(rows) >= BQ_MINI_BATCH_SIZE:
                total_appended += _flush(rows)
                rows = []

        # flush any remaining rows after loop
        total_appended += _flush(rows)

        if total_appended == 0:
            logger.info(
                "Partition %s/%s: no new rows to write", int(batch_index), int(num_batches)
            )
        else:
            logger.info(
                "Partition %s/%s: finished — total %s rows appended to %s",
                int(batch_index),
                int(num_batches),
                total_appended,
                cfg.dest_table,
            )
        return total_appended

    finally:
        http_session.close()
        logger.info("Partition %s/%s: HTTP session closed", int(batch_index), int(num_batches))


def existing_menu_pairs(
    client: bigquery.Client, dest_table: str, source_urls: Iterable[str]
) -> Set[Tuple[str, str]]:
    """Read-only ``SELECT`` on the destination table only (not the input table)."""
    if not source_urls:
        return set()
    urls = list(dict.fromkeys(source_urls))
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("src_urls", "STRING", urls),
        ]
    )
    q = f"""
    SELECT src_est_url, fetched_menu_url
    FROM `{dest_table}`
    WHERE src_est_url IN UNNEST(@src_urls)
    """
    try:
        result = client.query(q, job_config=job_config).result()
    except Exception as e:
        logger.info("Destination read failed (table may not exist yet): %s", e)
        return set()
    return {(r["src_est_url"], r["fetched_menu_url"]) for r in result}


# Third-party domains whose menu-URL validation is always skipped (bot-blocked, never actual menus).
#"tripadvisor.com", "tripadvisor.fr", "tripadvisor.de", "tripadvisor.es", "tripadvisor.it",
_SKIP_VALIDATION_HOSTS = frozenset({
    "instagram.com", "twitter.com", "x.com",
    "facebook.com", "fb.com",
    "youtube.com",
})

def _is_third_party_host(url: str) -> bool:
    """Return True when the URL host is a known third-party domain to skip validation."""
    try:
        host = urllib.parse.urlparse(url).netloc.lower().lstrip("www.")
        return any(host == h or host.endswith("." + h) for h in _SKIP_VALIDATION_HOSTS)
    except Exception:
        return False


def _same_host(url_a: str, url_b: str) -> bool:
    """Return True when both URLs share the same hostname (ignoring www. prefix)."""
    try:
        host_a = urllib.parse.urlparse(url_a).netloc.lower().lstrip("www.")
        host_b = urllib.parse.urlparse(url_b).netloc.lower().lstrip("www.")
        return bool(host_a) and host_a == host_b
    except Exception:
        return False


# Up to two GET attempts: one retry after transport failure, transient 5xx, or 403.
_HTTP_GET_MAX_ATTEMPTS = 2
_HTTP_RETRY_5XX = frozenset((502, 503, 504))
# Pauses before the second GET (initial + one retry only).
_HTTP_RETRY_TRANSPORT_BACKOFF_SEC = 3.0   # RequestException (timeout, connection, etc.)
_HTTP_RETRY_5XX_BACKOFF_SEC = 5.0         # 502 / 503 / 504
# One optional plain-HTTP retry for 403 (transient WAF / rate quirks) before giving up to caller / Playwright.
_HTTP_RETRY_403_BACKOFF_SEC = 1.5


def _request_get_with_retry(
    requester,
    url: str,
    cfg: ExtractorConfig,
):
    """
    Perform GET with up to ``_HTTP_GET_MAX_ATTEMPTS`` tries.

    Retries once after ``requests`` transport errors (``_HTTP_RETRY_TRANSPORT_BACKOFF_SEC``);
    when the response is 502/503/504 (``_HTTP_RETRY_5XX_BACKOFF_SEC``); or when the response
    is **403** (``_HTTP_RETRY_403_BACKOFF_SEC``). If ``cfg.proxy_on_403_retry`` is set, the
    second attempt after **403** uses that proxy (first attempt uses ``cfg.request_proxies`` only).
    """
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
                timeout=cfg.request_timeout_sec,
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
    """Returns (html_or_none, status, final_url_for_relative_links)."""
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


def _fetch_via_playwright(url: str, cfg: ExtractorConfig) -> Tuple[Optional[str], str, str, bool]:
    """Headless Chromium for JS-heavy sites; falls back to HTTP on failure or missing package.
    Returns (html, status, final_url, playwright_used)."""
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        logger.warning("playwright not installed; using HTTP GET")
        html, status, final = http_get(url, cfg)
        return html, status, final, False
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            ctx_kw: dict = {"user_agent": cfg.http_user_agent}
            if cfg.request_proxies:
                _srv = cfg.request_proxies.get("https") or cfg.request_proxies.get(
                    "http"
                )
                if _srv:
                    ctx_kw["proxy"] = {"server": _srv}
            context = browser.new_context(**ctx_kw)
            page = context.new_page()
            page.goto(
                url,
                wait_until="domcontentloaded",
                timeout=int(cfg.request_timeout_sec * 1000),
            )
            page.wait_for_timeout(2000)
            html = page.content()
            final = page.url
            context.close()
            browser.close()
        return html, "ok", final, True
    except Exception as e:
        logger.warning("Playwright failed (%s); HTTP fallback", e)
        html, status, final = http_get(url, cfg)
        return html, status, final, False


def fetch_html(
    url: str,
    cfg: ExtractorConfig,
    session: Optional[requests.Session] = None,
) -> Tuple[Optional[str], str, str, bool]:
    """
    Fetch page HTML. Returns (html, status, final_url, playwright_used).
    - fetch_mode=playwright : always use Playwright (headless Chromium).
    - fetch_mode=http (default): GET uses up to two attempts for transport
      errors, transient 5xx, and **403** (backoff before the second GET). If the
      result is still a known bot-detection signal (403, 429, 520, 503,
      ReadTimeout, ConnectionError), retry with Playwright so bot-protected
      sites are still crawlable without slowing down normal sites.
    """
    mode = (cfg.fetch_mode or "http").lower()
    if mode == "playwright":
        return _fetch_via_playwright(url, cfg)

    html, status, final = http_get(url, cfg, session=session)

    # Playwright fallback for bot-detection / timeout signals only
    _playwright_triggers = (
        "http_403", "http_429", "http_520", "http_503",
        "request_error:ReadTimeout", "request_error:ConnectionError",
    )
    if html is None and status in _playwright_triggers:
        logger.info(
            "HTTP %s for %s — retrying with Playwright", status, url
        )
        return _fetch_via_playwright(url, cfg)

    return html, status, final, False


def validate_url(
    url: str,
    cfg: ExtractorConfig,
    session: Optional[requests.Session] = None,
    src_url: str = "",
) -> Tuple[bool, str]:
    """
    Verify a discovered menu URL is reachable before writing it to BigQuery.

    Uses HTTP HEAD (faster, no body download). If the server returns 405 (Method
    Not Allowed) or 501 (Not Implemented) for HEAD, falls back to a GET request.
    Returns (is_valid, status_str):
      - is_valid: True when the final response status is < 400
      - status_str: human-readable code+reason, e.g. "200-OK", "404-Not Found"
        or an error token e.g. "request_error:ConnectTimeout"

    403 on the restaurant's own domain is treated as reachable: the page exists
    but requires a browser — Playwright will handle it later. Third-party 403s
    (TripAdvisor, Instagram, etc.) are still skipped via ``_is_third_party_host``.
    """
    from http import HTTPStatus

    def _status_label(code: int) -> str:
        try:
            reason = HTTPStatus(code).phrase
        except ValueError:
            reason = "Unknown"
        return f"{code}-{reason}"

    if _is_third_party_host(url):
        logger.debug("Skipping validation for third-party URL: %s", url)
        return False, "skipped:third_party"

    try:
        parsed = urllib.parse.urlparse(url)
        if parsed.scheme not in ("http", "https") or not parsed.netloc:
            logger.warning("Skipping invalid URL (bad scheme/host): %s", url)
            return False, "invalid_url:bad_scheme_or_host"
    except Exception:
        return False, "invalid_url:parse_error"

    requester = session or requests
    try:
        r = requester.head(
            url,
            timeout=cfg.request_timeout_sec,
            headers={"User-Agent": cfg.http_user_agent},
            allow_redirects=True,
            proxies=cfg.request_proxies,
        )
        if r.status_code in (405, 501):
            r = _request_get_with_retry(requester, url, cfg)
        # 403 on the restaurant's own domain: page exists but blocks bots.
        # Keep it — Playwright fallback will render it when the time comes.
        if r.status_code == 403 and src_url and _same_host(url, src_url):
            logger.info("403 on own domain — keeping menu URL: %s (source: %s)", url, src_url)
            return True, _status_label(r.status_code)
        return r.status_code < 400, _status_label(r.status_code)
    except requests.RequestException as e:
        return False, f"request_error:{type(e).__name__}"
    except Exception as e:
        logger.warning("Unexpected error validating URL %s: %s", url, e)
        return False, f"request_error:{type(e).__name__}"


def run_extraction(cfg: Optional[ExtractorConfig] = None) -> int:
    """
    Returns total new rows appended. Processes every distinct source URL in chunks of
    ``cfg.batch_limit`` (default 500), appending each non-empty result batch to the destination.
    """
    cfg = cfg or load_config()
    # Same as extract_link_by_id.LinkExtractor: initialize_db_connection() with default env
    db_conn = initialize_db_connection()
    try:
        logger.info("db_connections.initialize_db_connection OK")
    finally:
        db_conn.close()

    client, _ = get_bigquery_client(cfg.project_id, cfg.gcp_conn_id)

    try:
        import pandas_gbq
    except ImportError as e:  # pragma: no cover
        raise RuntimeError("pandas_gbq is required to load into BigQuery.") from e

    _reject_if_dest_is_source(cfg.source_table, cfg.dest_table)
    ensure_destination_table_exists(client, cfg.dest_table)
    dest_project, _, _ = _parse_bq_table_triplet(cfg.dest_table)

    total_appended = 0
    after = ""
    batch_num = 0

    while True:
        sources = fetch_source_url_batch(client, cfg, after)
        if not sources:
            if batch_num == 0:
                logger.info("No source URLs to process.")
            break

        batch_num += 1
        logger.info(
            "Source URL batch %s: %s distinct URLs (chunk size %s)",
            batch_num,
            len(sources),
            cfg.batch_limit,
        )

        existing = existing_menu_pairs(client, cfg.dest_table, sources)
        rows: list[dict] = []

        for src in sources:
            time.sleep(cfg.request_delay_sec)
            html, status, page_base, playwright_used = fetch_html(src, cfg)
            if html is None:
                logger.warning("Skip %s — %s", src, status)
                continue

            menus = discover_menu_urls(
                html, page_base, MENU_HREF_SUBSTRINGS, MENU_TEXT_SUBSTRINGS
            )
            if not menus:
                logger.info("No menu-like links for %s", src)
                continue

            for m in menus:
                if (src, m) in existing:
                    continue
                is_reachable, url_status = validate_url(m, cfg, src_url=src)
                if not is_reachable:
                    logger.warning(
                        "Menu URL unreachable [%s] %s (source: %s) — skipped",
                        url_status, m, src,
                    )
                    continue
                rows.append(
                    {
                        "menu_url_id":           new_menu_url_id(src, m),
                        "src_est_url":           src,
                        "fetched_menu_url":       m,
                        "menus_on_page":          len(menus),
                        "playwright_used":        playwright_used,
                        "menu_url_fetch_status":  url_status,
                    }
                )

        if rows:
            df = pd.DataFrame(rows)
            df["extracted_at"] = pd.Timestamp.utcnow()
            pandas_gbq.to_gbq(
                df,
                cfg.dest_table,
                project_id=dest_project,
                if_exists="append",
                progress_bar=False,
            )
            total_appended += len(df)
            logger.info(
                "Batch %s: appended %s rows to %s (run total %s)",
                batch_num,
                len(df),
                cfg.dest_table,
                total_appended,
            )

        after = sources[-1]

    if total_appended == 0 and batch_num > 0:
        logger.info("No new rows to write across all batches.")

    return total_appended


def main_airflow(**context) -> None:
    """Entry point for Airflow PythonOperator: python_callable=main_airflow"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    n = run_extraction()
    logger.info("Menu URL extraction finished, new rows: %s", n)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    run_extraction()
