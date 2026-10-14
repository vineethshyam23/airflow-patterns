"""
classify_menu_url_types.py
==========================
Classify effective_url types for the menu extraction control table.

Extension fast-path first; HTTP HEAD Content-Type for ambiguous URLs.
Updates effective_url_type and menu_fetch_status on control_lnd.
"""

from __future__ import annotations

import logging
import uuid

import requests
import urllib3
from google.cloud import bigquery

logger = logging.getLogger(__name__)

# Default python-requests agent gets blocked or timed out by many restaurant sites.
# Shared with the HTML fetch task.
BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )
}
_MAX_ATTEMPTS = 3


def status_for_url_type(url_type: str) -> str:
    if url_type == "html":
        return "classified"
    if url_type == "unreachable":
        # Transient (DNS, timeout, reset) — retried by the next run until _MAX_ATTEMPTS.
        return "classify_failed"
    return "skipped_non_html"


def classify_type_from_extension(url: str):
    """Return url type from path extension, or None if ambiguous."""
    if not url:
        return None
    path = url.split("?")[0].split("#")[0].lower()
    if path.endswith(".pdf"):
        return "pdf"
    if path.endswith((".jpg", ".jpeg")):
        return "jpeg"
    if path.endswith(".png"):
        return "png"
    if path.endswith((".html", ".htm")):
        return "html"
    return None


def classify_type_from_content_type(content_type: str) -> str:
    ct = (content_type or "").split(";")[0].strip().lower()
    if "html" in ct:
        return "html"
    if ct == "application/pdf":
        return "pdf"
    if ct in ("image/jpeg", "image/jpg"):
        return "jpeg"
    if ct == "image/png":
        return "png"
    if not ct:
        # Server sent no Content-Type; extension-less URLs are webpages in practice.
        return "html"
    return "other"


def _request(url: str, verify: bool = True):
    """HEAD the URL, falling back to GET when HEAD is refused or returns no type."""
    r = requests.head(
        url, timeout=15, allow_redirects=True, headers=BROWSER_HEADERS, verify=verify
    )
    if r.status_code in (405, 501) or not r.headers.get("Content-Type"):
        r = requests.get(
            url,
            timeout=15,
            stream=True,
            allow_redirects=True,
            headers=BROWSER_HEADERS,
            verify=verify,
        )
        r.close()
    return r


def head_classify(url: str) -> str:
    """Content-Type classification; 'unreachable' when the site cannot be contacted."""
    try:
        try:
            r = _request(url)
        except requests.exceptions.SSLError:
            # Expired/self-signed/mismatched certs are common on small sites.
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            r = _request(url, verify=False)
        if r.status_code >= 400:
            return "unknown"
        return classify_type_from_content_type(r.headers.get("Content-Type", ""))
    except Exception as exc:
        logger.warning("HEAD classify failed for %s: %s", url, exc)
        return "unreachable"


def _merge_head_results(
    client: bigquery.Client,
    control_table: str,
    batch: list,
) -> None:
    """Load HEAD results into a temp table, then MERGE into control_lnd."""
    if not batch:
        return

    project, dataset, _ = control_table.split(".")
    tmp_table = f"{project}.{dataset}._tmp_menu_url_classify_{uuid.uuid4().hex[:12]}"
    load_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("menu_url_id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("effective_url_type", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("menu_fetch_status", "STRING", mode="REQUIRED"),
        ],
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    try:
        client.load_table_from_json(batch, tmp_table, job_config=load_config).result()
        merge_sql = f"""
        MERGE `{control_table}` AS T
        USING `{tmp_table}` AS S
        ON T.menu_url_id = S.menu_url_id
        WHEN MATCHED THEN UPDATE SET
          T.effective_url_type = S.effective_url_type,
          T.menu_fetch_status  = S.menu_fetch_status,
          -- attempt_count is per stage: reset once the row moves on to the next one.
          T.attempt_count      = IF(
            S.menu_fetch_status = 'classify_failed',
            COALESCE(T.attempt_count, 0) + 1,
            0
          ),
          T._update_ts         = CURRENT_TIMESTAMP()
        """
        client.query(merge_sql).result()
    finally:
        client.delete_table(tmp_table, not_found_ok=True)


def classify_urls_for_country(
    client: bigquery.Client,
    control_table: str,
    country_name: str,
) -> None:
    """Classify effective_url → effective_url_type + menu_fetch_status for one country."""
    # Only rows with a clear file extension are handled here; extension-less URLs are
    # left untouched so the HEAD pass below can classify them instead of writing NULL.
    extension_sql = f"""
    UPDATE `{control_table}` AS T
    SET
      effective_url_type = (
        CASE
          WHEN REGEXP_CONTAINS(LOWER(T.effective_url), r'\\.pdf($|[?#])') THEN 'pdf'
          WHEN REGEXP_CONTAINS(LOWER(T.effective_url), r'\\.(jpe?g)($|[?#])') THEN 'jpeg'
          WHEN REGEXP_CONTAINS(LOWER(T.effective_url), r'\\.png($|[?#])') THEN 'png'
          WHEN REGEXP_CONTAINS(LOWER(T.effective_url), r'\\.html?($|[?#])') THEN 'html'
        END
      ),
      menu_fetch_status = (
        CASE
          WHEN REGEXP_CONTAINS(LOWER(T.effective_url), r'\\.html?($|[?#])') THEN 'classified'
          ELSE 'skipped_non_html'
        END
      ),
      _update_ts = CURRENT_TIMESTAMP()
    WHERE T.country = @country
      AND T.effective_url IS NOT NULL
      AND (T.menu_fetch_status = 'pending' OR T.menu_fetch_status IS NULL)
      AND REGEXP_CONTAINS(LOWER(T.effective_url), r'\\.(pdf|jpe?g|png|html?)($|[?#])')
    """
    # Picks up never-classified rows plus earlier unreachable ones, until attempts run out.
    ambiguous_sql = f"""
    SELECT menu_url_id, effective_url
    FROM `{control_table}`
    WHERE country = @country
      AND effective_url IS NOT NULL
      AND (menu_fetch_status IN ('pending', 'classify_failed') OR menu_fetch_status IS NULL)
      AND (effective_url_type IS NULL OR effective_url_type IN ('', 'unreachable'))
      AND COALESCE(attempt_count, 0) < {_MAX_ATTEMPTS}
    """

    try:
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("country", "STRING", country_name),
            ]
        )
        ext_job = client.query(extension_sql, job_config=job_config)
        ext_job.result()
        logger.info(
            "Extension classify done for country=%s — rows affected=%s",
            country_name,
            ext_job.num_dml_affected_rows or 0,
        )

        ambiguous = list(client.query(ambiguous_sql, job_config=job_config).result())
        if not ambiguous:
            logger.info("No ambiguous URLs for HEAD classify — country=%s", country_name)
            return

        batch = []
        for row in ambiguous:
            url_type = head_classify(row["effective_url"])
            batch.append(
                {
                    "menu_url_id": int(row["menu_url_id"]),
                    "effective_url_type": url_type,
                    "menu_fetch_status": status_for_url_type(url_type),
                }
            )

        _merge_head_results(client, control_table, batch)
        logger.info(
            "HEAD classify done for country=%s — %d URLs merged",
            country_name,
            len(batch),
        )
    except Exception as exc:
        logger.error("Failed classifying URLs for country=%s: %s", country_name, exc)
        raise RuntimeError(
            f"Failed classifying URLs for country={country_name}: {exc}"
        ) from exc
