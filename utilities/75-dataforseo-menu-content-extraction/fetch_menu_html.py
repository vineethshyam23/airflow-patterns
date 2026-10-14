"""
fetch_menu_html.py
==================
Fetch classified HTML menu pages into the GCS raw bucket.

Takes one NTILE batch per country, uploads content-addressed HTML, and updates
gcs_html_uri / content_hash / menu_fetch_status on control_lnd.
"""

from __future__ import annotations

import hashlib
import logging
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import urllib3
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from google.cloud import bigquery

from classify_menu_url_types import BROWSER_HEADERS

logger = logging.getLogger(__name__)

_MAX_ATTEMPTS = 3
_TIMEOUT_SEC = 30
_MAX_ERROR_CHARS = 500
FETCH_WORKERS = 5


def _delete_stale_siblings(bucket, folder_prefix: str, keep_object_name: str) -> None:
    """Delete objects under folder_prefix except keep_object_name."""
    try:
        for blob in bucket.list_blobs(prefix=folder_prefix.rstrip("/") + "/"):
            if blob.name != keep_object_name:
                blob.delete()
    except Exception as exc:
        logger.warning(
            "Stale GCS cleanup failed for %s (kept %s): %s",
            folder_prefix,
            keep_object_name,
            exc,
        )


def fetch_page_html(url: str):
    """Return (html, error); error is None on success, a short reason otherwise."""

    def _get(verify: bool = True):
        return requests.get(
            url,
            timeout=_TIMEOUT_SEC,
            allow_redirects=True,
            headers=BROWSER_HEADERS,
            verify=verify,
        )

    try:
        try:
            r = _get()
        except requests.exceptions.SSLError:
            # Expired/self-signed/mismatched certs are common on small sites.
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            r = _get(verify=False)
        if r.status_code >= 400:
            return None, f"http_{r.status_code}"
        return r.text, None
    except Exception as exc:
        return None, f"{type(exc).__name__}: {exc}"


def _merge_fetch_results(
    client: bigquery.Client,
    control_table: str,
    batch: list,
) -> None:
    """Load fetch results into a temp table, then MERGE into control_lnd."""
    if not batch:
        return

    project, dataset, _ = control_table.split(".")
    tmp_table = f"{project}.{dataset}._tmp_menu_html_fetch_{uuid.uuid4().hex[:12]}"
    load_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("menu_url_id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("gcs_html_uri", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("content_hash", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("menu_fetch_status", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("error_message", "STRING", mode="NULLABLE"),
        ],
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    try:
        client.load_table_from_json(batch, tmp_table, job_config=load_config).result()
        # COALESCE keeps the last good artifact when a re-fetch attempt fails.
        merge_sql = f"""
        MERGE `{control_table}` AS T
        USING `{tmp_table}` AS S
        ON T.menu_url_id = S.menu_url_id
        WHEN MATCHED THEN UPDATE SET
          T.gcs_html_uri      = COALESCE(S.gcs_html_uri, T.gcs_html_uri),
          T.content_hash      = COALESCE(S.content_hash, T.content_hash),
          T.menu_fetch_status = S.menu_fetch_status,
          T.error_message     = S.error_message,
          T.attempt_count     = IF(
            S.menu_fetch_status = 'fetch_failed',
            COALESCE(T.attempt_count, 0) + 1,
            0
          ),
          T._update_ts        = CURRENT_TIMESTAMP()
        """
        client.query(merge_sql).result()
    finally:
        client.delete_table(tmp_table, not_found_ok=True)


def fetch_html_for_country(
    client: bigquery.Client,
    control_table: str,
    country_name: str,
    bucket_name: str,
    gcs_prefix: str,
    gcp_conn_id: str,
    work_table: str,
    batch_idx: int,
) -> None:
    """Fetch one batch of classified HTML pages for one country into GCS."""
    # Never-fetched rows plus earlier failures, until attempts run out.
    pending_sql = f"""
    SELECT c.menu_url_id, c.effective_url
    FROM `{control_table}` c
    JOIN `{work_table}` w USING (menu_url_id)
    WHERE w.batch_id = @batch_idx
      AND c.country = @country
      AND c.effective_url_type = 'html'
      AND c.menu_fetch_status IN ('classified', 'fetch_failed')
      AND COALESCE(c.attempt_count, 0) < {_MAX_ATTEMPTS}
    """

    try:
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("country", "STRING", country_name),
                bigquery.ScalarQueryParameter("batch_idx", "INT64", batch_idx),
            ]
        )
        rows = list(client.query(pending_sql, job_config=job_config).result())
        if not rows:
            logger.debug("No HTML pages to fetch — country=%s", country_name)
            return

        # Use storage client directly — GCSHook.upload logs every success at INFO.
        gcs_client = GCSHook(gcp_conn_id=gcp_conn_id).get_conn()
        bucket = gcs_client.bucket(bucket_name)
        batch = []
        batch_lock = threading.Lock()

        def _fetch_and_collect(row):
            """Fetch a single URL, hash, upload, and return a record dict."""
            menu_url_id = int(row["menu_url_id"])
            html, error = fetch_page_html(row["effective_url"])
            if html is None:
                logger.warning("Fetch failed for %s: %s", row["effective_url"], error)
                return {
                    "menu_url_id": menu_url_id,
                    "gcs_html_uri": None,
                    "content_hash": None,
                    "menu_fetch_status": "fetch_failed",
                    "error_message": error[:_MAX_ERROR_CHARS],
                }

            content_hash = hashlib.sha256(html.encode("utf-8")).hexdigest()
            object_name = f"{gcs_prefix}/{country_name}/{menu_url_id}/{content_hash}.html"
            bucket.blob(object_name).upload_from_string(
                html, content_type="text/html"
            )
            _delete_stale_siblings(
                bucket,
                f"{gcs_prefix}/{country_name}/{menu_url_id}",
                object_name,
            )
            return {
                "menu_url_id": menu_url_id,
                "gcs_html_uri": f"gs://{bucket_name}/{object_name}",
                "content_hash": content_hash,
                "menu_fetch_status": "html_stored",
                "error_message": None,
            }

        with ThreadPoolExecutor(max_workers=FETCH_WORKERS) as executor:
            futures = {executor.submit(_fetch_and_collect, row): row for row in rows}
            for future in as_completed(futures):
                try:
                    result = future.result()
                    with batch_lock:
                        batch.append(result)
                except Exception as exc:
                    row = futures[future]
                    logger.error(
                        "Fetch error for %s: %s", row["effective_url"], exc
                    )
                    raise

        _merge_fetch_results(client, control_table, batch)
        logger.debug(
            "HTML fetch done for country=%s — %d stored, %d failed",
            country_name,
            sum(1 for r in batch if r["menu_fetch_status"] == "html_stored"),
            sum(1 for r in batch if r["menu_fetch_status"] == "fetch_failed"),
        )
    except Exception as exc:
        logger.error("Failed fetching HTML for country=%s: %s", country_name, exc)
        raise RuntimeError(
            f"Failed fetching HTML for country={country_name}: {exc}"
        ) from exc
