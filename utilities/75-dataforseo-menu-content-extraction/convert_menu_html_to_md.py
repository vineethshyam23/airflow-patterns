"""
convert_menu_html_to_md.py
==========================
Convert stored HTML menu pages to Markdown and write them to GCS.

Reads gcs_html_uri from control_lnd, converts with markdownify, uploads .md,
and updates gcs_md_uri / menu_fetch_status.
"""

from __future__ import annotations

import logging
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed

from airflow.providers.google.cloud.hooks.gcs import GCSHook
from google.cloud import bigquery
from markdownify import markdownify

logger = logging.getLogger(__name__)

_MAX_ATTEMPTS = 3
_MAX_ERROR_CHARS = 500
CONVERT_WORKERS = 5


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


def _parse_gs_uri(gs_uri: str) -> tuple[str, str]:
    """Split gs://bucket/object into (bucket, object_name)."""
    if not gs_uri or not gs_uri.startswith("gs://"):
        raise ValueError(f"Invalid GCS URI: {gs_uri}")
    path = gs_uri[5:]
    bucket, _, object_name = path.partition("/")
    if not bucket or not object_name:
        raise ValueError(f"Invalid GCS URI: {gs_uri}")
    return bucket, object_name


def html_to_markdown(html: str) -> str:
    """Convert HTML string to ATX markdown."""
    return markdownify(html, heading_style="ATX")


def _merge_convert_results(
    client: bigquery.Client,
    control_table: str,
    batch: list,
) -> None:
    """Load convert results into a temp table, then MERGE into control_lnd."""
    if not batch:
        return

    project, dataset, _ = control_table.split(".")
    tmp_table = f"{project}.{dataset}._tmp_menu_md_convert_{uuid.uuid4().hex[:12]}"
    load_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("menu_url_id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("gcs_md_uri", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("menu_fetch_status", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("error_message", "STRING", mode="NULLABLE"),
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
          T.gcs_md_uri        = COALESCE(S.gcs_md_uri, T.gcs_md_uri),
          T.menu_fetch_status = S.menu_fetch_status,
          T.error_message     = S.error_message,
          T.attempt_count     = IF(
            S.menu_fetch_status = 'md_convert_failed',
            COALESCE(T.attempt_count, 0) + 1,
            0
          ),
          T._update_ts        = CURRENT_TIMESTAMP()
        """
        client.query(merge_sql).result()
    finally:
        client.delete_table(tmp_table, not_found_ok=True)


def convert_html_to_md_for_country(
    client: bigquery.Client,
    control_table: str,
    country_name: str,
    bucket_name: str,
    gcs_md_prefix: str,
    gcp_conn_id: str,
    work_table: str,
    batch_idx: int,
) -> None:
    """Convert one batch of stored HTML pages to markdown for one country."""
    pending_sql = f"""
    SELECT c.menu_url_id, c.gcs_html_uri, c.content_hash
    FROM `{control_table}` c
    JOIN `{work_table}` w USING (menu_url_id)
    WHERE w.batch_id = @batch_idx
      AND c.country = @country
      AND c.menu_fetch_status IN ('html_stored', 'md_convert_failed')
      AND c.gcs_html_uri IS NOT NULL
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
            logger.debug("No HTML pages to convert — country=%s", country_name)
            return

        gcs_client = GCSHook(gcp_conn_id=gcp_conn_id).get_conn()
        batch = []
        batch_lock = threading.Lock()

        def _convert_and_collect(row):
            menu_url_id = int(row["menu_url_id"])
            content_hash = row["content_hash"] or "unknown"
            try:
                src_bucket, src_object = _parse_gs_uri(row["gcs_html_uri"])
                html = (
                    gcs_client.bucket(src_bucket)
                    .blob(src_object)
                    .download_as_text(encoding="utf-8")
                )
                markdown = html_to_markdown(html)
                object_name = (
                    f"{gcs_md_prefix}/{country_name}/{menu_url_id}/{content_hash}.md"
                )
                dest_bucket = gcs_client.bucket(bucket_name)
                dest_bucket.blob(object_name).upload_from_string(
                    markdown, content_type="text/markdown"
                )
                _delete_stale_siblings(
                    dest_bucket,
                    f"{gcs_md_prefix}/{country_name}/{menu_url_id}",
                    object_name,
                )
                return {
                    "menu_url_id": menu_url_id,
                    "gcs_md_uri": f"gs://{bucket_name}/{object_name}",
                    "menu_fetch_status": "md_converted",
                    "error_message": None,
                }
            except Exception as exc:
                error = f"{type(exc).__name__}: {exc}"
                logger.warning(
                    "MD convert failed for menu_url_id=%s: %s", menu_url_id, error
                )
                return {
                    "menu_url_id": menu_url_id,
                    "gcs_md_uri": None,
                    "menu_fetch_status": "md_convert_failed",
                    "error_message": error[:_MAX_ERROR_CHARS],
                }

        with ThreadPoolExecutor(max_workers=CONVERT_WORKERS) as executor:
            futures = {executor.submit(_convert_and_collect, row): row for row in rows}
            for future in as_completed(futures):
                try:
                    result = future.result()
                    with batch_lock:
                        batch.append(result)
                except Exception as exc:
                    row = futures[future]
                    logger.error(
                        "Convert error for menu_url_id=%s: %s",
                        row["menu_url_id"],
                        exc,
                    )
                    raise

        _merge_convert_results(client, control_table, batch)
        logger.debug(
            "MD convert done for country=%s — %d converted, %d failed",
            country_name,
            sum(1 for r in batch if r["menu_fetch_status"] == "md_converted"),
            sum(1 for r in batch if r["menu_fetch_status"] == "md_convert_failed"),
        )
    except Exception as exc:
        logger.error("Failed converting HTML to MD for country=%s: %s", country_name, exc)
        raise RuntimeError(
            f"Failed converting HTML to MD for country={country_name}: {exc}"
        ) from exc
