"""
stage_menu_md.py
================
Stage converted markdown files into BigQuery for BQML input.

Reads gcs_md_uri from control_lnd, downloads the .md, MERGEs the text into
menu_md_staging, and moves control_lnd to md_staged.
"""

from __future__ import annotations

import logging
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed

from airflow.providers.google.cloud.hooks.gcs import GCSHook
from google.cloud import bigquery

logger = logging.getLogger(__name__)

_MAX_ATTEMPTS = 3
_MAX_ERROR_CHARS = 500
MARKDOWN_MAX_CHARS = 500_000
STAGE_WORKERS = 5

# Stable per-menu key, same formula used downstream in the menus table.
_MENU_ID_SQL = (
    "FARM_FINGERPRINT(CONCAT(CAST(establishment_id AS STRING), '|', "
    "COALESCE(effective_url, '')))"
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


def _merge_stage_results(
    client: bigquery.Client,
    control_table: str,
    staging_table: str,
    batch: list,
) -> None:
    """Load results into a temp table, then MERGE staging and control_lnd."""
    if not batch:
        return

    project, dataset, _ = control_table.split(".")
    tmp_table = f"{project}.{dataset}._tmp_menu_md_stage_{uuid.uuid4().hex[:12]}"
    load_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("menu_url_id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("establishment_id", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("country", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("effective_url", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("gcs_md_uri", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("markdown_text", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("content_hash", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("menu_fetch_status", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("error_message", "STRING", mode="NULLABLE"),
        ],
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    try:
        client.load_table_from_json(batch, tmp_table, job_config=load_config).result()

        stage_sql = f"""
        MERGE `{staging_table}` AS T
        USING (
          SELECT
            menu_url_id,
            {_MENU_ID_SQL} AS menu_id,
            country,
            effective_url,
            gcs_md_uri,
            markdown_text,
            content_hash
          FROM `{tmp_table}`
          WHERE menu_fetch_status = 'md_staged'
        ) AS S
        ON T.menu_url_id = S.menu_url_id
        WHEN MATCHED THEN UPDATE SET
          T.menu_id       = S.menu_id,
          T.country       = S.country,
          T.effective_url = S.effective_url,
          T.gcs_md_uri    = S.gcs_md_uri,
          T.markdown_text = S.markdown_text,
          T.content_hash  = S.content_hash,
          T._load_ts      = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT (
          menu_url_id, menu_id, country, effective_url,
          gcs_md_uri, markdown_text, content_hash, _load_ts
        ) VALUES (
          S.menu_url_id, S.menu_id, S.country, S.effective_url,
          S.gcs_md_uri, S.markdown_text, S.content_hash, CURRENT_TIMESTAMP()
        )
        """
        client.query(stage_sql).result()

        control_sql = f"""
        MERGE `{control_table}` AS T
        USING (
          SELECT
            menu_url_id,
            {_MENU_ID_SQL} AS menu_id,
            menu_fetch_status,
            error_message
          FROM `{tmp_table}`
        ) AS S
        ON T.menu_url_id = S.menu_url_id
        WHEN MATCHED THEN UPDATE SET
          T.menu_id           = IF(S.menu_fetch_status = 'md_staged', S.menu_id, T.menu_id),
          T.menu_fetch_status = S.menu_fetch_status,
          T.error_message     = S.error_message,
          T.attempt_count     = IF(
            S.menu_fetch_status = 'stage_failed',
            COALESCE(T.attempt_count, 0) + 1,
            0
          ),
          T._update_ts        = CURRENT_TIMESTAMP()
        """
        client.query(control_sql).result()
    finally:
        client.delete_table(tmp_table, not_found_ok=True)


def stage_md_for_country(
    client: bigquery.Client,
    control_table: str,
    staging_table: str,
    country_name: str,
    gcp_conn_id: str,
    work_table: str,
    batch_idx: int,
) -> None:
    """Stage one batch of converted markdown files for one country."""
    pending_sql = f"""
    SELECT c.menu_url_id, c.establishment_id, c.country, c.effective_url,
           c.gcs_md_uri, c.content_hash
    FROM `{control_table}` c
    JOIN `{work_table}` w USING (menu_url_id)
    WHERE w.batch_id = @batch_idx
      AND c.country = @country
      AND c.menu_fetch_status IN ('md_converted', 'stage_failed')
      AND c.gcs_md_uri IS NOT NULL
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
            logger.debug("No markdown files to stage — country=%s", country_name)
            return

        gcs_client = GCSHook(gcp_conn_id=gcp_conn_id).get_conn()
        batch = []
        batch_lock = threading.Lock()

        def _download_and_collect(row):
            menu_url_id = int(row["menu_url_id"])
            record = {
                "menu_url_id": menu_url_id,
                "establishment_id": row["establishment_id"],
                "country": row["country"],
                "effective_url": row["effective_url"],
                "gcs_md_uri": row["gcs_md_uri"],
                "content_hash": row["content_hash"],
            }
            try:
                src_bucket, src_object = _parse_gs_uri(row["gcs_md_uri"])
                markdown = (
                    gcs_client.bucket(src_bucket)
                    .blob(src_object)
                    .download_as_text(encoding="utf-8")
                )
                record["markdown_text"] = markdown[:MARKDOWN_MAX_CHARS]
                record["menu_fetch_status"] = "md_staged"
                record["error_message"] = None
            except Exception as exc:
                error = f"{type(exc).__name__}: {exc}"
                logger.warning(
                    "MD stage failed for menu_url_id=%s: %s", menu_url_id, error
                )
                record["markdown_text"] = None
                record["menu_fetch_status"] = "stage_failed"
                record["error_message"] = error[:_MAX_ERROR_CHARS]
            return record

        with ThreadPoolExecutor(max_workers=STAGE_WORKERS) as executor:
            futures = {executor.submit(_download_and_collect, row): row for row in rows}
            for future in as_completed(futures):
                try:
                    result = future.result()
                    with batch_lock:
                        batch.append(result)
                except Exception as exc:
                    row = futures[future]
                    logger.error(
                        "Stage error for menu_url_id=%s: %s", row["menu_url_id"], exc
                    )
                    raise

        _merge_stage_results(client, control_table, staging_table, batch)
        logger.debug(
            "MD stage done for country=%s — %d staged, %d failed",
            country_name,
            sum(1 for r in batch if r["menu_fetch_status"] == "md_staged"),
            sum(1 for r in batch if r["menu_fetch_status"] == "stage_failed"),
        )
    except Exception as exc:
        logger.error("Failed staging markdown for country=%s: %s", country_name, exc)
        raise RuntimeError(
            f"Failed staging markdown for country={country_name}: {exc}"
        ) from exc
