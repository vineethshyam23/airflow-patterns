"""
extract_menu_bqml.py
====================
Extract page-level menus from staged markdown via AI.GENERATE_TABLE.

One NTILE batch per call. Successful rows MERGE into the menutable;
failures stay on control_lnd as bqml_failed for retry.
"""

from __future__ import annotations

import logging
import uuid

from google.cloud import bigquery

logger = logging.getLogger(__name__)

_MAX_ATTEMPTS = 3
_MAX_ERROR_CHARS = 500
_MAX_OUTPUT_TOKENS = 8192

_MENU_ID_SQL = (
    "FARM_FINGERPRINT(CONCAT(CAST(c.establishment_id AS STRING), '|', "
    "COALESCE(c.effective_url, '')))"
)

_PROMPT_PREFIX = (
    "Extract the website menu as a single page-level object. "
    "Do not explode individual dishes into separate rows. "
    "Fill menu_title (short title of this menu page), "
    "menu_text (cleaned menu text summary), "
    "and menu_content_json (JSON object with sections and items as they appear). "
    "Page content:\n\n"
)

_OUTPUT_SCHEMA = (
    "menu_title STRING, menu_text STRING, menu_content_json STRING"
)


def extract_menus_for_country(
    client: bigquery.Client,
    control_table: str,
    staging_table: str,
    menu_table: str,
    gemini_model: str,
    prompt_version: str,
    country_name: str,
    work_table: str,
    batch_idx: int,
) -> None:
    """Run AI.GENERATE_TABLE for one country batch and MERGE results."""
    project, dataset, _ = control_table.split(".")
    tmp_table = f"{project}.{dataset}._tmp_menu_bqml_{uuid.uuid4().hex[:12]}"

    extract_sql = f"""
    CREATE OR REPLACE TABLE `{tmp_table}` AS
    SELECT
      menu_url_id,
      menu_id,
      establishment_id,
      restaurant_name,
      places_id,
      country,
      website,
      effective_url,
      gcs_html_uri,
      gcs_md_uri,
      content_hash,
      menu_title,
      menu_text,
      menu_content_json,
      IF(status IS NULL OR status = '', 'ml_extracted', 'bqml_failed')
        AS menu_fetch_status,
      IF(
        status IS NULL OR status = '',
        CAST(NULL AS STRING),
        SUBSTR(CAST(status AS STRING), 1, {_MAX_ERROR_CHARS})
      ) AS error_message
    FROM AI.GENERATE_TABLE(
      MODEL `{gemini_model}`,
      (
        SELECT
          CONCAT(@prompt_prefix, s.markdown_text) AS prompt,
          c.menu_url_id,
          COALESCE(c.menu_id, {_MENU_ID_SQL}) AS menu_id,
          c.establishment_id,
          c.restaurant_name,
          c.places_id,
          c.country,
          c.website,
          c.effective_url,
          c.gcs_html_uri,
          c.gcs_md_uri,
          c.content_hash
        FROM `{control_table}` c
        JOIN `{work_table}` w USING (menu_url_id)
        JOIN `{staging_table}` s USING (menu_url_id)
        WHERE w.batch_id = @batch_idx
          AND c.country = @country
          AND c.menu_fetch_status IN ('md_staged', 'bqml_failed')
          AND s.markdown_text IS NOT NULL
          AND COALESCE(c.attempt_count, 0) < {_MAX_ATTEMPTS}
      ),
      STRUCT(
        '{_OUTPUT_SCHEMA}' AS output_schema,
        {_MAX_OUTPUT_TOKENS} AS max_output_tokens,
        0.0 AS temperature
      )
    )
    """

    menu_merge_sql = f"""
    MERGE `{menu_table}` AS T
    USING (
      SELECT * FROM `{tmp_table}`
      WHERE menu_fetch_status = 'ml_extracted'
    ) AS S
    ON T.menu_id = S.menu_id
    WHEN MATCHED THEN UPDATE SET
      T.menu_url_id         = S.menu_url_id,
      T.establishment_id    = S.establishment_id,
      T.country             = S.country,
      T.effective_url       = S.effective_url,
      T.restaurant_name     = S.restaurant_name,
      T.places_id           = S.places_id,
      T.website             = S.website,
      T.menu_title          = S.menu_title,
      T.menu_text           = S.menu_text,
      T.menu_content_json   = S.menu_content_json,
      T.gcs_html_uri        = S.gcs_html_uri,
      T.gcs_md_uri          = S.gcs_md_uri,
      T.content_hash        = S.content_hash,
      T.model_name          = @model_name,
      T.prompt_version      = @prompt_version,
      T._extracted_ts       = CURRENT_TIMESTAMP(),
      T._update_ts          = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
      menu_id, menu_url_id, establishment_id, country, effective_url,
      restaurant_name, places_id, website,
      menu_title, menu_text, menu_content_json,
      gcs_html_uri, gcs_md_uri, content_hash,
      model_name, prompt_version,
      _extracted_ts, _create_ts, _update_ts
    ) VALUES (
      S.menu_id, S.menu_url_id, S.establishment_id, S.country, S.effective_url,
      S.restaurant_name, S.places_id, S.website,
      S.menu_title, S.menu_text, S.menu_content_json,
      S.gcs_html_uri, S.gcs_md_uri, S.content_hash,
      @model_name, @prompt_version,
      CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
    )
    """

    control_merge_sql = f"""
    MERGE `{control_table}` AS T
    USING `{tmp_table}` AS S
    ON T.menu_url_id = S.menu_url_id
    WHEN MATCHED THEN UPDATE SET
      T.menu_id           = IF(
        S.menu_fetch_status = 'ml_extracted', S.menu_id, T.menu_id
      ),
      T.menu_fetch_status = S.menu_fetch_status,
      T.error_message     = S.error_message,
      T.attempt_count     = IF(
        S.menu_fetch_status = 'bqml_failed',
        COALESCE(T.attempt_count, 0) + 1,
        0
      ),
      T._processed_ts     = IF(
        S.menu_fetch_status = 'ml_extracted',
        CURRENT_TIMESTAMP(),
        T._processed_ts
      ),
      T._update_ts        = CURRENT_TIMESTAMP()
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("country", "STRING", country_name),
            bigquery.ScalarQueryParameter("batch_idx", "INT64", batch_idx),
            bigquery.ScalarQueryParameter("prompt_prefix", "STRING", _PROMPT_PREFIX),
            bigquery.ScalarQueryParameter("model_name", "STRING", gemini_model),
            bigquery.ScalarQueryParameter("prompt_version", "STRING", prompt_version),
        ]
    )

    try:
        pending_sql = f"""
        SELECT COUNT(*) AS row_count
        FROM `{control_table}` c
        JOIN `{work_table}` w USING (menu_url_id)
        JOIN `{staging_table}` s USING (menu_url_id)
        WHERE w.batch_id = @batch_idx
          AND c.country = @country
          AND c.menu_fetch_status IN ('md_staged', 'bqml_failed')
          AND s.markdown_text IS NOT NULL
          AND COALESCE(c.attempt_count, 0) < {_MAX_ATTEMPTS}
        """
        row_count = list(client.query(pending_sql, job_config=job_config).result())[0][
            "row_count"
        ]
        if not row_count:
            logger.debug(
                "No markdown rows to extract — country=%s batch=%s",
                country_name,
                batch_idx,
            )
            return

        client.query(extract_sql, job_config=job_config).result()
        client.query(menu_merge_sql, job_config=job_config).result()
        client.query(control_merge_sql, job_config=job_config).result()

        summary_sql = f"""
        SELECT menu_fetch_status, COUNT(*) AS n
        FROM `{tmp_table}`
        GROUP BY menu_fetch_status
        """
        counts = {
            row["menu_fetch_status"]: row["n"]
            for row in client.query(summary_sql).result()
        }
        failed = counts.get("bqml_failed", 0)
        if failed:
            logger.warning(
                "BQML extract country=%s batch=%s — %d extracted, %d failed",
                country_name,
                batch_idx,
                counts.get("ml_extracted", 0),
                failed,
            )
        else:
            logger.debug(
                "BQML extract country=%s batch=%s — %d extracted",
                country_name,
                batch_idx,
                counts.get("ml_extracted", 0),
            )
    except Exception as exc:
        logger.error(
            "Failed BQML extract for country=%s batch=%s: %s",
            country_name,
            batch_idx,
            exc,
        )
        raise RuntimeError(
            f"Failed BQML extract for country={country_name} batch={batch_idx}: {exc}"
        ) from exc
    finally:
        client.delete_table(tmp_table, not_found_ok=True)
