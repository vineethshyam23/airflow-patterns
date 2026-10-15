"""Menu extraction, phase 1 handoff.

Phase 1: MERGE listings into the URL ledger, then render pages that
plain HTTP cannot read. Cloud Run does the Chromium work.

Phase 2 (HTML → markdown → page-level BQML) is pattern 75. The extract
SQL below is the contract that DAG expects from `de.extracted_menus`.
Country × NTILE task groups live in patterns 18 and 75; this DAG is the
seam, on purpose smaller than those two.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from google.cloud import bigquery

from scraper_client import fetch_menu_page

logger = logging.getLogger(__name__)

PROJECT = "dwh_project"
DATASET = "de"
SOURCE = f"{PROJECT}.refined.seo_establishments"
LEDGER = f"{PROJECT}.{DATASET}.extracted_menu_urls"
MENU = f"{PROJECT}.{DATASET}.extracted_menus"
STAGING = f"{PROJECT}.{DATASET}.menu_md_staging"
CONTROL = f"{PROJECT}.{DATASET}.menu_extraction_control_lnd"
MODEL = f"{PROJECT}.{DATASET}.gemini_menu_extract_model"

LEDGER_MERGE = f"""
MERGE `{LEDGER}` T
USING (
  SELECT
    FARM_FINGERPRINT(CONCAT(CAST(establishment_id AS STRING), '|', places_id))
      AS menu_url_id,
    restaurant_name,
    places_id,
    country,
    first_seen,
    establishment_id,
    website,
    menu_url
  FROM `{SOURCE}`
  WHERE establishment_id IS NOT NULL
    AND places_id IS NOT NULL
    AND country IS NOT NULL
    AND (website IS NOT NULL OR menu_url IS NOT NULL)
) S
ON T.establishment_id = S.establishment_id
 AND T.places_id = S.places_id
WHEN MATCHED THEN UPDATE SET
  restaurant_name = S.restaurant_name,
  website = S.website,
  menu_url = S.menu_url,
  _update_ts = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
  menu_url_id, restaurant_name, places_id, country, first_seen,
  establishment_id, website, menu_url, fetched_menu_url,
  fetched_menu_url_status, playwright_used, _extraction_complete,
  _create_ts, _update_ts
) VALUES (
  S.menu_url_id, S.restaurant_name, S.places_id, S.country, S.first_seen,
  S.establishment_id, S.website, S.menu_url,
  S.menu_url,
  IF(S.menu_url IS NULL, 'pending', 'source_menu_url'),
  FALSE,
  S.menu_url IS NOT NULL,
  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
)
"""

# Page grain. Pattern 75 runs this per country batch; kept here so the
# menutable contract is visible next to the ledger MERGE.
MENU_EXTRACT = f"""
MERGE `{MENU}` T
USING (
  SELECT
    menu_url_id,
    menu_id,
    establishment_id,
    country,
    effective_url,
    restaurant_name,
    places_id,
    website,
    menu_title,
    menu_text,
    menu_content_json,
    gcs_html_uri,
    gcs_md_uri,
    content_hash
  FROM AI.GENERATE_TABLE(
    MODEL `{MODEL}`,
    (
      SELECT
        CONCAT(
          'Extract one page-level menu. Do not explode dishes. ',
          s.markdown_text
        ) AS prompt,
        c.menu_url_id,
        COALESCE(
          c.menu_id,
          FARM_FINGERPRINT(CONCAT(CAST(c.establishment_id AS STRING), '|',
            COALESCE(c.effective_url, '')))
        ) AS menu_id,
        c.establishment_id,
        c.country,
        c.effective_url,
        c.restaurant_name,
        c.places_id,
        c.website,
        c.gcs_html_uri,
        c.gcs_md_uri,
        c.content_hash
      FROM `{CONTROL}` c
      JOIN `{STAGING}` s USING (menu_url_id)
      WHERE c.country = @country
        AND c.menu_fetch_status IN ('md_staged', 'bqml_failed')
        AND s.markdown_text IS NOT NULL
        AND COALESCE(c.attempt_count, 0) < 3
    ),
    STRUCT(
      'menu_title STRING, menu_text STRING, menu_content_json STRING' AS output_schema,
      8192 AS max_output_tokens,
      0.0 AS temperature
    )
  )
  WHERE status IS NULL OR status = ''
) S
ON T.menu_id = S.menu_id
WHEN MATCHED THEN UPDATE SET
  menu_title = S.menu_title,
  menu_text = S.menu_text,
  menu_content_json = S.menu_content_json,
  content_hash = S.content_hash,
  _update_ts = CURRENT_TIMESTAMP(),
  _extracted_ts = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
  menu_id, menu_url_id, establishment_id, country, effective_url,
  restaurant_name, places_id, website, menu_title, menu_text,
  menu_content_json, gcs_html_uri, gcs_md_uri, content_hash,
  model_name, prompt_version, _extracted_ts, _create_ts, _update_ts
) VALUES (
  S.menu_id, S.menu_url_id, S.establishment_id, S.country, S.effective_url,
  S.restaurant_name, S.places_id, S.website, S.menu_title, S.menu_text,
  S.menu_content_json, S.gcs_html_uri, S.gcs_md_uri, S.content_hash,
  '{MODEL}', 'v1',
  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
)
"""


def _client() -> bigquery.Client:
    return bigquery.Client(project=PROJECT)


def task_sync_ledger(**_context):
    job = _client().query(LEDGER_MERGE)
    job.result()
    logger.info("ledger merge affected %s", job.num_dml_affected_rows)


def task_render_pending(**_context):
    """One page at a time for the reference. Pattern 18 batches by country.

    Rows with a source menu_url never hit Cloud Run.
    """
    scraper = Variable.get("playwright_scraper_url", default_var="")
    client = _client()
    rows = client.query(
        f"""
        SELECT menu_url_id, COALESCE(menu_url, website) AS url
        FROM `{LEDGER}`
        WHERE _extraction_complete IS NOT TRUE
          AND COALESCE(menu_url, website) IS NOT NULL
        LIMIT 50
        """
    ).result()
    for row in rows:
        html, status, final, used = fetch_menu_page(row["url"], scraper)
        # Link picking from the DOM is pattern 18. Here we only record
        # that the renderer ran and whether it returned a body.
        client.query(
            f"""
            UPDATE `{LEDGER}`
            SET playwright_used = @used,
                fetched_menu_url_status = @status,
                fetched_menu_url = IF(@used, @final, fetched_menu_url),
                _update_ts = CURRENT_TIMESTAMP()
            WHERE menu_url_id = @menu_url_id
            """,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("used", "BOOL", used),
                    bigquery.ScalarQueryParameter("status", "STRING", status),
                    bigquery.ScalarQueryParameter("final", "STRING", final),
                    bigquery.ScalarQueryParameter("menu_url_id", "INT64", row["menu_url_id"]),
                ]
            ),
        ).result()
        logger.info(
            "menu_url_id=%s status=%s playwright_used=%s bytes=%s",
            row["menu_url_id"],
            status,
            used,
            len(html or ""),
        )


def task_extract_country(country: str = "DE", **_context):
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("country", "STRING", country)]
    )
    job = _client().query(MENU_EXTRACT, job_config=job_config)
    job.result()
    logger.info("menu extract %s affected %s", country, job.num_dml_affected_rows)


default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email": ["dataops@example.com"],
    "email_on_failure": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="etl_menu_extraction",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["bigquery", "cloud-run", "menu"],
    doc_md=__doc__,
) as dag:
    start = EmptyOperator(task_id="start")
    sync_ledger = PythonOperator(task_id="sync_ledger", python_callable=task_sync_ledger)
    render_pending = PythonOperator(
        task_id="render_pending_http_then_cloud_run",
        python_callable=task_render_pending,
    )
    extract_menus = PythonOperator(
        task_id="extract_page_menus",
        python_callable=task_extract_country,
    )
    end = EmptyOperator(task_id="end")
    start >> sync_ledger >> render_pending >> extract_menus >> end
