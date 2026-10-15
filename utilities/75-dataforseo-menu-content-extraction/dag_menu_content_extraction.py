########################################################
# DAG: SEO Menu Content Extraction (v1 HTML)           #
#                                                      #
# From extracted_menu_urls: classify URLs, fetch HTML  #
# → GCS, convert to markdown, extract menus via        #
# BigQuery ML into an idempotent menutable.            #
# Menu-item (dish) extraction is a separate process.   #
########################################################

from __future__ import annotations

import json
import logging
import os
import re
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.utils.helpers import chain
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from google.cloud import bigquery

from classify_menu_url_types import classify_urls_for_country
from convert_menu_html_to_md import convert_html_to_md_for_country
from extract_menu_bqml import extract_menus_for_country
from fetch_menu_html import fetch_html_for_country
from stage_menu_md import stage_md_for_country

logger = logging.getLogger(__name__)


default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email": [
        "dataops@example.com",
    ],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "max_active_runs": 1,
    "retry_delay": timedelta(minutes=10),
}

environment = "env"
env = os.environ.get(environment, Variable.get(environment))

if env == "DEV":
    projectid = "dwh_project_dev"
    bucket_name = "dp_dev_rawzone"
    bigquery_conn_id = "bigquery_default_dev"
    gcp_conn_id = "google_cloud_dev"
else:
    projectid = "dwh_project"
    bucket_name = "dp_rawzone"
    bigquery_conn_id = "bigquery_default"
    gcp_conn_id = "google_cloud_default"

# Upstream: menu URL ledger from pattern 18 (read-only for this DAG)
SOURCE_TABLE = f"{projectid}.de.extracted_menu_urls"

# Pipeline control / staging / output tables
# Output grain: one row per website menu (page-level), not per dish/item
CONTROL_LND_TABLE = f"{projectid}.de.menu_extraction_control_lnd"
STAGING_TABLE = f"{projectid}.de.menu_md_staging"
MENU_TABLE = f"{projectid}.de.extracted_menus"

# GCS path prefixes
# Objects: seo/menu_content/{html|markdown}/{country}/{menu_url_id}/{hash}.{ext}
GCS_HTML_PREFIX = "seo/menu_content/html"
GCS_MARKDOWN_PREFIX = "seo/menu_content/markdown"

# Batch / scale settings — adjust without touching task logic
MAX_BATCH_SLOTS = 25
MAX_ACTIVE_TASKS = 10

# Rows per NTILE batch for small countries; big countries cap at MAX_BATCH_SLOTS
# batches and take more rows each, so nothing is left for the next run.
TARGET_BATCH_SIZE = 2000

# BQML remote model — pin prompt_version when the extract prompt changes.
GEMINI_MODEL = f"{projectid}.de.gemini_menu_extract_model"
PROMPT_VERSION = "v1"

# v1: only process HTML effective URLs after classification
V1_URL_TYPES = ("html",)

# Manual trigger for now (chain after URL DAG once stable)
schedule = None

# Country list for TaskGroup naming (parse-time Variable — no BQ at parse).
# JSON list of country names; refreshed by refresh_country_variable after each run.
# Empty list → country TaskGroups skipped this run (next parse picks up Variable).
COUNTRY_LIST_VAR = "seo_menu_extraction_countries"
try:
    _countries = json.loads(Variable.get(COUNTRY_LIST_VAR, default_var="[]"))
    COUNTRY_LIST: list[str] = sorted(_countries) if isinstance(_countries, list) else []
except Exception:
    COUNTRY_LIST = []

# Control / landing table schema (pipeline work queue)
# menu_url_id = unique key (PK), carried from source extracted_menu_urls
CONTROL_LND_SCHEMA = [
    bigquery.SchemaField("menu_url_id", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("establishment_id", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("restaurant_name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("places_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("country", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("website", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("menu_url", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("fetched_menu_url", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("effective_url", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("effective_url_type", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("gcs_html_uri", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("gcs_md_uri", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("content_hash", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("menu_id", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("menu_fetch_status", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("error_message", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("attempt_count", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("_create_ts", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("_update_ts", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("_processed_ts", "TIMESTAMP", mode="NULLABLE"),
]

# Markdown staging schema — BQML input, one row per menu_url_id
STAGING_SCHEMA = [
    bigquery.SchemaField("menu_url_id", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("menu_id", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("country", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("effective_url", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("gcs_md_uri", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("markdown_text", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("content_hash", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("_load_ts", "TIMESTAMP", mode="NULLABLE"),
]

# Final menutable — one row per website menu (page grain, not dishes)
MENU_SCHEMA = [
    bigquery.SchemaField("menu_id", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("menu_url_id", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("establishment_id", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("country", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("effective_url", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("restaurant_name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("places_id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("website", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("menu_title", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("menu_text", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("menu_content_json", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("gcs_html_uri", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("gcs_md_uri", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("content_hash", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("model_name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("prompt_version", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("_extracted_ts", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("_create_ts", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("_update_ts", "TIMESTAMP", mode="NULLABLE"),
]


def get_bq_client():
    """BigQuery client via Airflow connection."""
    return BigQueryHook(gcp_conn_id=bigquery_conn_id).get_client(project_id=projectid)


def _country_group_id(country_name: str) -> str:
    """Airflow-safe TaskGroup id from country name."""
    safe = re.sub(r"[^A-Za-z0-9_]+", "_", country_name.strip())
    return f"country_{safe}"


def _work_table_for(country_name: str) -> str:
    """Per-country table holding the batch_id assignment for this run."""
    safe = re.sub(r"[^A-Za-z0-9_]+", "_", country_name.strip())
    return f"{projectid}.de._work_menu_batches_{safe}"


def task_create_control_table(**context):
    """Task 1 — Create control landing table if it does not already exist.
    Clustered by country, establishment_id for per-country MERGE/filters later.
    Succeeds when table exists or is created. Fails hard on auth/permission,
    missing dataset, or other BigQuery errors so the DAG does not continue blind.
    """
    try:
        client = get_bq_client()
        table = bigquery.Table(CONTROL_LND_TABLE, schema=CONTROL_LND_SCHEMA)
        table.clustering_fields = ["country", "establishment_id"]
        client.create_table(table, exists_ok=True)
        logger.info(
            "Control table ready: %s (clustered by country, establishment_id)",
            CONTROL_LND_TABLE,
        )
    except Exception as exc:
        logger.error("Failed to create control table %s: %s", CONTROL_LND_TABLE, exc)
        raise RuntimeError(f"Failed to create control table {CONTROL_LND_TABLE}: {exc}") from exc


def task_create_staging_table(**context):
    """Create markdown staging table if it does not already exist.
    Clustered by country, menu_url_id for per-country staging MERGE.
    """
    try:
        client = get_bq_client()
        table = bigquery.Table(STAGING_TABLE, schema=STAGING_SCHEMA)
        table.clustering_fields = ["country", "menu_url_id"]
        client.create_table(table, exists_ok=True)
        logger.info("Staging table ready: %s", STAGING_TABLE)
    except Exception as exc:
        logger.error("Failed to create staging table %s: %s", STAGING_TABLE, exc)
        raise RuntimeError(f"Failed to create staging table {STAGING_TABLE}: {exc}") from exc


def task_create_menu_table(**context):
    """Create the page-level menutable if it does not already exist.
    Clustered by country, establishment_id for per-country extracts.
    """
    try:
        client = get_bq_client()
        table = bigquery.Table(MENU_TABLE, schema=MENU_SCHEMA)
        table.clustering_fields = ["country", "establishment_id"]
        client.create_table(table, exists_ok=True)
        logger.info("Menu table ready: %s", MENU_TABLE)
    except Exception as exc:
        logger.error("Failed to create menu table %s: %s", MENU_TABLE, exc)
        raise RuntimeError(f"Failed to create menu table {MENU_TABLE}: {exc}") from exc


def task_sync_control_for_country(country_name: str, **context):
    """Per-country MERGE: source → control_lnd for one country.

    Upsert on menu_url_id. New rows get menu_fetch_status='pending'.
    Matched rows refresh source fields only (pipeline state untouched).
    """
    client = get_bq_client()
    merge_sql = f"""
    MERGE `{CONTROL_LND_TABLE}` AS T
    USING (
      SELECT
        CAST(menu_url_id AS INT64) AS menu_url_id,
        CAST(establishment_id AS INT64) AS establishment_id,
        CAST(restaurant_name AS STRING) AS restaurant_name,
        CAST(places_id AS STRING) AS places_id,
        CAST(country AS STRING) AS country,
        CAST(website AS STRING) AS website,
        CAST(menu_url AS STRING) AS menu_url,
        CAST(fetched_menu_url AS STRING) AS fetched_menu_url,
        COALESCE(fetched_menu_url, menu_url, website) AS effective_url
      FROM `{SOURCE_TABLE}`
      WHERE country = @country
        AND menu_url_id IS NOT NULL
        AND establishment_id IS NOT NULL
        AND places_id IS NOT NULL
        AND COALESCE(fetched_menu_url, menu_url, website) IS NOT NULL
    ) AS S
    ON T.menu_url_id = S.menu_url_id
    WHEN MATCHED THEN UPDATE SET
      T.establishment_id    = S.establishment_id,
      T.restaurant_name     = S.restaurant_name,
      T.places_id           = S.places_id,
      T.country             = S.country,
      T.website             = S.website,
      T.menu_url            = S.menu_url,
      T.fetched_menu_url    = S.fetched_menu_url,
      T.effective_url       = S.effective_url,
      T._update_ts          = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
      menu_url_id, establishment_id, restaurant_name, places_id,
      country, website, menu_url, fetched_menu_url, effective_url,
      menu_fetch_status, attempt_count, _create_ts, _update_ts
    ) VALUES (
      S.menu_url_id, S.establishment_id, S.restaurant_name, S.places_id,
      S.country, S.website, S.menu_url, S.fetched_menu_url, S.effective_url,
      'pending', 0, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
    )
    """
    try:
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("country", "STRING", country_name),
            ]
        )
        job = client.query(merge_sql, job_config=job_config)
        job.result()
        logger.info(
            "MERGE done for country=%s — rows affected=%s",
            country_name,
            job.num_dml_affected_rows or 0,
        )
    except Exception as exc:
        logger.error("Failed syncing control for country=%s: %s", country_name, exc)
        raise RuntimeError(
            f"Failed syncing {CONTROL_LND_TABLE} for country={country_name}: {exc}"
        ) from exc


def task_classify_urls_for_country(country_name: str, **context):
    """Per-country classify effective_url → effective_url_type + menu_fetch_status."""
    classify_urls_for_country(
        client=get_bq_client(),
        control_table=CONTROL_LND_TABLE,
        country_name=country_name,
    )


def task_plan_batches_for_country(country_name: str, **context) -> dict:
    """Assign a stable batch_id to every html row so the stage lanes never overlap.

    Computed once per country per run: parallel batch tasks that each ran their own
    NTILE would repartition as sibling tasks update statuses mid-stage.
    """
    client = get_bq_client()
    work_table = _work_table_for(country_name)
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("country", "STRING", country_name),
        ]
    )
    count_sql = f"""
    SELECT COUNT(*) AS row_count
    FROM `{CONTROL_LND_TABLE}`
    WHERE country = @country AND effective_url_type = 'html'
    """
    try:
        row_count = list(client.query(count_sql, job_config=job_config).result())[0][
            "row_count"
        ]
        if not row_count:
            logger.info("No html rows to plan for country=%s", country_name)
            return {"country": country_name, "num_batches": 0, "row_count": 0}

        num_batches = min(
            MAX_BATCH_SLOTS,
            (row_count + TARGET_BATCH_SIZE - 1) // TARGET_BATCH_SIZE,
        )
        # NTILE needs a constant, so num_batches is interpolated (int, computed above).
        work_sql = f"""
        CREATE OR REPLACE TABLE `{work_table}` AS
        SELECT
          menu_url_id,
          NTILE({num_batches}) OVER (ORDER BY menu_url_id) AS batch_id
        FROM `{CONTROL_LND_TABLE}`
        WHERE country = @country AND effective_url_type = 'html'
        """
        client.query(work_sql, job_config=job_config).result()
        logger.info(
            "Country '%s': %d html rows → %d batches (~%d per batch)",
            country_name,
            row_count,
            num_batches,
            (row_count + num_batches - 1) // num_batches,
        )
        return {
            "country": country_name,
            "num_batches": num_batches,
            "row_count": row_count,
        }
    except Exception as exc:
        logger.error("Failed planning batches for country=%s: %s", country_name, exc)
        raise RuntimeError(
            f"Failed planning batches for country={country_name}: {exc}"
        ) from exc


def _batch_in_plan(country_name: str, batch_idx: int, context) -> bool:
    """True when this batch slot is within the planned batch count."""
    plan = (
        context["ti"].xcom_pull(
            task_ids=f"{_country_group_id(country_name)}.plan_batches"
        )
        or {}
    )
    return batch_idx <= int(plan.get("num_batches", 0))


def task_fetch_html_for_country(country_name: str, batch_idx: int, **context):
    """One batch lane of the per-country HTML fetch into the GCS raw bucket."""
    if not _batch_in_plan(country_name, batch_idx, context):
        return

    fetch_html_for_country(
        client=get_bq_client(),
        control_table=CONTROL_LND_TABLE,
        country_name=country_name,
        bucket_name=bucket_name,
        gcs_prefix=GCS_HTML_PREFIX,
        gcp_conn_id=gcp_conn_id,
        work_table=_work_table_for(country_name),
        batch_idx=batch_idx,
    )


def task_convert_html_to_md_for_country(country_name: str, batch_idx: int, **context):
    """One batch lane of the per-country HTML to markdown conversion."""
    if not _batch_in_plan(country_name, batch_idx, context):
        return

    convert_html_to_md_for_country(
        client=get_bq_client(),
        control_table=CONTROL_LND_TABLE,
        country_name=country_name,
        bucket_name=bucket_name,
        gcs_md_prefix=GCS_MARKDOWN_PREFIX,
        gcp_conn_id=gcp_conn_id,
        work_table=_work_table_for(country_name),
        batch_idx=batch_idx,
    )


def task_stage_md_for_country(country_name: str, batch_idx: int, **context):
    """One batch lane of the per-country markdown staging into BigQuery."""
    if not _batch_in_plan(country_name, batch_idx, context):
        return

    stage_md_for_country(
        client=get_bq_client(),
        control_table=CONTROL_LND_TABLE,
        staging_table=STAGING_TABLE,
        country_name=country_name,
        gcp_conn_id=gcp_conn_id,
        work_table=_work_table_for(country_name),
        batch_idx=batch_idx,
    )


def task_extract_menu_bqml_for_country(country_name: str, batch_idx: int, **context):
    """One batch lane of BQML menu extract into the menutable."""
    if not _batch_in_plan(country_name, batch_idx, context):
        return

    extract_menus_for_country(
        client=get_bq_client(),
        control_table=CONTROL_LND_TABLE,
        staging_table=STAGING_TABLE,
        menu_table=MENU_TABLE,
        gemini_model=GEMINI_MODEL,
        prompt_version=PROMPT_VERSION,
        country_name=country_name,
        work_table=_work_table_for(country_name),
        batch_idx=batch_idx,
    )


def task_refresh_country_variable(**context):
    """Refresh Airflow Variable used at next DAG parse for country TaskGroups."""
    client = get_bq_client()
    countries_sql = f"""
    SELECT DISTINCT country
    FROM `{SOURCE_TABLE}`
    WHERE country IS NOT NULL
      AND menu_url_id IS NOT NULL
      AND establishment_id IS NOT NULL
      AND places_id IS NOT NULL
      AND COALESCE(fetched_menu_url, menu_url, website) IS NOT NULL
    ORDER BY country
    """
    try:
        countries = [row["country"] for row in client.query(countries_sql).result()]
        Variable.set(COUNTRY_LIST_VAR, json.dumps(countries))
        logger.info(
            "Airflow Variable '%s' updated with %d countries: %s",
            COUNTRY_LIST_VAR,
            len(countries),
            countries,
        )
    except Exception as exc:
        logger.error("Failed refreshing country Variable: %s", exc)
        raise RuntimeError(
            f"Failed refreshing Variable {COUNTRY_LIST_VAR}: {exc}"
        ) from exc


dag = DAG(
    dag_id="etl_seo_menu_content_extraction",
    default_args=default_args,
    description=(
        "Extract website menus from SEO menu URLs "
        "(classify → GCS HTML → markdown → BigQuery ML). "
    ),
    schedule_interval=schedule,
    render_template_as_native_obj=True,
    catchup=False,
    max_active_tasks=MAX_ACTIVE_TASKS,
    tags=["bigquery", "seo", "extraction", "menu", "bqml"],
)

start = EmptyOperator(task_id="start", trigger_rule=TriggerRule.ALL_DONE, dag=dag)
end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE, dag=dag)

t1_create_control_table = PythonOperator(
    task_id="create_control_table",
    python_callable=task_create_control_table,
    provide_context=True,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    dag=dag,
)

t2_create_staging_table = PythonOperator(
    task_id="create_staging_table",
    python_callable=task_create_staging_table,
    provide_context=True,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    dag=dag,
)

t3_create_menu_table = PythonOperator(
    task_id="create_menu_table",
    python_callable=task_create_menu_table,
    provide_context=True,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    dag=dag,
)

t_refresh_country_variable = PythonOperator(
    task_id="refresh_country_variable",
    python_callable=task_refresh_country_variable,
    provide_context=True,
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
)

# Per-country TaskGroups: sync → classify → plan → fetch/convert/stage/bqml lanes
country_groups = []
for country_name in COUNTRY_LIST:
    with TaskGroup(group_id=_country_group_id(country_name), dag=dag) as country_tg:
        t_sync = PythonOperator(
            task_id="control_lnd_table_creation",
            python_callable=task_sync_control_for_country,
            op_kwargs={"country_name": country_name},
            provide_context=True,
            trigger_rule=TriggerRule.ALL_SUCCESS,
            dag=dag,
        )
        t_classify = PythonOperator(
            task_id="classify_url_types",
            python_callable=task_classify_urls_for_country,
            op_kwargs={"country_name": country_name},
            provide_context=True,
            trigger_rule=TriggerRule.ALL_SUCCESS,
            dag=dag,
        )
        t_plan = PythonOperator(
            task_id="plan_batches",
            python_callable=task_plan_batches_for_country,
            op_kwargs={"country_name": country_name},
            provide_context=True,
            trigger_rule=TriggerRule.ALL_SUCCESS,
            dag=dag,
        )
        # One lane per batch slot: fetch_i → convert_i → stage_i → extract_i.
        # ALL_DONE keeps a flaky batch from blocking the rest of its own lane.
        fetch_tasks = [
            PythonOperator(
                task_id=f"fetch_html_to_gcs_{i}",
                python_callable=task_fetch_html_for_country,
                op_kwargs={"country_name": country_name, "batch_idx": i},
                provide_context=True,
                trigger_rule=TriggerRule.ALL_DONE,
                dag=dag,
            )
            for i in range(1, MAX_BATCH_SLOTS + 1)
        ]
        convert_tasks = [
            PythonOperator(
                task_id=f"convert_html_to_markdown_{i}",
                python_callable=task_convert_html_to_md_for_country,
                op_kwargs={"country_name": country_name, "batch_idx": i},
                provide_context=True,
                trigger_rule=TriggerRule.ALL_DONE,
                dag=dag,
            )
            for i in range(1, MAX_BATCH_SLOTS + 1)
        ]
        stage_tasks = [
            PythonOperator(
                task_id=f"stage_markdown_to_bq_{i}",
                python_callable=task_stage_md_for_country,
                op_kwargs={"country_name": country_name, "batch_idx": i},
                provide_context=True,
                trigger_rule=TriggerRule.ALL_DONE,
                dag=dag,
            )
            for i in range(1, MAX_BATCH_SLOTS + 1)
        ]
        extract_tasks = [
            PythonOperator(
                task_id=f"extract_menu_bqml_{i}",
                python_callable=task_extract_menu_bqml_for_country,
                op_kwargs={"country_name": country_name, "batch_idx": i},
                provide_context=True,
                trigger_rule=TriggerRule.ALL_DONE,
                dag=dag,
            )
            for i in range(1, MAX_BATCH_SLOTS + 1)
        ]
        chain(
            t_sync,
            t_classify,
            t_plan,
            fetch_tasks,
            convert_tasks,
            stage_tasks,
            extract_tasks,
        )
    country_groups.append(country_tg)

chain(start, t1_create_control_table, t2_create_staging_table, t3_create_menu_table)

if country_groups:
    chain(*country_groups)
    chain(t3_create_menu_table, country_groups[0])
    chain(country_groups[-1], t_refresh_country_variable)
else:
    logger.warning(
        "No country TaskGroups — set/refresh Variable '%s' (JSON list). "
        "This run will only create the tables and refresh the Variable.",
        COUNTRY_LIST_VAR,
    )
    chain(t3_create_menu_table, t_refresh_country_variable)

chain(t_refresh_country_variable, end)
