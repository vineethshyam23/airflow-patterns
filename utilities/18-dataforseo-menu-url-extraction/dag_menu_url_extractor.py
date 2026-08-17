"""
Airflow DAG: SEO business-listing → menu URL extraction.

Phase 1 — load & validate:
  check source → country sizes Variable → ensure dest → MERGE →
  re-count → validate establishment / menu_url distribution

Phase 2 — extract:
  per-country TaskGroup: plan NTILE batches → parallel batch_N tasks
  (countries sequential; batches fan out under max_active_tasks)

Phase 3 — refresh Variable of null-menu-url counts for next parse.

Source (read-only):
  dags/etl_dataforseo_menu_url_extractor.py
  dags/horeca_digital/dataforseo_gbq_menu_url_extractor.py
  dags/horeca_digital/dataforseo_menu_url_discovery.py
  dags/horeca_digital/dataforseo_menu_url_utils.py

Distinct from patterns 12/14 (ranked menu-gap *scores*) and 17
(market-data listing export). This job finds navigable menu pages
on restaurant websites.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.models.baseoperator import chain
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from menu_url_extractor import MENU_URL_DEST_SCHEMA, load_config, run_extraction_partition

try:
    from airflow.operators.empty import EmptyOperator
except ModuleNotFoundError:
    from airflow.operators.dummy import DummyOperator as EmptyOperator  # type: ignore

logger = logging.getLogger(__name__)

SOURCE_TABLE = "dwh_project.de.refined_seo_business_listing"
DEST_TABLE = "dwh_project.de.extracted_menu_urls"

ENV = os.environ.get("env", Variable.get("env", default_var="DEV")).upper()
PROJECT_CONFIG = {
    "DEV": {"project_id": "dwh_project_dev", "gcp_conn_id": "bigquery_default_dev"},
    "PROD": {"project_id": "dwh_project", "gcp_conn_id": "bigquery_default"},
}
GCP_PROJECT = PROJECT_CONFIG[ENV]["project_id"]
GCP_CONN_ID = PROJECT_CONFIG[ENV]["gcp_conn_id"]

MAX_BATCH_SLOTS = 25

try:
    COUNTRY_LIST: list[str] = sorted(
        json.loads(
            Variable.get("seo_null_menuurls_per_country", default_var="{}")
        ).keys()
    )
except Exception:
    COUNTRY_LIST = []


def get_bq_client() -> bigquery.Client:
    try:
        from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

        hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID)
        return hook.get_client(project_id=GCP_PROJECT)
    except Exception as exc:
        logger.warning("BigQueryHook unavailable (%s); using ADC.", exc)
        return bigquery.Client(project=GCP_PROJECT)


def parse_table_ref(full_table_id: str) -> tuple:
    parts = full_table_id.strip().split(".")
    if len(parts) != 3:
        raise ValueError(f"Expected project.dataset.table, got: {full_table_id!r}")
    return parts[0], parts[1], parts[2]


def get_incremental_watermark(client: bigquery.Client, context: dict):
    source_cols = {f.name for f in client.get_table(SOURCE_TABLE).schema}
    if "dbt_updated_at" not in source_cols:
        return None
    rows = list(client.query(f"SELECT COUNT(*) AS cnt FROM `{DEST_TABLE}`").result())
    if int(rows[0]["cnt"]) == 0:
        return None
    return context.get("data_interval_start")


def task_check_src_table(**context) -> int:
    client = get_bq_client()
    try:
        client.get_table(SOURCE_TABLE)
    except NotFound as exc:
        raise RuntimeError(f"Source table not found: {SOURCE_TABLE}.") from exc
    rows = list(
        client.query(
            f"SELECT COUNT(DISTINCT establishment_id) AS est_count "
            f"FROM `{SOURCE_TABLE}` WHERE establishment_id IS NOT NULL"
        ).result()
    )
    count = int(rows[0]["est_count"])
    context["ti"].xcom_push(key="source_est_count", value=count)
    return count


def task_get_countries_with_null_menuurls(**context) -> dict:
    raw = Variable.get("seo_null_menuurls_per_country", default_var="{}")
    country_sizes = json.loads(raw)
    if not country_sizes:
        raise RuntimeError(
            "Airflow Variable 'seo_null_menuurls_per_country' is empty. "
            "Run update_variable_null_menuurls once first."
        )
    context["ti"].xcom_push(key="countries", value=sorted(country_sizes.keys()))
    context["ti"].xcom_push(key="country_sizes", value=country_sizes)
    return country_sizes


def task_check_or_create_dest_table(**context) -> str:
    client = get_bq_client()
    project_id, dataset_id, table_id = parse_table_ref(DEST_TABLE)
    dataset_ref = bigquery.DatasetReference(project_id, dataset_id)
    table_ref = dataset_ref.table(table_id)
    try:
        existing = client.get_table(table_ref)
        new_fields = [
            f for f in MENU_URL_DEST_SCHEMA if f.name not in {c.name for c in existing.schema}
        ]
        if new_fields:
            existing.schema = list(existing.schema) + new_fields
            client.update_table(existing, ["schema"])
        return "table_already_existed"
    except NotFound:
        pass
    try:
        client.get_dataset(dataset_ref)
    except NotFound:
        client.create_dataset(bigquery.Dataset(dataset_ref), exists_ok=True)
    client.create_table(bigquery.Table(table_ref, schema=MENU_URL_DEST_SCHEMA), exists_ok=True)
    return "table_created"


def task_load_source_to_destination(**context) -> dict:
    client = get_bq_client()
    watermark = get_incremental_watermark(client, context)
    if watermark is not None:
        where_clause = f"WHERE dbt_updated_at >= TIMESTAMP('{watermark.isoformat()}')"
        load_mode = "incremental"
    else:
        where_clause = ""
        load_mode = "full"

    merge_sql = f"""
    MERGE `{DEST_TABLE}` AS T
    USING (
      SELECT
        ABS(FARM_FINGERPRINT(CONCAT(
            COALESCE(CAST(establishment_id AS STRING), ''), '|',
            COALESCE(CAST(website AS STRING), ''), '|',
            COALESCE(CAST(menu_url AS STRING), '')
        ))) AS menu_url_id,
        CAST(restaurant_name AS STRING) AS restaurant_name,
        CAST(places_id AS STRING) AS places_id,
        CAST(country AS STRING) AS country,
        CAST(first_seen AS TIMESTAMP) AS first_seen,
        CAST(establishment_id AS INT64) AS establishment_id,
        CAST(website AS STRING) AS website,
        CAST(menu_url AS STRING) AS menu_url
      FROM `{SOURCE_TABLE}`
      {where_clause}
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY ABS(FARM_FINGERPRINT(CONCAT(
            COALESCE(CAST(establishment_id AS STRING), ''), '|',
            COALESCE(CAST(website AS STRING), ''), '|',
            COALESCE(CAST(menu_url AS STRING), '')
        )))
        ORDER BY first_seen DESC NULLS LAST
      ) = 1
    ) AS S
    ON T.menu_url_id = S.menu_url_id
    WHEN MATCHED THEN UPDATE SET
      T.restaurant_name = S.restaurant_name,
      T.places_id = S.places_id,
      T.country = S.country,
      T.first_seen = S.first_seen,
      T.establishment_id = S.establishment_id,
      T.website = S.website,
      T.menu_url = S.menu_url,
      T._update_ts = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
      menu_url_id, restaurant_name, places_id, country, first_seen,
      establishment_id, website, menu_url, _extraction_complete, _create_ts, _update_ts
    ) VALUES (
      S.menu_url_id, S.restaurant_name, S.places_id, S.country, S.first_seen,
      S.establishment_id, S.website, S.menu_url, FALSE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
    )
    """
    job = client.query(merge_sql)
    job.result()
    result = {
        "load_mode": load_mode,
        "watermark": str(watermark) if watermark else None,
        "rows_affected": job.num_dml_affected_rows or 0,
    }
    context["ti"].xcom_push(key="load_result", value=result)
    return result


def task_check_dest_table(**context) -> int:
    client = get_bq_client()
    dest_est_count = int(
        list(
            client.query(
                f"SELECT COUNT(DISTINCT establishment_id) AS est_count "
                f"FROM `{DEST_TABLE}` WHERE establishment_id IS NOT NULL"
            ).result()
        )[0]["est_count"]
    )
    context["ti"].xcom_push(key="dest_est_count", value=dest_est_count)
    return dest_est_count


def task_validate_counts(**context) -> dict:
    ti = context["ti"]
    client = get_bq_client()
    source_est = int(ti.xcom_pull(task_ids="check_src_table", key="return_value") or 0)
    dest_est = int(ti.xcom_pull(task_ids="check_dest_table", key="return_value") or 0)
    if source_est != dest_est:
        raise RuntimeError(
            f"Distinct establishment mismatch — source: {source_est}, dest: {dest_est}."
        )

    distribution_query = """
        SELECT country, COUNT(DISTINCT menu_url) AS menu_url_count
        FROM `{table}`
        WHERE country IS NOT NULL AND menu_url IS NOT NULL
        GROUP BY country
        ORDER BY country ASC
    """
    source_dist = {
        row["country"]: int(row["menu_url_count"])
        for row in client.query(distribution_query.format(table=SOURCE_TABLE)).result()
    }
    dest_dist = {
        row["country"]: int(row["menu_url_count"])
        for row in client.query(distribution_query.format(table=DEST_TABLE)).result()
    }
    mismatches = {
        c: {"source": source_dist.get(c, 0), "dest": dest_dist.get(c, 0)}
        for c in source_dist
        if dest_dist.get(c, 0) < source_dist.get(c, 0)
    }
    if mismatches:
        raise RuntimeError(f"Menu URL distribution mismatch: {mismatches}.")

    result = {
        "check1_source_establishments": source_est,
        "check1_dest_establishments": dest_est,
        "check2_countries_checked": len(source_dist),
        "validation_status": "passed",
    }
    ti.xcom_push(key="validation_result", value=result)
    return result


def plan_country_batches_callable(country_name: str, **context) -> dict:
    ti = context["ti"]
    countries = ti.xcom_pull(task_ids="get_countries_with_null_menuurls", key="countries") or []
    country_sizes = (
        ti.xcom_pull(task_ids="get_countries_with_null_menuurls", key="country_sizes") or {}
    )
    if country_name not in countries:
        return {"country": country_name, "num_batches": 0, "row_count": 0, "batch_size": 0}
    row_count = country_sizes.get(country_name, 0)
    num_batches = min(MAX_BATCH_SLOTS, max(1, row_count))
    batch_size = (row_count + num_batches - 1) // num_batches
    return {
        "country": country_name,
        "num_batches": num_batches,
        "row_count": row_count,
        "batch_size": batch_size,
    }


def run_country_batch_callable(country_name: str, batch_idx: int, **context) -> int:
    ti = context["ti"]
    plan = ti.xcom_pull(task_ids=f"country_{country_name}.plan_batches", key="return_value") or {}
    country = plan.get("country")
    num_batches = int(plan.get("num_batches", 0))
    if not country or batch_idx > num_batches:
        return 0
    cfg = load_config(source_table=SOURCE_TABLE, dest_table=DEST_TABLE)
    return run_extraction_partition(
        batch_idx, num_batches, cfg=cfg, country_filter=country
    )


def task_update_variable_null_menuurls(**context) -> None:
    client = get_bq_client()
    rows = list(
        client.query(
            f"SELECT country, COUNT(*) AS null_menu_url_count "
            f"FROM `{SOURCE_TABLE}` "
            f"WHERE country IS NOT NULL AND menu_url IS NULL "
            f"GROUP BY country ORDER BY country ASC"
        ).result()
    )
    country_sizes = {row["country"]: int(row["null_menu_url_count"]) for row in rows}
    Variable.set("seo_null_menuurls_per_country", json.dumps(country_sizes))


default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email": ["dataops@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}

dag = DAG(
    dag_id="etl_seo_menu_url_extractor",
    default_args=default_args,
    description="Extract menu URLs from SEO business listings into BigQuery",
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=5,
    dagrun_timeout=timedelta(hours=72),
    tags=["bigquery", "seo", "extraction", "menu"],
    doc_md=__doc__,
)

start = EmptyOperator(task_id="start", dag=dag)
end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE, dag=dag)

t1 = PythonOperator(
    task_id="check_src_table", python_callable=task_check_src_table, dag=dag
)
t2 = PythonOperator(
    task_id="get_countries_with_null_menuurls",
    python_callable=task_get_countries_with_null_menuurls,
    dag=dag,
)
t3 = PythonOperator(
    task_id="check_or_create_dest_table",
    python_callable=task_check_or_create_dest_table,
    dag=dag,
)
t4 = PythonOperator(
    task_id="load_source_to_destination",
    python_callable=task_load_source_to_destination,
    dag=dag,
)
t5 = PythonOperator(
    task_id="check_dest_table", python_callable=task_check_dest_table, dag=dag
)
t6 = PythonOperator(
    task_id="validate_counts", python_callable=task_validate_counts, dag=dag
)
t8 = PythonOperator(
    task_id="update_variable_null_menuurls",
    python_callable=task_update_variable_null_menuurls,
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
)

country_groups = []
for country_name in COUNTRY_LIST:
    with TaskGroup(group_id=f"country_{country_name}", dag=dag) as country_tg:
        plan_task = PythonOperator(
            task_id="plan_batches",
            python_callable=plan_country_batches_callable,
            op_kwargs={"country_name": country_name},
            dag=dag,
        )
        batch_tasks = [
            PythonOperator(
                task_id=f"batch_{b_idx}",
                python_callable=run_country_batch_callable,
                op_kwargs={"country_name": country_name, "batch_idx": b_idx},
                trigger_rule=TriggerRule.ALL_DONE,
                dag=dag,
            )
            for b_idx in range(1, MAX_BATCH_SLOTS + 1)
        ]
        chain(plan_task, batch_tasks)
    country_groups.append(country_tg)

chain(start, t1, t2, t3, t4, t5, t6)
if country_groups:
    chain(*country_groups)
    chain(t6, country_groups[0])
    chain(country_groups[-1], t8)
else:
    chain(t6, t8)
chain(t8, end)
