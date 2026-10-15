"""Offer Tool on-demand zone — manual multi-project publish.

Composer DAG with ``schedule_interval=None`` that WRITE_TRUNCATEs a
*lean* subset of warehouse refined / trusted tables into per-stage Offer
Tool GCP projects. Used when product needs a fresh establishments /
search-index / catalog slice without waiting for the weekday-aware
scheduled zone (pattern 48).

Stage list still follows the Wednesday cost policy at DAG parse time:
  - Wednesday (ISO weekday 3): fan-out to acc + stg + prod
  - Other days: prod only
DEV Composer targets the single ``offer-tool-dev`` project.

Distinct from:
  - pattern 27 — Cloud SQL → SCD Type 2 ingest of Offer Tool OLTP
  - pattern 46 — Food Graph refined zone *inside* the DWH
  - pattern 48 — scheduled weekday zone with gaps / assortment / scores

Source (read-only):
  dags/etl_customized_offerings_zone_on_demand.py
  dags/horeca_digital/customized_offering_queries.py
"""

from __future__ import annotations

import os
from datetime import date, datetime, timedelta

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

import zone_queries as queries

DWH_PROJECT = os.getenv("PROJECT", "dwh_project")
ZONE_DATASET = "offer_tool_zone"

STAGE_PROJECTS = {
    "dev": "offer-tool-dev",
    "acc": "offer-tool-acc",
    "stg": "offer-tool-stg",
    "prod": "offer-tool-prod",
}

# Broader footprint: establishments land for every market the product
# can open a card for.
ESTABLISHMENT_COUNTRIES = [
    "CZ", "ES", "IT", "RS", "SK", "TR", "UA",
    "AT", "DE", "FR", "HR", "HU", "NL", "PL", "PT", "RO",
]

# Narrower: search index only where Elasticsearch is wired in product.
ELASTICSEARCH_COUNTRIES = [
    "AT", "DE", "FR", "HR", "HU", "NL", "PL", "PT", "RO", "ES", "IT",
]


def resolve_stages(dwh_project: str, today: date | None = None) -> list[tuple[str, str]]:
    """Return (stage, project_id) pairs for this manual trigger.

    Same weekday lever as pattern 48 so a Wednesday on-demand run does
    not leave acc/stg stale relative to the scheduled publish.
    """
    today = today or date.today()
    if dwh_project.endswith("-dev") or dwh_project == "dwh_project_dev":
        return [("dev", STAGE_PROJECTS["dev"])]
    if today.isoweekday() == 3:  # Wednesday
        return [
            ("acc", STAGE_PROJECTS["acc"]),
            ("stg", STAGE_PROJECTS["stg"]),
            ("prod", STAGE_PROJECTS["prod"]),
        ]
    return [("prod", STAGE_PROJECTS["prod"])]


def fg_env_for_stage(stage: str) -> str:
    """Trusted Food Graph table suffix: dev stage → _dev, else _acc."""
    return "dev" if stage == "dev" else "acc"


def stage_label(project_id: str) -> str:
    """Stable task-id fragment from sanitized project id (offer-tool-prod → prod)."""
    return project_id.rsplit("-", 1)[-1]


stage_and_project = resolve_stages(DWH_PROJECT)
env_suffix = "_dev" if stage_and_project == [("dev", STAGE_PROJECTS["dev"])] else ""

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "start_date": datetime(2023, 10, 25),
    "email": ["dataops@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

dag = DAG(
    dag_id=f"etl_offer_tool_zone_on_demand{env_suffix}",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["offer-tool", "zone", "on-demand"],
)

# --- Path A: per-country establishments (product card base) ----------------
for iso_code in ESTABLISHMENT_COUNTRIES:
    for stage, project in stage_and_project:
        BigQueryInsertJobOperator(
            task_id=f"{stage}_all_establishments_{iso_code}",
            configuration={
                "query": {
                    "query": f"""
SELECT DISTINCT
  est.*,
  cust.has_order_tool,
  cust.order_tool_type,
  cust.order_tool_created_date,
  cust.has_pos,
  cust.pos_created_date,
  cust.has_pay,
  cust.pay_created_date,
  wholesale.establishment_name AS wholesale_name
FROM (
  SELECT *
  FROM `{DWH_PROJECT}.refined.all_establishments_{iso_code}`
  WHERE offer_tool_relevant
    AND data_source = 'all'
) est
LEFT JOIN `{DWH_PROJECT}.refined.platform_customer_base_establishment` cust
  ON est.sfdc_establishment_id = cust.establishment_sfid
LEFT JOIN `{DWH_PROJECT}.refined.all_wholesale_establishments_{iso_code}` wholesale
  ON est.wholesale_id = wholesale.wholesale_id
""",
                    "useLegacySql": False,
                    "destinationTable": {
                        "projectId": project,
                        "datasetId": ZONE_DATASET,
                        "tableId": f"all_establishments_{iso_code}",
                    },
                    "writeDisposition": "WRITE_TRUNCATE",
                    "createDisposition": "CREATE_IF_NEEDED",
                }
            },
            gcp_conn_id="bigquery_default",
            dag=dag,
        )

# --- Path B: stage-level catalog / geo / menu publishes --------------------
for stage, project in stage_and_project:
    fg_env = fg_env_for_stage(stage)
    label = stage_label(project)

    BigQueryInsertJobOperator(
        task_id=f"{stage}_fg_ingredients",
        configuration={
            "query": {
                "query": f"SELECT * FROM `{DWH_PROJECT}.trusted.fg_ingredients_{fg_env}`",
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": project,
                    "datasetId": ZONE_DATASET,
                    "tableId": f"fg_ingredients_{stage}",
                },
                "writeDisposition": "WRITE_TRUNCATE",
                "createDisposition": "CREATE_IF_NEEDED",
            }
        },
        gcp_conn_id="bigquery_default",
        dag=dag,
    )

    BigQueryInsertJobOperator(
        task_id=f"{stage}_fg_ingredients_translations",
        configuration={
            "query": {
                "query": (
                    f"SELECT * FROM `{DWH_PROJECT}.trusted.fg_ingredients_translations_{fg_env}`"
                ),
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": project,
                    "datasetId": ZONE_DATASET,
                    "tableId": f"fg_ingredients_translations_{stage}",
                },
                "writeDisposition": "WRITE_TRUNCATE",
                "createDisposition": "CREATE_IF_NEEDED",
            }
        },
        gcp_conn_id="bigquery_default",
        dag=dag,
    )

    BigQueryInsertJobOperator(
        task_id=f"{stage}_fg_gold_ingredients_images",
        configuration={
            "query": {
                "query": queries.ingredients_images_query(),
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": project,
                    "datasetId": ZONE_DATASET,
                    "tableId": "fg_gold_ingredients_images",
                },
                "writeDisposition": "WRITE_TRUNCATE",
                "createDisposition": "CREATE_IF_NEEDED",
            }
        },
        gcp_conn_id="bigquery_default",
        dag=dag,
    )

    BigQueryInsertJobOperator(
        task_id=f"{stage}_fg_articles_to_ingredients_mapping",
        configuration={
            "query": {
                "query": f"""
SELECT DISTINCT iso_code, art_no, ing_id, proper_name
FROM `{DWH_PROJECT}.trusted.fg_articles_to_ingredients_{fg_env}`
JOIN `{DWH_PROJECT}.trusted.fg_ingredients_translations_{fg_env}`
  USING (iso_code, ing_id)
""",
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": project,
                    "datasetId": ZONE_DATASET,
                    "tableId": "fg_articles_to_ingredients_mapping",
                },
                "writeDisposition": "WRITE_TRUNCATE",
                "createDisposition": "CREATE_IF_NEEDED",
            }
        },
        gcp_conn_id="bigquery_default",
        dag=dag,
    )

    BigQueryInsertJobOperator(
        task_id=f"{stage}_ingredient",
        configuration={
            "query": {
                "query": f"SELECT * FROM `{DWH_PROJECT}.refined.offer_tool_ingredient`",
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": project,
                    "datasetId": ZONE_DATASET,
                    "tableId": "ingredient",
                },
                "writeDisposition": "WRITE_TRUNCATE",
                "createDisposition": "CREATE_IF_NEEDED",
            }
        },
        gcp_conn_id="bigquery_default",
        dag=dag,
    )

    BigQueryInsertJobOperator(
        task_id=f"{stage}_ingredient_recommendation",
        configuration={
            "query": {
                "query": (
                    f"SELECT * FROM `{DWH_PROJECT}.refined.offer_tool_ingredient_recommendation`"
                ),
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": project,
                    "datasetId": ZONE_DATASET,
                    "tableId": "ingredient_recommendation",
                },
                "writeDisposition": "WRITE_TRUNCATE",
                "createDisposition": "CREATE_IF_NEEDED",
            }
        },
        gcp_conn_id="bigquery_default",
        dag=dag,
    )

    BigQueryInsertJobOperator(
        task_id=f"{stage}_zip_code_region_coordinates",
        configuration={
            "query": {
                "query": f"""
SELECT
  iso_code, zip_code, city, feature AS geo_data,
  region_id, region_desc, region_org_id, region_org_desc,
  region_org_area_id, region_org_area_type
FROM `{DWH_PROJECT}.refined.zip_region_wholesale`
""",
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": project,
                    "datasetId": ZONE_DATASET,
                    "tableId": "zip_code_region_coordinates",
                },
                "writeDisposition": "WRITE_TRUNCATE",
                "createDisposition": "CREATE_IF_NEEDED",
            }
        },
        gcp_conn_id="bigquery_default",
        dag=dag,
    )

    BigQueryInsertJobOperator(
        task_id=f"{label}_wholesale_stores",
        configuration={
            "query": {
                "query": f"SELECT * FROM `{DWH_PROJECT}.refined_foodgraph.vwholesale_stores`",
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": project,
                    "datasetId": ZONE_DATASET,
                    "tableId": "wholesale_stores",
                },
                "writeDisposition": "WRITE_TRUNCATE",
                "createDisposition": "CREATE_IF_NEEDED",
            }
        },
        gcp_conn_id="bigquery_default",
        dag=dag,
    )

    BigQueryInsertJobOperator(
        task_id=f"{label}_all_mappings",
        configuration={
            "query": {
                "query": f"""
SELECT * FROM `{DWH_PROJECT}.refined.all_mappings`
WHERE ({queries.exclude_deleted_statement(field='unique_wholesale_id')})
  AND data_source = 'all'
  AND ({queries.exclude_deleted_statement(field='wholesale_id')})
""",
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": project,
                    "datasetId": ZONE_DATASET,
                    "tableId": "all_mappings",
                },
                "writeDisposition": "WRITE_TRUNCATE",
                "createDisposition": "CREATE_IF_NEEDED",
            }
        },
        gcp_conn_id="bigquery_default",
        dag=dag,
    )

    BigQueryInsertJobOperator(
        task_id=f"{stage}_vendor_customer_menu",
        configuration={
            "query": {
                "query": f"""
SELECT * FROM `{DWH_PROJECT}.refined.all_menu_items`
WHERE data_source = 'vendor_menu'
""",
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": project,
                    "datasetId": ZONE_DATASET,
                    "tableId": "vendor_customer_menu",
                },
                "writeDisposition": "WRITE_TRUNCATE",
                "createDisposition": "CREATE_IF_NEEDED",
            }
        },
        gcp_conn_id="bigquery_default",
        dag=dag,
    )

    BigQueryInsertJobOperator(
        task_id=f"{label}_website_builder_menus",
        configuration={
            "query": {
                "query": f"SELECT * FROM `{DWH_PROJECT}.refined.vwebsite_builder_menus`",
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": project,
                    "datasetId": ZONE_DATASET,
                    "tableId": "website_builder_menus",
                },
                "writeDisposition": "WRITE_TRUNCATE",
                "createDisposition": "CREATE_IF_NEEDED",
            }
        },
        gcp_conn_id="bigquery_default",
        dag=dag,
    )

    BigQueryInsertJobOperator(
        task_id=f"{label}_data_tool_recommendation",
        configuration={
            "query": {
                "query": (
                    f"SELECT * FROM `{DWH_PROJECT}.refined_foodgraph.vdata_tool_recommendation`"
                ),
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": project,
                    "datasetId": ZONE_DATASET,
                    "tableId": "data_tool_recommendation",
                },
                "writeDisposition": "WRITE_TRUNCATE",
                "createDisposition": "CREATE_IF_NEEDED",
            }
        },
        gcp_conn_id="bigquery_default",
        dag=dag,
    )

    BigQueryInsertJobOperator(
        task_id=f"{stage}_all_menu_items",
        configuration={
            "query": {
                "query": f"""
SELECT * FROM `{DWH_PROJECT}.refined.all_menu_items`
WHERE data_source = 'all'
""",
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": project,
                    "datasetId": ZONE_DATASET,
                    "tableId": "all_menu_items",
                },
                "writeDisposition": "WRITE_TRUNCATE",
                "createDisposition": "CREATE_IF_NEEDED",
            }
        },
        gcp_conn_id="bigquery_default",
        dag=dag,
    )

# --- Path C: Elasticsearch search projection (narrower country set) --------
for iso_code in ELASTICSEARCH_COUNTRIES:
    for stage, project in stage_and_project:
        BigQueryInsertJobOperator(
            task_id=f"{stage}_elasticsearch_data_{iso_code}",
            configuration={
                "query": {
                    "query": queries.elasticsearch_data_query(stage, iso_code),
                    "useLegacySql": False,
                    "destinationTable": {
                        "projectId": project,
                        "datasetId": ZONE_DATASET,
                        "tableId": f"elasticsearch_data_{iso_code}",
                    },
                    "writeDisposition": "WRITE_TRUNCATE",
                    "createDisposition": "CREATE_IF_NEEDED",
                }
            },
            gcp_conn_id="bigquery_default",
            dag=dag,
        )
