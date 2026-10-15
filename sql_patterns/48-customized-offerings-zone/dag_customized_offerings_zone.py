"""Offer Tool zone — weekday-aware multi-project stage fan-out.

Composer DAG that WRITE_TRUNCATEs warehouse refined / trusted tables
into per-stage Offer Tool GCP projects so the field-sales product can
read a stable dataset contract without querying the DWH directly.

Weekday cost control (prod Composer):
  - Wednesday (ISO weekday 3): fan-out to acc + stg + prod
  - Other weekdays: prod only
DEV Composer short-circuits to Mondays only.

Distinct from pattern 27 (Cloud SQL → SCD Type 2 ingest of the Offer
Tool OLTP) and pattern 46 (Food Graph refined zone inside the DWH).
This DAG *publishes* DWH outputs into product-owned projects.

Source (read-only):
  dags/etl_customized_offering_zone.py
  dags/horeca_digital/customized_offering_queries.py
"""

from __future__ import annotations

import os
from datetime import date, datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from google.cloud import bigquery

import zone_queries as queries

DWH_PROJECT = os.getenv("PROJECT", "dwh_project")
ZONE_DATASET = "offer_tool_zone"

# Stage → product GCP project. Prod widens on Wednesdays.
STAGE_PROJECTS = {
    "dev": "offer-tool-dev",
    "acc": "offer-tool-acc",
    "stg": "offer-tool-stg",
    "prod": "offer-tool-prod",
}

ASSORTMENT_COUNTRIES = [
    "AT", "DE", "FR", "HR", "HU", "NL", "PL", "PT", "RO", "ES", "IT",
]
BENCHMARK_COUNTRIES = [
    "DE", "FR", "HR", "HU", "NL", "PL", "PT", "RO", "ES", "IT", "TR", "UA",
]
SCORE_COUNTRIES = [
    "DE", "FR", "CZ", "ES", "HR", "HU", "IT", "NL", "PL", "PT", "RO", "SK", "TR",
]
ARTICLE_REC_COUNTRIES = ["DE", "PL", "FR", "HR", "NL", "PT", "ES"]
ARTICLE_RECOMMENDER_COUNTRIES = ["PL", "DE", "PT"]


def resolve_stages(dwh_project: str, today: date | None = None) -> list[tuple[str, str]]:
    """Return (stage, project_id) pairs for this run.

    Cost lever: non-Wednesday prod runs skip acc/stg full rebuilds.
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


def monday_only_for_dev(**_context) -> bool:
    """Short-circuit: DEV runs Mondays only; prod always continues."""
    if DWH_PROJECT.endswith("-dev") or DWH_PROJECT == "dwh_project_dev":
        return date.today().isoweekday() == 1
    return True


def fg_env_for_stage(stage: str) -> str:
    """Trusted Food Graph table suffix: dev stage → _dev, else _acc."""
    return "dev" if stage == "dev" else "acc"


stage_and_project = resolve_stages(DWH_PROJECT)
env_suffix = "_dev" if stage_and_project == [("dev", STAGE_PROJECTS["dev"])] else ""
gcp_conn_id = (
    "bigquery_default_dev"
    if env_suffix
    else "bigquery_default"
)

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "start_date": datetime(2021, 6, 29),
    "email": ["dataops@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

dag = DAG(
    dag_id=f"etl_customized_offerings_zone{env_suffix}",
    default_args=default_args,
    schedule_interval=None,  # production: often triggered / aligned to refined zone
    max_active_runs=1,
    catchup=False,
    tags=["etl", "offer-tool", "zone", "multi-project"],
    doc_md=__doc__,
)

monday_gate = ShortCircuitOperator(
    task_id="monday_only_for_dev",
    python_callable=monday_only_for_dev,
    dag=dag,
)


def _bq_truncate(
    task_id: str,
    sql: str,
    project_id: str,
    table: str,
    *,
    time_partitioning: dict | None = None,
    clustering: dict | None = None,
) -> BigQueryInsertJobOperator:
    query_cfg: dict = {
        "query": sql,
        "useLegacySql": False,
        "destinationTable": {
            "projectId": project_id,
            "datasetId": ZONE_DATASET,
            "tableId": table,
        },
        "writeDisposition": "WRITE_TRUNCATE",
        "createDisposition": "CREATE_IF_NEEDED",
    }
    if time_partitioning:
        query_cfg["timePartitioning"] = time_partitioning
    if clustering:
        query_cfg["clustering"] = clustering
    return BigQueryInsertJobOperator(
        task_id=task_id,
        configuration={"query": query_cfg},
        gcp_conn_id=gcp_conn_id,
        dag=dag,
    )


# --- Nested vs unnested gaps reconciliation (prod monitoring) -------------

fg_gaps_validation = BigQueryInsertJobOperator(
    task_id="fg_gaps_validation",
    configuration={
        "query": {
            "query": f"""
            WITH nested_gaps AS (
                SELECT iso_code, prediction_type,
                       COUNT(DISTINCT establishment_id) AS establishment_count_nested_gaps
                FROM `{STAGE_PROJECTS['prod']}.{ZONE_DATASET}.fg_gaps`
                WHERE prediction_type IS NOT NULL
                  AND gap_ingredients != "[]"
                GROUP BY 1, 2
            ),
            unnested_gaps AS (
                SELECT iso_code, prediction_type,
                       COUNT(DISTINCT establishment_id) AS establishment_count_unnested_gaps
                FROM `{DWH_PROJECT}.trusted.fg_gaps_unnested_acc`
                WHERE prediction_type IS NOT NULL
                GROUP BY 1, 2
            )
            SELECT iso_code, prediction_type,
                   establishment_count_unnested_gaps - establishment_count_nested_gaps AS dif
            FROM nested_gaps
            JOIN unnested_gaps USING (iso_code, prediction_type)
            WHERE establishment_count_nested_gaps != establishment_count_unnested_gaps
            ORDER BY iso_code, prediction_type
            """,
            "useLegacySql": False,
            "destinationTable": {
                "projectId": DWH_PROJECT,
                "datasetId": "monitoring",
                "tableId": "offer_tool_fg_gaps_test",
            },
            "writeDisposition": "WRITE_TRUNCATE",
            "createDisposition": "CREATE_IF_NEEDED",
        }
    },
    gcp_conn_id=gcp_conn_id,
    dag=dag,
)


def alert_fg_gaps_diff(**_context) -> None:
    """Surface nested/unnested count diffs. Production used a webhook;
    portfolio version logs rows for operators to pick up."""
    client = bigquery.Client(project=DWH_PROJECT)
    rows = list(
        client.query(
            f"SELECT * FROM `{DWH_PROJECT}.monitoring.offer_tool_fg_gaps_test`"
        ).result()
    )
    for row in rows:
        # TODO: wire AlertManager / PagerDuty / Slack webhook via Connection
        print(
            f"fg_gaps delta iso={row.iso_code} type={row.prediction_type} dif={row.dif}"
        )


fg_gaps_alert = PythonOperator(
    task_id="fg_gaps_alert",
    python_callable=alert_fg_gaps_diff,
    dag=dag,
)

# --- Stage fan-out: Food Graph gaps + article recommendation branch -------

article_recommendation_branch_by_stage: dict[str, BigQueryInsertJobOperator] = {}

for stage, project in stage_and_project:
    fg_env = fg_env_for_stage(stage)

    fg_gaps = _bq_truncate(
        f"{stage}_fg_gaps",
        queries.fg_gaps_unnested_query(fg_env),
        project,
        "fg_gaps",
    )
    fg_gaps >> fg_gaps_validation >> fg_gaps_alert

    article_recommendation_branch = _bq_truncate(
        f"article_recommendation_branch_{stage}",
        queries.article_recommendation_branch_query(fg_env),
        project,
        "article_recommendation_branch",
    )
    article_recommendation_branch_by_stage[stage] = article_recommendation_branch

# --- Country × stage: assortment, masterdata, visit, article --------------

for iso_code in ASSORTMENT_COUNTRIES:
    assortment_country = "DE" if iso_code == "AT" else iso_code
    for stage, project in stage_and_project:
        _bq_truncate(
            f"{stage}_wholesale_assortment_{iso_code}",
            queries.wholesale_assortment_single_country(iso_code=assortment_country),
            project,
            f"wholesale_assortment_{iso_code}",
            clustering={"fields": ["art_no"]},
        )

        _bq_truncate(
            f"{stage}_wholesale_masterdata_{iso_code}",
            f"""SELECT * FROM `{DWH_PROJECT}.refined.wholesale_masterdata_{iso_code}`
                WHERE ({queries.exclude_deleted_statement(field='unique_wholesale_id', iso_code=iso_code)})
                  AND ({queries.exclude_deleted_statement(field='wholesale_id', iso_code=iso_code)})""",
            project,
            f"wholesale_masterdata_{iso_code}",
            clustering={"fields": ["wholesale_id", "unique_wholesale_id"]},
        )

        _bq_truncate(
            f"{stage}_wholesale_analytics_visit_{iso_code}",
            f"""SELECT * FROM `{DWH_PROJECT}.refined_foodgraph.wholesale_analytics_visit_{iso_code}`
                WHERE {queries.exclude_deleted_statement(field='wholesale_id', iso_code=iso_code)}""",
            project,
            f"wholesale_analytics_visit_{iso_code}",
            time_partitioning={"type": "DAY", "field": "date"},
            clustering={"fields": ["wholesale_id"]},
        )

        _bq_truncate(
            f"{stage}_wholesale_analytics_article_{iso_code}",
            f"""SELECT * FROM `{DWH_PROJECT}.refined_foodgraph.wholesale_analytics_article_{iso_code}`
                WHERE {queries.exclude_deleted_statement(field='wholesale_id', iso_code=iso_code)}""",
            project,
            f"wholesale_analytics_article_{iso_code}",
            time_partitioning={"type": "DAY", "field": "last_purchase_date"},
            clustering={"fields": ["wholesale_id", "article_id", "article_mikg_id"]},
        )

# --- Benchmarking gaps / topsellers (gated by Monday ShortCircuit in DEV) -

for iso_code in BENCHMARK_COUNTRIES:
    for stage, project in stage_and_project:
        stage_token = project.split("-")[-1]
        gaps = _bq_truncate(
            f"{stage_token}_benchmarking_gaps_{iso_code}",
            f"""SELECT unique_wholesale_id, establishment_id, last_transaction,
                       segment, segment_column, segment_id, customer_potential_category,
                       LTRIM(RTRIM(TO_JSON_STRING(benchmarking_gaps), "]"), "[") AS benchmarking_gaps,
                       _create_ts, _keyhash, _rowhash, _valid_from, _valid_until, _valid_flag
                FROM `{DWH_PROJECT}.refined.benchmarking_gaps_{iso_code}`
                WHERE {queries.exclude_deleted_statement(field='unique_wholesale_id', iso_code=iso_code)}
                  AND _valid_flag""",
            project,
            f"benchmarking_gaps_{iso_code}",
            time_partitioning={"type": "DAY", "field": "last_transaction"},
            clustering={"fields": ["unique_wholesale_id", "segment_column"]},
        )
        topsellers = _bq_truncate(
            f"{stage_token}_benchmarking_topsellers_{iso_code}",
            f"SELECT * FROM `{DWH_PROJECT}.refined.benchmarking_topsellers_{iso_code}` WHERE _valid_flag",
            project,
            f"benchmarking_topsellers_{iso_code}",
        )
        monday_gate >> gaps
        monday_gate >> topsellers

for stage, project in stage_and_project:
    fg_env = fg_env_for_stage(stage)
    stage_token = project.split("-")[-1]
    skeletons = _bq_truncate(
        f"{stage_token}_benchmarking_gaps_skeletons_{fg_env}",
        f"""SELECT iso_code, segment, segment_column, segment_id,
                   LTRIM(RTRIM(TO_JSON_STRING(benchmarking_gaps), "]"), "[") AS benchmarking_gaps,
                   _create_ts, _valid_from, _valid_until, _valid_flag, _keyhash, _rowhash
            FROM `{DWH_PROJECT}.refined.benchmarking_gaps_skeletons_*`
            WHERE _valid_flag""",
        project,
        "benchmarking_gaps_skeletons",
        clustering={"fields": ["iso_code", "segment"]},
    )
    monday_gate >> skeletons

# --- FBO / NBO scores -----------------------------------------------------

for iso_code in SCORE_COUNTRIES:
    for stage, project in stage_and_project:
        _bq_truncate(
            f"{stage}_fbo_scores_{iso_code}",
            queries.fbo_scores_export(iso_code),
            project,
            f"fbo_nbo_scores_{iso_code}",
        )

# --- Article recommendation (depends on branch table per stage) -----------

for iso_code in ARTICLE_REC_COUNTRIES:
    for stage, project in stage_and_project:
        branch = article_recommendation_branch_by_stage[stage]
        country_rec = _bq_truncate(
            f"{stage}_article_recommendation_{iso_code}",
            queries.article_recommendation_query(
                iso_code,
                f"`{project}.{ZONE_DATASET}.article_recommendation_branch`",
            ),
            project,
            f"article_recommendation_{iso_code}",
        )
        branch >> country_rec

# --- Article recommender (PL/DE/PT dense markets) -------------------------

for iso_code in ARTICLE_RECOMMENDER_COUNTRIES:
    for stage, project in stage_and_project:
        _bq_truncate(
            f"{stage}_article_recommender_{iso_code}",
            queries.article_recommender_query(iso_code, stage),
            project,
            f"article_recommender_{iso_code}",
            time_partitioning={"type": "DAY", "field": "creation_date"},
            clustering={"fields": ["establishment_id", "ingredient_name"]},
        )
