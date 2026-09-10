"""Food Graph ML propagation DAG.

Daily Composer job that:

1. Builds gold Food Graph reference tables in the DWH trusted dataset.
2. Copies Vertex preprocessed ML tables for ``dev`` and ``acc`` stages.
3. Gates optional acc-promotion tasks behind a ShortCircuit (off by default).
4. On the day before month-end, ranks menu gaps per country and copies
   a fixed column contract into the recommender project partition.
5. Refreshes payment-wallet match results and kicks a dbt Cloud job.

Source (read-only):
  dags/etl_foodgraph.py
  dags/horeca_digital/foodgraph_queries.py

Distinct from pattern 02 (POS name classification), pattern 12
(downstream ranked-gaps Avro export), and pattern 17 (market-data
export). This DAG is the multi-project ML landing + ranking spine.
"""

from __future__ import annotations

import calendar
from datetime import date, datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

import foodgraph_queries as queries

try:
    from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
except ImportError:  # pragma: no cover - reference stub
    DbtCloudRunJobOperator = None


VERTEX_PROJECT = Variable.get("foodgraph_vertex_project", default_var="vertex_ml_project")
DWH_PROJECT = Variable.get("foodgraph_dwh_project", default_var="dwh_project")
REX_PROJECT = Variable.get("foodgraph_rex_project", default_var="recommender_rex_project")
TRUSTED = "trusted"
TRUSTED_STAGING = "trusted_staging"
REFINED = "refined"

try:
    DBT_JOB_ID = int(Variable.get("foodgraph_match_result_dbt_job_id"))
except Exception:
    DBT_JOB_ID = None

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "start_date": datetime(2020, 7, 12),
    "email": ["dataops@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "dbt_cloud_conn_id": "dbt_conn",
    "account_id": 1,
}

dag = DAG(
    dag_id="etl_foodgraph",
    default_args=default_args,
    schedule_interval="30 3 * * *",
    max_active_runs=1,
    catchup=False,
    tags=["etl", "foodgraph", "ml", "vertex", "rex"],
    doc_md=__doc__,
)


def _bq_truncate(task_id, sql, project, dataset, table, **kwargs):
    return BigQueryInsertJobOperator(
        task_id=task_id,
        configuration={
            "query": {
                "query": sql,
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": project,
                    "datasetId": dataset,
                    "tableId": table,
                },
                "writeDisposition": "WRITE_TRUNCATE",
                "createDisposition": "CREATE_IF_NEEDED",
            }
        },
        gcp_conn_id="bigquery_default",
        dag=dag,
        **kwargs,
    )


# Phase A — gold reference tables
gold_ingredients_translation = _bq_truncate(
    "gold_ingredients_translation",
    queries.gold_ingredients_translation_query,
    DWH_PROJECT,
    TRUSTED,
    "fg_ingredients_translations",
)
gold_ingredient_ingredients = _bq_truncate(
    "gold_ingredient_ingredients",
    queries.gold_ingredient_ingredients_query,
    DWH_PROJECT,
    TRUSTED,
    "fg_ingredients",
)
gold_ingredients_synonyms = _bq_truncate(
    "gold_ingredients_synonyms",
    queries.gold_ingredients_synonyms_query,
    DWH_PROJECT,
    TRUSTED,
    "fg_ingredients_synonyms",
)
menu_items_drink_classification = _bq_truncate(
    "menu_items_drink_classification",
    queries.menu_items_drink_classification,
    DWH_PROJECT,
    TRUSTED,
    "fg_menu_items_drink_classification",
)

# Phase B — Vertex → DWH for dev + acc
STAGES = [
    ("dev", "foodgraph_dev_preprocessed"),
    ("acc", "foodgraph_acc_preprocessed"),
]

fg_gaps_unnested_dev = None

for stage, fg_dataset in STAGES:
    gaps = _bq_truncate(
        f"fg_gaps_data_unnested_load_{stage}",
        queries.gaps_unnested_query(fg_dataset),
        DWH_PROJECT,
        TRUSTED,
        f"fg_gaps_unnested_{stage}",
        trigger_rule="all_done",
    )
    if stage == "dev":
        fg_gaps_unnested_dev = gaps

    _bq_truncate(
        f"fg_articles_to_ingredients_{stage}",
        f"SELECT * FROM `{VERTEX_PROJECT}.{fg_dataset}.articles_to_ingredients`",
        DWH_PROJECT,
        TRUSTED,
        f"fg_articles_to_ingredients_{stage}",
        trigger_rule="all_done",
    )
    _bq_truncate(
        f"fg_ingredients_translations__{stage}",
        f"SELECT * FROM `{VERTEX_PROJECT}.{fg_dataset}.ingredients_translations`",
        DWH_PROJECT,
        TRUSTED,
        f"fg_ingredients_translations_{stage}",
        trigger_rule="all_done",
    )
    _bq_truncate(
        f"fg_ingredients_synonyms_{stage}",
        f"SELECT * FROM `{VERTEX_PROJECT}.{fg_dataset}.ingredients_synonyms`",
        DWH_PROJECT,
        TRUSTED,
        f"fg_ingredients_synonyms_{stage}",
        trigger_rule="all_done",
    )
    _bq_truncate(
        f"fg_ingredients_{stage}",
        f"SELECT * FROM `{VERTEX_PROJECT}.{fg_dataset}.ingredients`",
        DWH_PROJECT,
        TRUSTED,
        f"fg_ingredients_{stage}",
        trigger_rule="all_done",
    )
    _bq_truncate(
        f"fg_synthetic_menu_{stage}",
        f"SELECT * FROM `{VERTEX_PROJECT}.{fg_dataset}.synthetic_menus_llm_mc_combined`",
        DWH_PROJECT,
        TRUSTED,
        f"fg_synthetic_menus_llm_mc_combined_{stage}",
        trigger_rule="all_done",
    )


# Phase C — on-demand acc promotion
def ondemand(**_context) -> bool:
    return Variable.get("foodgraph_ondemand_enabled", default_var="false").lower() == "true"


on_demand = ShortCircuitOperator(
    task_id="on_demand",
    python_callable=ondemand,
    dag=dag,
)

menu_items_validity_data_load = _bq_truncate(
    "menu_items_validity_data_load_acc",
    f"SELECT * FROM `{VERTEX_PROJECT}.foodgraph_dev_preprocessed.menu_items_validity`",
    VERTEX_PROJECT,
    "foodgraph_acc_preprocessed",
    "menu_items_validity",
    trigger_rule="all_done",
)
llm_ingredients_for_synthetic_menus_data_load = _bq_truncate(
    "llm_ingredients_for_synthetic_menus_data_load_acc",
    f"SELECT * FROM `{VERTEX_PROJECT}.foodgraph_dev_preprocessed.llm_ingredients_for_synthetic_menus`",
    VERTEX_PROJECT,
    "foodgraph_acc_preprocessed",
    "llm_ingredients_for_synthetic_menus",
    trigger_rule="all_done",
)
menu_items_parsed_data_load = _bq_truncate(
    "menu_items_parsed_data_load_acc",
    f"SELECT * FROM `{VERTEX_PROJECT}.foodgraph_dev_preprocessed.menu_items_parsed`",
    VERTEX_PROJECT,
    "foodgraph_acc_preprocessed",
    "menu_items_parsed",
    trigger_rule="all_done",
)

# Production also had orphaned LLM / synthetic-append tasks. Left out;
# wire them under on_demand if you need that path.
on_demand >> [
    menu_items_validity_data_load,
    llm_ingredients_for_synthetic_menus_data_load,
    menu_items_parsed_data_load,
]


# Phase D — REX ranked menu gaps (day before month end)
def is_day_before_month_end() -> bool:
    today = date.today()
    last_day = calendar.monthrange(today.year, today.month)[1]
    return today.day == last_day - 1


day_before_month_end = ShortCircuitOperator(
    task_id="day_before_month_end",
    python_callable=is_day_before_month_end,
    dag=dag,
)

# Prefer logical date for backfills; production used parse-time now().
partition_date = "{{ ds_nodash }}"

rex_country_tasks = []
for iso_code in queries.ISOCODE_LIST:
    task = _bq_truncate(
        f"rex_menu_gaps_ranked_data_load_{iso_code}",
        queries.rex_menu_gaps_ranked(iso_code),
        VERTEX_PROJECT,
        "foodgraph_dev_preprocessed",
        f"rex_menu_gaps_ranked_{iso_code}",
        trigger_rule="all_success",
    )
    day_before_month_end >> task
    rex_country_tasks.append(task)

# Explicit column list — SELECT * failed partition writes after source
# gained menu_item_name / department_flag.
rex_menu_gaps_ranked_data_copy = BigQueryInsertJobOperator(
    task_id="rex_menu_gaps_ranked_data_copy",
    configuration={
        "query": {
            "query": f"""
                SELECT
                    wholesale_id,
                    iso_code,
                    establishment_id,
                    ingredient,
                    `type`,
                    menu_type,
                    relevance,
                    branch_desc,
                    article_no,
                    var_tu_key,
                    product_key,
                    CAST(NULL AS STRING) AS article_name,
                    one_year_revenue,
                    created_at,
                    rank_,
                    account_id,
                    person_id,
                    wholesale_cardholder_key,
                    wholesale_customer_key,
                    unique_wholesale_id
                FROM `{VERTEX_PROJECT}.foodgraph_dev_preprocessed.rex_menu_gaps_ranked_*`
            """,
            "useLegacySql": False,
            "destinationTable": {
                "projectId": REX_PROJECT,
                "datasetId": "datazone_wholesale_fr",
                "tableId": f"menu_gaps_ranked${partition_date}",
            },
            "writeDisposition": "WRITE_TRUNCATE",
            "createDisposition": "CREATE_IF_NEEDED",
        }
    },
    gcp_conn_id="bigquery_default",
    trigger_rule="all_success",
    dag=dag,
)

# Production only chained the last country → copy. Fixed: wait on all.
rex_country_tasks >> rex_menu_gaps_ranked_data_copy

for iso_code in queries.NON_WHOLESALE_MENU_GAPS_ACTIVE:
    non_ws = _bq_truncate(
        f"rex_menu_gaps_non_wholesale_data_load_{iso_code}",
        queries.non_wholesale_menu_gaps(iso_code),
        VERTEX_PROJECT,
        "foodgraph_dev_preprocessed",
        f"rex_menu_gaps_non_wholesale_{iso_code}",
        trigger_rule="all_success",
    )
    partner_refresh = _bq_truncate(
        f"partner_rex_menu_gaps_non_wholesale_refresh_{iso_code}",
        f"""
        SELECT *, CURRENT_TIMESTAMP() AS _update_ts
        FROM `{VERTEX_PROJECT}.foodgraph_dev_preprocessed.rex_menu_gaps_non_wholesale_{iso_code}`
        """,
        DWH_PROJECT,
        REFINED,
        f"partner_rex_menu_gaps_non_wholesale_{iso_code.lower()}",
        trigger_rule="all_success",
    )
    if fg_gaps_unnested_dev is not None:
        fg_gaps_unnested_dev >> non_ws >> partner_refresh


# Phase E — payment-wallet match results → dbt
DBT_TASK_ID = "match_result_payment_wallet_dbt"


def get_runids(ti):
    runids = []
    try:
        runids.append(ti.xcom_pull(task_ids=[DBT_TASK_ID], key="return_value")[0])
    except (IndexError, TypeError):
        url = ti.xcom_pull(task_ids=[DBT_TASK_ID], key="job_run_url")
        if url:
            runids.append(int(list(filter(None, url[0].split("/")))[-1]))
    Variable.set(key="etl_foodgraph_dbt_runids", value=runids)
    return runids


match_result_payment_wallet = _bq_truncate(
    "match_result_payment_wallet",
    f"SELECT * FROM `{VERTEX_PROJECT}.matching_engine_prod.match_result_payment_wallet`",
    DWH_PROJECT,
    TRUSTED_STAGING,
    "match_result_payment_wallet",
    trigger_rule="all_success",
)

if DbtCloudRunJobOperator is not None and DBT_JOB_ID is not None:
    match_result_payment_wallet_dbt = DbtCloudRunJobOperator(
        task_id=DBT_TASK_ID,
        job_id=DBT_JOB_ID,
        check_interval=10,
        do_xcom_push=True,
        timeout=1500,
        dag=dag,
    )
else:
    match_result_payment_wallet_dbt = PythonOperator(
        task_id=DBT_TASK_ID,
        python_callable=lambda: {"stub": True, "job_id": DBT_JOB_ID},
        dag=dag,
    )

get_runids_task = PythonOperator(
    task_id="get_runids_task",
    python_callable=get_runids,
    dag=dag,
)

match_result_payment_wallet >> match_result_payment_wallet_dbt >> get_runids_task
