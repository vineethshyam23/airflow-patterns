"""Food Graph refined analytics zone — multi-country BQ fan-out / fan-in.

Daily Composer DAG that materializes the ``refined_foodgraph`` dataset:

1. Per-country chains (×16): masterdata → assortments → txn_for_analytics
   → article / visit / branch_topseller in parallel.
2. Fan-in to global UNION ALL tables.
3. ``loop1`` DummyOperator barrier, then DAY-partitioned + clustered
   establishment transaction history for a subset of markets.
4. Additional global materializations (earliest visit, topseller, COP feed).

Distinct from pattern 40 (cross-project ML gold / ranked-gaps copy) and
pattern 45 (Vertex PipelineJob submit). This DAG owns the *refined
analytics layer* that ML and customized-offering zones consume.

Source (read-only):
  dags/etl_refined_foodgraph_zone.py
  dags/horeca_digital/foodgraph_queries.py
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.task_group import TaskGroup

import foodgraph_refined_queries as queries

DWH_PROJECT = Variable.get("foodgraph_dwh_project", default_var="dwh_project")
REFINED_FG = "refined_foodgraph"
# Optional night-ETL reservation path (production pinned jobs here).
BQ_RESERVATION = Variable.get(
    "foodgraph_bq_reservation",
    default_var="",
)

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "start_date": datetime(2021, 9, 16),
    "email": ["dataops@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

dag = DAG(
    dag_id="etl_refined_foodgraph_zone",
    default_args=default_args,
    schedule_interval="45 5 * * *",
    max_active_runs=1,
    catchup=False,
    tags=["etl", "foodgraph", "refined", "multi-country"],
    doc_md=__doc__,
)


def _bq_truncate(
    task_id: str,
    sql: str,
    table: str,
    *,
    time_partitioning: dict | None = None,
    clustering: dict | None = None,
    task_group: TaskGroup | None = None,
    **kwargs,
) -> BigQueryInsertJobOperator:
    query_cfg: dict = {
        "query": sql,
        "useLegacySql": False,
        "destinationTable": {
            "projectId": DWH_PROJECT,
            "datasetId": REFINED_FG,
            "tableId": table,
        },
        "writeDisposition": "WRITE_TRUNCATE",
        "createDisposition": "CREATE_IF_NEEDED",
    }
    if time_partitioning:
        query_cfg["timePartitioning"] = time_partitioning
    if clustering:
        query_cfg["clustering"] = clustering

    configuration: dict = {"query": query_cfg}
    if BQ_RESERVATION:
        configuration["reservation"] = BQ_RESERVATION

    return BigQueryInsertJobOperator(
        task_id=task_id,
        configuration=configuration,
        gcp_conn_id="bigquery_default",
        dag=dag,
        task_group=task_group,
        **kwargs,
    )


# Sync barrier: all country txn tasks must finish before partitioned loads.
loop1 = EmptyOperator(task_id="loop1_done", dag=dag)

# Global fan-in destinations (fed by per-country edges below).
txn_for_analytics = _bq_truncate(
    "txn_for_analytics",
    queries.txn_for_analytics_global,
    "txn_for_analytics",
)
analytics_article = _bq_truncate(
    "analytics_article",
    queries.analytics_article_global,
    "analytics_article",
)
analytics_visit = _bq_truncate(
    "analytics_visit",
    queries.analytics_visit_global,
    "analytics_visit",
)
analytics_branch_topseller = _bq_truncate(
    "analytics_branch_topseller",
    queries.analytics_branch_topseller_global,
    "analytics_branch_topseller",
)

assortments_tg = TaskGroup(
    group_id="wholesale_assortments",
    dag=dag,
    prefix_group_id=False,
)

for iso_code, country_code, currency_code in queries.COUNTRY_ANALYTICS:
    masterdata = _bq_truncate(
        f"wholesale_masterdata_{iso_code}",
        queries.wholesale_masterdata_query(iso_code, country_code),
        f"wholesale_masterdata_{iso_code}",
    )
    assortments = _bq_truncate(
        f"wholesale_assortments_{iso_code}",
        queries.wholesale_assortments_query(iso_code),
        f"wholesale_assortments_{iso_code}",
        task_group=assortments_tg,
    )
    txn_country = _bq_truncate(
        f"txn_for_analytics_{iso_code}",
        queries.txn_for_analytics_query(iso_code),
        f"txn_for_analytics_{iso_code}",
    )
    article = _bq_truncate(
        f"analytics_article_{iso_code}",
        queries.analytics_article_query(iso_code, currency_code),
        f"analytics_article_{iso_code}",
    )
    visit = _bq_truncate(
        f"analytics_visit_{iso_code}",
        queries.analytics_visit_query(iso_code, currency_code),
        f"analytics_visit_{iso_code}",
    )
    topseller = _bq_truncate(
        f"analytics_branch_topseller_{iso_code}",
        queries.analytics_branch_topseller_query(iso_code),
        f"analytics_branch_topseller_{iso_code}",
    )

    masterdata >> assortments >> txn_country
    txn_country >> [article, visit, topseller]
    txn_country >> loop1
    txn_country >> txn_for_analytics
    article >> analytics_article
    visit >> analytics_visit
    topseller >> analytics_branch_topseller


# Post-fan-in globals. Production left several of these unlinked in the
# >> graph (orphan risk). Portfolio wires the ones that clearly depend
# on the fan-in tables; COP feed stays independent of loop1.
earliest_visit_analytics = _bq_truncate(
    "earliest_visit_for_analytics",
    queries.earliest_visit_analytics_materialize,
    "earliest_visit_date_per_customer_for_analytics",
)
earliest_visit = _bq_truncate(
    "earliest_visit_per_customer",
    queries.earliest_visit_materialize,
    "earliest_visit_date_per_customer",
)
analytics_topseller = _bq_truncate(
    "analytics_topseller",
    queries.analytics_topseller_materialize,
    "analytics_topseller",
)
analytics_pwg = _bq_truncate(
    "analytics_pwg",
    queries.analytics_pwg_materialize,
    "analytics_pwg",
)
cop_masterdata = _bq_truncate(
    "masterdata_for_customized_offerings",
    queries.masterdata_for_customized_offerings,
    "masterdata_for_customized_offerings",
)

txn_for_analytics >> earliest_visit_analytics
txn_for_analytics >> analytics_article
earliest_visit >> analytics_topseller
# pwg / COP have no hard upstream in production — keep discoverable roots.
analytics_pwg
cop_masterdata


# Second loop: partitioned establishment transaction history.
for iso_code, _country_code in queries.PARTITIONED_TXN_COUNTRIES:
    partitioned = _bq_truncate(
        f"Insert_all_available_transactions_{iso_code}",
        queries.partitioned_txn_join_query(iso_code),
        f"all_available_transactions_{iso_code}",
        time_partitioning={"type": "DAY", "field": "date_of_day"},
        clustering={"fields": ["dwh_id"]},
    )
    loop1 >> partitioned
