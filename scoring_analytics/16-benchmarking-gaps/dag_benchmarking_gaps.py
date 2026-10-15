"""
Airflow DAG: multi-country peer benchmarking gaps materialization.

For each enabled country, two upstream branches converge on the final
gaps table:

  topsellers → skeletons ─┐
                          ├─→ gaps
  establishments → txns ──┘

BigQuery owns the heavy lifting (ARRAY nesting, percentiles, potential
bands). Composer only fans out InsertJob operators per ISO code.

Source (read-only):
  dags/etl_benchmarking_gaps.py
  dags/horeca_digital/benchmarking_gaps_queries.py

Distinct from pattern 12/14 (menu-gap opportunity ranking for a partner
feed). This DAG builds *peer purchase gap* tables used by sales tooling
and by the Avro export in benchmarking_gaps_export.py.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

import benchmarking_gaps_queries as bg

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email": ["dataops@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

dag = DAG(
    dag_id="etl_benchmarking_gaps",
    default_args=default_args,
    # Daily after overnight refined loads. Gaps SQL is heavy; do not
    # stack catchup runs if Composer is behind.
    schedule_interval="45 5 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["benchmarking", "gaps", "bigquery", "multi-country"],
    doc_md=(
        "Per-country peer benchmarking gaps: topsellers + skeletons and "
        "establishments + transactions converge into refined.benchmarking_gaps_{cc}. "
        "See scoring_analytics/16-benchmarking-gaps/."
    ),
)

PROJECT_ID = "dwh_project"
DATASET_STAGING = "staging"
DATASET = "refined"
RUN_DATE = "{{ ds }}"
GCP_CONN_ID = "bigquery_default"


def all_iso_and_country_codes():
    countries = [
        ("CZ", "cze"),
        ("ES", "esp"),
        ("IT", "ita"),
        ("RS", "srb"),
        ("SK", "svk"),
        ("TR", "tur"),
        ("UA", "ukr"),
        ("AT", "aus"),
        ("DE", "ger"),
        ("FR", "fra"),
        ("HR", "cro"),
        ("HU", "hun"),
        ("NL", "ned"),
        ("PL", "pol"),
        ("PT", "por"),
        ("RO", "rom"),
    ]
    countries.sort()
    return countries


def benchmarking_countries():
    # Subset that actually has reliable purchase + taxonomy coverage.
    # Keep the full list above for easy enable/disable without hunting
    # through task ids.
    countries = [
        ("DE", "ger"),
        ("FR", "fra"),
        ("AT", "aus"),
        ("HR", "cro"),
        ("HU", "hun"),
        ("NL", "ned"),
        ("PL", "pol"),
        ("PT", "por"),
        ("RO", "rom"),
        ("ES", "esp"),
        ("IT", "ita"),
        ("TR", "tur"),
        ("UA", "ukr"),
    ]
    countries.sort()
    return countries


enabled = set(benchmarking_countries())

for iso_code, country_code in all_iso_and_country_codes():
    if (iso_code, country_code) not in enabled:
        continue

    topsellers = BigQueryInsertJobOperator(
        task_id=f"benchmarking_topsellers_{iso_code}",
        configuration={
            "query": {
                "query": bg.benchmarking_topsellers_query(
                    iso_code, PROJECT_ID, DATASET_STAGING, DATASET, RUN_DATE
                ),
                "useLegacySql": False,
                "writeDisposition": "WRITE_TRUNCATE",
                "createDisposition": "CREATE_IF_NEEDED",
                "destinationTable": {
                    "projectId": PROJECT_ID,
                    "datasetId": DATASET,
                    "tableId": f"benchmarking_topsellers_{iso_code}",
                },
            }
        },
        gcp_conn_id=GCP_CONN_ID,
        dag=dag,
    )

    skeletons = BigQueryInsertJobOperator(
        task_id=f"benchmarking_skeletons_{iso_code}",
        configuration={
            "query": {
                "query": bg.benchmarking_gaps_skeletons_query(
                    iso_code, PROJECT_ID, DATASET_STAGING, DATASET, RUN_DATE
                ),
                "useLegacySql": False,
                "writeDisposition": "WRITE_TRUNCATE",
                "createDisposition": "CREATE_IF_NEEDED",
                "destinationTable": {
                    "projectId": PROJECT_ID,
                    "datasetId": DATASET,
                    "tableId": f"benchmarking_gaps_skeletons_{iso_code}",
                },
            }
        },
        gcp_conn_id=GCP_CONN_ID,
        dag=dag,
    )

    establishments = BigQueryInsertJobOperator(
        task_id=f"benchmarking_establishments_{iso_code}",
        configuration={
            "query": {
                "query": bg.benchmarking_gaps_establishment_query(iso_code),
                "useLegacySql": False,
                "writeDisposition": "WRITE_TRUNCATE",
                "createDisposition": "CREATE_IF_NEEDED",
                "destinationTable": {
                    "projectId": PROJECT_ID,
                    "datasetId": DATASET,
                    "tableId": f"benchmarking_gaps_establishments_{iso_code}",
                },
            }
        },
        gcp_conn_id=GCP_CONN_ID,
        dag=dag,
    )

    transactions = BigQueryInsertJobOperator(
        task_id=f"benchmarking_transactions_{iso_code}",
        configuration={
            "query": {
                "query": bg.benchmarking_gaps_transactions_query(iso_code),
                "useLegacySql": False,
                "writeDisposition": "WRITE_TRUNCATE",
                "createDisposition": "CREATE_IF_NEEDED",
                "destinationTable": {
                    "projectId": PROJECT_ID,
                    "datasetId": DATASET,
                    "tableId": f"benchmarking_gaps_transactions_{iso_code}",
                },
            }
        },
        gcp_conn_id=GCP_CONN_ID,
        dag=dag,
    )

    gaps = BigQueryInsertJobOperator(
        task_id=f"benchmarking_gaps_{iso_code}",
        configuration={
            "query": {
                "query": bg.benchmarking_gaps_query(
                    iso_code, PROJECT_ID, DATASET_STAGING, DATASET, RUN_DATE
                ),
                "useLegacySql": False,
                "writeDisposition": "WRITE_TRUNCATE",
                "createDisposition": "CREATE_IF_NEEDED",
                "destinationTable": {
                    "projectId": PROJECT_ID,
                    "datasetId": DATASET,
                    "tableId": f"benchmarking_gaps_{iso_code}",
                },
            }
        },
        gcp_conn_id=GCP_CONN_ID,
        dag=dag,
    )

    topsellers >> skeletons >> gaps
    establishments >> transactions >> gaps
