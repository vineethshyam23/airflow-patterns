"""Daily refined-zone SCD spine — focused subset of etl_refined_zone.

Production ``etl_refined_zone`` is a ~4.7k-line overnight DAG that
materializes dozens of analytical actuals, runs hash-based SCD2 hist
for Hydra / Reservation Tool entities, quarantines test establishments,
and fans out multi-country MCC / mapping tables. This pattern ships the
spine that everything else hangs off:

1. WRITE_TRUNCATE actuals from refined views
2. SCD2 insert of new ``_keyhash|_rowhash`` combos (WRITE_APPEND)
3. Deferrable UPDATE that expires rows missing from staging
4. Dual conservative/progressive test-establishment quarantine
5. Reservation-pinned BigQuery jobs so night ETL does not starve
   interactive / BI slots

Not shipped here: country MCC transaction fan-out, Food Graph mapping
unions, Order refined tables, bundle-over-time — those are sibling
concerns already covered elsewhere or too large for one pattern folder.

Source (read-only):
  dags/etl_refined_zone.py
  dags/horeca_digital/bq_reservation.py
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.models import Variable

from reserved_bq_operator import ReservedBigQueryInsertJobOperator

GCP_PROJECT = Variable.get("dwh_gcp_project", default_var="dwh_project")
GCP_CONN = Variable.get("dwh_gcp_conn", default_var="bigquery_default")
REFINED = "refined"
STAGING = "trusted_staging"

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "start_date": datetime(2021, 4, 3),
    "email": ["dataops@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

# 03:15 UTC — after trusted land / dbt trusted views settle, before
# partner exports and midday POS refresh consumers wake up.
SCHEDULE = "15 3 * * *"

# Representative SCD entities from the Hydra / RT analytical spine.
# Production chains many more (users, RT customers, feedback, messages…).
# Same insert → expire contract; extend the list rather than copy-paste.
SCD_ENTITIES = [
    {
        "slug": "establishments",
        "actual_view": "vw_analytical_establishments_actual",
        "actual_table": "analytical_establishments_actual",
        "hist_staging_view": "vw_analytical_establishments_hist",
        "hist_table": "analytical_establishments_hist",
    },
    {
        "slug": "openingtimes",
        "actual_view": "vw_analytical_openingtimes_actual",
        "actual_table": "analytical_openingtimes_actual",
        "hist_staging_view": "vw_analytical_openingtimes_hist",
        "hist_table": "analytical_openingtimes_hist",
    },
    {
        "slug": "countries",
        "actual_view": "vw_analytical_countries_actual",
        "actual_table": "analytical_countries_actual",
        "hist_staging_view": "vw_analytical_countries_hist",
        "hist_table": "analytical_countries_hist",
    },
    {
        "slug": "establishments2offerings",
        "actual_view": "vw_analytical_establishments2offerings_actual",
        "actual_table": "analytical_establishments2offerings_actual",
        "hist_staging_view": "vw_analytical_establishments2offerings_hist",
        "hist_table": "analytical_establishments2offerings_hist",
    },
    {
        "slug": "rt_establishments",
        "actual_view": "vw_analytical_rt_establishments_actual",
        "actual_table": "analytical_rt_establishments_actual",
        "hist_staging_view": "vw_analytical_rt_establishments_hist",
        "hist_table": "analytical_rt_establishments_hist",
    },
]


def _truncate_actual_config(view: str, table: str) -> dict[str, Any]:
    return {
        "query": {
            "query": f"SELECT * FROM `{REFINED}.{view}`",
            "useLegacySql": False,
            "writeDisposition": "WRITE_TRUNCATE",
            "createDisposition": "CREATE_IF_NEEDED",
            "allowLargeResults": True,
            "destinationTable": {
                "projectId": GCP_PROJECT,
                "datasetId": REFINED,
                "tableId": table,
            },
        }
    }


def _hist_insert_sql(staging_view: str, hist_table: str) -> str:
    # New versions = staging hash pair not already current in hist.
    return f"""
SELECT *
FROM `{GCP_PROJECT}.{STAGING}.{staging_view}`
WHERE CONCAT(_keyhash, _rowhash) NOT IN (
  SELECT CONCAT(_keyhash, _rowhash)
  FROM `{REFINED}.{hist_table}`
  WHERE _valid_flag = TRUE
)
""".strip()


def _hist_expire_sql(staging_view: str, hist_table: str) -> str:
    # Close current rows whose hash pair disappeared from staging.
    # valid_until lands on end-of-yesterday so point-in-time queries
    # that use CURRENT_DATE see a clean cutover.
    return f"""
UPDATE `{REFINED}.{hist_table}`
SET
  _valid_until = TIMESTAMP(
    FORMAT_TIMESTAMP(
      '%Y-%m-%d 23:59:59',
      TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
    )
  ),
  _valid_flag = FALSE
WHERE _valid_flag = TRUE
  AND CONCAT(_keyhash, _rowhash) NOT IN (
    SELECT CONCAT(_keyhash, _rowhash)
    FROM `{GCP_PROJECT}.{STAGING}.{staging_view}`
  )
""".strip()


def _hist_insert_config(staging_view: str, hist_table: str) -> dict[str, Any]:
    return {
        "query": {
            "query": _hist_insert_sql(staging_view, hist_table),
            "useLegacySql": False,
            "writeDisposition": "WRITE_APPEND",
            "allowLargeResults": True,
            "destinationTable": {
                "projectId": GCP_PROJECT,
                "datasetId": REFINED,
                "tableId": hist_table,
            },
        }
    }


def _hist_expire_config(staging_view: str, hist_table: str) -> dict[str, Any]:
    return {
        "query": {
            "query": _hist_expire_sql(staging_view, hist_table),
            "useLegacySql": False,
        }
    }


# Dual-mode test quarantine. Conservative = high-confidence test
# signals (known corporate domains, obvious test names / streets).
# Progressive = broader regex (email domain contains 'test', numbered
# dummy restaurant names). Downstream joins choose which list to apply.
#
# Sanitized: company email domains → generic placeholders; Russia /
# geo exclusion rows dropped (policy-specific, not the pattern).
TEST_ESTABLISHMENTS_SQL = r"""
SELECT
  a.id_sk AS establishment_id_sk,
  a.dwhid,
  NULLIF(a.salesforce_id, '') AS salesforce_id,
  'conservative' AS type,
  a._sourcesystem
FROM `trusted_views.ahyd_establishments` a
LEFT JOIN `trusted_views.ahyd_establishmentsloc` b
  ON a.id_sk = b.id_id_sk AND b.lang = 'default'
WHERE a.test_establishment = 0
  AND (
    REGEXP_CONTAINS(a.email_domain, r'example[.]com|testcorp|vendor-internal')
    OR REGEXP_CONTAINS(
      LOWER(b.name),
      r'test$|testsite|^test|test[0-9/]+|sample restaurant|demo kitchen'
    )
    OR REGEXP_CONTAINS(LOWER(a.city), r'^test|sample')
    OR REGEXP_CONTAINS(
      LOWER(a.street),
      r'samplestr|teststr|^test$| test$'
    )
    OR REGEXP_CONTAINS(
      LOWER(a.domain),
      r'test[-]?dwh|dwh[-]?test|test[-]restaurant|testerprod|^test'
    )
    OR REGEXP_CONTAINS(LOWER(b.title), r'^test[[:blank:]]?[0-9]?$')
    OR REGEXP_CONTAINS(LOWER(a.zip_code), r'^12345$|^123456$')
  )
  AND NOT REGEXP_CONTAINS(LOWER(a.city), r'la teste[[:blank:]-]?de[[:blank:]-]?buch')

UNION ALL

SELECT
  a.id_sk AS establishment_id_sk,
  a.dwhid,
  NULLIF(a.salesforce_id, '') AS salesforce_id,
  'progressive' AS type,
  a._sourcesystem
FROM `trusted_views.ahyd_establishments` a
LEFT JOIN `trusted_views.ahyd_establishmentsloc` b
  ON a.id_sk = b.id_id_sk AND b.lang = 'default'
WHERE a.test_establishment = 0
  AND (
    REGEXP_CONTAINS(a.email_domain, r'example[.]com|testcorp|vendor-internal')
    OR REGEXP_CONTAINS(
      LOWER(b.name),
      r'test$|testsite|^test|test[0-9/]+|sample restaurant|demo kitchen'
    )
    OR REGEXP_CONTAINS(LOWER(a.city), r'^test|sample')
    OR REGEXP_CONTAINS(
      LOWER(a.street),
      r'samplestr|teststr|^test$| test$'
    )
    OR REGEXP_CONTAINS(
      LOWER(a.domain),
      r'test[-]?dwh|dwh[-]?test|test[-]restaurant|testerprod|^test'
    )
    OR REGEXP_CONTAINS(LOWER(b.title), r'^test[[:blank:]]?[0-9]?$')
    OR REGEXP_CONTAINS(LOWER(a.zip_code), r'^12345$|^123456$')
    OR REGEXP_CONTAINS(a.email_domain, r'test')
    OR REGEXP_CONTAINS(LOWER(b.name), r'risto[0-9]+')
  )
  AND NOT REGEXP_CONTAINS(LOWER(a.city), r'la teste[[:blank:]-]?de[[:blank:]-]?buch')

UNION ALL

-- Reservation Tool establishments (same dual labels, flatter schema)
SELECT
  a.id_sk AS establishment_id_sk,
  a.dwhid,
  NULLIF(a.salesforce_id, '') AS salesforce_id,
  'conservative' AS type,
  a._sourcesystem
FROM `trusted_views.art_establishments` a
WHERE a.test_establishment = 0
  AND (
    REGEXP_CONTAINS(a.email_domain, r'example[.]com|testcorp|vendor-internal')
    OR REGEXP_CONTAINS(
      LOWER(a.name),
      r'test$|testsite|^test|test[0-9/]+|sample restaurant|demo kitchen'
    )
    OR REGEXP_CONTAINS(LOWER(a.city), r'^test|sample')
    OR REGEXP_CONTAINS(
      LOWER(a.street),
      r'samplestr|teststr|^test$| test$'
    )
    OR REGEXP_CONTAINS(
      LOWER(a.url),
      r'test[-]?dwh|dwh[-]?test|test[-]restaurant|testerprod|^test'
    )
    OR REGEXP_CONTAINS(LOWER(a.zip_code), r'^12345$|^123456$')
  )
  AND NOT REGEXP_CONTAINS(LOWER(a.city), r'la teste[[:blank:]-]?de[[:blank:]-]?buch')

UNION ALL

SELECT
  a.id_sk AS establishment_id_sk,
  a.dwhid,
  NULLIF(a.salesforce_id, '') AS salesforce_id,
  'progressive' AS type,
  a._sourcesystem
FROM `trusted_views.art_establishments` a
WHERE a.test_establishment = 0
  AND (
    REGEXP_CONTAINS(a.email_domain, r'example[.]com|testcorp|vendor-internal')
    OR REGEXP_CONTAINS(
      LOWER(a.name),
      r'test$|testsite|^test|test[0-9/]+|sample restaurant|demo kitchen'
    )
    OR REGEXP_CONTAINS(LOWER(a.city), r'^test|sample')
    OR REGEXP_CONTAINS(
      LOWER(a.street),
      r'samplestr|teststr|^test$| test$'
    )
    OR REGEXP_CONTAINS(
      LOWER(a.url),
      r'test[-]?dwh|dwh[-]?test|test[-]restaurant|testerprod|^test'
    )
    OR REGEXP_CONTAINS(LOWER(a.zip_code), r'^12345$|^123456$')
    OR REGEXP_CONTAINS(a.email_domain, r'test')
    OR REGEXP_CONTAINS(LOWER(a.name), r'risto[0-9]+')
  )
  AND NOT REGEXP_CONTAINS(LOWER(a.city), r'la teste[[:blank:]-]?de[[:blank:]-]?buch')
"""


dag = DAG(
    dag_id="etl_refined_zone_scd_spine",
    default_args=default_args,
    schedule_interval=SCHEDULE,
    dagrun_timeout=timedelta(minutes=240),
    catchup=False,
    max_active_runs=1,
    tags=["refined-zone", "scd2", "night-etl"],
)

# Sequential actuals keep slot pressure predictable on the reservation.
# Production wires Hydra actuals → RT actuals → test quarantine in one
# chain; hist branches fan out after each entity's actual lands.
prev_actual = None
actual_tasks: dict[str, ReservedBigQueryInsertJobOperator] = {}

for entity in SCD_ENTITIES:
    slug = entity["slug"]
    actual = ReservedBigQueryInsertJobOperator(
        task_id=f"actual_{slug}",
        configuration=_truncate_actual_config(
            entity["actual_view"], entity["actual_table"]
        ),
        gcp_conn_id=GCP_CONN,
        dag=dag,
    )
    hist_insert = ReservedBigQueryInsertJobOperator(
        task_id=f"hist_{slug}_insert",
        configuration=_hist_insert_config(
            entity["hist_staging_view"], entity["hist_table"]
        ),
        gcp_conn_id=GCP_CONN,
        dag=dag,
    )
    # Deferrable UPDATE: worker releases while BigQuery runs the expire.
    # Night ETL has many of these; without deferral Composer pool saturates.
    hist_expire = ReservedBigQueryInsertJobOperator(
        task_id=f"hist_{slug}_expire",
        deferrable=True,
        configuration=_hist_expire_config(
            entity["hist_staging_view"], entity["hist_table"]
        ),
        gcp_conn_id=GCP_CONN,
        dag=dag,
    )
    if prev_actual is not None:
        prev_actual >> actual
    actual >> hist_insert >> hist_expire
    actual_tasks[slug] = actual
    prev_actual = actual

test_establishments = ReservedBigQueryInsertJobOperator(
    task_id="derived_test_establishments",
    configuration={
        "query": {
            "query": TEST_ESTABLISHMENTS_SQL,
            "useLegacySql": False,
            "writeDisposition": "WRITE_TRUNCATE",
            "createDisposition": "CREATE_IF_NEEDED",
            "allowLargeResults": True,
            "destinationTable": {
                "projectId": GCP_PROJECT,
                "datasetId": REFINED,
                "tableId": "derived_test_establishments",
            },
        }
    },
    gcp_conn_id=GCP_CONN,
    dag=dag,
)

# Quarantine runs after the last actual in the spine — same contract as
# production (after RT establishments actual). Downstream Order / mapping
# tasks (not in this subset) anti-join this table.
actual_tasks["rt_establishments"] >> test_establishments
