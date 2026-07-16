"""
Airflow DAG: daily POS product category scoring.

Production ran the scorer as a BashOperator against a script on the Composer
worker. Downstream, a BigQuery job rebuilt a self-service product overview
that joined predictions onto sales aggregates.

Sanitized: project IDs, emails, and table names replaced. Model path is
parameterized via Airflow Variable where useful.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

PROJECT_ID = "dwh_project"
GCP_CONN_ID = "bigquery_default"

# Script lives next to other plugins/scripts on the Composer image in prod.
# For local reads of this pattern, point SCRIPT_PATH at predict_product_category.py.
SCRIPT_PATH = "/home/airflow/gcs/plugins/scripts/predict_product_category.py"

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "start_date": datetime(2021, 11, 18),
    "email": ["dataops@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

dag = DAG(
    dag_id="pos_predict_product_category",
    default_args=default_args,
    description="Score new POS products into food/beverage categories",
    schedule_interval="0 8 * * *",
    catchup=False,
    tags=["ml", "pos", "batch-scoring"],
)

# Daily incremental scoring — only names missing from the prediction table
score_products = BashOperator(
    task_id="score_product_categories",
    bash_command=f"python {SCRIPT_PATH}",
    dag=dag,
)

# Rebuild a denormalized product view for analysts after new scores land.
# Sales window kept short (2 months) so the job stays cheap.
PRODUCT_OVERVIEW_SQL = f"""
WITH sales_weeks_ago AS (
  SELECT
    branch_office_id,
    product_id,
    family_group_id,
    MAX(CASE WHEN week = EXTRACT(ISOWEEK FROM CURRENT_DATE()) - 1 THEN sale_count END)
      AS sale_one_cw_ago,
    MAX(CASE WHEN week = EXTRACT(ISOWEEK FROM CURRENT_DATE()) - 2 THEN sale_count END)
      AS sale_two_cw_ago
  FROM (
    SELECT
      branch_office_id,
      product_id,
      family_group_id,
      EXTRACT(ISOWEEK FROM timestamp) AS week,
      SUM(count) AS sale_count
    FROM `{PROJECT_ID}.trusted_views.pos_sale`
    WHERE DATE(timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 MONTH)
    GROUP BY 1, 2, 3, 4
  )
  GROUP BY 1, 2, 3
),
sales_last_week AS (
  SELECT
    branch_office_id,
    product_id,
    family_group_id,
    SUM(count) AS sale_current_week
  FROM `{PROJECT_ID}.trusted_views.pos_sale`
  WHERE DATE(timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
    AND DATE(timestamp) < CURRENT_DATE()
  GROUP BY 1, 2, 3
)
SELECT
  a.branch_office_id,
  a.name AS outlet_name,
  c.family_group_id,
  c.name AS family_group,
  d.product_id,
  d.name AS product,
  MIN(DATE(b.timestamp)) AS first_sale,
  MAX(DATE(b.timestamp)) AS last_sale,
  SUM(b.count) AS total_count,
  ROUND(SUM(b.total_money), 2) AS total_revenue,
  swl.sale_current_week,
  swa.sale_one_cw_ago,
  swa.sale_two_cw_ago,
  pred.prediction AS product_category
FROM `{PROJECT_ID}.trusted_views.pos_branch_office` a
INNER JOIN `{PROJECT_ID}.trusted_views.pos_sale` b
  USING (branch_office_id)
INNER JOIN `{PROJECT_ID}.trusted_views.pos_family_group` c
  USING (branch_office_id, family_group_id)
INNER JOIN `{PROJECT_ID}.trusted_views.pos_product` d
  USING (branch_office_id, product_id)
LEFT JOIN sales_weeks_ago swa
  ON swa.branch_office_id = b.branch_office_id
 AND swa.product_id = b.product_id
 AND swa.family_group_id = b.family_group_id
LEFT JOIN sales_last_week swl
  ON swl.branch_office_id = b.branch_office_id
 AND swl.product_id = b.product_id
 AND swl.family_group_id = b.family_group_id
LEFT JOIN `{PROJECT_ID}.trusted.pos_product_category_prediction` pred
  ON pred.product_name = CONCAT(c.name, " ", d.name)
WHERE a.is_test_office = 0
  AND b.count > 0
GROUP BY
  1, 2, 3, 4, 5, 6,
  swl.sale_current_week,
  swa.sale_one_cw_ago,
  swa.sale_two_cw_ago,
  pred.prediction
"""

rebuild_product_overview = BigQueryInsertJobOperator(
    task_id="rebuild_product_overview",
    gcp_conn_id=GCP_CONN_ID,
    configuration={
        "query": {
            "query": PRODUCT_OVERVIEW_SQL,
            "useLegacySql": False,
            "writeDisposition": "WRITE_TRUNCATE",
            "allowLargeResults": True,
            "destinationTable": {
                "projectId": PROJECT_ID,
                "datasetId": "selfservice",
                "tableId": "pos_product_view",
            },
        }
    },
    dag=dag,
)

score_products >> rebuild_product_overview
