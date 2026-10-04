"""SQL builders for MAG sales + acquisitions monthly historization.

Query 1 appends prior-month product-bundle sales counts.
Query 2 appends acquisitions with a running ``sales_all_time`` total
carried forward from the previous hist month (full-outer join so
countries with zero new sales still advance the cumulative).

Source (read-only):
  dags/horeca_digital/archived/etl_refined_zone_monthly.py
"""

from __future__ import annotations

REFINED = "refined"
VIEW_ACQ_BASE = "vw_acquisitions_base"
VIEW_ACQ_REPORTING = "vw_acquisitions_reporting"
HIST_SALES = "hist_sales_reporting"
HIST_ACQUISITIONS = "hist_acquisitions_reporting"

# Sanitized bundle labels (production used suite product nicknames).
BUNDLE_CASE = """
CASE
  WHEN ProductCode__c IN ('MTO_MTOPremium', 'MTO_MTOPremium_1Y')
    THEN 'suitePrem'
  WHEN ProductCode__c = 'MTO_MTOStarter'
    THEN 'suiteStart'
  WHEN ProductCode__c IN ('MTO_MTOProfOrder', 'MTO_MTOProfOrder_1Y')
    THEN 'suiteOrd'
  WHEN ProductCode__c IN ('MTO_MTOProfessional', 'MTO_MTOProfessional_1Y')
    THEN 'suiteRes'
  WHEN ProductCode__c = 'POS_L_Package'
    THEN 'suitePos'
END
""".strip()

BUNDLE_CASE_SALES = """
CASE
  WHEN ProductCode__c IN ('MTO_MTOPremium', 'MTO_MTOPremium_1Y')
    THEN 'suitePrem'
  WHEN ProductCode__c = 'MTO_MTOStarter'
    THEN 'suiteStart'
  WHEN ProductCode__c IN ('MTO_MTOProfOrder', 'MTO_MTOProfOrder_1Y')
    THEN 'suiteOrd'
  WHEN ProductCode__c IN ('MTO_MTOProfessional', 'MTO_MTOProfessional_1Y')
    THEN 'suiteRes'
END
""".strip()

PRIOR_MONTH_FILTER = """
EXTRACT(YEAR FROM CreatedDate)
  = EXTRACT(YEAR FROM DATE_SUB(CURRENT_DATE, INTERVAL 1 MONTH))
AND EXTRACT(MONTH FROM CreatedDate)
  = EXTRACT(MONTH FROM DATE_SUB(CURRENT_DATE, INTERVAL 1 MONTH))
""".strip()


def reporting_month_expr() -> str:
    """Prior calendar month (1st of that month) as the hist grain."""
    return (
        "DATE_SUB(DATE_TRUNC(CURRENT_DATE, MONTH), INTERVAL 1 MONTH)"
    )


def sales_hist_sql(project: str) -> str:
    """Prior-month sales by product bundle × reseller country."""
    return f"""
-- Prior-month MAG sales counts → hist_sales_reporting
SELECT
  DATE_TRUNC(DATE(CreatedDate), MONTH) AS date,
  {BUNDLE_CASE_SALES} AS product_bundle,
  Reseller_Country,
  COUNT(establishment_id) AS sales_value
FROM `{project}.{REFINED}.{VIEW_ACQ_BASE}`
WHERE {PRIOR_MONTH_FILTER}
GROUP BY 1, 2, 3
"""


def acquisitions_hist_sql(project: str) -> str:
    """Prior-month acquisitions + sales_all_time carry-forward.

    Full-outer joins current-month sales against the *previous*
    hist month so every country×bundle keeps a cumulative total
    even when sales_value is zero. Excludes market ``BE`` (production
    carve-out kept for fidelity).
    """
    hist = f"`{project}.{REFINED}.{HIST_ACQUISITIONS}`"
    view = f"`{project}.{REFINED}.{VIEW_ACQ_REPORTING}`"
    month = reporting_month_expr()

    return f"""
-- Prior-month acquisitions with cumulative sales_all_time
WITH a AS (
  SELECT
    DATE_TRUNC(DATE(CreatedDate), MONTH) AS date,
    Reseller_Country,
    {BUNDLE_CASE} AS product_bundle,
    COUNT(establishment_id) AS sales_value
  FROM {view}
  WHERE Reseller_Country NOT IN ('BE')
    AND {PRIOR_MONTH_FILTER}
  GROUP BY 1, 2, 3
),
-- Prior hist month: carry sales_all_time forward
b AS (
  SELECT
    date,
    product_bundle,
    Reseller_Country,
    0 AS sales_value,
    sales_all_time
  FROM {hist}
  WHERE Reseller_Country NOT IN ('BE')
    AND EXTRACT(YEAR FROM date)
      = EXTRACT(YEAR FROM DATE_SUB(CURRENT_DATE, INTERVAL 2 MONTH))
    AND EXTRACT(MONTH FROM date)
      = EXTRACT(MONTH FROM DATE_SUB(CURRENT_DATE, INTERVAL 2 MONTH))
),
cc AS (
  SELECT
    IFNULL(a.date, b.date) AS date,
    IFNULL(a.product_bundle, b.product_bundle) AS product_bundle,
    IFNULL(a.Reseller_Country, b.Reseller_Country) AS Reseller_Country,
    IFNULL(a.sales_value, 0) AS sales_value,
    b.sales_all_time
  FROM a
  FULL OUTER JOIN b
    ON a.product_bundle = b.product_bundle
   AND a.Reseller_Country = b.Reseller_Country
),
d AS (
  SELECT
    date,
    product_bundle,
    Reseller_Country,
    sales_value,
    (sales_value + sales_all_time) AS sales_all_time
  FROM cc
),
g AS (
  SELECT *
  FROM d
  WHERE date = {month}
),
h AS (
  SELECT
    {month} AS date,
    d.product_bundle,
    d.Reseller_Country,
    IFNULL(g.sales_value, d.sales_value) AS sales_value,
    IFNULL(g.sales_all_time, d.sales_all_time) AS sales_all_time
  FROM d
  FULL JOIN g
    ON g.product_bundle = d.product_bundle
   AND g.Reseller_Country = d.Reseller_Country
)
SELECT * FROM h
ORDER BY Reseller_Country, product_bundle
"""


if __name__ == "__main__":
    sales = sales_hist_sql("dwh_project")
    assert VIEW_ACQ_BASE in sales
    assert "suitePrem" in sales
    assert "FROM `dwh_project.refined.vw_acquisitions_base`" in sales
    acq = acquisitions_hist_sql("dwh_project")
    assert HIST_ACQUISITIONS in acq
    assert "sales_all_time" in acq
    assert "BE" in acq
    print("ok: sales + acquisitions SQL builders")
