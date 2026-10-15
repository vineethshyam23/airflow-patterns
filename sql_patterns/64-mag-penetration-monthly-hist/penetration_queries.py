"""SQL builders for monthly MAG penetration-rate historization.

Query 1 appends per-country actuals for the prior calendar month from
the live penetration view. Query 2 appends a synthetic ``corp`` rollup
row whose platform totals are **deltas against the prior month's hist**
(current customer-base counts minus last month's historized sums).

Source (read-only):
  dags/etl_refined_zone_2nd_of_month.py
"""

from __future__ import annotations

REFINED = "refined"
VIEW_PENETRATION = "vw_penetration_rates_reporting"
HIST_PENETRATION = "hist_penetration_rates_reporting"
CUSTOMER_BASE = "platform_customer_base_establishment"


def reporting_month_expr() -> str:
    """Prior calendar month (1st of that month) as the hist grain."""
    return (
        "DATE_SUB(DATE_TRUNC(CURRENT_DATE, MONTH), INTERVAL 1 MONTH)"
    )


def country_actuals_sql(project: str) -> str:
    """Per-country penetration actuals for the reporting month."""
    month = reporting_month_expr()
    return f"""
-- Country-level MAG penetration actuals for prior month
WITH actuals AS (
  SELECT
    {month} AS date,
    country,
    SUM(active_cust_wholesale) AS active_wholesale,
    SUM(buying_cust_wholesale) AS buying_wholesale,
    SUM(platform_cust_active) AS active_platform,
    SUM(platform_cust_paying) AS paying_platform,
    SUM(platform_vendor_pos) AS vendor_pos_platform,
    SUM(platform_vendor_all) AS vendor_all_platform,
    SUM(platform_suite_paying) AS paying_suite_platform,
    SUM(platform_suite_active) AS active_suite_platform,
    SUM(platform_brick_paying) AS paying_brick_platform,
    SUM(platform_brick_active) AS active_brick_platform,
    SUM(platform_brick_suite_paying) AS paying_brick_suite_platform,
    SUM(platform_brick_suite_active) AS active_brick_suite_platform,
    SUM(platform_vendor_pos_paying) AS paying_vendor_pos_platform,
    SUM(platform_vendor_pos_active) AS active_vendor_pos_platform,
    SUM(platform_vendor_pos_brick_paying)
      AS paying_vendor_pos_brick_platform,
    SUM(platform_vendor_pos_brick_active)
      AS active_vendor_pos_brick_platform,
    SUM(platform_vendor_pos_suite_paying)
      AS paying_vendor_pos_suite_platform,
    SUM(platform_vendor_pos_suite_active)
      AS active_vendor_pos_suite_platform,
    SUM(platform_vendor_pos_brick_suite_paying)
      AS paying_vendor_pos_brick_suite_platform,
    SUM(platform_vendor_pos_brick_suite_active)
      AS active_vendor_pos_brick_suite_platform,
    SUM(platform_suite_pos_paying) AS paying_suite_pos_platform,
    SUM(platform_suite_pos_active) AS active_suite_pos_platform,
    SUM(platform_suite_pos_suite_paying)
      AS paying_suite_pos_suite_platform,
    SUM(platform_suite_pos_suite_active)
      AS active_suite_pos_suite_platform
  FROM `{project}.{REFINED}.{VIEW_PENETRATION}`
  WHERE DATE_TRUNC(date_created, MONTH) <= {month}
  GROUP BY 1, 2
)
SELECT * FROM actuals
"""


def corp_delta_sql(project: str) -> str:
    """Corporate rollup row: current base minus prior-month hist sums.

    Production subtracts last month's historized platform totals from
    current customer-base aggregates so the ``corp`` row stores a
    *delta*, not a re-sum of country rows. Re-runs without a delete
    guard will double-append — see DATA_FLOW.md.
    """
    month = reporting_month_expr()
    hist = f"`{project}.{REFINED}.{HIST_PENETRATION}`"
    base = f"`{project}.{REFINED}.{CUSTOMER_BASE}`"

    def prior(col: str) -> str:
        return (
            f"(SELECT SUM({col}) FROM {hist} WHERE date = {month})"
        )

    return f"""
-- Corporate (corp) penetration rollup as delta vs prior-month hist
WITH base AS (
  SELECT
    id_est,
    has_vendor_active,
    has_suite_pos,
    establishment_active,
    has_mto_premium,
    has_mto_prof,
    has_mto_prof_order,
    has_mto,
    CASE
      WHEN has_mto_premium = 1
        OR has_mto_prof = 1
        OR has_mto_prof_order = 1
      THEN 1 ELSE 0
    END AS paying_suite_platform,
    IF(
      (
        has_vendor_bar_kitchen
        + has_vendor_staff_planning
        + has_vendor_reservation
        + has_vendor_ord_web
        + has_vendor_qr_ord
        + has_vendor_ord_kiosk
        + has_vendor_gift_card
        + has_vendor_qr_payment
        + has_vendor_bi_tool
      ) > 0
      AND has_vendor_active = 1,
      1,
      0
    ) AS has_brick
  FROM {base}
  WHERE DATE_TRUNC(DATE(date_acquisition), MONTH) <= {month}
     OR (vendor_id IS NOT NULL AND date_acquisition IS NULL)
),
corp AS (
  SELECT
    {month} AS date,
    'corp' AS country,
    0 AS active_wholesale,
    0 AS buying_wholesale,
    SUM(establishment_active) - {prior('active_platform')}
      AS active_platform,
    COUNTIF(
      has_mto_premium = 1
      OR has_mto_prof = 1
      OR has_mto_prof_order = 1
      OR has_suite_pos = 1
      OR has_vendor_active = 1
    ) - {prior('paying_platform')} AS paying_platform,
    SUM(IF(has_suite_pos IS NOT NULL, has_suite_pos, 0))
      - {prior('vendor_pos_platform')} AS vendor_pos_platform,
    SUM(IF(has_vendor_active IS NOT NULL, has_vendor_active, 0))
      - {prior('vendor_all_platform')} AS vendor_all_platform,
    COUNTIF(
      has_suite_pos = 0
      AND has_vendor_active = 0
      AND paying_suite_platform = 1
    ) - {prior('paying_suite_platform')} AS paying_suite_platform,
    SUM(
      IF(
        has_suite_pos = 0 AND has_vendor_active = 0,
        establishment_active,
        0
      )
    ) - {prior('active_suite_platform')} AS active_suite_platform,
    COUNTIF(
      has_suite_pos = 0
      AND has_vendor_active = 1
      AND paying_suite_platform = 0
    ) - {prior('paying_brick_platform')} AS paying_brick_platform,
    SUM(
      IF(
        has_suite_pos = 0 AND has_vendor_active = 1 AND has_mto = 0,
        establishment_active,
        0
      )
    ) - {prior('active_brick_platform')} AS active_brick_platform,
    COUNTIF(
      has_suite_pos = 0
      AND has_vendor_active = 1
      AND paying_suite_platform = 1
    ) - {prior('paying_brick_suite_platform')}
      AS paying_brick_suite_platform,
    SUM(
      IF(
        has_suite_pos = 0 AND has_vendor_active = 1 AND has_mto = 1,
        establishment_active,
        0
      )
    ) - {prior('active_brick_suite_platform')}
      AS active_brick_suite_platform,
    COUNTIF(
      has_suite_pos = 1
      AND has_vendor_active = 1
      AND has_brick = 0
      AND paying_suite_platform = 0
    ) - {prior('paying_vendor_pos_platform')}
      AS paying_vendor_pos_platform,
    SUM(
      IF(
        has_suite_pos = 1
        AND has_vendor_active = 1
        AND has_mto = 0
        AND has_brick = 0,
        establishment_active,
        0
      )
    ) - {prior('active_vendor_pos_platform')}
      AS active_vendor_pos_platform,
    COUNTIF(
      has_suite_pos = 1
      AND has_vendor_active = 1
      AND has_brick = 1
      AND paying_suite_platform = 0
    ) - {prior('paying_vendor_pos_brick_platform')}
      AS paying_vendor_pos_brick_platform,
    SUM(
      IF(
        has_suite_pos = 1
        AND has_vendor_active = 1
        AND has_mto = 0
        AND has_brick = 1,
        establishment_active,
        0
      )
    ) - {prior('active_vendor_pos_brick_platform')}
      AS active_vendor_pos_brick_platform,
    COUNTIF(
      has_suite_pos = 1
      AND has_vendor_active = 1
      AND has_brick = 0
      AND paying_suite_platform = 1
    ) - {prior('paying_vendor_pos_suite_platform')}
      AS paying_vendor_pos_suite_platform,
    SUM(
      IF(
        has_suite_pos = 1
        AND has_vendor_active = 1
        AND has_mto = 1
        AND has_brick = 0,
        establishment_active,
        0
      )
    ) - {prior('active_vendor_pos_suite_platform')}
      AS active_vendor_pos_suite_platform,
    COUNTIF(
      has_suite_pos = 1
      AND has_vendor_active = 1
      AND has_brick = 1
      AND paying_suite_platform = 1
    ) - {prior('paying_vendor_pos_brick_suite_platform')}
      AS paying_vendor_pos_brick_suite_platform,
    SUM(
      IF(
        has_suite_pos = 1
        AND has_vendor_active = 1
        AND has_mto = 1
        AND has_brick = 1,
        establishment_active,
        0
      )
    ) - {prior('active_vendor_pos_brick_suite_platform')}
      AS active_vendor_pos_brick_suite_platform,
    COUNTIF(
      has_suite_pos = 1
      AND has_vendor_active = 0
      AND paying_suite_platform = 0
    ) - {prior('paying_suite_pos_platform')}
      AS paying_suite_pos_platform,
    SUM(
      IF(
        has_suite_pos = 1 AND has_vendor_active = 0 AND has_mto = 0,
        establishment_active,
        0
      )
    ) - {prior('active_suite_pos_platform')}
      AS active_suite_pos_platform,
    COUNTIF(
      has_suite_pos = 1
      AND has_vendor_active = 0
      AND paying_suite_platform = 1
    ) - {prior('paying_suite_pos_suite_platform')}
      AS paying_suite_pos_suite_platform,
    SUM(
      IF(
        has_suite_pos = 1 AND has_vendor_active = 0 AND has_mto = 1,
        establishment_active,
        0
      )
    ) - {prior('active_suite_pos_suite_platform')}
      AS active_suite_pos_suite_platform
  FROM base
)
SELECT * FROM corp
"""


if __name__ == "__main__":
    sample = country_actuals_sql("dwh_project")
    assert "hist_penetration_rates_reporting" not in sample
    assert VIEW_PENETRATION in sample
    delta = corp_delta_sql("dwh_project")
    assert HIST_PENETRATION in delta
    assert "corp" in delta
    print("ok: country + corp SQL builders")
