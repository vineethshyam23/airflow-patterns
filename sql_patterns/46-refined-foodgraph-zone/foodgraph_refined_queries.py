"""Query builders for the Food Graph refined analytics zone.

Sanitized from production ``horeca_digital/foodgraph_queries.py``
(builders used by ``etl_refined_foodgraph_zone`` only).

Naming:
  wholesale_*  — card / cash-and-carry transaction sources
  refined_*    — DWH refined datasets
  foodgraph_*  — Food Graph refined analytics dataset
"""

from __future__ import annotations

DWH_PROJECT = "dwh_project"
REFINED = "refined"
REFINED_FG = "refined_foodgraph"
TRUSTED = "trusted"
TRUSTED_WHOLESALE = "trusted_wholesale"

# Country fan-out: ISO, source-system prefix, currency.
# Production used real wholesale country prefixes; shape preserved.
COUNTRY_ANALYTICS = [
    ("CZ", "cze", "CZK"),
    ("ES", "esp", "EUR"),
    ("IT", "ita", "EUR"),
    ("RS", "srb", "RSD"),
    ("SK", "svk", "EUR"),
    ("TR", "tur", "TRY"),
    ("UA", "ukr", "UAH"),
    ("AT", "aus", "EUR"),
    ("DE", "ger", "EUR"),
    ("FR", "fra", "EUR"),
    ("HR", "cro", "EUR"),
    ("HU", "hun", "HUF"),
    ("NL", "ned", "EUR"),
    ("PL", "pol", "PLN"),
    ("PT", "por", "EUR"),
    ("RO", "rom", "RON"),
]

# Subset that gets DAY-partitioned establishment transaction history.
PARTITIONED_TXN_COUNTRIES = [
    ("AT", "aus"),
    ("DE", "ger"),
    ("FR", "fra"),
    ("HR", "cro"),
    ("HU", "hun"),
    ("IT", "ita"),
    ("NL", "ned"),
    ("PL", "pol"),
    ("PT", "por"),
    ("RO", "rom"),
    ("ES", "esp"),
]

# Global fan-in UNION uses the denser Western / CEE markets.
FAN_IN_ISOS = ["AT", "DE", "FR", "HR", "HU", "NL", "PL", "PT", "RO"]


def _union_all(table_prefix: str, isos: list[str] | None = None) -> str:
    isos = isos or FAN_IN_ISOS
    parts = [
        f"SELECT * FROM `{DWH_PROJECT}.{REFINED_FG}.{table_prefix}_{iso}`"
        for iso in isos
    ]
    return "\nUNION ALL\n".join(parts)


def wholesale_masterdata_query(iso_code: str, country_code: str) -> str:
    """Establishment branch attributes for one wholesale market."""
    _ = country_code  # production joined address tables via this prefix
    return f"""
SELECT
  branch_id,
  branch_desc,
  branch_family_desc,
  unique_wholesale_id,
  wholesale_id,
  iso_code
FROM `{DWH_PROJECT}.{REFINED}.all_establishments_{iso_code}`
WHERE data_source = 'wholesale_card'
""".strip()


def wholesale_assortments_query(iso_code: str) -> str:
    """Food / drink / disposable article assortment for one market."""
    return f"""
SELECT
  iso_code,
  pcg_cat_full_id,
  mge_cat_full_id,
  art_no,
  var_no,
  tunit_no,
  tunit_qty,
  var_tu_key,
  mikg_art_no,
  art_name,
  var_type_desc,
  parsed_art_name,
  mge_main_cat_id,
  mge_cat_id,
  mge_sub_cat_id,
  pcg_main_cat_id,
  pcg_cat_id,
  pcg_sub_cat_id,
  pcg_main_cat_desc,
  is_ownbrand,
  food_flag,
  drink_flag,
  catman_buy_domain_desc,
  stratbuy_domain_desc
FROM `{DWH_PROJECT}.{REFINED}.analytical_wholesale_articles_{iso_code}`
WHERE food_flag IS TRUE
   OR drink_flag IS TRUE
   OR mge_main_cat_id = 483  -- disposables
""".strip()


def txn_for_analytics_query(iso_code: str) -> str:
    """Join wholesale transactions to assortments (AT skips var_tu_key)."""
    second_condition = (
        ""
        if iso_code == "AT"
        else "AND CAST(a.var_tu_key AS INT64) = b.var_tu_key"
    )
    return f"""
SELECT
  a.iso_code,
  a.unique_wholesale_id,
  a.wholesale_id,
  a.sale_qty AS sum_sell_qty_colli,
  a.art_no,
  a.var_tu_key,
  a.tunit_qty,
  SAFE_CAST(a.sum_disc_base_val_nsp AS FLOAT64) AS sum_disc_base_val_nsp,
  a.sale_money AS sum_sell_val_nsp,
  b.mikg_art_no,
  a.art_name,
  a.var_type_desc,
  a.date_of_day,
  b.pcg_main_cat_desc,
  b.pcg_main_cat_id,
  a.branch_id,
  a.branch_desc,
  a.branch_family_desc,
  b.pcg_cat_full_id,
  b.mge_cat_full_id,
  b.food_flag,
  b.drink_flag,
  a.channel_store,
  a.channel_delivery,
  a.channel_webshop
FROM `{DWH_PROJECT}.{REFINED}.analytical_wholesale_transactions_{iso_code}` a
INNER JOIN `{DWH_PROJECT}.{REFINED_FG}.wholesale_assortments_{iso_code}` b
  ON a.art_no = b.art_no
  {second_condition}
""".strip()


def analytics_article_query(iso_code: str, currency_code: str) -> str:
    """Per-customer / article last-purchase grain with purchase frequency."""
    return f"""
WITH base_set AS (
  SELECT
    iso_code,
    wholesale_id,
    art_no AS article_id,
    mikg_art_no AS article_mikg_id,
    date_of_day AS last_purchase_date,
    sum_sell_qty_colli AS last_purchase_amount,
    channel_delivery,
    channel_store,
    channel_webshop,
    ROUND(sum_sell_val_nsp, 2) AS last_purchase_sum,
    sum_disc_base_val_nsp,
    art_name,
    CONCAT(
      EXTRACT(YEAR FROM date_of_day), '-',
      LPAD(CAST(EXTRACT(MONTH FROM date_of_day) AS STRING), 2, '0'),
      '-01'
    ) AS purchase_month
  FROM `{DWH_PROJECT}.{REFINED_FG}.txn_for_analytics_{iso_code}`
),
intermediate AS (
  SELECT
    iso_code,
    wholesale_id,
    article_id,
    article_mikg_id,
    last_purchase_date,
    channel_delivery,
    channel_store,
    channel_webshop,
    DATE_DIFF(
      last_purchase_date,
      IFNULL(
        LAG(last_purchase_date) OVER (
          PARTITION BY wholesale_id, purchase_month, article_mikg_id
          ORDER BY last_purchase_date
        ),
        last_purchase_date
      ),
      DAY
    ) AS days_from_last_purchase,
    last_purchase_amount,
    last_purchase_sum,
    SUM(IFNULL(last_purchase_amount, 0)) OVER (
      PARTITION BY wholesale_id, purchase_month, article_mikg_id
    ) AS total_amount,
    ROUND(
      SUM(IFNULL(last_purchase_sum, 0)) OVER (
        PARTITION BY wholesale_id, purchase_month, article_mikg_id
      ),
      2
    ) AS total_sum,
    ROW_NUMBER() OVER (
      PARTITION BY wholesale_id, purchase_month, article_mikg_id
      ORDER BY last_purchase_date DESC
    ) AS row_number,
    purchase_month
  FROM base_set
)
SELECT
  iso_code,
  wholesale_id,
  article_id,
  article_mikg_id,
  last_purchase_date,
  purchase_month,
  last_purchase_amount,
  last_purchase_sum,
  ROUND(total_amount, 2) AS total_amount,
  total_sum,
  channel_delivery,
  channel_store,
  channel_webshop,
  CASE
    WHEN COUNT(days_from_last_purchase) OVER (
      PARTITION BY wholesale_id, purchase_month, article_mikg_id
    ) = 1 THEN -1
    ELSE ROUND(
      AVG(days_from_last_purchase) OVER (
        PARTITION BY wholesale_id, purchase_month, article_mikg_id
      ),
      2
    )
  END AS purchase_frequency,
  '{currency_code}' AS currency
FROM intermediate
WHERE row_number = 1
""".strip()


def analytics_visit_query(iso_code: str, currency_code: str) -> str:
    """Recent visit grain (top 1000 dates per customer) — simplified form.

    Production nests line items + optional delivery address into a JSON
    ``details`` column for richer markets. The AT path stayed flat.
    Portfolio keeps the flat shape used for the lighter markets.
    """
    return f"""
SELECT
  iso_code,
  wholesale_id,
  date_of_day AS `date`,
  '{currency_code}' AS currency,
  ROUND(total * 100) / 100 AS total_sum,
  CAST(NULL AS STRING) AS details,
  channel_store,
  channel_delivery,
  channel_webshop
FROM (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY wholesale_id
      ORDER BY date_of_day DESC
    ) AS rn
  FROM (
    SELECT
      channel_store,
      channel_delivery,
      channel_webshop,
      iso_code,
      wholesale_id,
      date_of_day,
      SUM(sum_sell_val_nsp) AS total
    FROM `{DWH_PROJECT}.{REFINED_FG}.txn_for_analytics_{iso_code}`
    GROUP BY 1, 2, 3, 4, 5, 6
  )
)
WHERE rn <= 1000
""".strip()


def analytics_branch_topseller_query(iso_code: str) -> str:
    """Top articles by branch / month with buyer penetration and frequency."""
    return f"""
WITH segments AS (
  SELECT
    a.rb_group,
    CONCAT('SEG-', CAST(a.rb_group_id AS STRING)) AS rb_group_id,
    b.unique_wholesale_id
  FROM `{DWH_PROJECT}.{REFINED}.di_segments_cofg` a
  INNER JOIN `{DWH_PROJECT}.{REFINED}.all_mappings` b
    ON CAST(a.md_establishment_id AS STRING) = b.establishment_id
   AND LOWER(a.iso_code) = LOWER(b.iso_code)
  WHERE b.unique_wholesale_id IS NOT NULL
    AND LOWER(b.data_source) = 'peer_enrichment'
),
base_data AS (
  SELECT
    a.iso_code,
    a.unique_wholesale_id,
    COALESCE(b.rb_group_id, a.branch_id) AS branch_id,
    COALESCE(b.rb_group, a.branch_family_desc) AS branch_family_desc,
    COALESCE(b.rb_group, a.branch_desc) AS branch_desc,
    a.date_of_day,
    a.art_no AS article_id,
    a.mikg_art_no AS article_mikg_id,
    a.art_name AS article_name,
    a.sum_sell_val_nsp AS revenue,
    a.sum_sell_qty_colli,
    CONCAT(
      EXTRACT(YEAR FROM date_of_day), '-',
      LPAD(CAST(EXTRACT(MONTH FROM date_of_day) AS STRING), 2, '0'),
      '-01'
    ) AS purchase_month,
    c.currency,
    CASE WHEN b.rb_group IS NOT NULL THEN 1 ELSE 0 END AS has_segment
  FROM `{DWH_PROJECT}.{REFINED_FG}.txn_for_analytics_{iso_code}` a
  LEFT JOIN segments b
    ON a.unique_wholesale_id = b.unique_wholesale_id
  LEFT JOIN `{DWH_PROJECT}.{TRUSTED}.country_iso_mapping` c
    ON LOWER(a.iso_code) = LOWER(c.iso_code)
),
total_articles AS (
  SELECT *
  FROM (
    SELECT
      RANK() OVER (
        PARTITION BY iso_code, branch_id, purchase_month
        ORDER BY total_revenue DESC
      ) AS revenue_rank,
      *
    FROM (
      SELECT
        iso_code,
        branch_id,
        article_mikg_id,
        purchase_month,
        SUM(sum_sell_qty_colli) AS total_sold,
        ROUND(SUM(revenue), 2) AS total_revenue
      FROM base_data
      GROUP BY 1, 2, 3, 4
    )
  )
  WHERE revenue_rank <= 1000
),
buyers_for_article AS (
  SELECT
    iso_code,
    branch_id,
    article_mikg_id,
    purchase_month,
    COUNT(DISTINCT unique_wholesale_id) AS total_customers_buying
  FROM base_data
  GROUP BY 1, 2, 3, 4
),
total_customers_in_branch AS (
  SELECT
    iso_code,
    branch_id,
    purchase_month,
    COUNT(DISTINCT unique_wholesale_id) AS total_customers_in_branch
  FROM base_data
  GROUP BY 1, 2, 3
)
SELECT
  a.iso_code,
  a.branch_id,
  a.article_mikg_id,
  a.purchase_month,
  a.total_revenue,
  b.total_customers_buying,
  ROUND(
    b.total_customers_buying / c.total_customers_in_branch,
    2
  ) AS percent_customers_buying,
  d.currency,
  d.has_segment
FROM total_articles a
INNER JOIN buyers_for_article b
  USING (iso_code, branch_id, article_mikg_id, purchase_month)
INNER JOIN total_customers_in_branch c
  USING (iso_code, branch_id, purchase_month)
INNER JOIN (
  SELECT DISTINCT
    iso_code,
    branch_id,
    article_mikg_id,
    purchase_month,
    currency,
    has_segment
  FROM base_data
) d
  USING (iso_code, branch_id, article_mikg_id, purchase_month)
""".strip()


def partitioned_txn_join_query(iso_code: str) -> str:
    """Map wholesale card IDs to platform dwh_id for partitioned history."""
    txn = f"{DWH_PROJECT}.{REFINED_FG}.txn_for_analytics_{iso_code}"
    mapping = f"{DWH_PROJECT}.{REFINED}.wholesale_to_dwh_id_mapping"
    return f"""
SELECT
  b.dwh_id,
  a.iso_code,
  a.wholesale_id,
  a.pcg_cat_full_id,
  a.mge_cat_full_id,
  a.mikg_art_no,
  a.art_name,
  a.var_type_desc,
  a.art_no,
  a.var_tu_key,
  a.tunit_qty,
  a.sum_sell_val_nsp,
  a.date_of_day
FROM `{txn}` a
INNER JOIN `{mapping}` b
  ON a.wholesale_id = b.wholesale_id
""".strip()


txn_for_analytics_global = _union_all("txn_for_analytics")
analytics_article_global = _union_all("analytics_article")
analytics_visit_global = _union_all("analytics_visit")
analytics_branch_topseller_global = _union_all("analytics_branch_topseller")

# Materialize views that live in the refined Food Graph dataset.
earliest_visit_materialize = f"""
SELECT * FROM `{DWH_PROJECT}.{REFINED_FG}.v_earliest_visit_date_per_customer`
""".strip()

earliest_visit_analytics_materialize = f"""
SELECT * FROM `{DWH_PROJECT}.{REFINED_FG}.v_earliest_visit_date_per_customer_for_analytics`
""".strip()

analytics_pwg_materialize = f"""
SELECT * FROM `{DWH_PROJECT}.{REFINED_FG}.v_analytics_pwg`
""".strip()

analytics_topseller_materialize = f"""
SELECT * FROM `{DWH_PROJECT}.{REFINED_FG}.v_analytics_topseller`
""".strip()

# Downstream customized-offerings feed (two densest markets in production).
# Contact columns kept as schema shape; treat as sensitive in real deploys.
masterdata_for_customized_offerings = f"""
WITH all_customers AS (
  SELECT 'DE' AS iso_code, * FROM `{DWH_PROJECT}.{TRUSTED_WHOLESALE}.ger_customer`
  UNION ALL
  SELECT 'PL' AS iso_code, * FROM `{DWH_PROJECT}.{TRUSTED_WHOLESALE}.pol_customer`
),
deduped AS (
  SELECT * EXCEPT (row_number)
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY unique_wholesale_id
        ORDER BY date_created DESC
      ) AS row_number
    FROM all_customers
  )
  WHERE row_number = 1
)
SELECT
  cust.unique_wholesale_id,
  cust.cust_name,
  cust.iso_code,
  cust.branch_family_id,
  cust.branch_family_desc,
  cust.branch_id,
  cust.branch_desc
FROM deduped cust
""".strip()


if __name__ == "__main__":
    sample = txn_for_analytics_query("DE")
    assert "dwh_project" in sample
    assert "var_tu_key" in sample
    print("ok:", len(COUNTRY_ANALYTICS), "country chains,", len(FAN_IN_ISOS), "fan-in")
