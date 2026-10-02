"""SQL builders for the Value Creation Dashboard (VCD) bi-monthly refresh.

Production embeds these CREATE OR REPLACE / CALL statements inline in the
Composer DAG. Pulling them into builders keeps country quirks (especially
Austria) reviewable without scrolling a 700-line module.

Naming (sanitized):
  trusted_wholesale  ← wholesale card / MCC trusted land
  refined            ← refined analytics
  refined_innovation ← mapping / dashboard lookup tables
  trusted_staging    ← vcd_* materializations for the dashboard + PSM
  discovery          ← short-lived working tables
"""

from __future__ import annotations

from typing import Iterable

# ISO → three-letter wholesale country code used in trusted_wholesale tables.
COUNTRY_MAP: list[tuple[str, str]] = [
    ("de", "ger"),
    ("pl", "pol"),
    ("pt", "por"),
    ("sk", "svk"),
    ("cz", "cze"),
    ("at", "aus"),
    ("fr", "fra"),
    ("es", "esp"),
    ("nl", "ned"),
    ("ro", "rom"),
    ("hr", "cro"),
    ("it", "ita"),
    ("hu", "hun"),
    ("ua", "ukr"),
    ("tr", "tur"),
    ("be", "bel"),
]

# Establishment fan-out uses uppercase ISO suffixes on refined tables.
ESTABLISHMENT_ISOS: list[str] = [
    "ES",
    "IT",
    "PL",
    "DE",
    "HU",
    "FR",
    "HR",
    "RS",
    "SK",
    "PT",
    "UA",
    "CZ",
    "AT",
    "TR",
    "NL",
    "RO",
]

# Stored-proc loop skips markets without a PSM uplift procedure.
PSM_SKIP_ISOS = frozenset({"RS", "UA", "AT", "TR"})

# Suffixes included in the wildcard union for the TTL discovery view.
# BE is unioned separately with a historical cut-off.
TXN_UNION_SUFFIXES = (
    "PL",
    "DE",
    "PT",
    "FR",
    "ES",
    "NL",
    "RO",
    "HR",
    "HU",
    "IT",
    "SK",
    "CZ",
    "TR",
    "UA",
)


def create_or_replace(project: str, dataset: str, table: str, select_sql: str) -> str:
    return (
        f"CREATE OR REPLACE TABLE `{project}.{dataset}.{table}` AS\n"
        f"{select_sql}"
    )


def wholesale_customer_sql(project: str, iso: str, country: str) -> str:
    """Per-country customer slice; AT drops hospitality filter + nulls assort fields."""
    if iso == "at":
        select_cols = """
            cust_no, home_store_id, wholesale_id, branch_main_group_id,
            branch_main_group_desc, date_created, status_cd,
            NULL AS cust_assort_section_desc, unique_cust_no, unique_home_store_id,
            unique_wholesale_id, corporate_abc_cd, NULL AS corporate_abc_detail_cd
        """
        where = ""
    else:
        select_cols = """
            cust_no, home_store_id, wholesale_id, branch_main_group_id,
            branch_main_group_desc, date_created, status_cd,
            cust_assort_section_desc, unique_cust_no, unique_home_store_id,
            unique_wholesale_id, corporate_abc_cd, corporate_abc_detail_cd
        """
        where = "WHERE LOWER(cust_assort_section_desc) LIKE '%hospitality%'"

    select_sql = f"""
        SELECT {select_cols}
        FROM `{project}.trusted_wholesale.{country}_customer`
        {where}
    """.strip()
    return create_or_replace(
        project, "trusted_staging", f"vcd_wholesale_customer_{iso}", select_sql
    )


def analytical_customer_sql(project: str, iso: str) -> str:
    iso_u = iso.upper()
    select_sql = (
        f"SELECT * FROM `{project}.refined.analytical_wholesale_customers_{iso_u}`"
    )
    return create_or_replace(
        project, "trusted_staging", f"vcd_analytical_customers_{iso_u}", select_sql
    )


def wholesale_article_sql(project: str, iso: str, country: str) -> str:
    select_sql = f"""
        SELECT art_no, CAST(mge_incl_adj_sales_ind AS STRING) AS mge_incl_adj_sales_ind
        FROM `{project}.trusted_wholesale.{country}_article`
    """.strip()
    return create_or_replace(
        project, "trusted_staging", f"vcd_wholesale_article_{iso}", select_sql
    )


def wholesale_assortment_sql(project: str, iso: str, country: str) -> str:
    if iso == "at":
        body = f"""
            SELECT DISTINCT
                art_no,
                art_name,
                NULL AS private_label_desc,
                CASE
                    WHEN is_private_label_ind = 1 THEN TRUE
                    WHEN is_private_label_ind = 0 THEN FALSE
                END AS private_label_ind
            FROM `{project}.trusted_wholesale.aus_article`
        """.strip()
    else:
        body = f"""
            SELECT DISTINCT
                a.art_no,
                a.art_name,
                b.private_label_desc,
                IF(private_label_ind = 1, TRUE, FALSE) AS private_label_ind
            FROM `{project}.trusted_wholesale.{country}_art_var_tu` a
            LEFT JOIN `{project}.trusted_wholesale.{country}_rt_private_label` b
              ON a.private_label_cd = b.private_label_cd
        """.strip()
    return create_or_replace(
        project, "trusted_staging", f"vcd_wholesale_assortment_{iso}", body
    )


def wholesale_transactions_sql(project: str, iso: str, country: str) -> str:
    """Invoice-line land. Austria synthesizes unique_wholesale_id from store+cust."""
    if iso == "at":
        unique_id = """
            CAST(
              CONCAT(
                '99999',
                LPAD(CAST(home_store_id AS STRING), 3, '0'),
                LPAD(
                  CASE
                    WHEN cust_no < 1000000 THEN CAST(cust_no AS STRING)
                    ELSE SUBSTR(CAST(cust_no AS STRING), 3)
                  END,
                  8,
                  '0'
                )
              ) AS INT64
            ) AS unique_wholesale_id
        """
        cust_no = """
            CASE
              WHEN cust_no < 1000000 THEN cust_no
              ELSE CAST(SUBSTR(CAST(cust_no AS STRING), 3) AS INT64)
            END AS cust_no
        """
    else:
        unique_id = "unique_wholesale_id"
        cust_no = "cust_no"

    select_sql = f"""
        SELECT
            {unique_id},
            '{iso.upper()}' AS iso_code,
            sum_sell_val_nsp,
            date_of_day,
            art_no,
            {cust_no}
        FROM `{project}.trusted_wholesale.{country}_cust_invoice_line`
    """.strip()
    return create_or_replace(
        project, "trusted_staging", f"vcd_wholesale_transactions_{iso}", select_sql
    )


def establishments_sql(project: str, iso_u: str) -> str:
    select_sql = f"""
        SELECT
            wholesale_id, real_wholesale_id, unique_wholesale_id,
            real_unique_wholesale_id, sfdc_establishment_id, sfdc_account_id,
            md_establishment_id, establishment_id, non_platform, non_wholesale
        FROM `{project}.refined.all_establishments_{iso_u}`
        WHERE data_source = 'all'
          AND wholesale_id IS NOT NULL
    """.strip()
    return create_or_replace(
        project, "trusted_staging", f"vcd_all_establishments_{iso_u}", select_sql
    )


def copy_star_sql(project: str, source_dataset: str, source_table: str, dest_table: str) -> str:
    select_sql = f"SELECT * FROM `{project}.{source_dataset}.{source_table}`"
    return create_or_replace(project, "trusted_staging", dest_table, select_sql)


def global_reference_jobs(project: str) -> list[tuple[str, str]]:
    """(task_id, sql) for global VCD reference / MAG / mapping tables."""
    specs = [
        ("db_value_creation_mcc_mapping", "refined_innovation", "db_value_creation_mcc_mapping", "vcd_db_value_creation_mcc_mapping"),
        ("db_value_creation_mcc_mapping_churn", "refined_innovation", "db_value_creation_mcc_mapping_churn", "vcd_db_value_creation_mcc_mapping_churn"),
        ("invoice_line_export", "trusted_views", "odoo_wsl_invoice_lines", "vcd_invoice_line_export"),
        ("vcd_platform_mcc_mapping", "refined_innovation", "vcd_platform_mcc_mapping", "vcd_platform_mcc_mapping"),
        ("vcd_pos_vendor_mcc_mapping", "refined_innovation", "vcd_pos_vendor_mcc_mapping", "vcd_pos_vendor_mcc_mapping"),
        ("plan_fy_snapshot", "refined", "plan_fy_snapshot", "vcd_plan_fy_snapshot"),
        ("mag_acquisitions_base", "refined", "vw_mag_acquisitions_base", "vcd_mag_acquisitions_base"),
        ("hist_sales_mag_reporting", "refined", "hist_sales_mag_reporting", "vcd_hist_sales_mag_reporting"),
        ("hist_acquisitions_mag_reporting", "refined", "hist_acquisitions_mag_reporting", "vcd_hist_acquisitions_mag_reporting"),
        ("onboardings_reporting_hist", "refined", "onboardings_reporting_hist", "vcd_onboardings_reporting_hist"),
    ]
    jobs: list[tuple[str, str]] = []
    for task_id, src_ds, src_tbl, dest in specs:
        jobs.append((task_id, copy_star_sql(project, src_ds, src_tbl, dest)))

    # Wildcard external targets table — keep shard pattern visible.
    mag_targets = create_or_replace(
        project,
        "trusted_staging",
        "vcd_mag_targets_longformat",
        f"SELECT * FROM `{project}.external.mag_targets_longformat_*`",
    )
    jobs.append(("mag_targets_longformat", mag_targets))
    return jobs


def reactivation_select(project: str) -> str:
    return f"SELECT * FROM `{project}.discovery.fh_rt_reactivation_ids`"


def customer_establishment_select(project: str) -> str:
    return f"SELECT * FROM `{project}.discovery.nnt_value_creation_dashboard`"


def transaction_union_sql(project: str) -> str:
    """Wildcard union of country shards + BE historical cut-off.

    Destination lives in discovery with a 15-day TTL so failed runs do not
    leave orphaned working tables forever.
    """
    suffixes = ", ".join(f"'{s}'" for s in TXN_UNION_SUFFIXES)
    return f"""
        CREATE OR REPLACE TABLE `{project}.discovery.v_wholesale_transaction_source`
        OPTIONS (
          expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 15 DAY)
        ) AS
        SELECT
            cust_no,
            unique_wholesale_id,
            UPPER(_TABLE_SUFFIX) AS iso_code,
            CAST(sum_sell_val_nsp AS FLOAT64) AS sum_sell_val_nsp,
            date_of_day,
            art_no
        FROM `{project}.trusted_staging.vcd_wholesale_transactions_*`
        WHERE UPPER(_TABLE_SUFFIX) IN ({suffixes})
        UNION DISTINCT
        SELECT
            cust_no,
            unique_wholesale_id,
            'BE' AS iso_code,
            CAST(sum_sell_val_nsp AS FLOAT64) AS sum_sell_val_nsp,
            date_of_day,
            art_no
        FROM `{project}.trusted_staging.vcd_wholesale_transactions_be`
        WHERE date_of_day < '2022-10-01'
    """.strip()


def psm_uplift_call(project: str, iso: str, env: str) -> str:
    return f"CALL `{project}.trusted.get_psm_uplift_values_v2`('{iso}', '{env}');"


def psm_iso_envs(isos: Iterable[str] | None = None) -> list[tuple[str, str]]:
    markets = list(isos) if isos is not None else ESTABLISHMENT_ISOS
    out: list[tuple[str, str]] = []
    for iso in markets:
        if iso in PSM_SKIP_ISOS:
            continue
        for env in ("prod", "dev"):
            out.append((iso, env))
    return out


if __name__ == "__main__":
    # Lightweight sanity checks — no Airflow / BQ required.
    p = "dwh_project"
    assert "hospitality" in wholesale_customer_sql(p, "de", "ger")
    assert "NULL AS cust_assort_section_desc" in wholesale_customer_sql(p, "at", "aus")
    assert "99999" in wholesale_transactions_sql(p, "at", "aus")
    assert "INTERVAL 15 DAY" in transaction_union_sql(p)
    assert len(psm_iso_envs()) == 24  # 12 countries × prod/dev
    print("vcd_queries ok")
