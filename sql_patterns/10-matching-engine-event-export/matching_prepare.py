"""
Build the partner matching export staging table.

Joins match-engine results (CRM establishment ↔ wholesale customer, and
POS/Booq establishment ↔ wholesale customer) onto the establishment
product footprint, unpivots product flags into one row per active
service, then writes a replaceable BigQuery staging table the ingest
tasks read by country.

Source (read-only):
  dags/horeca_digital/matching_export_to_DANA.py

Distinct from pattern 01 (SCD Type 2 history of match_result). This
module consumes the current valid matches and shapes them for a
downstream event bus — it does not maintain SCD history.
"""

from __future__ import annotations

import logging
from datetime import date
from timeit import default_timer as timer
from typing import Dict

import numpy as np
import pandas as pd
import pandas_gbq as gbq

# Country ISO → wholesale customer table suffix. Tables are
# trusted_mcc.amcc_{suffix}_customer_unique in the warehouse.
COUNTRY_TABLE_SUFFIX: Dict[str, str] = {
    "be": "bel",
    "cz": "cze",
    "de": "ger",
    "es": "esp",
    "fr": "fra",
    "hr": "cro",
    "hu": "hun",
    "it": "ita",
    "nl": "ned",
    "pl": "pol",
    "pt": "por",
    "ro": "rom",
    "sk": "svk",
    "tr": "tur",
    "ua": "ukr",
}

# Service code catalog shipped to the partner. Inactive establishments
# get code + 700 so the consumer can filter without a separate status
# column on every product row.
SERVICE_CATALOG: Dict[str, str] = {
    "100": "Dish Website",
    "101": "Dish Reservation",
    "102": "Dish Starter",
    "103": "Dish Professional Reservation",
    "104": "Dish Professional Order",
    "105": "Dish Premium",
    "106": "Dish Order",
    "107": "Dish WebListing",
    "108": "Dish MenuKit",
    "109": "Dish Cockpit",
    "110": "Dish Lynn",
    "111": "Dish POS",
    "201": "GastroConsulting",
}

PRODUCT_FLAGS = [
    "wb_customer",
    "rt_customer",
    "mto_start_customer",
    "mto_pro_customer",
    "mto_proforder_customer",
    "mto_premium_customer",
    "do_customer",
    "wl_customer",
    "mk_customer",
    "cp_customer",
    "ly_customer",
    "gastro_customer",
    "pos_customer",
]

# Match quality cutoff — higher scores are weaker fuzzy matches in this
# engine. 150 was the production accept threshold when this shipped.
MATCH_QUALITY_MAX = 150

OPEN_ENDED_DATE_TO = "2949-12-31"


def get_sql_query(country_suffix: Dict[str, str], country: str) -> str:
    """Per-country SQL: valid matches + wholesale attrs + product footprint."""
    suffix = country_suffix[country]
    return f"""
WITH hd_cust AS (
  SELECT
    DISTINCT cb.id_est AS HD_cust_ident,
    cb.establishment_sfid AS UID__c,
    cb.booq_id,
    cb.has_dish_website AS wb_customer,
    cb.has_dish_reservation AS rt_customer,
    cb.has_dish_teamplan AS tp_customer,
    cb.has_dish_weblisting AS wl_customer,
    cb.has_cockpit AS cp_customer,
    cb.has_menukit AS mk_customer,
    cb.has_lynn AS ly_customer,
    cb.has_dish_order AS do_customer,
    cb.has_Dish_POS AS pos_customer,
    cb.has_mto_starter AS mto_start_customer,
    cb.has_mto_prof AS mto_pro_customer,
    cb.has_mto_proforder AS mto_proforder_customer,
    cb.has_mto_premium AS mto_premium_customer,
    cb.has_MenuEngineering AS gastro_customer,
    cb.establishment_activity_score,
    cb.establishment_active,
    MIN(cb.dish_website_createddate) AS wb_creation_date,
    MIN(cb.dish_reservation_createddate) AS rt_creation_date,
    MIN(cb.teamplan_createddate) AS tp_creation_date,
    MIN(cb.dish_weblisting_createddate) AS wl_creation_date,
    MIN(cb.cockpit_createddate) AS cp_creation_date,
    MIN(cb.menukit_createddate) AS mk_creation_date,
    MIN(cb.lynn_createddate) AS ly_creation_date,
    MIN(cb.dish_order_createddate) AS do_creation_date,
    MIN(cb.Dish_POS_createdDate) AS pos_creation_date,
    MIN(cb.mto_starter_createddate) AS mto_start_creation_date,
    MIN(cb.mto_prof_createddate) AS mto_pro_creation_date,
    MIN(cb.mto_proforder_createddate) AS mto_proforder_creation_date,
    MIN(cb.mto_premium_createddate) AS mto_premium_creation_date,
    MAX(cb.dish_website_deleteddate) AS wb_deletion_date,
    MAX(cb.dish_reservation_deleteddate) AS rt_deletion_date,
    MAX(cb.dish_weblisting_deleteddate) AS wl_deletion_date,
    MAX(cb.menukit_deleteddate) AS mk_deletion_date,
    MAX(cb.dish_order_deleteddate) AS do_deletion_date,
    MAX(cb.Dish_POS_deletedDate) AS pos_deletion_date,
    MIN(cb.mto_starter_disableddate) AS mto_start_deletion_date,
    MIN(cb.mto_prof_disableddate) AS mto_pro_deletion_date,
    MIN(cb.mto_proforder_disableddate) AS mto_proforder_deletion_date,
    MIN(cb.mto_premium_disableddate) AS mto_premium_deletion_date,
    MIN(cb.date_acquisition) AS date_acquisition,
    MAX(cb.date_deletion) AS date_deletion,
    MAX(cb.SFDC_createdDate) AS SFDC_createdDate
  FROM `dwh_project.refined.customer_base_establishment` cb
  GROUP BY
    1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19
),
raw_match_result AS (
  SELECT DISTINCT
    UPPER(me.iso_code) AS country,
    me.id_source_2,
    me.id_source_1,
    me.unique_metro_id,
    me.original_request,
    me.list_type,
    me.match_quality
  FROM `dwh_project.trusted.match_result` me
  WHERE me.original_request IN (
      'mcc_company_booq_establishment',
      'sf_establishment_mcc_company'
    )
    AND CAST(PARSE_NUMERIC(me.match_quality) AS INT64) <= {MATCH_QUALITY_MAX}
    AND me._valid_flag IS TRUE
    AND UPPER(me.iso_code) = '{country.upper()}'
),
mcc_booq AS (
  SELECT DISTINCT
    country,
    id_source_2 AS booq_id,
    unique_metro_id,
    list_type,
    match_quality
  FROM raw_match_result
  WHERE original_request = 'mcc_company_booq_establishment'
),
mcc_sfdc AS (
  SELECT DISTINCT
    country,
    id_source_1 AS salesforce_id,
    unique_metro_id,
    list_type,
    match_quality
  FROM raw_match_result
  WHERE original_request = 'sf_establishment_mcc_company'
),
mcc_cust AS (
  SELECT DISTINCT
    IFNULL(ms.country, mb.country) AS country,
    IFNULL(ms.unique_metro_id, mb.unique_metro_id) AS unique_metro_id,
    salesforce_id AS UID__c,
    booq_id,
    CAST(mcc.unique_home_store_id AS STRING) AS unique_home_store_id,
    CAST(mcc.unique_cust_no AS STRING) AS unique_cust_no,
    mcc.status_cd,
    mcc.blocking_reason_cd,
    COALESCE(ms.match_quality, mb.match_quality) AS match_quality
  FROM mcc_sfdc ms
  FULL JOIN mcc_booq mb
    ON mb.unique_metro_id = ms.unique_metro_id
  LEFT JOIN `dwh_project.trusted_mcc.amcc_{suffix}_customer_unique` mcc
    ON IFNULL(ms.unique_metro_id, mb.unique_metro_id)
       = CAST(mcc.unique_metro_id AS STRING)
)
SELECT
  a.* EXCEPT (booq_id),
  b.* EXCEPT (UID__c, booq_id)
FROM mcc_cust a
JOIN hd_cust b
  ON IFNULL(CAST(a.UID__c AS STRING), 'non')
     = IFNULL(CAST(b.UID__c AS STRING), 'non')
  AND IFNULL(CAST(LOWER(a.booq_id) AS STRING), 'non')
     = IFNULL(CAST(LOWER(b.booq_id) AS STRING), 'non')
"""


def _rename_map_for_products() -> Dict[str, str]:
    column_names: Dict[str, str] = {}
    for flag in PRODUCT_FLAGS:
        prefix = flag.replace("_customer", "")
        column_names[f"{prefix}_creation_date"] = "date_from"
        column_names[f"{prefix}_deletion_date"] = "date_to"
    return column_names


def _slice_product(
    df: pd.DataFrame,
    flag: str,
    label: str,
    unique_cols,
    activity_cols,
    column_names,
) -> pd.DataFrame:
    """Keep rows with the product flag set; rename date cols; stamp label."""
    # Cockpit / Lynn / Gastro historically lack deletion columns in the
    # customer base extract — select only columns that exist.
    candidates = [flag, f"{flag.replace('_customer', '')}_creation_date",
                  f"{flag.replace('_customer', '')}_deletion_date"]
    present = [c for c in candidates if c in df.columns]
    out = df.loc[df[flag] == 1, unique_cols + present + activity_cols].rename(
        columns=column_names
    )
    out["w360_service_desc"] = label
    return out


def prepare_data(
    country_suffix: Dict[str, str],
    project_id: str,
    service_catalog: Dict[str, str],
) -> pd.DataFrame:
    df_list = []
    column_names = _rename_map_for_products()
    desc_to_code = {v: k for k, v in service_catalog.items()}

    for country in country_suffix.keys():
        logging.info("Started for %s", country.upper())
        try:
            df = gbq.read_gbq(
                get_sql_query(country_suffix=country_suffix, country=country),
                project_id=project_id,
            )
        except Exception:
            logging.exception(
                "Caught exception while loading data for %s", country.upper()
            )
            continue

        logging.info("Loaded data for %s — starting reshape", country.upper())

        unique_cols = [
            "unique_metro_id",
            "UID__c",
            "HD_cust_ident",
            "unique_home_store_id",
            "unique_cust_no",
            "country",
            "status_cd",
            "blocking_reason_cd",
            "match_quality",
        ]
        activity_cols = [
            "establishment_active",
            "establishment_activity_score",
            "date_acquisition",
            "date_deletion",
            "SFDC_createdDate",
        ]

        slices = [
            _slice_product(df, "wb_customer", "Dish Website", unique_cols, activity_cols, column_names),
            _slice_product(df, "rt_customer", "Dish Reservation", unique_cols, activity_cols, column_names),
            _slice_product(df, "mto_start_customer", "Dish Starter", unique_cols, activity_cols, column_names),
            _slice_product(df, "mto_pro_customer", "Dish Professional Reservation", unique_cols, activity_cols, column_names),
            _slice_product(df, "mto_proforder_customer", "Dish Professional Order", unique_cols, activity_cols, column_names),
            _slice_product(df, "mto_premium_customer", "Dish Premium", unique_cols, activity_cols, column_names),
            _slice_product(df, "do_customer", "Dish Order", unique_cols, activity_cols, column_names),
            _slice_product(df, "wl_customer", "Dish WebListing", unique_cols, activity_cols, column_names),
            _slice_product(df, "mk_customer", "Dish MenuKit", unique_cols, activity_cols, column_names),
            _slice_product(df, "cp_customer", "Dish Cockpit", unique_cols, activity_cols, column_names),
            _slice_product(df, "ly_customer", "Dish Lynn", unique_cols, activity_cols, column_names),
            _slice_product(df, "gastro_customer", "GastroConsulting", unique_cols, activity_cols, column_names),
            _slice_product(df, "pos_customer", "Dish POS", unique_cols, activity_cols, column_names),
        ]

        df = pd.concat(slices).reset_index(drop=True)
        drop_flags = [c for c in PRODUCT_FLAGS if c in df.columns]
        if drop_flags:
            df = df.drop(columns=drop_flags)

        logging.info(
            "country: %s\n%s", country.upper(), df["w360_service_desc"].value_counts()
        )

        df["w360_service_cd"] = df["w360_service_desc"].map(desc_to_code)

        # Active wholesale IDs (via establishments marked active).
        mcc_active_id = (
            pd.merge(
                left=df.loc[df["establishment_active"] == 1, ["UID__c"]].drop_duplicates(),
                right=df[["UID__c", "unique_metro_id"]].drop_duplicates(),
                on="UID__c",
                how="inner",
            )
            .drop(columns=["UID__c"])
            .drop_duplicates()
        )

        # Inactive → code + 700. Keeps the catalog contiguous for the
        # consumer without a second status dimension on every row.
        df["w360_service_cd"] = np.where(
            df["unique_metro_id"].isin(mcc_active_id["unique_metro_id"].values),
            df["w360_service_cd"],
            df["w360_service_cd"].astype(int) + 700,
        )

        # Dedup rule 1: one HD id per wholesale id — keep highest activity.
        active_establishments = df.sort_values(
            by=["unique_metro_id", "HD_cust_ident", "establishment_activity_score"]
        ).drop_duplicates(subset=["unique_metro_id"], keep="last")

        df = pd.merge(
            left=active_establishments.filter(items=["unique_metro_id", "HD_cust_ident"]),
            right=df,
            on=["unique_metro_id", "HD_cust_ident"],
            how="inner",
        )

        # Dedup rule 2: same activity → keep oldest service start.
        oldest = df.sort_values(
            by=["unique_metro_id", "HD_cust_ident", "date_from"]
        ).drop_duplicates(subset=["unique_metro_id"], keep="first")

        df = pd.merge(
            left=oldest.filter(items=["unique_metro_id", "HD_cust_ident"]),
            right=df,
            on=["unique_metro_id", "HD_cust_ident"],
            how="inner",
        )

        df["date_from"] = (
            df["date_from"].fillna(df["date_acquisition"]).dt.strftime("%Y-%m-%d")
        )
        df["date_to"] = (
            df["date_to"]
            .fillna(df["date_deletion"])
            .dt.strftime("%Y-%m-%d")
            .fillna(OPEN_ENDED_DATE_TO)
        )

        # Price was always zero when this export shipped — partner schema
        # still requires the field.
        df["price"] = 0

        vars_list = [
            "unique_home_store_id",
            "unique_cust_no",
            "country",
            "w360_service_cd",
            "w360_service_desc",
            "date_from",
            "date_to",
            "price",
            "HD_cust_ident",
            "UID__c",
            "SFDC_createdDate",
            "status_cd",
            "blocking_reason_cd",
            "match_quality",
        ]
        df_list.append(df[vars_list].drop_duplicates())
        logging.info("Finished for %s", country.upper())

    if not df_list:
        raise RuntimeError("No country produced matching export rows")

    return (
        pd.concat(df_list)
        .sort_values(by=["country", "unique_home_store_id", "unique_cust_no"])
        .drop_duplicates()
        .reset_index(drop=True)
    )


def organise_data(
    country_suffix: Dict[str, str],
    project_id: str,
    service_catalog: Dict[str, str],
) -> pd.DataFrame:
    df_final = prepare_data(
        country_suffix=country_suffix,
        project_id=project_id,
        service_catalog=service_catalog,
    )
    df_final["creation_date"] = str(date.today())
    df_final["unique_home_store_id"] = (
        df_final["unique_home_store_id"].fillna(0).astype("int64")
    )
    df_final["unique_cust_no"] = df_final["unique_cust_no"].fillna(0).astype("int64")
    df_final["w360_service_cd"] = df_final["w360_service_cd"].astype("str")
    df_final["price"] = df_final["price"].astype("float64")
    df_final["SFDC_createdDate"] = df_final["SFDC_createdDate"].astype(str)
    df_final["status_cd"] = df_final["status_cd"].astype(str)
    df_final["blocking_reason_cd"] = df_final["blocking_reason_cd"].astype(str)
    df_final["match_quality"] = (
        df_final["match_quality"].astype(str).astype("int64")
    )
    return df_final


DESTINATION_SCHEMA = [
    {"name": "unique_home_store_id", "type": "INT64"},
    {"name": "unique_cust_no", "type": "INT64"},
    {"name": "country", "type": "STRING"},
    {"name": "w360_service_cd", "type": "STRING"},
    {"name": "w360_service_desc", "type": "STRING"},
    {"name": "date_from", "type": "STRING"},
    {"name": "date_to", "type": "STRING"},
    {"name": "price", "type": "NUMERIC"},
    {"name": "HD_cust_ident", "type": "STRING"},
    {"name": "creation_date", "type": "STRING"},
    {"name": "UID__c", "type": "STRING"},
    {"name": "SFDC_createdDate", "type": "STRING"},
    {"name": "status_cd", "type": "STRING"},
    {"name": "blocking_reason_cd", "type": "STRING"},
    {"name": "match_quality", "type": "INT64"},
]


def matching_export_prepare(
    project_id: str = "dwh_project",
    destination_dataset: str = "refined",
    destination_table_name: str = "partner_matching_export",
) -> None:
    """Replace the staging table the country ingest tasks read from."""
    start = timer()
    df_final = organise_data(
        country_suffix=COUNTRY_TABLE_SUFFIX,
        project_id=project_id,
        service_catalog=SERVICE_CATALOG,
    )
    logging.info(
        "Writing %s rows → %s.%s (project=%s)",
        len(df_final),
        destination_dataset,
        destination_table_name,
        project_id,
    )
    df_final.to_gbq(
        destination_dataset + "." + destination_table_name,
        project_id=project_id,
        table_schema=DESTINATION_SCHEMA,
        chunksize=None,
        if_exists="replace",
    )
    logging.info("Done in %.3fs", timer() - start)


if __name__ == "__main__":
    logging.basicConfig(
        format="%(asctime)s %(levelname)-4s %(message)s",
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    # Dry path: print one country SQL so reviewers can see the join shape
    # without needing BigQuery credentials.
    print(get_sql_query(COUNTRY_TABLE_SUFFIX, "de")[:800], "...")
