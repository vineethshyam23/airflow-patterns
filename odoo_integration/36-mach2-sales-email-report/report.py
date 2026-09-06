"""
report_runner.py
────────────────
Executes BigQuery reports, writes timestamped Excel files, and returns payloads
for the shared ``email_delivery`` module.

Environment variables (set by Airflow before main() runs):
  GCP_PROJECT_ID, BQ_DATASET_REFINED_SALES, BQ_DATASET_REFINED,
  BQ_TABLE_*, EMAIL_FROM, EMAIL_FROM_NAME,
  EMAIL_TO_REPORT_1, EMAIL_TO_REPORT_2, EMAIL_TO_REPORT_3, EMAIL_CC (optional)
  EMAIL_PROVIDER — "smtp" (default) or "sendgrid" (used by main() local runs)
  SMTP_* / SENDGRID_API_KEY — see email_delivery module
"""

from __future__ import annotations

import io
import logging
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from textwrap import dedent
from typing import Any, Callable, Optional
from zoneinfo import ZoneInfo

import pandas as pd
from google.cloud import bigquery
from openpyxl import Workbook
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────


def _env(key: str, required: bool = True) -> str:
    value = os.getenv(key, "")
    if required and not value:
        raise EnvironmentError(f"Required environment variable '{key}' is not set.")
    return value


@dataclass(frozen=True)
class BQTables:
    """Fully-qualified BigQuery table references built from env vars."""

    project: str
    ds_sales: str
    ds_refined: str

    # individual table names
    res_partner: str
    sale_order: str
    sale_order_line: str
    res_country: str
    product_product: str
    product_template: str
    close_reason: str
    partner_matching: str

    def fq(self, dataset: str, table: str) -> str:
        """Return a fully-qualified `project.dataset.table` reference."""
        return f"`{self.project}.{dataset}.{table}`"

    @classmethod
    def from_env(cls) -> "BQTables":
        return cls(
            project=_env("GCP_PROJECT_ID"),
            ds_sales=_env("BQ_DATASET_REFINED_SALES"),
            ds_refined=_env("BQ_DATASET_REFINED"),
            res_partner=_env("BQ_TABLE_RES_PARTNER"),
            sale_order=_env("BQ_TABLE_SALE_ORDER"),
            sale_order_line=_env("BQ_TABLE_SALE_ORDER_LINE"),
            res_country=_env("BQ_TABLE_RES_COUNTRY"),
            product_product=_env("BQ_TABLE_PRODUCT_PRODUCT"),
            product_template=_env("BQ_TABLE_PRODUCT_TEMPLATE"),
            close_reason=_env("BQ_TABLE_SALE_ORDER_CLOSE_REASON"),
            partner_matching=_env("BQ_TABLE_PARTNER_MATCHING"),
        )


@dataclass
class ReportSpec:
    """Everything needed to produce one report."""

    name: str
    filename_prefix: str
    sheet_name: str
    subject: str
    html_body: Callable[..., str]
    recipient_env_key: str
    query_fn: Callable
    rename_cols: dict = field(default_factory=dict)


# ── Query builders ────────────────────────────────────────────────────────────






def _query_activations(t: BQTables) -> str:
    """Report 1 – Activations of new add-ons for old customers."""
    rp   = t.fq(t.ds_sales, t.res_partner)
    so   = t.fq(t.ds_sales, t.sale_order)
    sol  = t.fq(t.ds_sales, t.sale_order_line)
    rc   = t.fq(t.ds_sales, t.res_country)
    pp   = t.fq(t.ds_sales, t.product_product)
    pt   = t.fq(t.ds_sales, t.product_template)
    cr   = t.fq(t.ds_sales, t.close_reason)
    eij  = t.fq(t.ds_refined, t.partner_matching)

    return dedent(f"""
        WITH
          rp AS (
            SELECT
              active                                   AS Active_Establishment,
              country_id,
              id                                       AS Partner_ID,
              parent_id                                AS Company_ID,
              country_id                               AS CountryID,
              name                                     AS Establishment_Name,
              dish_partner_uuid                        AS Establishment_UID,
              dish_establishment_machine_code          AS Machine_Code,
              dish_establishment_fiskaltrust_acces_token AS FT_Access_Token_Badge,
              dish_establishment_fiskaltrust_cashboxID AS FT_Cashbox_Id,
              dish_establishment_pos_clientID          AS Client_ID,
              dish_establishment_pos_licenseID         AS License_ID,
              dish_establishment_pos_debitor_number,
              vat
            FROM {rp}
            WHERE active = TRUE
          ),
          rcomp AS (
            SELECT
              id     AS CompanyID,
              name   AS Company_Name,
              street AS Company_Street,
              zip    AS Company_Zip_Code,
              city   AS Company_City,
              email  AS Email,
              mobile AS Mobile_Phone
            FROM {rp}
          ),
          sol AS (
            SELECT
              Asset_UID,
              Order_UID,
              Description,
              ProductID,
              product_uom_qty,
              instance
            FROM (
              SELECT
                id             AS Asset_UID,
                order_id       AS Order_UID,
                name           AS Description,
                product_id     AS ProductID,
                product_uom_qty,
                GENERATE_ARRAY(1, product_uom_qty) AS instances
              FROM {sol}
            )
            CROSS JOIN UNNEST(instances) AS instance
          ),
          base_assets AS (
            SELECT
              rp.Establishment_UID,
              rp.Partner_ID,
              rp.Company_ID,
              rcomp.Company_Name,
              rcomp.Company_Street,
              rcomp.Company_Zip_Code,
              rcomp.Company_City,
              rcomp.Email,
              rcomp.Mobile_Phone,
              rp.Establishment_Name,
              rc.code                                  AS Country_Code,
              JSON_VALUE(rc.name, '$.en_US')           AS Country_Name,
              rp.Machine_Code,
              rp.FT_Access_Token_Badge,
              rp.FT_Cashbox_Id,
              rp.Client_ID,
              rp.License_ID,
              rp.vat,
              sol.Asset_UID,
              sol.Order_UID,
              sol.Description,
              sol.ProductID,
              sol.product_uom_qty,
              sol.instance,
              CASE
                WHEN pt.name.en_US LIKE 'POS_L_Lice'      THEN 'POS License Package'
                WHEN pt.name.en_US = 'Licenza aggiuntiva' THEN 'Additional POS License'
                ELSE pt.name.en_US
              END                                      AS Product_Name,
              pp.dish_product_code                     AS Product_Code,
              CASE
                WHEN pt.dish_product_code = 'POS_L_DAteV'                              THEN '452521'
                WHEN pt.dish_product_code = 'POS_L_Fiscalisation'                      THEN '452514'
                WHEN pt.dish_product_code IN ('POS_L_FO','POS_POSDEVICE')              THEN '452519'
                WHEN pt.dish_product_code = 'POS_L_Lite'                               THEN '452525'
                WHEN pt.dish_product_code IN ('POS_L_ORDER2POS','POS_WEBSHOP')         THEN '452523'
                WHEN pt.dish_product_code IN ('POS_L_ORDERAGG','POS_ORDERAGG')         THEN '452524'
                WHEN pt.dish_product_code = 'POS_L_ORDERAGGMP'                         THEN '452524'
                WHEN pt.dish_product_code = 'POS_L_Licence'                            THEN '452510'
                WHEN pt.dish_product_code IN ('POS_L_QR','POS_SSQR')                  THEN '452518'
                WHEN pt.name.en_US = 'Licenza aggiuntiva'                              THEN '452519'
                WHEN pt.dish_product_code IN ('POS_L_QRPayment','POS_QRPAY')           THEN '452517'
                WHEN pt.dish_product_code IN ('POS_L_TapToPayConnected','POS_EFTDEVICE') THEN '452526'
                WHEN pt.dish_product_code = 'PT_PTPayAccept'                           THEN '452513'
              END                                      AS `Partner Article Number`,
              rp.dish_establishment_pos_debitor_number AS `Debnr infosys`,
              eij.Vestcode_infosys                     AS `Vestcode infosys`,
              sol.product_uom_qty                      AS qty,
              CAST(so.create_date AS DATE)             AS CreatedDate,
              CAST(so.start_date  AS DATE)             AS start_date,
              CAST(closed_at      AS DATE)             AS DisabledDate,
              CAST(so.end_date    AS DATE)             AS end_date
            FROM sol
            LEFT JOIN {so}  so  ON sol.Order_UID      = so.id
            LEFT JOIN rp        ON rp.partner_id      = so.partner_id
            LEFT JOIN rcomp     ON rcomp.CompanyID    = rp.Company_ID
            LEFT JOIN {rc}  rc  ON rp.country_id      = rc.id
            LEFT JOIN {pp}  pp  ON pp.id              = sol.ProductID
            LEFT JOIN {pt}  pt  ON pp.product_tmpl_id = pt.id
            LEFT JOIN {eij} eij ON eij.establishment_id = rp.Establishment_UID
            LEFT JOIN {cr}  cr  ON cr.id              = so.close_reason_id
            WHERE
              (  pt.dish_product_code LIKE 'POS_L%'
                 OR pt.dish_product_code IN (
                   'POS_POSDEVICE','POS_SUMMER','POS_WINTER','POS_ORDERAGG',
                   'POS_SSQR','POS_QRPAY','POS_GIFT','POS_WEBSHOP','POS_EFTDEVICE'
                 )
              )
              AND rp.dish_establishment_pos_debitor_number IS NOT NULL
              AND so.state IN ('sale', 'done')
              AND so.active = TRUE
          ),
          new_addon AS (
            SELECT
              Establishment_UID,
              Order_UID,                           -- keep Order_UID to tie qty to correct order
              MAX(CreatedDate)         AS addon_created_date,
              MAX(DisabledDate)        AS addon_disabled_date,
              Product_Code             AS product_code,
              Product_Name             AS product_name,
              MAX(product_uom_qty)     AS qty       -- correct per order line
            FROM base_assets
            WHERE Product_Code IN (
              'POS_POSDEVICE','POS_SUMMER','POS_WINTER','POS_ORDERAGG',
              'POS_SSQR','POS_QRPAY','POS_GIFT','POS_WEBSHOP','POS_EFTDEVICE'
            )
              AND (DisabledDate IS NULL OR DATE(DisabledDate) >= CURRENT_DATE())
            GROUP BY Establishment_UID, Order_UID, Product_Code, Product_Name  -- group per order
          )
        SELECT
          a.Country_Name                               AS `Country`,
          a.Establishment_UID                          AS `Establishment UID`,
          a.Company_Name                               AS `Company Name`,
          a.Establishment_Name                         AS `Establishment Name`,
          CASE
            WHEN b.Product_Name = 'Access Point EA U6 Pro'                        THEN 'POS Access Point'
            WHEN b.Product_Name IN ('Additional POS License',
                                    'Additional POS License Flex')                 THEN 'POS Additional Front-Office License'
            WHEN b.Product_Name = 'Cash Drawer'                                   THEN 'POS Cash Drawer'
            WHEN b.Product_Name = 'DateV'                                         THEN 'DAteV'
            WHEN b.Product_Name = 'Pay Tap to Pay Terminal'                  THEN 'Tap to Pay Android Hardware'
            WHEN b.Product_Name = 'POS Front Office License - Summer'        THEN 'Summer License'
            WHEN b.Product_Name = 'POS Front Office License - Winter'        THEN 'Winter License'
            WHEN b.Product_Name = 'Electronic Invoice Flex'                       THEN 'Electronic invoice'
            WHEN b.Product_Name = 'Fiscal Printer Maintenance'                    THEN 'Fiscal printer maintenance service'
            WHEN b.Product_Name = 'Fiscalisation License'                         THEN 'POS Fiscalisation License'
            WHEN b.Product_Name = 'Gift Card Management Flex'                     THEN 'Gift Card Management'
            WHEN b.Product_Name = 'Handheld Sumi'                                 THEN 'Handheld Sunmi'
            WHEN b.Product_Name IN ('Licenza aggiuntiva',
                                    'Licenza aggiuntiva Flex')                    THEN 'POS Additional Front-Office License'
            WHEN b.Product_Name = 'Mini Server M3'                                THEN 'Mini Server M3 Pro'
            WHEN b.Product_Name = 'Mini Server P1'                                THEN 'Miniserver'
            WHEN b.Product_Name = 'Order Aggregation'                             THEN 'Order Aggregation: Otter'
            WHEN b.Product_Name = 'Order2POS Flex'                                THEN 'Order2POS'
            WHEN b.Product_Name = 'POS Consultant'                                THEN 'Installation Only'
            WHEN b.Product_Name = 'POS Lite Screen T3'                            THEN 'T3 Lite Screen'
            WHEN b.Product_Name = 'POS Router'                                    THEN 'POS Router ER605'
            WHEN b.Product_Name = 'Printer DPT201'                                THEN 'POS Printer DPT201'
            WHEN b.Product_Name = 'Printer TM-M30II'                              THEN 'POS Printer TM30'
            WHEN b.Product_Name = 'QR Ordering License'                           THEN 'POS QR Ordering License'
            WHEN b.Product_Name = 'QR Ordering License Flex'                      THEN 'POS QR Ordering License'
            WHEN b.Product_Name = 'QR Payment'                                    THEN 'POS QR Payment'
            WHEN b.Product_Name LIKE '%Verifone P400 Plus Hardware%'              THEN 'Verifone P400 Plus'
            WHEN b.Product_Name LIKE '%S1F2 Hardware%'                            THEN 'S1F2 mobile with printer'
            WHEN b.Product_Name LIKE '%V400c Hardware%'                           THEN 'V400c desk with printer'
            WHEN b.Product_Name LIKE '%V400m Hardware%'                           THEN 'Verifone V400m'
            WHEN b.Product_Name LIKE 'POS License%'                               THEN 'POS License Package'
            WHEN b.Product_Name = 'Payment Acceptance'                            THEN 'Platform Payment Acceptance License'
            WHEN b.Product_Name = 'DateV Flex'                                    THEN 'DAteV'
            WHEN b.Product_Name = 'Fiscalisation License Flex'                    THEN 'POS Fiscalisation License'
            WHEN b.Product_Name = 'POS License Flex'                              THEN 'POS License Package'
            WHEN b.Product_Name = 'Licenza aggiuntiva Flex'                       THEN 'POS Additional License IT'
            WHEN b.Product_Name = 'POS License Package Flex - DE'                 THEN 'POS License Package'
            WHEN b.Product_Name = 'POS License Package Flex - ES'                 THEN 'POS License Package'
            WHEN b.Product_Name = 'POS License Package Flex - FR'                 THEN 'POS License Package'
            WHEN b.Product_Name = 'POS License Package Flex - IT'                 THEN 'POS License Package'
            ELSE b.Product_Name
          END                                          AS `Product Name`,
          CASE
            WHEN b.Product_Name LIKE 'Mini%' OR b.Product_Name LIKE '%Miniserver%'
              THEN a.Machine_Code
            ELSE NULL
          END                                          AS MachineCode,
          a.Company_Street                             AS `Company Street`,
          a.Company_Zip_Code                           AS `Company ZipPostal Code`,
          a.Company_City                               AS `Company City`,
          a.Email,
          a.Mobile_Phone                               AS `Mobile Phone`,
          CONCAT(a.Asset_UID, a.instance)              AS `Asset UID`,
          a.Order_UID                                  AS `Order UID`,
          a.FT_Cashbox_Id                              AS `Fiskaltrust Cashbox ID`,
          CASE
            WHEN a.Country_Code = 'FR'           THEN 'http://host.docker.internal:1300/ftrest'
            WHEN a.Country_Code IN ('IT', 'ES')  THEN 'http://host.docker.internal:5618'
            ELSE                                       'http://host.docker.internal:1500/ftrest'
          END                                          AS `Fiskaltrust Queue URL`,
          CASE
            WHEN a.Country_Code = 'FR'           THEN '00000000-0000-4000-8000-000000000001'
            WHEN a.Country_Code IN ('IT', 'ES')  THEN a.Establishment_UID
            ELSE                                       '00000000-0000-4000-8000-000000000002'
          END                                          AS `Fiskaltrust POS System ID`,
          a.FT_Access_Token_Badge                      AS `FT Access TokenBadge`,
          CASE WHEN a.Country_Code = 'IT' THEN a.vat ELSE NULL END AS `VAT ID IT`,
          b.qty                                        AS `Qty`,
        FROM base_assets a
        INNER JOIN new_addon b USING (Establishment_UID)
        WHERE (
          a.Product_Code LIKE 'POS_L_Package%'
          OR a.Product_Code LIKE 'POS_L_Package_Flex%'
          OR a.Product_Code = 'POS_L_Licence'
        )
          AND b.addon_created_date = CURRENT_DATE() - 1
          AND (a.DisabledDate IS NULL OR DATE(a.DisabledDate) > CURRENT_DATE())
        QUALIFY ROW_NUMBER() OVER (
          PARTITION BY a.Establishment_UID, b.product_code, b.Order_UID
          ORDER BY a.Order_UID DESC
        ) = 1
    """).strip()



def _query_sales_channels(t: BQTables) -> str:
    """Report 2 – Sales Channels for MACH2 customers."""
    rp   = t.fq(t.ds_sales, t.res_partner)
    so   = t.fq(t.ds_sales, t.sale_order)
    sol  = t.fq(t.ds_sales, t.sale_order_line)
    rc   = t.fq(t.ds_sales, t.res_country)
    pp   = t.fq(t.ds_sales, t.product_product)
    pt   = t.fq(t.ds_sales, t.product_template)
    cr   = t.fq(t.ds_sales, t.close_reason)
    eij  = t.fq(t.ds_refined, t.partner_matching)

    return dedent(f"""
        WITH
          rp AS (
            SELECT
              active                                   AS Active_Establishment,
              country_id,
              id                                       AS Partner_ID,
              parent_id                                AS Company_ID,
              country_id                               AS CountryID,
              name                                     AS Establishment_Name,
              dish_partner_uuid                        AS Establishment_UID,
              dish_establishment_pos_debitor_number,
              vat
            FROM {rp}
            WHERE active = TRUE
          ),
          rcomp AS (
            SELECT
              id   AS CompanyID,
              name AS Company_Name
            FROM {rp}
          ),
          base_assets AS (
            SELECT
              rp.Establishment_UID,
              rp.Partner_ID,
              rp.Company_ID,
              rcomp.Company_Name,
              rp.Establishment_Name,
              rc.code                                  AS Country_Code,
              pp.dish_product_code                     AS Product_Code,
              rp.dish_establishment_pos_debitor_number AS `Debnr infosys`,
              eij.Vestcode_infosys                     AS `Vestcode infosys`,
              sol.product_uom_qty                      AS qty,
              CAST(so.create_date AS DATE)             AS CreatedDate,
              CAST(so.start_date  AS DATE)             AS start_date,
              CAST(closed_at      AS DATE)             AS DisabledDate,
              CAST(so.end_date    AS DATE)             AS end_date
            FROM {sol} sol
            LEFT JOIN {so}  so  ON sol.order_id       = so.id
            LEFT JOIN rp        ON rp.partner_id      = so.partner_id
            LEFT JOIN rcomp     ON rcomp.CompanyID    = rp.Company_ID
            LEFT JOIN {rc}  rc  ON rp.country_id      = rc.id
            LEFT JOIN {pp}  pp  ON pp.id              = sol.product_id
            LEFT JOIN {pt}  pt  ON pp.product_tmpl_id = pt.id
            LEFT JOIN {eij} eij ON eij.establishment_id = rp.Establishment_UID
            LEFT JOIN {cr}  cr  ON cr.id              = so.close_reason_id
            WHERE
              pt.dish_product_code LIKE 'POS_%'
              AND so.state IN ('sale', 'done')
              AND so.active = TRUE
          ),
          sales_channel AS (
            SELECT
              Establishment_UID,
              CreatedDate  AS channel_created_date,
              DisabledDate AS channel_disabled_date,
              Product_Code AS feature_code,
              CASE
                WHEN Product_Code IN ('POS_QRPAY')   THEN 'QR'
                WHEN Product_Code IN ('POS_SSQR')    THEN 'QR'
                WHEN Product_Code IN ('POS_KIOSK')   THEN 'KIOSK'
                WHEN Product_Code IN ('POS_SSCANC')  THEN 'Self-Scan checkout'
                WHEN Product_Code IN ('POS_SSC')     THEN 'Self-Service checkout'
                WHEN Product_Code IN ('POS_WEBSHOP') THEN 'Webshop'
              END AS sales_channel_type
            FROM base_assets
            WHERE Product_Code IN (
              'POS_QRPAY',
              'POS_SSQR',
              'POS_KIOSK',
              'POS_SSCANC',
              'POS_SSC',
              'POS_WEBSHOP'
            )
              AND (DisabledDate IS NULL OR DATE(DisabledDate) >= CURRENT_DATE())
          )
        SELECT
          'Sales Channels for MACH2 customers' AS list_type,
          a.Country_Code,
          a.Establishment_UID,
          a.Company_ID,
          a.Company_Name,
          a.Establishment_Name,
          a.CreatedDate,
          a.DisabledDate,
          a.Product_Code AS product_code,
          sc.feature_code,
          sc.channel_created_date,
          sc.channel_disabled_date,
          sc.sales_channel_type,
          `Debnr infosys` as `Debitor_Number`
        FROM base_assets a
        INNER JOIN sales_channel sc USING (Establishment_UID)
        WHERE
          a.Product_Code LIKE 'POS_M2_BASIC%'
          AND (a.DisabledDate IS NULL OR DATE(a.DisabledDate) > CURRENT_DATE())
          AND CreatedDate = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
          
    """).strip()


def _query_mach2_cancellation_addons(t: BQTables) -> str:
    """Mach2 Cancellation Addons – old-main-license customers who cancelled an old addon yesterday."""
    rp  = t.fq(t.ds_sales, t.res_partner)
    so  = t.fq(t.ds_sales, t.sale_order)
    sol = t.fq(t.ds_sales, t.sale_order_line)
    rc  = t.fq(t.ds_sales, t.res_country)
    pp  = t.fq(t.ds_sales, t.product_product)
    pt  = t.fq(t.ds_sales, t.product_template)
    cr  = t.fq(t.ds_sales, t.close_reason)
    eij = t.fq(t.ds_refined, t.partner_matching)

    return dedent(f"""
        WITH
          ao AS (
            SELECT
              rp.dish_partner_uuid                     AS establishment_id,
              so.split_from_id                         AS Sale_Id,
              so.id                                    AS Id,
              SUM(
                CASE
                  WHEN pp.dish_product_code LIKE 'POS_L_Lic%'
                    OR pp.dish_product_code LIKE 'POS_L_DPL%'
                    THEN 1
                  ELSE 0
                END
              )                                        AS Number,
              MIN(so.date_order)                       AS CreatedDate
            FROM {sol} sol
            LEFT JOIN {so} so ON so.id        = sol.order_id
            LEFT JOIN {rp} rp ON rp.id        = so.partner_id
            LEFT JOIN {pp} pp ON sol.product_id = pp.id
            WHERE
              so.active = TRUE
              AND (
                pp.dish_product_code LIKE 'POS_L_Lic%'
                OR pp.dish_product_code LIKE 'POS_L_DPL%'
              )
              AND so.state IN ('done', 'sale')
            GROUP BY ALL
          ),
          base AS (
            SELECT
              rp.dish_partner_uuid                       AS UID,
              rp.name                                    AS `Account Name`,
              rc.code                                    AS `Establishment CountryCode`,
              pp.dish_product_code                       AS Product_Code,
              CASE
                WHEN pt.name.en_US LIKE 'POS_L_Lice'      THEN 'POS License Package'
                WHEN pt.name.en_US = 'Licenza aggiuntiva'  THEN 'Additional POS License'
                ELSE pt.name.en_US
              END                                        AS ProductName,
              CASE
                WHEN pt.dish_product_code = 'POS_L_DAteV'                                THEN '452521'
                WHEN pt.dish_product_code = 'POS_L_Fiscalisation'                        THEN '452514'
                WHEN pt.dish_product_code IN ('POS_L_FO', 'POS_POSDEVICE')              THEN '452519'
                WHEN pt.dish_product_code = 'POS_L_Lite'                                 THEN '452525'
                WHEN pt.dish_product_code IN ('POS_L_ORDER2POS', 'POS_WEBSHOP')         THEN '452523'
                WHEN pt.dish_product_code IN ('POS_L_ORDERAGG', 'POS_ORDERAGG')         THEN '452524'
                WHEN pt.dish_product_code = 'POS_L_ORDERAGGMP'                           THEN '452524'
                WHEN pt.dish_product_code = 'POS_L_Licence'                              THEN '452510'
                WHEN pt.dish_product_code IN ('POS_L_QR', 'POS_SSQR')                  THEN '452518'
                WHEN pt.name.en_US = 'Licenza aggiuntiva'                                THEN '452519'
                WHEN pt.dish_product_code IN ('POS_L_QRPayment', 'POS_QRPAY')           THEN '452517'
                WHEN pt.dish_product_code IN ('POS_L_TapToPayConnected', 'POS_EFTDEVICE') THEN '452526'
                WHEN pt.dish_product_code = 'PT_PTPayAccept'                             THEN '452513'
              END                                        AS `Partner Article Number`,
              rp.dish_establishment_pos_debitor_number   AS `Debnr infosys`,
              eij.Vestcode_infosys                       AS `Vestcode infosys`,
              sol.product_uom_qty                        AS qty,
              CAST(closed_at   AS DATE)                  AS closed_at,
              CAST(so.end_date AS DATE)                  AS end_date
            FROM {sol} sol
            LEFT JOIN {so}  so  ON sol.order_id       = so.id
            LEFT JOIN {rp}  rp  ON rp.id              = so.partner_id
            LEFT JOIN {rc}  rc  ON rp.country_id      = rc.id
            LEFT JOIN {pp}  pp  ON pp.id              = sol.product_id
            LEFT JOIN {pt}  pt  ON pp.product_tmpl_id = pt.id
            LEFT JOIN {eij} eij ON eij.establishment_id = rp.dish_partner_uuid
            LEFT JOIN {cr}  cr  ON cr.id              = so.close_reason_id
            WHERE
              pt.dish_product_code IN (
                'POS_POSDEVICE', 'POS_SUMMER',  'POS_WINTER',   'POS_ORDERAGG',
                'POS_SSQR',      'POS_QRPAY',   'POS_GIFT',     'POS_WEBSHOP',
                'POS_EFTDEVICE', 'POS_FATTURA_ELETTRONICA',      'POS_CONTO_ALLA_ROMANA',
                'POS_PM',        'POS_KIOSK',   'POS_LOYALITY', 'POS_VEMT',
                'POS_CUSTDISP',  'POS_SSCANC',  'POS_SSC'
              )
              AND CAST(so.closed_at AS DATE) <= CURRENT_DATE()
              AND CAST(so.end_date  AS DATE) = CURRENT_DATE() - 1
              AND pt.name.en_US NOT LIKE '%Package%'
              AND rp.dish_establishment_pos_debitor_number IS NOT NULL
              AND so.state IN ('sale', 'done')
              AND so.active = TRUE
              AND cr.name != 'The subscription was renewed with a new plan'
              AND cr.name IS NOT NULL
          )
        SELECT
          ao.establishment_id                          AS UID,
          `Account Name`,
          `Establishment CountryCode`,
          Product_Code,
          ProductName,
          `Partner Article Number`,
          `Debnr infosys`,
          `Vestcode infosys`,
          SUM(qty)                                     AS Disabled,
          closed_at,
          end_date
        FROM ao
        INNER JOIN base ON ao.establishment_id = base.UID
        GROUP BY ALL
    """).strip()


def _query_pos_cancellation(t: BQTables) -> str:
    """POS Cancellation – POS_L_* products cancelled yesterday."""
    rp  = t.fq(t.ds_sales, t.res_partner)
    so  = t.fq(t.ds_sales, t.sale_order)
    sol = t.fq(t.ds_sales, t.sale_order_line)
    rc  = t.fq(t.ds_sales, t.res_country)
    pp  = t.fq(t.ds_sales, t.product_product)
    pt  = t.fq(t.ds_sales, t.product_template)
    cr  = t.fq(t.ds_sales, t.close_reason)
    eij = t.fq(t.ds_refined, t.partner_matching)

    return dedent(f"""
        WITH base AS (
          SELECT
            rp.dish_partner_uuid                       AS UID,
            rp.name                                    AS `Account Name`,
            rc.code                                    AS `Establishment CountryCode`,
            CASE
              WHEN pt.name.en_US LIKE 'POS_L_Lice'      THEN 'POS License Package'
              WHEN pt.name.en_US = 'Licenza aggiuntiva'  THEN 'Additional POS License'
              ELSE pt.name.en_US
            END                                        AS ProductName,
            CASE
              WHEN pt.dish_product_code = 'POS_L_DAteV'            THEN '452521'
              WHEN pt.dish_product_code = 'POS_L_Fiscalisation'    THEN '452514'
              WHEN pt.dish_product_code = 'POS_L_FO'               THEN '452519'
              WHEN pt.dish_product_code = 'POS_L_Lite'             THEN '452525'
              WHEN pt.dish_product_code = 'POS_L_ORDER2POS'        THEN '452523'
              WHEN pt.dish_product_code = 'POS_L_ORDERAGG'         THEN '452524'
              WHEN pt.dish_product_code = 'POS_L_ORDERAGGMP'       THEN '452524'
              WHEN pt.dish_product_code = 'POS_L_Licence'          THEN '452510'
              WHEN pt.dish_product_code = 'POS_L_QR'               THEN '452518'
              WHEN pt.name.en_US = 'Licenza aggiuntiva'            THEN '452519'
              WHEN pt.dish_product_code = 'POS_L_QRPayment'        THEN '452517'
              WHEN pt.dish_product_code = 'POS_L_TapToPayConnected' THEN '452526'
              WHEN pt.dish_product_code = 'PT_PTPayAccept'         THEN '452513'
            END                                        AS `Partner Article Number`,
            rp.dish_establishment_pos_debitor_number   AS `Debnr infosys`,
            eij.Vestcode_infosys                       AS `Vestcode infosys`,
            sol.product_uom_qty                        AS qty,
            CAST(closed_at   AS DATE)                  AS closed_at,
            CAST(so.end_date AS DATE)                  AS end_date
          FROM {sol} sol
          LEFT JOIN {so}  so  ON sol.order_id       = so.id
          LEFT JOIN {rp}  rp  ON rp.id              = so.partner_id
          LEFT JOIN {rc}  rc  ON rp.country_id      = rc.id
          LEFT JOIN {pp}  pp  ON pp.id              = sol.product_id
          LEFT JOIN {pt}  pt  ON pp.product_tmpl_id = pt.id
          LEFT JOIN {eij} eij ON eij.establishment_id = rp.dish_partner_uuid
          LEFT JOIN {cr}  cr  ON cr.id              = so.close_reason_id
          WHERE
            pt.dish_product_code LIKE 'POS_L%'
            AND CAST(so.closed_at AS DATE) <= CURRENT_DATE()
            AND CAST(so.end_date  AS DATE) = CURRENT_DATE() - 1
            AND pt.name.en_US NOT LIKE '%Package%'
            AND rp.dish_establishment_pos_debitor_number IS NOT NULL
            AND so.state IN ('sale', 'done')
            AND so.active = TRUE
            AND cr.name != 'The subscription was renewed with a new plan'
            AND cr.name IS NOT NULL
        )
        SELECT
          UID,
          `Account Name`,
          `Establishment CountryCode`,
          ProductName,
          `Partner Article Number`,
          `Debnr infosys`,
          `Vestcode infosys`,
          SUM(qty) AS Disabled,
          closed_at,
          end_date
        FROM base
        GROUP BY ALL
    """).strip()



def _query_pos_activation(t: BQTables) -> str:
    """Report – POS Activation list (yesterday's activations)."""
    rp   = t.fq(t.ds_sales, t.res_partner)
    so   = t.fq(t.ds_sales, t.sale_order)
    sol  = t.fq(t.ds_sales, t.sale_order_line)
    pp   = t.fq(t.ds_sales, t.product_product)
    pt   = t.fq(t.ds_sales, t.product_template)
    rc   = t.fq(t.ds_sales, t.res_country)
    mtv  = t.fq(t.ds_sales, "odoo_mail_tracking_value")
    eij  = t.fq(t.ds_refined, t.partner_matching)

    return dedent(f"""
        WITH
          excluded_product_codes AS (
            SELECT code
            FROM UNNEST([
              'POS_M2_BASIC_DE',
              'POS_M2_BASIC_FR',
              'POS_M2_BASIC_IT',
              'POS_M2_BASIC_ES',
              'POS_POSDEVICE',
              'POS_SUMMER',
              'POS_WINTER',
              'POS_ORDERAGG',
              'POS_SSQR',
              'POS_QRPAY',
              'POS_GIFT',
              'POS_WEBSHOP',
              'POS_EFTDEVICE',
              'POS_FATTURA_ELETTRONICA',
              'POS_CONTO_ALLA_ROMANA',
              'POS_PM',
              'POS_KIOSK',
              'POS_LOYALITY',
              'POS_VEMT',
              'POS_CUSTDISP',
              'POS_SSCANC',
              'POS_SSC'
            ]) AS code
          ),
          rp AS (
            SELECT
              active                                     AS Active_Establishment,
              id                                         AS Partner_ID,
              parent_id                                  AS Company_ID,
              country_id                                 AS CountryID,
              name                                       AS Establishment_Name,
              dish_partner_uuid                          AS Establishment_UID,
              dish_establishment_machine_code            AS Machine_Code,
              dish_establishment_fiskaltrust_acces_token AS FT_Access_Token_Badge,
              dish_establishment_fiskaltrust_cashboxID   AS FT_Cashbox_Id,
              dish_establishment_pos_clientID            AS Client_ID,
              dish_establishment_pos_licenseID           AS License_ID,
              vat                                        AS vat
            FROM {rp}
            WHERE active = TRUE
          ),
          rcomp AS (
            SELECT
              id     AS CompanyID,
              name   AS Company_Name,
              street AS Company_Street,
              zip    AS Company_Zip_Code,
              city   AS Company_City,
              email  AS Email,
              mobile AS Mobile_Phone
            FROM {rp}
          ),
          ao AS (
            SELECT
              so.split_from_id   AS Sale_Id,
              so.id              AS Id,
              rp.Establishment_UID,
              SUM(
                CASE
                  WHEN pp.dish_product_code LIKE 'POS_L_Lic%'
                    OR pp.dish_product_code LIKE 'POS_L_DPL%'
                    THEN 1
                  ELSE 0
                END
              )                  AS Number,
              MIN(so.date_order) AS CreatedDate
            FROM {sol} sol
            LEFT JOIN {so} so ON so.id          = sol.order_id
            LEFT JOIN {pp} pp ON sol.product_id = pp.id
            LEFT JOIN rp       ON so.partner_id = rp.Partner_ID
            WHERE
              so.active = TRUE
              AND (
                pp.dish_product_code LIKE 'POS_%'
                OR pp.dish_product_code LIKE 'PT_PTPay%'
              )
              AND pp.dish_product_code NOT LIKE 'POS_M2_BASIC_%'
              AND pp.dish_product_code NOT IN (SELECT code FROM excluded_product_codes)
              AND so.state IN ('done', 'sale')
            GROUP BY ALL
          ),
          cust_with_main_license AS (
            SELECT *
            FROM ao
            WHERE Number > 0
          ),
          mtv AS (
            SELECT
              field_desc     AS Field,
              new_value_char AS New_Value,
              create_date    AS Create_Date,
              res_id         AS Establishment
            FROM {mtv}
            WHERE
              (field = 16229 AND DATE(write_date) = DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY))
              OR (field = 16230 AND DATE(write_date) = DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY))
          ),
          so AS (
            SELECT
              id                      AS Sale_Order_Id,
              split_from_id           AS Split_from_Id,
              date_order              AS Order_Date,
              origin                  AS Origin,
              partner_id              AS PartnerID,
              stage_category          AS Stage,
              state                   AS State,
              start_date              AS Start_Date,
              active                  AS Active,
              subscription_management AS Migration_Direction
            FROM {so}
            WHERE active = TRUE AND state IN ('done', 'sale')
          ),
          sol AS (
            SELECT
              Asset_UID,
              Order_UID,
              Description,
              ProductID,
              instance
            FROM (
              SELECT
                id             AS Asset_UID,
                order_id       AS Order_UID,
                name           AS Description,
                product_id     AS ProductID,
                GENERATE_ARRAY(1, product_uom_qty) AS instances
              FROM {sol}
            )
            CROSS JOIN UNNEST(instances) AS instance
          ),
          rc AS (
            SELECT DISTINCT
              id   AS Country_ID,
              code AS Country_Code,
              CASE code
                WHEN 'IT' THEN 'Italy'
                WHEN 'DE' THEN 'Germany'
                WHEN 'ES' THEN 'Spain'
                WHEN 'FR' THEN 'France'
                WHEN 'RO' THEN 'Romania'
                WHEN 'PT' THEN 'Portugal'
                WHEN 'PL' THEN 'Poland'
                WHEN 'NL' THEN 'Netherlands'
                WHEN 'BE' THEN 'Belgium'
                WHEN 'SK' THEN 'Slovakia'
                WHEN 'CZ' THEN 'Czech Republic'
                WHEN 'AT' THEN 'Austria'
                WHEN 'TR' THEN 'Turkey'
                WHEN 'UA' THEN 'Ukraine'
                WHEN 'HU' THEN 'Hungary'
                WHEN 'RU' THEN 'Russia'
                WHEN 'HR' THEN 'Croatia'
                ELSE 'country unknown'
              END AS Country
            FROM {rc}
          ),
          pp AS (
            SELECT DISTINCT
              id              AS Product_ID,
              product_tmpl_id AS Product_Template_ID
            FROM {pp}
          ),
          pt AS (
            SELECT DISTINCT
              id                AS Product_TemplateID,
              dish_product_code,
              recurring_invoice,
              name.en_US        AS Product_Name
            FROM {pt}
          )
        SELECT DISTINCT
          Country,
          rp.Establishment_UID                         AS `Establishment UID`,
          Company_Name                                 AS `Company Name`,
          Establishment_Name                           AS `Establishment Name`,
          CASE
            WHEN Product_Name = 'Access Point EA U6 Pro'                                   THEN 'POS Access Point'
            WHEN Product_Name = 'Additional POS License'                                   THEN 'POS Additional Front-Office License'
            WHEN Product_Name = 'Additional POS License Flex'                             THEN 'POS Additional Front-Office License'
            WHEN Product_Name = 'Cash Drawer'                                             THEN 'POS Cash Drawer'
            WHEN Product_Name = 'DateV'                                                   THEN 'DAteV'
            WHEN Product_Name = 'Pay Tap to Pay Terminal'                            THEN 'Tap to Pay Android Hardware'
            WHEN Product_Name = 'POS Front Office License - Summer'                  THEN 'Summer License'
            WHEN Product_Name = 'POS Front Office License - Winter'                  THEN 'Winter License'
            WHEN Product_Name = 'Electronic Invoice Flex'                                 THEN 'Electronic invoice'
            WHEN Product_Name = 'Fiscal Printer Maintenance'                              THEN 'Fiscal printer maintenance service'
            WHEN Product_Name = 'Fiscalisation License'                                   THEN 'POS Fiscalisation License'
            WHEN Product_Name = 'Gift Card Management Flex'                               THEN 'Gift Card Management'
            WHEN Product_Name = 'Handheld Sumi'                                           THEN 'Handheld Sunmi'
            WHEN Product_Name = 'Licenza aggiuntiva'                                      THEN 'POS Additional Front-Office License'
            WHEN Product_Name = 'Licenza aggiuntiva Flex'                                 THEN 'POS Additional Front-Office License'
            WHEN Product_Name = 'Mini Server M3'                                          THEN 'Mini Server M3 Pro'
            WHEN Product_Name = 'Mini Server P1'                                          THEN 'Miniserver'
            WHEN Product_Name = 'Order Aggregation'                                       THEN 'Order Aggregation: Otter'
            WHEN Product_Name = 'Order2POS Flex'                                          THEN 'Order2POS'
            WHEN Product_Name = 'POS Consultant'                                          THEN 'Installation Only'
            WHEN Product_Name = 'POS Lite Screen T3'                                      THEN 'T3 Lite Screen'
            WHEN Product_Name = 'POS Router'                                              THEN 'POS Router ER605'
            WHEN Product_Name = 'Printer DPT201'                                          THEN 'POS Printer DPT201'
            WHEN Product_Name = 'Printer TM-M30II'                                        THEN 'POS Printer TM30'
            WHEN Product_Name = 'QR Ordering License'                                     THEN 'POS QR Ordering License'
            WHEN Product_Name = 'QR Ordering License Flex'                                THEN 'POS QR Ordering License'
            WHEN Product_Name = 'QR Payment'                                              THEN 'POS QR Payment'
            WHEN Product_Name LIKE '%Verifone P400 Plus Hardware%'                        THEN 'Verifone P400 Plus'
            WHEN Product_Name LIKE '%S1F2 Hardware%'                                      THEN 'S1F2 mobile with printer'
            WHEN Product_Name LIKE '%V400c Hardware%'                                     THEN 'V400c desk with printer'
            WHEN Product_Name LIKE '%V400m Hardware%'                                     THEN 'Verifone V400m'
            WHEN Product_Name LIKE 'POS License%'                                         THEN 'POS License Package'
            WHEN Product_Name = 'Payment Acceptance'                                      THEN 'Platform Payment Acceptance License'
            WHEN Product_Name = 'DateV Flex'                                              THEN 'DAteV'
            WHEN Product_Name = 'Fiscalisation License Flex'                              THEN 'POS Fiscalisation License'
            WHEN Product_Name = 'Additional POS License Flex'                             THEN 'POS Additional Front-Office License'
            WHEN Product_Name = 'Gift Card Management Flex'                               THEN 'Gift Card Management'
            WHEN Product_Name = 'POS License Flex'                                        THEN 'POS License Package'
            WHEN Product_Name = 'Licenza aggiuntiva Flex'                                 THEN 'POS Additional License IT'
            WHEN Product_Name = 'Order2POS Flex'                                          THEN 'Order2POS'
            WHEN Product_Name = 'POS License Package Flex - DE'                           THEN 'POS License Package'
            WHEN Product_Name = 'POS License Package Flex - ES'                           THEN 'POS License Package'
            WHEN Product_Name = 'POS License Package Flex - FR'                           THEN 'POS License Package'
            WHEN Product_Name = 'POS License Package Flex - IT'                           THEN 'POS License Package'
            ELSE Product_Name
          END                                          AS `Product Name`,
          CASE
            WHEN Product_Name LIKE 'Mini%' OR Product_Name LIKE '%Miniserver%'
              THEN Machine_Code
            ELSE NULL
          END                                          AS MachineCode,
          Company_Street                               AS `Company Street`,
          Company_Zip_Code                             AS `Company ZipPostal Code`,
          Company_City                                 AS `Company City`,
          Email,
          Mobile_Phone                                 AS `Mobile Phone`,
          CONCAT(Asset_UID, instance)                  AS `Asset UID`,
          Order_UID                                    AS `ORDER UID`,
          FT_Cashbox_Id                                AS `Fiskaltrust Cashbox ID`,
          CASE Country
            WHEN 'France' THEN 'http://host.docker.internal:1300/ftrest'
            WHEN 'Italy'  THEN 'http://host.docker.internal:5618'
            WHEN 'Spain'  THEN 'http://host.docker.internal:5618'
            ELSE               'http://host.docker.internal:1500/ftrest'
          END                                          AS `Fiskaltrust Queue URL`,
          CASE Country
            WHEN 'France' THEN '00000000-0000-4000-8000-000000000001'
            WHEN 'Italy'  THEN rp.Establishment_UID
            WHEN 'Spain'  THEN rp.Establishment_UID
            ELSE               '00000000-0000-4000-8000-000000000002'
          END                                          AS `Fiskaltrust POS System ID`,
          FT_Access_Token_Badge                        AS `FT Access TokenBadge`,
          CASE WHEN Country = 'Italy' THEN vat ELSE NULL END AS `VAT ID IT`,
          dish_product_code,
        FROM sol
        LEFT JOIN so     ON so.Sale_Order_Id  = sol.Order_UID
                         OR sol.Order_UID     = so.Split_from_Id
        LEFT JOIN rp     ON so.PartnerID      = rp.Partner_ID
        LEFT JOIN rcomp  ON rcomp.CompanyID   = rp.Company_ID
        LEFT JOIN rc     ON rc.Country_ID     = rp.CountryID
        LEFT JOIN pp     ON sol.ProductID     = pp.Product_ID
        LEFT JOIN pt     ON pt.Product_TemplateID = pp.Product_Template_ID
        LEFT JOIN mtv    ON so.PartnerID      = mtv.Establishment
        LEFT JOIN ao     ON sol.Order_UID     = ao.Sale_Id
                         OR sol.Order_UID     = ao.Id
        WHERE
          rp.Establishment_UID IN (
            SELECT DISTINCT Establishment_UID FROM cust_with_main_license
          )
          AND pt.dish_product_code NOT LIKE 'POS_M2_BASIC_%'
          AND pt.dish_product_code NOT IN (SELECT code FROM excluded_product_codes)
          AND (
            (
              (
                dish_product_code LIKE 'POS_%'
                OR dish_product_code LIKE 'PT_PTPayAccept%'
                OR Product_Name LIKE '%Verifone P400 Plus Hardware%'
                OR Product_Name LIKE '%V400m Hardware%'
                OR Product_Name LIKE '%S1F2 Hardware%'
                OR Product_Name LIKE '%V400c Hardware%'
              )
              AND Product_Name NOT IN ('POS Consultant', 'Menu Setup Service')
              AND Product_Name NOT LIKE 'POS License Package%'
              AND FT_Cashbox_Id IS NOT NULL
              AND FT_Access_Token_Badge IS NOT NULL
              AND Machine_Code IS NOT NULL
              AND License_ID IS NULL
              AND Create_Date IS NOT NULL
              AND Number > 0
              AND so.Migration_Direction = 'create'
            )
            OR (
              (
                dish_product_code LIKE 'POS_L_%'
                OR dish_product_code LIKE 'PT_PTPayAccept%'
              )
              AND Product_Name NOT IN ('POS Consultant', 'Menu Setup Service')
              AND Product_Name NOT LIKE 'POS License Package%'
              AND FT_Cashbox_Id IS NOT NULL
              AND FT_Access_Token_Badge IS NOT NULL
              AND Machine_Code IS NOT NULL
              AND (
                (License_ID IS NULL AND Create_Date IS NOT NULL)
                OR (DATE(CreatedDate) = DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY))
              )
              AND Number = 0
              AND so.Migration_Direction = 'create'
            )
          )
        ORDER BY rp.Establishment_UID ASC
    """).strip()



# ── Excel helpers ─────────────────────────────────────────────────────────────

_HEADER_FILL  = PatternFill("solid", fgColor="1F4E79")
_HEADER_FONT  = Font(name="Arial", bold=True, color="FFFFFF", size=10)
_DATA_FONT    = Font(name="Arial", size=10)
_ALT_FILL     = PatternFill("solid", fgColor="D6E4F0")
_CENTER_ALIGN = Alignment(horizontal="center", vertical="center", wrap_text=True)
_LEFT_ALIGN   = Alignment(horizontal="left",   vertical="center", wrap_text=False)


def _df_to_excel(df: pd.DataFrame, sheet_name: str) -> bytes:
    """Render a DataFrame into a formatted Excel workbook and return raw bytes."""
    wb = Workbook()
    ws = wb.active
    ws.title = sheet_name[:31]  # Excel tab name limit

    headers = list(df.columns)
    ws.append(headers)

    # Style header row
    for col_idx, _ in enumerate(headers, start=1):
        cell = ws.cell(row=1, column=col_idx)
        cell.font      = _HEADER_FONT
        cell.fill      = _HEADER_FILL
        cell.alignment = _CENTER_ALIGN

    # Write data rows with alternating fill
    for row_idx, row in enumerate(df.itertuples(index=False), start=2):
        fill = _ALT_FILL if row_idx % 2 == 0 else None
        for col_idx, value in enumerate(row, start=1):
            cell = ws.cell(row=row_idx, column=col_idx, value=value)
            cell.font      = _DATA_FONT
            cell.alignment = _LEFT_ALIGN
            if fill:
                cell.fill = fill

    # Auto-fit column widths (capped at 60)
    for col_idx, col_name in enumerate(headers, start=1):
        col_values  = df.iloc[:, col_idx - 1].astype(str)
        max_content = max(col_values.str.len().max(), len(col_name)) if len(df) else len(col_name)
        ws.column_dimensions[get_column_letter(col_idx)].width = min(max_content + 4, 60)

    # Freeze header row
    ws.freeze_panes = "A2"

    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


# ── Email templates (see templates/ package) ──────────────────────────────────

# Allow ``from templates...`` when Airflow scans this file under dags/ (no .airflowignore).
_MACH2_DIR = Path(__file__).resolve().parent
if str(_MACH2_DIR) not in sys.path:
    sys.path.insert(0, str(_MACH2_DIR))

from templates.activation_addons    import build as email_body_activations
from templates.sales_channels       import build as email_body_sales_channels
from templates.cancellation         import build as email_body_cancellations
from templates.pos_cancellation    import build as email_body_pos_cancellation
from templates.pos_activation   import build as email_body_pos_activation


# ── BigQuery execution ────────────────────────────────────────────────────────


def run_query(client: bigquery.Client, sql: str, report_name: str, project: str) -> pd.DataFrame:
    log.info("Running query: %s", report_name)
    job_config = bigquery.QueryJobConfig(
        # Explicitly set the billing/quota project so ADC user credentials
        # don't cause "quota exceeded" or silent empty results.
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        use_legacy_sql=False,
    )
    job = client.query(sql, job_config=job_config, project=project)
    rows = list(job.result())
    if not rows:
        df = pd.DataFrame()
    else:
        # Build via REST row iterator — avoids BigQuery Storage API permissions
        # (bigquery.readsessions.create) required by to_dataframe()/to_arrow().
        df = pd.DataFrame([dict(row.items()) for row in rows])
    log.info("  → %d rows returned", len(df))
    return df


# ── Orchestration ─────────────────────────────────────────────────────────────


def _parse_recipients(env_key: str, required: bool = True) -> list[str]:
    """Parse a comma-separated list of email addresses from an env var."""
    raw = _env(env_key, required=required)
    return [addr.strip() for addr in raw.split(",") if addr.strip()]


def build_report_specs(
    *,
    run_label: str,
) -> list[ReportSpec]:
    """Return report definitions for the current run."""
    return [
        ReportSpec(
            name="Mach2 Sales Channels",
            filename_prefix="mach2_sales_channels",
            sheet_name="Sales Channels",
            subject=f"Mach2 Sales Channels [{run_label}]",
            html_body=email_body_sales_channels,
            recipient_env_key="EMAIL_TO_REPORT_1",
            query_fn=_query_sales_channels,
        ),
        ReportSpec(
            name="Mach2 Activation Addons",
            filename_prefix="mach2_activation_addons",
            sheet_name="Activations",
            subject=f"Mach2 Activation Addons [{run_label}]",
            html_body=email_body_activations,
            recipient_env_key="EMAIL_TO_REPORT_2",
            query_fn=_query_activations,
            rename_cols={
                "Company ZipPostal Code": "Company Zip/Postal Code",
                "FT Access TokenBadge": "FT Access Token/Badge",
                "VAT ID IT": "VAT ID (IT)",
            },
        ),
        ReportSpec(
            name="Mach2 Cancellation Addons",
            filename_prefix="mach2_cancellation_addons",
            sheet_name="Cancellation Addons",
            subject=f"Mach2 Cancellation Addons [{run_label}]",
            html_body=email_body_cancellations,
            recipient_env_key="EMAIL_TO_REPORT_2",
            query_fn=_query_mach2_cancellation_addons,
        ),
        ReportSpec(
            name="POS Cancellation",
            filename_prefix="pos_cancellation",
            sheet_name="POS Cancellation",
            subject=f"POS Cancellation [{run_label}]",
            html_body=email_body_pos_cancellation,
            recipient_env_key="EMAIL_TO_REPORT_2",
            query_fn=_query_pos_cancellation,
        ),
        ReportSpec(
            name="POS Activation",
            filename_prefix="pos_activation",
            sheet_name="Activation",
            subject=f"POS Activation [{run_label}]",
            html_body=email_body_pos_activation,
            recipient_env_key="EMAIL_TO_REPORT_3",
            query_fn=_query_pos_activation,
            rename_cols={
                "Company_ZipPostal_Code": "Company Zip/Postal Code",
                "FT_Access_TokenBadge": "FT Access Token/Badge",
                "VAT_ID_IT": "VAT ID (IT)",
                "Company ZipPostal Code": "Company Zip/Postal Code",
                "FT Access TokenBadge": "FT Access Token/Badge",
                "VAT ID IT": "VAT ID (IT)",
            },
        ),
    ]


def generate_reports(output_dir: Path | str) -> list[dict[str, Any]]:
    """
    Run all Mach2 queries, write Excel attachments to ``output_dir``, and return
    serializable email payloads for the Airflow email task.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    tables = BQTables.from_env()
    bq_client = bigquery.Client(project=tables.project)
    now = datetime.now(tz=timezone.utc).astimezone(ZoneInfo("Europe/Berlin"))
    run_ts = now.strftime("%Y%m%d_%H%M%S")
    run_date = now.strftime("%d %b %Y")       # e.g. "08 Jun 2026"
    run_label = now.strftime("%d.%m.%Y")       # e.g. "08.06.2026" — used in subjects
    run_ts_disp = now.strftime("%Y-%m-%d %H:%M %Z")  # shown inside email body

    payloads: list[dict[str, Any]] = []
    for spec in build_report_specs(run_label=run_label):
        log.info("━━━ Processing: %s ━━━", spec.name)

        try:
            sql = spec.query_fn(tables)
            df = run_query(bq_client, sql, spec.name, tables.project)
        except Exception:
            log.exception("BigQuery error for report '%s' – skipping.", spec.name)
            continue

        filename = f"{spec.filename_prefix}_{run_ts}.xlsx"
        recipients = _parse_recipients(spec.recipient_env_key)
        cc = _parse_recipients("EMAIL_CC", required=False)
        rendered_html = spec.html_body(
            df=df, run_date=run_date, run_ts=run_ts_disp, filename=filename
        )

        attachment_path: str | None = None
        if df.empty:
            log.info(
                "Report '%s' returned 0 rows – email will be sent with no attachment.",
                spec.name,
            )
        else:
            if spec.rename_cols:
                df = df.rename(columns=spec.rename_cols)
            try:
                excel_bytes = _df_to_excel(df, spec.sheet_name)
                attachment_path = str(output_dir / filename)
                Path(attachment_path).write_bytes(excel_bytes)
            except Exception:
                log.exception("Excel generation failed for '%s' – skipping.", spec.name)
                continue

        payloads.append(
            {
                "report_name": spec.name,
                "subject": spec.subject,
                "recipients": recipients,
                "cc": cc,
                "html_body": rendered_html,
                "attachment_path": attachment_path,
                "row_count": len(df),
                "plain_text_summary": (
                    f"{spec.name} — {len(df)} row(s). "
                    f"Generated {run_ts_disp}."
                ),
            }
        )

    log.info("Generated %d report payload(s).", len(payloads))
    return payloads


def main() -> None:
    """Local entrypoint: generate reports then send via email_delivery."""
    try:
        from email_delivery import EmailDelivery, EmailMessage
    except ImportError:
        log.error(
            "Cannot import email_delivery. "
            "Run via Airflow or set PYTHONPATH to the dags folder."
        )
        raise

    staging = Path(os.getenv("MACH2_STAGING_DIR", "/tmp/mach2_reports"))
    payloads = generate_reports(staging)
    delivery = EmailDelivery.from_env()
    for payload in payloads:
        try:
            delivery.send(
                EmailMessage(
                    recipients=payload["recipients"],
                    subject=payload["subject"],
                    html_body=payload["html_body"],
                    cc=payload.get("cc") or None,
                    attachment_path=payload.get("attachment_path"),
                    plain_text_summary=payload.get("plain_text_summary"),
                )
            )
        except Exception:
            log.exception("Failed to send email for report '%s'.", payload["report_name"])

    log.info("All reports processed.")


if __name__ == "__main__":
    main()