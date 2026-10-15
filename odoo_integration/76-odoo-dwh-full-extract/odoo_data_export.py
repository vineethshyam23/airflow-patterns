"""
Odoo Postgres → NDJSON extract class for the DWH inbound spine.

Production ships ~105 per-table pull methods (~2.6k lines). This folder
keeps the shared machinery plus a cross-domain subset that covers every
extract style: full dim, create/write lookback, mail_message rename +
external-ID join, CRM log-note filter, and 50k-row chunked junction
tables.

Credentials stay in Airflow Variables. Connection is lazy — opening
Postgres at construct time used to contribute to DagBag parse timeouts
when the DAG imported this module for ~105 tables.

Source (read-only):
  dags/horeca_digital/odoo_data_export.py (OdooDataPull)
"""

from __future__ import annotations

import decimal
import json
import logging
import os
import time
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Union

import psycopg2
from airflow.models import Variable

from odoo_postgres import connect_odoo_postgres

# Incremental extract: today + yesterday. Do not raise without a catch-up plan.
ODOO_INCREMENTAL_LOOKBACK_DAYS = 1

# Tables that use create/write date lookback instead of full scan.
INCREMENTAL_TABLES = frozenset(
    {
        "ir_model_data",
        "account_move",
        "account_move_line",
        "res_partner",
        "sale_order_log",
        "sale_order",
        "sale_order_line",
        "helpdesk_ticket",
        "sdd_mandate",
        "account_payment",
        "res_partner_bank",
        "crm_lead",
    }
)


def _odoo_incremental_date_sql(
    create_col: str = "create_date", write_col: str = "write_date"
) -> str:
    """Postgres filter: DATE(create/write) in CURRENT_DATE .. CURRENT_DATE-N."""
    dates = ", ".join(
        ["CURRENT_DATE"]
        + [
            f"CURRENT_DATE - INTERVAL '{i} day'"
            for i in range(1, ODOO_INCREMENTAL_LOOKBACK_DAYS + 1)
        ]
    )
    return f"(DATE({create_col}) IN ({dates}) OR DATE({write_col}) IN ({dates}))"


class DateTimeEncoder(json.JSONEncoder):
    """Serialize datetime/date/Decimal for NDJSON landing."""

    def default(self, obj: Any) -> Any:
        if isinstance(obj, datetime):
            return obj.strftime("%Y-%m-%d %H:%M:%S")
        if isinstance(obj, date):
            return obj.strftime("%Y-%m-%d")
        if isinstance(obj, decimal.Decimal):
            return str(obj)
        return super().default(obj)


class OdooDataPull:
    """Pull selected Odoo tables to local NDJSON for GCS upload."""

    def __init__(
        self,
        env: str = "DM",
        project_id: str = "dwh_project_dev",
        tmp_loc: Optional[str] = None,
        creds: Optional[Dict[str, str]] = None,
    ):
        odoo_creds_var = f"odoo_{env.lower()}_creds"
        odoo_creds = creds or Variable.get(odoo_creds_var, deserialize_json=True)
        self.project_id = project_id
        self.tmp_loc = tmp_loc or "/tmp/odoo_extract/"
        self._endpoint = odoo_creds["hostname"]
        self._db_name = odoo_creds["database"]
        self._db_user = odoo_creds["db_user"]
        self._db_pwd = odoo_creds["db_pwd"]
        self._conn = None
        logging.info("Odoo %s instance creds acquired (project=%s)", env, project_id)

    @property
    def conn(self):
        """Lazy PostgreSQL connection — opened on first query, not at construct."""
        if self._conn is not None and not self._conn.closed:
            return self._conn

        last_error = None
        for attempt in range(1, 4):
            logging.info(
                "Opening Odoo PostgreSQL connection host=%s attempt=%s/3",
                self._endpoint,
                attempt,
            )
            try:
                self._conn = connect_odoo_postgres(
                    host=self._endpoint,
                    database=self._db_name,
                    user=self._db_user,
                    password=self._db_pwd,
                )
                logging.info(
                    "Opened Odoo PostgreSQL connection host=%s", self._endpoint
                )
                return self._conn
            except psycopg2.OperationalError as exc:
                last_error = exc
                logging.warning(
                    "Odoo Postgres connect attempt %s/3 failed host=%s: %s",
                    attempt,
                    self._endpoint,
                    exc,
                )
                if attempt < 3:
                    time.sleep(10 * attempt)
        raise last_error

    def close(self) -> None:
        if self._conn is not None and not self._conn.closed:
            self._conn.close()
            logging.info("Closed Odoo PostgreSQL connection")
        self._conn = None

    def pull_odoo_data(self, table_name: str, query: str) -> None:
        """Execute query and write NDJSON (or 50k-row chunks for junction tables)."""
        conn = self.conn
        cur = conn.cursor()
        cur.execute(query)

        output_name = (
            "mail_message_email_activities"
            if table_name == "mail_message"
            else table_name
        )

        if table_name != "account_tax_sale_order_line_rel":
            records = cur.fetchall()
            logging.info("Table: %s Total rows: %s", table_name, len(records))
            out_path = f"{self.tmp_loc}{output_name}.json"
            with open(out_path, "w") as outfile:
                for row in records:
                    row_dict = {
                        desc.name: value
                        for desc, value in zip(cur.description, row)  # type: ignore
                    }
                    row_dict["load_date"] = date.today().strftime("%Y-%m-%d")
                    json.dump(row_dict, outfile, cls=DateTimeEncoder)
                    outfile.write("\n")
                outfile.flush()
                os.fsync(outfile.fileno())
            logging.info("Wrote %s bytes to %s", os.path.getsize(out_path), out_path)
        else:
            batch_dir = os.path.join(self.tmp_loc, table_name)
            os.makedirs(batch_dir, exist_ok=True)
            chunk_size = 50000
            chunk_idx = 0
            while True:
                records = cur.fetchmany(chunk_size)
                if not records:
                    break
                logging.info(
                    "Table: %s chunk %s rows: %s",
                    table_name,
                    chunk_idx,
                    len(records),
                )
                chunk_path = os.path.join(
                    batch_dir, f"{table_name}_{chunk_idx}.json"
                )
                with open(chunk_path, "w") as outfile:
                    for row in records:
                        row_dict = {
                            desc.name: value
                            for desc, value in zip(cur.description, row)  # type: ignore
                        }
                        row_dict["load_date"] = date.today().strftime("%Y-%m-%d")
                        json.dump(row_dict, outfile, cls=DateTimeEncoder)
                        outfile.write("\n")
                    outfile.flush()
                    os.fsync(outfile.fileno())
                chunk_idx += 1
            logging.info(
                "Wrote %s NDJSON chunks for %s under %s",
                chunk_idx,
                table_name,
                batch_dir,
            )
        cur.close()

    def pull_table_data(self, table_details: Dict[str, Any]) -> None:
        """Build the SELECT for one table (special cases + incremental list)."""
        table_name = table_details["table_name"]
        columns = table_details["column_names"]
        columns_str = (
            ", ".join(columns) if isinstance(columns, list) else columns
        )

        if table_name == "mail_message":
            # SFDC activity bridge rows only — not the entire chatter dump.
            query = f"""
            SELECT mm.* FROM {table_name} mm
            INNER JOIN ir_model_data imd ON imd.res_id = mm.id
              AND imd.model = 'mail.message'
              AND imd.module = '__sfdc_activity__'
            WHERE {_odoo_incremental_date_sql("mm.create_date", "mm.write_date")}
            """
        elif table_name == "mail_message_crm_log_notes":
            # Lead chatter comments since a fixed cutover date.
            query = """
            SELECT DISTINCT
                    id, parent_id, res_id, subtype_id, mail_activity_type_id,
                    author_id, create_uid, write_uid, subject, model,
                    record_name, message_type, email_from, message_id,
                    body, is_internal, "date", create_date, write_date
            FROM mail_message
            WHERE model = 'crm.lead'
              AND message_type = 'comment'
              AND body IS NOT NULL
              AND BTRIM(body) <> ''
              AND (DATE(create_date) >= DATE '2025-05-01'
                   OR DATE(write_date) >= DATE '2025-05-01')
            """
        elif table_name == "helpdesk_team":
            query = (
                f"SELECT ht.{columns_str}, ht.name ->> 'en_US' AS name "
                f"FROM {table_name} ht"
            )
        elif table_name in INCREMENTAL_TABLES:
            query = (
                f"SELECT {columns_str} FROM {table_name} "
                f"WHERE {_odoo_incremental_date_sql()}"
            )
        else:
            query = f"SELECT {columns_str} FROM {table_name}"

        self.pull_odoo_data(table_name, query)

    def pull_specific_table(
        self, table_name: str, column_names: Union[List[str], str]
    ) -> None:
        self.pull_table_data(
            {"table_name": table_name, "column_names": column_names}
        )

    # --- Representative pull methods (subset of ~105 production tables) ---

    def odoo_crm_stage_pull(self) -> None:
        self.pull_specific_table(
            "crm_stage",
            [
                "id",
                "sequence",
                "team_id",
                "name",
                "is_won",
                "fold",
                "create_date",
                "write_date",
            ],
        )

    def odoo_crm_lead_pull(self) -> None:
        # Brand-prefixed custom columns aliased to neutral names in SQL.
        columns = [
            "id",
            "contact_name",
            "email_from",
            "phone",
            "mobile",
            "country_id",
            "zip",
            "city",
            "street",
            "establishment_name",
            "name",
            "lang_id",
            "source_id",
            "medium_id",
            "partner_id",
            "partner_name",
            "team_id",
            "user_id",
            "stage_id",
            "type",
            "active",
            "probability",
            "expected_revenue",
            "campaign_id",
            "company_id",
            "create_date",
            "write_date",
            "date_closed",
            "date_open",
            "lost_reason_id",
            "won_status",
            "dish_ecom_campaign AS platform_ecom_campaign",
            "dish_ecom_source AS platform_ecom_source",
            "dish_is_consent AS platform_is_consent",
            "dish_is_newsletter AS platform_is_newsletter",
            "dish_lang_id AS platform_lang_id",
            "dish_merchant AS platform_merchant",
            "dish_owner AS platform_owner",
            "dish_product AS platform_product",
            "dish_reseller AS platform_reseller",
            "dish_tracking_id AS platform_tracking_id",
            "dish_contact_id AS platform_contact_id",
            "vat",
            "page_url",
        ]
        self.pull_specific_table("crm_lead", columns)

    def odoo_crm_tag_pull(self) -> None:
        self.pull_specific_table(
            "crm_tag",
            ["id", "name", "color", "create_date", "write_date"],
        )

    def odoo_crm_tag_rel_pull(self) -> None:
        self.pull_specific_table(
            "crm_tag_rel",
            ["lead_id", "tag_id"],
        )

    def odoo_ir_model_data_pull(self) -> None:
        self.pull_specific_table(
            "ir_model_data",
            ["id", "res_id", "name", "module", "model", "create_date", "write_date"],
        )

    def odoo_mail_message_pull(self) -> None:
        # Column list unused — pull_table_data builds the SFDC join query.
        self.pull_specific_table("mail_message", "*")

    def odoo_mail_message_crm_log_notes_pull(self) -> None:
        self.pull_specific_table("mail_message_crm_log_notes", "*")

    def odoo_helpdesk_ticket_pull(self) -> None:
        columns = [
            "id",
            "name",
            "team_id",
            "stage_id",
            "user_id",
            "partner_id",
            "ticket_type_id",
            "priority",
            "create_date",
            "write_date",
            "close_date",
            "description",
        ]
        self.pull_specific_table("helpdesk_ticket", columns)

    def odoo_helpdesk_team_pull(self) -> None:
        self.pull_specific_table(
            "helpdesk_team",
            ["id", "active", "sequence", "create_date", "write_date"],
        )

    def odoo_res_partner_pull(self) -> None:
        columns = [
            "id",
            "name",
            "email",
            "phone",
            "mobile",
            "is_company",
            "parent_id",
            "country_id",
            "state_id",
            "city",
            "zip",
            "street",
            "street2",
            "vat",
            "active",
            "create_date",
            "write_date",
            "company_id",
            "user_id",
            "lang",
            "dish_contact_id AS platform_contact_id",
            "dish_customer_ref AS platform_customer_ref",
        ]
        self.pull_specific_table("res_partner", columns)

    def odoo_res_country_pull(self) -> None:
        self.pull_specific_table(
            "res_country",
            ["id", "code", "name", "phone_code", "create_date", "write_date"],
        )

    def odoo_sale_order_pull(self) -> None:
        columns = [
            "id",
            "name",
            "state",
            "partner_id",
            "partner_invoice_id",
            "partner_shipping_id",
            "company_id",
            "user_id",
            "team_id",
            "currency_id",
            "pricelist_id",
            "campaign_id",
            "source_id",
            "medium_id",
            "amount_untaxed",
            "amount_tax",
            "amount_total",
            "invoice_status",
            "date_order",
            "create_date",
            "write_date",
            "opportunity_id",
            "warehouse_id",
            "is_subscription",
            "stage_id",
            "close_reason_id",
            "end_date",
            "start_date",
            "next_invoice_date",
            "country_id",
            "dish_commitment_period_id AS platform_commitment_period_id",
            # access_token intentionally omitted (portal secret)
        ]
        self.pull_specific_table("sale_order", columns)

    def odoo_sale_order_line_pull(self) -> None:
        columns = [
            "id",
            "order_id",
            "product_id",
            "name",
            "product_uom_qty",
            "price_unit",
            "price_subtotal",
            "price_tax",
            "price_total",
            "discount",
            "state",
            "invoice_status",
            "company_id",
            "currency_id",
            "create_date",
            "write_date",
            "order_partner_id",
            "salesman_id",
            "sequence",
            "dish_billing_recurrence_id AS platform_billing_recurrence_id",
            "dish_commitment_period_id AS platform_commitment_period_id",
            "dish_has_downgrade AS platform_has_downgrade",
            "dish_has_upgrade AS platform_has_upgrade",
            "order_dish_establishment_id AS platform_establishment_id",
        ]
        self.pull_specific_table("sale_order_line", columns)

    def odoo_account_tax_sale_order_line_rel_pull(self) -> None:
        self.pull_specific_table(
            "account_tax_sale_order_line_rel",
            ["sale_order_line_id", "account_tax_id"],
        )

    def odoo_product_template_pull(self) -> None:
        self.pull_specific_table(
            "product_template",
            [
                "id",
                "name",
                "default_code",
                "type",
                "categ_id",
                "list_price",
                "active",
                "sale_ok",
                "purchase_ok",
                "company_id",
                "create_date",
                "write_date",
            ],
        )

    def odoo_product_category_pull(self) -> None:
        self.pull_specific_table(
            "product_category",
            ["id", "name", "parent_id", "create_date", "write_date"],
        )
