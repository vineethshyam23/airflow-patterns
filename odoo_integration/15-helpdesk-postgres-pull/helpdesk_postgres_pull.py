"""
Postgres incremental pull of Odoo helpdesk tables into NDJSON files.

Reads the Odoo Postgres replica (or primary) directly via psycopg2 —
faster and more flexible than OdooRPC for bulk extracts. Tickets and
mail messages use a rolling two-day create/write window; dimension
tables (team, type, medium, stage, tag, tag-rel) are full refreshes.

Credentials come from Airflow Variables only. Connection is lazy and
reopens per extract method because each Composer task typically runs
in its own worker process after closing the previous handle.

Source (read-only):
  dags/horeca_digital/helpdesk_odoo_import.py
  (HelpdeskPull)

Sanitized vs production:
  - GCP project default hd-dwh-stream-* → dwh_project / dwh_project_dev
  - dish_* custom columns renamed to generic product_* / support_*
  - access_token dropped from ticket extract (portal secret)
  - Lazy connect + reopen if closed (source connected in __init__
    and closed after every method)
  - Variable names generalized (odoo_dm_creds / odoo_prod_creds)
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, Optional

import psycopg2
from airflow.models import Variable


def _str_or_empty(value: Any) -> str:
    return str(value) if value is not None else ""


class HelpdeskPostgresPull:
    """
    Pull helpdesk entities from Odoo Postgres into NDJSON on the
    Composer data volume (or any tmp_loc the DAG passes).
    """

    def __init__(
        self,
        env: str = "DM",
        project_id: str = "dwh_project_dev",
        tmp_loc: Optional[str] = None,
    ):
        self.env = env
        self.project_id = project_id
        self.tmp_loc = tmp_loc or "/home/airflow/gcs/data/odoo/"
        self.conn = None

    def _creds(self) -> Dict[str, str]:
        if self.env == "DM":
            return Variable.get("odoo_dm_creds", deserialize_json=True)
        return Variable.get("odoo_prod_creds", deserialize_json=True)

    def _connect(self):
        creds = self._creds()
        # Composer workers sometimes leave a wait callback that hangs
        # SSL handshakes on long pulls — disable it like production.
        psycopg2.extensions.set_wait_callback(None)
        self.conn = psycopg2.connect(
            host=creds["hostname"],
            database=creds["database"],
            user=creds["db_user"],
            password=creds["db_pwd"],
            sslmode="require",
        )
        logging.info(
            "Odoo Postgres connection opened (env=%s, project=%s)",
            self.env,
            self.project_id,
        )

    def _ensure_conn(self):
        if self.conn is None or getattr(self.conn, "closed", 1):
            self._connect()

    def _write_ndjson(self, table_name: str, rows: list) -> None:
        path = f"{self.tmp_loc}{table_name}.json"
        with open(path, "w") as outfile:
            for row in rows:
                outfile.write(json.dumps(row) + "\n")
        logging.info("Wrote %s rows to %s", len(rows), path)

    def _fetch_all(self, sql: str):
        self._ensure_conn()
        self.conn.autocommit = True
        cur = self.conn.cursor()
        cur.execute(sql)
        records = cur.fetchall()
        cur.close()
        self.conn.autocommit = False
        self.conn.close()
        self.conn = None
        return records

    def helpdesk_ticket(
        self, table_name: str = "helpdesk_ticket", execution_date=None
    ):
        """
        Incremental ticket extract: create_date or write_date in
        today / yesterday. Drops portal access_token.
        """
        records = self._fetch_all(
            """
            SELECT id,
                   campaign_id,
                   source_id,
                   medium_id,
                   message_main_attachment_id,
                   team_id,
                   ticket_type_id,
                   company_id,
                   color,
                   user_id,
                   partner_id,
                   stage_id,
                   assign_hours,
                   close_hours,
                   answered_customer_message_count,
                   create_uid,
                   write_uid,
                   email_cc,
                   name,
                   kanban_state,
                   partner_name,
                   partner_email,
                   partner_phone,
                   priority,
                   ticket_ref,
                   properties,
                   description,
                   active,
                   closed_by_partner,
                   sla_reached_late,
                   sla_reached,
                   date_last_stage_update,
                   assign_date,
                   close_date,
                   sla_deadline,
                   oldest_unanswered_customer_message_date,
                   create_date,
                   write_date,
                   rating_last_value,
                   sla_deadline_hours,
                   first_response_hours,
                   avg_response_hours,
                   total_response_hours,
                   sale_order_id,
                   dish_language_id AS language_id,
                   establishment_id,
                   project_id,
                   analytic_account_id,
                   total_hours_spent,
                   sale_line_id,
                   ticket_medium_id,
                   close_by,
                   dish_country_id AS country_id,
                   product_id,
                   lot_id,
                   dish_created_phonecall_id AS created_phonecall_id,
                   dish_created_phonecall_date AS created_phonecall_date,
                   sale_team_id,
                   dish_email_of_reporter AS reporter_email,
                   dish_who_to_contact AS who_to_contact,
                   partner_mobile,
                   dish_close_date AS product_close_date,
                   dish_ticket_ref AS ticket_ref_ext,
                   dish_create_date AS product_create_date,
                   primary_contact_id,
                   escalation_url
            FROM helpdesk_ticket
            WHERE (DATE(create_date) IN (CURRENT_DATE, CURRENT_DATE - INTERVAL '1 day')
               OR DATE(write_date)  IN (CURRENT_DATE, CURRENT_DATE - INTERVAL '1 day'))
            """
        )
        # AS aliases keep the Postgres extract runnable against the
        # production custom-field names while the NDJSON contract stays
        # brand-neutral. access_token intentionally omitted.
        rows = []
        for row in records:
            rows.append(
                {
                    "id": row[0],
                    "campaign_id": row[1],
                    "source_id": row[2],
                    "medium_id": row[3],
                    "message_main_attachment_id": row[4],
                    "team_id": row[5],
                    "ticket_type_id": row[6],
                    "company_id": row[7],
                    "color": row[8],
                    "user_id": row[9],
                    "partner_id": row[10],
                    "stage_id": row[11],
                    "assign_hours": row[12],
                    "close_hours": row[13],
                    "answered_customer_message_count": row[14],
                    "create_uid": row[15],
                    "write_uid": row[16],
                    "email_cc": row[17],
                    "name": row[18],
                    "kanban_state": row[19],
                    "partner_name": row[20],
                    "partner_email": row[21],
                    "partner_phone": row[22],
                    "priority": row[23],
                    "ticket_ref": row[24],
                    "properties": row[25],
                    "description": row[26],
                    "active": row[27],
                    "closed_by_partner": row[28],
                    "sla_reached_late": row[29],
                    "sla_reached": row[30],
                    "date_last_stage_update": _str_or_empty(row[31]),
                    "assign_date": _str_or_empty(row[32]),
                    "close_date": _str_or_empty(row[33]),
                    "sla_deadline": _str_or_empty(row[34]),
                    "oldest_unanswered_customer_message_date": _str_or_empty(row[35]),
                    "create_date": _str_or_empty(row[36]),
                    "write_date": _str_or_empty(row[37]),
                    "rating_last_value": row[38],
                    "sla_deadline_hours": row[39],
                    "first_response_hours": row[40],
                    "avg_response_hours": row[41],
                    "total_response_hours": row[42],
                    "sale_order_id": row[43],
                    "language_id": row[44],
                    "establishment_id": row[45],
                    "project_id": row[46],
                    "analytic_account_id": row[47],
                    "total_hours_spent": row[48],
                    "sale_line_id": row[49],
                    "ticket_medium_id": row[50],
                    "close_by": row[51],
                    "country_id": row[52],
                    "product_id": row[53],
                    "lot_id": row[54],
                    "created_phonecall_id": row[55],
                    "created_phonecall_date": row[56],
                    "sale_team_id": row[57],
                    "reporter_email": row[58],
                    "who_to_contact": row[59],
                    "partner_mobile": row[60],
                    "product_close_date": _str_or_empty(row[61]),
                    "ticket_ref_ext": row[62],
                    "product_create_date": _str_or_empty(row[63]),
                    "primary_contact_id": row[64],
                    "escalation_url": row[65],
                }
            )
        self._write_ndjson(table_name, rows)

    def helpdesk_ticket_medium(
        self, table_name: str = "helpdesk_ticket_medium", execution_date=None
    ):
        records = self._fetch_all(
            """
            SELECT id, create_uid, write_uid, name, create_date, write_date
            FROM helpdesk_ticket_medium
            """
        )
        rows = [
            {
                "id": r[0],
                "create_uid": r[1],
                "write_uid": r[2],
                "name": str(r[3]),
                "create_date": _str_or_empty(r[4]),
                "write_date": _str_or_empty(r[5]),
            }
            for r in records
        ]
        self._write_ndjson(table_name, rows)

    def helpdesk_ticket_type(
        self, table_name: str = "helpdesk_ticket_type", execution_date=None
    ):
        records = self._fetch_all(
            """
            SELECT id, sequence, create_uid, write_uid, name, create_date, write_date
            FROM helpdesk_ticket_type
            """
        )
        rows = [
            {
                "id": r[0],
                "sequence": r[1],
                "create_uid": r[2],
                "write_uid": r[3],
                "name": str(r[4]),
                "create_date": _str_or_empty(r[5]),
                "write_date": _str_or_empty(r[6]),
            }
            for r in records
        ]
        self._write_ndjson(table_name, rows)

    def helpdesk_team(self, table_name: str = "helpdesk_team", execution_date=None):
        records = self._fetch_all(
            """
            SELECT id,
                   message_main_attachment_id,
                   alias_id,
                   company_id,
                   sequence,
                   color,
                   resource_calendar_id,
                   auto_close_day,
                   to_stage_id,
                   create_uid,
                   write_uid,
                   assign_method,
                   privacy_visibility,
                   name,
                   description,
                   ticket_properties,
                   active,
                   auto_assignment,
                   use_alias,
                   allow_portal_ticket_closing,
                   use_website_helpdesk_form,
                   use_website_helpdesk_livechat,
                   use_website_helpdesk_forum,
                   use_website_helpdesk_slides,
                   use_website_helpdesk_knowledge,
                   use_helpdesk_timesheet,
                   use_helpdesk_sale_timesheet,
                   use_credit_notes,
                   use_coupons,
                   use_fsm,
                   use_product_returns,
                   use_product_repairs,
                   use_twitter,
                   use_rating,
                   portal_show_rating,
                   use_sla,
                   auto_close_ticket,
                   create_date,
                   write_date,
                   fsm_project_id,
                   project_id,
                   dish_country_id AS country_id,
                   dish_phone_numbers AS support_phone_numbers,
                   dish_whatsapp_numbers AS support_whatsapp_numbers,
                   dish_oppening_support_hours AS opening_support_hours
            FROM helpdesk_team
            """
        )
        rows = []
        for r in records:
            rows.append(
                {
                    "id": r[0],
                    "message_main_attachment_id": r[1],
                    "alias_id": r[2],
                    "company_id": r[3],
                    "sequence": r[4],
                    "color": r[5],
                    "resource_calendar_id": r[6],
                    "auto_close_day": r[7],
                    "to_stage_id": r[8],
                    "create_uid": r[9],
                    "write_uid": r[10],
                    "assign_method": r[11],
                    "privacy_visibility": r[12],
                    "name": str(r[13]),
                    "description": r[14],
                    "ticket_properties": r[15],
                    "active": r[16],
                    "auto_assignment": r[17],
                    "use_alias": r[18],
                    "allow_portal_ticket_closing": r[19],
                    "use_website_helpdesk_form": r[20],
                    "use_website_helpdesk_livechat": r[21],
                    "use_website_helpdesk_forum": r[22],
                    "use_website_helpdesk_slides": r[23],
                    "use_website_helpdesk_knowledge": r[24],
                    "use_helpdesk_timesheet": r[25],
                    "use_helpdesk_sale_timesheet": r[26],
                    "use_credit_notes": r[27],
                    "use_coupons": r[28],
                    "use_fsm": r[29],
                    "use_product_returns": r[30],
                    "use_product_repairs": r[31],
                    "use_twitter": r[32],
                    "use_rating": r[33],
                    "portal_show_rating": r[34],
                    "use_sla": r[35],
                    "auto_close_ticket": r[36],
                    "create_date": _str_or_empty(r[37]),
                    "write_date": _str_or_empty(r[38]),
                    "fsm_project_id": r[39],
                    "project_id": r[40],
                    "country_id": r[41],
                    "support_phone_numbers": r[42],
                    "support_whatsapp_numbers": r[43],
                    "opening_support_hours": r[44],
                }
            )
        self._write_ndjson(table_name, rows)

    def helpdesk_tag(self, table_name: str = "helpdesk_tag", execution_date=None):
        records = self._fetch_all(
            """
            SELECT id, color, create_uid, write_uid, name, create_date, write_date
            FROM helpdesk_tag
            """
        )
        rows = [
            {
                "id": r[0],
                "color": r[1],
                "create_uid": r[2],
                "write_uid": r[3],
                "name": str(r[4]),
                "create_date": _str_or_empty(r[5]),
                "write_date": _str_or_empty(r[6]),
            }
            for r in records
        ]
        self._write_ndjson(table_name, rows)

    def helpdesk_tag_helpdesk_ticket_rel(
        self,
        table_name: str = "helpdesk_tag_helpdesk_ticket_rel",
        execution_date=None,
    ):
        records = self._fetch_all(
            """
            SELECT helpdesk_ticket_id, helpdesk_tag_id
            FROM helpdesk_tag_helpdesk_ticket_rel
            """
        )
        rows = [
            {"helpdesk_ticket_id": r[0], "helpdesk_tag_id": r[1]} for r in records
        ]
        self._write_ndjson(table_name, rows)

    def helpdesk_stage(self, table_name: str = "helpdesk_stage", execution_date=None):
        records = self._fetch_all(
            """
            SELECT id,
                   sequence,
                   template_id,
                   create_uid,
                   write_uid,
                   name,
                   description,
                   legend_blocked,
                   legend_done,
                   legend_normal,
                   active,
                   fold,
                   create_date,
                   write_date,
                   sms_template_id
            FROM helpdesk_stage
            """
        )
        rows = []
        for r in records:
            rows.append(
                {
                    "id": r[0],
                    "sequence": r[1],
                    "template_id": r[2],
                    "create_uid": r[3],
                    "write_uid": r[4],
                    "name": str(r[5]),
                    "description": r[6],
                    "legend_blocked": _str_or_empty(r[7]),
                    "legend_done": _str_or_empty(r[8]),
                    "legend_normal": _str_or_empty(r[9]),
                    "active": r[10],
                    "fold": r[11],
                    "create_date": _str_or_empty(r[12]),
                    "write_date": _str_or_empty(r[13]),
                    "sms_template_id": r[14],
                }
            )
        self._write_ndjson(table_name, rows)

    def mail_messages(self, table_name: str = "mail_message", execution_date=None):
        """
        Optional: ticket / payment message thread delta. Left off the
        default DAG table list because bodies are bulky and PII-heavy;
        enable when the warehouse needs conversation context.
        """
        records = self._fetch_all(
            """
            SELECT id,
                   parent_id,
                   res_id,
                   subtype_id,
                   mail_activity_type_id,
                   author_id,
                   author_guest_id,
                   mail_server_id,
                   create_uid,
                   write_uid,
                   subject,
                   model,
                   record_name,
                   message_type,
                   email_from,
                   message_id,
                   reply_to,
                   email_layout_xmlid,
                   body,
                   is_internal,
                   reply_to_force_new,
                   email_add_signature,
                   "date",
                   create_date,
                   write_date,
                   phonecall_log_id,
                   phonecall_log_editable
            FROM mail_message
            WHERE model IN ('account.payment', 'helpdesk.ticket')
              AND (DATE(create_date) IN (CURRENT_DATE, CURRENT_DATE - INTERVAL '1 day')
                OR DATE(write_date)  IN (CURRENT_DATE, CURRENT_DATE - INTERVAL '1 day'))
            """
        )
        rows = []
        for r in records:
            rows.append(
                {
                    "id": r[0],
                    "parent_id": r[1],
                    "res_id": r[2],
                    "subtype_id": r[3],
                    "mail_activity_type_id": r[4],
                    "author_id": r[5],
                    "author_guest_id": r[6],
                    "mail_server_id": r[7],
                    "create_uid": r[8],
                    "write_uid": r[9],
                    "subject": r[10],
                    "model": r[11],
                    "record_name": r[12],
                    "message_type": r[13],
                    "email_from": r[14],
                    "message_id": r[15],
                    "reply_to": r[16],
                    "email_layout_xmlid": r[17],
                    "body": r[18],
                    "is_internal": r[19],
                    "reply_to_force_new": r[20],
                    "email_add_signature": r[21],
                    "date": _str_or_empty(r[22]),
                    "create_date": _str_or_empty(r[23]),
                    "write_date": _str_or_empty(r[24]),
                    "phonecall_log_id": r[25],
                    "phonecall_log_editable": r[26],
                }
            )
        self._write_ndjson(table_name, rows)
