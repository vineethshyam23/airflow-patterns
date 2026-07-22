"""
Avro bulk ingest of Odoo WSL (wholesale) invoice lines to an external event API.

Reads a BigQuery trusted-table query, encodes each row as Avro binary (base64),
chunks to 500 records, and POSTs to /ingestbulk/{country}/{schema_id}.

Credentials come from Airflow Variables only — nothing hard-coded.

Source (read-only): dags/horeca_digital/dana_odoo_wsl_invoices.py

Note on a production bug we fixed here:
  Source mapped theo_total_rec_revenue from row["theo_total_onetime_revenue"].
  That silently duplicated onetime into recurring. Correct column used below.
"""

from __future__ import annotations

import base64
import io
import json
import logging
import os
from datetime import date, datetime
from typing import Iterator, List

import avro.io
import avro.schema
import requests
from airflow.models import Variable
from google.cloud import bigquery

ENV_VAR_NAME = "env"
ENV = os.environ.get(ENV_VAR_NAME, Variable.get(ENV_VAR_NAME))

if ENV == "DEV":
    OAUTH_USERNAME = Variable.get("event_api_username")
    OAUTH_PASSWORD = Variable.get("event_api_password")
    CLIENT_ID = Variable.get("event_api_client_id_dev")
    CLIENT_SECRET = Variable.get("event_api_client_secret_dev")
    OAUTH2_URL = Variable.get("event_api_oauth2_url_dev")
    BIGQUERY_PROJECT = "dwh_project_dev"
    SCHEMA_ID = Variable.get(
        "odoo_wsl_invoices_event_schema_id_dev", default_var="SCHEMA_ID_DEV"
    )
    SCHEMA_NAME = "odoo_wsl_invoices_dev"
    INGEST_BASE = Variable.get(
        "event_api_ingest_base_dev",
        default_var="https://api.example.com/event-ingest/bulk",
    )
else:
    OAUTH_USERNAME = Variable.get("event_api_username")
    OAUTH_PASSWORD = Variable.get("event_api_password")
    CLIENT_ID = Variable.get("event_api_client_id")
    CLIENT_SECRET = Variable.get("event_api_client_secret")
    OAUTH2_URL = Variable.get("event_api_oauth2_url")
    BIGQUERY_PROJECT = "dwh_project"
    SCHEMA_ID = Variable.get(
        "odoo_wsl_invoices_event_schema_id", default_var="SCHEMA_ID_PROD"
    )
    SCHEMA_NAME = "odoo_wsl_invoices"
    INGEST_BASE = Variable.get(
        "event_api_ingest_base",
        default_var="https://api.example.com/event-ingest/bulk",
    )

# Avro contract for the event bus. Field set mirrors the trusted intermediate.
# Dates use Avro logicalType date (days since epoch).
WSL_INVOICES_AVRO_SCHEMA = json.dumps(
    {
        "namespace": "company",
        "type": "record",
        "name": SCHEMA_NAME,
        "doc": "Odoo WSL wholesale invoice line facts for event ingest",
        "fields": [
            {"name": "uni_key", "type": ["null", "string"], "default": None},
            {"name": "parent_bill", "type": ["null", "string"], "default": None},
            {
                "name": "booking_month",
                "type": {"type": "int", "logicalType": "date"},
                "doc": "Booking month (first of month)",
            },
            {
                "name": "booking_date",
                "type": {"type": "int", "logicalType": "date"},
                "doc": "Booking date",
            },
            {
                "name": "actual_delivery_start",
                "type": {"type": "int", "logicalType": "date"},
                "doc": "Actual delivery start",
            },
            {"name": "billing_country", "type": "string"},
            {"name": "billing_country_code", "type": "string"},
            {"name": "company", "type": "string"},
            {"name": "sales_agency_id", "type": "string"},
            {"name": "merchant", "type": "string"},
            {"name": "order_id", "type": "string"},
            {"name": "establishment_id", "type": "string"},
            {"name": "metro_id", "type": ["null", "long"], "default": None},
            {"name": "account_id", "type": "string"},
            {
                "name": "establishment_name",
                "type": ["null", "string"],
                "default": None,
            },
            {
                "name": "establishment_type",
                "type": ["null", "string"],
                "default": None,
            },
            {
                "name": "establishment_city",
                "type": ["null", "string"],
                "default": None,
            },
            {
                "name": "establishment_street",
                "type": ["null", "string"],
                "default": None,
            },
            {
                "name": "establishment_postalcode",
                "type": ["null", "int"],
                "default": None,
            },
            {
                "name": "establishment_latitude",
                "type": ["null", "float"],
                "default": None,
            },
            {
                "name": "establishment_longitude",
                "type": ["null", "float"],
                "default": None,
            },
            {
                "name": "establishment_geo_accuracy",
                "type": ["null", "string"],
                "default": None,
            },
            {"name": "main_category", "type": ["null", "string"], "default": None},
            {"name": "product_base_code", "type": ["null", "string"], "default": None},
            {"name": "product_code", "type": ["null", "string"], "default": None},
            {"name": "quantity", "type": ["null", "int"], "default": None},
            {
                "name": "theoretical_total_amount",
                "type": ["null", "float"],
                "default": None,
            },
            {
                "name": "promotions_total_amount",
                "type": ["null", "float"],
                "default": None,
            },
            {"name": "actual_total_amount", "type": ["null", "float"], "default": None},
            {
                "name": "theo_total_rec_revenue",
                "type": ["null", "float"],
                "default": None,
            },
            {
                "name": "promotions_rec_amount",
                "type": ["null", "float"],
                "default": None,
            },
            {"name": "actual_rec_revenue", "type": ["null", "float"], "default": None},
            {
                "name": "theo_total_onetime_revenue",
                "type": ["null", "float"],
                "default": None,
            },
            {
                "name": "promotions_onetime_amount",
                "type": ["null", "float"],
                "default": None,
            },
            {
                "name": "actual_onetime_revenue",
                "type": ["null", "float"],
                "default": None,
            },
        ],
    }
)


def chunks(items: List, size: int) -> Iterator[List]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


def _b64(s: str) -> str:
    return base64.b64encode(s.encode()).decode()


def _avro_date(value):
    """BigQuery DATE → Avro logical date (int days since 1970-01-01)."""
    if value is None:
        return None
    if isinstance(value, date):
        return (value - date(1970, 1, 1)).days
    return value


class EventApiClient:
    """Password-grant OAuth client with one-shot 401 retry on POST."""

    def __init__(self, token_url: str, client_id: str, client_secret: str):
        self.token_url = token_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.token = None

    def get_token(self) -> str:
        headers = {
            "Authorization": "Basic " + _b64(self.client_id + ":" + self.client_secret),
            "Content-Type": "application/x-www-form-urlencoded",
        }
        params = {
            "grant_type": "password",
            "username": OAUTH_USERNAME,
            "password": OAUTH_PASSWORD,
        }
        response = requests.post(
            self.token_url, headers=headers, params=params, timeout=60
        )
        response.raise_for_status()
        self.token = response.json()["access_token"]
        return self.token

    def post_json(self, url: str, data=None):
        if self.token is None:
            self.get_token()

        headers = {
            "Authorization": "Bearer " + self.token,
            "Content-Type": "application/vnd.company.events.json",
        }
        response = requests.post(url, headers=headers, data=data, timeout=120)

        if response.status_code == 401:
            self.token = None
            return self.post_json(url, data=data)

        response.raise_for_status()
        return response.json()


def _encode_row(row, schema) -> str:
    writer = avro.io.DatumWriter(schema)
    bytes_writer = io.BytesIO()
    encoder = avro.io.BinaryEncoder(bytes_writer)
    payload = {
        "uni_key": row["uni_key"],
        "parent_bill": row["parent_bill"],
        "booking_month": _avro_date(row["booking_month"]),
        "booking_date": _avro_date(row["booking_date"]),
        "actual_delivery_start": _avro_date(row["actual_delivery_start"]),
        "billing_country": row["billing_country"],
        "billing_country_code": row["billing_country_code"],
        "company": row["company"],
        "sales_agency_id": row["sales_agency_id"],
        "merchant": row["merchant"],
        "order_id": row["order_id"],
        "establishment_id": row["establishment_id"],
        "metro_id": row["metro_id"],
        "account_id": row["account_id"],
        "establishment_name": row["establishment_name"],
        "establishment_type": row["establishment_type"],
        "establishment_city": row["establishment_city"],
        "establishment_street": row["establishment_street"],
        "establishment_postalcode": row["establishment_postalcode"],
        "establishment_latitude": row["establishment_latitude"],
        "establishment_longitude": row["establishment_longitude"],
        "establishment_geo_accuracy": row["establishment_geo_accuracy"],
        "main_category": row["main_category"],
        "product_base_code": row["product_base_code"],
        "product_code": row["product_code"],
        "quantity": row["quantity"],
        "theoretical_total_amount": row["theoretical_total_amount"],
        "promotions_total_amount": row["promotions_total_amount"],
        "actual_total_amount": row["actual_total_amount"],
        # Fixed: source incorrectly read theo_total_onetime_revenue here.
        "theo_total_rec_revenue": row["theo_total_rec_revenue"],
        "promotions_rec_amount": row["promotions_rec_amount"],
        "actual_rec_revenue": row["actual_rec_revenue"],
        "theo_total_onetime_revenue": row["theo_total_onetime_revenue"],
        "promotions_onetime_amount": row["promotions_onetime_amount"],
        "actual_onetime_revenue": row["actual_onetime_revenue"],
    }
    writer.write(payload, encoder)
    return base64.b64encode(bytes_writer.getvalue()).decode("utf-8")


def send_wsl_invoices_data(country: str, query: str) -> None:
    """
    Run the trusted-table query, Avro-encode rows, POST in chunks of 500.

    Full-table send (not a hash delta). Caller is responsible for upstream
    dbt refresh so the trusted intermediate is current before this runs.
    """
    client = EventApiClient(OAUTH2_URL, CLIENT_ID, CLIENT_SECRET)
    # Parse once outside the row loop (source parsed every row).
    schema = avro.schema.parse(WSL_INVOICES_AVRO_SCHEMA)

    logging.info("%s - Executing Odoo WSL invoices query...", datetime.now())
    bq = bigquery.Client(project=BIGQUERY_PROJECT)
    results = bq.query(query).result()

    encoded: List[str] = []
    logging.info(
        "%s - Processing Odoo WSL invoices result set - %s...",
        datetime.now(),
        country.upper(),
    )
    for row in results:
        encoded.append(_encode_row(row, schema))

    logging.info(
        "%s - %s rows to send for %s", datetime.now(), len(encoded), country.upper()
    )

    base_url = f"{INGEST_BASE.rstrip('/')}/{country}/{SCHEMA_ID}"
    sent = 0
    for chunk in chunks(encoded, 500):
        sent += len(chunk)
        payload = {"records": [{"value": record} for record in chunk]}
        logging.info(
            "%s - %s chunk progress %s / %s",
            datetime.now(),
            country.upper(),
            sent,
            len(encoded),
        )
        client.post_json(url=base_url, data=json.dumps(payload))

    logging.info(
        "%s - Odoo WSL invoices ingest completed for %s",
        datetime.now(),
        country.upper(),
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    raise SystemExit(
        "Call send_wsl_invoices_data(country, query) from Airflow or a notebook"
    )
