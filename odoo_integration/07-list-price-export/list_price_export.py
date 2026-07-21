"""
Avro bulk ingest of Odoo list-price / commission deltas to an external event API.

Reads a BigQuery delta query, encodes each row as Avro binary (base64),
chunks to 500 records, and POSTs to /ingestbulk/{country}/{schema_id}.

Credentials come from Airflow Variables only — nothing hard-coded.

Source (read-only): dags/horeca_digital/dana_odoo_list_price_export.py
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
        "odoo_list_price_event_schema_id_dev", default_var="SCHEMA_ID_DEV"
    )
    SCHEMA_NAME = "odoo_price_list_dev"
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
        "odoo_list_price_event_schema_id", default_var="SCHEMA_ID_PROD"
    )
    SCHEMA_NAME = "odoo_price_list"
    INGEST_BASE = Variable.get(
        "event_api_ingest_base",
        default_var="https://api.example.com/event-ingest/bulk",
    )

# Avro contract for the event bus. Field set mirrors the BQ send query.
# Dates use Avro logicalType date (days since epoch) where the bus expects it.
# Field names keep production spelling (including typos like theoretica_ /
# reccuring) so the consumer schema stays compatible.
LIST_PRICE_AVRO_SCHEMA = json.dumps(
    {
        "namespace": "company",
        "type": "record",
        "name": SCHEMA_NAME,
        "doc": "Odoo invoice-line list price / commission measures for event ingest",
        "fields": [
            {"name": "parent_bill", "type": ["null", "string"], "doc": "Parent bill id"},
            {
                "name": "booking_date",
                "type": ["null", {"type": "int", "logicalType": "date"}],
                "doc": "Booking date",
            },
            {
                "name": "actual_delivery_start",
                "type": ["null", {"type": "int", "logicalType": "date"}],
                "doc": "Actual delivery start",
            },
            {"name": "billing_country", "type": ["null", "string"], "default": None},
            {"name": "billing_country_code", "type": ["null", "string"], "default": None},
            {"name": "company", "type": ["null", "string"], "default": None},
            {"name": "sales_agency_id", "type": ["null", "string"], "default": None},
            {"name": "merchant", "type": ["null", "string"], "default": None},
            {"name": "salesforce_order_id", "type": ["null", "string"], "default": None},
            {
                "name": "salesforce_establishment_id",
                "type": ["null", "string"],
                "default": None,
            },
            {"name": "salesforce_account_id", "type": ["null", "string"], "default": None},
            {"name": "metro_id", "type": ["null", "long"], "default": None},
            {"name": "siren_no", "type": ["null", "string"], "default": None},
            {"name": "product_code", "type": ["null", "string"], "default": None},
            {"name": "product_base_code", "type": ["null", "string"], "default": None},
            {"name": "label", "type": ["null", "string"], "default": None},
            {"name": "quantity", "type": ["null", "int"], "default": None},
            {"name": "price_per_unit", "type": ["null", "float"], "default": None},
            {
                "name": "theoretica_oneshot_amount",
                "type": ["null", "float"],
                "default": None,
            },
            {"name": "actual_oneshot_amount", "type": ["null", "float"], "default": None},
            {
                "name": "theoretical_MFR_commission_oneshot",
                "type": ["null", "float"],
                "default": None,
            },
            {
                "name": "promotions_amount_oneshot",
                "type": ["null", "float"],
                "default": None,
            },
            {"name": "net_group_oneshot", "type": ["null", "float"], "default": None},
            {"name": "net_MFR_oneshot", "type": ["null", "float"], "default": None},
            {
                "name": "monthly_invoiced_subscription",
                "type": ["null", "float"],
                "default": None,
            },
            {
                "name": "theoretical_MFR_commission_monthly",
                "type": ["null", "float"],
                "default": None,
            },
            {
                "name": "amount_promotions_recurring",
                "type": ["null", "float"],
                "default": None,
            },
            {"name": "net_group_monthly", "type": ["null", "float"], "default": None},
            {"name": "net_MFR_monthly", "type": ["null", "float"], "default": None},
            {
                "name": "total_theoretical_reccuring_monthly_MFR",
                "type": ["null", "float"],
                "default": None,
            },
            {
                "name": "total_theoretical_one_shot_monthly_MFR",
                "type": ["null", "float"],
                "default": None,
            },
            {
                "name": "total_promotion_one_shot",
                "type": ["null", "float"],
                "default": None,
            },
            {
                "name": "total_promotion_reccuring",
                "type": ["null", "float"],
                "default": None,
            },
            {
                "name": "total_commission_reccuring_MFR",
                "type": ["null", "float"],
                "default": None,
            },
            {
                "name": "total_commission_one_shot_MFR",
                "type": ["null", "float"],
                "default": None,
            },
            {"name": "total_monthly_promo", "type": ["null", "float"], "default": None},
            {"name": "total_monthly_DISH", "type": ["null", "float"], "default": None},
            {"name": "total_invoiced", "type": ["null", "float"], "default": None},
            {
                "name": "_ldts",
                "type": {"type": "int", "logicalType": "date"},
                "doc": "Load date",
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
    """BigQuery DATE → Avro logical date (int days since 1970-01-01) or None."""
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

        # Token expiry mid-run — refresh once and retry the same chunk.
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
        "parent_bill": row["parent_bill"],
        "booking_date": _avro_date(row["booking_date"]),
        "actual_delivery_start": _avro_date(row["actual_delivery_start"]),
        "billing_country": row["billing_country"],
        "billing_country_code": row["billing_country_code"],
        "company": row["company"],
        "sales_agency_id": row["sales_agency_id"],
        "merchant": row["merchant"],
        "salesforce_order_id": row["salesforce_order_id"],
        "salesforce_establishment_id": row["salesforce_establishment_id"],
        "salesforce_account_id": row["salesforce_account_id"],
        "metro_id": row["metro_id"],
        "siren_no": row["siren_no"],
        "product_code": row["product_code"],
        "product_base_code": row["product_base_code"],
        "label": row["label"],
        "quantity": row["quantity"],
        "price_per_unit": row["price_per_unit"],
        "theoretica_oneshot_amount": row["theoretica_oneshot_amount"],
        "actual_oneshot_amount": row["actual_oneshot_amount"],
        "theoretical_MFR_commission_oneshot": row["theoretical_MFR_commission_oneshot"],
        "promotions_amount_oneshot": row["promotions_amount_oneshot"],
        "net_group_oneshot": row["net_group_oneshot"],
        "net_MFR_oneshot": row["net_MFR_oneshot"],
        "monthly_invoiced_subscription": row["monthly_invoiced_subscription"],
        "theoretical_MFR_commission_monthly": row["theoretical_MFR_commission_monthly"],
        "amount_promotions_recurring": row["amount_promotions_recurring"],
        "net_group_monthly": row["net_group_monthly"],
        "net_MFR_monthly": row["net_MFR_monthly"],
        "total_theoretical_reccuring_monthly_MFR": row[
            "total_theoretical_reccuring_monthly_MFR"
        ],
        "total_theoretical_one_shot_monthly_MFR": row[
            "total_theoretical_one_shot_monthly_MFR"
        ],
        "total_promotion_one_shot": row["total_promotion_one_shot"],
        "total_promotion_reccuring": row["total_promotion_reccuring"],
        "total_commission_reccuring_MFR": row["total_commission_reccuring_MFR"],
        "total_commission_one_shot_MFR": row["total_commission_one_shot_MFR"],
        "total_monthly_promo": row["total_monthly_promo"],
        "total_monthly_DISH": row["total_monthly_DISH"],
        "total_invoiced": row["total_invoiced"],
        "_ldts": _avro_date(row["_ldts"]),
    }
    writer.write(payload, encoder)
    return base64.b64encode(bytes_writer.getvalue()).decode("utf-8")


def send_odoo_list_price_data(query: str, country: str = "fr") -> None:
    """
    Run the delta query, Avro-encode rows, POST in chunks of 500.

    Empty result is fine — quiet months after a large billing wave.
    """
    client = EventApiClient(OAUTH2_URL, CLIENT_ID, CLIENT_SECRET)
    # Parse once outside the row loop (source parsed every row).
    schema = avro.schema.parse(LIST_PRICE_AVRO_SCHEMA)

    logging.info("%s - Executing Odoo list-price query...", datetime.now())
    bq = bigquery.Client(project=BIGQUERY_PROJECT)
    results = bq.query(query).result()

    encoded: List[str] = []
    logging.info(
        "%s - Processing Odoo list-price result set - %s...",
        datetime.now(),
        country,
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
        "%s - Odoo list-price ingest completed for %s",
        datetime.now(),
        country.upper(),
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    raise SystemExit(
        "Call send_odoo_list_price_data(query, country) from Airflow or a notebook"
    )
