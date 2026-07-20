"""
Avro bulk ingest of Odoo helpdesk tickets to an external event API.

Runs a BigQuery delta query (yesterday's creates), encodes each row as Avro
binary (base64), chunks to 500, and POSTs to /ingestbulk/{country}/{schema_id}.

Credentials come from Airflow Variables only.

Source (read-only): dags/horeca_digital/dana_odoo_helpdesk_ticket.py
"""

from __future__ import annotations

import base64
import io
import json
import logging
import os
from datetime import datetime
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
        "helpdesk_ticket_event_schema_id_dev", default_var="SCHEMA_ID_DEV"
    )
    SCHEMA_NAME = "odoo_helpdesk_tickets_dev"
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
        "helpdesk_ticket_event_schema_id", default_var="SCHEMA_ID_PROD"
    )
    SCHEMA_NAME = "odoo_helpdesk_tickets"
    INGEST_BASE = Variable.get(
        "event_api_ingest_base",
        default_var="https://api.example.com/event-ingest/bulk",
    )

# Avro contract for the event bus. Timestamps arrive as strings from BQ CAST.
HELPDESK_TICKETS_AVRO_SCHEMA = json.dumps(
    {
        "namespace": "company",
        "type": "record",
        "name": SCHEMA_NAME,
        "doc": "Odoo Level-1 helpdesk tickets for event ingest",
        "gdpr_info": {"table_PII": "no", "column_PII": []},
        "fields": [
            {"name": "ticket_number", "type": ["null", "string"], "default": None},
            {"name": "ticket_name", "type": ["null", "string"], "default": None},
            {"name": "create_date", "type": ["null", "string"], "default": None},
            {"name": "ticket_type", "type": ["null", "string"], "default": None},
            {"name": "ticket_tag", "type": ["null", "string"], "default": None},
            {"name": "close_date", "type": ["null", "string"], "default": None},
            {"name": "country", "type": ["null", "string"], "default": None},
            {"name": "escalated_check", "type": ["null", "string"], "default": None},
            {"name": "current_status", "type": ["null", "string"], "default": None},
            {"name": "ticket_medium", "type": ["null", "string"], "default": None},
            {"name": "account_identifier", "type": ["null", "string"], "default": None},
            {"name": "customer_id", "type": ["null", "string"], "default": None},
            {"name": "store_id", "type": ["null", "int"], "default": None},
        ],
    }
)


def chunks(items: List, size: int) -> Iterator[List]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


def _b64(s: str) -> str:
    return base64.b64encode(s.encode()).decode()


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
        "ticket_number": row["ticket_number"],
        "ticket_name": row["ticket_name"],
        "create_date": row["create_date"],
        "ticket_type": row["ticket_type"],
        "ticket_tag": row["ticket_tag"],
        "close_date": row["close_date"],
        "country": row["country"],
        "escalated_check": row["escalated_check"],
        "current_status": row["current_status"],
        "ticket_medium": row["ticket_medium"],
        "account_identifier": row["account_identifier"],
        "customer_id": row["customer_id"],
        "store_id": row["store_id"],
    }
    writer.write(payload, encoder)
    return base64.b64encode(bytes_writer.getvalue()).decode("utf-8")


def send_helpdesk_tickets_data(country: str, query: str) -> None:
    """
    Run the helpdesk delta query, Avro-encode rows, POST in chunks of 500.

    Empty result is fine — quiet support days happen.
    """
    client = EventApiClient(OAUTH2_URL, CLIENT_ID, CLIENT_SECRET)
    # Parse once. Production source parsed inside the per-row loop; that
    # burned CPU on busy days for no benefit.
    schema = avro.schema.parse(HELPDESK_TICKETS_AVRO_SCHEMA)

    logging.info("%s - Executing Odoo helpdesk tickets query...", datetime.now())
    bq = bigquery.Client(project=BIGQUERY_PROJECT)
    results = bq.query(query).result()

    encoded: List[str] = []
    logging.info(
        "%s - Processing helpdesk tickets result set - %s...",
        datetime.now(),
        country.upper(),
    )
    for row in results:
        encoded.append(_encode_row(row, schema))

    logging.info(
        "%s - %s rows to send for %s",
        datetime.now(),
        len(encoded),
        country.upper(),
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
        "%s - Helpdesk tickets ingest completed for %s",
        datetime.now(),
        country.upper(),
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    raise SystemExit(
        "Call send_helpdesk_tickets_data(country, query) from Airflow or a notebook"
    )
