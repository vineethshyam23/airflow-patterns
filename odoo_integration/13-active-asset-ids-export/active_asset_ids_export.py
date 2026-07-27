"""
Avro bulk ingest for the weekly active sale-order-line ID snapshot.

One send function: BigQuery → Avro encode → chunk 500 →
POST /ingestbulk/{country}/{schema_id}.

This is the deletion-semantics companion to pattern 09 (lifecycle
deltas). Pattern 09 ships status changes; this DAG ships the full
active ID set so the consumer can LEFT JOIN and treat absences as
deletes.

Credentials and schema ids come from Airflow Variables only.

Source (read-only):
  dags/horeca_digital/dana_odoo_assets_leads_lifecycle_export.py
  (DANAexport.send_active_asset_ids_data)

Sanitized fixes vs production module:
  - Avro schema parsed once per send (source parsed every row)
  - HTTP errors raise instead of only logging the body
  - Schema id / ingest base externalized to Variables
  - BigQuery DATE → Avro logical date conversion made explicit
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
    ACTIVE_IDS_SCHEMA_ID = Variable.get(
        "odoo_active_asset_ids_schema_id_dev",
        default_var="ACTIVE_ASSET_IDS_SCHEMA_DEV",
    )
    SCHEMA_NAME = "odoo_active_asset_ids_dev"
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
    ACTIVE_IDS_SCHEMA_ID = Variable.get(
        "odoo_active_asset_ids_schema_id",
        default_var="ACTIVE_ASSET_IDS_SCHEMA_PROD",
    )
    SCHEMA_NAME = "odoo_active_asset_ids"
    INGEST_BASE = Variable.get(
        "event_api_ingest_base",
        default_var="https://api.example.com/event-ingest/bulk",
    )


ACTIVE_IDS_AVRO_SCHEMA = json.dumps(
    {
        "namespace": "company",
        "type": "record",
        "name": SCHEMA_NAME,
        "doc": (
            "Weekly snapshot of active sale order line IDs from Odoo. "
            "Rows absent from this table should be treated as deleted."
        ),
        "gdpr_info": {"table_PII": "no", "column_PII": []},
        "fields": [
            {
                "name": "sale_order_line_id",
                "type": "long",
                "doc": "Active sale order line ID — join key with asset lifecycle",
            },
            {
                "name": "sale_order_id",
                "type": "long",
                "doc": "Parent sale order ID",
            },
            {
                "name": "establishment_id",
                "type": ["null", "string"],
                "default": None,
                "doc": "Establishment / partner UUID",
            },
            {
                "name": "_ldts",
                "type": {"type": "int", "logicalType": "date"},
                "doc": "Loading date of this snapshot",
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


def _encode_bytes(payload: dict, schema) -> str:
    writer = avro.io.DatumWriter(schema)
    bytes_writer = io.BytesIO()
    encoder = avro.io.BinaryEncoder(bytes_writer)
    writer.write(payload, encoder)
    return base64.b64encode(bytes_writer.getvalue()).decode("utf-8")


def send_active_asset_ids_data(country: str, query: str) -> None:
    """
    Full weekly snapshot of active sale_order_line IDs for one market.

    Source of truth is refined_sales.odoo_sale_order_line — cleanup
    deletions are already applied there. Consumer LEFT JOINs on
    sale_order_line_id to filter deleted assets out of lifecycle views.
    """
    client_api = EventApiClient(OAUTH2_URL, CLIENT_ID, CLIENT_SECRET)
    schema = avro.schema.parse(ACTIVE_IDS_AVRO_SCHEMA)

    logging.info("%s - Executing active asset IDs query...", datetime.now())
    bq = bigquery.Client(project=BIGQUERY_PROJECT)
    results = bq.query(query).result()

    encoded: List[str] = []
    logging.info(
        "%s - Processing active asset IDs result set - %s...",
        datetime.now(),
        country,
    )

    for row in results:
        record = {
            "sale_order_line_id": row["sale_order_line_id"],
            "sale_order_id": row["sale_order_id"],
            "establishment_id": row["establishment_id"],
            "_ldts": _avro_date(row["_ldts"]),
        }
        encoded.append(_encode_bytes(record, schema))

    chunked = list(chunks(encoded, 500))
    logging.info("%s - Total chunks: %s", datetime.now(), len(chunked))

    base_url = f"{INGEST_BASE.rstrip('/')}/{country}/{ACTIVE_IDS_SCHEMA_ID}"
    logging.info("%s - Base URL: %s", datetime.now(), base_url)

    posted = 0
    for chunk in chunked:
        posted += len(chunk)
        payload = {"records": [{"value": row} for row in chunk]}
        logging.info(
            "%s - Result for country %s, chunk %s of %s",
            datetime.now(),
            country.upper(),
            posted,
            len(encoded),
        )
        logging.info(client_api.post_json(url=base_url, data=json.dumps(payload)))

    logging.info("%s - Process completed for active asset IDs data.", datetime.now())
