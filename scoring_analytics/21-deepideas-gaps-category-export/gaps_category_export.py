"""
Avro bulk ingest for Deepideas main-category gap rows.

Pulls a SELECT (usually the today/yesterday hash-delta from
delta_queries.py), Avro-encodes each row, and POSTs chunks of 500 to
/ingestbulk/{country}/{schema_id}.

Credentials and schema ids come from Airflow Variables only.

Source (read-only):
  dags/horeca_digital/dana_deepideas_gaps_category_export.py

Sanitized fixes vs production module:
  - Schema id / ingest base externalized to Variables
  - GCP project / Avro namespace / field names generalized
  - wholesale_id replaces metro_id; product_main_cat_* replaces mge_*
  - 401 clears token and retries the same payload
  - Schema parsed once per send (not once per row)
"""

from __future__ import annotations

import base64
import io
import json
import logging
import os
from datetime import datetime
from typing import Any, Dict, Iterable, List

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
        "gaps_category_schema_id_dev",
        default_var="GAPS_CATEGORY_SCHEMA_DEV",
    )
    SCHEMA_NAME = "foodgraph_gap_main_category_dev"
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
        "gaps_category_schema_id",
        default_var="GAPS_CATEGORY_SCHEMA_PROD",
    )
    SCHEMA_NAME = "foodgraph_gap_main_category"
    INGEST_BASE = Variable.get(
        "event_api_ingest_base",
        default_var="https://api.example.com/event-ingest/bulk",
    )

AVRO_SCHEMA = json.dumps(
    {
        "namespace": "company",
        "type": "record",
        "name": SCHEMA_NAME,
        "doc": (
            "Menu-implied product main categories with no wholesale "
            "purchase revenue in the last year (category-level gaps)"
        ),
        "gdpr_info": {"table_PII": "no", "column_PII": []},
        "fields": [
            {"name": "wholesale_id", "type": "long", "doc": "Wholesale customer id"},
            {
                "name": "product_main_cat_desc",
                "type": "string",
                "doc": "Name of gap product main category",
            },
            {
                "name": "product_main_cat_id",
                "type": "int",
                "doc": "Id of gap product main category",
            },
            {
                "name": "avg_relevance",
                "type": "string",
                "doc": (
                    "Average ingredient relevance on main-category "
                    "level from menu prioritization; higher is stronger"
                ),
            },
        ],
    }
)

CHUNK_SIZE = 500
log = logging.getLogger(__name__)

FIELD_ORDER = [
    "wholesale_id",
    "product_main_cat_desc",
    "product_main_cat_id",
    "avg_relevance",
]


def _b64(s: str) -> str:
    return base64.b64encode(s.encode()).decode()


def chunks(items: List[Any], n: int) -> Iterable[List[Any]]:
    for i in range(0, len(items), n):
        yield items[i : i + n]


class EventApiClient:
    def __init__(self, token_url: str, client_id: str, client_secret: str):
        self.token_url = token_url
        self.token = None
        self.client_id = client_id
        self.client_secret = client_secret

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
        r = requests.post(self.token_url, headers=headers, params=params, timeout=60)
        r.raise_for_status()
        self.token = r.json()["access_token"]
        return self.token

    def post_events(self, url: str, data: str) -> Dict[str, Any]:
        if self.token is None:
            self.get_token()
        headers = {
            "Authorization": "Bearer " + self.token,
            "Content-Type": "application/vnd.company.events.json",
        }
        r = requests.post(url, headers=headers, data=data, timeout=120)
        if r.status_code == 401:
            # Token expired mid-batch — clear and retry the same payload.
            self.token = None
            return self.post_events(url, data)
        r.raise_for_status()
        return r.json()


def _row_to_avro(row, schema) -> str:
    writer = avro.io.DatumWriter(schema)
    bytes_writer = io.BytesIO()
    encoder = avro.io.BinaryEncoder(bytes_writer)
    payload = {name: row[name] for name in FIELD_ORDER}
    writer.write(payload, encoder)
    return base64.b64encode(bytes_writer.getvalue()).decode("utf-8")


def send_gaps_category_data(country: str, query: str) -> None:
    """Run BQ query, Avro-encode rows, POST in chunks of CHUNK_SIZE."""
    client_api = EventApiClient(OAUTH2_URL, CLIENT_ID, CLIENT_SECRET)
    schema = avro.schema.parse(AVRO_SCHEMA)
    bq = bigquery.Client(project=BIGQUERY_PROJECT)

    log.info("%s - Executing gaps category query...", datetime.now())
    results = bq.query(query).result()

    encoded: List[str] = []
    for row in results:
        encoded.append(_row_to_avro(row, schema))

    log.info("%s - Encoded %s rows; chunking...", datetime.now(), len(encoded))
    base_url = f"{INGEST_BASE.rstrip('/')}/{country}/{SCHEMA_ID}"
    sent = 0
    for chunk in chunks(encoded, CHUNK_SIZE):
        sent += len(chunk)
        payload = {"records": [{"value": row} for row in chunk]}
        log.info(
            "%s - country=%s chunk_progress=%s/%s",
            datetime.now(),
            country.upper(),
            sent,
            len(encoded),
        )
        client_api.post_events(base_url, json.dumps(payload))

    log.info("%s - Gaps category export complete (%s rows).", datetime.now(), len(encoded))


if __name__ == "__main__":
    # Smoke: schema parses; no live BQ / HTTP without Composer Variables.
    avro.schema.parse(AVRO_SCHEMA)
    print("schema_ok", SCHEMA_NAME, "fields", len(FIELD_ORDER))
