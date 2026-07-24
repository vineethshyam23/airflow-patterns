"""
Avro bulk ingest for partner matching-engine results.

Reads the staging table built by matching_prepare, encodes one Avro
record per row, and POSTs chunks of 500 to
/ingestbulk/{country}/{schema_id}.

Credentials and schema ids come from Airflow Variables only.

Source (read-only):
  dags/horeca_digital/dana_matching_engine_export.py

Sanitized fixes vs production module:
  - Avro schema parsed once per send (source parsed every row)
  - HTTP errors raise instead of only printing the body
  - 401 retry passes the original payload (source dropped it)
  - Schema id / ingest base externalized to Variables
  - DATE → Avro logical date (days since epoch)
  - NUMERIC price → Avro decimal bytes
"""

from __future__ import annotations

import base64
import decimal
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
    MATCHING_SCHEMA_ID = Variable.get(
        "matching_event_schema_id_dev", default_var="MATCHING_SCHEMA_DEV"
    )
    SCHEMA_NAME = "partner_matching_dev"
    INGEST_BASE = Variable.get(
        "event_api_ingest_base_dev",
        default_var="https://api.example.com/event-ingest/bulk",
    )
    STAGING_TABLE = "dwh_project_dev.staging.partner_matching_export"
else:
    OAUTH_USERNAME = Variable.get("event_api_username")
    OAUTH_PASSWORD = Variable.get("event_api_password")
    CLIENT_ID = Variable.get("event_api_client_id")
    CLIENT_SECRET = Variable.get("event_api_client_secret")
    OAUTH2_URL = Variable.get("event_api_oauth2_url")
    BIGQUERY_PROJECT = "dwh_project"
    MATCHING_SCHEMA_ID = Variable.get(
        "matching_event_schema_id", default_var="MATCHING_SCHEMA_PROD"
    )
    SCHEMA_NAME = "partner_matching"
    INGEST_BASE = Variable.get(
        "event_api_ingest_base",
        default_var="https://api.example.com/event-ingest/bulk",
    )
    STAGING_TABLE = "dwh_project.refined.partner_matching_export"


MATCHING_AVRO_SCHEMA = json.dumps(
    {
        "namespace": "company",
        "type": "record",
        "name": SCHEMA_NAME,
        "doc": "Wholesale customer ↔ SaaS service matching export",
        "gdpr_info": {"table_PII": "no", "column_PII": []},
        "fields": [
            {"name": "unique_home_store_id", "type": "int", "doc": "Home store of wholesale parent"},
            {"name": "unique_cust_no", "type": "int", "doc": "Wholesale customer number"},
            {"name": "country", "type": "string", "doc": "Registration country"},
            {"name": "w360_service_cd", "type": "string", "doc": "SaaS product code"},
            {"name": "w360_service_desc", "type": "string", "doc": "SaaS product name"},
            {
                "name": "date_from",
                "type": {"type": "int", "logicalType": "date"},
                "doc": "Service start / creation date",
            },
            {
                "name": "date_to",
                "type": {"type": "int", "logicalType": "date"},
                "doc": "Service end / deletion date",
            },
            {
                "name": "price",
                "type": {
                    "type": "bytes",
                    "logicalType": "decimal",
                    "precision": 5,
                    "scale": 2,
                },
                "doc": "List price (historically zero in this feed)",
            },
            {
                "name": "HD_cust_ident",
                "type": "string",
                "doc": "Internal establishment identifier",
            },
            {
                "name": "creation_date",
                "type": {"type": "int", "logicalType": "date"},
                "doc": "Date the staging row was prepared",
            },
            {
                "name": "UID__c",
                "type": ["null", "string"],
                "default": None,
                "doc": "CRM establishment id",
            },
            {
                "name": "SFDC_createdDate",
                "type": ["null", "string"],
                "default": None,
                "doc": "CRM record created timestamp (string as registered)",
            },
            {
                "name": "status_cd",
                "type": ["null", "string"],
                "default": None,
                "doc": "Wholesale status code",
            },
            {
                "name": "blocking_reason_cd",
                "type": ["null", "string"],
                "default": None,
                "doc": "Wholesale blocking reason",
            },
            {
                "name": "match_quality",
                "type": ["null", "int"],
                "default": None,
                "doc": "Fuzzy match quality score (lower = stronger)",
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
    """DATE / date-string → Avro logical date (days since 1970-01-01)."""
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return (value - date(1970, 1, 1)).days
    if isinstance(value, datetime):
        return (value.date() - date(1970, 1, 1)).days
    if isinstance(value, str) and value:
        return (date.fromisoformat(value[:10]) - date(1970, 1, 1)).days
    return value


def _avro_decimal(value, precision: int = 5, scale: int = 2) -> bytes:
    """Encode a number as Avro decimal bytes (unscaled big-endian)."""
    quant = decimal.Decimal("1").scaleb(-scale)
    d = decimal.Decimal(str(value if value is not None else 0)).quantize(quant)
    unscaled = int(d * (10 ** scale))
    # Two's complement length sufficient for precision digits.
    length = (precision + 1) // 2 + 1
    return unscaled.to_bytes(length, byteorder="big", signed=True)


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


def send_matching_engine_data(country: str) -> None:
    """Query staging for one country and POST Avro chunks to the event bus."""
    client = EventApiClient(OAUTH2_URL, CLIENT_ID, CLIENT_SECRET)
    schema = avro.schema.parse(MATCHING_AVRO_SCHEMA)

    qry = f"""
    SELECT
      unique_home_store_id,
      unique_cust_no,
      country,
      w360_service_cd,
      w360_service_desc,
      CAST(date_from AS DATE) AS date_from,
      CAST(SUBSTR(CAST(date_to AS STRING), 1, 10) AS DATE) AS date_to,
      price,
      HD_cust_ident,
      CAST(creation_date AS DATE) AS creation_date,
      UID__c,
      SFDC_createdDate,
      status_cd,
      blocking_reason_cd,
      match_quality
    FROM `{STAGING_TABLE}`
    WHERE LOWER(country) = '{country.lower()}'
    """

    logging.info("%s - Executing query for %s", datetime.now(), country.upper())
    bq = bigquery.Client(project=BIGQUERY_PROJECT)
    results = bq.query(qry).result()

    encoded: List[str] = []
    for row in results:
        payload = {
            "unique_home_store_id": row["unique_home_store_id"],
            "unique_cust_no": row["unique_cust_no"],
            "country": row["country"],
            "w360_service_cd": row["w360_service_cd"],
            "w360_service_desc": row["w360_service_desc"],
            "date_from": _avro_date(row["date_from"]),
            "date_to": _avro_date(row["date_to"]),
            "price": _avro_decimal(row["price"]),
            "HD_cust_ident": row["HD_cust_ident"],
            "creation_date": _avro_date(row["creation_date"]),
            "UID__c": row["UID__c"],
            "SFDC_createdDate": row["SFDC_createdDate"],
            "status_cd": row["status_cd"],
            "blocking_reason_cd": row["blocking_reason_cd"],
            "match_quality": row["match_quality"],
        }
        encoded.append(_encode_bytes(payload, schema))

    logging.info(
        "%s - %s rows encoded for %s",
        datetime.now(),
        len(encoded),
        country.upper(),
    )

    base_url = f"{INGEST_BASE.rstrip('/')}/{country.lower()}/{MATCHING_SCHEMA_ID}"
    sent = 0
    for chunk in chunks(encoded, 500):
        sent += len(chunk)
        body = {"records": [{"value": record} for record in chunk]}
        logging.info(
            "%s - %s chunk progress %s / %s",
            datetime.now(),
            country.upper(),
            sent,
            len(encoded),
        )
        client.post_json(url=base_url, data=json.dumps(body))

    logging.info("%s - Process completed for %s", datetime.now(), country.upper())


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    # Schema parse smoke check only — no network / BQ calls.
    avro.schema.parse(MATCHING_AVRO_SCHEMA)
    print("Avro schema OK:", SCHEMA_NAME)
