"""
Avro bulk ingest for wholesale acquisition (MAG) historical reporting.

Per country: SELECT date / product_bundle / sales metrics from the
refined historical acquisitions table → Avro → chunked POST to the
partner event bus.

Source (read-only):
  dags/horeca_digital/dana_mag_acquisition.py

Sanitized fixes vs production module:
  - Avro schema parsed once per send (source parsed every row)
  - 401 retry keeps the original payload (source dropped `data`)
  - HTTP errors raise instead of only printing the body
  - Schema id / ingest base externalized to Variables
  - `__main__` no longer references an undefined `country`
  - Brand / project / table names generalized
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
        "mag_acquisition_schema_id_dev",
        default_var="MAG_ACQUISITION_SCHEMA_DEV",
    )
    SCHEMA_NAME = "mag_reporting_acquisition_dev"
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
        "mag_acquisition_schema_id",
        default_var="MAG_ACQUISITION_SCHEMA_PROD",
    )
    SCHEMA_NAME = "mag_reporting_acquisition"
    INGEST_BASE = Variable.get(
        "event_api_ingest_base",
        default_var="https://api.example.com/event-ingest/bulk",
    )

# Aggregate / corporate rollup market code used in Composer task ids.
# Warehouse stores that rollup under country = 'corp'.
AGGREGATE_COUNTRY_CODE = "ag"
AGGREGATE_WAREHOUSE_COUNTRY = "corp"

CHUNK_SIZE = 500

ACQUISITION_AVRO_SCHEMA = json.dumps(
    {
        "namespace": "company",
        "type": "record",
        "name": SCHEMA_NAME,
        "doc": "MAG reporting - historical acquisition values by product bundle",
        "gdpr_info": {"table_PII": "no", "column_PII": []},
        "fields": [
            {
                "name": "date",
                "type": {"type": "int", "logicalType": "date"},
                "doc": "Date of the values",
            },
            {
                "name": "product_bundle",
                "type": "string",
                "doc": "Name of the product bundle",
            },
            {
                "name": "sales_value",
                "type": ["null", "int"],
                "default": None,
                "doc": "Sales value for the period",
            },
            {
                "name": "sales_all_time",
                "type": ["null", "int"],
                "default": None,
                "doc": "All-time sales value",
            },
        ],
    }
)


def chunks(items: List[str], n: int) -> Iterator[List[str]]:
    for i in range(0, len(items), n):
        yield items[i : i + n]


def _b64(s: str) -> str:
    return base64.b64encode(s.encode()).decode()


class EventApiClient:
    """OAuth password-grant client for event bulk ingest."""

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
        r = requests.post(self.token_url, headers=headers, params=params, timeout=60)
        r.raise_for_status()
        self.token = r.json()["access_token"]
        return self.token

    def post_json(self, url: str, data: str):
        if self.token is None:
            self.get_token()

        headers = {
            "Authorization": "Bearer " + self.token,
            "Content-Type": "application/vnd.company.events.json",
        }
        r = requests.post(url, headers=headers, data=data, timeout=120)

        if r.status_code == 401:
            # Keep the same payload on retry — source dropped `data` here.
            self.token = None
            return self.post_json(url=url, data=data)

        r.raise_for_status()
        return r.json()


def _warehouse_country(country: str) -> str:
    if country.lower() == AGGREGATE_COUNTRY_CODE:
        return AGGREGATE_WAREHOUSE_COUNTRY
    return country.lower()


def _build_query(country: str) -> str:
    cc = _warehouse_country(country)
    return f"""
    SELECT date, product_bundle, sales_value, sales_all_time
    FROM `refined.hist_acquisitions_reporting`
    WHERE lower(reseller_country) = '{cc}'
    """


def send_mag_acquisition_data(country: str):
    """Export one country's acquisition history to event ingest."""
    client = EventApiClient(OAUTH2_URL, CLIENT_ID, CLIENT_SECRET)
    qry = _build_query(country)

    print(f"{datetime.now()} - [{country}] acquisition query...")
    bq = bigquery.Client(project=BIGQUERY_PROJECT)
    results = bq.query(qry).result()

    schema_parsed = avro.schema.parse(ACQUISITION_AVRO_SCHEMA)
    writer = avro.io.DatumWriter(schema_parsed)

    encoded: List[str] = []
    for row in results:
        bw = io.BytesIO()
        encoder = avro.io.BinaryEncoder(bw)
        writer.write(
            {
                "date": row["date"],
                "product_bundle": row["product_bundle"],
                "sales_value": row["sales_value"],
                "sales_all_time": row["sales_all_time"],
            },
            encoder,
        )
        encoded.append(base64.b64encode(bw.getvalue()).decode("utf-8"))

    base_url = f"{INGEST_BASE.rstrip('/')}/{country.lower()}/{SCHEMA_ID}"
    chunked = list(chunks(encoded, CHUNK_SIZE))
    print(
        f"{datetime.now()} - [{country}] acquisition rows={len(encoded)} "
        f"chunks={len(chunked)}"
    )

    sent = 0
    for chunk in chunked:
        sent += len(chunk)
        payload = json.dumps({"records": [{"value": rec} for rec in chunk]})
        resp = client.post_json(url=base_url, data=payload)
        print(
            f"{datetime.now()} - [{country}] acquisition "
            f"chunk through {sent}/{len(encoded)} — {resp}"
        )

    print(f"{datetime.now()} - [{country}] acquisition DONE.")


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    parsed = avro.schema.parse(ACQUISITION_AVRO_SCHEMA)
    print("avro fields:", [f.name for f in parsed.fields])
    print(_build_query("de")[:200])
    print(_build_query(AGGREGATE_COUNTRY_CODE)[:200])
