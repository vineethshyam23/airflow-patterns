"""
Avro bulk ingest for wholesale penetration (MAG) historical reporting.

Per country: SELECT date + active/buying wholesale counts + active/
paying platform subscription counts → Avro → chunked POST.

Source (read-only):
  dags/horeca_digital/dana_mag_penetration.py

Sanitized fixes vs production module:
  - Avro schema parsed once per send (source parsed every row)
  - 401 retry keeps the original payload (source dropped `data`)
  - HTTP errors raise instead of only printing the body
  - Schema id / ingest base externalized to Variables
  - `__main__` no longer references an undefined `country`
  - MCC / HD field names → wholesale / platform
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
        "mag_penetration_schema_id_dev",
        default_var="MAG_PENETRATION_SCHEMA_DEV",
    )
    SCHEMA_NAME = "mag_reporting_penetration_dev"
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
        "mag_penetration_schema_id",
        default_var="MAG_PENETRATION_SCHEMA_PROD",
    )
    SCHEMA_NAME = "mag_reporting_penetration"
    INGEST_BASE = Variable.get(
        "event_api_ingest_base",
        default_var="https://api.example.com/event-ingest/bulk",
    )

AGGREGATE_COUNTRY_CODE = "ag"
AGGREGATE_WAREHOUSE_COUNTRY = "corp"

CHUNK_SIZE = 500

PENETRATION_AVRO_SCHEMA = json.dumps(
    {
        "namespace": "company",
        "type": "record",
        "name": SCHEMA_NAME,
        "doc": "MAG reporting - wholesale / platform penetration rates",
        "gdpr_info": {"table_PII": "no", "column_PII": []},
        "fields": [
            {
                "name": "date",
                "type": {"type": "int", "logicalType": "date"},
                "doc": "Date of the values",
            },
            {
                "name": "active_wholesale",
                "type": "int",
                "doc": "Active wholesale customers",
            },
            {
                "name": "buying_wholesale",
                "type": "int",
                "doc": "Buying wholesale customers",
            },
            {
                "name": "active_platform",
                "type": "int",
                "doc": "Active wholesale customers with a platform subscription",
            },
            {
                "name": "paying_platform",
                "type": "int",
                "doc": "Active wholesale customers with a billable platform subscription",
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
    # Aggregate rollup historically had no IFNULL wrapping; keep that
    # divergence visible so a null spike in corp is not silently zeroed.
    if country.lower() == AGGREGATE_COUNTRY_CODE:
        return f"""
        SELECT date, active_wholesale, buying_wholesale,
               active_platform, paying_platform
        FROM `refined.hist_penetration_rates_reporting`
        WHERE lower(country) = '{cc}'
        """
    return f"""
    SELECT
        date,
        ifnull(active_wholesale, 0) AS active_wholesale,
        ifnull(buying_wholesale, 0) AS buying_wholesale,
        ifnull(active_platform, 0) AS active_platform,
        ifnull(paying_platform, 0) AS paying_platform
    FROM `refined.hist_penetration_rates_reporting`
    WHERE lower(country) = '{cc}'
    """


def send_mag_penetration_data(country: str):
    """Export one country's penetration history to event ingest."""
    client = EventApiClient(OAUTH2_URL, CLIENT_ID, CLIENT_SECRET)
    qry = _build_query(country)

    print(f"{datetime.now()} - [{country}] penetration query...")
    bq = bigquery.Client(project=BIGQUERY_PROJECT)
    results = bq.query(qry).result()

    schema_parsed = avro.schema.parse(PENETRATION_AVRO_SCHEMA)
    writer = avro.io.DatumWriter(schema_parsed)

    encoded: List[str] = []
    for row in results:
        bw = io.BytesIO()
        encoder = avro.io.BinaryEncoder(bw)
        writer.write(
            {
                "date": row["date"],
                "active_wholesale": row["active_wholesale"],
                "buying_wholesale": row["buying_wholesale"],
                "active_platform": row["active_platform"],
                "paying_platform": row["paying_platform"],
            },
            encoder,
        )
        encoded.append(base64.b64encode(bw.getvalue()).decode("utf-8"))

    base_url = f"{INGEST_BASE.rstrip('/')}/{country.lower()}/{SCHEMA_ID}"
    chunked = list(chunks(encoded, CHUNK_SIZE))
    print(
        f"{datetime.now()} - [{country}] penetration rows={len(encoded)} "
        f"chunks={len(chunked)}"
    )

    sent = 0
    for chunk in chunked:
        sent += len(chunk)
        payload = json.dumps({"records": [{"value": rec} for rec in chunk]})
        resp = client.post_json(url=base_url, data=payload)
        print(
            f"{datetime.now()} - [{country}] penetration "
            f"chunk through {sent}/{len(encoded)} — {resp}"
        )

    print(f"{datetime.now()} - [{country}] penetration DONE.")


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    parsed = avro.schema.parse(PENETRATION_AVRO_SCHEMA)
    print("avro fields:", [f.name for f in parsed.fields])
    print(_build_query("de")[:280])
    print(_build_query(AGGREGATE_COUNTRY_CODE)[:280])
