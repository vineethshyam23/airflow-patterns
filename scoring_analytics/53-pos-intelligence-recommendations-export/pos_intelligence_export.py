"""
Monthly full-load Avro export of POS Intelligence article recommendations.

Streams Vertex / ML preprocessed recommendation tables to a partner
event ingest API. One country at a time; 500-row Avro chunks.

Why this exists separately from menu-gap exports (patterns 12/14):
  Those ship ranked *menu* opportunity gaps. This feed ships
  wholesale *article* recommendations driven by POS usage vs
  purchase gap — different schema, different upstream (Vertex
  preprocessed tables), different auth path (client_credentials
  first, password-grant fallback).

Source (read-only):
  dags/etl_dana_pos_intelligence_recommendations_export.py
  dags/horeca_digital/dana_pos_intelligence_export.py

Sanitized vs production:
  - GCP project / dataset / schema names generalized
  - Event API host + schema ids externalized to Variables
  - Real OAuth Variable names generalized
  - Package import horeca_digital.utils.dsa_cost.batched → local batched
  - Hard-coded prod schema hex → Variable with placeholder default
  - Brand / ticket / owner identifiers removed
"""

from __future__ import annotations

import base64
import io
import json
import logging
import os
import time
from datetime import datetime
from typing import Iterable, Iterator, List, Optional, Tuple, TypeVar

import avro.io
import avro.schema
import requests
from airflow.models import Variable
from google.cloud import bigquery

T = TypeVar("T")

ENV_VAR_NAME = "env"
ENV = os.environ.get(ENV_VAR_NAME, Variable.get(ENV_VAR_NAME))

# Pilot market. Append "de" (etc.) once the Vertex _{CC} table exists.
COUNTRY_ISO_CODES = ["fr"]
CHUNK_SIZE = 500

VERTEX_PROJECT = "ml_project"
PROD_SCHEMA_ID_DEFAULT = "POS_INTELLIGENCE_SCHEMA_PROD"

if ENV == "DEV":
    OAUTH_USERNAME = Variable.get("event_api_username")
    OAUTH_PASSWORD = Variable.get("event_api_password")
    CLIENT_ID = Variable.get("event_api_client_id_dev")
    CLIENT_SECRET: Optional[Tuple[Optional[str], str]] = (
        "event_api_client_secret_dev",
        Variable.get("event_api_client_secret_dev"),
    )
    OAUTH2_URL = Variable.get("event_api_oauth2_url_dev")
    BIGQUERY_PROJECT = "dwh_project_dev"
    SCHEMA_ID = Variable.get(
        "pos_intelligence_schema_id_dev",
        default_var=PROD_SCHEMA_ID_DEFAULT,
    )
    SOURCE_DATASET = "foodgraph_preprocessed_dev"
    INGEST_BASE = Variable.get(
        "event_api_ingest_base_dev",
        default_var="https://api.example.com/event-ingest/bulk",
    )
else:
    OAUTH_USERNAME = Variable.get("event_api_username")
    OAUTH_PASSWORD = Variable.get("event_api_password")
    CLIENT_ID = Variable.get("event_api_client_id")
    prod_secrets: List[Tuple[Optional[str], str]] = []
    for _name in ("event_api_client_secret",):
        try:
            _val = Variable.get(_name)
        except Exception:
            _val = None
        if _val:
            prod_secrets.append((_name, _val))
    CLIENT_SECRET = prod_secrets[0] if prod_secrets else None
    OAUTH2_URL = Variable.get("event_api_oauth2_url")
    BIGQUERY_PROJECT = "dwh_project"
    SCHEMA_ID = Variable.get(
        "pos_intelligence_schema_id",
        default_var=PROD_SCHEMA_ID_DEFAULT,
    )
    # TEMP in production: ACC table lagged the pilot. Switch to
    # foodgraph_preprocessed once the acceptance Vertex table lands.
    SOURCE_DATASET = "foodgraph_preprocessed_dev"
    INGEST_BASE = Variable.get(
        "event_api_ingest_base",
        default_var="https://api.example.com/event-ingest/bulk",
    )

# Avro contract v1. menu_item_names intentionally omitted — not on the
# registered partner schema for the pilot.
POS_INTELLIGENCE_SCHEMA = """{
  "namespace": "platform",
  "type": "record",
  "name": "pos_intelligence_recommendations",
  "doc": "POS Intelligence wholesale article recommendations",
  "gdpr_info": {
    "table_PII": "no",
    "column_PII": []
  },
  "fields": [
    {"name": "establishment_id", "type": ["null", "string"], "default": null},
    {"name": "wholesale_id", "type": ["null", "string"], "default": null},
    {"name": "customer_no", "type": ["null", "long"], "default": null},
    {"name": "home_store_id", "type": ["null", "long"], "default": null},
    {"name": "months_available_pos_data", "type": ["null", "long"], "default": null},
    {"name": "is_pos_active", "type": ["null", "boolean"], "default": null},
    {"name": "period_start_date", "type": ["null", "string"], "default": null},
    {"name": "period_length", "type": ["null", "string"], "default": null},
    {"name": "ingredient_name", "type": ["null", "string"], "default": null},
    {"name": "estimated_pos_usage_kg", "type": ["null", "double"], "default": null},
    {"name": "wholesale_purchase_kg", "type": ["null", "double"], "default": null},
    {"name": "gap", "type": ["null", "double"], "default": null},
    {"name": "branch_desc", "type": ["null", "string"], "default": null},
    {"name": "article_no", "type": ["null", "long"], "default": null},
    {"name": "variant_tu_key", "type": ["null", "long"], "default": null},
    {"name": "article_name", "type": ["null", "string"], "default": null},
    {"name": "description", "type": ["null", "string"], "default": null},
    {"name": "n_bundle_article_quantity", "type": ["null", "double"], "default": null},
    {"name": "article_quantity", "type": ["null", "double"], "default": null},
    {"name": "article_rank", "type": ["null", "long"], "default": null},
    {"name": "run_date", "type": ["null", "string"], "default": null},
    {"name": "time_grain", "type": ["null", "string"], "default": null},
    {"name": "country_code", "type": ["null", "string"], "default": null}
  ]
}"""


def batched(iterable: Iterable[T], n: int) -> Iterator[List[T]]:
    """Yield lists of up to n items from iterable (streaming-friendly)."""
    if n < 1:
        raise ValueError("n must be >= 1")
    batch: List[T] = []
    for item in iterable:
        batch.append(item)
        if len(batch) >= n:
            yield batch
            batch = []
    if batch:
        yield batch


def _b64(s: str) -> str:
    return base64.b64encode(s.encode()).decode()


def _long(d: dict, k: str):
    v = d.get(k)
    return int(v) if v is not None else None


def _double(d: dict, k: str):
    v = d.get(k)
    return float(v) if v is not None else None


def _str(d: dict, k: str):
    v = d.get(k)
    return (v if isinstance(v, str) else str(v)) if v is not None else None


def _bool(d: dict, k: str):
    v = d.get(k)
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    return bool(v)


class EventIngestAPI:
    """OAuth + chunked POST for the partner event bus.

    Tries client_credentials across configured secrets first, then
    password-grant. 401 on ingest refreshes the token and retries the
    same payload — important for long monthly full loads that outlive
    a short-lived access token.
    """

    def __init__(self, token_url, client_id, client_secret):
        self.token_url = token_url
        self.token = None
        self.client_id = client_id
        self.client_secrets: List[Tuple[Optional[str], str]] = []
        if client_secret:
            if isinstance(client_secret, tuple) and len(client_secret) == 2:
                self.client_secrets = [client_secret]
            elif isinstance(client_secret, (list, tuple)):
                for s in client_secret:
                    if isinstance(s, tuple) and len(s) == 2:
                        self.client_secrets.append(s)
                    elif s:
                        self.client_secrets.append((None, s))
            else:
                self.client_secrets = [(None, client_secret)]
        self.successful_credential = None

    def get_token(self):
        realm = None
        for _var in ("EVENT_API_REALM", "event_api_realm", "event_api_realm_id"):
            try:
                _val = Variable.get(_var)
            except Exception:
                _val = None
            if _val:
                realm = _val
                break

        last_exc = None

        for name, candidate in self.client_secrets:
            if not candidate:
                continue
            headers = {
                "Authorization": "Basic " + _b64(self.client_id + ":" + candidate),
                "Content-Type": "application/x-www-form-urlencoded",
            }
            params_cc = {
                "grant_type": "client_credentials",
                "client_id": self.client_id,
            }
            if realm:
                params_cc["realm_id"] = realm
            try:
                r = requests.post(
                    self.token_url, headers=headers, data=params_cc, timeout=30
                )
                r.raise_for_status()
                resp_json = r.json()
                if "access_token" in resp_json:
                    self.token = resp_json["access_token"]
                    self.successful_credential = name or "unnamed_secret"
                    logging.info(
                        "Acquired token with client_credentials using: %s",
                        self.successful_credential,
                    )
                    return self.token
                last_exc = RuntimeError(
                    "Missing access_token in client_credentials response"
                )
            except Exception as e:
                logging.warning(
                    "client_credentials failed for %s: %s", name or "<unnamed>", e
                )
                last_exc = e

        params_pw = {
            "grant_type": "password",
            "username": OAUTH_USERNAME,
            "password": OAUTH_PASSWORD,
        }
        if realm:
            params_pw["realm_id"] = realm

        if self.client_secrets:
            for name, candidate in self.client_secrets:
                if not candidate:
                    continue
                headers = {
                    "Authorization": "Basic "
                    + _b64(self.client_id + ":" + candidate),
                    "Content-Type": "application/x-www-form-urlencoded",
                }
                try:
                    r = requests.post(
                        self.token_url, headers=headers, data=params_pw, timeout=30
                    )
                    r.raise_for_status()
                    resp_json = r.json()
                    if "access_token" in resp_json:
                        self.token = resp_json["access_token"]
                        self.successful_credential = (
                            name or "unnamed_secret"
                        ) + "/password"
                        logging.info(
                            "Acquired token with password grant using: %s",
                            self.successful_credential,
                        )
                        return self.token
                    last_exc = RuntimeError(
                        "Missing access_token in password grant response"
                    )
                except Exception as e:
                    logging.warning(
                        "password grant failed for %s: %s", name or "<unnamed>", e
                    )
                    last_exc = e
        else:
            try:
                r = requests.post(self.token_url, data=params_pw, timeout=30)
                r.raise_for_status()
                resp_json = r.json()
                if "access_token" in resp_json:
                    self.token = resp_json["access_token"]
                    self.successful_credential = "password"
                    logging.info("Acquired token with password grant (no client secret)")
                    return self.token
                last_exc = RuntimeError(
                    "Missing access_token in password grant response"
                )
            except Exception as e:
                logging.warning("password grant failed (no client secret): %s", e)
                last_exc = e

        logging.error("All token authentication options exhausted.")
        if last_exc:
            raise last_exc
        raise RuntimeError("Failed to obtain access token")

    def endpoint(self, url, data=None, max_retries=10):
        if self.token is None:
            self.get_token()

        headers = {
            "Authorization": "Bearer " + self.token,
            "Content-Type": "application/vnd.example.events.json",
            "accept": "application/vnd.example.events.json",
        }

        for attempt in range(max_retries):
            try:
                r = requests.post(url, headers=headers, data=data, timeout=120)

                if r.status_code == 401:
                    logging.warning(
                        "401 from event ingest — refreshing token and retrying"
                    )
                    self.token = None
                    self.get_token()
                    headers["Authorization"] = "Bearer " + self.token
                    continue

                try:
                    r.raise_for_status()
                except requests.HTTPError:
                    logging.error(
                        "Event ingest error status %s, body: %s",
                        r.status_code,
                        getattr(r, "text", "<unreadable>"),
                    )
                    raise

                try:
                    return r.json()
                except ValueError:
                    logging.info(
                        "Event ingest returned non-JSON: %s",
                        getattr(r, "text", None),
                    )
                    return r.text

            except (
                requests.exceptions.JSONDecodeError,
                requests.exceptions.RequestException,
            ) as e:
                wait = min(5 * (attempt + 1), 60)
                logging.warning(
                    "Event API attempt %d/%d failed for url=%s: %s — retry in %ds",
                    attempt + 1,
                    max_retries,
                    url,
                    e,
                    wait,
                )
                if attempt < max_retries - 1:
                    time.sleep(wait)
                else:
                    raise


def _source_table(iso_code_lower: str) -> str:
    cc = iso_code_lower.upper()
    return (
        f"`{VERTEX_PROJECT}.{SOURCE_DATASET}"
        f".pos_article_final_recommendation_{cc}`"
    )


def _build_query(iso_code_lower: str) -> str:
    """Full-load SELECT aligned with Avro contract v1. Omits menu_item_names."""
    table = _source_table(iso_code_lower)
    return f"""
    SELECT
        CAST(establishment_id AS STRING) AS establishment_id,
        CAST(wholesale_id AS STRING) AS wholesale_id,
        CAST(customer_no AS INT64) AS customer_no,
        CAST(home_store_id AS INT64) AS home_store_id,
        CAST(months_available_pos_data AS INT64) AS months_available_pos_data,
        CAST(is_pos_active AS BOOL) AS is_pos_active,
        CAST(period_start_date AS STRING) AS period_start_date,
        CAST(period_length AS STRING) AS period_length,
        CAST(ingredient_name AS STRING) AS ingredient_name,
        CAST(estimated_pos_usage_kg AS FLOAT64) AS estimated_pos_usage_kg,
        CAST(wholesale_purchase_kg AS FLOAT64) AS wholesale_purchase_kg,
        CAST(gap AS FLOAT64) AS gap,
        CAST(branch_desc AS STRING) AS branch_desc,
        CAST(article_no AS INT64) AS article_no,
        CAST(variant_tu_key AS INT64) AS variant_tu_key,
        CAST(article_name AS STRING) AS article_name,
        CAST(description AS STRING) AS description,
        CAST(n_bundle_article_quantity AS FLOAT64) AS n_bundle_article_quantity,
        CAST(article_quantity AS FLOAT64) AS article_quantity,
        CAST(article_rank AS INT64) AS article_rank,
        CAST(run_date AS STRING) AS run_date,
        CAST(time_grain AS STRING) AS time_grain,
        CAST(country_code AS STRING) AS country_code
    FROM {table}
    """


def _row_to_avro(row) -> dict:
    d = dict(row.items())
    return {
        "establishment_id": _str(d, "establishment_id"),
        "wholesale_id": _str(d, "wholesale_id"),
        "customer_no": _long(d, "customer_no"),
        "home_store_id": _long(d, "home_store_id"),
        "months_available_pos_data": _long(d, "months_available_pos_data"),
        "is_pos_active": _bool(d, "is_pos_active"),
        "period_start_date": _str(d, "period_start_date"),
        "period_length": _str(d, "period_length"),
        "ingredient_name": _str(d, "ingredient_name"),
        "estimated_pos_usage_kg": _double(d, "estimated_pos_usage_kg"),
        "wholesale_purchase_kg": _double(d, "wholesale_purchase_kg"),
        "gap": _double(d, "gap"),
        "branch_desc": _str(d, "branch_desc"),
        "article_no": _long(d, "article_no"),
        "variant_tu_key": _long(d, "variant_tu_key"),
        "article_name": _str(d, "article_name"),
        "description": _str(d, "description"),
        "n_bundle_article_quantity": _double(d, "n_bundle_article_quantity"),
        "article_quantity": _double(d, "article_quantity"),
        "article_rank": _long(d, "article_rank"),
        "run_date": _str(d, "run_date"),
        "time_grain": _str(d, "time_grain"),
        "country_code": _str(d, "country_code"),
    }


def _encode_row(row, writer) -> str:
    bw = io.BytesIO()
    encoder = avro.io.BinaryEncoder(bw)
    writer.write(_row_to_avro(row), encoder)
    return base64.b64encode(bw.getvalue()).decode("utf-8")


def _ingestbulk_url(country: str, schema_id: str) -> str:
    base = INGEST_BASE.rstrip("/")
    return f"{base}/{country.lower()}/{schema_id}"


def send_pos_intelligence_data(country: str = "fr"):
    """Full-load POS Intelligence recommendations for one country."""
    api = EventIngestAPI(OAUTH2_URL, CLIENT_ID, CLIENT_SECRET)
    try:
        api.get_token()
        logging.info(
            "Token acquired using credential: %s",
            getattr(api, "successful_credential", None),
        )
    except Exception as e:
        logging.error("Failed to acquire token during startup: %s", e)

    query = _build_query(country)
    label = "pos_intelligence_recommendations"
    print(f"{datetime.now()} - [{country}] {label} querying BQ {SOURCE_DATASET}...")
    client = bigquery.Client(project=BIGQUERY_PROJECT)
    results = client.query(query).result()
    total_rows = results.total_rows
    logging.info(
        "%s - [%s] %s query rows: %s", datetime.now(), country, label, total_rows
    )
    base_url = _ingestbulk_url(country, SCHEMA_ID)
    logging.info("%s - [%s] %s ingest URL: %s", datetime.now(), country, label, base_url)

    schema_parsed = avro.schema.parse(POS_INTELLIGENCE_SCHEMA)
    writer = avro.io.DatumWriter(schema_parsed)

    row_count = 0
    encoded = (_encode_row(row, writer) for row in results)
    for chunk in batched(encoded, CHUNK_SIZE):
        payload = json.dumps({"records": [{"value": rec} for rec in chunk]})
        try:
            resp = api.endpoint(url=base_url, data=payload)
            logging.info("Event ingest response: %s", resp)
        except Exception:
            logging.exception(
                "Failed chunk for country %s label %s. Payload size=%d",
                country,
                label,
                len(chunk),
            )
            raise
        row_count += len(chunk)
        print(f"{datetime.now()} - [{country}] {label} rows {row_count} — {resp}")

    print(f"{datetime.now()} - [{country}] {label} DONE. Total rows: {row_count}")


if __name__ == "__main__":
    # Smoke: schema parse + SQL shape for the pilot country. No BQ / HTTP.
    parsed = avro.schema.parse(POS_INTELLIGENCE_SCHEMA)
    assert parsed.name == "pos_intelligence_recommendations"
    sql = _build_query("fr")
    assert "pos_article_final_recommendation_FR" in sql
    assert "menu_item_names" not in sql
    print("schema ok; SQL prefix:")
    print(sql.strip().splitlines()[0])
    print(f"countries={COUNTRY_ISO_CODES} chunk={CHUNK_SIZE}")
