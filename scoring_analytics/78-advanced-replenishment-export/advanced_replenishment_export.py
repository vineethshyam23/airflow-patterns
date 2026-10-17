"""
Export Advanced Replenishment extracts to a partner event bus (Avro + ingestbulk).

Subjects:
  ar_predicted_total_quantity_ingredients  daily full load (hash-partitioned)
  ar_platform_pos_customers                monthly full load on the 5th (UTC)
  ar_articles_to_ingredients               monthly full load on the 5th (UTC)

Streams rows from BigQuery to avoid loading a market-wide prediction
table into worker memory. Ten parallel tasks share the daily quantity
feed via FARM_FINGERPRINT on establishment + ingredient + period.

Source (read-only):
  dags/horeca_digital/dana_advanced_replenishment_export.py

Sanitized vs production:
  - GCP project / dataset / table names generalized
  - Event API host + schema ids externalized to Variables
  - Brand / ticket / owner identifiers removed
  - metro_* → wholesale_*; POS product brand → platform
  - Package import horeca_digital.dana_auth → local event_api_auth
"""

from __future__ import annotations

import base64
import io
import json
import logging
import os
import time
from datetime import datetime
from typing import Any, List, Optional, Tuple

import avro.io
import avro.schema
import requests
from airflow.models import Variable
from google.cloud import bigquery

from event_api_auth import (
    acquire_token_with_retry,
    raise_retryable_token_error,
    stagger_parallel_token_request,
)

ENV_VAR_NAME = "env"
ENV = os.environ.get(ENV_VAR_NAME, Variable.get(ENV_VAR_NAME))

ML_PROJECT = "ml_project"
COUNTRY = "de"
CHUNK_SIZE = 2000

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
    SOURCE_DATASET = "foodgraph_preprocessed_dev"
    PREDICTED_SCHEMA_ID = Variable.get(
        "ar_predicted_quantity_schema_id_dev",
        default_var="AR_PREDICTED_QUANTITY_SCHEMA_DEV",
    )
    CUSTOMERS_SCHEMA_ID = Variable.get(
        "ar_platform_pos_customers_schema_id_dev",
        default_var="AR_POS_CUSTOMERS_SCHEMA_DEV",
    )
    A2I_SCHEMA_ID = Variable.get(
        "ar_articles_to_ingredients_schema_id_dev",
        default_var="AR_A2I_SCHEMA_DEV",
    )
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
    SOURCE_DATASET = "foodgraph_preprocessed"
    PREDICTED_SCHEMA_ID = Variable.get(
        "ar_predicted_quantity_schema_id",
        default_var="AR_PREDICTED_QUANTITY_SCHEMA_PROD",
    )
    CUSTOMERS_SCHEMA_ID = Variable.get(
        "ar_platform_pos_customers_schema_id",
        default_var="AR_POS_CUSTOMERS_SCHEMA_PROD",
    )
    A2I_SCHEMA_ID = Variable.get(
        "ar_articles_to_ingredients_schema_id",
        default_var="AR_A2I_SCHEMA_PROD",
    )
    INGEST_BASE = Variable.get(
        "event_api_ingest_base",
        default_var="https://api.example.com/event-ingest/bulk",
    )

PREDICTED_SCHEMA = """{
  "namespace": "platform",
  "type": "record",
  "name": "ar_predicted_total_quantity_ingredients",
  "doc": "Advanced Replenishment predicted ingredient quantity. Daily full load.",
  "gdpr_info": {"table_PII": "no", "column_PII": []},
  "fields": [
    {"name": "period_start_date", "type": ["null", "string"], "default": null},
    {"name": "period_length", "type": ["null", "string"], "default": null},
    {"name": "establishment_id", "type": ["null", "string"], "default": null},
    {"name": "wholesale_id", "type": ["null", "long"], "default": null},
    {"name": "ingredient_name_local", "type": ["null", "string"], "default": null},
    {"name": "matched_ingredient", "type": ["null", "string"], "default": null},
    {"name": "ingredient_form", "type": ["null", "string"], "default": null},
    {"name": "unit", "type": ["null", "string"], "default": null},
    {"name": "total_ingredient_quantity", "type": ["null", "double"], "default": null},
    {"name": "total_ingredient_quantity_kg", "type": ["null", "double"], "default": null},
    {"name": "run_date", "type": ["null", "string"], "default": null},
    {"name": "run_timestamp", "type": ["null", "string"], "default": null},
    {"name": "time_grain", "type": ["null", "string"], "default": null},
    {"name": "country_code", "type": ["null", "string"], "default": null}
  ]
}"""

CUSTOMERS_SCHEMA = """{
  "namespace": "platform",
  "type": "record",
  "name": "ar_platform_pos_customers",
  "doc": "Advanced Replenishment platform POS customers. Monthly full load on the 5th.",
  "gdpr_info": {"table_PII": "no", "column_PII": []},
  "fields": [
    {"name": "establishment_id", "type": ["null", "string"], "default": null},
    {"name": "home_store_id", "type": ["null", "long"], "default": null},
    {"name": "cust_no", "type": ["null", "long"], "default": null},
    {"name": "segment_nr", "type": ["null", "long"], "default": null},
    {"name": "segment_name", "type": ["null", "string"], "default": null},
    {"name": "pos_onboarding_date", "type": ["null", "string"], "default": null},
    {"name": "pos_status", "type": ["null", "string"], "default": null},
    {"name": "nr_transaction_days", "type": ["null", "long"], "default": null},
    {"name": "last_transaction_date", "type": ["null", "string"], "default": null}
  ]
}"""

A2I_SCHEMA = """{
  "namespace": "platform",
  "type": "record",
  "name": "ar_articles_to_ingredients",
  "doc": "Advanced Replenishment articles-to-ingredients. Monthly full load on the 5th.",
  "gdpr_info": {"table_PII": "no", "column_PII": []},
  "fields": [
    {"name": "iso_code", "type": ["null", "string"], "default": null},
    {"name": "article_no", "type": ["null", "long"], "default": null},
    {"name": "ingredient_id", "type": ["null", "long"], "default": null}
  ]
}"""


def _b64(s: str) -> str:
    return base64.b64encode(s.encode()).decode()


class EventIngestAPI:
    """OAuth + chunked POST for the partner event bus.

    Tries client_credentials across configured secrets first, then
    password-grant. Throttled token responses are retried via
    acquire_token_with_retry before falling through. 401 on ingest
    refreshes the token and retries the same payload.
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

    def get_token(self, max_retries: int = 8):
        return acquire_token_with_retry(self._get_token_once, max_retries=max_retries)

    def _get_token_once(self):
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
                raise_retryable_token_error(e)

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

    def endpoint(self, url, data=None, max_retries=10) -> Any:
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

                if r.status_code in (401, 403):
                    logging.warning(
                        "Received %s from event ingest — refreshing token and retrying",
                        r.status_code,
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


def _long(d, k):
    v = d.get(k)
    return int(v) if v is not None else None


def _double(d, k):
    v = d.get(k)
    return float(v) if v is not None else None


def _str(d, k):
    v = d.get(k)
    return (v if isinstance(v, str) else str(v)) if v is not None else None


def _predicted_row_to_avro(row) -> dict:
    d = dict(row.items())
    return {
        "period_start_date": _str(d, "period_start_date"),
        "period_length": _str(d, "period_length"),
        "establishment_id": _str(d, "establishment_id"),
        "wholesale_id": _long(d, "wholesale_id"),
        "ingredient_name_local": _str(d, "ingredient_name_local"),
        "matched_ingredient": _str(d, "matched_ingredient"),
        "ingredient_form": _str(d, "ingredient_form"),
        "unit": _str(d, "unit"),
        "total_ingredient_quantity": _double(d, "total_ingredient_quantity"),
        "total_ingredient_quantity_kg": _double(d, "total_ingredient_quantity_kg"),
        "run_date": _str(d, "run_date"),
        "run_timestamp": _str(d, "run_timestamp"),
        "time_grain": _str(d, "time_grain"),
        "country_code": _str(d, "country_code"),
    }


def _customers_row_to_avro(row) -> dict:
    d = dict(row.items())
    return {
        "establishment_id": _str(d, "establishment_id"),
        "home_store_id": _long(d, "home_store_id"),
        "cust_no": _long(d, "cust_no"),
        "segment_nr": _long(d, "segment_nr"),
        "segment_name": _str(d, "segment_name"),
        "pos_onboarding_date": _str(d, "pos_onboarding_date"),
        "pos_status": _str(d, "pos_status"),
        "nr_transaction_days": _long(d, "nr_transaction_days"),
        "last_transaction_date": _str(d, "last_transaction_date"),
    }


def _a2i_row_to_avro(row) -> dict:
    d = dict(row.items())
    return {
        "iso_code": _str(d, "iso_code"),
        "article_no": _long(d, "article_no"),
        "ingredient_id": _long(d, "ingredient_id"),
    }


def _predicted_query(batch_number: int, total_batches: int) -> str:
    """One hash slice of the daily predicted-quantity table.

    Composite fingerprint (establishment + ingredient + period) keeps
    rows for the same establishment/ingredient/period together while
    still spreading load across TOTAL_BATCHES workers.
    """
    table = (
        f"`{ML_PROJECT}.{SOURCE_DATASET}"
        f".advanced_replenishment_predicted_total_quantity_DE`"
    )
    return f"""
    SELECT
        CAST(period_start_date AS STRING) AS period_start_date,
        CAST(period_length AS STRING) AS period_length,
        CAST(establishment_id AS STRING) AS establishment_id,
        CAST(wholesale_id AS INT64) AS wholesale_id,
        CAST(ingredient_name_local AS STRING) AS ingredient_name_local,
        CAST(matched_ingredient AS STRING) AS matched_ingredient,
        CAST(ingredient_form AS STRING) AS ingredient_form,
        CAST(unit AS STRING) AS unit,
        CAST(total_ingredient_quantity AS FLOAT64) AS total_ingredient_quantity,
        CAST(total_ingredient_quantity_kg AS FLOAT64) AS total_ingredient_quantity_kg,
        CAST(run_date AS STRING) AS run_date,
        CAST(run_timestamp AS STRING) AS run_timestamp,
        CAST(time_grain AS STRING) AS time_grain,
        CAST(country_code AS STRING) AS country_code
    FROM {table}
    WHERE MOD(
        ABS(FARM_FINGERPRINT(CONCAT(
            CAST(establishment_id AS STRING), '-',
            CAST(ingredient_name_local AS STRING), '-',
            CAST(period_start_date AS STRING)
        ))),
        {total_batches}
    ) = {batch_number}
    """


def _customers_query() -> str:
    """POS asset + txn activity joined to wholesale match and behavioural segments.

    Segmentation stays on the discovery project until the prod
    segmentation dataset is cut over — keep that join explicit so a
    future migrate is a table rename, not a silent empty LEFT JOIN.
    """
    return f"""
    WITH pos_status AS (
        SELECT
            establishment_id,
            ARRAY_AGG(asset_status ORDER BY asset_created_date DESC LIMIT 1)[OFFSET(0)]
                AS pos_status,
            MIN(onboarding_date) AS pos_onboarding_date
        FROM `{BIGQUERY_PROJECT}.product_spot.erp_asset`
        WHERE (
            product_code IN (
                'POS_L_Licence',
                'POS_L_Licence_Flex',
                'POS_L_DPL_POS_License'
            )
            OR product_code LIKE '%POS_M2_BASIC_%'
        )
        AND asset_status IN ('Active', 'Disabled', 'Suspended')
        AND country_code = 'DE'
        GROUP BY 1
    ),
    pos_transaction_data AS (
        SELECT
            establishment_id,
            COUNT(DISTINCT work_day) AS nr_transaction_days,
            MAX(work_day) AS last_transaction_date
        FROM `{BIGQUERY_PROJECT}.product_spot.pos_sold_item_transactions`
        WHERE country_code = 'DE'
        GROUP BY 1
    ),
    pos_data AS (
        SELECT *
        FROM pos_transaction_data
        INNER JOIN pos_status
        USING (establishment_id)
    ),
    segmentation_data AS (
        SELECT
            wholesale_id,
            home_store_id,
            customer_number AS cust_no,
            cluster AS segment_nr,
            segment_name
        FROM `{ML_PROJECT}.discovery.behavioural_segmentation_result`
        WHERE country_code = 'DE'
    ),
    pos_wholesale_mapping AS (
        SELECT
            establishment_id,
            CAST(home_store_id AS INT64) AS home_store_id,
            CAST(cust_no AS INT64) AS cust_no
        FROM `{BIGQUERY_PROJECT}.refined.match_result_erp_wholesale`
        WHERE country_code = 'DE'
    )
    SELECT DISTINCT
        CAST(establishment_id AS STRING) AS establishment_id,
        CAST(home_store_id AS INT64) AS home_store_id,
        CAST(cust_no AS INT64) AS cust_no,
        CAST(segment_nr AS INT64) AS segment_nr,
        CAST(segment_name AS STRING) AS segment_name,
        CAST(pos_onboarding_date AS STRING) AS pos_onboarding_date,
        CAST(pos_status AS STRING) AS pos_status,
        CAST(nr_transaction_days AS INT64) AS nr_transaction_days,
        CAST(last_transaction_date AS STRING) AS last_transaction_date
    FROM pos_data
    LEFT JOIN pos_wholesale_mapping
    USING (establishment_id)
    LEFT JOIN segmentation_data
    USING (home_store_id, cust_no)
    """


def _a2i_query() -> str:
    table = f"`{ML_PROJECT}.{SOURCE_DATASET}.articles_to_ingredients`"
    return f"""
    SELECT DISTINCT
        CAST(iso_code AS STRING) AS iso_code,
        CAST(article_no AS INT64) AS article_no,
        CAST(ingredient_id AS INT64) AS ingredient_id
    FROM {table}
    WHERE iso_code = 'DE'
    """


def _ingestbulk_url(country: str, schema_id: str) -> str:
    base = INGEST_BASE.rstrip("/")
    return f"{base}/{country.lower()}/{schema_id}"


def _send_dataset(query, schema_json, schema_id, row_to_avro, label, country=COUNTRY):
    """Stream one extract from BigQuery to partner ingestbulk."""
    stagger_parallel_token_request(slot_key=label)
    api = EventIngestAPI(OAUTH2_URL, CLIENT_ID, CLIENT_SECRET)
    try:
        api.get_token()
        logging.info(
            "Token acquired using credential: %s",
            getattr(api, "successful_credential", None),
        )
    except Exception as e:
        logging.error("Failed to acquire token during startup: %s", e)
        raise

    print(f"{datetime.now()} - [{country}] {label} querying BQ...")
    client = bigquery.Client(project=BIGQUERY_PROJECT)
    results = client.query(query).result()
    total_rows = results.total_rows
    logging.info(
        "%s - [%s] %s query rows: %s", datetime.now(), country, label, total_rows
    )

    base_url = _ingestbulk_url(country, schema_id)
    logging.info("%s - [%s] %s ingest URL: %s", datetime.now(), country, label, base_url)

    schema_parsed = avro.schema.parse(schema_json)
    writer = avro.io.DatumWriter(schema_parsed)

    chunk_buf: List[str] = []
    row_count = 0

    def _send_chunk(chunk):
        payload = json.dumps({"records": [{"value": rec} for rec in chunk]})
        try:
            resp = api.endpoint(url=base_url, data=payload)
            logging.info("Event ingest response: %s", resp)
            return resp
        except Exception:
            logging.exception(
                "Failed to send chunk for country %s label %s. Payload size=%d",
                country,
                label,
                len(chunk),
            )
            raise

    for row in results:
        bw = io.BytesIO()
        encoder = avro.io.BinaryEncoder(bw)
        writer.write(row_to_avro(row), encoder)
        chunk_buf.append(base64.b64encode(bw.getvalue()).decode("utf-8"))
        row_count += 1

        if len(chunk_buf) >= CHUNK_SIZE:
            resp = _send_chunk(chunk_buf)
            print(f"{datetime.now()} - [{country}] {label} rows {row_count} — {resp}")
            chunk_buf = []

    if chunk_buf:
        resp = _send_chunk(chunk_buf)
        print(f"{datetime.now()} - [{country}] {label} rows {row_count} — {resp}")

    print(f"{datetime.now()} - [{country}] {label} DONE. Total rows: {row_count}")


def send_predicted_total_quantity(
    batch_number: int, total_batches: int, country=COUNTRY
):
    """One hash partition of the daily predicted-quantity load."""
    _send_dataset(
        query=_predicted_query(batch_number, total_batches),
        schema_json=PREDICTED_SCHEMA,
        schema_id=PREDICTED_SCHEMA_ID,
        row_to_avro=_predicted_row_to_avro,
        label=f"ar_predicted_total_quantity_ingredients batch {batch_number}/{total_batches}",
        country=country,
    )


def send_platform_pos_customers(country=COUNTRY):
    """Monthly full load of platform POS customers for Advanced Replenishment."""
    _send_dataset(
        query=_customers_query(),
        schema_json=CUSTOMERS_SCHEMA,
        schema_id=CUSTOMERS_SCHEMA_ID,
        row_to_avro=_customers_row_to_avro,
        label="ar_platform_pos_customers",
        country=country,
    )


def send_articles_to_ingredients(country=COUNTRY):
    """Monthly full load of articles-to-ingredients for the market."""
    _send_dataset(
        query=_a2i_query(),
        schema_json=A2I_SCHEMA,
        schema_id=A2I_SCHEMA_ID,
        row_to_avro=_a2i_row_to_avro,
        label="ar_articles_to_ingredients",
        country=country,
    )


if __name__ == "__main__":
    # Smoke: Avro schemas parse and SQL builders return non-empty text.
    for name, schema in (
        ("predicted", PREDICTED_SCHEMA),
        ("customers", CUSTOMERS_SCHEMA),
        ("a2i", A2I_SCHEMA),
    ):
        avro.schema.parse(schema)
        print(f"ok schema {name}")
    q = _predicted_query(0, 10)
    assert "FARM_FINGERPRINT" in q
    assert "= 0" in q
    print("ok predicted SQL prefix:", q.strip().splitlines()[0])
    print(
        "ok customers SQL has segmentation join:",
        "segmentation_data" in _customers_query(),
    )
    print("ok a2i SQL:", "articles_to_ingredients" in _a2i_query())
