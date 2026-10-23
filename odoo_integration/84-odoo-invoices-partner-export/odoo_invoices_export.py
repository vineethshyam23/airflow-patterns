"""
Export Odoo invoice lines from a refined BigQuery table to a partner
event bus (Avro + ingestbulk).

Source: refined.odoo_invoices (filtered by country_code).
Markets: FR, ES (lowercase ISO in the ingest path).

Daily full load — each country slice is re-shipped, deterministically
sharded across TOTAL_BATCHES via FARM_FINGERPRINT on
(odoo_id, odoo_invoice_id, product_code).

Distinct from pattern 08 (WSL wholesale trusted intermediate +
recommender APPEND for a pilot DE market). This path ships the
general invoice-line fact for partner analytics, with no ML history
sink and no wholesale-only filter.

Source (read-only):
  dags/horeca_digital/dana_odoo_invoices_export.py
  dags/horeca_digital/dana_auth.py (token helpers → event_api_auth)
"""

from __future__ import annotations

import base64
import io
import json
import logging
import os
import time
from datetime import date, datetime, timedelta

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

if ENV == "DEV":
    OAUTH_USERNAME = Variable.get("event_api_username")
    OAUTH_PASSWORD = Variable.get("event_api_password")
    CLIENT_ID = Variable.get("event_api_client_id_dev")
    CLIENT_SECRET = Variable.get("event_api_client_secret_dev")
    OAUTH2_URL = Variable.get("event_api_oauth2_url_dev")
    BIGQUERY_PROJECT = "dwh_project_dev"
    SCHEMA_ID = Variable.get(
        "odoo_invoices_event_schema_id_dev", default_var="SCHEMA_ID_DEV"
    )
    SCHEMA_NAME = "odoo_invoices_dev"
    INGEST_BASE = Variable.get(
        "event_api_ingest_base_dev",
        default_var="https://api.example.com/event-ingest/bulk",
    )
else:
    OAUTH_USERNAME = Variable.get("event_api_username")
    OAUTH_PASSWORD = Variable.get("event_api_password")
    CLIENT_ID = Variable.get("event_api_client_id")
    # Prefer a single production secret Variable; accept a (name, value)
    # tuple so logs can name which credential succeeded without printing it.
    prod_secrets = []
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
        "odoo_invoices_event_schema_id", default_var="SCHEMA_ID_PROD"
    )
    SCHEMA_NAME = "odoo_invoices"
    # Prod host may omit a path prefix that sandbox keeps — externalize
    # the whole base so env differences stay out of code.
    INGEST_BASE = Variable.get(
        "event_api_ingest_base",
        default_var="https://api.example.com/event-ingest/bulk",
    )

# Active markets for this export (lowercase ISO for ingest path).
COUNTRY_ISO_CODES = ["fr", "es"]

_LONG_FIELDS = ["odoo_id", "odoo_invoice_id", "company_partner_id"]
_DOUBLE_FIELDS = ["net_price_eur", "gross_price_local"]
_BOOLEAN_FIELDS = ["product_is_setup", "_valid_flag"]
_DATE_FIELDS = ["booking_date", "invoice_date_due", "paid_on"]
_TIMESTAMP_FIELDS = ["_update_ts", "_valid_from", "_valid_to"]

FIELD_ORDER = [
    "odoo_id",
    "odoo_invoice_id",
    "establishment_id",
    "company_partner_id",
    "postalcode",
    "country_code",
    "wholesale_id",
    "invoice_partner_display_name",
    "commercial_partner_name",
    "vat",
    "booking_date",
    "invoice_date_due",
    "parent_bill",
    "move_type",
    "payment_status",
    "paid_on",
    "parent_state",
    "product_code",
    "product_is_setup",
    "price_adjustment",
    "discount_code",
    "net_price_eur",
    "gross_price_local",
    "_update_ts",
    "_valid_from",
    "_valid_to",
    "_valid_flag",
]

CHUNK_SIZE = 1000

_AVRO_EPOCH = date(1970, 1, 1)

# Inline Avro schema — same field set as the registered partner subject.
# Do not load from schemas/*.avsc on Composer; keep the contract next to
# the encoder so a missing mount cannot ship a mismatched payload.
_ODOO_INVOICES_SCHEMA_TEMPLATE = """{
  "namespace": "company",
  "type": "record",
  "name": "__SCHEMA_NAME__",
  "doc": "Odoo invoice line facts exported to partner event ingest (per country)",
  "fields": [
    {"name": "odoo_id",                      "doc": "Odoo record identifier",                               "type": ["null", "long"],    "default": null},
    {"name": "odoo_invoice_id",               "doc": "Odoo invoice identifier",                              "type": ["null", "long"],    "default": null},
    {"name": "establishment_id",              "doc": "Establishment identifier",                             "type": ["null", "string"],  "default": null},
    {"name": "company_partner_id",            "doc": "Company partner identifier in Odoo",                   "type": ["null", "long"],    "default": null},
    {"name": "postalcode",                     "doc": "Postal/ZIP code",                                      "type": ["null", "string"],  "default": null},
    {"name": "country_code",                  "doc": "Two-letter ISO country code",                          "type": ["null", "string"],  "default": null},
    {"name": "wholesale_id",                  "doc": "Wholesale customer identifier",                       "type": ["null", "string"],  "default": null},
    {"name": "invoice_partner_display_name",  "doc": "Display name of the invoice partner",                  "type": ["null", "string"],  "default": null},
    {"name": "commercial_partner_name",       "doc": "Commercial partner name",                              "type": ["null", "string"],  "default": null},
    {"name": "vat",                           "doc": "VAT number of the partner",                            "type": ["null", "string"],  "default": null},
    {"name": "booking_date",                  "doc": "Invoice booking date",                                 "type": ["null", {"type": "int", "logicalType": "date"}], "default": null},
    {"name": "invoice_date_due",              "doc": "Invoice due date",                                     "type": ["null", {"type": "int", "logicalType": "date"}], "default": null},
    {"name": "parent_bill",                   "doc": "Parent bill reference",                                "type": ["null", "string"],  "default": null},
    {"name": "move_type",                     "doc": "Odoo move/invoice type",                               "type": ["null", "string"],  "default": null},
    {"name": "payment_status",                "doc": "Payment status of the invoice",                        "type": ["null", "string"],  "default": null},
    {"name": "paid_on",                       "doc": "Date the invoice was paid",                            "type": ["null", {"type": "int", "logicalType": "date"}], "default": null},
    {"name": "parent_state",                  "doc": "Parent invoice state",                                 "type": ["null", "string"],  "default": null},
    {"name": "product_code",                  "doc": "Product code on the invoice line",                     "type": ["null", "string"],  "default": null},
    {"name": "product_is_setup",              "doc": "Whether the product is a setup product",               "type": ["null", "boolean"], "default": null},
    {"name": "price_adjustment",              "doc": "Price adjustment details",                             "type": ["null", "string"],  "default": null},
    {"name": "discount_code",                 "doc": "Discount code applied",                                "type": ["null", "string"],  "default": null},
    {"name": "net_price_eur",                 "doc": "Net price in EUR",                                     "type": ["null", "double"],  "default": null},
    {"name": "gross_price_local",             "doc": "Gross price in local currency",                        "type": ["null", "double"],  "default": null},
    {"name": "_update_ts",                    "doc": "Timestamp of last update in the refined table",        "type": ["null", "string"],  "default": null},
    {"name": "_valid_from",                   "doc": "Record validity start timestamp",                      "type": ["null", "string"],  "default": null},
    {"name": "_valid_to",                     "doc": "Record validity end timestamp",                        "type": ["null", "string"],  "default": null},
    {"name": "_valid_flag",                   "doc": "Whether the record is currently valid",                "type": ["null", "boolean"], "default": null}
  ]
}"""

ODOO_INVOICES_SCHEMA = _ODOO_INVOICES_SCHEMA_TEMPLATE.replace(
    "__SCHEMA_NAME__", SCHEMA_NAME
)


def _base64_encode_string(s: str) -> str:
    return base64.b64encode(s.encode()).decode()


class EventAPI:
    """OAuth client for partner event ingest.

    Prefer client_credentials; fall back to password grant only after
    non-throttle failures. Throttled IdP responses are retried via
    acquire_token_with_retry before falling through.
    """

    def __init__(self, token_url, client_id, client_secret):
        self.token_url = token_url
        self.token = None
        self.client_id = client_id
        self.client_secrets = []
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

    def get_token(self, max_retries=8):
        return acquire_token_with_retry(self._get_token_once, max_retries=max_retries)

    def _get_token_once(self):
        realm = None
        for _var in ("event_api_realm", "event_api_realm_id"):
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
                "Authorization": "Basic "
                + _base64_encode_string(self.client_id + ":" + candidate),
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
                    "client_credentials attempt failed for candidate %s: %s",
                    name or "<unnamed>",
                    e,
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
                    + _base64_encode_string(self.client_id + ":" + candidate),
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
                        "password grant failed for candidate %s: %s",
                        name or "<unnamed>",
                        e,
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
                    logging.info(
                        "Acquired token with password grant (no client secret)"
                    )
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
        }
        for attempt in range(max_retries):
            try:
                r = requests.post(url, headers=headers, data=data, timeout=120)
                if r.status_code == 401:
                    self.token = None
                    self.get_token()
                    headers["Authorization"] = "Bearer " + self.token
                    continue
                r.raise_for_status()
                return r.json()
            except (
                requests.exceptions.JSONDecodeError,
                requests.exceptions.RequestException,
            ) as e:
                wait = min(5 * (attempt + 1), 60)
                logging.warning(
                    "Event API attempt %d/%d failed: %s — retrying in %ds",
                    attempt + 1,
                    max_retries,
                    e,
                    wait,
                )
                if attempt < max_retries - 1:
                    time.sleep(wait)
                else:
                    raise


def _to_avro_date(v):
    """Return a datetime.date for Avro logicalType date.

    Accepts BQ DATE, datetime, days-since-epoch int/str, or ISO date string.
    """
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    try:
        days = int(v)
        return _AVRO_EPOCH + timedelta(days=days)
    except (TypeError, ValueError):
        pass
    return date.fromisoformat(str(v)[:10])


def _row_to_avro_dict(row) -> dict:
    d = dict(row.items())
    out = {}
    for name in FIELD_ORDER:
        v = d.get(name)
        if v is None:
            out[name] = None
            continue
        if name in _LONG_FIELDS:
            out[name] = int(v)
        elif name in _DOUBLE_FIELDS:
            out[name] = float(v)
        elif name in _BOOLEAN_FIELDS:
            out[name] = bool(v)
        elif name in _DATE_FIELDS:
            out[name] = _to_avro_date(v)
        elif name in _TIMESTAMP_FIELDS:
            out[name] = v if isinstance(v, str) else str(v)
        else:
            out[name] = v if isinstance(v, str) else str(v)
    return out


def _build_query(iso_code_lower: str, batch_number: int, total_batches: int) -> str:
    """Full daily load: whole country slice, sharded by fingerprint."""
    cc = iso_code_lower.lower()
    select_cols = []
    for name in FIELD_ORDER:
        if name in _DATE_FIELDS:
            select_cols.append(f"CAST({name} AS DATE) AS {name}")
        elif name in _TIMESTAMP_FIELDS:
            select_cols.append(
                f"FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%E*S', {name}) AS {name}"
            )
        else:
            select_cols.append(name)
    select_clause = ",\n        ".join(select_cols)

    return f"""
    SELECT
        {select_clause}
    FROM `refined.odoo_invoices`
    WHERE UPPER(country_code) = '{cc.upper()}'
      AND MOD(ABS(FARM_FINGERPRINT(CONCAT(
          CAST(odoo_id AS STRING), '-',
          CAST(odoo_invoice_id AS STRING), '-',
          IFNULL(product_code, '')
      ))), {total_batches}) = {batch_number}
    """


def send_ranged_batch(
    iso_code_lower: str, batch_number: int, total_batches: int
) -> None:
    """Export one hash partition for a country to partner ingestbulk."""
    stagger_parallel_token_request(
        slot_key=f"{iso_code_lower}-batch{batch_number}"
    )
    api = EventAPI(OAUTH2_URL, CLIENT_ID, CLIENT_SECRET)
    try:
        api.get_token()
        logging.info(
            "Event API token acquired using credential: %s",
            getattr(api, "successful_credential", None),
        )
    except Exception as e:
        logging.error("Failed to acquire event API token during startup: %s", e)
        raise

    query = _build_query(iso_code_lower, batch_number, total_batches)
    print(
        f"{datetime.now()} - [{iso_code_lower}] batch "
        f"{batch_number}/{total_batches} querying BQ..."
    )
    client = bigquery.Client(project=BIGQUERY_PROJECT)
    results = client.query(query).result()

    base_url = f"{INGEST_BASE.rstrip('/')}/{iso_code_lower.lower()}/{SCHEMA_ID}"
    schema_parsed = avro.schema.parse(ODOO_INVOICES_SCHEMA)
    writer = avro.io.DatumWriter(schema_parsed)

    chunk_buf = []
    row_count = 0

    for row in results:
        bw = io.BytesIO()
        encoder = avro.io.BinaryEncoder(bw)
        writer.write(_row_to_avro_dict(row), encoder)
        chunk_buf.append(base64.b64encode(bw.getvalue()).decode("utf-8"))
        row_count += 1

        if len(chunk_buf) >= CHUNK_SIZE:
            payload = json.dumps({"records": [{"value": rec} for rec in chunk_buf]})
            resp = api.endpoint(url=base_url, data=payload)
            print(
                f"{datetime.now()} - [{iso_code_lower}] batch "
                f"{batch_number} rows {row_count} — {resp}"
            )
            chunk_buf = []

    if chunk_buf:
        payload = json.dumps({"records": [{"value": rec} for rec in chunk_buf]})
        resp = api.endpoint(url=base_url, data=payload)
        print(
            f"{datetime.now()} - [{iso_code_lower}] batch "
            f"{batch_number} rows {row_count} — {resp}"
        )

    print(
        f"{datetime.now()} - [{iso_code_lower}] batch "
        f"{batch_number}/{total_batches} DONE. Total rows: {row_count}"
    )


if __name__ == "__main__":
    # Smoke: schema parse + one batch SQL string.
    avro.schema.parse(ODOO_INVOICES_SCHEMA)
    print(_build_query("fr", 0, 5))
