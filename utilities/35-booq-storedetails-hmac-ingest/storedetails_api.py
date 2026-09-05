"""POS vendor store-details API: HMAC-MD5 auth, CSV repair, GCS normalize.

Fetches a semicolon-delimited establishment snapshot from a vendor
webservice, validates the header against a hard-coded contract, repairs
rows where commas inside the address field blow the column count, and
writes a comma-delimited CSV for BigQuery.

Also exposes a GCS repair pass used as a second DAG step so a load never
sees "Too many values" after the Composer → rawzone copy.

Source (read-only):
  dags/horeca_digital/booq_storedetails.py
"""

from __future__ import annotations

import csv
import hashlib
import hmac
import io
import logging
import os
from datetime import date, datetime

import requests

logger = logging.getLogger(__name__)

# Parse-time date stamp (production behaviour). Prefer logical date /
# {{ ds }} if you rewrite for backfill-safe paths.
TODAY = date.today().strftime("%Y-%m-%d")

EXPECTED_COLUMNS = 27
# Column indices: 0=customer_id … 5=address (merge target on overflow)
ADDRESS_COLUMN_INDEX = 5

# Vendor header contract. Update in lockstep with the BigQuery schema
# JSON when the API adds / removes / reorders columns.
EXPECTED_HEADER = (
    "klantenid",
    "Login",
    "Debnr",
    "Vestcode",
    "bedrijf",
    "adres",
    "stad",
    "klantGroep",
    "start_date_FO",
    "change_date_FO",
    "start_date_BKM",
    "change_datum_BKM",
    "FO_BO",
    "POS",
    "EFT",
    "IDPMS",
    "personeelsplanner",
    "Webshop",
    "QR",
    "BKM_terminal",
    "BKM_Netwerk",
    "XAFAX",
    "Kasstaat",
    "SmartTap",
    "Script",
    "Mews",
    "BlackBox",
)

DEFAULT_LOCAL_DIR = "/home/airflow/gcs/data/booq_storedetails"
DEFAULT_ENDPOINT = "https://vendor.example.com/webservice/getStoreDetails.aspx"


def create_hex_string_hmac(key: str, data: str) -> str:
    """Return hex digest of HMAC-MD5(key, data)."""
    return hmac.new(
        key.encode("utf-8"),
        data.encode("utf-8"),
        hashlib.md5,
    ).hexdigest()


def _repair_row_columns(row: list[str], row_index: int) -> list[str]:
    """Force row to EXPECTED_COLUMNS for a stable BigQuery load.

    Too many columns: merge extras back into the address field (index 5).
    Typical cause is a comma inside the address that the vendor CSV did
    not quote, so our comma-delimited writer (or a re-parse) splits it.

    Too few columns: pad empty strings at the **end** only. That assumes
    missing fields are trailing. If a middle column were omitted, padding
    at the end would misalign — we log so ops can investigate.
    """
    n = len(row)
    if n == EXPECTED_COLUMNS:
        return row
    if n < EXPECTED_COLUMNS:
        padded = row + [""] * (EXPECTED_COLUMNS - n)
        row_id = row[0] if row else "?"
        logger.warning(
            "Padded row index=%s id=%s: %s -> %s "
            "(assumes trailing columns missing)",
            row_index,
            row_id,
            n,
            EXPECTED_COLUMNS,
        )
        return padded

    extra = n - EXPECTED_COLUMNS
    adres_start = ADDRESS_COLUMN_INDEX
    adres_end = adres_start + extra + 1
    merged = ", ".join(row[adres_start:adres_end])
    repaired = row[:adres_start] + [merged] + row[adres_end:]
    row_id = row[0] if row else "?"
    logger.info(
        "Repaired row index=%s id=%s: %s -> %s columns",
        row_index,
        row_id,
        n,
        EXPECTED_COLUMNS,
    )
    return repaired


def _validate_header(header_row: list[str]) -> None:
    """Fail fast on schema drift before any file is written."""
    actual = tuple((c or "").strip() for c in header_row)
    if actual == EXPECTED_HEADER:
        return
    n = len(actual)
    if n != EXPECTED_COLUMNS:
        raise ValueError(
            f"Schema change detected: header has {n} columns, "
            f"expected {EXPECTED_COLUMNS}. Update EXPECTED_HEADER, "
            f"EXPECTED_COLUMNS, ADDRESS_COLUMN_INDEX, and BigQuery schema."
        )
    raise ValueError(
        f"Schema change detected: header differs from expected. "
        f"Actual: {actual!r}. Expected: {EXPECTED_HEADER!r}. "
        f"Update EXPECTED_HEADER and BigQuery schema if the API changed."
    )


def fetch_csv_response_from_post_request(
    endpoint_url: str,
    form_data: dict,
    local_dir: str = DEFAULT_LOCAL_DIR,
    as_of: str | None = None,
) -> str:
    """POST for semicolon CSV, validate header, write comma CSV locally.

    Returns the local path written. Raises before writing on non-200 or
    header mismatch so upload / load never see a partial file.
    """
    response = requests.post(endpoint_url, data=form_data, timeout=120)
    if response.status_code != 200:
        raise ValueError(
            f"Error fetching CSV response: {response.status_code}"
        )

    content = response.content.decode("utf-8")
    lines = content.splitlines()
    if not lines:
        raise ValueError("API returned empty CSV")

    header_row = next(csv.reader(io.StringIO(lines[0]), delimiter=";"))
    _validate_header(header_row)

    stamp = as_of or TODAY
    os.makedirs(local_dir, exist_ok=True)
    out_path = os.path.join(local_dir, f"{stamp}.csv")

    reader = csv.reader(io.StringIO(content), delimiter=";")
    rows = list(reader)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(rows[0])
        for row_index, row in enumerate(rows[1:], start=1):
            writer.writerow(_repair_row_columns(row, row_index))

    logger.info("Wrote %s rows to %s", len(rows) - 1, out_path)
    return out_path


def repair_csv_in_gcs(bucket_name: str, object_key: str) -> None:
    """Download GCS CSV, normalize every row to EXPECTED_COLUMNS, upload."""
    from google.cloud import storage

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_key)
    content = blob.download_as_text(encoding="utf-8")

    reader = csv.reader(io.StringIO(content))
    rows = list(reader)
    repaired = [_repair_row_columns(row, i) for i, row in enumerate(rows)]

    out = io.StringIO()
    csv.writer(out).writerows(repaired)
    blob.upload_from_string(out.getvalue(), content_type="text/csv")
    logger.info(
        "Repaired CSV in gs://%s/%s (%s rows)",
        bucket_name,
        object_key,
        len(repaired),
    )


def main(
    key: str,
    endpoint_url: str = DEFAULT_ENDPOINT,
    local_dir: str = DEFAULT_LOCAL_DIR,
    as_of: str | None = None,
) -> str:
    """Build daily HMAC and fetch store-details CSV.

    Signature material is ``getStoreDetails`` + ``YYYYMMDD`` (server-local
    calendar day). Key rotation is daily — yesterday's HMAC will not
    authenticate today.
    """
    if not key:
        raise ValueError("HMAC key is empty; set vendor_storedetails_hmac_key")

    current_date = datetime.now()
    data = "getStoreDetails" + current_date.strftime("%Y%m%d")
    calculated_key = create_hex_string_hmac(key, data)
    form_data = {"hmac": calculated_key}

    return fetch_csv_response_from_post_request(
        endpoint_url,
        form_data,
        local_dir=local_dir,
        as_of=as_of,
    )
