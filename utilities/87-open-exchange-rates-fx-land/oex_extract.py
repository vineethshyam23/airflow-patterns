"""Open Exchange Rates extract + trusted insert SQL.

Fetches /api/latest.json (USD base), reshapes the rates map into a
nested JSONL row {timestamp, rates[{currency, rate}]}, and writes it
to Composer data/ for the GCS→rawzone handoff.

Also owns the UNNEST + EUR cross-rate INSERT used by the DAG.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import requests
from airflow.hooks.base import BaseHook

log = logging.getLogger(__name__)

OEX_LATEST_URL = "https://openexchangerates.org/api/latest.json"
DEFAULT_CONN_ID = "api_openexchangerates"
DEFAULT_TMP_DIR = "/home/airflow/gcs/data/oex"
DEFAULT_FILENAME = "oex.json"


def fetch_latest_rates(app_id: str, timeout_s: int = 60) -> dict[str, Any]:
    """Call OEX latest.json and return the parsed body.

    Raises on non-200. Production wrote nothing and let the task go
    green — that left staging empty and trusted unchanged with no
    alert. Fail loud here.
    """
    response = requests.get(
        OEX_LATEST_URL,
        headers={"Content-Type": "application/json"},
        params={"app_id": app_id},
        timeout=timeout_s,
    )
    if response.status_code != 200:
        raise RuntimeError(
            f"OEX latest.json returned HTTP {response.status_code}: "
            f"{response.text[:200]}"
        )
    return response.json()


def reshape_to_nested_row(payload: dict[str, Any]) -> dict[str, Any]:
    """Flatten rates map → repeated record list for BQ nested load."""
    rates = payload.get("rates") or {}
    return {
        "timestamp": payload["timestamp"],
        "rates": [
            {"currency": currency, "rate": rate}
            for currency, rate in rates.items()
        ],
    }


def write_nested_jsonl(row: dict[str, Any], path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        handle.write(json.dumps(row) + "\n")
    return path


def extract_oex(
    conn_id: str = DEFAULT_CONN_ID,
    tmp_dir: str = DEFAULT_TMP_DIR,
    filename: str = DEFAULT_FILENAME,
) -> str:
    """PythonOperator entrypoint: fetch → reshape → Composer data/ write."""
    app_id = BaseHook.get_connection(conn_id).password
    if not app_id:
        raise ValueError(f"Airflow connection {conn_id!r} has empty password (app_id)")

    payload = fetch_latest_rates(app_id)
    row = reshape_to_nested_row(payload)
    out_path = write_nested_jsonl(row, Path(tmp_dir) / filename)
    log.info(
        "Wrote OEX snapshot ts=%s currencies=%s → %s",
        row["timestamp"],
        len(row["rates"]),
        out_path,
    )
    return str(out_path)


def build_fx_insert_sql(
    project_id: str,
    staging_dataset: str,
    trusted_dataset: str,
    staging_table: str = "oex_data",
    trusted_table: str = "exchange_rates",
) -> str:
    """UNNEST staging rates, derive EUR/USD factors, skip known timestamps."""
    staging = f"`{project_id}.{staging_dataset}.{staging_table}`"
    trusted = f"`{project_id}.{trusted_dataset}.{trusted_table}`"
    return f"""
WITH rates_unnested AS (
  SELECT
    rates.currency AS currency,
    rates.rate AS rate,
    TIMESTAMP_SECONDS(timestamp) AS timestamp
  FROM {staging},
  UNNEST(rates) AS rates
),
eur_base AS (
  SELECT currency, rate, timestamp
  FROM rates_unnested
  WHERE currency = 'EUR'
)
SELECT
  r.timestamp,
  r.currency,
  r.rate / e.rate AS to_eur,
  r.rate AS to_usd,
  1 / (r.rate / e.rate) AS eur_to_currency,
  1 / r.rate AS usd_to_currency
FROM rates_unnested AS r
CROSS JOIN eur_base AS e
WHERE r.timestamp = e.timestamp
  AND r.timestamp NOT IN (
    SELECT DISTINCT timestamp FROM {trusted}
  )
""".strip()
