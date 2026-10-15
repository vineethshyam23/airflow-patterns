"""Fetch AppFigures report CSVs onto the Composer data volume.

Maps a logical file_name (sales, ratings, ratings_product,
ratings_country, usage) to the AppFigures /v2/reports/{type}/
endpoint and group_by dimensions, then writes the CSV body to
/home/airflow/gcs/data/appfigures/ so a later GCSToGCSOperator can
promote it into the raw zone.

Production originally passed a Bearer PAT as an op_kwarg. Prefer an
Airflow Variable or Secret Manager secret — never commit the token.
"""

from __future__ import annotations

import logging
from pathlib import Path

import requests

LOG = logging.getLogger(__name__)

COMPOSER_DATA_DIR = Path("/home/airflow/gcs/data/appfigures")
API_BASE = "https://api.appfigures.com/v2/reports"

# Logical output name → (report_type path segment, group_by)
REPORT_GROUP_BY = {
    "sales": ("sales", "products,countries,dates"),
    "ratings": ("ratings", "product,date"),
    "ratings_product": ("ratings", "product"),
    "ratings_country": ("ratings", "country"),
    "usage": ("usage", "network,product,country,date"),
}


def fetch_appfigures_data(**kwargs) -> str:
    """Pull one AppFigures report and write CSV to Composer local disk.

    Expected kwargs:
      file_name: logical report key (see REPORT_GROUP_BY)
      parameters: dict with start_date, end_date, format (usually csv)
      authorization_token: Bearer token string (from Variable / secret)
      report_type: optional override; otherwise derived from file_name

    Returns the local CSV path for XCom consumers / debugging.
    """
    file_name = kwargs["file_name"]
    parameters = dict(kwargs["parameters"])
    authorization_token = kwargs["authorization_token"]

    if file_name not in REPORT_GROUP_BY:
        raise ValueError(f"Unknown AppFigures file_name: {file_name}")

    default_type, group_by = REPORT_GROUP_BY[file_name]
    report_type = kwargs.get("report_type") or default_type
    parameters["group_by"] = group_by

    url = f"{API_BASE}/{report_type}/"
    headers = {
        "accept": "application/json",
        "Authorization": authorization_token,
        "Content-Type": "application/json",
    }

    LOG.info("AppFigures GET %s params=%s", url, parameters)
    response = requests.get(url, params=parameters, headers=headers, timeout=120)

    # Production wrote the body even on non-200. Fail closed here so a
    # bad token does not land empty CSVs into trusted.
    if response.status_code != 200:
        raise RuntimeError(
            f"AppFigures {file_name} HTTP {response.status_code}: "
            f"{response.text[:300]}"
        )

    COMPOSER_DATA_DIR.mkdir(parents=True, exist_ok=True)
    out_path = COMPOSER_DATA_DIR / f"appfigures_{file_name}.csv"
    out_path.write_text(response.text, encoding="utf-8")
    LOG.info("Wrote %s (%s bytes)", out_path, out_path.stat().st_size)
    return str(out_path)
