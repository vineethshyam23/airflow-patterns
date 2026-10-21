"""
lpv_report.py
─────────────
Monthly LPV Adjustment report.

Queries discovery.lpv_adjusted for the previous calendar month,
classifies each row as Recurring or Onetime from product_is_setup,
writes one UTF-8 CSV per country_code into a zip, and returns an
email payload for the shared email_delivery module.

Environment variables (set by Airflow before generate_reports() runs):
    BQ_PROJECT, BQ_DATASET, BQ_TABLE
    EMAIL_FROM, EMAIL_FROM_NAME, EMAIL_TO
    EMAIL_CC (optional)
"""

from __future__ import annotations

import io
import logging
import os
import sys
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
from google.cloud import bigquery

try:
    from dotenv import load_dotenv

    load_dotenv(Path(__file__).parent / ".env")
except ImportError:
    pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

BUSINESS_TZ = ZoneInfo("Europe/Amsterdam")


def _env(key: str, required: bool = True, default: str = "") -> str:
    value = os.getenv(key, default)
    if required and not value:
        raise EnvironmentError(f"Required environment variable '{key}' is not set.")
    return value


def _parse_recipients(env_key: str, required: bool = True) -> list[str]:
    raw = _env(env_key, required=required)
    return [addr.strip() for addr in raw.split(",") if addr.strip()]


def _load_config() -> dict[str, Any]:
    return {
        "bq_project": _env("BQ_PROJECT"),
        "bq_dataset": _env("BQ_DATASET"),
        "bq_table": _env("BQ_TABLE"),
        "email_from": _env("EMAIL_FROM"),
        "email_from_name": _env("EMAIL_FROM_NAME"),
        "email_to": _parse_recipients("EMAIL_TO"),
        "email_cc": _parse_recipients("EMAIL_CC", required=False),
    }


def _previous_month_label() -> str:
    today = datetime.now(BUSINESS_TZ).date().replace(day=1)
    last_month = today - timedelta(days=1)
    return last_month.strftime("%B %Y")


def _fmt_bytes(n: int) -> str:
    size = float(n)
    for unit in ("B", "KB", "MB", "GB"):
        if size < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} TB"


def run_query(cfg: dict[str, Any]) -> tuple[pd.DataFrame, str]:
    """Query LPV adjustments for the previous calendar month."""
    table_ref = f"`{cfg['bq_project']}.{cfg['bq_dataset']}.{cfg['bq_table']}`"
    sql = f"""
        SELECT
        cal_month,
        country_code,
        CASE
            WHEN product_is_setup IS FALSE THEN 'Recurring'
            WHEN product_is_setup IS TRUE THEN 'Onetime'
        END AS revenue_model,
        * EXCEPT (cal_month, country_code, product_is_setup)
        FROM {table_ref}
        WHERE cal_month = DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 MONTH)
    """

    log.info("Connecting to BigQuery project '%s'...", cfg["bq_project"])
    client = bigquery.Client(project=cfg["bq_project"])
    log.info("Running query against %s...", table_ref)
    df = client.query(sql, project=cfg["bq_project"]).to_dataframe()

    for col in df.select_dtypes(include=["datetimetz"]).columns:
        df[col] = df[col].dt.tz_localize(None)

    df = df.where(df.notna(), other=None)

    if "cal_month" in df.columns and not df.empty:
        period_dt = pd.to_datetime(df["cal_month"].iloc[0])
        period_label = period_dt.strftime("%B %Y")
    else:
        period_label = _previous_month_label()

    log.info("Query returned %d rows for period: %s", len(df), period_label)
    return df, period_label


def build_country_zip(df: pd.DataFrame, period_safe: str) -> tuple[bytes, int, int]:
    """Write one UTF-8 CSV per country_code into a single zip archive."""
    countries = sorted(df["country_code"].dropna().unique())
    log.info(
        "Building CSVs for %d country/countries: %s",
        len(countries),
        ", ".join(str(c) for c in countries),
    )

    zip_buffer = io.BytesIO()
    total_uncompressed = 0

    with zipfile.ZipFile(
        zip_buffer, mode="w", compression=zipfile.ZIP_DEFLATED, compresslevel=9
    ) as archive:
        for country in countries:
            country_df = df[df["country_code"] == country].reset_index(drop=True)
            country_safe = str(country).replace("/", "-").replace(" ", "_").replace("\\", "-")
            filename = f"LPV_Adjustment_{country_safe}_{period_safe}.csv"
            csv_bytes = country_df.to_csv(index=False).encode("utf-8")
            total_uncompressed += len(csv_bytes)
            archive.writestr(filename, csv_bytes)
            log.info(
                "  %-40s  %6d rows  %s",
                filename,
                len(country_df),
                _fmt_bytes(len(csv_bytes)),
            )

    return zip_buffer.getvalue(), total_uncompressed, len(countries)


def _build_html(
    period_label: str,
    row_count: int,
    filename: str,
    cfg: dict[str, Any],
    country_count: int,
) -> str:
    generated_at = datetime.now(BUSINESS_TZ).strftime("%d %b %Y, %H:%M CET")
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>LPV Adjustment Report</title>
  <style>
    body        {{ font-family: Calibri, Arial, sans-serif; background:#f4f6f9; margin:0; padding:0; }}
    .wrapper    {{ max-width:640px; margin:32px auto; background:#ffffff; border-radius:8px;
                   overflow:hidden; box-shadow:0 2px 8px rgba(0,0,0,.12); }}
    .header     {{ background:#003865; padding:28px 32px; }}
    .header h1  {{ color:#ffffff; margin:0; font-size:22px; font-weight:700; letter-spacing:.5px; }}
    .header p   {{ color:#a8c4d8; margin:6px 0 0; font-size:13px; }}
    .body       {{ padding:28px 32px; color:#333333; }}
    .body p     {{ font-size:14px; line-height:1.6; margin:0 0 16px; }}
    .stats      {{ display:flex; gap:16px; margin:20px 0; }}
    .stat-pill  {{ flex:1; background:#f0f4f8; border-left:4px solid #003865;
                   border-radius:4px; padding:12px 16px; }}
    .stat-pill .label {{ font-size:11px; color:#666; text-transform:uppercase;
                         letter-spacing:.6px; margin-bottom:4px; }}
    .stat-pill .value {{ font-size:20px; font-weight:700; color:#003865; }}
    .attach-box {{ border:1px solid #dce3ea; border-radius:6px; padding:14px 18px;
                   display:flex; align-items:center; gap:14px; margin:20px 0; }}
    .attach-box .meta .name {{ font-weight:600; font-size:14px; color:#003865; }}
    .attach-box .meta .sub  {{ font-size:12px; color:#888; margin-top:2px; }}
    .footer     {{ background:#f0f4f8; padding:16px 32px; font-size:11px;
                   color:#999; text-align:center; border-top:1px solid #dce3ea; }}
  </style>
</head>
<body>
  <div class="wrapper">
    <div class="header">
      <h1>LPV Adjustment Report</h1>
      <p>Reporting period: <strong>{period_label}</strong></p>
    </div>
    <div class="body">
      <p>Hi team,</p>
      <p>
        Please find attached the <strong>LPV Adjustment Report</strong> for
        <strong>{period_label}</strong>. The zip contains one CSV file per country
        from <code>{cfg['bq_dataset']}.{cfg['bq_table']}</code> for the previous
        calendar month.
      </p>

      <div class="stats">
        <div class="stat-pill">
          <div class="label">Total Records</div>
          <div class="value">{row_count:,}</div>
        </div>
        <div class="stat-pill">
          <div class="label">Countries</div>
          <div class="value">{country_count}</div>
        </div>
        <div class="stat-pill">
          <div class="label">Reporting Period</div>
          <div class="value">{period_label}</div>
        </div>
      </div>

      <div class="attach-box">
        <div class="meta">
          <div class="name">{filename}</div>
          <div class="sub">CSV files (zipped) &nbsp;·&nbsp; {country_count} country file(s) &nbsp;·&nbsp; {row_count:,} total rows</div>
        </div>
      </div>

      <p>
        This report is generated automatically. Please do not reply to this email.
        For questions, contact the Data Platform team.
      </p>
    </div>
    <div class="footer">
      Generated by {cfg['email_from_name']} &nbsp;·&nbsp; {generated_at}
    </div>
  </div>
</body>
</html>"""


def generate_reports(output_dir: Path | str) -> list[dict[str, Any]]:
    """Query BigQuery, write the country zip, and return an email payload."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    cfg = _load_config()

    df, period_label = run_query(cfg)
    if df.empty:
        log.warning(
            "Query returned 0 rows for period '%s'. Email will not be sent.",
            period_label,
        )
        return []

    period_safe = period_label.replace(" ", "_")
    zip_filename = f"LPV_Adjustment_{period_safe}.zip"
    log.info("Building per-country CSV files and compressing...")
    zip_bytes, total_uncompressed, country_count = build_country_zip(df, period_safe)
    attachment_path = output_dir / zip_filename
    attachment_path.write_bytes(zip_bytes)
    log.info(
        "Zip ready: %s  (%s compressed, %.1f%% of %s uncompressed)",
        zip_filename,
        _fmt_bytes(len(zip_bytes)),
        100 * len(zip_bytes) / total_uncompressed if total_uncompressed else 0,
        _fmt_bytes(total_uncompressed),
    )

    row_count = len(df)
    return [
        {
            "report_name": "LPV Adjustment",
            "subject": f"LPV Adjustment Report — {period_label}",
            "recipients": cfg["email_to"],
            "cc": cfg["email_cc"],
            "html_body": _build_html(period_label, row_count, zip_filename, cfg, country_count),
            "attachment_path": str(attachment_path),
            "row_count": row_count,
            "plain_text_summary": (
                f"LPV Adjustment Report for {period_label}: {row_count:,} rows "
                f"across {country_count} countries. See attached {zip_filename}."
            ),
        }
    ]
