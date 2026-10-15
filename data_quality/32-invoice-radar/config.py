"""Load Invoice Radar settings from Airflow Variables into process environment."""

from __future__ import annotations

import os
from typing import Any

from airflow.models import Variable

CONFIG_VAR_NAME = "invoice_radar_config"
PASSWORD_VAR_NAME = "invoice_radar_smtp_password"
SENDGRID_VAR_NAME = "sendgrid_api_key"
ENV_VAR_NAME = "env"

# Dev testing — restrict report delivery until prod go-live.
DEV_TEST_RECIPIENTS = (
    "dataops@example.com"
)

# GitLab source synced to this folder on the Composer worker.
INVOICE_RADAR_SOURCE_DIR = "/home/airflow/gcs/dags/invoice_radar"  # Composer sync path
INVOICE_RADAR_ENTRYPOINT = "invoice_radar.py"
INVOICE_RADAR_STAGING_DIR = "/home/airflow/gcs/data/invoice_radar"


def _parse_recipients(value: str | list[str] | None) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        return ", ".join(value)
    return value


def _project_id_for_env(env: str, config: dict[str, Any]) -> str:
    if config.get("gcp_project_id"):
        return str(config["gcp_project_id"])
    if env == "DEV":
        return "dwh_project_dev"
    return "dwh_project"


def _fq(project: str, dataset: str, table: str) -> str:
    return f"{project}.{dataset}.{table}"


def load_invoice_radar_environment() -> dict[str, str]:
    """Apply Invoice Radar config from Airflow Variables to ``os.environ``."""
    env = os.environ.get(ENV_VAR_NAME, Variable.get(ENV_VAR_NAME, default_var="DEV"))
    config = Variable.get(CONFIG_VAR_NAME, deserialize_json=True)
    email_provider = str(config.get("email_provider", "smtp")).lower()

    if email_provider == "sendgrid":
        password = ""
        sendgrid_api_key = Variable.get(SENDGRID_VAR_NAME)
    else:
        password = Variable.get(PASSWORD_VAR_NAME)
        sendgrid_api_key = ""

    project_id = _project_id_for_env(env, config)
    ds_discovery = str(config.get("bq_dataset_discovery", "discovery"))
    ds_product_spot = str(config.get("bq_dataset_product_spot", "product_spot"))
    ds_sales = str(config.get("bq_dataset_refined_sales", "refined_sales"))
    ds_dest = str(config.get("bq_dest_dataset", "bi"))

    source_view = _fq(project_id, ds_discovery, str(config.get("bq_table_lpv_adj", "lpv_adjusted")))
    asset_table = _fq(
        project_id, ds_product_spot, str(config.get("bq_table_asset", "erp_asset"))
    )
    pricing_table = _fq(
        project_id,
        ds_sales,
        str(config.get("bq_table_pricing", "ic_pricing_table")),
    )
    dest_full = _fq(project_id, ds_dest, str(config.get("bq_dest_table", "all_invoices")))

    env_map = {
        "EMAIL_PROVIDER": email_provider,
        "SENDGRID_API_KEY": sendgrid_api_key,
        "SMTP_HOST": str(config.get("smtp_host", "")),
        "SMTP_PORT": str(config.get("smtp_port", 587)),
        "SMTP_USE_TLS": str(config.get("smtp_use_tls", True)).lower(),
        "SMTP_USER": str(config.get("smtp_user", "")),
        "SMTP_TIMEOUT": str(config.get("smtp_timeout", 60)),
        "SMTP_PASSWORD": password,
        "EMAIL_FROM": str(config.get("email_from", "")),
        "EMAIL_FROM_NAME": str(config.get("email_from_name", "Invoice Radar")),
        "EMAIL_RECIPIENTS": _parse_recipients(
            config.get("email_recipients") or config.get("email_to")
        ),
        "EMAIL_CC": _parse_recipients(config.get("email_cc")),
        "GCP_PROJECT_ID": project_id,
        "BQ_SOURCE_VIEW": str(config.get("bq_source_view") or source_view),
        "BQ_ASSET_TABLE": str(config.get("bq_asset_table") or asset_table),
        "BQ_PRICING_TABLE": str(config.get("bq_pricing_table") or pricing_table),
        "BQ_DEST_FULL": str(config.get("bq_dest_full") or dest_full),
        "WRITE_ALL_INVOICES": str(config.get("write_all_invoices", True)).lower(),
    }

    for key, value in env_map.items():
        os.environ[key] = value

    if env == "DEV":
        os.environ["EMAIL_RECIPIENTS"] = DEV_TEST_RECIPIENTS
        os.environ["EMAIL_CC"] = ""
        env_map["EMAIL_RECIPIENTS"] = DEV_TEST_RECIPIENTS
        env_map["EMAIL_CC"] = ""

    return env_map
