"""Load LPV Adjustment report settings from Airflow Variables into process environment."""

from __future__ import annotations

import os
from typing import Any

from airflow.models import Variable

CONFIG_VAR_NAME = "lpv_report_config"
PASSWORD_VAR_NAME = "lpv_report_smtp_password"
SENDGRID_VAR_NAME = "sendgrid_api_key"
ENV_VAR_NAME = "env"

# Forced recipient list so a leftover test list in lpv_report_config
# cannot drop finance stakeholders on a PROD run.
REPORT_RECIPIENTS = (
    "finance-lpv@example.com, "
    "revenue-ops@example.com, "
    "dataops@example.com"
)

# Composer sync path for the notebook-shaped report module.
LPV_SOURCE_DIR = "/home/airflow/gcs/dags/lpv_report"
LPV_ENTRYPOINT = "lpv_report.py"
LPV_STAGING_DIR = "/home/airflow/gcs/data/lpv_report"


def _parse_recipients(value: str | list[str] | None) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        return ", ".join(value)
    return value


def _project_id_for_env(env: str, config: dict[str, Any]) -> str:
    explicit = config.get("bq_project") or config.get("gcp_project_id")
    if explicit:
        return str(explicit)
    if env == "DEV":
        return "dwh_project_dev"
    return "dwh_project"


def load_lpv_environment() -> dict[str, str]:
    """Apply LPV report config from Airflow Variables to ``os.environ``."""
    env = os.environ.get(ENV_VAR_NAME, Variable.get(ENV_VAR_NAME, default_var="DEV"))
    config = Variable.get(CONFIG_VAR_NAME, deserialize_json=True)
    email_provider = str(config.get("email_provider", "smtp")).lower()

    if email_provider == "sendgrid":
        password = ""
        sendgrid_api_key = Variable.get(SENDGRID_VAR_NAME)
    else:
        password = Variable.get(PASSWORD_VAR_NAME)
        sendgrid_api_key = ""

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
        "EMAIL_FROM_NAME": str(config.get("email_from_name", "Platform Reports")),
        "EMAIL_TO": _parse_recipients(config.get("email_to") or config.get("email_recipients")),
        "EMAIL_CC": _parse_recipients(config.get("email_cc")),
        "BQ_PROJECT": _project_id_for_env(env, config),
        "BQ_DATASET": str(config.get("bq_dataset", "discovery")),
        "BQ_TABLE": str(config.get("bq_table", "lpv_adjusted")),
    }

    for key, value in env_map.items():
        os.environ[key] = value

    # Always pin recipients; config JSON is for transport + BQ only.
    os.environ["EMAIL_TO"] = REPORT_RECIPIENTS
    os.environ["EMAIL_CC"] = ""
    env_map["EMAIL_TO"] = REPORT_RECIPIENTS
    env_map["EMAIL_CC"] = ""

    return env_map
