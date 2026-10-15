"""Load Mach2 report settings from Airflow Variables into process environment."""

from __future__ import annotations

import os
from typing import Any

from airflow.models import Variable

CONFIG_VAR_NAME = "mach2_report_config"
PASSWORD_VAR_NAME = "mach2_report_smtp_password"
SENDGRID_VAR_NAME = "sendgrid_api_key"
ENV_VAR_NAME = "env"

# Dev testing — restrict report delivery until prod go-live.
DEV_TEST_RECIPIENTS = (
    "dataops@example.com"
)

# GitLab source synced to this folder on the Composer worker.
MACH2_SOURCE_DIR = "/home/airflow/gcs/dags/mach2_report"  # Composer sync path
MACH2_ENTRYPOINT = "report.py"
MACH2_STAGING_DIR = "/home/airflow/gcs/data/mach2_reports"


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


def load_mach2_environment() -> dict[str, str]:
    """Apply Mach2 config from Airflow Variables to ``os.environ``."""
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
        "EMAIL_FROM_NAME": str(config.get("email_from_name", "")),
        "EMAIL_TO_REPORT_1": _parse_recipients(config.get("email_to_report_1")),
        "EMAIL_TO_REPORT_2": _parse_recipients(config.get("email_to_report_2")),
        "EMAIL_TO_REPORT_3": _parse_recipients(config.get("email_to_report_3")),
        "EMAIL_CC": _parse_recipients(config.get("email_cc")),
        "GCP_PROJECT_ID": project_id,
        "BQ_DATASET_REFINED_SALES": str(config.get("bq_dataset_refined_sales", "refined_sales")),
        "BQ_DATASET_REFINED": str(config.get("bq_dataset_refined", "refined")),
        "BQ_TABLE_RES_PARTNER": str(config.get("bq_table_res_partner", "odoo_res_partner")),
        "BQ_TABLE_SALE_ORDER": str(config.get("bq_table_sale_order", "odoo_sale_order")),
        "BQ_TABLE_SALE_ORDER_LINE": str(config.get("bq_table_sale_order_line", "odoo_sale_order_line")),
        "BQ_TABLE_RES_COUNTRY": str(config.get("bq_table_res_country", "odoo_res_country")),
        "BQ_TABLE_PRODUCT_PRODUCT": str(config.get("bq_table_product_product", "odoo_product_product")),
        "BQ_TABLE_PRODUCT_TEMPLATE": str(config.get("bq_table_product_template", "odoo_product_template")),
        "BQ_TABLE_SALE_ORDER_CLOSE_REASON": str(
            config.get("bq_table_sale_order_close_reason", "odoo_sale_order_close_reason")
        ),
        "BQ_TABLE_PARTNER_MATCHING": str(
            config.get("bq_table_partner_matching", "partner_crm_matching_ids")
        ),
    }

    for key, value in env_map.items():
        os.environ[key] = value

    if env == "DEV":
        os.environ["EMAIL_TO_REPORT_1"] = DEV_TEST_RECIPIENTS
        os.environ["EMAIL_TO_REPORT_2"] = DEV_TEST_RECIPIENTS
        os.environ["EMAIL_TO_REPORT_3"] = DEV_TEST_RECIPIENTS
        os.environ["EMAIL_CC"] = ""
        env_map["EMAIL_TO_REPORT_1"] = DEV_TEST_RECIPIENTS
        env_map["EMAIL_TO_REPORT_2"] = DEV_TEST_RECIPIENTS
        env_map["EMAIL_TO_REPORT_3"] = DEV_TEST_RECIPIENTS
        env_map["EMAIL_CC"] = ""

    return env_map
