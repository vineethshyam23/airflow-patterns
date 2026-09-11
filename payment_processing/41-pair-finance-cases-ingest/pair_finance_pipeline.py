"""Extract / GCS helpers for the collections partner daily DAG.

API keys are read from Secret Manager ``collections-{market}-api-key``,
then Airflow Variable ``pair_finance_api_keys`` as fallback.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from google.cloud import storage

from pair_finance_api import (
    cases_to_ndjson,
    flatten_case,
    get_case,
    list_cases,
    normalize_market,
)

logger = logging.getLogger(__name__)

RAW_PREFIX = "pair-finance"
SECRET_ID_TEMPLATE = "collections-{market}-api-key"
DEV_SECRET_PROJECT = "dwh_project_dev"
PROD_SECRET_PROJECT = "dwh_project"

# Must match dbt macro create_pair_finance_cases_raw_if_missing (do not autodetect).
STAGING_SCHEMA_FIELDS = [
    {"name": "case_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "reference_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "merchant", "type": "STRING", "mode": "NULLABLE"},
    {"name": "status", "type": "STRING", "mode": "NULLABLE"},
    {"name": "phase", "type": "STRING", "mode": "NULLABLE"},
    {"name": "amount", "type": "STRING", "mode": "NULLABLE"},
    {"name": "currency", "type": "STRING", "mode": "NULLABLE"},
    {"name": "market", "type": "STRING", "mode": "NULLABLE"},
    {"name": "created_at", "type": "STRING", "mode": "NULLABLE"},
    {"name": "updated_at", "type": "STRING", "mode": "NULLABLE"},
    {"name": "customer_number", "type": "STRING", "mode": "NULLABLE"},
    {"name": "first_name", "type": "STRING", "mode": "NULLABLE"},
    {"name": "last_name", "type": "STRING", "mode": "NULLABLE"},
    {"name": "company_name", "type": "STRING", "mode": "NULLABLE"},
    {"name": "raw_json", "type": "STRING", "mode": "NULLABLE"},
    {"name": "load_date", "type": "DATE", "mode": "NULLABLE"},
    {"name": "ingested_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
]


def gcs_object_name(load_date: str, market: str, filename: str = "cases.ndjson") -> str:
    return f"{RAW_PREFIX}/{load_date}/{normalize_market(market)}/{filename}"


def _variable_json(key: str, default: Optional[dict] = None) -> Dict[str, Any]:
    from airflow.models import Variable

    raw = Variable.get(key, default_var="")
    if not raw:
        return default or {}
    if isinstance(raw, dict):
        return raw
    return json.loads(raw)


def secret_id(market: str) -> str:
    return SECRET_ID_TEMPLATE.format(market=normalize_market(market).lower())


def resolve_env() -> str:
    """Composer sets ``env`` and ``PROJECT``. Prod is dwh_project / PROD."""
    env = (os.environ.get("env") or "").strip().upper()
    if env in {"DEV", "PROD"}:
        return env
    project = (os.environ.get("PROJECT") or "").strip()
    if project == PROD_SECRET_PROJECT:
        return "PROD"
    if project == DEV_SECRET_PROJECT:
        return "DEV"
    try:
        from airflow.models import Variable

        env = (Variable.get("env", default_var="") or "").strip().upper()
        if env in {"DEV", "PROD"}:
            return env
    except Exception:
        pass
    return "DEV"


def secret_project_id() -> str:
    """Secrets live in the same GCP project as the Composer environment."""
    from airflow.models import Variable

    override = Variable.get("pair_finance_secret_project", default_var="")
    if override:
        return override
    project = (os.environ.get("PROJECT") or "").strip()
    if project:
        return project
    return PROD_SECRET_PROJECT if resolve_env() == "PROD" else DEV_SECRET_PROJECT


def _from_secret_manager(market: str) -> Optional[str]:
    code = normalize_market(market)
    project = secret_project_id()
    resource = f"projects/{project}/secrets/{secret_id(code)}/versions/latest"
    try:
        from google.cloud import secretmanager
    except ImportError:
        logger.warning(
            "google-cloud-secret-manager is not installed; skipping Secret Manager for %s",
            code,
        )
        return None
    try:
        client = secretmanager.SecretManagerServiceClient()
        payload = client.access_secret_version(request={"name": resource}).payload.data
        key = payload.decode("utf-8").strip() if payload else ""
    except Exception as exc:
        logger.info("No Secret Manager key for %s (%s): %s", code, resource, type(exc).__name__)
        return None
    if not key:
        return None
    logger.info("Loaded collections partner API key for %s from Secret Manager", code)
    return key


def get_api_key(market: str) -> Optional[str]:
    """Return the market API key, or None if it has not been provisioned yet."""
    code = normalize_market(market)
    key = _from_secret_manager(code)
    if key:
        return key
    keys = _variable_json("pair_finance_api_keys")
    fallback = keys.get(code) or keys.get(code.lower())
    return str(fallback) if fallback else None


def composer_bucket_name() -> str:
    from airflow.models import Variable

    return Variable.get("composer_bucket", default_var="")


def raw_bucket_name() -> str:
    """Landing + BQ source bucket."""
    from airflow.models import Variable

    configured = (Variable.get("pair_finance_raw_bucket", default_var="") or "").strip()
    fallback = composer_bucket_name()
    if not fallback:
        fallback = "composer-data" if resolve_env() == "PROD" else ""
    if configured and configured != fallback:
        client = storage.Client()
        if not client.bucket(configured).exists():
            logger.warning(
                "pair_finance_raw_bucket gs://%s does not exist; using gs://%s",
                configured,
                fallback,
            )
            return fallback
        return configured
    return configured or fallback


def fetch_details_enabled() -> bool:
    from airflow.models import Variable

    return Variable.get("pair_finance_fetch_details", default_var="false").lower() in {
        "1",
        "true",
        "yes",
    }


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "y"}


def is_full_load(context: Optional[Dict[str, Any]] = None) -> bool:
    """One-time seed: Trigger DAG with conf ``{"full_load": true}``."""
    conf = {}
    if context:
        dag_run = context.get("dag_run")
        conf = (getattr(dag_run, "conf", None) or {}) if dag_run is not None else {}
        if isinstance(context.get("params"), dict) and not conf:
            conf = context["params"]
    if _truthy(conf.get("full_load")):
        return True
    from airflow.models import Variable

    return _truthy(Variable.get("pair_finance_full_load", default_var="false"))


def unix_window(ds: str) -> tuple[int, int]:
    """UTC [ds 00:00, next day 00:00) as unix seconds for updated_from/updated_to."""
    start = datetime.strptime(ds, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    return int(start.timestamp()), int(end.timestamp())


def gcs_blob_exists(bucket_name: str, object_name: str) -> bool:
    if not bucket_name:
        return False
    client = storage.Client()
    return client.bucket(bucket_name).blob(object_name).exists()


def gcs_blob_size(bucket_name: str, object_name: str) -> Optional[int]:
    if not bucket_name:
        return None
    client = storage.Client()
    blob = client.bucket(bucket_name).blob(object_name)
    if not blob.exists():
        return None
    blob.reload()
    return blob.size


def upload_ndjson(bucket_name: str, object_name: str, ndjson: str) -> None:
    client = storage.Client()
    blob = client.bucket(bucket_name).blob(object_name)
    blob.upload_from_string(ndjson, content_type="application/x-ndjson")
    logger.info("Uploaded gs://%s/%s (%s bytes)", bucket_name, object_name, len(ndjson.encode("utf-8")))


def extract_cases(market: str, **context) -> Dict[str, Any]:
    """API → NDJSON on the raw GCS prefix. Skips if the date/market object already exists."""
    ds = context["ds"]
    code = normalize_market(market)
    object_name = gcs_object_name(ds, code)
    dest_bucket = raw_bucket_name()
    landing_bucket = dest_bucket
    from airflow.exceptions import AirflowSkipException

    full_load = is_full_load(context)
    existing_size = gcs_blob_size(dest_bucket, object_name)
    if existing_size and not full_load:
        raise AirflowSkipException(
            f"Already loaded gs://{dest_bucket}/{object_name} — skipping extract"
        )

    api_key = get_api_key(code)
    if not api_key:
        raise AirflowSkipException(
            f"No API key for {code}. Expected Secret Manager "
            f"{secret_id(code)} in {secret_project_id()} (or Variable pair_finance_api_keys)."
        )

    list_kwargs: Dict[str, Any] = {}
    if full_load:
        logger.info("Collections partner full load for %s (no updated_from/updated_to filter)", code)
    else:
        updated_from, updated_to = unix_window(ds)
        list_kwargs["updated_from"] = updated_from
        list_kwargs["updated_to"] = updated_to
    cases = list_cases(code, api_key, **list_kwargs)

    if fetch_details_enabled():
        detailed = []
        for row in cases:
            case_id = row.get("id")
            if not case_id:
                detailed.append(row)
                continue
            detailed.append(get_case(str(case_id), code, api_key))
        cases = detailed

    ingested_at = datetime.now(timezone.utc).isoformat()
    rows = [flatten_case(case, code, ds, ingested_at) for case in cases]
    if rows:
        upload_ndjson(landing_bucket, object_name, cases_to_ndjson(rows))
    else:
        logger.info("No collections partner cases for %s on %s; nothing uploaded", code, ds)

    return {
        "market": code,
        "load_date": ds,
        "record_count": len(rows),
        "object_name": object_name,
        "landing_bucket": landing_bucket,
        "dest_bucket": dest_bucket,
        "full_load": full_load,
    }


def load_gcs(market: str, **context) -> Dict[str, Any]:
    """Copy landing object to the dest bucket when they differ."""
    from google.cloud.exceptions import NotFound

    ds = context["ds"]
    code = normalize_market(market)
    ti = context["ti"]
    extract_meta = ti.xcom_pull(task_ids=f"task_group_{code}.extract_cases") or {}

    object_name = extract_meta.get("object_name") or gcs_object_name(ds, code)
    dest_bucket = extract_meta.get("dest_bucket") or raw_bucket_name()
    landing_bucket = extract_meta.get("landing_bucket") or dest_bucket
    record_count = int(extract_meta.get("record_count") or 0)

    if record_count == 0:
        logger.info("No records for %s; skipping GCS copy", code)
        return extract_meta

    if landing_bucket == dest_bucket:
        if not gcs_blob_exists(dest_bucket, object_name):
            raise FileNotFoundError(f"Missing gs://{dest_bucket}/{object_name}")
        logger.info("Raw object already at destination gs://%s/%s", dest_bucket, object_name)
        return extract_meta

    client = storage.Client()
    source_bucket = client.bucket(landing_bucket)
    dest_bucket_obj = client.bucket(dest_bucket)
    source = source_bucket.blob(object_name)
    try:
        source_bucket.copy_blob(source, dest_bucket_obj, object_name)
    except NotFound as exc:
        raise FileNotFoundError(f"Missing gs://{landing_bucket}/{object_name}") from exc
    logger.info(
        "Copied gs://%s/%s → gs://%s/%s",
        landing_bucket,
        object_name,
        dest_bucket,
        object_name,
    )
    return {
        "market": code,
        "load_date": ds,
        "object_name": object_name,
        "landing_bucket": landing_bucket,
        "dest_bucket": dest_bucket,
        "record_count": extract_meta.get("record_count"),
    }


def has_records(market: str, **context) -> bool:
    """Skip BQ load when extract wrote zero case files."""
    code = normalize_market(market)
    meta = context["ti"].xcom_pull(task_ids=f"task_group_{code}.extract_cases") or {}
    return int(meta.get("record_count") or 0) > 0
