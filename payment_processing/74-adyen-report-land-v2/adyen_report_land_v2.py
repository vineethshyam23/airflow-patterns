"""Adyen report helpers for ``etl_adyen_report_land_v2`` only.

v1 keeps using ``adyen_report_land_v1.py``. Do not import this module from v1.

* **Settlement** — ``fetch_settlement_date_range_to_gcs``: probe every daily
  filename in a date range. Each hit is uploaded to rawzone as its own object.
* **Received payment details** — ``fetch_received_payments_date_range_to_gcs``:
  probe ``received_payments_report_YYYY_MM_DD.csv`` (hyphen date as fallback)
  for the same date range.
* **Platform margins** — team drop in
  ``gs://dp-analytics-drop/adyen-report-upload/``. After BQ load,
  ``archive_gcs_csvs_to_rawzone`` moves files to
  ``adyen/report/platform_margins/YYYY-MM-DD/``.
* **v2** — ``add_file_name_column`` / ``stage_drop_csvs_with_file_name`` stamp
  ``file_name`` onto each CSV. No in-worker merge.
* **Staging tables** — ``ensure_dbt_source_tables`` runs ``CREATE TABLE IF NOT
  EXISTS`` from rawzone schema JSON so dbt sources exist even when a fetch or
  margins drop is skipped. Existing tables are left unchanged.

No Adyen webhook is used.
"""

from __future__ import annotations

import base64
import csv
import io
import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Sequence, Tuple

import requests
from airflow.exceptions import AirflowSkipException
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from requests.exceptions import RequestException

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AdyenReportNotFoundError(Exception):
    """Raised when Adyen returns HTTP 404 for the requested report file."""


DOWNLOAD_HOST_TEMPLATE = "https://ca-{environment}.adyen.com"
DOWNLOAD_PATH_TEMPLATE = "/reports/download/{account_type}/{account_name}/{file_name}"

DEFAULT_ENVIRONMENT = "live"
DEFAULT_ACCOUNT_TYPE = "Company"

REQUEST_TIMEOUT_SECONDS = 300
DOWNLOAD_MAX_RETRIES = 3
DOWNLOAD_RETRY_DELAY_SECONDS = 60

TMP_LOC = "/home/airflow/gcs/data/adyen/report"

DEFAULT_ANALYTICS_DROP_BUCKET = "dp-analytics-drop"
DEFAULT_PLATFORM_MARGINS_LANDING_PREFIX = "adyen-report-upload"
DEFAULT_SETTLEMENT_FILE_PREFIX = "settlement_detail_report_"

# Adyen report grain — used by dbt incremental unique_key (not GCS load).
# Settlement: one payment can have several journal lines (Settled, Refunded,
# Chargeback, MerchantPayout, …). Psp_Reference + Type + Modification_Reference
# identify the line; Batch_Number separates payout-batch repeats / empty PSP
# MerchantPayout rows. Docs: settlement-details-report.
SETTLEMENT_UNIQUE_KEY = (
    "Psp_Reference",
    "Type",
    "Modification_Reference",
    "Batch_Number",
)
# Received payment details: Psp_Reference is Adyen's globally unique payment id.
RECEIVED_PAYMENTS_UNIQUE_KEY = ("Psp_Reference",)
# Platform margins / fee export: one fee amount per month + account + fee + ccy.
PLATFORM_MARGINS_UNIQUE_KEY = (
    "Billing_Month",
    "AccountHolder",
    "BalanceAccount",
    "Fee_Type",
    "Fee_Name",
    "Currency_Code",
)


@dataclass
class AdyenReportConfig:
    """Connection settings for downloading Adyen reports."""

    account_name: str
    account_type: str = DEFAULT_ACCOUNT_TYPE
    environment: str = DEFAULT_ENVIRONMENT
    api_key: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None

    def __post_init__(self) -> None:
        if not self.account_name:
            raise ValueError("AdyenReportConfig requires a non-empty account_name")
        if not self.api_key and not (self.username and self.password):
            raise ValueError(
                "AdyenReportConfig requires either api_key or username/password"
            )

    @classmethod
    def from_variable(cls, creds: Dict[str, Any]) -> "AdyenReportConfig":
        account_name = (
            creds.get("account_name")
            or creds.get("merchant_account")
            or creds.get("company_account")
            or ""
        )
        return cls(
            account_name=account_name,
            account_type=creds.get("account_type", DEFAULT_ACCOUNT_TYPE),
            environment=creds.get("environment", DEFAULT_ENVIRONMENT),
            api_key=creds.get("api_key"),
            username=creds.get("username"),
            password=creds.get("password"),
        )

    @property
    def headers(self) -> Dict[str, str]:
        if self.api_key:
            return {"X-API-Key": self.api_key}
        token = base64.b64encode(
            f"{self.username}:{self.password}".encode()
        ).decode()
        return {"Authorization": f"Basic {token}"}

    def download_url(self, file_name: str) -> str:
        host = DOWNLOAD_HOST_TEMPLATE.format(environment=self.environment)
        path = DOWNLOAD_PATH_TEMPLATE.format(
            account_type=self.account_type,
            account_name=self.account_name,
            file_name=file_name,
        )
        return f"{host}{path}"


class AdyenReportClient:
    """Minimal HTTP client for downloading a named Adyen report file."""

    def __init__(self, config: AdyenReportConfig) -> None:
        self._config = config

    @property
    def config(self) -> AdyenReportConfig:
        return self._config

    def download_report(self, file_name: str) -> str:
        """Download a single report file and return its raw CSV text."""
        local_tmp = f"/tmp/adyen_report_{os.getpid()}_{file_name}"
        try:
            self.download_report_to_file(file_name, local_tmp)
            with open(local_tmp, encoding="utf-8") as handle:
                return handle.read()
        finally:
            if os.path.exists(local_tmp):
                os.remove(local_tmp)

    def download_report_to_file(self, file_name: str, path: str) -> int:
        """Stream one report to ``path``. Returns bytes written."""
        url = self._config.download_url(file_name)
        last_error: Optional[BaseException] = None
        max_attempts = 1 + DOWNLOAD_MAX_RETRIES
        for attempt in range(1, max_attempts + 1):
            try:
                with requests.get(
                    url,
                    headers=self._config.headers,
                    timeout=REQUEST_TIMEOUT_SECONDS,
                    stream=True,
                ) as response:
                    if response.status_code == 404:
                        raise AdyenReportNotFoundError(
                            f"Adyen report not found (404): {file_name}"
                        )
                    response.raise_for_status()
                    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
                    written = 0
                    with open(path, "wb") as handle:
                        for chunk in response.iter_content(chunk_size=8 * 1024 * 1024):
                            if chunk:
                                handle.write(chunk)
                                written += len(chunk)
                logger.info(
                    "Downloaded Adyen report file=%s bytes=%s",
                    file_name,
                    written,
                )
                return written
            except AdyenReportNotFoundError:
                raise
            except RequestException as exc:
                last_error = exc
                status = getattr(getattr(exc, "response", None), "status_code", None)
                if status == 404:
                    raise AdyenReportNotFoundError(
                        f"Adyen report not found (404): {file_name}"
                    ) from exc
                logger.error(
                    "Adyen report download failed file=%s attempt=%s/%s "
                    "http_status=%s error=%s",
                    file_name,
                    attempt,
                    max_attempts,
                    status,
                    exc,
                )
                if attempt < max_attempts:
                    time.sleep(DOWNLOAD_RETRY_DELAY_SECONDS)
        assert last_error is not None
        raise last_error


def append_file_name_column(csv_text: str, file_name: str) -> str:
    """Append a ``file_name`` column so BQ schema JSON can include lineage."""
    if not (csv_text or "").strip():
        return ""
    reader = csv.reader(io.StringIO(csv_text))
    rows = list(reader)
    if not rows:
        return ""
    header = [(cell or "").lstrip("\ufeff") for cell in rows[0]]
    out = io.StringIO()
    writer = csv.writer(out, lineterminator="\n")
    if header and header[-1] == "file_name":
        writer.writerow(header)
        writer.writerows(rows[1:])
        return out.getvalue()
    writer.writerow([*header, "file_name"])
    for row in rows[1:]:
        writer.writerow([*row, file_name])
    return out.getvalue()


def _stamp_local_csv(*, path: str, file_name: str) -> None:
    with open(path, encoding="utf-8") as handle:
        stamped = append_file_name_column(handle.read(), file_name)
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(stamped)


def _clear_gcs_prefix(*, bucket: str, prefix: str, gcp_conn_id: str) -> None:
    """Delete existing objects under ``prefix`` so wildcard loads stay clean."""
    hook = GCSHook(gcp_conn_id=gcp_conn_id)
    prefix = prefix.rstrip("/") + "/"
    blobs = hook.list(bucket_name=bucket, prefix=prefix) or []
    for blob in blobs:
        if blob.endswith("/"):
            continue
        hook.delete(bucket_name=bucket, object_name=blob)
        logger.info("Deleted stale GCS object gs://%s/%s", bucket, blob)


def settlement_daily_file_names(start: date, end: date) -> List[str]:
    """Build daily settlement filenames for each day in ``[start, end]``."""
    if end < start:
        return []
    names: List[str] = []
    current = start
    while current <= end:
        day = current.strftime("%Y-%m-%d")
        names.append(f"settlement_detail_report_{day}_{day}.csv")
        current += timedelta(days=1)
    return names


def received_payments_daily_file_candidates(
    start: date, end: date
) -> List[List[str]]:
    """Candidate daily Received payment details names for ``[start, end]``.

    Adyen's documented name uses underscores
    (``received_payments_report_YYYY_MM_DD.csv``). Some CA exports use hyphens.
    Each inner list is tried in order; the first hit wins for that day.
    """
    if end < start:
        return []
    groups: List[List[str]] = []
    current = start
    while current <= end:
        ymd_us = current.strftime("%Y_%m_%d")
        ymd_hy = current.strftime("%Y-%m-%d")
        groups.append(
            [
                f"received_payments_report_{ymd_us}.csv",
                f"received_payments_report_{ymd_hy}.csv",
            ]
        )
        current += timedelta(days=1)
    return groups


def _month_starts(start: date, end: date) -> List[date]:
    """First day of each calendar month overlapping ``[start, end]``."""
    if end < start:
        return []
    cursor = date(start.year, start.month, 1)
    last = date(end.year, end.month, 1)
    months: List[date] = []
    while cursor <= last:
        months.append(cursor)
        if cursor.month == 12:
            cursor = date(cursor.year + 1, 1, 1)
        else:
            cursor = date(cursor.year, cursor.month + 1, 1)
    return months


def platform_margins_month_file_names(
    start: date,
    end: date,
    account_name: str = "PartnerPayments",
) -> List[str]:
    """Candidate monthly platform-margins filenames for each month in range.

    Tries predictable patterns Adyen may use. Files that still include a random
    generation suffix (``..._PartnerPayments_<id>.csv``) cannot be invented here;
    pass those via ``report_file_names`` as extras.
    """
    names: List[str] = []
    for month_start in _month_starts(start, end):
        ym = month_start.strftime("%Y_%m")
        names.append(f"platform_margins_report_{ym}.csv")
        names.append(f"platform_margins_report_{ym}_{account_name}.csv")
    return names


def _parse_date(value: Any) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    return datetime.strptime(str(value)[:10], "%Y-%m-%d").date()


def _merge_csv_texts(csv_texts: Sequence[str]) -> str:
    """Merge CSVs keeping the first header only."""
    header: Optional[str] = None
    body_lines: List[str] = []
    for text in csv_texts:
        lines = [line for line in text.splitlines() if line.strip() != ""]
        if not lines:
            continue
        if header is None:
            header = lines[0]
            body_lines.extend(lines[1:])
        else:
            body_lines.extend(lines[1:])
    if header is None:
        return ""
    return "\n".join([header, *body_lines]) + ("\n" if body_lines else "\n")


def _append_csv_text_to_file(
    path: str,
    csv_text: str,
    *,
    header_written: bool,
) -> bool:
    """Append one CSV to ``path``. Returns whether a header is now on disk."""
    lines = [line for line in csv_text.splitlines() if line.strip() != ""]
    if not lines:
        return header_written
    os.makedirs(os.path.dirname(path), exist_ok=True)
    mode = "a" if header_written else "w"
    with open(path, mode, encoding="utf-8") as handle:
        if not header_written:
            handle.write(lines[0] + "\n")
            body = lines[1:]
        else:
            body = lines[1:]
        for line in body:
            handle.write(line + "\n")
    return True


def _build_client(**kwargs: Any) -> AdyenReportClient:
    creds: Dict[str, Any] = dict(kwargs["creds"])
    for override_key in ("account_name", "account_type", "environment"):
        override_value = kwargs.get(override_key)
        if override_value:
            creds[override_key] = override_value
    return AdyenReportClient(AdyenReportConfig.from_variable(creds))


def _download_gcs_text(hook: GCSHook, bucket: str, object_name: str) -> str:
    if hasattr(hook, "download_as_byte_array"):
        raw = hook.download_as_byte_array(
            bucket_name=bucket, object_name=object_name
        )
        return raw.decode("utf-8")
    local_tmp = f"/tmp/{os.path.basename(object_name)}"
    hook.download(bucket_name=bucket, object_name=object_name, filename=local_tmp)
    with open(local_tmp, encoding="utf-8") as handle:
        return handle.read()


def list_gcs_csv_objects(
    *,
    bucket: str,
    prefix: str,
    gcp_conn_id: str,
) -> List[str]:
    """Return object names of every ``*.csv`` under ``prefix`` (sorted)."""
    hook = GCSHook(gcp_conn_id=gcp_conn_id)
    prefix = prefix.rstrip("/") + "/"
    blobs = hook.list(bucket_name=bucket, prefix=prefix) or []
    return sorted(
        blob
        for blob in blobs
        if blob.lower().endswith(".csv") and not blob.endswith("/")
    )


def list_gcs_csv_texts(
    *,
    bucket: str,
    prefix: str,
    gcp_conn_id: str,
) -> List[Tuple[str, str]]:
    """Return ``(object_name, csv_text)`` for every ``*.csv`` under ``prefix``."""
    hook = GCSHook(gcp_conn_id=gcp_conn_id)
    csv_blobs = list_gcs_csv_objects(
        bucket=bucket, prefix=prefix, gcp_conn_id=gcp_conn_id
    )
    results: List[Tuple[str, str]] = []
    for blob in csv_blobs:
        try:
            results.append((blob, _download_gcs_text(hook, bucket, blob)))
            logger.info("Loaded landing CSV gs://%s/%s", bucket, blob)
        except Exception:
            logger.exception("Failed reading landing CSV gs://%s/%s", bucket, blob)
    return results



_BQ_DDL_TYPES = {
    "INTEGER": "INT64",
    "INT64": "INT64",
    "FLOAT": "FLOAT64",
    "FLOAT64": "FLOAT64",
    "BOOLEAN": "BOOL",
    "BOOL": "BOOL",
}


def _schema_fields_to_ddl(fields: Sequence[Dict[str, Any]]) -> str:
    parts: List[str] = []
    for field in fields:
        name = str(field["name"]).strip()
        raw_type = str(field.get("type") or "STRING").upper()
        bq_type = _BQ_DDL_TYPES.get(raw_type, raw_type)
        parts.append(f"`{name}` {bq_type}")
    if not parts:
        raise ValueError("schema JSON has no fields")
    return ",\n  ".join(parts)


def _run_bq_sql(
    *,
    gcp_conn_id: str,
    project_id: str,
    location: str,
    sql: str,
) -> None:
    hook = BigQueryHook(
        gcp_conn_id=gcp_conn_id,
        use_legacy_sql=False,
        location=location,
    )
    if hasattr(hook, "insert_job"):
        hook.insert_job(
            configuration={"query": {"query": sql, "useLegacySql": False}},
            project_id=project_id,
        )
        return
    client = hook.get_client(project_id=project_id)
    client.query(sql, location=location).result()


def ensure_dbt_source_tables(**kwargs: Any) -> Dict[str, Any]:
    """Create dbt source staging tables if missing. Never truncates existing data.

    Reads the same GCS schema JSON used by ``GCSToBigQueryOperator``. Skipped
    Adyen/margins loads no longer leave dbt pointing at a missing table.
    """
    gcp_conn_id = (kwargs.get("gcp_conn_id") or "google_cloud_default").strip()
    project_id = (kwargs.get("project_id") or "").strip()
    dataset_id = (kwargs.get("dataset_id") or "trusted_staging").strip()
    location = (kwargs.get("location") or "EU").strip()
    schema_bucket = (kwargs.get("schema_bucket") or "").strip()
    tables = kwargs.get("tables") or []
    if not project_id or not schema_bucket or not tables:
        raise ValueError("project_id, schema_bucket, and tables are required")

    gcs_hook = GCSHook(gcp_conn_id=gcp_conn_id)
    created: List[str] = []
    for spec in tables:
        table_id = str(spec["table_id"]).strip()
        schema_object = str(spec["schema_object"]).strip()
        schema_text = _download_gcs_text(gcs_hook, schema_bucket, schema_object)
        fields = json.loads(schema_text)
        if not isinstance(fields, list):
            raise ValueError(f"schema JSON must be a list of fields: {schema_object}")
        fq_table = f"`{project_id}.{dataset_id}.{table_id}`"
        sql = (
            f"CREATE TABLE IF NOT EXISTS {fq_table} (\n  "
            f"{_schema_fields_to_ddl(fields)}\n)"
        )
        logger.info("Ensuring dbt source table %s from gs://%s/%s", fq_table, schema_bucket, schema_object)
        _run_bq_sql(
            gcp_conn_id=gcp_conn_id,
            project_id=project_id,
            location=location,
            sql=sql,
        )
        created.append(f"{project_id}.{dataset_id}.{table_id}")
    return {"tables": created}


def assert_gcs_csv_uploads(**kwargs: Any) -> List[str]:
    """Return drop-folder CSV names. Empty list short-circuits the margins branch."""
    bucket = (kwargs.get("bucket") or "").strip()
    prefix = (kwargs.get("prefix") or DEFAULT_PLATFORM_MARGINS_LANDING_PREFIX).strip()
    gcp_conn_id = (kwargs.get("gcp_conn_id") or "google_cloud_default").strip()
    csvs = list_gcs_csv_objects(bucket=bucket, prefix=prefix, gcp_conn_id=gcp_conn_id)
    if not csvs:
        logger.info("No CSV uploads in gs://%s/%s/", bucket, prefix.rstrip("/"))
        return []
    logger.info("Found %s CSV upload(s) under gs://%s/%s", len(csvs), bucket, prefix)
    return csvs


def archive_gcs_csvs_to_rawzone(**kwargs: Any) -> Dict[str, Any]:
    """Copy processed drop CSVs into dated rawzone folders, then delete the drop objects."""
    source_bucket = (kwargs.get("source_bucket") or "").strip()
    dest_bucket = (kwargs.get("dest_bucket") or "").strip()
    dest_prefix = (kwargs.get("dest_prefix") or "").strip().rstrip("/")
    gcp_conn_id = (kwargs.get("gcp_conn_id") or "google_cloud_default").strip()
    csvs = kwargs.get("source_objects")
    if csvs is None:
        ti = kwargs.get("ti")
        source_task_id = kwargs.get("source_task_id")
        csvs = ti.xcom_pull(task_ids=source_task_id) if ti and source_task_id else None
    csvs = [str(name) for name in (csvs or []) if str(name).strip()]
    if not csvs:
        raise ValueError("No CSV objects to archive")

    hook = GCSHook(gcp_conn_id=gcp_conn_id)
    archived: List[str] = []
    for blob in csvs:
        filename = blob.rsplit("/", 1)[-1]
        dest_object = f"{dest_prefix}/{filename}"
        try:
            hook.copy(
                source_bucket=source_bucket,
                source_object=blob,
                destination_bucket=dest_bucket,
                destination_object=dest_object,
            )
        except TypeError:
            hook.copy(source_bucket, blob, dest_bucket, dest_object)
        hook.delete(bucket_name=source_bucket, object_name=blob)
        archived.append(dest_object)
        logger.info(
            "Archived gs://%s/%s → gs://%s/%s",
            source_bucket,
            blob,
            dest_bucket,
            dest_object,
        )
    return {
        "archived_count": len(archived),
        "dest_bucket": dest_bucket,
        "dest_prefix": dest_prefix,
        "objects": archived,
    }


def stage_drop_csvs_with_file_name(**kwargs: Any) -> Dict[str, Any]:
    """Copy each drop CSV to rawzone with a ``file_name`` column. No merge."""
    source_bucket = (kwargs.get("source_bucket") or "").strip()
    dest_bucket = (kwargs.get("dest_bucket") or "").strip()
    dest_prefix = (kwargs.get("dest_prefix") or "").strip().rstrip("/")
    gcp_conn_id = (kwargs.get("gcp_conn_id") or "google_cloud_default").strip()
    csvs = kwargs.get("source_objects")
    if csvs is None:
        ti = kwargs.get("ti")
        source_task_id = kwargs.get("source_task_id")
        csvs = ti.xcom_pull(task_ids=source_task_id) if ti and source_task_id else None
    csvs = [str(name) for name in (csvs or []) if str(name).strip()]
    if not csvs:
        raise ValueError("No CSV objects to stage")
    if not dest_bucket or not dest_prefix:
        raise ValueError("dest_bucket and dest_prefix are required")

    _clear_gcs_prefix(
        bucket=dest_bucket, prefix=dest_prefix, gcp_conn_id=gcp_conn_id
    )
    hook = GCSHook(gcp_conn_id=gcp_conn_id)
    objects: List[str] = []
    source_names: List[str] = []
    local_dir = f"/tmp/adyen_report_margins_{os.getpid()}"
    os.makedirs(local_dir, exist_ok=True)
    try:
        for blob in csvs:
            filename = blob.rsplit("/", 1)[-1]
            text = _download_gcs_text(hook, source_bucket, blob)
            stamped = append_file_name_column(text, filename)
            local_path = os.path.join(local_dir, filename)
            with open(local_path, "w", encoding="utf-8") as handle:
                handle.write(stamped)
            dest_object = f"{dest_prefix}/{filename}"
            hook.upload(
                bucket_name=dest_bucket,
                object_name=dest_object,
                filename=local_path,
            )
            objects.append(dest_object)
            source_names.append(filename)
            logger.info(
                "Staged gs://%s/%s → gs://%s/%s (file_name=%s)",
                source_bucket,
                blob,
                dest_bucket,
                dest_object,
                filename,
            )
    finally:
        for leftover in os.listdir(local_dir) if os.path.isdir(local_dir) else []:
            try:
                os.remove(os.path.join(local_dir, leftover))
            except OSError:
                pass
        try:
            os.rmdir(local_dir)
        except OSError:
            pass

    return {
        "downloaded_files": source_names,
        "objects": objects,
        "downloaded_count": len(objects),
        "dest_bucket": dest_bucket,
        "dest_prefix": dest_prefix,
        "source_objects": csvs,
    }


def delete_gcs_objects(**kwargs: Any) -> Dict[str, Any]:
    """Delete drop-folder CSVs after a successful rawzone + BQ load."""
    bucket = (kwargs.get("bucket") or "").strip()
    gcp_conn_id = (kwargs.get("gcp_conn_id") or "google_cloud_default").strip()
    csvs = kwargs.get("source_objects")
    if csvs is None:
        ti = kwargs.get("ti")
        source_task_id = kwargs.get("source_task_id")
        csvs = ti.xcom_pull(task_ids=source_task_id) if ti and source_task_id else None
    csvs = [str(name) for name in (csvs or []) if str(name).strip()]
    if not csvs:
        raise ValueError("No GCS objects to delete")
    hook = GCSHook(gcp_conn_id=gcp_conn_id)
    deleted: List[str] = []
    for blob in csvs:
        hook.delete(bucket_name=bucket, object_name=blob)
        deleted.append(blob)
        logger.info("Deleted drop object gs://%s/%s", bucket, blob)
    return {"deleted_count": len(deleted), "objects": deleted}


def _fetch_report_name_groups_to_gcs(
    *,
    client: AdyenReportClient,
    file_name_groups: Sequence[Sequence[str]],
    dest_bucket: str,
    dest_prefix: str,
    gcp_conn_id: str,
    mode: str,
    start: date,
    end: date,
    add_file_name_column: bool = False,
    dest_object_name: Optional[str] = None,
) -> Dict[str, Any]:
    """Download the first existing name in each group and upload to GCS.

    When ``add_file_name_column`` is True, each CSV gets a ``file_name``
    column (original Adyen basename) so the GCS schema JSON can include it.
    ``dest_object_name`` (v2, one file per day) writes a stable object name
    instead of the Adyen filename; do not set it when multiple days upload.
    """
    if not dest_bucket or not dest_prefix:
        raise ValueError("dest_bucket and dest_prefix are required")

    probe_count = sum(len(group) for group in file_name_groups)
    logger.info(
        "%s probing %s candidate names in %s day-groups "
        "(date_range=%s..%s) → gs://%s/%s/",
        mode,
        probe_count,
        len(file_name_groups),
        start,
        end,
        dest_bucket,
        dest_prefix,
    )

    _clear_gcs_prefix(
        bucket=dest_bucket, prefix=dest_prefix, gcp_conn_id=gcp_conn_id
    )
    hook = GCSHook(gcp_conn_id=gcp_conn_id)
    local_dir = f"/tmp/adyen_report_{mode}_{os.getpid()}"
    os.makedirs(local_dir, exist_ok=True)

    source_names: List[str] = []
    objects: List[str] = []
    try:
        for group in file_name_groups:
            downloaded = False
            for file_name in group:
                local_path = os.path.join(local_dir, file_name)
                try:
                    client.download_report_to_file(file_name, local_path)
                except AdyenReportNotFoundError:
                    logger.info("Skipping missing Adyen report file=%s", file_name)
                    continue
                if add_file_name_column:
                    _stamp_local_csv(path=local_path, file_name=file_name)
                object_name = f"{dest_prefix}/{dest_object_name or file_name}"
                hook.upload(
                    bucket_name=dest_bucket,
                    object_name=object_name,
                    filename=local_path,
                )
                source_names.append(file_name)
                objects.append(object_name)
                os.remove(local_path)
                logger.info("Uploaded gs://%s/%s", dest_bucket, object_name)
                downloaded = True
                break
            if not downloaded and group:
                logger.info(
                    "No Adyen report present for candidates=%s", list(group)
                )
    finally:
        for leftover in os.listdir(local_dir) if os.path.isdir(local_dir) else []:
            try:
                os.remove(os.path.join(local_dir, leftover))
            except OSError:
                pass
        try:
            os.rmdir(local_dir)
        except OSError:
            pass

    if not source_names:
        logger.info("No %s report files present for start=%s end=%s", mode, start, end)
        return False

    logger.info(
        "%s uploaded count=%s prefix=gs://%s/%s/",
        mode,
        len(source_names),
        dest_bucket,
        dest_prefix,
    )
    return {
        "mode": mode,
        "downloaded_files": source_names,
        "objects": objects,
        "downloaded_count": len(source_names),
        "dest_bucket": dest_bucket,
        "dest_prefix": dest_prefix,
        "account_name": client.config.account_name,
        "environment": client.config.environment,
    }


def fetch_settlement_date_range_to_gcs(**kwargs: Any) -> Dict[str, Any]:
    """Download each settlement file and upload it to GCS (no merge).

    Daily probe names for ``[start_date, end_date]``. Large settlement days
    are tens of MB; merging May→now in the worker OOMs. Write one object per
    file under ``dest_prefix`` and let BigQuery load ``dest_prefix/*.csv``.
    """
    start = _parse_date(kwargs["start_date"])
    end = _parse_date(kwargs["end_date"])
    if end < start:
        logger.info("settlement date range empty: start=%s end=%s", start, end)
        return False

    dest_bucket = (kwargs.get("dest_bucket") or "").strip()
    dest_prefix = (kwargs.get("dest_prefix") or "").strip().rstrip("/")
    gcp_conn_id = (kwargs.get("gcp_conn_id") or "google_cloud_default").strip()

    client = _build_client(**kwargs)
    file_names = settlement_daily_file_names(start, end)
    extras = [
        str(name).strip()
        for name in (kwargs.get("report_file_names") or [])
        if str(name).strip()
    ]
    seen = set(file_names)
    for name in extras:
        if name not in seen:
            seen.add(name)
            file_names.append(name)

    return _fetch_report_name_groups_to_gcs(
        client=client,
        file_name_groups=[[name] for name in file_names],
        dest_bucket=dest_bucket,
        dest_prefix=dest_prefix,
        gcp_conn_id=gcp_conn_id,
        mode="settlement_date_range_gcs",
        start=start,
        end=end,
        add_file_name_column=bool(kwargs.get("add_file_name_column")),
        dest_object_name=(kwargs.get("dest_object_name") or None),
    )


def fetch_received_payments_date_range_to_gcs(**kwargs: Any) -> Dict[str, Any]:
    """Download daily Received payment details files and upload them to GCS."""
    start = _parse_date(kwargs["start_date"])
    end = _parse_date(kwargs["end_date"])
    if end < start:
        logger.info("received_payments date range empty: start=%s end=%s", start, end)
        return False

    dest_bucket = (kwargs.get("dest_bucket") or "").strip()
    dest_prefix = (kwargs.get("dest_prefix") or "").strip().rstrip("/")
    gcp_conn_id = (kwargs.get("gcp_conn_id") or "google_cloud_default").strip()

    client = _build_client(**kwargs)
    groups = received_payments_daily_file_candidates(start, end)
    extras = [
        [str(name).strip()]
        for name in (kwargs.get("report_file_names") or [])
        if str(name).strip()
    ]
    return _fetch_report_name_groups_to_gcs(
        client=client,
        file_name_groups=[*groups, *extras],
        dest_bucket=dest_bucket,
        dest_prefix=dest_prefix,
        gcp_conn_id=gcp_conn_id,
        mode="received_payments_date_range_gcs",
        start=start,
        end=end,
        add_file_name_column=bool(kwargs.get("add_file_name_column")),
        dest_object_name=(kwargs.get("dest_object_name") or None),
    )


def fetch_reports_full_load(**kwargs: Any) -> Dict[str, Any]:
    """Download / collect all present files and merge to one CSV.

    Modes:
        ``platform_margins_month_range`` — legacy probe of monthly name patterns.
        ``adyen_file_list`` — download each name in ``report_file_names`` from Adyen.
        ``gcs_landing`` — load every ``*.csv`` under ``landing_bucket`` / ``landing_prefix``.

    For settlement full-load use ``fetch_settlement_date_range_to_gcs`` instead
    (per-day GCS objects; no in-worker merge).
    """
    output_path: str = kwargs["output_path"]
    mode = (kwargs.get("mode") or "").strip()

    source_names: List[str] = []
    header_written = False
    account_name: Any = None
    environment: Any = None

    # Fresh merge each run.
    if os.path.exists(output_path):
        os.remove(output_path)

    def _ingest(name: str, text: str) -> None:
        nonlocal header_written
        header_written = _append_csv_text_to_file(
            output_path, text, header_written=header_written
        )
        source_names.append(name)

    if mode == "settlement_date_range":
        # Kept for backwards compatibility; prefer fetch_settlement_date_range_to_gcs.
        client = _build_client(**kwargs)
        start = _parse_date(kwargs["start_date"])
        end = _parse_date(kwargs["end_date"])
        if end < start:
            raise AirflowSkipException(
                f"settlement date range empty: start={start} end={end}"
            )
        file_names = settlement_daily_file_names(start, end)
        logger.info(
            "Settlement full-load (merged) probing %s daily files from %s to %s",
            len(file_names),
            start,
            end,
        )
        for file_name in file_names:
            try:
                _ingest(file_name, client.download_report(file_name))
            except AdyenReportNotFoundError:
                logger.info("Skipping missing Adyen report file=%s", file_name)
        account_name = client.config.account_name
        environment = client.config.environment

    elif mode == "platform_margins_month_range":
        client = _build_client(**kwargs)
        start = _parse_date(kwargs["start_date"])
        end = _parse_date(kwargs["end_date"])
        account = (
            kwargs.get("account_name")
            or client.config.account_name
            or "PartnerPayments"
        )
        file_names = platform_margins_month_file_names(start, end, account)
        extras = [
            str(name).strip()
            for name in (kwargs.get("report_file_names") or [])
            if str(name).strip()
        ]
        seen = set()
        ordered = []
        for name in file_names + extras:
            if name not in seen:
                seen.add(name)
                ordered.append(name)
        for file_name in ordered:
            try:
                _ingest(file_name, client.download_report(file_name))
            except AdyenReportNotFoundError:
                logger.info("Skipping missing Adyen report file=%s", file_name)
        account_name = client.config.account_name
        environment = client.config.environment

    elif mode == "adyen_file_list":
        client = _build_client(**kwargs)
        file_names = [
            str(name).strip()
            for name in (kwargs.get("report_file_names") or [])
            if str(name).strip()
        ]
        if not file_names:
            raise AirflowSkipException(
                "adyen_file_list mode needs report_file_names "
                "(exact CA filenames from View reports)"
            )
        for file_name in file_names:
            try:
                _ingest(file_name, client.download_report(file_name))
            except AdyenReportNotFoundError:
                logger.info("Skipping missing Adyen report file=%s", file_name)
        account_name = client.config.account_name
        environment = client.config.environment

    elif mode == "gcs_landing":
        bucket = (kwargs.get("landing_bucket") or "").strip()
        prefix = (
            kwargs.get("landing_prefix") or DEFAULT_PLATFORM_MARGINS_LANDING_PREFIX
        ).strip()
        gcp_conn_id = (kwargs.get("gcp_conn_id") or "google_cloud_default").strip()
        if not bucket:
            raise AirflowSkipException("landing_bucket is required for gcs_landing")
        loaded = list_gcs_csv_texts(
            bucket=bucket, prefix=prefix, gcp_conn_id=gcp_conn_id
        )
        for object_name, text in loaded:
            _ingest(object_name, text)
        account_name = kwargs.get("account_name")
        environment = None

    else:
        raise ValueError(f"Unsupported full-load mode: {mode!r}")

    if not source_names:
        raise AirflowSkipException(
            f"No report files present for mode={mode} sources_tried_or_listed"
        )

    logger.info(
        "Full-load saved merged CSV path=%s files=%s count=%s",
        output_path,
        source_names,
        len(source_names),
    )
    return {
        "mode": mode,
        "output_path": output_path,
        "downloaded_files": source_names,
        "downloaded_count": len(source_names),
        "account_name": account_name,
        "environment": environment,
    }
