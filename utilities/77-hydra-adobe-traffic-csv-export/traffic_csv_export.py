"""Hydra / Adobe site-traffic CSV publish helpers.

Reads refined BigQuery metric tables for a rolling date window, writes
CSV to a Composer staging prefix, then publishes to a consumer bucket
with delete-then-copy overwrite and 60-day retention cleanup.
"""

from __future__ import annotations

import logging
import os
import tempfile
from datetime import datetime, timedelta
from typing import Any

import pandas as pd
from airflow.models import Variable
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook

logger = logging.getLogger(__name__)

BQ_CONN_ID = "google_cloud_default"

# Defaults are sanitized; override via Airflow Variables in real envs.
SOURCE_BQ_PROJECT_ID = Variable.get("dwh_project_id", default_var="dwh_project")
SOURCE_BQ_DATASET = Variable.get("refined_dataset", default_var="refined")
SOURCE_BUCKET = Variable.get("composer_bucket", default_var="composer-bucket")
SOURCE_FOLDER = Variable.get("site_traffic_staging_folder", default_var="data/site-traffic")
TARGET_BUCKET = Variable.get(
    "site_traffic_publish_bucket",
    default_var="refinedzone-site-traffic",
)

TABLE_LIST = [
    "site_traffic_mailphoneclicks",
    "site_traffic_openingtimes",
    "site_traffic_traffic",
    "site_traffic_visitsbydevice",
    "site_traffic_visitsbylocale",
]

FILE_NAME_MAPPING = {
    "site_traffic_mailphoneclicks": "mailphoneclicks",
    "site_traffic_openingtimes": "openingtimes",
    "site_traffic_traffic": "traffic",
    "site_traffic_visitsbydevice": "visitsbydevice",
    "site_traffic_visitsbylocale": "visitsbylocale",
}

# Rolling window: day-before-yesterday through yesterday.
# Computed at import time in production — prefer logical_date / ds in a rewrite.
START_DATE = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")
END_DATE = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

DEVICE_MAPPING = {
    "tv": "Television",
    "console": "Gaming Console",
    "tablet": "Tablet",
    "mobile": "Mobile Phone",
}

RETENTION_DAYS = 60


def get_date_range() -> list[str]:
    """Inclusive list of YYYY-MM-DD dates between START_DATE and END_DATE."""
    start = datetime.strptime(START_DATE, "%Y-%m-%d")
    end = datetime.strptime(END_DATE, "%Y-%m-%d")

    dates: list[str] = []
    current = start
    while current <= end:
        dates.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)
    return dates


def extract_table_data_for_date(table_name: str, date: str, **_context: Any) -> dict[str, Any]:
    """Query one refined table for one date, write CSV to Composer staging."""
    logger.info("Processing table %s for date %s", table_name, date)

    bq_hook = BigQueryHook(gcp_conn_id=BQ_CONN_ID, use_legacy_sql=False)
    gcs_hook = GCSHook(gcp_conn_id=BQ_CONN_ID)

    try:
        query = f"""
        SELECT *
        FROM `{SOURCE_BQ_PROJECT_ID}.{SOURCE_BQ_DATASET}.{table_name}`
        WHERE DATE(date) = '{date}'
        """
        logger.info("Executing query for %s / %s", table_name, date)

        df = bq_hook.get_pandas_df(sql=query)

        if df.empty:
            logger.warning("No data for table %s on date %s", table_name, date)
            return {"status": "no_data", "table": table_name, "date": date}

        logger.info("Retrieved %s rows for %s on %s", len(df), table_name, date)

        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"])
            # Consumer expects d-m-yyyy without zero-padding (filenames stay ISO).
            df["date"] = df["date"].dt.strftime("%-d-%-m-%Y")
        else:
            logger.warning("Date column not found in table %s", table_name)

        if table_name == "site_traffic_visitsbydevice" and "deviceType" in df.columns:
            lower = df["deviceType"].astype(str).str.lower()
            df["deviceType"] = lower.map(DEVICE_MAPPING).fillna("Other")
            logger.info(
                "Device types after mapping: %s",
                df["deviceType"].value_counts().to_dict(),
            )
        elif table_name == "site_traffic_visitsbydevice":
            logger.warning("deviceType column not found in table %s", table_name)

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp_file:
            csv_path = tmp_file.name

        df.to_csv(csv_path, index=False)

        file_name = FILE_NAME_MAPPING.get(table_name, table_name)
        csv_filename = f"{file_name}_{date}.csv"
        gcs_path = f"{SOURCE_FOLDER}/{csv_filename}"

        gcs_hook.upload(
            bucket_name=SOURCE_BUCKET,
            object_name=gcs_path,
            filename=csv_path,
        )
        os.unlink(csv_path)

        logger.info(
            "Uploaded %s to gs://%s/%s",
            csv_filename,
            SOURCE_BUCKET,
            gcs_path,
        )
        return {
            "status": "success",
            "table": table_name,
            "date": date,
            "rows": len(df),
            "file": csv_filename,
        }

    except Exception as exc:  # noqa: BLE001 — per-table errors must not abort the loop
        logger.error("Error processing %s for %s: %s", table_name, date, exc)
        return {
            "status": "error",
            "table": table_name,
            "date": date,
            "error": str(exc),
        }


def process_all_tables_and_dates(**context: Any) -> dict[str, Any]:
    """Sequential extract for every table × date in the window."""
    dates = get_date_range()
    logger.info(
        "Processing %s tables for %s dates (%s → %s)",
        len(TABLE_LIST),
        len(dates),
        START_DATE,
        END_DATE,
    )

    results: dict[str, Any] = {
        "success": 0,
        "no_data": 0,
        "errors": 0,
        "details": [],
    }

    for table_name in TABLE_LIST:
        for date in dates:
            result = extract_table_data_for_date(table_name, date, **context)
            results["details"].append(result)
            status = result["status"]
            if status == "success":
                results["success"] += 1
            elif status == "no_data":
                results["no_data"] += 1
            else:
                results["errors"] += 1

    logger.info(
        "Extract complete: %s success, %s no_data, %s errors",
        results["success"],
        results["no_data"],
        results["errors"],
    )
    return results


def delete_target_files_for_date_range(**_context: Any) -> dict[str, Any]:
    """Idempotent overwrite: remove target objects for this date window first."""
    gcs_hook = GCSHook(gcp_conn_id=BQ_CONN_ID)
    dates = get_date_range()

    date_patterns: list[str] = []
    for date in dates:
        date_obj = datetime.strptime(date, "%Y-%m-%d")
        date_patterns.append(date_obj.strftime("%Y-%m-%d"))
        date_patterns.append(date_obj.strftime("%Y-%-m-%-d"))
    date_patterns = list(dict.fromkeys(date_patterns))

    delete_results: dict[str, Any] = {
        "total_files_deleted": 0,
        "total_files_failed": 0,
        "bucket": TARGET_BUCKET,
        "files_processed": [],
        "errors": [],
        "date_range": f"{START_DATE} to {END_DATE}",
        "date_patterns_checked": date_patterns,
    }

    logger.info(
        "Deleting target files for %s → %s in gs://%s",
        START_DATE,
        END_DATE,
        TARGET_BUCKET,
    )

    try:
        objects = gcs_hook.list(bucket_name=TARGET_BUCKET)
        if not objects:
            logger.info("No files in target bucket %s", TARGET_BUCKET)
            return delete_results

        files_to_delete: list[str] = []
        for obj_name in objects:
            for table_name in TABLE_LIST:
                prefix = FILE_NAME_MAPPING.get(table_name, table_name)
                for date_pattern in date_patterns:
                    expected = f"{prefix}_{date_pattern}.csv"
                    if obj_name == expected:
                        files_to_delete.append(obj_name)
                        break
                if obj_name in files_to_delete:
                    break

        files_to_delete = list(set(files_to_delete))
        logger.info("Marked %s target files for deletion", len(files_to_delete))

        for file_name in files_to_delete:
            try:
                gcs_hook.delete(bucket_name=TARGET_BUCKET, object_name=file_name)
                delete_results["files_processed"].append(
                    {"file": file_name, "status": "deleted"}
                )
                delete_results["total_files_deleted"] += 1
            except Exception as exc:  # noqa: BLE001
                delete_results["files_processed"].append(
                    {"file": file_name, "status": "error", "error": str(exc)}
                )
                delete_results["errors"].append(f"{file_name}: {exc}")
                delete_results["total_files_failed"] += 1
                logger.error("Failed to delete %s: %s", file_name, exc)

    except Exception as exc:  # noqa: BLE001
        delete_results["errors"].append(f"Bucket listing error: {exc}")
        logger.error("Error listing target bucket: %s", exc)

    return delete_results


def copy_files_to_target_bucket(**context: Any) -> dict[str, Any]:
    """Copy successful staging CSVs to the publish bucket root."""
    extract_results = context["task_instance"].xcom_pull(
        task_ids="extract_and_upload_csv_files"
    )
    if not extract_results:
        logger.error("No extract results from extract_and_upload_csv_files")
        return {"status": "error", "message": "No extract results found"}

    gcs_hook = GCSHook(gcp_conn_id=BQ_CONN_ID)
    successful = [
        detail for detail in extract_results["details"] if detail["status"] == "success"
    ]

    copy_results: dict[str, Any] = {
        "total_files": len(successful),
        "successful_copies": 0,
        "failed_copies": 0,
        "details": [],
        "bucket": TARGET_BUCKET,
        "files": [],
    }

    logger.info(
        "Copying %s files to gs://%s/",
        len(successful),
        TARGET_BUCKET,
    )

    for file_detail in successful:
        source_file = file_detail["file"]
        source_path = f"{SOURCE_FOLDER}/{source_file}"
        target_path = source_file

        try:
            gcs_hook.copy(
                source_bucket=SOURCE_BUCKET,
                source_object=source_path,
                destination_bucket=TARGET_BUCKET,
                destination_object=target_path,
            )
            copy_results["files"].append(
                {
                    "file": source_file,
                    "status": "success",
                    "source": f"gs://{SOURCE_BUCKET}/{source_path}",
                    "destination": f"gs://{TARGET_BUCKET}/{target_path}",
                }
            )
            copy_results["successful_copies"] += 1
        except Exception as exc:  # noqa: BLE001
            copy_results["files"].append(
                {
                    "file": source_file,
                    "status": "error",
                    "error": str(exc),
                    "source": f"gs://{SOURCE_BUCKET}/{source_path}",
                    "destination": f"gs://{TARGET_BUCKET}/{target_path}",
                }
            )
            copy_results["failed_copies"] += 1
            logger.error("Failed to copy %s: %s", source_file, exc)

        copy_results["details"].append(
            {
                "file": source_file,
                "source_bucket": SOURCE_BUCKET,
                "target_bucket": TARGET_BUCKET,
            }
        )

    total = copy_results["total_files"]
    copy_results["success_rate"] = (
        (copy_results["successful_copies"] / total * 100) if total else 0
    )
    return copy_results


def generate_copy_summary_report(**context: Any) -> dict[str, Any] | None:
    """Log copy outcome for on-call (XCom summary also returned)."""
    copy_results = context["task_instance"].xcom_pull(task_ids="copy_files_to_targets")
    if not copy_results:
        logger.error("No copy results from copy_files_to_targets")
        return None

    logger.info("FILE COPY SUMMARY — execution_date=%s", context["ds"])
    logger.info(
        "bucket=%s total=%s ok=%s failed=%s rate=%.1f%%",
        copy_results["bucket"],
        copy_results["total_files"],
        copy_results["successful_copies"],
        copy_results["failed_copies"],
        copy_results["success_rate"],
    )
    return {
        "execution_date": context["ds"],
        "total_files": copy_results["total_files"],
        "successful_copies": copy_results["successful_copies"],
        "failed_copies": copy_results["failed_copies"],
        "success_rate": copy_results["success_rate"],
        "target_bucket": copy_results["bucket"],
    }


def generate_summary_report(**context: Any) -> dict[str, Any] | None:
    """Log extract outcome for on-call."""
    results = context["task_instance"].xcom_pull(
        task_ids="extract_and_upload_csv_files"
    )
    if not results:
        logger.error("No extract results from extract_and_upload_csv_files")
        return None

    dates = get_date_range()
    total_expected = len(TABLE_LIST) * len(dates)
    success_rate = (results["success"] / total_expected * 100) if total_expected else 0

    logger.info("EXTRACT SUMMARY — execution_date=%s", context["ds"])
    logger.info(
        "range=%s→%s expected=%s success=%s no_data=%s errors=%s rate=%.1f%%",
        START_DATE,
        END_DATE,
        total_expected,
        results["success"],
        results["no_data"],
        results["errors"],
        success_rate,
    )

    return {
        "execution_date": context["ds"],
        "date_range": f"{START_DATE} to {END_DATE}",
        "total_expected": total_expected,
        "success_count": results["success"],
        "no_data_count": results["no_data"],
        "error_count": results["errors"],
        "success_rate": success_rate,
        "files_created": results["success"],
    }


def _parse_filename_date(date_part: str) -> datetime | None:
    """Parse YYYY-MM-DD or YYYY-M-D from a published CSV stem."""
    try:
        return datetime.strptime(date_part, "%Y-%m-%d")
    except ValueError:
        pass
    parts = date_part.split("-")
    if len(parts) == 3:
        try:
            return datetime(int(parts[0]), int(parts[1]), int(parts[2]))
        except ValueError:
            return None
    return None


def delete_old_files_from_target_bucket(**_context: Any) -> dict[str, Any]:
    """Retention: drop published CSVs older than RETENTION_DAYS before END_DATE."""
    gcs_hook = GCSHook(gcp_conn_id=BQ_CONN_ID)
    end_date_obj = datetime.strptime(END_DATE, "%Y-%m-%d")
    cutoff_date = end_date_obj - timedelta(days=RETENTION_DAYS)

    delete_results: dict[str, Any] = {
        "total_files_found": 0,
        "files_deleted": 0,
        "files_failed": 0,
        "cutoff_date": cutoff_date.strftime("%Y-%m-%d"),
        "bucket": TARGET_BUCKET,
        "files_processed": [],
        "errors": [],
    }

    logger.info(
        "Retention cleanup: delete files older than %s from gs://%s",
        cutoff_date.strftime("%Y-%m-%d"),
        TARGET_BUCKET,
    )

    try:
        objects = gcs_hook.list(bucket_name=TARGET_BUCKET)
        if not objects:
            return delete_results

        delete_results["total_files_found"] = len(objects)
        files_to_delete: list[dict[str, str]] = []

        for obj_name in objects:
            for table_name in TABLE_LIST:
                prefix = FILE_NAME_MAPPING.get(table_name, table_name)
                if not (
                    obj_name.startswith(f"{prefix}_") and obj_name.endswith(".csv")
                ):
                    continue
                date_part = obj_name.replace(f"{prefix}_", "").replace(".csv", "")
                file_date = _parse_filename_date(date_part)
                if file_date and file_date < cutoff_date:
                    files_to_delete.append(
                        {
                            "filename": obj_name,
                            "file_date": file_date.strftime("%Y-%m-%d"),
                            "table": table_name,
                        }
                    )
                    break

        logger.info("Retention marked %s files", len(files_to_delete))

        for file_info in files_to_delete:
            filename = file_info["filename"]
            try:
                gcs_hook.delete(bucket_name=TARGET_BUCKET, object_name=filename)
                delete_results["files_processed"].append(
                    {
                        "file": filename,
                        "date": file_info["file_date"],
                        "table": file_info["table"],
                        "status": "deleted",
                    }
                )
                delete_results["files_deleted"] += 1
            except Exception as exc:  # noqa: BLE001
                delete_results["files_processed"].append(
                    {
                        "file": filename,
                        "date": file_info["file_date"],
                        "table": file_info["table"],
                        "status": "error",
                        "error": str(exc),
                    }
                )
                delete_results["errors"].append(f"{filename}: {exc}")
                delete_results["files_failed"] += 1
                logger.error("Failed retention delete %s: %s", filename, exc)

    except Exception as exc:  # noqa: BLE001
        delete_results["errors"].append(f"Bucket listing error: {exc}")
        logger.error("Error during retention cleanup: %s", exc)

    return delete_results


def cleanup_source_files(**context: Any) -> dict[str, Any]:
    """Remove staging CSVs only after a successful publish copy."""
    copy_results = context["task_instance"].xcom_pull(task_ids="copy_files_to_targets")
    if not copy_results:
        logger.error("No copy results — skipping staging cleanup")
        return {"status": "error", "message": "No copy results found"}

    gcs_hook = GCSHook(gcp_conn_id=BQ_CONN_ID)
    cleanup_results: dict[str, Any] = {
        "total_files": 0,
        "successfully_deleted": 0,
        "failed_deletions": 0,
        "skipped_deletions": 0,
        "details": [],
    }

    files_to_delete: list[str] = []
    for file_detail in copy_results.get("details", []):
        file_name = file_detail["file"]
        ok = any(
            f["file"] == file_name and f["status"] == "success"
            for f in copy_results["files"]
        )
        if ok:
            files_to_delete.append(file_name)
        else:
            cleanup_results["skipped_deletions"] += 1
            cleanup_results["details"].append(
                {
                    "file": file_name,
                    "status": "skipped",
                    "reason": "Not successfully copied to target bucket",
                }
            )

    cleanup_results["total_files"] = len(files_to_delete)

    for file_name in files_to_delete:
        source_path = f"{SOURCE_FOLDER}/{file_name}"
        try:
            gcs_hook.delete(bucket_name=SOURCE_BUCKET, object_name=source_path)
            cleanup_results["successfully_deleted"] += 1
            cleanup_results["details"].append(
                {
                    "file": file_name,
                    "status": "deleted",
                    "source_path": f"gs://{SOURCE_BUCKET}/{source_path}",
                }
            )
        except Exception as exc:  # noqa: BLE001
            cleanup_results["failed_deletions"] += 1
            cleanup_results["details"].append(
                {
                    "file": file_name,
                    "status": "error",
                    "error": str(exc),
                    "source_path": f"gs://{SOURCE_BUCKET}/{source_path}",
                }
            )
            logger.error("Failed staging cleanup %s: %s", file_name, exc)

    total = cleanup_results["total_files"]
    cleanup_results["success_rate"] = (
        (cleanup_results["successfully_deleted"] / total * 100) if total else 0
    )
    return cleanup_results
