"""Send the staged LPV report using the shared email delivery module."""

from __future__ import annotations

import logging
from pathlib import Path

from airflow.models import Variable
from google.cloud import storage

from config import load_lpv_environment
from email_delivery import EmailDelivery, EmailMessage

log = logging.getLogger(__name__)

GENERATE_TASK_ID = "generate_lpv_reports"
# SendGrid rejects messages over ~30 MB; base64 adds ~33%, so keep raw
# attachments under 20 MB and offload larger workbooks to GCS.
MAX_ATTACHMENT_BYTES = 20 * 1024 * 1024
GCS_PREFIX = "lpv_report"


def _maybe_offload_attachment(payload: dict) -> dict:
    """Upload oversized zips to GCS and drop the email attachment."""
    path_str = payload.get("attachment_path")
    if not path_str:
        return payload

    path = Path(path_str)
    if not path.is_file():
        return payload

    size = path.stat().st_size
    log.info("LPV workbook size: %.1f MB", size / (1024 * 1024))
    if size <= MAX_ATTACHMENT_BYTES:
        return payload

    bucket_name = Variable.get("composer_bucket")
    blob_name = f"{GCS_PREFIX}/{path.name}"
    storage.Client().bucket(bucket_name).blob(blob_name).upload_from_filename(path)
    gcs_uri = f"gs://{bucket_name}/{blob_name}"
    log.warning(
        "Zip exceeds SendGrid attachment limit (%.1f MB). Uploaded to %s",
        size / (1024 * 1024),
        gcs_uri,
    )

    note = (
        f"<p><strong>Attachment too large for email ({size / (1024 * 1024):.1f} MB).</strong> "
        f"Download the report from <code>{gcs_uri}</code> "
        f"(path <code>{blob_name}</code> in the Composer bucket).</p>"
    )
    updated = dict(payload)
    updated["attachment_path"] = None
    updated["html_body"] = payload["html_body"].replace("</body>", f"{note}</body>", 1)
    updated["plain_text_summary"] = (
        f"{payload.get('plain_text_summary', '')} File location: {gcs_uri}"
    ).strip()
    return updated


def send_lpv_report_emails(**context) -> None:
    """Airflow callable: read XCom payloads and send the report email."""
    load_lpv_environment()
    ti = context["ti"]
    payloads = ti.xcom_pull(task_ids=GENERATE_TASK_ID) or []

    if not payloads:
        log.warning("No report payloads received from %s — nothing to send.", GENERATE_TASK_ID)
        return

    delivery = EmailDelivery.from_env()
    failures = 0
    for payload in payloads:
        report_name = payload.get("report_name", "report")
        payload = _maybe_offload_attachment(payload)
        try:
            delivery.send(
                EmailMessage(
                    recipients=payload["recipients"],
                    subject=payload["subject"],
                    html_body=payload["html_body"],
                    cc=payload.get("cc") or None,
                    attachment_path=payload.get("attachment_path"),
                    plain_text_summary=payload.get("plain_text_summary"),
                )
            )
        except Exception:
            failures += 1
            log.exception("Failed to send email for report '%s'.", report_name)

    if failures:
        raise RuntimeError(f"{failures} LPV report email(s) failed to send.")

    log.info("All report emails processed.")
