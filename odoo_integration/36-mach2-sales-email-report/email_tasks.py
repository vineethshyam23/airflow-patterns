"""Send staged Mach2 reports using the shared email delivery module."""

from __future__ import annotations

import logging

from email_delivery import EmailDelivery, EmailMessage
from config import load_mach2_environment

log = logging.getLogger(__name__)

GENERATE_TASK_ID = "generate_mach2_reports"


def send_mach2_report_emails(**context) -> None:
    """Airflow callable: read XCom payloads and send each report email."""
    load_mach2_environment()
    ti = context["ti"]
    payloads = ti.xcom_pull(task_ids=GENERATE_TASK_ID) or []

    if not payloads:
        log.warning("No report payloads received from %s — nothing to send.", GENERATE_TASK_ID)
        return

    delivery = EmailDelivery.from_env()
    for payload in payloads:
        report_name = payload.get("report_name", "report")
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
            log.exception("Failed to send email for report '%s'.", report_name)
            continue

    log.info("All report emails processed.")
