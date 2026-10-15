"""Send HTML emails with optional attachments via SendGrid or SMTP."""

from __future__ import annotations

import base64
import logging
import os
import smtplib
import ssl
from dataclasses import dataclass
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formataddr, formatdate
from pathlib import Path
from typing import Sequence

log = logging.getLogger(__name__)

_XLSX_MIME = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"


def _env(key: str, required: bool = True) -> str:
    value = os.getenv(key, "")
    if required and not value:
        raise EnvironmentError(f"Required environment variable '{key}' is not set.")
    return value


@dataclass(frozen=True)
class EmailMessage:
    """Payload for a single outbound email."""

    recipients: list[str]
    subject: str
    html_body: str
    cc: list[str] | None = None
    attachment_path: str | None = None
    plain_text_summary: str | None = None


@dataclass(frozen=True)
class _DeliveryConfig:
    provider: str
    sender: str
    sender_name: str
    smtp_host: str
    smtp_port: int
    smtp_use_tls: bool
    smtp_user: str
    smtp_password: str
    smtp_timeout: int
    sendgrid_api_key: str

    @property
    def from_address(self) -> str:
        return formataddr((self.sender_name, self.sender))

    @classmethod
    def from_env(cls) -> "_DeliveryConfig":
        provider = os.getenv("EMAIL_PROVIDER", "smtp").lower()
        sender = _env("EMAIL_FROM")
        sender_name = os.getenv("EMAIL_FROM_NAME", "Platform Reports")
        if provider == "sendgrid":
            return cls(
                provider=provider,
                sender=sender,
                sender_name=sender_name,
                smtp_host="",
                smtp_port=0,
                smtp_use_tls=False,
                smtp_user="",
                smtp_password="",
                smtp_timeout=0,
                sendgrid_api_key=_env("SENDGRID_API_KEY"),
            )
        return cls(
            provider=provider,
            sender=sender,
            sender_name=sender_name,
            smtp_host=_env("SMTP_HOST"),
            smtp_port=int(_env("SMTP_PORT")),
            smtp_use_tls=_env("SMTP_USE_TLS").lower() in ("1", "true", "yes"),
            smtp_user=_env("SMTP_USER"),
            smtp_password=_env("SMTP_PASSWORD"),
            smtp_timeout=int(os.getenv("SMTP_TIMEOUT", "30")),
            sendgrid_api_key="",
        )


class EmailDelivery:
    """Send emails from environment configuration (SendGrid or SMTP)."""

    def __init__(self, config: _DeliveryConfig | None = None) -> None:
        self._config = config or _DeliveryConfig.from_env()

    @classmethod
    def from_env(cls) -> "EmailDelivery":
        return cls(_DeliveryConfig.from_env())

    def send(self, message: EmailMessage) -> None:
        attachment_bytes, attachment_name = self._load_attachment(message.attachment_path)
        if self._config.provider == "sendgrid":
            self._send_sendgrid(message, attachment_bytes, attachment_name)
        else:
            self._send_smtp(message, attachment_bytes, attachment_name)

    @staticmethod
    def _load_attachment(
        attachment_path: str | None,
    ) -> tuple[bytes | None, str | None]:
        if not attachment_path:
            return None, None
        path = Path(attachment_path)
        if not path.is_file():
            raise FileNotFoundError(f"Attachment not found: {attachment_path}")
        return path.read_bytes(), path.name

    def _send_sendgrid(
        self,
        message: EmailMessage,
        attachment_bytes: bytes | None,
        attachment_filename: str | None,
    ) -> None:
        try:
            from sendgrid import SendGridAPIClient
            from sendgrid.helpers.mail import (
                Attachment,
                Cc,
                Disposition,
                Email,
                FileContent,
                FileName,
                FileType,
                Mail,
            )
        except ImportError as exc:
            raise RuntimeError(
                "EMAIL_PROVIDER=sendgrid but the sendgrid package is not installed."
            ) from exc

        mail = Mail(
            from_email=Email(self._config.sender, self._config.sender_name),
            to_emails=message.recipients,
            subject=message.subject,
            html_content=message.html_body,
        )
        if message.plain_text_summary:
            mail.plain_text_content = message.plain_text_summary
        for addr in message.cc or []:
            mail.add_cc(Cc(addr))
        if attachment_bytes is not None and attachment_filename is not None:
            mail.add_attachment(
                Attachment(
                    file_content=FileContent(base64.b64encode(attachment_bytes).decode()),
                    file_name=FileName(attachment_filename),
                    file_type=FileType(_XLSX_MIME),
                    disposition=Disposition("attachment"),
                )
            )

        log.info(
            "Sending '%s' to %s%s via SendGrid%s",
            message.subject,
            message.recipients,
            f" (CC: {message.cc})" if message.cc else "",
            f" (attachment: {attachment_filename})" if attachment_filename else "",
        )
        try:
            response = SendGridAPIClient(self._config.sendgrid_api_key).send(mail)
        except Exception as exc:
            raise RuntimeError(f"SendGrid error while sending: {exc}") from exc
        if response.status_code not in (200, 201, 202):
            raise RuntimeError(
                f"SendGrid returned unexpected status {response.status_code}: {response.body}"
            )
        log.info("  → Sent successfully (SendGrid %s).", response.status_code)

    def _send_smtp(
        self,
        message: EmailMessage,
        attachment_bytes: bytes | None,
        attachment_filename: str | None,
    ) -> None:
        cfg = self._config
        msg = MIMEMultipart("related")
        msg["From"] = self._config.from_address
        msg["To"] = ", ".join(message.recipients)
        if message.cc:
            msg["Cc"] = ", ".join(message.cc)
        msg["Subject"] = message.subject
        msg["Date"] = formatdate(localtime=True)

        alt = MIMEMultipart("alternative")
        plain = message.plain_text_summary or (
            "Please open this email in an HTML-capable client to view the report."
        )
        alt.attach(MIMEText(plain, "plain", "utf-8"))
        alt.attach(MIMEText(message.html_body, "html", "utf-8"))
        msg.attach(alt)

        if attachment_bytes is not None and attachment_filename is not None:
            part = MIMEApplication(attachment_bytes, Name=attachment_filename)
            part["Content-Disposition"] = f'attachment; filename="{attachment_filename}"'
            msg.attach(part)

        all_recipients: Sequence[str] = list(message.recipients) + list(message.cc or [])
        context = ssl.create_default_context()
        log.info(
            "Sending '%s' to %s%s via %s:%d (timeout=%ds)%s",
            message.subject,
            message.recipients,
            f" (CC: {message.cc})" if message.cc else "",
            cfg.smtp_host,
            cfg.smtp_port,
            cfg.smtp_timeout,
            f" (attachment: {attachment_filename})" if attachment_filename else "",
        )
        try:
            if cfg.smtp_use_tls:
                with smtplib.SMTP(cfg.smtp_host, cfg.smtp_port, timeout=cfg.smtp_timeout) as server:
                    server.ehlo()
                    server.starttls(context=context)
                    server.ehlo()
                    server.login(cfg.smtp_user, cfg.smtp_password)
                    server.sendmail(cfg.sender, all_recipients, msg.as_bytes())
            else:
                with smtplib.SMTP_SSL(
                    cfg.smtp_host, cfg.smtp_port, context=context, timeout=cfg.smtp_timeout
                ) as server:
                    server.login(cfg.smtp_user, cfg.smtp_password)
                    server.sendmail(cfg.sender, all_recipients, msg.as_bytes())
        except TimeoutError as exc:
            raise RuntimeError(
                f"SMTP connection to {cfg.smtp_host}:{cfg.smtp_port} timed out after "
                f"{cfg.smtp_timeout}s."
            ) from exc
        except smtplib.SMTPAuthenticationError as exc:
            raise RuntimeError(f"SMTP authentication failed for user '{cfg.smtp_user}'.") from exc
        except smtplib.SMTPException as exc:
            raise RuntimeError(f"SMTP error while sending: {exc}") from exc
        log.info("  → Sent successfully.")

    def send_batch(self, messages: Sequence[EmailMessage]) -> None:
        for item in messages:
            self.send(item)
