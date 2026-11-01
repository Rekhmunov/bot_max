from __future__ import annotations

import smtplib
from email.message import EmailMessage

from app.config import settings


def _smtp_is_configured() -> bool:
    return bool((settings.smtp_host or "").strip() and int(settings.smtp_port or 0) > 0)


def send_email_verification(to_email: str, verify_link: str) -> tuple[bool, str]:
    """
    Sends an email verification message.
    Returns (ok, error_message). On success error_message is empty.
    """
    recipient = (to_email or "").strip()
    if not recipient:
        return False, "email_empty"
    if not _smtp_is_configured():
        return False, "smtp_not_configured"

    sender = (settings.smtp_sender or "").strip() or (settings.smtp_username or "").strip()
    if not sender:
        return False, "smtp_sender_not_configured"

    message = EmailMessage()
    message["Subject"] = "Подтверждение email в FeedPilot"
    message["From"] = sender
    message["To"] = recipient
    message.set_content(
        (
            "Здравствуйте!\n\n"
            "Чтобы подтвердить email в FeedPilot, перейдите по ссылке:\n"
            f"{verify_link}\n\n"
            "Если вы не регистрировались, просто проигнорируйте это письмо."
        )
    )

    host = (settings.smtp_host or "").strip()
    port = int(settings.smtp_port or 0)
    username = (settings.smtp_username or "").strip()
    password = settings.smtp_password or ""
    timeout = max(int(settings.smtp_timeout_seconds or 15), 3)

    try:
        if bool(settings.smtp_use_ssl):
            with smtplib.SMTP_SSL(host=host, port=port, timeout=timeout) as smtp:
                if username:
                    smtp.login(username, password)
                smtp.send_message(message)
        else:
            with smtplib.SMTP(host=host, port=port, timeout=timeout) as smtp:
                smtp.ehlo()
                if bool(settings.smtp_use_tls):
                    smtp.starttls()
                    smtp.ehlo()
                if username:
                    smtp.login(username, password)
                smtp.send_message(message)
    except Exception as exc:  # pragma: no cover - external SMTP failures are environment dependent
        return False, f"smtp_send_failed:{exc}"
    return True, ""

