from __future__ import annotations

import base64
import hashlib
import json
import re
import logging
import httpx
from datetime import UTC, date, datetime, timedelta
from dataclasses import dataclass
from typing import Optional
import asyncio
from pathlib import Path
from urllib.parse import quote_plus, urlsplit
from uuid import uuid4
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from sqlalchemy import func
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.auth import create_manager_mini_token
from app.config import settings as app_settings
from app.max_client import MaxClient
from app.storage import (
    delete_by_public_url,
    iter_local_upload_files,
    local_upload_abspath,
    normalize_storage_public_url,
    save_upload_bytes,
    storage_public_url_for_key,
    upload_file_public_url,
)
from app.models import (
    AuditLog,
    BotSettings,
    ChatMessage,
    ChatFolder,
    ChatMessageMedia,
    Conversation,
    ConversationFolderLink,
    ConversationPin,
    ConversationMeta,
    CustomerProfile,
    IntroStep,
    ManagerDispatch,
    MessageLog,
    MessageTemplate,
    OutboxMessage,
    QuickReply,
    QuickReplyMedia,
    QuickReplyMediaAssetLink,
    ServiceUser,
    MediaAsset,
    Workspace,
    StorageCleanupRun,
    WorkspaceRetentionPolicy,
    WorkspaceBusinessException,
    WorkspaceBusinessHours,
    WorkspaceBusinessSlot,
)
from app.schemas import MaxWebhookEvent
from app.services import get_or_create_settings
from app.ops import (
    can_create_dialog,
    can_send_message_this_month,
    ensure_workspace_limits_and_state,
    track_message_sent,
)
from app.services import DEFAULT_WORKSPACE_ID

TEMPLATE_PRESTART = "prestart_message"
TEMPLATE_START = "start_message"
TEMPLATE_AFTER_PHONE = "after_phone_message"
MANAGER_HELP_TEXT = (
    "Чат менеджера отключен для сквозных сообщений.\n"
    "Работа ведется только через mini-app.\n"
    "Открыть mini-app: /mini"
)


DEFAULT_TEMPLATES: dict[str, str] = {
    TEMPLATE_PRESTART: (
        "Здравствуйте! Это чат-бот поддержки.\n"
        "Чтобы начать, нажмите Start/Начать."
    ),
    TEMPLATE_START: (
        "Здравствуйте! Сейчас позову менеджера.\n"
        "Чтобы подтвердить, что общаемся не с мошенником, "
        "нажмите кнопку \"Поделиться номером\"."
    ),
    TEMPLATE_AFTER_PHONE: (
        "Спасибо! Номер подтвержден.\n"
        "Пожалуйста, опишите, какой именно товар хотите с кэшбэком. "
        "Менеджер скоро ответит."
    ),
}

OUTBOX_RETRY_BACKOFF_SECONDS = (5, 20, 60, 300)
BLOCKED_FOLDER_NAME = "Заблокированные пользователи"
BLOCKED_NOTICE_COOLDOWN_SECONDS = 30
DEFAULT_OFFHOURS_MESSAGE = "Сейчас мы вне рабочего времени. Мы ответим в рабочие часы."
DEFAULT_OFFHOURS_COOLDOWN_SECONDS = 6 * 60 * 60
DEFAULT_BUSINESS_TIMEZONE = "UTC"

logger = logging.getLogger(__name__)


def _workspace_client(db: Session, *, workspace_id: int) -> MaxClient:
    settings_row = db.query(BotSettings).filter(BotSettings.workspace_id == workspace_id).first()
    token = (settings_row.bot_token if settings_row is not None else "") or ""
    return MaxClient(token=token.strip())


def _normalize_timezone_name(value: str) -> str:
    tz_name = (value or "").strip() or DEFAULT_BUSINESS_TIMEZONE
    try:
        ZoneInfo(tz_name)
        return tz_name
    except ZoneInfoNotFoundError:
        return DEFAULT_BUSINESS_TIMEZONE


def _minute_of_day(value: datetime) -> int:
    return int(value.hour) * 60 + int(value.minute)


def _minutes_to_hhmm(minutes: int) -> str:
    clamped = max(0, min(24 * 60, int(minutes)))
    if clamped == 24 * 60:
        return "24:00"
    return f"{clamped // 60:02d}:{clamped % 60:02d}"


def _weekday_label(weekday: int) -> str:
    labels = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
    idx = int(weekday) if isinstance(weekday, int) else -1
    return labels[idx] if 0 <= idx < len(labels) else "—"


def _merge_slots(slots: list[tuple[int, int]]) -> list[tuple[int, int]]:
    if not slots:
        return []
    ordered = sorted(slots, key=lambda item: (int(item[0]), int(item[1])))
    merged: list[list[int]] = []
    for start, end in ordered:
        start_i = max(0, min(24 * 60 - 1, int(start)))
        end_i = max(start_i + 1, min(24 * 60, int(end)))
        if not merged or start_i > merged[-1][1]:
            merged.append([start_i, end_i])
            continue
        merged[-1][1] = max(merged[-1][1], end_i)
    return [(item[0], item[1]) for item in merged]


def _default_business_slots() -> list[dict[str, int]]:
    # Mon-Fri 09:00-18:00
    return [
        {"weekday": weekday, "start_minute": 9 * 60, "end_minute": 18 * 60}
        for weekday in range(5)
    ]


def _normalize_business_slots(raw_slots: list[dict[str, int]] | None) -> list[dict[str, int]]:
    grouped: dict[int, list[tuple[int, int]]] = {}
    for item in raw_slots or []:
        try:
            weekday = int(item.get("weekday", 0))
            start_minute = int(item.get("start_minute", 0))
            end_minute = int(item.get("end_minute", 0))
        except (AttributeError, TypeError, ValueError):
            continue
        if weekday < 0 or weekday > 6:
            continue
        if start_minute < 0 or start_minute > 1439:
            continue
        if end_minute <= start_minute or end_minute > 1440:
            continue
        grouped.setdefault(weekday, []).append((start_minute, end_minute))
    normalized: list[dict[str, int]] = []
    for weekday in sorted(grouped.keys()):
        for start_minute, end_minute in _merge_slots(grouped[weekday]):
            normalized.append(
                {
                    "weekday": int(weekday),
                    "start_minute": int(start_minute),
                    "end_minute": int(end_minute),
                }
            )
    return normalized


def _normalize_business_exceptions(raw_exceptions: list[dict] | None) -> list[dict]:
    normalized: list[dict] = []
    for item in raw_exceptions or []:
        if not isinstance(item, dict):
            continue
        raw_from = item.get("date_from")
        raw_to = item.get("date_to")
        if not isinstance(raw_from, date) or not isinstance(raw_to, date):
            continue
        if raw_to < raw_from:
            continue
        mode = str(item.get("mode") or "closed_all_day").strip().lower()
        if mode not in {"closed_all_day", "open_custom"}:
            mode = "closed_all_day"
        start_minute: int | None = None
        end_minute: int | None = None
        if mode == "open_custom":
            try:
                start_minute = int(item.get("start_minute"))
                end_minute = int(item.get("end_minute"))
            except (TypeError, ValueError):
                continue
            if start_minute < 0 or start_minute > 1439:
                continue
            if end_minute <= start_minute or end_minute > 1440:
                continue
        normalized.append(
            {
                "date_from": raw_from,
                "date_to": raw_to,
                "mode": mode,
                "start_minute": start_minute,
                "end_minute": end_minute,
                "note": str(item.get("note") or "").strip()[:255],
            }
        )
    normalized.sort(key=lambda row: (row["date_from"], row["date_to"], row["mode"]))
    return normalized


def get_or_create_workspace_business_hours(
    db: Session,
    *,
    workspace_id: int = DEFAULT_WORKSPACE_ID,
) -> WorkspaceBusinessHours:
    row = (
        db.query(WorkspaceBusinessHours)
        .filter(WorkspaceBusinessHours.workspace_id == workspace_id)
        .first()
    )
    created = False
    if row is None:
        row = WorkspaceBusinessHours(
            workspace_id=workspace_id,
            enabled=False,
            timezone=DEFAULT_BUSINESS_TIMEZONE,
            offhours_message=DEFAULT_OFFHOURS_MESSAGE,
            cooldown_seconds=DEFAULT_OFFHOURS_COOLDOWN_SECONDS,
        )
        db.add(row)
        db.commit()
        db.refresh(row)
        created = True
    changed = False
    normalized_timezone = _normalize_timezone_name(str(row.timezone or ""))
    if normalized_timezone != str(row.timezone or ""):
        row.timezone = normalized_timezone
        changed = True
    if int(row.cooldown_seconds or 0) <= 0:
        row.cooldown_seconds = DEFAULT_OFFHOURS_COOLDOWN_SECONDS
        changed = True
    if not (row.offhours_message or "").strip():
        row.offhours_message = DEFAULT_OFFHOURS_MESSAGE
        changed = True
    slots_exist = (
        db.query(WorkspaceBusinessSlot.id)
        .filter(WorkspaceBusinessSlot.workspace_id == workspace_id)
        .first()
    )
    if slots_exist is None:
        for slot in _default_business_slots():
            db.add(
                WorkspaceBusinessSlot(
                    workspace_id=workspace_id,
                    weekday=int(slot["weekday"]),
                    start_minute=int(slot["start_minute"]),
                    end_minute=int(slot["end_minute"]),
                )
            )
        changed = True
    if changed or created:
        db.add(row)
        db.commit()
        db.refresh(row)
    return row


def get_workspace_business_hours_payload(
    db: Session,
    *,
    workspace_id: int = DEFAULT_WORKSPACE_ID,
) -> dict[str, object]:
    row = get_or_create_workspace_business_hours(db, workspace_id=workspace_id)
    slots_rows = (
        db.query(WorkspaceBusinessSlot)
        .filter(WorkspaceBusinessSlot.workspace_id == workspace_id)
        .order_by(
            WorkspaceBusinessSlot.weekday.asc(),
            WorkspaceBusinessSlot.start_minute.asc(),
            WorkspaceBusinessSlot.id.asc(),
        )
        .all()
    )
    exceptions_rows = (
        db.query(WorkspaceBusinessException)
        .filter(WorkspaceBusinessException.workspace_id == workspace_id)
        .order_by(
            WorkspaceBusinessException.date_from.asc(),
            WorkspaceBusinessException.date_to.asc(),
            WorkspaceBusinessException.id.asc(),
        )
        .all()
    )
    return {
        "enabled": bool(row.enabled),
        "timezone": _normalize_timezone_name(str(row.timezone or DEFAULT_BUSINESS_TIMEZONE)),
        "offhours_message": str(row.offhours_message or DEFAULT_OFFHOURS_MESSAGE),
        "cooldown_seconds": max(60, int(row.cooldown_seconds or DEFAULT_OFFHOURS_COOLDOWN_SECONDS)),
        "slots": [
            {
                "weekday": int(item.weekday or 0),
                "start_minute": int(item.start_minute or 0),
                "end_minute": int(item.end_minute or 0),
            }
            for item in slots_rows
        ],
        "exceptions": [
            {
                "id": int(item.id),
                "date_from": item.date_from,
                "date_to": item.date_to,
                "mode": str(item.mode or "closed_all_day"),
                "start_minute": (int(item.start_minute) if item.start_minute is not None else None),
                "end_minute": (int(item.end_minute) if item.end_minute is not None else None),
                "note": str(item.note or ""),
            }
            for item in exceptions_rows
        ],
    }


def replace_workspace_business_hours(
    db: Session,
    *,
    workspace_id: int,
    enabled: bool,
    timezone_name: str,
    offhours_message: str,
    cooldown_seconds: int,
    slots: list[dict[str, int]] | None = None,
    exceptions: list[dict] | None = None,
    commit: bool = True,
) -> WorkspaceBusinessHours:
    row = get_or_create_workspace_business_hours(db, workspace_id=workspace_id)
    normalized_slots = _normalize_business_slots(slots)
    normalized_exceptions = _normalize_business_exceptions(exceptions)
    row.enabled = bool(enabled)
    row.timezone = _normalize_timezone_name(timezone_name)
    row.offhours_message = (offhours_message or "").strip() or DEFAULT_OFFHOURS_MESSAGE
    row.cooldown_seconds = max(60, min(7 * 24 * 60 * 60, int(cooldown_seconds or DEFAULT_OFFHOURS_COOLDOWN_SECONDS)))
    db.add(row)
    db.query(WorkspaceBusinessSlot).filter(WorkspaceBusinessSlot.workspace_id == workspace_id).delete()
    for slot in normalized_slots:
        db.add(
            WorkspaceBusinessSlot(
                workspace_id=workspace_id,
                weekday=int(slot["weekday"]),
                start_minute=int(slot["start_minute"]),
                end_minute=int(slot["end_minute"]),
            )
        )
    db.query(WorkspaceBusinessException).filter(WorkspaceBusinessException.workspace_id == workspace_id).delete()
    for item in normalized_exceptions:
        db.add(
            WorkspaceBusinessException(
                workspace_id=workspace_id,
                date_from=item["date_from"],
                date_to=item["date_to"],
                mode=item["mode"],
                start_minute=item["start_minute"],
                end_minute=item["end_minute"],
                note=item["note"],
            )
        )
    if commit:
        db.commit()
        db.refresh(row)
    return row


def _effective_day_slots(
    *,
    target_date: date,
    weekday: int,
    slots_by_weekday: dict[int, list[tuple[int, int]]],
    exceptions: list[dict[str, object]],
) -> list[tuple[int, int]]:
    matched_exception: dict[str, object] | None = None
    for item in exceptions:
        date_from = item.get("date_from")
        date_to = item.get("date_to")
        if not isinstance(date_from, date) or not isinstance(date_to, date):
            continue
        if date_from <= target_date <= date_to:
            matched_exception = item
            break
    if matched_exception is not None:
        mode = str(matched_exception.get("mode") or "closed_all_day").strip().lower()
        if mode == "open_custom":
            start_raw = matched_exception.get("start_minute")
            end_raw = matched_exception.get("end_minute")
            if isinstance(start_raw, int) and isinstance(end_raw, int) and 0 <= start_raw < end_raw <= 1440:
                return [(int(start_raw), int(end_raw))]
        return []
    return list(slots_by_weekday.get(int(weekday), []))


def _find_next_open_local(
    *,
    local_now: datetime,
    slots_by_weekday: dict[int, list[tuple[int, int]]],
    exceptions: list[dict[str, object]],
) -> datetime | None:
    for offset in range(0, 15):
        current_date = local_now.date() + timedelta(days=offset)
        weekday = int(current_date.weekday())
        intervals = _effective_day_slots(
            target_date=current_date,
            weekday=weekday,
            slots_by_weekday=slots_by_weekday,
            exceptions=exceptions,
        )
        if not intervals:
            continue
        minute_now = _minute_of_day(local_now) if offset == 0 else -1
        for start_minute, _end_minute in intervals:
            if offset == 0 and start_minute <= minute_now:
                continue
            return datetime(
                year=current_date.year,
                month=current_date.month,
                day=current_date.day,
                hour=start_minute // 60,
                minute=start_minute % 60,
                tzinfo=local_now.tzinfo,
            )
    return None


def evaluate_workspace_business_hours(
    db: Session,
    *,
    workspace_id: int,
    now_utc: datetime | None = None,
) -> dict[str, object]:
    payload = get_workspace_business_hours_payload(db, workspace_id=workspace_id)
    tz_name = _normalize_timezone_name(str(payload.get("timezone") or DEFAULT_BUSINESS_TIMEZONE))
    tz = ZoneInfo(tz_name)
    now_base = now_utc or _utc_now()
    if now_base.tzinfo is None:
        now_base = now_base.replace(tzinfo=UTC)
    local_now = now_base.astimezone(tz)
    slots_by_weekday: dict[int, list[tuple[int, int]]] = {}
    for slot in payload.get("slots", []) or []:
        if not isinstance(slot, dict):
            continue
        try:
            weekday = int(slot.get("weekday", 0))
            start_minute = int(slot.get("start_minute", 0))
            end_minute = int(slot.get("end_minute", 0))
        except (TypeError, ValueError):
            continue
        if weekday < 0 or weekday > 6:
            continue
        if start_minute < 0 or end_minute > 1440 or end_minute <= start_minute:
            continue
        slots_by_weekday.setdefault(weekday, []).append((start_minute, end_minute))
    for weekday in list(slots_by_weekday.keys()):
        slots_by_weekday[weekday] = _merge_slots(slots_by_weekday[weekday])
    exceptions: list[dict[str, object]] = []
    for item in payload.get("exceptions", []) or []:
        if not isinstance(item, dict):
            continue
        exceptions.append(item)
    current_intervals = _effective_day_slots(
        target_date=local_now.date(),
        weekday=local_now.weekday(),
        slots_by_weekday=slots_by_weekday,
        exceptions=exceptions,
    )
    minute = _minute_of_day(local_now)
    is_open = any(start <= minute < end for start, end in current_intervals)
    next_open_local = None if is_open else _find_next_open_local(
        local_now=local_now,
        slots_by_weekday=slots_by_weekday,
        exceptions=exceptions,
    )
    next_open_label = None
    if next_open_local is not None:
        next_open_label = f"{_weekday_label(next_open_local.weekday())}, {next_open_local.strftime('%H:%M')}"
    return {
        "enabled": bool(payload.get("enabled", False)),
        "timezone": tz_name,
        "offhours_message": str(payload.get("offhours_message") or DEFAULT_OFFHOURS_MESSAGE),
        "cooldown_seconds": int(payload.get("cooldown_seconds") or DEFAULT_OFFHOURS_COOLDOWN_SECONDS),
        "is_open": bool(is_open),
        "local_now": local_now,
        "next_open_local": next_open_local,
        "next_open_label": next_open_label,
    }


def _render_offhours_message(template_text: str, evaluation: dict[str, object]) -> str:
    text = (template_text or "").strip() or DEFAULT_OFFHOURS_MESSAGE
    next_label = str(evaluation.get("next_open_label") or "в рабочее время")
    timezone_name = str(evaluation.get("timezone") or DEFAULT_BUSINESS_TIMEZONE)
    text = text.replace("{next_work_time}", next_label)
    text = text.replace("{timezone}", timezone_name)
    return text


async def maybe_send_offhours_autoreply(
    db: Session,
    *,
    conversation: Conversation,
    meta: ConversationMeta,
    event: MaxWebhookEvent,
    client: MaxClient,
    workspace_id: int,
) -> bool:
    evaluation = evaluate_workspace_business_hours(db, workspace_id=workspace_id)
    if not bool(evaluation.get("enabled")):
        return False
    if bool(evaluation.get("is_open")):
        return False
    cooldown_seconds = max(
        60,
        int(evaluation.get("cooldown_seconds") or DEFAULT_OFFHOURS_COOLDOWN_SECONDS),
    )
    last_notice = meta.offhours_notice_sent_at
    if isinstance(last_notice, datetime):
        delta = _as_naive_utc(_utc_now()) - _as_naive_utc(last_notice)
        if delta.total_seconds() < cooldown_seconds:
            return False
    rendered = _render_offhours_message(
        str(evaluation.get("offhours_message") or DEFAULT_OFFHOURS_MESSAGE),
        evaluation,
    )
    # Off-hours auto-reply must not alter operator chat timeline/polling behavior.
    # Send directly to customer in MAX without creating ChatMessage/Outbox records.
    # Protect webhook path from slow upstream responses during bursts.
    try:
        send_result = await asyncio.wait_for(
            client.send_text(
                chat_id=event.chat_id,
                text=rendered,
                text_format="markdown",
            ),
            timeout=5.0,
        )
    except TimeoutError:
        send_result = {"success": False, "error": "offhours_send_timeout"}
    send_ok = bool(send_result.get("success", True) or send_result.get("message"))
    if not send_ok and str(event.sender_id or "").strip():
        try:
            fallback_result = await asyncio.wait_for(
                client.send_text_to_user(
                    user_id=str(event.sender_id).strip(),
                    text=rendered,
                    text_format="markdown",
                ),
                timeout=5.0,
            )
        except TimeoutError:
            fallback_result = {"success": False, "error": "offhours_send_timeout"}
        send_ok = bool(fallback_result.get("success", True) or fallback_result.get("message"))
        if send_ok:
            send_result = fallback_result
    if not send_ok:
        return False
    meta.offhours_notice_sent_at = _as_naive_utc(_utc_now())
    db.add(meta)
    db.add(
        AuditLog(
            workspace_id=workspace_id,
            actor_user_id=None,
            action="offhours_autoreply_sent",
            object_type="conversation",
            object_id=str(conversation.id),
            details_json=safe_json_dumps(
                {
                    "timezone": str(evaluation.get("timezone") or DEFAULT_BUSINESS_TIMEZONE),
                    "next_open_label": str(evaluation.get("next_open_label") or ""),
                },
            ),
        )
    )
    db.commit()
    return True


def _parse_manager_ids(raw_value: str) -> list[str]:
    """
    Parse comma/newline/semicolon separated manager account IDs.
    """
    raw = (raw_value or "").strip()
    if not raw:
        return []
    items: list[str] = []
    for part in re.split(r"[\s,;]+", raw):
        value = (part or "").strip()
        if not value:
            continue
        if value not in items:
            items.append(value)
    return items


@dataclass
class ForwardResult:
    ok: bool
    message: str


@dataclass
class ChatThreadItem:
    conversation_id: int
    chat_id: str
    customer_account_id: str
    ticket_no: int | None
    customer_label: str
    status: str
    phone_verified: bool
    last_message_preview: str
    last_message_is_max_incoming: bool
    has_delivery_errors: bool
    is_unread: bool
    is_blocked: bool
    last_activity_id: int
    folder_id: int | None
    folder_name: str
    folder_ids: list[int]
    folder_names: list[str]
    is_pinned: bool
    pin_order: int | None


@dataclass
class DeliveryStats:
    total_sent_attempts: int
    success_count: int
    failed_count: int
    retry_sum: int
    permanent_failures: int

    @property
    def success_rate(self) -> float:
        if self.total_sent_attempts <= 0:
            return 0.0
        return round((self.success_count / self.total_sent_attempts) * 100.0, 2)

    @property
    def avg_retry_count(self) -> float:
        if self.total_sent_attempts <= 0:
            return 0.0
        return round(self.retry_sum / self.total_sent_attempts, 2)


@dataclass
class MediaDiagnosticsStats:
    window_minutes: int
    total_outbox: int
    sent_outbox: int
    failed_outbox: int
    deduped_outbox: int
    send_message_total: int
    send_message_sent: int
    send_message_failed: int
    media_send_total: int
    media_send_sent: int
    media_send_failed: int
    fallback_markers: int
    proto_payload_errors: int
    upload_token_missing_errors: int
    estimated_payload_for_10_photos_bytes: int
    max_upload_bytes_limit: int
    estimated_payload_10_over_limit: bool

    @property
    def send_success_rate(self) -> float:
        if self.total_outbox <= 0:
            return 0.0
        return round((self.sent_outbox / self.total_outbox) * 100.0, 2)


def ensure_default_templates(db: Session, *, workspace_id: int = DEFAULT_WORKSPACE_ID) -> None:
    existing = {
        item.template_key: item
        for item in db.query(MessageTemplate)
        .filter(
            MessageTemplate.workspace_id == workspace_id,
            MessageTemplate.template_key.in_(list(DEFAULT_TEMPLATES.keys())),
        )
        .all()
    }
    changed = False
    for key, text in DEFAULT_TEMPLATES.items():
        if key in existing:
            continue
        db.add(MessageTemplate(workspace_id=workspace_id, template_key=key, template_text=text))
        changed = True
    if changed:
        db.commit()


def get_template_text(db: Session, key: str, *, workspace_id: int = DEFAULT_WORKSPACE_ID) -> str:
    row = (
        db.query(MessageTemplate)
        .filter(
            MessageTemplate.workspace_id == workspace_id,
            MessageTemplate.template_key == key,
        )
        .first()
    )
    if row and row.template_text.strip():
        return row.template_text
    return DEFAULT_TEMPLATES.get(key, "")


def set_template_text(
    db: Session,
    key: str,
    value: str,
    *,
    workspace_id: int = DEFAULT_WORKSPACE_ID,
) -> None:
    row = (
        db.query(MessageTemplate)
        .filter(
            MessageTemplate.workspace_id == workspace_id,
            MessageTemplate.template_key == key,
        )
        .first()
    )
    if row is None:
        row = MessageTemplate(
            workspace_id=workspace_id,
            template_key=key,
            template_text=value.strip(),
        )
    else:
        row.template_text = value.strip()
    db.add(row)
    db.commit()


def _get_or_create_conversation(
    db: Session,
    chat_id: str,
    customer_id: str,
    *,
    workspace_id: int = DEFAULT_WORKSPACE_ID,
) -> Conversation:
    can_dialog, _ = can_create_dialog(db, workspace_id=workspace_id)
    if not can_dialog:
        # Keep deterministic behavior for callers: stop creating new rows over quota.
        existing = (
            db.query(Conversation)
            .filter(
                Conversation.workspace_id == workspace_id,
                Conversation.chat_id == chat_id,
            )
            .first()
        )
        if existing:
            return existing
        raise ValueError("dialogs_limit_exceeded")
    # chat_id is globally unique in the current schema.
    # If we find a legacy row in another workspace for the same customer/chat,
    # rebind it to the resolved workspace to keep tenant routing consistent.
    conversation = (
        db.query(Conversation)
        .filter(
            Conversation.workspace_id == workspace_id,
            Conversation.chat_id == chat_id,
        )
        .first()
    )
    if conversation is None:
        conversation = db.query(Conversation).filter(Conversation.chat_id == chat_id).first()
        if conversation is not None:
            same_customer = (
                str(conversation.customer_account_id or "").strip() == str(customer_id or "").strip()
            )
            if same_customer and int(conversation.workspace_id or DEFAULT_WORKSPACE_ID) != int(workspace_id):
                conversation.workspace_id = workspace_id
                conversation.is_active = True
                db.add(conversation)
                db.commit()
                db.refresh(conversation)
    if conversation:
        return conversation
    conversation = Conversation(
        workspace_id=workspace_id,
        chat_id=chat_id,
        customer_account_id=customer_id,
        manager_added=False,
        is_active=True,
    )
    db.add(conversation)
    db.commit()
    db.refresh(conversation)
    return conversation


def _get_or_create_meta(
    db: Session,
    conversation_id: int,
    *,
    workspace_id: int = DEFAULT_WORKSPACE_ID,
) -> ConversationMeta:
    meta = (
        db.query(ConversationMeta)
        .filter(ConversationMeta.conversation_id == conversation_id)
        .first()
    )
    if meta:
        if int(meta.workspace_id or DEFAULT_WORKSPACE_ID) != int(workspace_id):
            meta.workspace_id = workspace_id
            db.add(meta)
            db.commit()
            db.refresh(meta)
        return meta
    max_ticket = (
        db.query(func.max(ConversationMeta.ticket_no))
        .filter(ConversationMeta.workspace_id == workspace_id)
        .scalar()
    )
    next_ticket = (max_ticket or 1000) + 1
    meta = ConversationMeta(
        workspace_id=workspace_id,
        conversation_id=conversation_id,
        ticket_no=next_ticket,
        status="new",
        phone_verified=False,
        start_prompt_sent=False,
    )
    db.add(meta)
    try:
        db.commit()
        db.refresh(meta)
        return meta
    except IntegrityError:
        # Legacy DB snapshots may keep global UNIQUE(ticket_no). Retry with next global value.
        db.rollback()
        global_max_ticket = db.query(func.max(ConversationMeta.ticket_no)).scalar() or 1000
        meta.ticket_no = int(global_max_ticket) + 1
        db.add(meta)
        db.commit()
        db.refresh(meta)
        return meta


def _upsert_customer_profile(
    db: Session,
    customer_id: str,
    chat_id: str,
    first_name: Optional[str],
    username: Optional[str],
    phone_number: Optional[str] = None,
    *,
    workspace_id: int = DEFAULT_WORKSPACE_ID,
) -> CustomerProfile:
    profile = (
        db.query(CustomerProfile)
        .filter(
            CustomerProfile.workspace_id == workspace_id,
            CustomerProfile.customer_account_id == customer_id,
        )
        .first()
    )
    if profile is None:
        profile = CustomerProfile(
            workspace_id=workspace_id,
            customer_account_id=customer_id,
            first_name=first_name,
            username=username,
            phone_number=phone_number or "",
            source_chat_id=chat_id,
        )
    else:
        profile.first_name = first_name or profile.first_name
        profile.username = username or profile.username
        profile.source_chat_id = chat_id or profile.source_chat_id
        if phone_number:
            profile.phone_number = phone_number
    db.add(profile)
    db.commit()
    db.refresh(profile)
    return profile


def _render_ticket_line(meta: ConversationMeta, customer: CustomerProfile | None, event: MaxWebhookEvent) -> str:
    name = customer.first_name if customer and customer.first_name else (event.sender_first_name or "Покупатель")
    username = customer.username if customer and customer.username else (event.sender_username or "")
    username_part = f" @{username}" if username else ""
    phone_state = "phone:yes" if meta.phone_verified else "phone:no"
    icon = "🆕" if meta.status == "new" else "📝"
    return f"{icon} [T-{meta.ticket_no}] {name}{username_part} ({phone_state})"


def _extract_sent_mid(send_result: dict) -> str | None:
    if not isinstance(send_result, dict):
        return None

    def _normalize_mid(value: object) -> str | None:
        raw = str(value or "").strip()
        if not raw:
            return None
        return raw

    key_candidates = (
        "idMessage",
        "id_message",
        "messageId",
        "message_id",
        "message_mid",
        "max_message_mid",
        "mid",
    )

    # 1) Prefer top-level explicit keys first.
    for key in key_candidates:
        normalized = _normalize_mid(send_result.get(key))
        if normalized:
            return normalized

    # 2) Then check common nested containers returned by providers.
    nested_roots: list[dict] = []
    for root_key in ("message", "body", "result", "data", "response", "payload"):
        root_value = send_result.get(root_key)
        if isinstance(root_value, dict):
            nested_roots.append(root_value)
    for root in nested_roots:
        for key in key_candidates:
            normalized = _normalize_mid(root.get(key))
            if normalized:
                return normalized

    # 3) Finally do a deep walk to support wrappers unknown in advance.
    stack: list[object] = [send_result]
    visited: set[int] = set()
    while stack:
        node = stack.pop()
        marker = id(node)
        if marker in visited:
            continue
        visited.add(marker)
        if isinstance(node, dict):
            for key in key_candidates:
                normalized = _normalize_mid(node.get(key))
                if normalized:
                    return normalized
            for value in node.values():
                if isinstance(value, (dict, list)):
                    stack.append(value)
        elif isinstance(node, list):
            for item in node:
                if isinstance(item, (dict, list)):
                    stack.append(item)
    return None


def _is_multi_image_payload_validation_error(result_value: dict, *, min_images: int = 2) -> bool:
    """
    Detect provider-side validation failures that appear only for multi-image sends.
    We fallback to URL attachments in those cases to keep operator flow unblocked.
    """
    if not isinstance(result_value, dict):
        return False
    raw_status = result_value.get("status_code")
    try:
        status_code = int(raw_status) if raw_status is not None else None
    except (TypeError, ValueError):
        status_code = None
    if status_code != 400:
        return False
    response = result_value.get("response")
    if not isinstance(response, dict):
        return False
    code = str(response.get("code") or "").strip().lower()
    message = str(response.get("message") or "").strip().lower()
    if code == "proto.payload" and ("errors.required" in message or "required" in message):
        return True
    # Some providers only return generic image upload error for batch payload.
    if code == "proto.payload" and ("failed to upload image" in message):
        return bool(int(min_images or 0) >= 2)
    return False


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _parse_schedule_at_iso(value: str) -> datetime | None:
    raw = (value or "").strip()
    if not raw:
        return None
    # HTML datetime-local may produce trailing "Z" when explicitly normalized in JS.
    # datetime.fromisoformat requires "+00:00" format, so normalize first.
    if raw.endswith("Z"):
        raw = f"{raw[:-1]}+00:00"
    try:
        scheduled = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if scheduled.tzinfo is None:
        scheduled = scheduled.replace(tzinfo=UTC)
    else:
        scheduled = scheduled.astimezone(UTC)
    # Tiny leeway: if time is effectively "now", send immediately.
    if scheduled <= _utc_now() + timedelta(seconds=5):
        return None
    return scheduled


def _as_naive_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(UTC).replace(tzinfo=None)


def _is_retriable_error(result: dict) -> bool:
    if not isinstance(result, dict):
        return False
    status_code = result.get("status_code")
    if isinstance(status_code, int) and (status_code == 429 or status_code >= 500):
        return True
    response = result.get("response")
    if isinstance(response, dict):
        code = str(response.get("code") or "").strip().lower()
        if code == "attachment.not.ready":
            return True
    return result.get("error") == "http_error"


def _next_backoff_delay(retry_count: int) -> int:
    idx = min(max(retry_count - 1, 0), len(OUTBOX_RETRY_BACKOFF_SECONDS) - 1)
    return OUTBOX_RETRY_BACKOFF_SECONDS[idx]


def _schedule_outbox_retry(item: OutboxMessage, result: dict) -> None:
    item.state = "failed"
    item.is_permanent_failure = False
    item.retry_count += 1
    item.last_attempt_at = _as_naive_utc(_utc_now())
    item.last_error = _format_delivery_error(result)
    item.next_retry_at = _as_naive_utc(_utc_now() + timedelta(seconds=_next_backoff_delay(item.retry_count)))


def _normalize_image_urls_json(image_urls_json: str | None, fallback_image_url: str | None = None) -> str:
    raw = str(image_urls_json or "").strip()
    parsed: list[str] = []
    if raw:
        try:
            value = json.loads(raw)
            if isinstance(value, list):
                parsed = [str(item).strip() for item in value if str(item).strip()]
        except Exception:
            parsed = []
    if not parsed and str(fallback_image_url or "").strip():
        parsed = [str(fallback_image_url or "").strip()]
    return json.dumps(parsed, ensure_ascii=False)


def _parse_image_urls_json(image_urls_json: str | None, fallback_image_url: str | None = None) -> list[str]:
    raw = str(image_urls_json or "").strip()
    parsed: list[str] = []
    if raw:
        try:
            value = json.loads(raw)
            if isinstance(value, list):
                parsed = [str(item).strip() for item in value if str(item).strip()]
        except Exception:
            parsed = []
    if not parsed and str(fallback_image_url or "").strip():
        parsed = [str(fallback_image_url or "").strip()]
    # Preserve order while removing duplicates.
    deduped: list[str] = []
    seen: set[str] = set()
    for item in parsed:
        if item in seen:
            continue
        seen.add(item)
        deduped.append(item)
    return deduped


def _to_local_static_media_path(value: str | None) -> str | None:
    return normalize_storage_public_url(value)


def _normalize_media_public_url(value: str | None) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    local = _to_local_static_media_path(raw)
    if local:
        return local
    return raw


def _resolve_local_static_media_file(value: str | None) -> Path | None:
    media_path = _to_local_static_media_path(value)
    if not media_path:
        return None
    file_path = local_upload_abspath(media_path)
    return file_path if file_path.exists() else None


def _to_external_media_url(value: str | None) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    if raw.startswith("http://") or raw.startswith("https://"):
        return raw
    local = _to_local_static_media_path(raw)
    if local and local.startswith("/static/"):
        base = str(app_settings.public_base_url or "").strip().rstrip("/")
        if base:
            return f"{base}{local}"
    return raw


def _incoming_media_extension(*, source_url: str, content_type: str) -> str:
    path_ext = Path(urlsplit(str(source_url or "").strip()).path).suffix.lower()
    if path_ext in {".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp"}:
        return path_ext
    mime = str(content_type or "").strip().lower().split(";", 1)[0]
    if mime == "image/jpeg":
        return ".jpg"
    if mime == "image/png":
        return ".png"
    if mime == "image/gif":
        return ".gif"
    if mime == "image/webp":
        return ".webp"
    if mime == "image/bmp":
        return ".bmp"
    return ".jpg"


def _incoming_media_auth_headers(client: MaxClient) -> list[dict[str, str]]:
    token_value = str(getattr(client, "token", "") or "").strip()
    if not token_value:
        return [{}]
    candidates: list[str] = [token_value]
    if not token_value.lower().startswith("bearer "):
        candidates.append(f"Bearer {token_value}")
    headers: list[dict[str, str]] = []
    seen: set[str] = set()
    for item in candidates:
        key = item.strip()
        if not key or key in seen:
            continue
        seen.add(key)
        headers.append({"Authorization": key})
    headers.append({})
    return headers


async def _download_and_store_incoming_media(
    *,
    media_url: str,
    client: MaxClient,
    workspace_id: int,
) -> str | None:
    url = str(media_url or "").strip()
    if not url or not (url.startswith("http://") or url.startswith("https://")):
        return None
    max_bytes = 20 * 1024 * 1024
    headers_candidates = _incoming_media_auth_headers(client)
    for headers in headers_candidates:
        try:
            async with httpx.AsyncClient(timeout=30, follow_redirects=True) as http_client:
                response = await http_client.get(url, headers=headers)
        except Exception as exc:
            logger.info("[INCOMING_MEDIA] download failed transport: %s", str(exc))
            continue
        if response.status_code >= 400:
            logger.info(
                "[INCOMING_MEDIA] download http error status=%s url=%s",
                response.status_code,
                url,
            )
            continue
        payload = bytes(response.content or b"")
        if not payload:
            logger.info("[INCOMING_MEDIA] download returned empty payload url=%s", url)
            continue
        if len(payload) > max_bytes:
            logger.warning(
                "[INCOMING_MEDIA] download skipped: payload too large bytes=%s url=%s",
                len(payload),
                url,
            )
            continue
        extension = _incoming_media_extension(
            source_url=url,
            content_type=str(response.headers.get("content-type", "") or ""),
        )
        safe_name = f"incoming-{int(workspace_id)}-{uuid4().hex[:12]}{extension}"
        try:
            stored_url = save_upload_bytes(file_name=safe_name, content=payload)
        except Exception as exc:
            logger.exception("[INCOMING_MEDIA] failed to store payload: %s", str(exc))
            return None
        local_url = _to_local_static_media_path(stored_url)
        return local_url or str(stored_url or "").strip() or None
    return None


async def _materialize_incoming_image_urls(
    *,
    image_urls: list[str],
    client: MaxClient,
    workspace_id: int,
) -> list[str]:
    prepared: list[str] = []
    seen: set[str] = set()
    for raw_value in image_urls or []:
        candidate = str(raw_value or "").strip()
        if not candidate:
            continue
        local_candidate = _to_local_static_media_path(candidate)
        if local_candidate:
            if local_candidate not in seen:
                seen.add(local_candidate)
                prepared.append(local_candidate)
            continue
        downloaded_local = await _download_and_store_incoming_media(
            media_url=candidate,
            client=client,
            workspace_id=workspace_id,
        )
        if downloaded_local:
            if downloaded_local not in seen:
                seen.add(downloaded_local)
                prepared.append(downloaded_local)
            continue
        # Keep original URL as graceful fallback when remote content cannot be downloaded.
        # This preserves previous behavior and prevents data loss in history rows.
        if candidate not in seen:
            seen.add(candidate)
            prepared.append(candidate)
    return prepared


def _mime_from_extension(path_value: str | None) -> str:
    ext = Path(str(path_value or "")).suffix.lower()
    if ext in {".jpg", ".jpeg"}:
        return "image/jpeg"
    if ext == ".png":
        return "image/png"
    if ext == ".gif":
        return "image/gif"
    if ext == ".webp":
        return "image/webp"
    if ext == ".bmp":
        return "image/bmp"
    return "application/octet-stream"


def _compute_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while True:
            chunk = stream.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _get_or_create_media_asset(
    db: Session,
    *,
    workspace_id: int,
    media_path: str,
) -> MediaAsset | None:
    normalized = str(media_path or "").strip()
    if not normalized.startswith("/static/"):
        return None
    file_path = _resolve_local_static_media_file(normalized)
    if file_path is None or not file_path.exists():
        return None
    existing = (
        db.query(MediaAsset)
        .filter(
            MediaAsset.workspace_id == int(workspace_id),
            MediaAsset.storage_provider == "local",
            MediaAsset.storage_key == normalized.removeprefix("/static/"),
        )
        .first()
    )
    if existing is not None:
        return existing
    try:
        byte_size = int(file_path.stat().st_size)
    except Exception:
        byte_size = 0
    asset = MediaAsset(
        workspace_id=int(workspace_id),
        storage_provider="local",
        storage_key=normalized.removeprefix("/static/"),
        public_url=upload_file_public_url(normalized),
        mime_type=_mime_from_extension(normalized),
        byte_size=byte_size,
        sha256=_compute_sha256(file_path),
    )
    db.add(asset)
    db.commit()
    db.refresh(asset)
    return asset


def _link_chat_message_media_assets(
    db: Session,
    *,
    chat_message_id: int,
    workspace_id: int,
    media_paths: list[str],
) -> None:
    normalized_paths = [str(item).strip() for item in media_paths if str(item).strip()]
    if not normalized_paths:
        return
    for idx, media_path in enumerate(normalized_paths):
        asset = _get_or_create_media_asset(
            db,
            workspace_id=int(workspace_id),
            media_path=media_path,
        )
        if asset is None:
            continue
        exists = (
            db.query(ChatMessageMedia.id)
            .filter(
                ChatMessageMedia.workspace_id == int(workspace_id),
                ChatMessageMedia.chat_message_id == int(chat_message_id),
                ChatMessageMedia.media_asset_id == int(asset.id),
                ChatMessageMedia.role == "image",
            )
            .first()
            is not None
        )
        if exists:
            continue
        db.add(
            ChatMessageMedia(
                workspace_id=int(workspace_id),
                chat_message_id=int(chat_message_id),
                media_asset_id=int(asset.id),
                sort_order=int(idx),
                role="image",
            )
        )
    db.commit()


def _media_paths_from_asset_links(db: Session, *, chat_message_id: int, workspace_id: int) -> list[str]:
    links = (
        db.query(ChatMessageMedia, MediaAsset)
        .join(MediaAsset, MediaAsset.id == ChatMessageMedia.media_asset_id)
        .filter(
            ChatMessageMedia.workspace_id == int(workspace_id),
            ChatMessageMedia.chat_message_id == int(chat_message_id),
        )
        .order_by(ChatMessageMedia.sort_order.asc(), ChatMessageMedia.id.asc())
        .all()
    )
    result: list[str] = []
    for link_row, asset in links:
        raw = str(asset.public_url or "").strip()
        if not raw:
            key = str(asset.storage_key or "").strip()
            if key:
                raw = f"/static/{key}"
        if raw:
            result.append(raw)
    return result


def _encode_image_bytes_for_payload(content: bytes) -> str:
    if not isinstance(content, (bytes, bytearray)):
        return ""
    return base64.b64encode(bytes(content)).decode("ascii")


def _decode_image_bytes_from_payload(value: object) -> bytes:
    if isinstance(value, (bytes, bytearray)):
        return bytes(value)
    if not isinstance(value, str):
        return b""
    raw = value.strip()
    if not raw:
        return b""
    try:
        return base64.b64decode(raw, validate=True)
    except Exception:
        return b""


def _chat_message_media_paths(item: ChatMessage) -> list[str]:
    linked_paths = _media_paths_from_asset_links(
        db=Session.object_session(item),  # type: ignore[arg-type]
        chat_message_id=int(getattr(item, "id", 0) or 0),
        workspace_id=int(getattr(item, "workspace_id", DEFAULT_WORKSPACE_ID) or DEFAULT_WORKSPACE_ID),
    ) if Session.object_session(item) is not None and int(getattr(item, "id", 0) or 0) > 0 else []
    if linked_paths:
        normalized_linked: list[str] = []
        for entry in linked_paths:
            local = _to_local_static_media_path(entry)
            normalized_linked.append(local or entry)
        return normalized_linked
    urls = _parse_image_urls_json(
        getattr(item, "image_urls_json", None),
        fallback_image_url=getattr(item, "image_url", None),
    )
    normalized: list[str] = []
    for entry in urls:
        local = _to_local_static_media_path(entry)
        normalized.append(local or entry)
    # Keep at least first image_url fallback for very old rows.
    if not normalized:
        fallback = _to_local_static_media_path(getattr(item, "image_url", None))
        if fallback:
            normalized = [fallback]
    return normalized


def _chat_message_has_media_ref(
    item: ChatMessage,
    *,
    media_path: str,
    skip_chat_message_id: int | None = None,
) -> bool:
    target = str(media_path or "").strip()
    if not target:
        return False
    if skip_chat_message_id is not None and int(getattr(item, "id", 0) or 0) == int(skip_chat_message_id):
        return False
    paths = _chat_message_media_paths(item)
    return any(str(path or "").strip() == target for path in paths)


def _try_delete_unreferenced_media_file(
    db: Session,
    *,
    media_path: str,
    skip_chat_message_id: int | None = None,
) -> None:
    normalized_path = str(media_path or "").strip()
    if not normalized_path.startswith("/static/"):
        return
    chat_rows = (
        db.query(ChatMessage)
        .filter(ChatMessage.image_url == normalized_path)
        .all()
    )
    has_chat_ref = any(
        _chat_message_has_media_ref(
            row,
            media_path=normalized_path,
            skip_chat_message_id=skip_chat_message_id,
        )
        for row in chat_rows
    )
    if not has_chat_ref:
        other_rows = (
            db.query(ChatMessage)
            .filter(ChatMessage.image_urls_json.like(f"%{normalized_path}%"))
            .all()
        )
        has_chat_ref = any(
            _chat_message_has_media_ref(
                row,
                media_path=normalized_path,
                skip_chat_message_id=skip_chat_message_id,
            )
            for row in other_rows
        )
    if not has_chat_ref:
        # Also check grouped media JSON for references.
        group_query = db.query(ChatMessage.id).filter(ChatMessage.image_urls_json.like(f"%{normalized_path}%"))
        if skip_chat_message_id is not None:
            group_query = group_query.filter(ChatMessage.id != int(skip_chat_message_id))
        has_chat_ref = group_query.first() is not None
    if has_chat_ref:
        return
    quick_media_ref = (
        db.query(QuickReplyMedia.id)
        .filter(QuickReplyMedia.media_path == normalized_path)
        .first()
        is not None
    )
    if quick_media_ref:
        return
    quick_legacy_ref = (
        db.query(QuickReply.id)
        .filter(QuickReply.image_path == normalized_path)
        .first()
        is not None
    )
    if quick_legacy_ref:
        return
    delete_by_public_url(normalized_path)


def _pick_manager_for_workspace(db: Session, *, workspace_id: int, settings: BotSettings) -> str | None:
    managers = (
        db.query(ServiceUser)
        .filter(
            ServiceUser.workspace_id == workspace_id,
            ServiceUser.role == "manager",
            ServiceUser.is_active.is_(True),
            ServiceUser.is_blocked.is_(False),
        )
        .order_by(ServiceUser.id.asc())
        .all()
    )
    if not managers:
        fallback_ids = _parse_manager_ids(settings.manager_account_id)
        return fallback_ids[0] if fallback_ids else None

    mode = (settings.routing_mode or "round_robin").strip().lower()
    if mode == "random":
        idx = int(_utc_now().timestamp()) % len(managers)
    else:
        cursor = int(settings.routing_rr_cursor or 0)
        idx = cursor % len(managers)
        settings.routing_rr_cursor = cursor + 1
        db.add(settings)
        db.commit()

    picked = managers[idx]
    manager_id = (picked.max_account_id or picked.username or "").strip()
    return manager_id or None


def _mark_outbox_sent(item: OutboxMessage, result: dict) -> None:
    item.state = "sent"
    item.is_permanent_failure = False
    item.last_attempt_at = _as_naive_utc(_utc_now())
    item.sent_at = _as_naive_utc(_utc_now())
    item.last_error = ""
    item.external_message_mid = _extract_sent_mid(result)
    item.next_retry_at = _as_naive_utc(_utc_now())


def _format_delivery_error(result: dict) -> str:
    if not isinstance(result, dict):
        return "Неизвестная ошибка доставки"

    status_code = result.get("status_code")
    response = result.get("response") if isinstance(result.get("response"), dict) else {}
    code = str(response.get("code") or "").strip()
    message = str(response.get("message") or "").strip()

    lowered_message = message.lower()
    lowered_code = code.lower()

    if lowered_code == "verify.token" or status_code == 401:
        human = "Недействительный токен бота"
    elif lowered_code == "chat.not_found":
        human = "Не найден чат получателя"
    elif "dialogs.suspended" in lowered_message or lowered_code == "chat.denied":
        human = "Пользователь запретил сообщения от бота или не активировал диалог"
    elif status_code == 403:
        human = "Нет прав на отправку сообщения"
    elif status_code == 404:
        human = "Получатель недоступен"
    elif result.get("error") == "http_error":
        human = "Сетевая ошибка при отправке в Max API"
    else:
        human = "Ошибка отправки сообщения"

    details: list[str] = []
    if status_code:
        details.append(f"status={status_code}")
    if code:
        details.append(f"code={code}")
    if message:
        details.append(f"message={message}")
    raw_error = str(result.get("error") or "").strip()
    raw_details = str(result.get("details") or "").strip()
    if raw_error:
        details.append(f"error={raw_error}")
    if raw_details:
        details.append(f"details={raw_details}")
    if result.get("endpoint"):
        details.append(f"endpoint={result.get('endpoint')}")
    detail_text = "; ".join(details)
    return f"{human} ({detail_text})"[:2000] if detail_text else human


def _enqueue_outbox_message(
    db: Session,
    *,
    conversation_id: int | None,
    chat_message_id: int | None,
    target_chat_id: str,
    target_user_id: str | None,
    operation: str,
    payload: dict,
    workspace_id: int | None = None,
    next_retry_at: datetime | None = None,
) -> OutboxMessage:
    resolved_workspace_id = workspace_id
    if resolved_workspace_id is None and conversation_id is not None:
        resolved_workspace_id = (
            db.query(Conversation.workspace_id)
            .filter(Conversation.id == conversation_id)
            .scalar()
        )
    if resolved_workspace_id is None:
        resolved_workspace_id = DEFAULT_WORKSPACE_ID
    normalized_payload = payload if isinstance(payload, dict) else {}
    payload_json = json.dumps(normalized_payload, ensure_ascii=False)
    fingerprint = _outbox_dedupe_fingerprint(
        operation=operation,
        target_chat_id=target_chat_id,
        target_user_id=target_user_id,
        payload=normalized_payload,
    )
    if hasattr(OutboxMessage, "idempotency_fingerprint"):
        idempotency_attr = "idempotency_fingerprint"
    elif hasattr(OutboxMessage, "idempotency_key"):
        idempotency_attr = "idempotency_key"
    else:
        idempotency_attr = ""
    if idempotency_attr and _recent_outbox_duplicate_exists(
        db,
        workspace_id=int(resolved_workspace_id),
        fingerprint=fingerprint,
    ):
        existing = (
            db.query(OutboxMessage)
            .filter(
                OutboxMessage.workspace_id == int(resolved_workspace_id),
                OutboxMessage.state.in_(["queued", "sending"]),
            )
            .order_by(OutboxMessage.id.desc())
            .all()
        )
        for row in existing:
            if idempotency_attr and str(getattr(row, idempotency_attr, "") or "").strip() == fingerprint:
                logger.info(
                    "[OUTBOX_ENQUEUE_DEDUP] hit workspace=%s operation=%s fingerprint=%s existing_id=%s",
                    int(resolved_workspace_id or 0),
                    str(operation or "").strip().lower(),
                    fingerprint[:12],
                    int(getattr(row, "id", 0) or 0),
                )
                return row
    outbox_kwargs: dict[str, object] = {
        "workspace_id": resolved_workspace_id,
        "conversation_id": conversation_id,
        "chat_message_id": chat_message_id,
        "target_chat_id": target_chat_id,
        "operation": operation,
        "target_user_id": target_user_id or "",
        "payload_json": payload_json,
        "state": "queued",
        "retry_count": 0,
        "next_retry_at": _as_naive_utc(next_retry_at or _utc_now()),
        "last_error": "",
    }
    if hasattr(OutboxMessage, "idempotency_fingerprint"):
        outbox_kwargs["idempotency_fingerprint"] = fingerprint
    if hasattr(OutboxMessage, "idempotency_key"):
        outbox_kwargs["idempotency_key"] = fingerprint
    if hasattr(OutboxMessage, "idempotency_expires_at"):
        outbox_kwargs["idempotency_expires_at"] = _as_naive_utc(_utc_now() + timedelta(minutes=5))
    item = OutboxMessage(
        **outbox_kwargs,
    )
    db.add(item)
    db.commit()
    db.refresh(item)
    logger.info(
        "[OUTBOX_ENQUEUE] created id=%s workspace=%s operation=%s chat_message_id=%s fingerprint=%s",
        int(getattr(item, "id", 0) or 0),
        int(getattr(item, "workspace_id", 0) or 0),
        str(getattr(item, "operation", "") or "").strip().lower(),
        int(getattr(item, "chat_message_id", 0) or 0),
        fingerprint[:12],
    )
    return item


def _claim_outbox_item_for_send(db: Session, *, outbox_id: int) -> OutboxMessage | None:
    """
    Atomically transition queued/failed outbox row to sending for immediate dispatch.
    This prevents duplicate sends when immediate path and background worker overlap.
    """
    claimed = (
        db.query(OutboxMessage)
        .filter(
            OutboxMessage.id == int(outbox_id),
            OutboxMessage.state.in_(["queued", "failed"]),
        )
        .update(
            {
                OutboxMessage.state: "sending",
                OutboxMessage.last_attempt_at: _as_naive_utc(_utc_now()),
            },
            synchronize_session=False,
        )
    )
    db.commit()
    if int(claimed or 0) <= 0:
        logger.info(
            "[OUTBOX_CLAIM] skipped id=%s reason=already_claimed_or_processed",
            int(outbox_id or 0),
        )
        return None
    claimed_row = db.query(OutboxMessage).filter(OutboxMessage.id == int(outbox_id)).first()
    logger.info(
        "[OUTBOX_CLAIM] acquired id=%s workspace=%s operation=%s",
        int(getattr(claimed_row, "id", 0) or 0),
        int(getattr(claimed_row, "workspace_id", 0) or 0),
        str(getattr(claimed_row, "operation", "") or "").strip().lower(),
    )
    return claimed_row


def _payload_fingerprint(payload: dict) -> str:
    normalized = json.dumps(payload or {}, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(normalized.encode("utf-8", errors="ignore")).hexdigest()


def _outbox_dedupe_fingerprint(
    *,
    operation: str,
    target_chat_id: str,
    target_user_id: str | None,
    payload: dict,
) -> str:
    base = {
        "operation": str(operation or "").strip().lower(),
        "target_chat_id": str(target_chat_id or "").strip(),
        "target_user_id": str(target_user_id or "").strip(),
        "payload_fp": _payload_fingerprint(payload),
    }
    normalized = json.dumps(base, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(normalized.encode("utf-8", errors="ignore")).hexdigest()


def _recent_outbox_duplicate_exists(
    db: Session,
    *,
    workspace_id: int,
    fingerprint: str,
    time_window_seconds: int = 180,
) -> bool:
    fp = str(fingerprint or "").strip()
    if not fp:
        return False
    window_from = _as_naive_utc(_utc_now() - timedelta(seconds=max(1, int(time_window_seconds or 1))))
    if hasattr(OutboxMessage, "idempotency_key"):
        idempotency_field = getattr(OutboxMessage, "idempotency_key")
    else:
        # Legacy schema without idempotency columns: fallback to payload hash only.
        # Keep dedupe conservative and avoid referencing missing ORM attributes.
        idempotency_field = None
    existing = (
        db.query(OutboxMessage.id)
        .filter(
            OutboxMessage.workspace_id == int(workspace_id),
            (idempotency_field == fp) if idempotency_field is not None else (OutboxMessage.payload_json == fp),
            OutboxMessage.created_at >= window_from,
            OutboxMessage.state.in_(["queued", "sending"]),
        )
        .order_by(OutboxMessage.id.desc())
        .first()
    )
    return bool(existing)


def _update_chat_message_delivery(
    db: Session,
    *,
    chat_message_id: int | None,
    state: str,
    error: str,
    retry_count: int,
    next_retry_at: datetime | None,
    max_mid: str | None = None,
) -> None:
    if chat_message_id is None:
        return
    msg = db.query(ChatMessage).filter(ChatMessage.id == chat_message_id).first()
    if msg is None:
        return
    msg.delivery_state = state
    msg.delivery_error = error
    msg.delivery_retry_count = retry_count
    msg.delivery_next_retry_at = _as_naive_utc(next_retry_at) if next_retry_at else None
    if max_mid:
        msg.max_message_mid = max_mid
    db.add(msg)
    db.commit()


def mark_conversation_messages_read_by_customer(
    db: Session,
    *,
    workspace_id: int,
    chat_id: str,
    customer_id: str,
    read_up_to_mid: str | None = None,
    read_at: datetime | None = None,
) -> int:
    """
    Mark outbound bot messages as read by customer for one conversation.
    Returns number of newly updated rows.
    """
    chat_value = str(chat_id or "").strip()
    customer_value = str(customer_id or "").strip()
    read_mid_value = str(read_up_to_mid or "").strip()
    logger.info(
        "[READ_FLOW] Received message_read, idMessage=%r, chatId=%r, customerId=%r, workspace=%s",
        read_mid_value,
        chat_value,
        customer_value,
        int(workspace_id or 0),
    )

    conversation_id: int | None = None

    # Primary strategy: if provider returns MID in read event, map by MID first.
    # This is robust even when sender/chat fields vary by event flavor.
    if read_mid_value:
        row = (
            db.query(ChatMessage.conversation_id)
            .join(
                Conversation,
                Conversation.id == ChatMessage.conversation_id,
            )
            .filter(
                ChatMessage.workspace_id == workspace_id,
                Conversation.workspace_id == workspace_id,
                ChatMessage.direction == "bot",
                ChatMessage.max_message_mid == read_mid_value,
            )
            .order_by(ChatMessage.id.desc())
            .first()
        )
        if row and row[0]:
            conversation_id = int(row[0])
            logger.info(
                "[READ_FLOW] MID matched conversation_id=%s for idMessage=%r",
                conversation_id,
                read_mid_value,
            )
        else:
            logger.warning(
                "[READ_FLOW] No ChatMessage found by idMessage=%r (max_message_mid); checking fallbacks",
                read_mid_value,
            )

    # Fallback: strict chat + customer match.
    if conversation_id is None and chat_value and customer_value:
        conversation = (
            db.query(Conversation)
            .filter(
                Conversation.workspace_id == workspace_id,
                Conversation.chat_id == chat_value,
                Conversation.customer_account_id == customer_value,
            )
            .first()
        )
        if conversation is not None:
            conversation_id = int(conversation.id)

    # Fallback for providers/events where sender_id can be omitted or not customer.
    if conversation_id is None and chat_value:
        conversation = (
            db.query(Conversation)
            .filter(
                Conversation.workspace_id == workspace_id,
                Conversation.chat_id == chat_value,
            )
            .order_by(Conversation.id.desc())
            .first()
        )
        if conversation is not None:
            conversation_id = int(conversation.id)

    if conversation_id is None and customer_value:
        conversation = (
            db.query(Conversation)
            .filter(
                Conversation.workspace_id == workspace_id,
                Conversation.customer_account_id == customer_value,
            )
            .order_by(Conversation.id.desc())
            .first()
        )
        if conversation is not None:
            conversation_id = int(conversation.id)

    if conversation_id is None:
        logger.warning(
            "[READ_FLOW] Unable to resolve conversation for read event idMessage=%r chatId=%r customerId=%r",
            read_mid_value,
            chat_value,
            customer_value,
        )
        return 0

    updated = mark_messages_read_by_customer(
        db,
        workspace_id=workspace_id,
        conversation_id=conversation_id,
        read_up_to_mid=(read_mid_value or None),
        read_at=read_at,
    )
    # If MID lookup failed silently for this provider payload, fallback to
    # marking all sent messages in the resolved conversation as read.
    if updated == 0 and read_mid_value:
        updated = mark_messages_read_by_customer(
            db,
            workspace_id=workspace_id,
            conversation_id=conversation_id,
            read_up_to_mid=None,
            read_at=read_at,
        )
    logger.info(
        "[READ_FLOW] Updated read flags count=%s for conversation_id=%s idMessage=%r",
        int(updated or 0),
        int(conversation_id or 0),
        read_mid_value,
    )
    return updated


async def _dispatch_outbox(
    db: Session,
    *,
    client: MaxClient,
    item: OutboxMessage,
) -> tuple[bool, bool, dict]:
    try:
        payload = json.loads(item.payload_json or "{}")
    except json.JSONDecodeError:
        payload = {}

    target_user_id = (item.target_user_id or "").strip() or None
    target_chat_id = (item.target_chat_id or "").strip()

    def _resolve_direct_attachments() -> list[dict]:
        attachments_payload = payload.get("attachments")
        if not isinstance(attachments_payload, list) or not attachments_payload:
            return []
        normalized: list[dict] = []
        for row in attachments_payload:
            if not isinstance(row, dict):
                continue
            row_type = str(row.get("type") or "").strip().lower()
            row_payload = row.get("payload")
            if row_type in {"inline_keyboard", "keyboard"} and isinstance(row_payload, dict):
                buttons = row_payload.get("buttons")
                if isinstance(buttons, list) and buttons:
                    normalized.append({"type": "inline_keyboard", "payload": {"buttons": buttons}})
                continue
            # Preserve message-button row for /start-like flows.
            if row_type == "message":
                button_text = str(row.get("text") or "").strip()
                if button_text:
                    normalized.append({"type": "message", "text": button_text})
                continue
            # Keep URL/link buttons if present in outgoing payload.
            if row_type in {"url", "link"}:
                button_text = str(row.get("text") or "").strip()
                button_url = str(row.get("url") or "").strip()
                if button_text and button_url:
                    normalized.append({"type": row_type, "text": button_text, "url": button_url})
                continue
            # Keep callback buttons if caller intentionally passed them.
            if row_type == "callback":
                button_text = str(row.get("text") or "").strip()
                button_payload = str(row.get("payload") or "").strip()
                if button_text and button_payload:
                    normalized.append({"type": "callback", "text": button_text, "payload": button_payload})
                continue
        return normalized

    def _resolve_fallback_attachments() -> list[dict]:
        attachments_payload = payload.get("attachments")
        if isinstance(attachments_payload, list) and attachments_payload:
            normalized: list[dict] = []
            for row in attachments_payload:
                if not isinstance(row, dict):
                    continue
                row_type = str(row.get("type") or "").strip().lower()
                row_payload = row.get("payload")
                if not isinstance(row_payload, dict):
                    continue
                token = str(row_payload.get("token") or "").strip()
                photos = row_payload.get("photos")
                url = _to_external_media_url(row_payload.get("url"))
                photo_id = row_payload.get("photo_id")
                image_payload: dict[str, object] = {}
                if token:
                    image_payload["token"] = token
                if isinstance(photos, list) and photos:
                    image_payload["photos"] = photos
                if url:
                    image_payload["url"] = url
                if photo_id is not None and str(photo_id).strip():
                    image_payload["photo_id"] = photo_id
                if not image_payload:
                    continue
                # Max API expects media as type=image payload.
                # Normalize legacy image_url payloads into image payload.
                if row_type in {"image", "image_url", "photo"}:
                    normalized.append({"type": "image", "payload": image_payload})
            if normalized:
                return normalized

        message_id = int(item.chat_message_id or 0)
        if message_id <= 0:
            return []
        chat_message = db.query(ChatMessage).filter(ChatMessage.id == message_id).first()
        if chat_message is None:
            return []

        urls: list[str] = []
        raw_urls_json = str(getattr(chat_message, "image_urls_json", "") or "").strip()
        if raw_urls_json:
            try:
                decoded = json.loads(raw_urls_json)
            except Exception:
                decoded = []
            if isinstance(decoded, list):
                for value in decoded:
                    url = str(value or "").strip()
                    if url:
                        urls.append(url)
        if not urls:
            single_url = str(getattr(chat_message, "image_url", "") or "").strip()
            if single_url:
                urls.append(single_url)
        normalized_urls = [_to_external_media_url(url) for url in urls]
        normalized_urls = [url for url in normalized_urls if url]
        return [{"type": "image", "payload": {"url": url}} for url in normalized_urls]

    def _is_proto_payload_upload_error(result_value: dict) -> bool:
        if not isinstance(result_value, dict):
            return False
        raw_status = result_value.get("status_code")
        try:
            status_code = int(raw_status) if raw_status is not None else None
        except (TypeError, ValueError):
            status_code = None
        if status_code != 400:
            return False
        response = result_value.get("response")
        if not isinstance(response, dict):
            return False
        code = str(response.get("code") or "").strip().lower()
        message = str(response.get("message") or "").strip().lower()
        if code != "proto.payload":
            return False
        return (
            ("failed to upload image" in message)
            or ("can't deserialize body" in message)
            or ("deserialize" in message)
            or ("required" in message)
            or ("attachment" in message)
            or ("photo" in message)
        )

    def _is_multi_image_proto_payload_error(result_value: dict, *, image_count: int) -> bool:
        if int(image_count or 0) < 2:
            return False
        if _is_multi_image_payload_validation_error(result_value, min_images=image_count):
            return True
        if not isinstance(result_value, dict):
            return False
        try:
            status_code = int(result_value.get("status_code"))
        except (TypeError, ValueError):
            status_code = None
        if status_code != 400:
            return False
        response = result_value.get("response")
        if not isinstance(response, dict):
            return False
        code = str(response.get("code") or "").strip().lower()
        if code != "proto.payload":
            return False
        # For 2+ images, any proto.payload 400 is treated as payload-shape incompatibility
        # and should fall back to URL attachments rather than failing hard.
        return True

    def _is_multi_image_proto_required_error(result_value: dict, *, image_count: int) -> bool:
        if image_count < 2:
            return False
        return _is_multi_image_payload_validation_error(result_value, min_images=image_count)

    async def _send_message_with_attachment_ready_retry(
        *,
        chat_id: str | None = None,
        user_id: str | None = None,
        text: str | None = None,
        attachments: list[dict] | None = None,
        max_attempts: int = 4,
    ) -> dict:
        wait_seconds = 0.6
        attempts = max(1, int(max_attempts or 1))
        for idx in range(attempts):
            result_value = await client.send_message(
                chat_id=chat_id,
                user_id=user_id,
                text=text,
                attachments=attachments,
            )
            code = ""
            response = result_value.get("response") if isinstance(result_value, dict) else {}
            if isinstance(response, dict):
                code = str(response.get("code") or "").strip().lower()
            if code != "attachment.not.ready":
                return result_value
            if idx >= attempts - 1:
                return result_value
            await asyncio.sleep(wait_seconds)
            wait_seconds = min(wait_seconds * 2.0, 5.0)
        return {"success": False, "error": "attachment_not_ready_retry_exhausted"}

    async def _send_by_chat() -> dict:

        if item.operation == "send_text":
            return await client.send_text(
                chat_id=target_chat_id,
                text=str(payload.get("text") or ""),
                text_format=str(payload.get("format") or "").strip().lower() or None,
            )
        if item.operation == "send_photo":
            return await client.send_photo(
                chat_id=target_chat_id,
                photo_url=str(payload.get("photo_url") or ""),
                caption=str(payload.get("caption")) if payload.get("caption") is not None else None,
            )
        if item.operation == "send_message":
            direct_attachments = _resolve_direct_attachments()
            images_payload = payload.get("images")
            if isinstance(images_payload, list) and images_payload:
                images: list[tuple[str, bytes]] = []
                images_count = 0
                for row in images_payload:
                    if not isinstance(row, list) or len(row) != 2:
                        images = []
                        break
                    file_name = str(row[0] or "").strip() or "image.jpg"
                    content_raw = row[1]
                    content_bytes = _decode_image_bytes_from_payload(content_raw)
                    if not content_bytes:
                        images = []
                        break
                    images.append((file_name, content_bytes))
                images_count = len(images)
                if images:
                    logger.info(
                        "[MEDIA_SEND] outbox_id=%s mode=byte_upload route=chat images=%s target_chat=%s",
                        int(getattr(item, "id", 0) or 0),
                        int(images_count or 0),
                        str(target_chat_id or "").strip(),
                    )
                    images_result = await client.send_images(
                        chat_id=target_chat_id,
                        images=images,
                        text=str(payload.get("text")) if payload.get("text") is not None else None,
                    )
                    if isinstance(images_result, dict) and (
                        str(images_result.get("error") or "").strip().lower() == "upload_token_missing"
                        or _is_proto_payload_upload_error(images_result)
                        or _is_multi_image_proto_payload_error(images_result, image_count=images_count)
                    ):
                        logger.info(
                            "[MEDIA_SEND_FALLBACK] outbox_id=%s route=chat images=%s reason=%s",
                            int(getattr(item, "id", 0) or 0),
                            int(images_count or 0),
                            str(_format_delivery_error(images_result) or "").strip()[:240],
                        )
                        # Fallback for upload providers returning non-standard token response:
                        # send by URL attachments to avoid blocking operator flow.
                        return await _send_message_with_attachment_ready_retry(
                            chat_id=target_chat_id,
                            text=str(payload.get("text")) if payload.get("text") is not None else None,
                            attachments=_resolve_fallback_attachments(),
                        )
                    return images_result
            attachments_to_send = (
                direct_attachments
                if direct_attachments
                else _resolve_fallback_attachments()
            )
            return await _send_message_with_attachment_ready_retry(
                chat_id=target_chat_id,
                text=str(payload.get("text")) if payload.get("text") is not None else None,
                attachments=attachments_to_send,
            )
        return {"success": False, "error": "unsupported_operation", "operation": item.operation}

    async def _send_by_user() -> dict:

        if not target_user_id:
            return {"success": False, "error": "user_id_unavailable"}
        if item.operation == "send_text":
            return await client.send_text_to_user(
                user_id=target_user_id,
                text=str(payload.get("text") or ""),
                text_format=str(payload.get("format") or "").strip().lower() or None,
            )
        if item.operation == "send_photo":
            return await client.send_photo_to_user(
                user_id=target_user_id,
                photo_url=str(payload.get("photo_url") or ""),
                caption=str(payload.get("caption")) if payload.get("caption") is not None else None,
            )
        if item.operation == "send_message":
            direct_attachments = _resolve_direct_attachments()
            images_payload = payload.get("images")
            if isinstance(images_payload, list) and images_payload:
                images: list[tuple[str, bytes]] = []
                images_count = 0
                for row in images_payload:
                    if not isinstance(row, list) or len(row) != 2:
                        images = []
                        break
                    file_name = str(row[0] or "").strip() or "image.jpg"
                    content_raw = row[1]
                    content_bytes = _decode_image_bytes_from_payload(content_raw)
                    if not content_bytes:
                        images = []
                        break
                    images.append((file_name, content_bytes))
                images_count = len(images)
                if images:
                    logger.info(
                        "[MEDIA_SEND] outbox_id=%s mode=byte_upload route=user images=%s target_user=%s",
                        int(getattr(item, "id", 0) or 0),
                        int(images_count or 0),
                        str(target_user_id or "").strip(),
                    )
                    images_result = await client.send_images(
                        user_id=target_user_id,
                        images=images,
                        text=str(payload.get("text")) if payload.get("text") is not None else None,
                    )
                    if isinstance(images_result, dict) and (
                        str(images_result.get("error") or "").strip().lower() == "upload_token_missing"
                        or _is_proto_payload_upload_error(images_result)
                        or _is_multi_image_proto_payload_error(images_result, image_count=images_count)
                    ):
                        logger.info(
                            "[MEDIA_SEND_FALLBACK] outbox_id=%s route=user images=%s reason=%s",
                            int(getattr(item, "id", 0) or 0),
                            int(images_count or 0),
                            str(_format_delivery_error(images_result) or "").strip()[:240],
                        )
                        # Fallback for upload providers returning non-standard token response:
                        # send by URL attachments to avoid blocking operator flow.
                        return await _send_message_with_attachment_ready_retry(
                            user_id=target_user_id,
                            text=str(payload.get("text")) if payload.get("text") is not None else None,
                            attachments=_resolve_fallback_attachments(),
                        )
                    return images_result
            attachments_to_send = (
                direct_attachments
                if direct_attachments
                else _resolve_fallback_attachments()
            )
            return await _send_message_with_attachment_ready_retry(
                user_id=target_user_id,
                text=str(payload.get("text")) if payload.get("text") is not None else None,
                attachments=attachments_to_send,
            )
        return {"success": False, "error": "unsupported_operation", "operation": item.operation}

    def _is_chat_not_found_error(result_value: dict) -> bool:
        if not isinstance(result_value, dict):
            return False
        raw_status = result_value.get("status_code")
        try:
            status_code = int(raw_status) if raw_status is not None else None
        except (TypeError, ValueError):
            status_code = None
        if status_code != 404:
            return False
        response = result_value.get("response")
        if not isinstance(response, dict):
            return False
        code = str(response.get("code") or "").strip().lower()
        message = str(response.get("message") or "").strip().lower()
        return "chat.not_found" in code or "chat " in message and " not found" in message

    if target_chat_id:
        result = await _send_by_chat()
        if not (bool(result.get("success", True) or result.get("message"))) and _is_chat_not_found_error(result):
            # For dialogs Max may reject chat_id while accepting user_id. Try fallback.
            fallback = await _send_by_user()
            result = fallback
    elif target_user_id:
        result = await _send_by_user()
    else:
        result = {"success": False, "error": "chat_id_or_user_id_required"}

    # Idempotency guard: if an identical send_message payload was recently sent
    # in this workspace, mark current outbox row as deduplicated-success.
    if str(getattr(item, "operation", "") or "").strip().lower() == "send_message":
        fingerprint_attr = (
            "idempotency_key"
            if hasattr(OutboxMessage, "idempotency_key")
            else "idempotency_fingerprint"
        )
        current_fingerprint = str(getattr(item, fingerprint_attr, "") or "").strip()
        if current_fingerprint:
            duplicate_sent = (
                db.query(OutboxMessage.id)
                .filter(
                    OutboxMessage.workspace_id == int(item.workspace_id or 0),
                    getattr(OutboxMessage, fingerprint_attr) == current_fingerprint,
                    OutboxMessage.id != int(item.id or 0),
                    OutboxMessage.state == "sent",
                    OutboxMessage.idempotency_expires_at.isnot(None),
                    OutboxMessage.idempotency_expires_at >= _as_naive_utc(_utc_now()),
                )
                .order_by(OutboxMessage.id.desc())
                .first()
            )
            if duplicate_sent:
                logger.info(
                    "[OUTBOX_DISPATCH_DEDUP] id=%s workspace=%s fingerprint=%s duplicate_sent_id=%s",
                    int(getattr(item, "id", 0) or 0),
                    int(getattr(item, "workspace_id", 0) or 0),
                    current_fingerprint[:12],
                    int(duplicate_sent[0] or 0),
                )
                item.state = "sent"
                item.is_permanent_failure = False
                item.last_attempt_at = _as_naive_utc(_utc_now())
                item.sent_at = _as_naive_utc(_utc_now())
                item.last_error = "deduplicated_by_fingerprint"
                db.add(item)
                db.commit()
                _update_chat_message_delivery(
                    db,
                    chat_message_id=item.chat_message_id,
                    state="sent",
                    error="",
                    retry_count=item.retry_count,
                    next_retry_at=item.next_retry_at,
                    max_mid=item.external_message_mid,
                )
                return True, False, {"success": True, "deduplicated": True}

    ok = bool(result.get("success", True) or result.get("message"))
    retriable = _is_retriable_error(result)
    logger.info(
        "[OUTBOX_DISPATCH_RESULT] id=%s workspace=%s operation=%s ok=%s retriable=%s state_before_finalize=%s",
        int(getattr(item, "id", 0) or 0),
        int(getattr(item, "workspace_id", 0) or 0),
        str(getattr(item, "operation", "") or "").strip().lower(),
        bool(ok),
        bool(retriable),
        str(getattr(item, "state", "") or "").strip().lower(),
    )
    if ok:
        can_send, reason = can_send_message_this_month(db, workspace_id=item.workspace_id)
        if not can_send:
            fail = {"status_code": 402, "response": {"code": reason, "message": reason}, "endpoint": "billing_limit"}
            item.state = "failed"
            item.is_permanent_failure = True
            item.retry_count += 1
            item.last_attempt_at = _as_naive_utc(_utc_now())
            item.last_error = _format_delivery_error(fail)
            item.next_retry_at = _as_naive_utc(_utc_now() + timedelta(hours=24))
            db.add(item)
            db.commit()
            _update_chat_message_delivery(
                db,
                chat_message_id=item.chat_message_id,
                state="failed",
                error=item.last_error,
                retry_count=item.retry_count,
                next_retry_at=item.next_retry_at,
            )
            return False, False, fail
        _mark_outbox_sent(item, result)
        db.add(item)
        db.commit()
        track_message_sent(
            db,
            workspace_id=item.workspace_id,
            external_id=item.external_message_mid or "",
        )
        _update_chat_message_delivery(
            db,
            chat_message_id=item.chat_message_id,
            state="sent",
            error="",
            retry_count=item.retry_count,
            next_retry_at=item.next_retry_at,
            max_mid=item.external_message_mid,
        )
        return True, False, result

    if retriable:
        _schedule_outbox_retry(item, result)
        db.add(item)
        db.commit()
        _update_chat_message_delivery(
            db,
            chat_message_id=item.chat_message_id,
            state="failed",
            error=item.last_error,
            retry_count=item.retry_count,
            next_retry_at=item.next_retry_at,
        )
        return False, True, result

    item.state = "failed"
    item.is_permanent_failure = True
    item.retry_count += 1
    item.last_attempt_at = _as_naive_utc(_utc_now())
    item.last_error = _format_delivery_error(result)
    item.next_retry_at = _as_naive_utc(_utc_now() + timedelta(hours=24))
    db.add(item)
    db.commit()
    _update_chat_message_delivery(
        db,
        chat_message_id=item.chat_message_id,
        state="failed",
        error=item.last_error,
        retry_count=item.retry_count,
        next_retry_at=item.next_retry_at,
    )
    return False, False, result


async def process_outbox_queue(
    db: Session,
    *,
    limit: int = 20,
    workspace_id: int | None = None,
) -> int:
    now = _as_naive_utc(_utc_now())
    query = db.query(OutboxMessage).filter(
        OutboxMessage.state.in_(["queued", "failed"]),
        OutboxMessage.next_retry_at <= now,
    )
    if workspace_id is not None:
        query = query.filter(OutboxMessage.workspace_id == workspace_id)
    items = query.order_by(OutboxMessage.next_retry_at.asc(), OutboxMessage.id.asc()).limit(limit).all()
    if not items:
        return 0
    processed = 0
    for item in items:
        # Atomic claim to avoid duplicate sends when immediate dispatch and
        # background queue overlap for the same outbox row.
        claimed = (
            db.query(OutboxMessage)
            .filter(
                OutboxMessage.id == int(item.id),
                OutboxMessage.state.in_(["queued", "failed"]),
            )
            .update(
                {
                    OutboxMessage.state: "sending",
                    OutboxMessage.last_attempt_at: _as_naive_utc(_utc_now()),
                },
                synchronize_session=False,
            )
        )
        db.commit()
        if int(claimed or 0) <= 0:
            continue
        db.refresh(item)
        client = _workspace_client(db, workspace_id=item.workspace_id)
        await _dispatch_outbox(db, client=client, item=item)
        processed += 1
    return processed


async def retry_failed_outbox_message(
    db: Session,
    *,
    chat_message_id: int,
    workspace_id: int | None = None,
) -> bool:
    query = db.query(OutboxMessage).filter(
        OutboxMessage.chat_message_id == chat_message_id,
        OutboxMessage.state == "failed",
    )
    if workspace_id is not None:
        query = query.filter(OutboxMessage.workspace_id == workspace_id)
    item = query.order_by(OutboxMessage.id.desc()).first()
    if item is None:
        return False
    item.state = "queued"
    item.next_retry_at = _as_naive_utc(_utc_now())
    db.add(item)
    db.commit()
    client = _workspace_client(db, workspace_id=item.workspace_id)
    ok, _, _ = await _dispatch_outbox(db, client=client, item=item)
    return ok


async def enqueue_and_process_send_text(
    db: Session,
    *,
    conversation_id: int,
    target_chat_id: str,
    target_user_id: str | None = None,
    text: str,
    source: str,
    link_mid: str | None = None,
    text_format: str | None = None,
) -> bool:
    now_utc_naive = _as_naive_utc(_utc_now())
    msg = _store_chat_message(
        db,
        conversation_id=conversation_id,
        direction="bot",
        source=source,
        text=text,
        link_mid=link_mid,
        delivery_state="queued",
        delivery_error="",
        delivery_retry_count=0,
        delivery_next_retry_at=now_utc_naive,
    )
    item = _enqueue_outbox_message(
        db,
        conversation_id=conversation_id,
        chat_message_id=msg.id,
        target_chat_id=target_chat_id,
        target_user_id=target_user_id,
        operation="send_text",
        payload={"text": text, "format": text_format},
    )
    claimed_item = _claim_outbox_item_for_send(db, outbox_id=int(item.id))
    if claimed_item is None:
        return False
    client = _workspace_client(db, workspace_id=claimed_item.workspace_id)
    ok, _, _ = await _dispatch_outbox(db, client=client, item=claimed_item)
    return ok


async def enqueue_and_process_send_photo(
    db: Session,
    *,
    conversation_id: int,
    target_chat_id: str,
    target_user_id: str | None = None,
    photo_url: str,
    caption: str,
    source: str,
) -> bool:
    now_utc_naive = _as_naive_utc(_utc_now())
    msg = _store_chat_message(
        db,
        conversation_id=conversation_id,
        direction="bot",
        source=source,
        text=caption,
        image_url=photo_url,
        delivery_state="queued",
        delivery_error="",
        delivery_retry_count=0,
        delivery_next_retry_at=now_utc_naive,
    )
    item = _enqueue_outbox_message(
        db,
        conversation_id=conversation_id,
        chat_message_id=msg.id,
        target_chat_id=target_chat_id,
        target_user_id=target_user_id,
        operation="send_photo",
        payload={"photo_url": photo_url, "caption": caption},
    )
    claimed_item = _claim_outbox_item_for_send(db, outbox_id=int(item.id))
    if claimed_item is None:
        return False
    client = _workspace_client(db, workspace_id=claimed_item.workspace_id)
    ok, _, _ = await _dispatch_outbox(db, client=client, item=claimed_item)
    return ok


async def enqueue_and_process_send_media_group(
    db: Session,
    *,
    conversation_id: int,
    target_chat_id: str,
    target_user_id: str | None = None,
    photo_urls: list[str],
    text: str = "",
    source: str,
) -> bool:
    urls = [str(item).strip() for item in (photo_urls or []) if str(item).strip()]
    if not urls:
        return False
    attachments = [{"type": "image", "payload": {"url": _to_external_media_url(value)}} for value in urls]
    attachments = [item for item in attachments if str((item.get("payload") or {}).get("url") or "").strip()]
    now_utc_naive = _as_naive_utc(_utc_now())
    first_image = urls[0]
    msg = _store_chat_message(
        db,
        conversation_id=conversation_id,
        direction="bot",
        source=source,
        text=(text or "").strip(),
        image_url=first_image,
        image_urls_json=json.dumps(urls, ensure_ascii=False),
        delivery_state="queued",
        delivery_error="",
        delivery_retry_count=0,
        delivery_next_retry_at=now_utc_naive,
    )
    image_payloads: list[tuple[str, bytes]] = []
    for value in urls:
        local_file = _resolve_local_static_media_file(value)
        if local_file is None:
            image_payloads = []
            break
        try:
            image_payloads.append((local_file.name or "image.jpg", local_file.read_bytes()))
        except Exception:
            image_payloads = []
            break
    item = _enqueue_outbox_message(
        db,
        conversation_id=conversation_id,
        chat_message_id=msg.id,
        target_chat_id=target_chat_id,
        target_user_id=target_user_id,
        operation="send_message",
        payload=(
            {
                "text": (text or "").strip() or None,
                "images": [[name, _encode_image_bytes_for_payload(content)] for name, content in image_payloads],
                # Keep URL attachments even on byte-upload path so fallback
                # after upload_token_missing can resend without payload loss.
                "attachments": attachments,
            }
            if image_payloads
            else {"text": (text or "").strip() or None, "attachments": attachments}
        ),
    )
    claimed_item = _claim_outbox_item_for_send(db, outbox_id=int(item.id))
    if claimed_item is None:
        return False
    client = _workspace_client(db, workspace_id=claimed_item.workspace_id)
    ok, _, _ = await _dispatch_outbox(db, client=client, item=claimed_item)
    return ok


async def queue_only_send_text(
    db: Session,
    *,
    conversation_id: int,
    target_chat_id: str,
    target_user_id: str | None = None,
    text: str,
    source: str,
    link_mid: str | None = None,
    scheduled_for: datetime | None = None,
    text_format: str | None = None,
) -> None:
    next_retry_at = _as_naive_utc(scheduled_for or _utc_now())
    is_scheduled_message = scheduled_for is not None
    msg = _store_chat_message(
        db,
        conversation_id=conversation_id,
        direction="bot",
        source=source,
        text=text,
        link_mid=link_mid,
        delivery_state="queued",
        delivery_error="",
        delivery_retry_count=0,
        delivery_next_retry_at=next_retry_at,
        is_scheduled_message=is_scheduled_message,
    )
    _enqueue_outbox_message(
        db,
        conversation_id=conversation_id,
        chat_message_id=msg.id,
        target_chat_id=target_chat_id,
        target_user_id=target_user_id,
        operation="send_text",
        payload={"text": text, "format": text_format},
        next_retry_at=next_retry_at,
    )


async def queue_only_send_media_group(
    db: Session,
    *,
    conversation_id: int,
    target_chat_id: str,
    target_user_id: str | None = None,
    photo_urls: list[str],
    text: str = "",
    source: str,
    scheduled_for: datetime | None = None,
) -> None:
    urls = [str(item).strip() for item in (photo_urls or []) if str(item).strip()]
    if not urls:
        return
    next_retry_at = _as_naive_utc(scheduled_for or _utc_now())
    is_scheduled_message = scheduled_for is not None
    first_image = urls[0]
    msg = _store_chat_message(
        db,
        conversation_id=conversation_id,
        direction="bot",
        source=source,
        text=(text or "").strip(),
        image_url=first_image,
        image_urls_json=json.dumps(urls, ensure_ascii=False),
        delivery_state="queued",
        delivery_error="",
        delivery_retry_count=0,
        delivery_next_retry_at=next_retry_at,
        is_scheduled_message=is_scheduled_message,
    )
    attachments = [{"type": "image", "payload": {"url": _to_external_media_url(value)}} for value in urls]
    attachments = [item for item in attachments if str((item.get("payload") or {}).get("url") or "").strip()]
    image_payloads: list[tuple[str, bytes]] = []
    for value in urls:
        local_file = _resolve_local_static_media_file(value)
        if local_file is None:
            image_payloads = []
            break
        try:
            image_payloads.append((local_file.name or "image.jpg", local_file.read_bytes()))
        except Exception:
            image_payloads = []
            break
    _enqueue_outbox_message(
        db,
        conversation_id=conversation_id,
        chat_message_id=msg.id,
        target_chat_id=target_chat_id,
        target_user_id=target_user_id,
        operation="send_message",
        payload=(
            {
                "text": (text or "").strip() or None,
                "images": [[name, _encode_image_bytes_for_payload(content)] for name, content in image_payloads],
                # Preserve URL attachments for retry/fallback path.
                "attachments": attachments,
            }
            if image_payloads
            else {"text": (text or "").strip() or None, "attachments": attachments}
        ),
        next_retry_at=next_retry_at,
    )


async def queue_only_send_photo(
    db: Session,
    *,
    conversation_id: int,
    target_chat_id: str,
    target_user_id: str | None = None,
    photo_url: str,
    caption: str,
    source: str,
    scheduled_for: datetime | None = None,
) -> None:
    next_retry_at = _as_naive_utc(scheduled_for or _utc_now())
    is_scheduled_message = scheduled_for is not None
    msg = _store_chat_message(
        db,
        conversation_id=conversation_id,
        direction="bot",
        source=source,
        text=caption,
        image_url=photo_url,
        delivery_state="queued",
        delivery_error="",
        delivery_retry_count=0,
        delivery_next_retry_at=next_retry_at,
        is_scheduled_message=is_scheduled_message,
    )
    _enqueue_outbox_message(
        db,
        conversation_id=conversation_id,
        chat_message_id=msg.id,
        target_chat_id=target_chat_id,
        target_user_id=target_user_id,
        operation="send_photo",
        payload={"photo_url": photo_url, "caption": caption},
        next_retry_at=next_retry_at,
    )


def _store_chat_message(
    db: Session,
    *,
    conversation_id: int,
    direction: str,
    source: str,
    text: str = "",
    image_url: str | None = None,
    image_urls_json: str | None = None,
    max_message_mid: str | None = None,
    link_mid: str | None = None,
    delivery_state: str = "sent",
    delivery_error: str = "",
    delivery_retry_count: int = 0,
    delivery_next_retry_at: datetime | None = None,
    is_scheduled_message: bool = False,
    workspace_id: int | None = None,
) -> ChatMessage:
    resolved_workspace_id = workspace_id
    if resolved_workspace_id is None:
        resolved_workspace_id = (
            db.query(Conversation.workspace_id)
            .filter(Conversation.id == conversation_id)
            .scalar()
        )
    if resolved_workspace_id is None:
        resolved_workspace_id = DEFAULT_WORKSPACE_ID
    is_read_by_customer = (direction != "bot")
    item = ChatMessage(
        workspace_id=resolved_workspace_id,
        conversation_id=conversation_id,
        direction=direction,
        source=source,
        text=text,
        image_url=image_url,
        image_urls_json=_normalize_image_urls_json(image_urls_json, image_url),
        max_message_mid=max_message_mid,
        link_mid=link_mid,
        delivery_state=delivery_state,
        is_scheduled_message=is_scheduled_message,
        delivery_error=delivery_error,
        delivery_retry_count=delivery_retry_count,
        delivery_next_retry_at=delivery_next_retry_at,
        is_read_by_customer=is_read_by_customer,
        read_at=(_as_naive_utc(_utc_now()) if is_read_by_customer else None),
    )
    db.add(item)
    db.commit()
    db.refresh(item)
    linked_media_paths = _chat_message_media_paths(item)
    if linked_media_paths:
        _link_chat_message_media_assets(
            db,
            chat_message_id=int(item.id),
            workspace_id=int(item.workspace_id or DEFAULT_WORKSPACE_ID),
            media_paths=linked_media_paths,
        )
    return item


def mark_messages_read_by_customer(
    db: Session,
    *,
    workspace_id: int,
    conversation_id: int,
    read_up_to_mid: str | None = None,
    read_at: datetime | None = None,
) -> int:
    """
    Mark outgoing bot messages as read by customer.
    If read_up_to_mid is provided, only messages up to that MID are marked.
    """
    query = db.query(ChatMessage).filter(
        ChatMessage.workspace_id == workspace_id,
        ChatMessage.conversation_id == conversation_id,
        ChatMessage.direction == "bot",
        ChatMessage.delivery_state == "sent",
    )
    if read_up_to_mid:
        target_mid = str(read_up_to_mid).strip()
        if not target_mid:
            return 0
        target_row = (
            query
            .filter(ChatMessage.max_message_mid == target_mid)
            .order_by(ChatMessage.id.desc())
            .first()
        )
        if target_row is None:
            return 0
        query = query.filter(ChatMessage.id <= int(target_row.id))
    rows = query.order_by(ChatMessage.id.asc()).all()
    updated = 0
    now_read = _as_naive_utc(read_at or _utc_now())
    for msg in rows:
        if not bool(getattr(msg, "is_read_by_customer", False)):
            msg.is_read_by_customer = True
            msg.read_at = now_read
            db.add(msg)
            updated += 1
    if updated:
        db.commit()
    return updated


def _mark_conversation_unread_from_customer(db: Session, *, meta: ConversationMeta) -> None:
    changed = False
    if not meta.is_unread:
        meta.is_unread = True
        changed = True
    # Reopen thread in queue when customer writes again.
    if meta.status != "new":
        meta.status = "new"
        changed = True
    if changed:
        db.add(meta)
        db.commit()


def _extract_contact_from_manager_text(text: str) -> str | None:
    normalized = text.strip()
    if not normalized:
        return None
    lowered = normalized.lower()
    if not lowered.startswith("/contact"):
        return None
    parts = normalized.split(maxsplit=1)
    if len(parts) < 2:
        return None
    return parts[1].strip() or None


def _parse_ticket_identifier(raw_value: str) -> int | None:
    value = (raw_value or "").strip()
    if not value:
        return None
    value = value.strip("[](){}<>")
    value = value.replace("Т", "T").replace("т", "t")
    value = value.replace("–", "-").replace("—", "-")
    value = value.upper()
    if value.startswith("T-"):
        value = value[2:]
    elif value.startswith("T"):
        value = value[1:]
    value = value.strip()
    if not value.isdigit():
        return None
    return int(value)


def _build_manager_ticket_inbox_text(db: Session, *, limit: int = 12) -> str:
    threads = load_chat_threads(db)
    if not threads:
        return "Активных тикетов пока нет."

    rows: list[str] = ["Тикеты (сначала новые):"]
    shown = 0
    for thread in threads:
        if thread.ticket_no is None:
            continue
        unread_mark = "●" if thread.is_unread else "○"
        preview = (thread.last_message_preview or "—").replace("\n", " ").strip()
        if len(preview) > 48:
            preview = preview[:47] + "…"
        rows.append(
            f"{unread_mark} T-{thread.ticket_no} {thread.customer_label} [{thread.status}] — {preview}"
        )
        shown += 1
        if shown >= limit:
            break

    rows.append("")
    rows.append("Ответ: /reply T-1001 ваш текст")
    rows.append("Шаблон: /reply T-1001 /price")
    return "\n".join(rows)


def _resolve_conversation_by_ticket(db: Session, ticket_no: int) -> Conversation | None:
    meta = db.query(ConversationMeta).filter(ConversationMeta.ticket_no == ticket_no).first()
    if meta is None:
        return None
    return db.query(Conversation).filter(Conversation.id == meta.conversation_id).first()


def _parse_manager_ticket_reply(text_value: str) -> tuple[int, str] | None:
    normalized = (text_value or "").strip()
    if not normalized:
        return None
    # Supports:
    # /reply T-1001 текст
    # /reply@botname T-1001 текст
    # /r T-1001 текст
    # /reply [T-1001] текст
    match = re.match(
        r"^/(?:reply|r)(?:@\S+)?\s+([^\s]+)\s+(.+)$",
        normalized,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not match:
        return None
    ticket_no = _parse_ticket_identifier(match.group(1))
    if ticket_no is None:
        return None
    payload = (match.group(2) or "").strip()
    if not payload:
        return None
    return ticket_no, payload


async def _send_manager_payload_to_conversation(
    db: Session,
    *,
    client: MaxClient,
    conversation: Conversation,
    manager_event: MaxWebhookEvent,
    payload_text: str,
) -> bool:
    customer_chat_id = conversation.chat_id
    customer_user_id = conversation.customer_account_id

    contact_from_text = _extract_contact_from_manager_text(payload_text)
    if contact_from_text:
        meta = (
            db.query(ConversationMeta)
            .filter(ConversationMeta.conversation_id == conversation.id)
            .first()
        )
        if meta:
            meta.phone_verified = True
            meta.phone_number = contact_from_text
            meta.status = "waiting_manager"
            db.add(meta)
            db.commit()
        _upsert_customer_profile(
            db=db,
            customer_id=conversation.customer_account_id,
            chat_id=conversation.chat_id,
            first_name=None,
            username=None,
            phone_number=contact_from_text,
        )
        return await enqueue_and_process_send_text(
            db,
            conversation_id=conversation.id,
            target_chat_id=customer_chat_id,
            target_user_id=customer_user_id,
            text=get_template_text(db, TEMPLATE_AFTER_PHONE),
            source="manager",
            link_mid=manager_event.link_mid,
            text_format="markdown",
        )

    if payload_text.startswith("/"):
        return await send_quick_reply_to_customer(
            db=db,
            conversation_id=conversation.id,
            customer_chat_id=customer_chat_id,
            customer_user_id=customer_user_id,
            command_text=payload_text,
        )

    return await enqueue_and_process_send_text(
        db,
        conversation_id=conversation.id,
        target_chat_id=customer_chat_id,
        target_user_id=customer_user_id,
        text=payload_text,
        source="manager",
        link_mid=manager_event.link_mid,
    )


def _make_panel_keyboard_for_ticket(ticket_no: int, status: str) -> list[dict]:
    upper_status = (status or "").strip().lower()
    if upper_status == "new":
        row = [
            {"type": "callback", "text": "Взять", "payload": f"mgr:take:{ticket_no}"},
            {"type": "callback", "text": "Готово", "payload": f"mgr:done:{ticket_no}"},
        ]
    elif upper_status == "in_progress":
        row = [
            {"type": "callback", "text": "Мой", "payload": f"mgr:mine:{ticket_no}"},
            {"type": "callback", "text": "Готово", "payload": f"mgr:done:{ticket_no}"},
        ]
    else:
        row = [{"type": "callback", "text": "Открыть", "payload": f"mgr:show:{ticket_no}"}]
    return [{"type": "inline_keyboard", "payload": {"buttons": [row]}}]


async def _send_manager_text_with_attachments(
    db: Session,
    *,
    conversation_id: int | None,
    target_user_id: str,
    text: str,
    attachments: list[dict] | None,
    source: str = "bot_system",
) -> bool:
    chat_message_id: int | None = None
    if conversation_id is not None:
        msg = _store_chat_message(
            db,
            conversation_id=conversation_id,
            direction="bot",
            source=source,
            text=text,
            delivery_state="queued",
            delivery_error="",
            delivery_retry_count=0,
            delivery_next_retry_at=_as_naive_utc(_utc_now()),
        )
        chat_message_id = msg.id
    item = _enqueue_outbox_message(
        db,
        conversation_id=conversation_id,
        chat_message_id=chat_message_id,
        target_chat_id="",
        target_user_id=target_user_id,
        operation="send_message",
        payload={"text": text, "attachments": attachments or []},
    )
    claimed_item = _claim_outbox_item_for_send(db, outbox_id=int(item.id))
    if claimed_item is None:
        return False
    client = _workspace_client(db, workspace_id=claimed_item.workspace_id)
    ok, _, _ = await _dispatch_outbox(db, client=client, item=claimed_item)
    return ok


def _load_meta_for_ticket(db: Session, ticket_no: int) -> tuple[ConversationMeta | None, Conversation | None]:
    meta = db.query(ConversationMeta).filter(ConversationMeta.ticket_no == ticket_no).first()
    if meta is None:
        return None, None
    conversation = db.query(Conversation).filter(Conversation.id == meta.conversation_id).first()
    return meta, conversation


def _ticket_brief_for_manager(
    db: Session,
    *,
    conversation: Conversation,
    meta: ConversationMeta,
) -> str:
    profile = (
        db.query(CustomerProfile)
        .filter(CustomerProfile.customer_account_id == conversation.customer_account_id)
        .first()
    )
    customer_name = (profile.first_name if profile and profile.first_name else "Покупатель").strip()
    username = (profile.username if profile and profile.username else "").strip()
    username_part = f" @{username}" if username else ""
    last_msg = (
        db.query(ChatMessage)
        .filter(ChatMessage.conversation_id == conversation.id)
        .order_by(ChatMessage.id.desc())
        .first()
    )
    preview = (last_msg.text or "").replace("\n", " ").strip() if last_msg else "—"
    if len(preview) > 80:
        preview = preview[:79] + "…"
    owner = (meta.manager_owner_id or "").strip() or "—"
    return (
        f"[T-{meta.ticket_no}] {customer_name}{username_part}\n"
        f"Статус: {meta.status}\n"
        f"Ответственный: {owner}\n"
        f"Клиент: {preview}\n"
        f"Ответ: /reply T-{meta.ticket_no} ваш текст"
    )


def _filter_threads_for_manager(
    db: Session,
    *,
    manager_id: str,
    mode: str,
) -> list[tuple[ChatThreadItem, ConversationMeta]]:
    threads = load_chat_threads(db)
    rows: list[tuple[ChatThreadItem, ConversationMeta]] = []
    for thread in threads:
        if thread.ticket_no is None:
            continue
        meta = db.query(ConversationMeta).filter(ConversationMeta.conversation_id == thread.conversation_id).first()
        if meta is None:
            continue
        if mode == "new" and meta.status != "new":
            continue
        if mode == "mine" and (meta.manager_owner_id or "").strip() != manager_id:
            continue
        rows.append((thread, meta))
    return rows


def _build_panel_summary_text(db: Session, *, manager_id: str) -> str:
    total_new = db.query(ConversationMeta).filter(ConversationMeta.status == "new").count()
    total_in_progress = db.query(ConversationMeta).filter(ConversationMeta.status == "in_progress").count()
    mine_in_progress = (
        db.query(ConversationMeta)
        .filter(
            ConversationMeta.status == "in_progress",
            ConversationMeta.manager_owner_id == manager_id,
        )
        .count()
    )
    total_done = db.query(ConversationMeta).filter(ConversationMeta.status == "done").count()
    return (
        "Диспетчер тикетов\n"
        f"Новые: {total_new}\n"
        f"В работе: {total_in_progress}\n"
        f"Мои в работе: {mine_in_progress}\n"
        f"Завершенные: {total_done}\n\n"
        "Команды: /new, /mine, /next, /take T-1001, /done T-1001"
    )


async def _send_dispatch_panel(
    db: Session,
    *,
    manager_id: str,
) -> bool:
    text = _build_panel_summary_text(db, manager_id=manager_id)
    keyboard = [
        {
            "type": "inline_keyboard",
            "payload": {
                "buttons": [
                    [
                        {"type": "callback", "text": "Новые", "payload": "mgr:new"},
                        {"type": "callback", "text": "Мои", "payload": "mgr:mine"},
                        {"type": "callback", "text": "След.", "payload": "mgr:next"},
                    ]
                ]
            },
        }
    ]
    return await _send_manager_text_with_attachments(
        db,
        conversation_id=None,
        target_user_id=manager_id,
        text=text,
        attachments=keyboard,
    )


async def _send_first_ticket_from_mode(
    db: Session,
    *,
    manager_id: str,
    mode: str,
) -> dict:
    rows = _filter_threads_for_manager(db, manager_id=manager_id, mode=mode)
    if not rows:
        msg = "Подходящих тикетов нет." if mode != "new" else "Новых тикетов нет."
        manager_workspace = (
            db.query(ServiceUser.workspace_id)
            .filter(ServiceUser.role == "manager", ServiceUser.max_account_id == manager_id)
            .scalar()
            or DEFAULT_WORKSPACE_ID
        )
        client = _workspace_client(db, workspace_id=int(manager_workspace))
        await client.send_text_to_user(user_id=manager_id, text=msg)
        return {"ok": True, "empty": True, "mode": mode}

    thread, meta = rows[0]
    conversation = db.query(Conversation).filter(Conversation.id == thread.conversation_id).first()
    if conversation is None:
        manager_workspace = (
            db.query(ServiceUser.workspace_id)
            .filter(ServiceUser.role == "manager", ServiceUser.max_account_id == manager_id)
            .scalar()
            or DEFAULT_WORKSPACE_ID
        )
        client = _workspace_client(db, workspace_id=int(manager_workspace))
        await client.send_text_to_user(user_id=manager_id, text="Тикет не найден.")
        return {"ok": True, "error": "conversation_not_found"}
    text = _ticket_brief_for_manager(db, conversation=conversation, meta=meta)
    attachments = _make_panel_keyboard_for_ticket(meta.ticket_no, meta.status)
    ok = await _send_manager_text_with_attachments(
        db,
        conversation_id=conversation.id,
        target_user_id=manager_id,
        text=text,
        attachments=attachments,
    )
    return {"ok": True, "ticket_sent": ok, "ticket_no": meta.ticket_no, "mode": mode}


async def _apply_ticket_action(
    db: Session,
    *,
    manager_id: str,
    action: str,
    ticket_no: int,
) -> dict:
    meta, conversation = _load_meta_for_ticket(db, ticket_no)
    if meta is None or conversation is None:
        manager_workspace = (
            db.query(ServiceUser.workspace_id)
            .filter(ServiceUser.role == "manager", ServiceUser.max_account_id == manager_id)
            .scalar()
            or DEFAULT_WORKSPACE_ID
        )
        client = _workspace_client(db, workspace_id=int(manager_workspace))
        await client.send_text_to_user(
            user_id=manager_id,
            text=f"Тикет T-{ticket_no} не найден.",
        )
        return {"ok": True, "ignored": "ticket_not_found", "ticket_no": ticket_no}

    result_text = ""
    normalized_action = action.strip().lower()
    if normalized_action == "take":
        meta.status = "in_progress"
        meta.manager_owner_id = manager_id
        result_text = f"Тикет T-{ticket_no} взят в работу."
    elif normalized_action == "done":
        meta.status = "done"
        meta.manager_owner_id = manager_id
        result_text = f"Тикет T-{ticket_no} отмечен как завершенный."
    elif normalized_action == "mine":
        owner = (meta.manager_owner_id or "").strip() or "—"
        result_text = f"T-{ticket_no}: статус={meta.status}, ответственный={owner}"
    elif normalized_action == "show":
        result_text = _ticket_brief_for_manager(db, conversation=conversation, meta=meta)
    else:
        return {"ok": True, "ignored": "unknown_action", "action": action}

    db.add(meta)
    db.commit()

    attachments = _make_panel_keyboard_for_ticket(meta.ticket_no, meta.status)
    if normalized_action in {"show", "mine"}:
        await _send_manager_text_with_attachments(
            db,
            conversation_id=conversation.id,
            target_user_id=manager_id,
            text=result_text,
            attachments=attachments,
        )
        return {"ok": True, "ticket_no": ticket_no, "action": normalized_action}

    await _send_manager_text_with_attachments(
        db,
        conversation_id=conversation.id,
        target_user_id=manager_id,
        text=result_text,
        attachments=attachments,
    )
    return {"ok": True, "ticket_no": ticket_no, "action": normalized_action}


def _parse_manager_ticket_action(text_value: str) -> tuple[str, int] | None:
    normalized = (text_value or "").strip()
    if not normalized:
        return None
    # Supports /take T-1001 and /take@bot T-1001 (same for /done).
    match = re.match(r"^/(take|done)(?:@\S+)?\s+([^\s]+)\s*$", normalized, flags=re.IGNORECASE)
    if not match:
        return None
    action = match.group(1).lower()
    ticket_no = _parse_ticket_identifier(match.group(2))
    if ticket_no is None:
        return None
    return action, ticket_no


def _parse_manager_callback_action(
    raw_payload: dict,
) -> tuple[str, int | None] | None:
    if not isinstance(raw_payload, dict):
        return None
    callback = raw_payload.get("callback")
    if not isinstance(callback, dict):
        callback = raw_payload.get("message_callback")
    if not isinstance(callback, dict):
        return None

    payload_value = (
        callback.get("payload")
        or callback.get("data")
        or callback.get("callback_data")
        or callback.get("command")
    )
    if payload_value is None:
        payload_root = raw_payload.get("payload")
        if isinstance(payload_root, dict):
            payload_value = payload_root.get("payload")
        elif isinstance(payload_root, str):
            payload_value = payload_root
    if not isinstance(payload_value, str):
        return None
    normalized = payload_value.strip().lower()
    if not normalized.startswith("mgr:"):
        return None
    parts = normalized.split(":")
    if len(parts) == 2 and parts[1] in {"new", "mine", "next", "panel"}:
        return parts[1], None
    if len(parts) == 3 and parts[1] in {"take", "done", "show"}:
        ticket_raw = parts[2].strip()
        ticket_no = _parse_ticket_identifier(ticket_raw)
        if ticket_no is None and ticket_raw.isdigit():
            ticket_no = int(ticket_raw)
        if ticket_no is None:
            return None
        return parts[1], ticket_no
    return None


def _is_start_intent_event(event: MaxWebhookEvent) -> bool:
    update_type = str(event.update_type or "").strip().lower()
    if update_type in {"bot_started", "bot_start"}:
        return True
    callback_payload = str(event.callback_payload or "").strip().lower()
    # Backward compatibility for previously sent callback-based start buttons.
    if callback_payload == "customer:start_fallback":
        return True
    text_value = str(event.text or "").strip().lower()
    if text_value in {"/start", "start", "bot_started", "bot_start"}:
        return True
    return False


async def _send_contact_request_prompt(
    db: Session,
    conversation_id: int,
    client: MaxClient,
    chat_id: str,
    user_id: str | None,
    text: str,
) -> dict:
    full_text = text
    attachments = [
        {
            "type": "inline_keyboard",
            "payload": {
                "buttons": [
                    [
                        {
                            "type": "request_contact",
                            "text": "Поделиться номером",
                        }
                    ]
                ]
            },
        }
    ]
    msg = _store_chat_message(
        db,
        conversation_id=conversation_id,
        direction="bot",
        source="bot_system",
        text=full_text,
        delivery_state="queued",
        delivery_error="",
        delivery_retry_count=0,
        delivery_next_retry_at=_as_naive_utc(_utc_now()),
    )
    item = _enqueue_outbox_message(
        db,
        conversation_id=conversation_id,
        chat_message_id=msg.id,
        target_chat_id=chat_id,
        target_user_id=user_id,
        operation="send_message",
        payload={"text": full_text, "attachments": attachments, "format": "markdown"},
    )
    claimed_item = _claim_outbox_item_for_send(db, outbox_id=int(item.id))
    if claimed_item is None:
        return {"success": False, "error": "outbox_claim_failed"}
    ok, _, result = await _dispatch_outbox(db, client=client, item=claimed_item)
    if ok:
        return result
    return {"success": False, **result}


async def _send_start_fallback_prompt(
    db: Session,
    *,
    conversation_id: int,
    client: MaxClient,
    chat_id: str,
    user_id: str | None,
    text: str,
) -> dict:
    attachments = [
        {
            "type": "inline_keyboard",
            "payload": {
                "buttons": [
                    [
                        {
                            # Primary start path should produce normal message_created text "/start".
                            "type": "message",
                            "text": "/start",
                        }
                    ]
                ]
            },
        }
    ]
    msg = _store_chat_message(
        db,
        conversation_id=conversation_id,
        direction="bot",
        source="bot_system",
        text=text,
        delivery_state="queued",
        delivery_error="",
        delivery_retry_count=0,
        delivery_next_retry_at=_as_naive_utc(_utc_now()),
    )
    item = _enqueue_outbox_message(
        db,
        conversation_id=conversation_id,
        chat_message_id=msg.id,
        target_chat_id=chat_id,
        target_user_id=user_id,
        operation="send_message",
        payload={"text": text, "attachments": attachments, "format": "markdown"},
    )
    claimed_item = _claim_outbox_item_for_send(db, outbox_id=int(item.id))
    if claimed_item is None:
        return {"success": False, "error": "outbox_claim_failed"}
    ok, _, result = await _dispatch_outbox(db, client=client, item=claimed_item)
    if ok:
        return result
    return {"success": False, **result}


def _ensure_blocked_folder(
    db: Session,
    *,
    workspace_id: int = DEFAULT_WORKSPACE_ID,
) -> ChatFolder:
    existing = (
        db.query(ChatFolder)
        .filter(
            ChatFolder.workspace_id == workspace_id,
            func.lower(ChatFolder.name) == BLOCKED_FOLDER_NAME.lower(),
        )
        .first()
    )
    if existing is not None:
        return existing
    return create_chat_folder(db, folder_name=BLOCKED_FOLDER_NAME, workspace_id=workspace_id)


def block_conversation(
    db: Session,
    conversation_id: int,
    *,
    actor_user_id: int | None = None,
    reason: str = "",
    workspace_id: int | None = None,
) -> bool:
    conversation_query = db.query(Conversation).filter(Conversation.id == conversation_id)
    if workspace_id is not None:
        conversation_query = conversation_query.filter(Conversation.workspace_id == workspace_id)
    conversation = conversation_query.first()
    if conversation is None:
        return False
    ws_id = int(conversation.workspace_id or DEFAULT_WORKSPACE_ID)
    meta_query = db.query(ConversationMeta).filter(ConversationMeta.conversation_id == conversation_id)
    if workspace_id is not None:
        meta_query = meta_query.filter(ConversationMeta.workspace_id == workspace_id)
    meta = meta_query.first()
    if meta is None:
        meta = _get_or_create_meta(db, conversation_id=conversation_id, workspace_id=ws_id)
    if bool(meta.is_blocked):
        return True
    blocked_folder = _ensure_blocked_folder(db, workspace_id=ws_id)
    current_folder_ids = get_conversation_folder_ids(
        db,
        conversation_id=conversation_id,
        workspace_id=ws_id,
    )
    previous_folder_id = (
        current_folder_ids[0]
        if current_folder_ids
        else (conversation.folder_id if conversation.folder_id else None)
    )
    target_folder_ids = list(current_folder_ids)
    if blocked_folder.id not in target_folder_ids:
        target_folder_ids.append(blocked_folder.id)
    replace_conversation_folder_links(
        db,
        conversation_id=conversation_id,
        folder_ids=target_folder_ids,
        workspace_id=ws_id,
        commit=False,
    )
    conversation.folder_id = blocked_folder.id
    meta.is_blocked = True
    meta.blocked_at = _as_naive_utc(_utc_now())
    meta.blocked_by_user_id = actor_user_id
    meta.blocked_reason = (reason or "").strip()
    meta.blocked_prev_folder_id = previous_folder_id
    db.add(conversation)
    db.add(meta)
    db.commit()
    return True


def unblock_conversation(
    db: Session,
    conversation_id: int,
    *,
    workspace_id: int | None = None,
) -> bool:
    conversation_query = db.query(Conversation).filter(Conversation.id == conversation_id)
    if workspace_id is not None:
        conversation_query = conversation_query.filter(Conversation.workspace_id == workspace_id)
    conversation = conversation_query.first()
    if conversation is None:
        return False
    ws_id = int(conversation.workspace_id or DEFAULT_WORKSPACE_ID)
    meta_query = db.query(ConversationMeta).filter(ConversationMeta.conversation_id == conversation_id)
    if workspace_id is not None:
        meta_query = meta_query.filter(ConversationMeta.workspace_id == workspace_id)
    meta = meta_query.first()
    if meta is None:
        meta = _get_or_create_meta(db, conversation_id=conversation_id, workspace_id=ws_id)
    if not bool(meta.is_blocked):
        return True
    blocked_folder_ids = {
        int(row[0])
        for row in (
            db.query(ChatFolder.id)
            .filter(
                ChatFolder.workspace_id == ws_id,
                func.lower(ChatFolder.name) == BLOCKED_FOLDER_NAME.lower(),
            )
            .all()
        )
        if row and row[0]
    }
    current_folder_ids = get_conversation_folder_ids(
        db,
        conversation_id=conversation_id,
        workspace_id=ws_id,
    )
    next_folder_ids = [
        folder_id
        for folder_id in current_folder_ids
        if int(folder_id) not in blocked_folder_ids
    ]
    replace_conversation_folder_links(
        db,
        conversation_id=conversation_id,
        folder_ids=next_folder_ids,
        workspace_id=ws_id,
        commit=False,
    )
    restore_folder_id = meta.blocked_prev_folder_id
    if restore_folder_id is not None and restore_folder_id in next_folder_ids:
        conversation.folder_id = restore_folder_id
    elif next_folder_ids:
        conversation.folder_id = int(next_folder_ids[0])
    else:
        conversation.folder_id = None
    meta.is_blocked = False
    meta.blocked_at = None
    meta.blocked_by_user_id = None
    meta.blocked_reason = ""
    meta.blocked_prev_folder_id = None
    db.add(conversation)
    db.add(meta)
    db.commit()
    return True


def block_conversation_customer(
    db: Session,
    conversation_id: int,
    *,
    actor_user_id: int | None = None,
    reason: str = "",
    workspace_id: int | None = None,
) -> bool:
    return block_conversation(
        db,
        conversation_id=conversation_id,
        actor_user_id=actor_user_id,
        reason=reason,
        workspace_id=workspace_id,
    )


def unblock_conversation_customer(
    db: Session,
    conversation_id: int,
    *,
    actor_user_id: int | None = None,  # kept for API symmetry/audit extensions
    workspace_id: int | None = None,
) -> bool:
    _ = actor_user_id
    return unblock_conversation(
        db,
        conversation_id=conversation_id,
        workspace_id=workspace_id,
    )


def is_conversation_customer_blocked(
    db: Session,
    *,
    chat_id: str,
    customer_id: str,
    workspace_id: int = DEFAULT_WORKSPACE_ID,
) -> bool:
    chat_value = str(chat_id or "").strip()
    customer_value = str(customer_id or "").strip()
    if not chat_value or not customer_value:
        return False
    conversation = (
        db.query(Conversation)
        .filter(
            Conversation.workspace_id == workspace_id,
            Conversation.chat_id == chat_value,
            Conversation.customer_account_id == customer_value,
        )
        .first()
    )
    if conversation is None:
        return False
    meta = (
        db.query(ConversationMeta)
        .filter(
            ConversationMeta.workspace_id == workspace_id,
            ConversationMeta.conversation_id == conversation.id,
        )
        .first()
    )
    return bool(meta.is_blocked) if meta else False


async def handle_customer_event(
    db: Session,
    client: MaxClient,
    settings: BotSettings,
    event: MaxWebhookEvent,
) -> dict:
    workspace_id = settings.workspace_id or DEFAULT_WORKSPACE_ID
    require_phone = bool(getattr(settings, "request_customer_phone", True))
    try:
        conversation = _get_or_create_conversation(
            db,
            chat_id=event.chat_id,
            customer_id=event.sender_id,
            workspace_id=workspace_id,
        )
    except ValueError:
        await client.send_text(
            chat_id=event.chat_id,
            text="Достигнут лимит активных диалогов по вашему workspace. Попробуйте позже.",
        )
        return {"ok": True, "flow": "dialogs_limit_exceeded"}
    meta = _get_or_create_meta(db, conversation_id=conversation.id, workspace_id=workspace_id)
    customer = _upsert_customer_profile(
        db=db,
        customer_id=event.sender_id,
        chat_id=event.chat_id,
        first_name=event.sender_first_name,
        username=event.sender_username,
        phone_number=event.contact_phone,
        workspace_id=workspace_id,
    )

    db.add(
        MessageLog(
            workspace_id=workspace_id,
            conversation_id=conversation.id,
            sender_account_id=event.sender_id,
            message_text=event.text or "",
            message_type="customer",
        )
    )
    db.commit()
    normalized_image_urls = await _materialize_incoming_image_urls(
        image_urls=[str(url).strip() for url in (getattr(event, "image_urls", []) or []) if str(url).strip()],
        client=client,
        workspace_id=int(workspace_id),
    )
    _store_chat_message(
        db,
        conversation_id=conversation.id,
        direction="customer",
        source="customer",
        text=event.text or "",
        image_url=(normalized_image_urls[0] if normalized_image_urls else None),
        image_urls_json=(
            json.dumps(normalized_image_urls, ensure_ascii=False)
            if normalized_image_urls
            else None
        ),
        max_message_mid=event.message_mid,
        link_mid=event.link_mid,
        workspace_id=workspace_id,
    )
    _mark_conversation_unread_from_customer(db, meta=meta)

    phone_just_verified = False
    # If phone is already available (privacy allows it), skip explicit phone-confirmation step.
    if event.contact_phone and not meta.phone_verified:
        meta.phone_verified = True
        meta.phone_number = event.contact_phone
        if meta.status == "new":
            meta.status = "waiting_manager"
        db.add(meta)
        db.commit()
        phone_just_verified = True
        _upsert_customer_profile(
            db=db,
            customer_id=event.sender_id,
            chat_id=event.chat_id,
            first_name=event.sender_first_name,
            username=event.sender_username,
            phone_number=event.contact_phone,
            workspace_id=workspace_id,
        )

    update_type = (event.update_type or "").strip().lower()
    is_bot_started = _is_start_intent_event(event)
    is_message_event = update_type in {"", "message_created", "message_callback", "new_message"}

    if bool(meta.is_blocked):
        last_notice = meta.blocked_notice_sent_at
        should_notify = True
        if isinstance(last_notice, datetime):
            delta = _as_naive_utc(_utc_now()) - _as_naive_utc(last_notice)
            should_notify = delta.total_seconds() >= BLOCKED_NOTICE_COOLDOWN_SECONDS
        if should_notify:
            await queue_only_send_text(
                db,
                conversation_id=conversation.id,
                target_chat_id=event.chat_id,
                target_user_id=event.sender_id,
                text="К сожалению, вы не можете писать в данный чат.",
                source="bot_system",
                text_format="markdown",
            )
            await process_outbox_queue(db, limit=20)
            meta.blocked_notice_sent_at = _as_naive_utc(_utc_now())
            db.add(meta)
            db.commit()
        return {"ok": True, "flow": "blocked_customer"}

    if is_message_event:
        await maybe_send_offhours_autoreply(
            db,
            conversation=conversation,
            meta=meta,
            event=event,
            client=client,
            workspace_id=workspace_id,
        )

    # Message before Start (custom behavior for message_created before start)
    # Do not intercept explicit /start text commands here: they should enter
    # the normal start flow and not loop back to prestart.
    text_value_normalized = str(event.text or "").strip().lower()
    is_manual_start_command = text_value_normalized in {"/start", "start", "bot_start", "bot_started"}
    if (
        not meta.start_prompt_sent
        and is_message_event
        and not is_bot_started
        and not event.contact_phone
        and not is_manual_start_command
    ):
        prestart_text = get_template_text(db, TEMPLATE_PRESTART, workspace_id=workspace_id)
        if prestart_text:
            await _send_start_fallback_prompt(
                db,
                conversation_id=conversation.id,
                client=client,
                chat_id=event.chat_id,
                user_id=event.sender_id,
                text=prestart_text,
            )
        return {"ok": True, "flow": "prestart"}

    # Start event should run onboarding only once for active dialog.
    # Re-run is allowed only when customer history contains only system/service
    # markers (bot_started, empty text, contact share), which is typical for
    # bot reinstall/new chat lifecycle before real customer messages.
    if is_bot_started:
        customer_history_rows = (
            db.query(ChatMessage.text, ChatMessage.max_message_mid, ChatMessage.link_mid)
            .filter(
                ChatMessage.workspace_id == workspace_id,
                ChatMessage.conversation_id == conversation.id,
                ChatMessage.direction == "customer",
            )
            .all()
        )
        has_substantive_customer_history = False
        for row in customer_history_rows:
            row_text = str((row[0] if row else "") or "").strip().lower()
            has_service_marker = bool((row[1] if row else None) or (row[2] if row else None))
            if row_text in {"", "/start", "start", "bot_started", "bot_start"}:
                continue
            # Contact share can arrive with empty text + service marker.
            if has_service_marker and not row_text:
                continue
            has_substantive_customer_history = True
            break
        if meta.start_prompt_sent and has_substantive_customer_history:
            return {"ok": True, "flow": "start_ignored_active_dialog"}
        meta.start_prompt_sent = True
        if require_phone:
            if not event.contact_phone:
                # Force explicit confirmation each time Start is pressed.
                meta.phone_verified = False
                db.add(meta)
                db.commit()
                start_text = get_template_text(db, TEMPLATE_START, workspace_id=workspace_id)
                await _send_contact_request_prompt(
                    db=db,
                    conversation_id=conversation.id,
                    client=client,
                    chat_id=event.chat_id,
                    user_id=event.sender_id,
                    text=start_text,
                )
                return {"ok": True, "flow": "start_prompt"}
            # If contact is already available in Start event payload, treat as verified.
            meta.phone_verified = True
            meta.phone_number = event.contact_phone
            if meta.status == "new":
                meta.status = "waiting_manager"
            db.add(meta)
            db.commit()
            await queue_only_send_text(
                db,
                conversation_id=conversation.id,
                target_chat_id=event.chat_id,
                target_user_id=event.sender_id,
                text=get_template_text(db, TEMPLATE_AFTER_PHONE, workspace_id=workspace_id),
                source="bot_system",
                text_format="markdown",
            )
            await process_outbox_queue(db, limit=20)
            return {"ok": True, "flow": "start_prompt_skipped_phone"}
        else:
            if not meta.phone_verified:
                meta.phone_verified = True
                if meta.status == "new":
                    meta.status = "waiting_manager"
                db.add(meta)
                db.commit()
            await queue_only_send_text(
                db,
                conversation_id=conversation.id,
                target_chat_id=event.chat_id,
                target_user_id=event.sender_id,
                text=get_template_text(db, TEMPLATE_AFTER_PHONE, workspace_id=workspace_id),
                source="bot_system",
                text_format="markdown",
            )
            await process_outbox_queue(db, limit=20)
            return {"ok": True, "flow": "start_prompt_phone_not_required"}

    if phone_just_verified:
        intro_steps = (
            db.query(IntroStep)
            .filter(
                IntroStep.workspace_id == workspace_id,
                IntroStep.is_active.is_(True),
            )
            .order_by(IntroStep.step_order.asc(), IntroStep.id.asc())
            .all()
        )
        if intro_steps:
            for step in intro_steps:
                if step.delay_seconds > 0:
                    await asyncio.sleep(min(step.delay_seconds, 30))
                text_value = (step.text or "").strip()
                if not text_value:
                    continue
                await queue_only_send_text(
                    db,
                    conversation_id=conversation.id,
                    target_chat_id=event.chat_id,
                    target_user_id=event.sender_id,
                    text=text_value,
                    source="bot_system",
                    text_format="markdown",
                )
            meta.intro_sent = True
            db.add(meta)
            db.commit()
        else:
            await queue_only_send_text(
                db,
                conversation_id=conversation.id,
                target_chat_id=event.chat_id,
                target_user_id=event.sender_id,
                text=get_template_text(db, TEMPLATE_AFTER_PHONE, workspace_id=workspace_id),
                source="bot_system",
                text_format="markdown",
            )
        await process_outbox_queue(db, limit=20)
        return {"ok": True, "flow": "phone_verified"}

    if require_phone and not meta.phone_verified and event.text.strip():
        return {"ok": True, "flow": "waiting_contact_confirmation"}

    # If phone request is disabled and conversation came from legacy state where
    # phone wasn't verified yet, mark it verified to keep downstream stats/status aligned.
    if not require_phone and not meta.phone_verified:
        meta.phone_verified = True
        if meta.status == "new":
            meta.status = "waiting_manager"
        db.add(meta)
        db.commit()

    # Customer message stays in thread for manager mini-app.
    if (meta.phone_verified or not require_phone) and event.text.strip():
        manager_id = _pick_manager_for_workspace(db, workspace_id=workspace_id, settings=settings)
        if manager_id:
            meta.manager_owner_id = manager_id
            db.add(meta)
            db.commit()
        return {"ok": True, "flow": "queued_for_mini_app"}

    return {"ok": True, "flow": "ignored_before_phone"}


async def forward_customer_message_to_manager(
    db: Session,
    client: MaxClient,
    settings: BotSettings,
    conversation: Conversation,
    meta: ConversationMeta,
    customer: CustomerProfile | None,
    event: MaxWebhookEvent,
    extra_header: str | None,
) -> ForwardResult:
    manager_ids = _parse_manager_ids(settings.manager_account_id)
    manager_user_id = manager_ids[0] if manager_ids else ""
    if not manager_user_id:
        return ForwardResult(ok=False, message="manager_account_id is empty")

    ticket_line = _render_ticket_line(meta, customer, event)
    text = event.text.strip() or "(без текста)"
    header = f"{extra_header}\n" if extra_header else ""
    manager_text = f"{header}{ticket_line}\nКлиент: {text}"

    ok = await enqueue_and_process_send_text(
        db,
        conversation_id=conversation.id,
        target_chat_id="",
        target_user_id=manager_user_id,
        text=manager_text,
        source="bot_system",
    )
    if not ok:
        return ForwardResult(ok=False, message="queue_or_send_failed")

    sent_msg = (
        db.query(ChatMessage)
        .filter(
            ChatMessage.conversation_id == conversation.id,
            ChatMessage.direction == "bot",
            ChatMessage.source == "bot_system",
            ChatMessage.text == manager_text,
        )
        .order_by(ChatMessage.id.desc())
        .first()
    )
    manager_mid = sent_msg.max_message_mid if sent_msg else None
    if manager_mid:
        db.add(
            ManagerDispatch(
                conversation_id=conversation.id,
                manager_message_mid=manager_mid,
                dispatch_type="customer_to_manager",
            )
        )
        db.commit()
    # Message was already stored in chat history by queue send.
    return ForwardResult(ok=True, message="sent")


async def handle_manager_message(
    db: Session,
    client: MaxClient,
    settings: BotSettings,
    event: MaxWebhookEvent,
) -> dict:
    manager_ids = _parse_manager_ids(settings.manager_account_id)
    if str(event.sender_id).strip() not in {str(item).strip() for item in manager_ids}:
        return {"ok": True, "ignored": "not_manager_sender"}
    manager_user_id = str(event.sender_id).strip()

    text_value = event.text.strip()
    if text_value.lower() in {"/help", "/h"}:
        await client.send_text_to_user(user_id=manager_user_id, text=MANAGER_HELP_TEXT)
        return {"ok": True, "help_sent": True}

    if text_value.lower() in {"/mini", "/app", "/miniapp"}:
        mini_token = create_manager_mini_token(
            manager_user_id,
            workspace_id=settings.workspace_id or DEFAULT_WORKSPACE_ID,
        )
        mini_url = (
            f"{app_settings.public_base_url.rstrip('/')}/mini/manager?token={quote_plus(mini_token)}"
        )
        await client.send_text_to_user(
            user_id=manager_user_id,
            text=(
                "Откройте mini-app менеджера:\n"
                f"{mini_url}"
            ),
        )
        return {"ok": True, "mini_sent": True}
    return {"ok": True, "ignored": "manager_chat_disabled"}


async def send_quick_reply_to_customer(
    db: Session,
    conversation_id: int,
    customer_chat_id: str,
    command_text: str,
    customer_user_id: str | None = None,
    *,
    owner_user_id: int = 0,
    sender_prefix: str | None = None,
    source: str = "manager",
    workspace_id: int = DEFAULT_WORKSPACE_ID,
    process_immediately: bool = True,
) -> tuple[bool, str]:
    command = command_text.strip().lstrip("/").strip().lower()
    if not command:
        return False, "Не найден быстрый ответ"

    quick_reply = (
        db.query(QuickReply)
        .filter(
            QuickReply.workspace_id == workspace_id,
            QuickReply.owner_user_id == int(owner_user_id or 0),
            QuickReply.command == command,
            QuickReply.is_active.is_(True),
        )
        .first()
    )
    if not quick_reply:
        return False, "Не найден быстрый ответ"

    media_items = (
        db.query(QuickReplyMedia)
        .filter(QuickReplyMedia.quick_reply_id == quick_reply.id)
        .order_by(QuickReplyMedia.sort_order.asc(), QuickReplyMedia.id.asc())
        .all()
    )
    media_urls: list[str] = []
    if media_items:
        for media in media_items:
            media_url = _normalize_media_public_url(media.media_path)
            if not media_url:
                continue
            media_urls.append(media_url)
    elif quick_reply.image_path:
        # Backward-compatibility for legacy quick replies with single image_path.
        legacy_path = _normalize_media_public_url(quick_reply.image_path)
        if legacy_path:
            media_urls.append(legacy_path)

    if quick_reply.text:
        rendered_text = (
            f"{sender_prefix}{quick_reply.text}" if sender_prefix is not None else quick_reply.text
        )
    else:
        rendered_text = ""

    if media_urls:
        if process_immediately:
            ok = await enqueue_and_process_send_media_group(
                db,
                conversation_id=conversation_id,
                target_chat_id=customer_chat_id,
                target_user_id=customer_user_id,
                photo_urls=media_urls,
                text=rendered_text,
                source=source,
            )
        else:
            await queue_only_send_media_group(
                db,
                conversation_id=conversation_id,
                target_chat_id=customer_chat_id,
                target_user_id=customer_user_id,
                photo_urls=media_urls,
                text=rendered_text,
                source=source,
            )
            ok = True
        if not ok:
            latest = (
                db.query(ChatMessage)
                .filter(
                    ChatMessage.conversation_id == int(conversation_id),
                    ChatMessage.direction == "bot",
                )
                .order_by(ChatMessage.id.desc())
                .first()
            )
            reason = str(getattr(latest, "delivery_error", "") or "").strip() or "Неизвестная ошибка отправки"
            return False, reason
    elif rendered_text:
        if process_immediately:
            ok = await enqueue_and_process_send_text(
                db,
                conversation_id=conversation_id,
                target_chat_id=customer_chat_id,
                target_user_id=customer_user_id,
                text=rendered_text,
                source=source,
                text_format="markdown",
            )
        else:
            await queue_only_send_text(
                db,
                conversation_id=conversation_id,
                target_chat_id=customer_chat_id,
                target_user_id=customer_user_id,
                text=rendered_text,
                source=source,
                text_format="markdown",
            )
            ok = True
        if not ok:
            latest = (
                db.query(ChatMessage)
                .filter(
                    ChatMessage.conversation_id == int(conversation_id),
                    ChatMessage.direction == "bot",
                )
                .order_by(ChatMessage.id.desc())
                .first()
            )
            reason = str(getattr(latest, "delivery_error", "") or "").strip() or "Неизвестная ошибка отправки"
            return False, reason

    return True, ""


def list_active_quick_replies(
    db: Session,
    *,
    workspace_id: int = DEFAULT_WORKSPACE_ID,
    owner_user_id: int = 0,
) -> list[QuickReply]:
    return (
        db.query(QuickReply)
        .filter(
            QuickReply.workspace_id == workspace_id,
            QuickReply.owner_user_id == int(owner_user_id or 0),
            QuickReply.is_active.is_(True),
        )
        .order_by(QuickReply.command.asc())
        .all()
    )


async def send_admin_quick_reply(
    db: Session,
    *,
    conversation_id: int,
    command_text: str,
    workspace_id: int | None = None,
    owner_user_id: int = 0,
) -> tuple[bool, str]:
    conversation = get_conversation_by_id(db, conversation_id, workspace_id=workspace_id)
    if conversation is None:
        return False, "Диалог не найден"
    # Queue-first path keeps UI request latency stable under media bursts.
    # Background worker drains outbox and delivers the payload shortly after.
    return await send_quick_reply_to_customer(
        db=db,
        conversation_id=conversation_id,
        customer_chat_id=conversation.chat_id,
        customer_user_id=conversation.customer_account_id,
        command_text=command_text,
        owner_user_id=owner_user_id,
        sender_prefix=None,
        source="bot_system",
        workspace_id=conversation.workspace_id,
        process_immediately=False,
    )


def load_chat_threads(
    db: Session,
    query: str = "",
    *,
    workspace_id: int = DEFAULT_WORKSPACE_ID,
    service_user_id: int = 0,
) -> list[ChatThreadItem]:
    conversations = (
        db.query(Conversation)
        .filter(Conversation.workspace_id == workspace_id)
        .order_by(Conversation.id.desc())
        .all()
    )
    conversation_ids = [int(item.id) for item in conversations]
    folder_links_map: dict[int, list[int]] = {}
    if conversation_ids:
        for conv_id, folder_id in (
            db.query(ConversationFolderLink.conversation_id, ConversationFolderLink.folder_id)
            .filter(
                ConversationFolderLink.workspace_id == workspace_id,
                ConversationFolderLink.conversation_id.in_(conversation_ids),
            )
            .all()
        ):
            conv_key = int(conv_id or 0)
            folder_key = int(folder_id or 0)
            if conv_key <= 0 or folder_key <= 0:
                continue
            links = folder_links_map.setdefault(conv_key, [])
            if folder_key not in links:
                links.append(folder_key)

    for conv in conversations:
        links = folder_links_map.setdefault(int(conv.id), [])
        if conv.folder_id and int(conv.folder_id) not in links:
            links.append(int(conv.folder_id))

    all_folder_ids = {
        int(folder_id)
        for folder_ids in folder_links_map.values()
        for folder_id in folder_ids
        if int(folder_id) > 0
    }
    folder_meta: dict[int, tuple[int, str]] = {}
    if all_folder_ids:
        for folder_id, sort_order, name in (
            db.query(ChatFolder.id, ChatFolder.sort_order, ChatFolder.name)
            .filter(
                ChatFolder.workspace_id == workspace_id,
                ChatFolder.id.in_(sorted(all_folder_ids)),
            )
            .all()
        ):
            folder_meta[int(folder_id)] = (int(sort_order or 0), str(name or ""))

    for conv_id, folder_ids in folder_links_map.items():
        folder_ids.sort(key=lambda fid: folder_meta.get(int(fid), (10**9, "")))

    pin_map: dict[int, int] = {}
    user_key = int(service_user_id or 0)
    if user_key >= 0 and conversation_ids:
        for conv_id, sort_order in (
            db.query(ConversationPin.conversation_id, ConversationPin.sort_order)
            .filter(
                ConversationPin.workspace_id == workspace_id,
                ConversationPin.service_user_id == user_key,
                ConversationPin.conversation_id.in_(conversation_ids),
            )
            .all()
        ):
            conv_key = int(conv_id or 0)
            if conv_key <= 0:
                continue
            pin_map[conv_key] = int(sort_order or 0)

    meta_by_conversation_id: dict[int, ConversationMeta] = {}
    profile_by_customer_id: dict[str, CustomerProfile] = {}
    last_msg_by_conversation_id: dict[int, ChatMessage] = {}
    failed_conversation_ids: set[int] = set()

    if conversation_ids:
        meta_rows = (
            db.query(ConversationMeta)
            .filter(
                ConversationMeta.workspace_id == workspace_id,
                ConversationMeta.conversation_id.in_(conversation_ids),
            )
            .all()
        )
        meta_by_conversation_id = {
            int(row.conversation_id): row for row in meta_rows if int(row.conversation_id or 0) > 0
        }

        customer_ids = list(
            {
                str(conv.customer_account_id or "").strip()
                for conv in conversations
                if str(conv.customer_account_id or "").strip()
            }
        )
        if customer_ids:
            profile_rows = (
                db.query(CustomerProfile)
                .filter(
                    CustomerProfile.workspace_id == workspace_id,
                    CustomerProfile.customer_account_id.in_(customer_ids),
                )
                .order_by(CustomerProfile.id.asc())
                .all()
            )
            for row in profile_rows:
                key = str(row.customer_account_id or "").strip()
                if not key or key in profile_by_customer_id:
                    continue
                profile_by_customer_id[key] = row

        last_msg_ids = (
            db.query(func.max(ChatMessage.id), ChatMessage.conversation_id)
            .filter(
                ChatMessage.workspace_id == workspace_id,
                ChatMessage.conversation_id.in_(conversation_ids),
            )
            .group_by(ChatMessage.conversation_id)
            .all()
        )
        latest_chat_message_ids = [
            int(row[0]) for row in last_msg_ids if row and int(row[0] or 0) > 0
        ]
        if latest_chat_message_ids:
            for row in (
                db.query(ChatMessage)
                .filter(ChatMessage.id.in_(latest_chat_message_ids))
                .all()
            ):
                conv_key = int(row.conversation_id or 0)
                if conv_key > 0:
                    last_msg_by_conversation_id[conv_key] = row

        failed_conversation_ids = {
            int(row[0] or 0)
            for row in (
                db.query(ChatMessage.conversation_id)
                .filter(
                    ChatMessage.workspace_id == workspace_id,
                    ChatMessage.conversation_id.in_(conversation_ids),
                    ChatMessage.direction == "bot",
                    ChatMessage.delivery_state == "failed",
                )
                .distinct()
                .all()
            )
            if row and int(row[0] or 0) > 0
        }

    needle = query.strip().lower()
    items: list[ChatThreadItem] = []
    for conv in conversations:
        conv_key = int(conv.id or 0)
        meta = meta_by_conversation_id.get(conv_key)
        profile = profile_by_customer_id.get(str(conv.customer_account_id or "").strip())
        last_msg = last_msg_by_conversation_id.get(conv_key)
        name = (profile.first_name if profile and profile.first_name else "Покупатель").strip()
        username = (profile.username if profile and profile.username else "").strip()
        label = f"{name}{(' @' + username) if username else ''}"
        preview = ""
        last_message_is_max_incoming = False
        if last_msg:
            preview = (last_msg.text or "").strip()
            if not preview and last_msg.image_url:
                preview = "[изображение]"
            last_message_is_max_incoming = (
                str(getattr(last_msg, "direction", "") or "").strip().lower() == "customer"
                and str(getattr(last_msg, "source", "") or "").strip().lower() == "customer"
            )
        status = meta.status if meta else "new"
        # Show unread highlight when there are unread messages OR manual reminder mark.
        is_unread = (bool(meta.is_unread) or bool(meta.manual_unread_mark)) if meta else True
        is_blocked = bool(meta.is_blocked) if meta else False
        phone_verified = bool(meta.phone_verified) if meta else False
        ticket_no = meta.ticket_no if meta else None
        last_activity_id = last_msg.id if last_msg else conv.id
        has_delivery_errors = conv_key in failed_conversation_ids

        searchable = " ".join(
            [
                conv.chat_id,
                conv.customer_account_id,
                label,
                preview,
                status,
                str(ticket_no or ""),
                profile.phone_number if profile and profile.phone_number else "",
            ]
        ).lower()
        if needle and needle not in searchable:
            continue

        conv_folder_ids = list(folder_links_map.get(int(conv.id), []))
        conv_folder_names = [
            folder_meta[int(folder_id)][1]
            for folder_id in conv_folder_ids
            if int(folder_id) in folder_meta and folder_meta[int(folder_id)][1]
        ]
        primary_folder_id = int(conv_folder_ids[0]) if conv_folder_ids else None
        primary_folder_name = conv_folder_names[0] if conv_folder_names else ""

        items.append(
            ChatThreadItem(
                conversation_id=conv.id,
                chat_id=conv.chat_id,
                customer_account_id=str(conv.customer_account_id or ""),
                ticket_no=ticket_no,
                customer_label=label,
                status=status,
                phone_verified=phone_verified,
                last_message_preview=preview,
                last_message_is_max_incoming=last_message_is_max_incoming,
                has_delivery_errors=has_delivery_errors,
                is_unread=is_unread,
                is_blocked=is_blocked,
                last_activity_id=last_activity_id,
                folder_id=primary_folder_id,
                folder_name=primary_folder_name,
                folder_ids=conv_folder_ids,
                folder_names=conv_folder_names,
                is_pinned=(int(conv.id) in pin_map),
                pin_order=(pin_map.get(int(conv.id)) if int(conv.id) in pin_map else None),
            )
        )
    # Pinned chats first (manual order), then unread/activity order.
    items.sort(
        key=lambda item: (
            0 if item.is_pinned else 1,
            int(item.pin_order or 10**9) if item.is_pinned else 10**9,
            0 if item.is_unread else 1,
            -item.last_activity_id,
        )
    )
    return items


def get_pinned_conversation_ids_for_user(
    db: Session,
    *,
    workspace_id: int,
    service_user_id: int,
) -> list[int]:
    user_key = int(service_user_id or 0)
    if user_key < 0:
        return []
    rows = (
        db.query(ConversationPin.conversation_id)
        .filter(
            ConversationPin.workspace_id == workspace_id,
            ConversationPin.service_user_id == user_key,
        )
        .order_by(ConversationPin.sort_order.asc(), ConversationPin.id.asc())
        .all()
    )
    return [int(row[0]) for row in rows if row and int(row[0] or 0) > 0]


def pin_conversation_for_user(
    db: Session,
    *,
    workspace_id: int,
    service_user_id: int,
    conversation_id: int,
) -> tuple[bool, str]:
    user_key = int(service_user_id or 0)
    conv_key = int(conversation_id or 0)
    if user_key < 0 or conv_key <= 0:
        return False, "invalid_arguments"

    conversation_exists = (
        db.query(Conversation.id)
        .filter(
            Conversation.workspace_id == workspace_id,
            Conversation.id == conv_key,
        )
        .first()
    )
    if conversation_exists is None:
        return False, "conversation_not_found"

    from app.ops import can_pin_chat

    existing = (
        db.query(ConversationPin)
        .filter(
            ConversationPin.workspace_id == workspace_id,
            ConversationPin.service_user_id == user_key,
            ConversationPin.conversation_id == conv_key,
        )
        .first()
    )
    if existing is not None:
        return True, ""

    can_pin, reason = can_pin_chat(db, workspace_id=workspace_id, service_user_id=user_key)
    if not can_pin:
        return False, reason

    max_sort_row = (
        db.query(func.max(ConversationPin.sort_order))
        .filter(
            ConversationPin.workspace_id == workspace_id,
            ConversationPin.service_user_id == user_key,
        )
        .first()
    )
    next_sort = int(max_sort_row[0] or 0) + 1
    db.add(
        ConversationPin(
            workspace_id=workspace_id,
            service_user_id=user_key,
            conversation_id=conv_key,
            sort_order=next_sort,
        )
    )
    db.commit()
    return True, ""


def unpin_conversation_for_user(
    db: Session,
    *,
    workspace_id: int,
    service_user_id: int,
    conversation_id: int,
) -> bool:
    user_key = int(service_user_id or 0)
    conv_key = int(conversation_id or 0)
    if user_key < 0 or conv_key <= 0:
        return False

    pin_row = (
        db.query(ConversationPin)
        .filter(
            ConversationPin.workspace_id == workspace_id,
            ConversationPin.service_user_id == user_key,
            ConversationPin.conversation_id == conv_key,
        )
        .first()
    )
    if pin_row is None:
        return False

    db.delete(pin_row)
    db.commit()
    _normalize_pins_sort_order(db, workspace_id=workspace_id, service_user_id=user_key)
    return True


def _normalize_pins_sort_order(
    db: Session,
    *,
    workspace_id: int,
    service_user_id: int,
) -> None:
    rows = (
        db.query(ConversationPin)
        .filter(
            ConversationPin.workspace_id == workspace_id,
            ConversationPin.service_user_id == int(service_user_id),
        )
        .order_by(ConversationPin.sort_order.asc(), ConversationPin.id.asc())
        .all()
    )
    changed = False
    for idx, row in enumerate(rows, start=1):
        if int(row.sort_order or 0) != idx:
            row.sort_order = idx
            db.add(row)
            changed = True
    if changed:
        db.commit()


def reorder_pins_for_user(
    db: Session,
    *,
    workspace_id: int,
    service_user_id: int,
    ordered_conversation_ids: list[int],
) -> bool:
    user_key = int(service_user_id or 0)
    if user_key < 0:
        return False
    normalized_ids: list[int] = []
    seen: set[int] = set()
    for raw in ordered_conversation_ids:
        conv_id = int(raw or 0)
        if conv_id <= 0 or conv_id in seen:
            continue
        seen.add(conv_id)
        normalized_ids.append(conv_id)
    if not normalized_ids:
        return False

    pins = (
        db.query(ConversationPin)
        .filter(
            ConversationPin.workspace_id == workspace_id,
            ConversationPin.service_user_id == user_key,
        )
        .all()
    )
    pins_by_conv = {int(row.conversation_id): row for row in pins}
    if set(normalized_ids) != set(pins_by_conv.keys()):
        return False

    changed = False
    for idx, conv_id in enumerate(normalized_ids, start=1):
        row = pins_by_conv.get(conv_id)
        if row is None:
            return False
        if int(row.sort_order or 0) != idx:
            row.sort_order = idx
            db.add(row)
            changed = True
    if changed:
        db.commit()
    return True


def delete_conversation(
    db: Session,
    conversation_id: int,
    *,
    workspace_id: int | None = None,
) -> bool:
    query = db.query(Conversation).filter(Conversation.id == conversation_id)
    if workspace_id is not None:
        query = query.filter(Conversation.workspace_id == workspace_id)
    conversation = query.first()
    if conversation is None:
        return False
    ws_id = conversation.workspace_id
    # Collect media paths before message rows are removed, so we can
    # safely delete now-unreferenced local files after conversation delete.
    media_paths_to_cleanup: list[str] = []
    chat_media_rows = (
        db.query(ChatMessage)
        .filter(
            ChatMessage.workspace_id == ws_id,
            ChatMessage.conversation_id == conversation_id,
        )
        .all()
    )
    for row in chat_media_rows:
        media_paths_to_cleanup.extend(_chat_message_media_paths(row))
    message_ids_to_remove = [int(row.id) for row in chat_media_rows if int(getattr(row, "id", 0) or 0) > 0]
    db.query(OutboxMessage).filter(
        OutboxMessage.workspace_id == ws_id,
        OutboxMessage.conversation_id == conversation_id,
    ).delete()
    db.query(ManagerDispatch).filter(
        ManagerDispatch.workspace_id == ws_id,
        ManagerDispatch.conversation_id == conversation_id,
    ).delete()
    db.query(ConversationFolderLink).filter(
        ConversationFolderLink.workspace_id == ws_id,
        ConversationFolderLink.conversation_id == conversation_id,
    ).delete()
    db.query(ConversationPin).filter(
        ConversationPin.workspace_id == ws_id,
        ConversationPin.conversation_id == conversation_id,
    ).delete()
    db.query(ConversationMeta).filter(
        ConversationMeta.workspace_id == ws_id,
        ConversationMeta.conversation_id == conversation_id,
    ).delete()
    db.query(ChatMessage).filter(
        ChatMessage.workspace_id == ws_id,
        ChatMessage.conversation_id == conversation_id,
    ).delete()
    if message_ids_to_remove:
        db.query(ChatMessageMedia).filter(
            ChatMessageMedia.workspace_id == ws_id,
            ChatMessageMedia.chat_message_id.in_(message_ids_to_remove),
        ).delete(synchronize_session=False)
    db.query(MessageLog).filter(
        MessageLog.workspace_id == ws_id,
        MessageLog.conversation_id == conversation_id,
    ).delete()
    db.delete(conversation)
    db.commit()
    for media_path in media_paths_to_cleanup:
        _try_delete_unreferenced_media_file(
            db,
            media_path=media_path,
        )
    return True


def _retention_policy_for_workspace(db: Session, *, workspace_id: int) -> WorkspaceRetentionPolicy:
    policy = (
        db.query(WorkspaceRetentionPolicy)
        .filter(WorkspaceRetentionPolicy.workspace_id == int(workspace_id))
        .first()
    )
    if policy is not None:
        return policy
    policy = WorkspaceRetentionPolicy(workspace_id=int(workspace_id))
    db.add(policy)
    db.commit()
    db.refresh(policy)
    return policy


def _normalize_storage_key(value: str | None) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    if raw.startswith("/static/"):
        return raw.removeprefix("/static/")
    return raw


def _guess_media_mime_type_from_path(path_value: str | None) -> str:
    ext = Path(str(path_value or "").strip()).suffix.lower()
    mapping = {
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".png": "image/png",
        ".gif": "image/gif",
        ".webp": "image/webp",
        ".bmp": "image/bmp",
    }
    return mapping.get(ext, "")


def _resolve_local_media_info_from_path(path_value: str | None) -> tuple[str, int]:
    media_file = _resolve_local_static_media_file(path_value)
    if media_file is None:
        return "", 0
    try:
        return str(media_file), int(media_file.stat().st_size or 0)
    except Exception:
        return str(media_file), 0


def _upsert_media_asset(
    db: Session,
    *,
    workspace_id: int,
    source_url: str,
) -> MediaAsset:
    normalized_url = str(source_url or "").strip()
    if not normalized_url:
        normalized_url = "unknown://empty"
    storage_key = _normalize_storage_key(normalized_url)
    provider = "local" if storage_key.startswith("uploads/") else "external"
    existing = (
        db.query(MediaAsset)
        .filter(
            MediaAsset.workspace_id == int(workspace_id),
            MediaAsset.storage_provider == provider,
            MediaAsset.storage_key == storage_key,
        )
        .first()
    )
    if existing is not None:
        if normalized_url and not str(existing.public_url or "").strip():
            existing.public_url = normalized_url
            db.add(existing)
            db.commit()
            db.refresh(existing)
        return existing

    local_file_path, local_size = _resolve_local_media_info_from_path(normalized_url)
    mime_type = _guess_media_mime_type_from_path(local_file_path or normalized_url)
    asset = MediaAsset(
        workspace_id=int(workspace_id),
        storage_provider=provider,
        storage_key=storage_key or normalized_url,
        public_url=upload_file_public_url(normalized_url),
        mime_type=mime_type,
        byte_size=int(local_size or 0),
    )
    db.add(asset)
    db.commit()
    db.refresh(asset)
    return asset


def _sync_chat_message_media_links(
    db: Session,
    *,
    chat_message_id: int,
    workspace_id: int,
    image_urls: list[str] | None = None,
) -> None:
    urls: list[str]
    if image_urls is not None:
        urls = [str(item).strip() for item in image_urls if str(item).strip()]
    else:
        row = (
            db.query(ChatMessage)
            .filter(ChatMessage.id == int(chat_message_id))
            .first()
        )
        if row is None:
            return
        urls = _chat_message_media_paths(row)
    if not urls:
        return

    existing_links = (
        db.query(ChatMessageMedia)
        .filter(ChatMessageMedia.chat_message_id == int(chat_message_id))
        .all()
    )
    existing_by_asset_id = {int(link.media_asset_id): link for link in existing_links}
    desired_asset_ids: list[int] = []
    changed = False
    for idx, url in enumerate(urls):
        asset = _upsert_media_asset(
            db,
            workspace_id=int(workspace_id),
            source_url=url,
        )
        asset_id = int(asset.id)
        desired_asset_ids.append(asset_id)
        link = existing_by_asset_id.get(asset_id)
        if link is None:
            db.add(
                ChatMessageMedia(
                    workspace_id=int(workspace_id),
                    chat_message_id=int(chat_message_id),
                    media_asset_id=asset_id,
                    sort_order=int(idx),
                    role="image",
                )
            )
            changed = True
        elif int(link.sort_order or 0) != int(idx):
            link.sort_order = int(idx)
            db.add(link)
            changed = True

    desired_asset_ids_set = set(desired_asset_ids)
    for link in existing_links:
        if int(link.media_asset_id) not in desired_asset_ids_set:
            db.delete(link)
            changed = True

    if changed:
        db.commit()


def _sync_quick_reply_media_asset_links(
    db: Session,
    *,
    quick_reply_id: int,
    workspace_id: int,
    media_paths: list[str] | None = None,
) -> None:
    if media_paths is None:
        media_rows = (
            db.query(QuickReplyMedia)
            .filter(QuickReplyMedia.quick_reply_id == int(quick_reply_id))
            .order_by(QuickReplyMedia.sort_order.asc(), QuickReplyMedia.id.asc())
            .all()
        )
        paths = [str(row.media_path or "").strip() for row in media_rows if str(row.media_path or "").strip()]
    else:
        paths = [str(item).strip() for item in media_paths if str(item).strip()]
    existing_links = (
        db.query(QuickReplyMediaAssetLink)
        .filter(QuickReplyMediaAssetLink.quick_reply_id == int(quick_reply_id))
        .all()
    )
    existing_by_asset_id = {int(link.media_asset_id): link for link in existing_links}
    desired_asset_ids: list[int] = []
    changed = False
    for idx, media_path in enumerate(paths):
        asset = _upsert_media_asset(
            db,
            workspace_id=int(workspace_id),
            source_url=media_path,
        )
        asset_id = int(asset.id)
        desired_asset_ids.append(asset_id)
        link = existing_by_asset_id.get(asset_id)
        if link is None:
            db.add(
                QuickReplyMediaAssetLink(
                    workspace_id=int(workspace_id),
                    quick_reply_id=int(quick_reply_id),
                    media_asset_id=asset_id,
                    sort_order=int(idx),
                )
            )
            changed = True
        elif int(link.sort_order or 0) != int(idx):
            link.sort_order = int(idx)
            db.add(link)
            changed = True
    desired_set = set(desired_asset_ids)
    for link in existing_links:
        if int(link.media_asset_id) not in desired_set:
            db.delete(link)
            changed = True
    if changed:
        db.commit()


def _remove_chat_message_media_links(
    db: Session,
    *,
    chat_message_ids: list[int] | set[int] | tuple[int, ...],
    workspace_id: int | None = None,
) -> int:
    normalized_ids = sorted(
        {
            int(value)
            for value in (chat_message_ids or [])
            if int(value or 0) > 0
        }
    )
    if not normalized_ids:
        return 0
    query = db.query(ChatMessageMedia).filter(ChatMessageMedia.chat_message_id.in_(normalized_ids))
    if workspace_id is not None:
        query = query.filter(ChatMessageMedia.workspace_id == int(workspace_id))
    removed = int(query.delete(synchronize_session=False) or 0)
    return removed


def cleanup_orphan_chat_message_media_links(
    db: Session,
    *,
    workspace_id: int | None = None,
    limit: int = 500,
) -> int:
    orphan_query = (
        db.query(ChatMessageMedia.id)
        .outerjoin(ChatMessage, ChatMessage.id == ChatMessageMedia.chat_message_id)
        .filter(ChatMessage.id.is_(None))
    )
    if workspace_id is not None:
        orphan_query = orphan_query.filter(ChatMessageMedia.workspace_id == int(workspace_id))
    orphan_ids = [
        int(row[0])
        for row in orphan_query
        .order_by(ChatMessageMedia.id.asc())
        .limit(max(1, int(limit)))
        .all()
        if row and int(row[0] or 0) > 0
    ]
    if not orphan_ids:
        return 0
    removed = int(
        db.query(ChatMessageMedia)
        .filter(ChatMessageMedia.id.in_(orphan_ids))
        .delete(synchronize_session=False)
        or 0
    )
    if removed:
        db.commit()
    return removed


def _filter_existing_local_media_urls(urls: list[str]) -> list[str]:
    filtered: list[str] = []
    seen: set[str] = set()
    for value in urls or []:
        url = str(value or "").strip()
        if not url or url in seen:
            continue
        seen.add(url)
        local_path = _to_local_static_media_path(url)
        # Do not expose stale /static URLs to UI; this avoids broken previews.
        if local_path:
            abs_local = local_upload_abspath(local_path)
            if abs_local is None or not abs_local.exists():
                continue
        filtered.append(url)
    return filtered


def list_chat_message_media_urls_map(
    db: Session,
    *,
    chat_message_ids: list[int],
) -> dict[int, list[str]]:
    ids = sorted({int(value) for value in (chat_message_ids or []) if int(value or 0) > 0})
    if not ids:
        return {}
    rows = (
        db.query(
            ChatMessageMedia.chat_message_id,
            MediaAsset.public_url,
            MediaAsset.storage_key,
        )
        .join(MediaAsset, MediaAsset.id == ChatMessageMedia.media_asset_id)
        .filter(ChatMessageMedia.chat_message_id.in_(ids))
        .order_by(
            ChatMessageMedia.chat_message_id.asc(),
            ChatMessageMedia.sort_order.asc(),
            ChatMessageMedia.id.asc(),
        )
        .all()
    )
    grouped: dict[int, list[str]] = {}
    for chat_message_id, public_url, storage_key in rows:
        message_id = int(chat_message_id or 0)
        if message_id <= 0:
            continue
        url = str(public_url or "").strip()
        if not url:
            key_value = str(storage_key or "").strip()
            if key_value.startswith("uploads/"):
                url = storage_public_url_for_key(storage_key=key_value)
        if not url:
            continue
        grouped.setdefault(message_id, []).append(url)
    deduped_grouped: dict[int, list[str]] = {}
    for message_id, values in grouped.items():
        deduped_grouped[message_id] = _filter_existing_local_media_urls(values)
    return deduped_grouped


def list_chat_message_media_urls(
    db: Session,
    *,
    chat_message_id: int,
) -> list[str]:
    return list_chat_message_media_urls_map(
        db,
        chat_message_ids=[int(chat_message_id or 0)],
    ).get(int(chat_message_id or 0), [])


def get_message_media_urls(
    item: ChatMessage,
    *,
    linked_urls_map: dict[int, list[str]] | None = None,
) -> list[str]:
    """Read message media from normalized links with legacy fallback."""
    preloaded_urls = getattr(item, "image_urls", None)
    if isinstance(preloaded_urls, list) and preloaded_urls:
        return _filter_existing_local_media_urls(preloaded_urls)
    legacy_urls_raw = _parse_image_urls_json(
        getattr(item, "image_urls_json", None),
        fallback_image_url=getattr(item, "image_url", None),
    )
    legacy_urls = _filter_existing_local_media_urls(legacy_urls_raw)
    message_id = int(getattr(item, "id", 0) or 0)
    if message_id > 0:
        try:
            if linked_urls_map is not None:
                linked_urls = _filter_existing_local_media_urls(linked_urls_map.get(message_id, []) or [])
            else:
                linked_urls: list[str] = []
                item_db = Session.object_session(item)  # type: ignore[arg-type]
                if item_db is not None:
                    linked_urls = list_chat_message_media_urls(
                        item_db,
                        chat_message_id=message_id,
                    )
            if linked_urls:
                linked_set = {str(url).strip() for url in linked_urls if str(url).strip()}
                legacy_set = {str(url).strip() for url in legacy_urls if str(url).strip()}
                # UI must prefer full message payload when normalized links are partial/stale.
                # This keeps operator history accurate (e.g. 2 sent photos must stay 2 in bubble).
                if legacy_urls and (
                    len(linked_set) != len(legacy_set)
                    or linked_set != legacy_set
                ):
                    return legacy_urls
                return linked_urls
        except Exception:
            pass
    return legacy_urls


def get_quick_reply_media_paths(db: Session, *, quick_reply_id: int) -> list[str]:
    links = (
        db.query(QuickReplyMediaAssetLink)
        .join(MediaAsset, MediaAsset.id == QuickReplyMediaAssetLink.media_asset_id)
        .filter(QuickReplyMediaAssetLink.quick_reply_id == int(quick_reply_id))
        .order_by(QuickReplyMediaAssetLink.sort_order.asc(), QuickReplyMediaAssetLink.id.asc())
        .all()
    )
    paths: list[str] = []
    for link in links:
        asset = (
            db.query(MediaAsset)
            .filter(MediaAsset.id == int(link.media_asset_id))
            .first()
        )
        if asset is None:
            continue
        public_url = str(asset.public_url or "").strip()
        storage_key = str(asset.storage_key or "").strip()
        if public_url:
            paths.append(public_url)
            continue
        if storage_key.startswith("uploads/"):
            paths.append(storage_public_url_for_key(storage_key=storage_key))
    deduped: list[str] = []
    seen: set[str] = set()
    for value in paths:
        if value in seen:
            continue
        seen.add(value)
        deduped.append(value)
    return deduped


def ensure_media_asset_for_path(
    db: Session,
    *,
    workspace_id: int,
    media_path: str,
) -> None:
    path_value = str(media_path or "").strip()
    if not path_value:
        return
    _upsert_media_asset(
        db,
        workspace_id=int(workspace_id),
        source_url=path_value,
    )


def sync_quick_reply_media_asset_links(
    db: Session,
    *,
    quick_reply_id: int,
    workspace_id: int,
) -> None:
    _sync_quick_reply_media_asset_links(
        db,
        quick_reply_id=int(quick_reply_id),
        workspace_id=int(workspace_id),
    )


def remove_quick_reply_media_asset_link(
    db: Session,
    *,
    quick_reply_id: int,
    media_path: str,
) -> None:
    path_value = str(media_path or "").strip()
    if not path_value:
        return
    storage_key = _normalize_storage_key(path_value)
    provider = "local" if storage_key.startswith("uploads/") else "external"
    asset = (
        db.query(MediaAsset)
        .filter(
            MediaAsset.storage_provider == provider,
            MediaAsset.storage_key == storage_key,
        )
        .first()
    )
    if asset is None:
        return
    (
        db.query(QuickReplyMediaAssetLink)
        .filter(
            QuickReplyMediaAssetLink.quick_reply_id == int(quick_reply_id),
            QuickReplyMediaAssetLink.media_asset_id == int(asset.id),
        )
        .delete(synchronize_session=False)
    )
    db.commit()


def _delete_rows_with_limit(query, *, model_id_column, limit: int) -> int:
    row_ids = [
        int(row[0])
        for row in query.order_by(model_id_column.asc()).limit(max(1, int(limit))).all()
        if row and row[0]
    ]
    if not row_ids:
        return 0
    deleted = (
        query.session.query(model_id_column.class_)
        .filter(model_id_column.in_(row_ids))
        .delete(synchronize_session=False)
    )
    return int(deleted or 0)


def _scan_orphan_upload_files(
    db: Session,
    *,
    now: datetime,
    deleted_media_grace_days: int,
) -> tuple[int, int]:
    orphans_scanned = 0
    orphans_deleted = 0
    files = list(iter_local_upload_files())
    if not files:
        return 0, 0
    grace_before = now - timedelta(days=deleted_media_grace_days)
    for file_path in files:
        orphans_scanned += 1
        if deleted_media_grace_days > 0:
            modified = datetime.utcfromtimestamp(file_path.stat().st_mtime)
            if modified >= grace_before:
                continue
        media_path = f"/static/uploads/{file_path.name}"
        before_exists = file_path.exists()
        _try_delete_unreferenced_media_file(db, media_path=media_path)
        if before_exists and not file_path.exists():
            orphans_deleted += 1
    return orphans_scanned, orphans_deleted


def run_storage_cleanup_cycle(
    db: Session,
    *,
    workspace_id: int,
    now: datetime | None = None,
    outbox_batch_limit: int = 500,
    logs_batch_limit: int = 1000,
    scan_orphans: bool = True,
) -> dict[str, int]:
    workspace_value = int(workspace_id or DEFAULT_WORKSPACE_ID)
    current = _as_naive_utc(now or _utc_now())
    policy = _retention_policy_for_workspace(db, workspace_id=workspace_value)

    outbox_sent_ttl_days = max(1, int(policy.outbox_sent_ttl_days or 30))
    outbox_failed_ttl_days = max(1, int(policy.outbox_failed_ttl_days or 90))
    logs_ttl_days = max(1, int(policy.message_logs_ttl_days or 180))
    deleted_media_grace_days = max(0, int(policy.deleted_media_grace_days or 7))

    sent_before = current - timedelta(days=outbox_sent_ttl_days)
    failed_before = current - timedelta(days=outbox_failed_ttl_days)
    logs_before = current - timedelta(days=logs_ttl_days)

    outbox_sent_ids_query = (
        db.query(OutboxMessage.id)
        .filter(
            OutboxMessage.workspace_id == workspace_value,
            OutboxMessage.state == "sent",
            OutboxMessage.sent_at.is_not(None),
            OutboxMessage.sent_at < sent_before,
        )
    )
    removed_outbox_sent = _delete_rows_with_limit(
        outbox_sent_ids_query,
        model_id_column=OutboxMessage.id,
        limit=outbox_batch_limit,
    )
    outbox_failed_ids_query = (
        db.query(OutboxMessage.id)
        .filter(
            OutboxMessage.workspace_id == workspace_value,
            OutboxMessage.state == "failed",
            OutboxMessage.updated_at < failed_before,
        )
    )
    removed_outbox_failed = _delete_rows_with_limit(
        outbox_failed_ids_query,
        model_id_column=OutboxMessage.id,
        limit=outbox_batch_limit,
    )
    logs_ids_query = (
        db.query(MessageLog.id)
        .filter(
            MessageLog.workspace_id == workspace_value,
            MessageLog.created_at < logs_before,
        )
    )
    removed_logs = _delete_rows_with_limit(
        logs_ids_query,
        model_id_column=MessageLog.id,
        limit=logs_batch_limit,
    )
    db.commit()

    orphans_scanned = 0
    orphans_deleted = 0
    if scan_orphans:
        orphans_scanned, orphans_deleted = _scan_orphan_upload_files(
            db,
            now=current,
            deleted_media_grace_days=deleted_media_grace_days,
        )

    return {
        "workspace_id": workspace_value,
        "removed_outbox_sent": int(removed_outbox_sent or 0),
        "removed_outbox_failed": int(removed_outbox_failed or 0),
        "removed_message_logs": int(removed_logs or 0),
        "orphans_scanned": int(orphans_scanned),
        "orphans_deleted": int(orphans_deleted),
    }


def run_storage_cleanup_for_all_workspaces(db: Session) -> dict[str, int]:
    started = _as_naive_utc(_utc_now())
    run = StorageCleanupRun(
        status="running",
        details_json="{}",
        started_at=started,
    )
    db.add(run)
    db.commit()
    db.refresh(run)
    total = {
        "workspaces_total": 0,
        "removed_outbox_sent": 0,
        "removed_outbox_failed": 0,
        "removed_message_logs": 0,
        "orphans_scanned": 0,
        "orphans_deleted": 0,
    }
    try:
        workspace_ids = [int(row[0]) for row in db.query(Workspace.id).all() if row and row[0]]
        if not workspace_ids:
            workspace_ids = [DEFAULT_WORKSPACE_ID]
        max_grace_days = 0
        for ws_id in sorted(set(workspace_ids)):
            result = run_storage_cleanup_cycle(
                db,
                workspace_id=int(ws_id),
                scan_orphans=False,
            )
            total["workspaces_total"] += 1
            total["removed_outbox_sent"] += int(result.get("removed_outbox_sent") or 0)
            total["removed_outbox_failed"] += int(result.get("removed_outbox_failed") or 0)
            total["removed_message_logs"] += int(result.get("removed_message_logs") or 0)
            policy = _retention_policy_for_workspace(db, workspace_id=int(ws_id))
            max_grace_days = max(max_grace_days, int(policy.deleted_media_grace_days or 7))
        scanned, deleted = _scan_orphan_upload_files(
            db,
            now=_as_naive_utc(_utc_now()),
            deleted_media_grace_days=max_grace_days,
        )
        total["orphans_scanned"] = int(scanned)
        total["orphans_deleted"] = int(deleted)
        run.status = "ok"
        run.finished_at = _as_naive_utc(_utc_now())
        run.details_json = json.dumps(total, ensure_ascii=False)
        db.add(run)
        db.commit()
        return total
    except Exception as exc:
        run.status = "failed"
        run.finished_at = _as_naive_utc(_utc_now())
        run.details_json = json.dumps({"error": str(exc)}, ensure_ascii=False)
        db.add(run)
        db.commit()
        raise


def backfill_chat_message_media_assets(
    db: Session,
    *,
    workspace_id: int | None = None,
    limit: int = 500,
) -> dict[str, int]:
    query = db.query(ChatMessage)
    if workspace_id is not None:
        query = query.filter(ChatMessage.workspace_id == int(workspace_id))
    rows = query.order_by(ChatMessage.id.asc()).limit(max(1, int(limit))).all()
    processed = 0
    linked = 0
    for row in rows:
        processed += 1
        urls = _parse_image_urls_json(
            getattr(row, "image_urls_json", None),
            fallback_image_url=getattr(row, "image_url", None),
        )
        if not urls:
            continue
        linked += _ensure_chat_message_media_links(
            db,
            chat_message=row,
            urls=urls,
        )
    if linked:
        db.commit()
    return {"processed": int(processed), "linked": int(linked)}


def backfill_quick_reply_media_assets(
    db: Session,
    *,
    workspace_id: int | None = None,
    limit: int = 500,
) -> dict[str, int]:
    query = db.query(QuickReplyMedia)
    if workspace_id is not None:
        query = query.filter(QuickReplyMedia.workspace_id == int(workspace_id))
    rows = query.order_by(QuickReplyMedia.id.asc()).limit(max(1, int(limit))).all()
    processed = 0
    linked = 0
    for row in rows:
        processed += 1
        media_path = str(row.media_path or "").strip()
        if not media_path:
            continue
        rel = media_path.removeprefix("/static/")
        storage_key = f"local:{rel}" if rel else f"url:{media_path}"
        workspace_value = int(getattr(row, "workspace_id", 0) or DEFAULT_WORKSPACE_ID)
        asset = _upsert_media_asset(
            db,
            workspace_id=workspace_value,
            storage_key=storage_key,
            public_url=media_path,
            mime_type="image/*",
            byte_size=0,
            sha256="",
        )
        exists = (
            db.query(QuickReplyMediaAssetLink.id)
            .filter(
                QuickReplyMediaAssetLink.workspace_id == workspace_value,
                QuickReplyMediaAssetLink.quick_reply_media_id == int(row.id),
                QuickReplyMediaAssetLink.media_asset_id == int(asset.id),
            )
            .first()
        )
        if exists:
            continue
        db.add(
            QuickReplyMediaAssetLink(
                workspace_id=workspace_value,
                quick_reply_media_id=int(row.id),
                media_asset_id=int(asset.id),
            )
        )
        linked += 1
    if linked:
        db.commit()
    return {"processed": int(processed), "linked": int(linked)}


def get_delivery_metrics(db: Session, *, workspace_id: int = DEFAULT_WORKSPACE_ID) -> DeliveryStats:
    total = db.query(OutboxMessage).filter(OutboxMessage.workspace_id == workspace_id).count()
    success_count = (
        db.query(OutboxMessage)
        .filter(OutboxMessage.workspace_id == workspace_id, OutboxMessage.state == "sent")
        .count()
    )
    failed_count = (
        db.query(OutboxMessage)
        .filter(OutboxMessage.workspace_id == workspace_id, OutboxMessage.state == "failed")
        .count()
    )
    # Count permanent failures by visible failed chat messages so the metric
    # resets after failed messages are removed from chat history.
    permanent_failures = (
        db.query(ChatMessage)
        .filter(
            ChatMessage.workspace_id == workspace_id,
            ChatMessage.delivery_state == "failed",
        )
        .count()
    )
    retry_sum = int(
        db.query(func.sum(OutboxMessage.retry_count))
        .filter(OutboxMessage.workspace_id == workspace_id)
        .scalar()
        or 0
    )
    return DeliveryStats(
        total_sent_attempts=total,
        success_count=success_count,
        failed_count=failed_count,
        retry_sum=retry_sum,
        permanent_failures=permanent_failures,
    )


def get_media_diagnostics_metrics(
    db: Session,
    *,
    workspace_id: int = DEFAULT_WORKSPACE_ID,
    window_minutes: int = 30,
) -> MediaDiagnosticsStats:
    window_value = max(5, min(int(window_minutes or 30), 24 * 60))
    since = _as_naive_utc(_utc_now() - timedelta(minutes=window_value))
    recent_rows = (
        db.query(OutboxMessage)
        .filter(
            OutboxMessage.workspace_id == int(workspace_id),
            OutboxMessage.created_at >= since,
        )
        .all()
    )
    total_outbox = len(recent_rows)
    sent_outbox = 0
    failed_outbox = 0
    deduped_outbox = 0
    send_message_total = 0
    send_message_sent = 0
    send_message_failed = 0
    media_send_total = 0
    media_send_sent = 0
    media_send_failed = 0
    fallback_markers = 0
    proto_payload_errors = 0
    upload_token_missing_errors = 0

    for row in recent_rows:
        state = str(getattr(row, "state", "") or "").strip().lower()
        operation = str(getattr(row, "operation", "") or "").strip().lower()
        payload_raw = str(getattr(row, "payload_json", "") or "").strip()
        last_error_raw = str(getattr(row, "last_error", "") or "").strip()
        last_error_lower = last_error_raw.lower()

        if state == "sent":
            sent_outbox += 1
        elif state == "failed":
            failed_outbox += 1

        if "deduplicated_by_fingerprint" in last_error_lower:
            deduped_outbox += 1
        if "proto.payload" in last_error_lower:
            proto_payload_errors += 1
        if "upload_token_missing" in last_error_lower:
            upload_token_missing_errors += 1

        if operation == "send_message":
            send_message_total += 1
            if state == "sent":
                send_message_sent += 1
            elif state == "failed":
                send_message_failed += 1
            if "fallback" in last_error_lower:
                fallback_markers += 1
            payload_lower = payload_raw.lower()
            if (
                '"images"' in payload_lower
                or '"attachments"' in payload_lower
                and '"type": "image"' in payload_lower
            ):
                media_send_total += 1
                if state == "sent":
                    media_send_sent += 1
                elif state == "failed":
                    media_send_failed += 1

    # Client sends multipart form-data with photo binaries; this estimate is for
    # "how close 10 photos can get to request-body limit" diagnostics in settings.
    max_upload_bytes_limit = int(getattr(app_settings, "max_upload_bytes", 0) or 0)
    estimated_payload_for_10_photos_bytes = max(0, int(max_upload_bytes_limit)) * 10
    estimated_payload_10_over_limit = (
        bool(max_upload_bytes_limit > 0)
        and int(estimated_payload_for_10_photos_bytes) > int(max_upload_bytes_limit)
    )

    return MediaDiagnosticsStats(
        window_minutes=window_value,
        total_outbox=total_outbox,
        sent_outbox=sent_outbox,
        failed_outbox=failed_outbox,
        deduped_outbox=deduped_outbox,
        send_message_total=send_message_total,
        send_message_sent=send_message_sent,
        send_message_failed=send_message_failed,
        media_send_total=media_send_total,
        media_send_sent=media_send_sent,
        media_send_failed=media_send_failed,
        fallback_markers=fallback_markers,
        proto_payload_errors=proto_payload_errors,
        upload_token_missing_errors=upload_token_missing_errors,
        estimated_payload_for_10_photos_bytes=int(estimated_payload_for_10_photos_bytes),
        max_upload_bytes_limit=int(max_upload_bytes_limit),
        estimated_payload_10_over_limit=bool(estimated_payload_10_over_limit),
    )


def get_media_send_diagnostics(
    db: Session,
    *,
    workspace_id: int = DEFAULT_WORKSPACE_ID,
    window_minutes: int = 30,
) -> dict[str, int | float]:
    stats = get_media_diagnostics_metrics(
        db,
        workspace_id=workspace_id,
        window_minutes=window_minutes,
    )
    return {
        "window_minutes": int(stats.window_minutes),
        "total_outbox": int(stats.total_outbox),
        "sent_outbox": int(stats.sent_outbox),
        "failed_outbox": int(stats.failed_outbox),
        "deduped_outbox": int(stats.deduped_outbox),
        "send_message_total": int(stats.send_message_total),
        "send_message_sent": int(stats.send_message_sent),
        "send_message_failed": int(stats.send_message_failed),
        "media_send_total": int(stats.media_send_total),
        "media_send_sent": int(stats.media_send_sent),
        "media_send_failed": int(stats.media_send_failed),
        "fallback_markers": int(stats.fallback_markers),
        "proto_payload_errors": int(stats.proto_payload_errors),
        "upload_token_missing_errors": int(stats.upload_token_missing_errors),
        "send_success_rate": float(stats.send_success_rate),
        "estimated_payload_for_10_photos_bytes": int(stats.estimated_payload_for_10_photos_bytes),
        "max_upload_bytes_limit": int(stats.max_upload_bytes_limit),
        "estimated_payload_10_over_limit": bool(stats.estimated_payload_10_over_limit),
    }


def get_media_diagnostics_metrics_snapshot(
    db: Session,
    *,
    workspace_id: int = DEFAULT_WORKSPACE_ID,
    window_minutes: int = 30,
) -> dict[str, int | float]:
    # Backward-compatible alias used by settings pages.
    return get_media_send_diagnostics(
        db,
        workspace_id=workspace_id,
        window_minutes=window_minutes,
    )


def get_chat_metrics(db: Session, *, workspace_id: int = DEFAULT_WORKSPACE_ID) -> dict[str, int]:
    return {
        "new_count": (
            db.query(ConversationMeta)
            .filter(
                ConversationMeta.workspace_id == workspace_id,
                ConversationMeta.status == "new",
            )
            .count()
        ),
        "in_progress_count": (
            db.query(ConversationMeta)
            .filter(
                ConversationMeta.workspace_id == workspace_id,
                ConversationMeta.status == "in_progress",
            )
            .count()
        ),
        "done_count": (
            db.query(ConversationMeta)
            .filter(
                ConversationMeta.workspace_id == workspace_id,
                ConversationMeta.status == "done",
            )
            .count()
        ),
    }


def get_conversation_meta(
    db: Session,
    conversation_id: int,
    *,
    workspace_id: int | None = None,
) -> ConversationMeta | None:
    query = db.query(ConversationMeta).filter(ConversationMeta.conversation_id == conversation_id)
    if workspace_id is not None:
        query = query.filter(ConversationMeta.workspace_id == workspace_id)
    return query.first()


def list_conversation_quick_commands(
    db: Session,
    conversation_id: int,
    limit: int = 8,
    *,
    workspace_id: int = DEFAULT_WORKSPACE_ID,
) -> list[str]:
    recent_commands = (
        db.query(ChatMessage.text)
        .filter(
            ChatMessage.workspace_id == workspace_id,
            ChatMessage.conversation_id == conversation_id,
            ChatMessage.direction == "bot",
            ChatMessage.source.in_(["manager", "bot_system"]),
        )
        .order_by(ChatMessage.id.desc())
        .limit(200)
        .all()
    )
    seen: list[str] = []
    for (text_value,) in recent_commands:
        if not text_value:
            continue
        normalized = text_value.strip()
        if not normalized.startswith("/"):
            continue
        command = normalized.split(maxsplit=1)[0].strip().lower()
        if command and command not in seen:
            seen.append(command)
        if len(seen) >= limit:
            break
    if seen:
        return seen
    return [f"/{item.command}" for item in list_active_quick_replies(db, workspace_id=workspace_id)[:limit]]


def list_chat_folders(db: Session, *, workspace_id: int = DEFAULT_WORKSPACE_ID) -> list[ChatFolder]:
    return (
        db.query(ChatFolder)
        .filter(ChatFolder.workspace_id == workspace_id)
        .order_by(ChatFolder.sort_order.asc(), ChatFolder.name.asc())
        .all()
    )


def create_chat_folder(
    db: Session,
    folder_name: str,
    *,
    workspace_id: int = DEFAULT_WORKSPACE_ID,
) -> ChatFolder:
    normalized = folder_name.strip()
    if not normalized:
        raise ValueError("folder_name is empty")
    exists = (
        db.query(ChatFolder)
        .filter(
            ChatFolder.workspace_id == workspace_id,
            func.lower(ChatFolder.name) == normalized.lower(),
        )
        .first()
    )
    if exists:
        return exists
    max_sort = (
        db.query(func.max(ChatFolder.sort_order))
        .filter(ChatFolder.workspace_id == workspace_id)
        .scalar()
    )
    folder = ChatFolder(
        workspace_id=workspace_id,
        name=normalized,
        sort_order=(int(max_sort or 0) + 1),
    )
    db.add(folder)
    try:
        db.commit()
        db.refresh(folder)
        return folder
    except IntegrityError:
        # Concurrent/create-race safe path for workspace+name unique index.
        db.rollback()
        existing = (
            db.query(ChatFolder)
            .filter(
                ChatFolder.workspace_id == workspace_id,
                ChatFolder.name == normalized,
            )
            .first()
        )
        if existing is None:
            existing = (
                db.query(ChatFolder)
                .filter(
                    ChatFolder.workspace_id == workspace_id,
                    func.lower(ChatFolder.name) == normalized.lower(),
                )
                .first()
            )
        if existing is not None:
            return existing
        raise


def delete_chat_folders(
    db: Session,
    *,
    folder_ids: list[int],
    workspace_id: int = DEFAULT_WORKSPACE_ID,
) -> list[int]:
    normalized_ids: list[int] = []
    seen: set[int] = set()
    for value in folder_ids:
        folder_id = int(value or 0)
        if folder_id <= 0 or folder_id in seen:
            continue
        seen.add(folder_id)
        normalized_ids.append(folder_id)
    if not normalized_ids:
        return []

    target_rows = (
        db.query(ChatFolder.id, ChatFolder.name)
        .filter(
            ChatFolder.workspace_id == workspace_id,
            ChatFolder.id.in_(normalized_ids),
        )
        .all()
    )
    if not target_rows:
        return []

    # System folder for blocked users must remain available.
    protected_name = BLOCKED_FOLDER_NAME.strip().lower()
    target_ids = [
        int(row[0])
        for row in target_rows
        if str(row[1] or "").strip().lower() != protected_name
    ]
    if not target_ids:
        return []

    affected_conversation_ids = [
        int(row[0])
        for row in (
            db.query(ConversationFolderLink.conversation_id)
            .filter(
                ConversationFolderLink.workspace_id == workspace_id,
                ConversationFolderLink.folder_id.in_(target_ids),
            )
            .distinct()
            .all()
        )
        if row and row[0]
    ]

    db.query(ConversationFolderLink).filter(
        ConversationFolderLink.workspace_id == workspace_id,
        ConversationFolderLink.folder_id.in_(target_ids),
    ).delete(synchronize_session=False)

    if affected_conversation_ids:
        conversations = (
            db.query(Conversation)
            .filter(
                Conversation.workspace_id == workspace_id,
                Conversation.id.in_(affected_conversation_ids),
            )
            .all()
        )
        for conversation in conversations:
            first_link = (
                db.query(ConversationFolderLink.folder_id)
                .filter(
                    ConversationFolderLink.workspace_id == workspace_id,
                    ConversationFolderLink.conversation_id == int(conversation.id),
                )
                .order_by(ConversationFolderLink.id.asc())
                .first()
            )
            conversation.folder_id = int(first_link[0]) if first_link else None
            db.add(conversation)

    db.query(Conversation).filter(
        Conversation.workspace_id == workspace_id,
        Conversation.folder_id.in_(target_ids),
    ).update({Conversation.folder_id: None}, synchronize_session=False)

    db.query(ConversationMeta).filter(
        ConversationMeta.workspace_id == workspace_id,
        ConversationMeta.blocked_prev_folder_id.in_(target_ids),
    ).update({ConversationMeta.blocked_prev_folder_id: None}, synchronize_session=False)

    db.query(ChatFolder).filter(
        ChatFolder.workspace_id == workspace_id,
        ChatFolder.id.in_(target_ids),
    ).delete(synchronize_session=False)

    db.commit()
    return target_ids


def get_conversation_folder_links(
    db: Session,
    *,
    conversation_id: int,
    workspace_id: int | None = None,
) -> list[ConversationFolderLink]:
    conversation_query = db.query(Conversation).filter(Conversation.id == conversation_id)
    if workspace_id is not None:
        conversation_query = conversation_query.filter(Conversation.workspace_id == workspace_id)
    conversation = conversation_query.first()
    if conversation is None:
        return []
    ws_id = int(conversation.workspace_id or DEFAULT_WORKSPACE_ID)
    links = (
        db.query(ConversationFolderLink)
        .filter(
            ConversationFolderLink.workspace_id == ws_id,
            ConversationFolderLink.conversation_id == conversation_id,
        )
        .order_by(ConversationFolderLink.id.asc())
        .all()
    )
    if links:
        return links
    if conversation.folder_id:
        folder = (
            db.query(ChatFolder.id)
            .filter(
                ChatFolder.workspace_id == ws_id,
                ChatFolder.id == int(conversation.folder_id),
            )
            .first()
        )
        if folder is not None:
            link = ConversationFolderLink(
                workspace_id=ws_id,
                conversation_id=conversation_id,
                folder_id=int(conversation.folder_id),
            )
            db.add(link)
            db.commit()
            db.refresh(link)
            links = [link]
    return links


def get_conversation_folder_ids(
    db: Session,
    *,
    conversation_id: int,
    workspace_id: int | None = None,
) -> list[int]:
    links = get_conversation_folder_links(
        db,
        conversation_id=conversation_id,
        workspace_id=workspace_id,
    )
    folder_ids: list[int] = []
    seen: set[int] = set()
    for link in links:
        folder_id = int(link.folder_id or 0)
        if folder_id <= 0 or folder_id in seen:
            continue
        seen.add(folder_id)
        folder_ids.append(folder_id)
    return folder_ids


def replace_conversation_folder_links(
    db: Session,
    *,
    conversation_id: int,
    folder_ids: list[int],
    workspace_id: int | None = None,
    commit: bool = True,
) -> bool:
    conversation_query = db.query(Conversation).filter(Conversation.id == conversation_id)
    if workspace_id is not None:
        conversation_query = conversation_query.filter(Conversation.workspace_id == workspace_id)
    conversation = conversation_query.first()
    if conversation is None:
        return False
    ws_id = int(conversation.workspace_id or DEFAULT_WORKSPACE_ID)
    normalized_ids: list[int] = []
    seen: set[int] = set()
    for value in folder_ids:
        folder_id = int(value or 0)
        if folder_id <= 0 or folder_id in seen:
            continue
        seen.add(folder_id)
        normalized_ids.append(folder_id)
    if normalized_ids:
        valid_ids = {
            int(row[0])
            for row in (
                db.query(ChatFolder.id)
                .filter(
                    ChatFolder.workspace_id == ws_id,
                    ChatFolder.id.in_(normalized_ids),
                )
                .all()
            )
        }
        if len(valid_ids) != len(normalized_ids):
            return False
    db.query(ConversationFolderLink).filter(
        ConversationFolderLink.workspace_id == ws_id,
        ConversationFolderLink.conversation_id == conversation_id,
    ).delete()
    for folder_id in normalized_ids:
        db.add(
            ConversationFolderLink(
                workspace_id=ws_id,
                conversation_id=conversation_id,
                folder_id=folder_id,
            )
        )
    conversation.folder_id = normalized_ids[0] if normalized_ids else None
    db.add(conversation)
    if commit:
        db.commit()
    return True


def assign_conversation_to_folder(
    db: Session,
    *,
    conversation_id: int,
    folder_id: int | None,
    workspace_id: int | None = None,
) -> bool:
    next_folder_ids = [int(folder_id)] if folder_id is not None else []
    return replace_conversation_folder_links(
        db,
        conversation_id=conversation_id,
        folder_ids=next_folder_ids,
        workspace_id=workspace_id,
        commit=True,
    )


def mark_thread_unread(
    db: Session,
    conversation_id: int,
    *,
    workspace_id: int | None = None,
) -> bool:
    query = db.query(ConversationMeta).filter(ConversationMeta.conversation_id == conversation_id)
    if workspace_id is not None:
        query = query.filter(ConversationMeta.workspace_id == workspace_id)
    meta = query.first()
    if meta is None:
        return False
    if bool(meta.manual_unread_mark):
        return True
    meta.manual_unread_mark = True
    db.add(meta)
    db.commit()
    return True


def load_chat_messages(
    db: Session,
    conversation_id: int,
    *,
    workspace_id: int | None = None,
    limit: int | None = None,
) -> list[ChatMessage]:
    query = db.query(ChatMessage).filter(ChatMessage.conversation_id == conversation_id)
    if workspace_id is not None:
        query = query.filter(ChatMessage.workspace_id == workspace_id)
    limit_value = int(limit or 0)
    if limit_value > 0:
        rows = (
            query
            .order_by(ChatMessage.id.desc())
            .limit(limit_value)
            .all()
        )
        rows.reverse()
        return rows
    return (
        query
        .order_by(ChatMessage.id.asc())
        .all()
    )


def get_conversation_by_id(
    db: Session,
    conversation_id: int,
    *,
    workspace_id: int | None = None,
) -> Conversation | None:
    query = db.query(Conversation).filter(Conversation.id == conversation_id)
    if workspace_id is not None:
        query = query.filter(Conversation.workspace_id == workspace_id)
    return query.first()


def mark_thread_read(
    db: Session,
    conversation_id: int,
    *,
    workspace_id: int | None = None,
) -> None:
    query = db.query(ConversationMeta).filter(ConversationMeta.conversation_id == conversation_id)
    if workspace_id is not None:
        query = query.filter(ConversationMeta.workspace_id == workspace_id)
    meta = query.first()
    if meta is None:
        return
    if meta.status != "read" or meta.is_unread or bool(meta.manual_unread_mark):
        meta.status = "read"
        meta.is_unread = False
        meta.manual_unread_mark = False
        db.add(meta)
        db.commit()


async def send_admin_chat_message(
    db: Session,
    *,
    conversation_id: int,
    text: str,
    image_paths: list[str] | None = None,
    workspace_id: int | None = None,
    schedule_at_iso: str = "",
) -> tuple[bool, str]:
    conversation = get_conversation_by_id(db, conversation_id, workspace_id=workspace_id)
    if conversation is None:
        return False, "Диалог не найден"
    scheduled_for = _parse_schedule_at_iso(schedule_at_iso)
    sent_any = False

    normalized_image_paths = [str(item).strip() for item in (image_paths or []) if str(item).strip()]

    # If message contains media, send one grouped message with optional text
    # (Telegram-style UX). Plain text-only path stays unchanged.
    if normalized_image_paths:
        image_urls = [str(path).strip() for path in normalized_image_paths if str(path).strip()]
        if scheduled_for:
            await queue_only_send_media_group(
                db,
                conversation_id=conversation_id,
                target_chat_id=conversation.chat_id,
                target_user_id=conversation.customer_account_id,
                photo_urls=image_urls,
                text=(text or "").strip(),
                source="bot_system",
                scheduled_for=scheduled_for,
            )
            sent_any = True
        else:
            ok = await enqueue_and_process_send_media_group(
                db,
                conversation_id=conversation_id,
                target_chat_id=conversation.chat_id,
                target_user_id=conversation.customer_account_id,
                photo_urls=image_urls,
                text=(text or "").strip(),
                source="bot_system",
            )
            if ok:
                sent_any = True
        if sent_any:
            return True, ""
        latest = (
            db.query(ChatMessage)
            .filter(
                ChatMessage.conversation_id == int(conversation_id),
                ChatMessage.direction == "bot",
            )
            .order_by(ChatMessage.id.desc())
            .first()
        )
        reason = str(getattr(latest, "delivery_error", "") or "").strip() or "Неизвестная ошибка отправки"
        return False, reason

    if text:
        if scheduled_for:
            await queue_only_send_text(
                db,
                conversation_id=conversation_id,
                target_chat_id=conversation.chat_id,
                target_user_id=conversation.customer_account_id,
                text=text,
                source="bot_system",
                scheduled_for=scheduled_for,
            )
            ok = True
        else:
            ok = await enqueue_and_process_send_text(
                db,
                conversation_id=conversation_id,
                target_chat_id=conversation.chat_id,
                target_user_id=conversation.customer_account_id,
                text=text,
                source="bot_system",
            )
        if ok:
            sent_any = True
        else:
            latest = (
                db.query(ChatMessage)
                .filter(
                    ChatMessage.conversation_id == int(conversation_id),
                    ChatMessage.direction == "bot",
                )
                .order_by(ChatMessage.id.desc())
                .first()
            )
            reason = str(getattr(latest, "delivery_error", "") or "").strip() or "Неизвестная ошибка отправки"
            return False, reason

    return sent_any, ""


async def update_chat_message_text(
    db: Session,
    chat_message_id: int,
    new_text: str,
    *,
    workspace_id: int | None = None,
) -> ChatMessage | None:
    query = db.query(ChatMessage).filter(ChatMessage.id == chat_message_id)
    if workspace_id is not None:
        query = query.filter(ChatMessage.workspace_id == workspace_id)
    msg = query.first()
    if not msg:
        return None
    text_value = new_text.strip()
    if msg.max_message_mid:
        client = _workspace_client(db, workspace_id=msg.workspace_id)
        await client.edit_message(message_id=msg.max_message_mid, text=text_value)
    msg.text = text_value
    db.add(msg)
    db.commit()
    db.refresh(msg)
    return msg


async def remove_chat_message(
    db: Session,
    chat_message_id: int,
    *,
    workspace_id: int | None = None,
) -> bool:
    query = db.query(ChatMessage).filter(ChatMessage.id == chat_message_id)
    if workspace_id is not None:
        query = query.filter(ChatMessage.workspace_id == workspace_id)
    msg = query.first()
    if not msg:
        return False
    if msg.max_message_mid:
        client = _workspace_client(db, workspace_id=msg.workspace_id)
        await client.delete_message(message_id=msg.max_message_mid)
    media_paths = _chat_message_media_paths(msg)
    _remove_chat_message_media_links(
        db,
        chat_message_ids=[int(chat_message_id)],
        workspace_id=int(getattr(msg, "workspace_id", 0) or 0) or workspace_id,
    )
    db.delete(msg)
    db.commit()
    for media_path in media_paths:
        _try_delete_unreferenced_media_file(
            db,
            media_path=media_path,
            skip_chat_message_id=chat_message_id,
        )
    return True
