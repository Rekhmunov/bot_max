from __future__ import annotations

import json
import re
from datetime import UTC, datetime, timedelta
from dataclasses import dataclass
from typing import Optional
from urllib.parse import quote_plus

from sqlalchemy import func
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.auth import create_manager_mini_token
from app.config import settings as app_settings
from app.max_client import MaxClient
from app.models import (
    BotSettings,
    ChatMessage,
    ChatFolder,
    Conversation,
    ConversationMeta,
    CustomerProfile,
    IntroStep,
    ManagerDispatch,
    MessageLog,
    MessageTemplate,
    OutboxMessage,
    QuickReply,
    QuickReplyMedia,
    ServiceUser,
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


def _workspace_client(db: Session, *, workspace_id: int) -> MaxClient:
    settings_row = db.query(BotSettings).filter(BotSettings.workspace_id == workspace_id).first()
    token = (settings_row.bot_token if settings_row is not None else "") or ""
    return MaxClient(token=token.strip())


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
    has_delivery_errors: bool
    is_unread: bool
    is_blocked: bool
    last_activity_id: int
    folder_id: int | None
    folder_name: str


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
    message = send_result.get("message") if isinstance(send_result.get("message"), dict) else {}
    body = message.get("body") if isinstance(message.get("body"), dict) else {}
    mid = body.get("mid") or message.get("mid")
    return str(mid) if mid is not None else None


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _parse_schedule_at_iso(value: str) -> datetime | None:
    raw = (value or "").strip()
    if not raw:
        return None
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

    if lowered_code == "chat.not_found":
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
    item = OutboxMessage(
        workspace_id=resolved_workspace_id,
        conversation_id=conversation_id,
        chat_message_id=chat_message_id,
        target_chat_id=target_chat_id,
        operation=operation,
        target_user_id=target_user_id or "",
        payload_json=json.dumps(payload, ensure_ascii=False),
        state="queued",
        retry_count=0,
        next_retry_at=_as_naive_utc(next_retry_at or _utc_now()),
        last_error="",
    )
    db.add(item)
    db.commit()
    db.refresh(item)
    return item


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
            return await client.send_message(
                chat_id=target_chat_id,
                text=str(payload.get("text")) if payload.get("text") is not None else None,
                attachments=payload.get("attachments"),
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
            return await client.send_message(
                user_id=target_user_id,
                text=str(payload.get("text")) if payload.get("text") is not None else None,
                attachments=payload.get("attachments"),
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

    ok = bool(result.get("success", True) or result.get("message"))
    retriable = _is_retriable_error(result)
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
        delivery_next_retry_at=_as_naive_utc(_utc_now()),
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
    client = _workspace_client(db, workspace_id=item.workspace_id)
    ok, _, _ = await _dispatch_outbox(db, client=client, item=item)
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
        delivery_next_retry_at=_as_naive_utc(_utc_now()),
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
    client = _workspace_client(db, workspace_id=item.workspace_id)
    ok, _, _ = await _dispatch_outbox(db, client=client, item=item)
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
    max_message_mid: str | None = None,
    link_mid: str | None = None,
    delivery_state: str = "sent",
    delivery_error: str = "",
    delivery_retry_count: int = 0,
    delivery_next_retry_at: datetime | None = None,
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
    item = ChatMessage(
        workspace_id=resolved_workspace_id,
        conversation_id=conversation_id,
        direction=direction,
        source=source,
        text=text,
        image_url=image_url,
        max_message_mid=max_message_mid,
        link_mid=link_mid,
        delivery_state=delivery_state,
        delivery_error=delivery_error,
        delivery_retry_count=delivery_retry_count,
        delivery_next_retry_at=delivery_next_retry_at,
    )
    db.add(item)
    db.commit()
    db.refresh(item)
    return item


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
            client=client,
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
    client = _workspace_client(db, workspace_id=item.workspace_id)
    ok, _, _ = await _dispatch_outbox(db, client=client, item=item)
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
    ok, _, result = await _dispatch_outbox(db, client=client, item=item)
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
                            "type": "callback",
                            "text": "Start / Начать",
                            "payload": "customer:start_fallback",
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
    ok, _, result = await _dispatch_outbox(db, client=client, item=item)
    if ok:
        return result
    return {"success": False, **result}


def _ensure_blocked_folder(
    db: Session,
    *,
    workspace_id: int = DEFAULT_WORKSPACE_ID,
) -> ChatFolder:
    return create_chat_folder(db, folder_name=BLOCKED_FOLDER_NAME, workspace_id=workspace_id)


def block_conversation(
    db: Session,
    conversation_id: int,
    *,
    actor_user_id: int | None = None,
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
    previous_folder_id = conversation.folder_id
    conversation.folder_id = blocked_folder.id
    meta.is_blocked = True
    meta.blocked_at = _as_naive_utc(_utc_now())
    meta.blocked_by_user_id = actor_user_id
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
    restore_folder_id = meta.blocked_prev_folder_id
    if restore_folder_id is not None:
        folder_exists = (
            db.query(ChatFolder.id)
            .filter(
                ChatFolder.workspace_id == ws_id,
                ChatFolder.id == restore_folder_id,
            )
            .first()
        )
        conversation.folder_id = restore_folder_id if folder_exists else None
    else:
        conversation.folder_id = None
    meta.is_blocked = False
    meta.blocked_at = None
    meta.blocked_by_user_id = None
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
    workspace_id: int | None = None,
) -> bool:
    return block_conversation(
        db,
        conversation_id=conversation_id,
        actor_user_id=actor_user_id,
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
    _store_chat_message(
        db,
        conversation_id=conversation.id,
        direction="customer",
        source="customer",
        text=event.text or "",
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
    is_fallback_start_callback = (str(event.callback_payload or "").strip().lower() == "customer:start_fallback")
    is_bot_started = update_type in {"bot_started", "bot_start"} or is_fallback_start_callback
    is_message_event = update_type in {"", "message_created", "message_callback", "new_message"}

    if bool(meta.is_blocked):
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
        return {"ok": True, "flow": "blocked_customer"}

    # Message before Start (custom behavior for message_created before start)
    if not meta.start_prompt_sent and is_message_event and not is_bot_started and not event.contact_phone:
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
    client: MaxClient,
    conversation_id: int,
    customer_chat_id: str,
    command_text: str,
    customer_user_id: str | None = None,
    *,
    owner_user_id: int = 0,
    sender_prefix: str | None = None,
    source: str = "manager",
    image_caption: str = "Менеджер отправил изображение",
    workspace_id: int = DEFAULT_WORKSPACE_ID,
) -> bool:
    command = command_text.strip().lstrip("/").strip().lower()
    if not command:
        return False

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
        return False

    media_items = (
        db.query(QuickReplyMedia)
        .filter(QuickReplyMedia.quick_reply_id == quick_reply.id)
        .order_by(QuickReplyMedia.sort_order.asc(), QuickReplyMedia.id.asc())
        .all()
    )
    if media_items:
        for media in media_items:
            image_url = f"{app_settings.public_base_url.rstrip('/')}{media.media_path}"
            ok = await enqueue_and_process_send_photo(
                db,
                conversation_id=conversation_id,
                target_chat_id=customer_chat_id,
                target_user_id=customer_user_id,
                photo_url=image_url,
                caption=image_caption,
                source=source,
            )
            if not ok:
                return False
    elif quick_reply.image_path:
        # Backward-compatibility for legacy quick replies with single image_path.
        image_url = f"{app_settings.public_base_url.rstrip('/')}{quick_reply.image_path}"
        ok = await enqueue_and_process_send_photo(
            db,
            conversation_id=conversation_id,
            target_chat_id=customer_chat_id,
            target_user_id=customer_user_id,
            photo_url=image_url,
            caption=image_caption,
            source=source,
        )
        if not ok:
            return False

    if quick_reply.text:
        rendered_text = (
            f"{sender_prefix}{quick_reply.text}" if sender_prefix is not None else quick_reply.text
        )
        ok = await enqueue_and_process_send_text(
            db,
            conversation_id=conversation_id,
            target_chat_id=customer_chat_id,
            target_user_id=customer_user_id,
            text=rendered_text,
            source=source,
            text_format="markdown",
        )
        if not ok:
            return False

    return True


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
) -> bool:
    conversation = get_conversation_by_id(db, conversation_id, workspace_id=workspace_id)
    if conversation is None:
        return False
    client = _workspace_client(db, workspace_id=conversation.workspace_id)
    return await send_quick_reply_to_customer(
        db=db,
        client=client,
        conversation_id=conversation_id,
        customer_chat_id=conversation.chat_id,
        customer_user_id=conversation.customer_account_id,
        command_text=command_text,
        owner_user_id=owner_user_id,
        sender_prefix=None,
        source="bot_system",
        image_caption="Изображение от оператора",
        workspace_id=conversation.workspace_id,
    )


def load_chat_threads(
    db: Session,
    query: str = "",
    *,
    workspace_id: int = DEFAULT_WORKSPACE_ID,
) -> list[ChatThreadItem]:
    conversations = (
        db.query(Conversation)
        .filter(Conversation.workspace_id == workspace_id)
        .order_by(Conversation.id.desc())
        .all()
    )
    needle = query.strip().lower()
    items: list[ChatThreadItem] = []
    for conv in conversations:
        meta = (
            db.query(ConversationMeta)
            .filter(
                ConversationMeta.workspace_id == workspace_id,
                ConversationMeta.conversation_id == conv.id,
            )
            .first()
        )
        profile = (
            db.query(CustomerProfile)
            .filter(
                CustomerProfile.workspace_id == workspace_id,
                CustomerProfile.customer_account_id == conv.customer_account_id,
            )
            .first()
        )
        last_msg = (
            db.query(ChatMessage)
            .filter(
                ChatMessage.workspace_id == workspace_id,
                ChatMessage.conversation_id == conv.id,
            )
            .order_by(ChatMessage.id.desc())
            .first()
        )
        name = (profile.first_name if profile and profile.first_name else "Покупатель").strip()
        username = (profile.username if profile and profile.username else "").strip()
        label = f"{name}{(' @' + username) if username else ''}"
        preview = ""
        if last_msg:
            preview = (last_msg.text or "").strip()
            if not preview and last_msg.image_url:
                preview = "[изображение]"
        status = meta.status if meta else "new"
        # Show unread highlight when there are unread messages OR manual reminder mark.
        is_unread = (bool(meta.is_unread) or bool(meta.manual_unread_mark)) if meta else True
        is_blocked = bool(meta.is_blocked) if meta else False
        phone_verified = bool(meta.phone_verified) if meta else False
        ticket_no = meta.ticket_no if meta else None
        last_activity_id = last_msg.id if last_msg else conv.id
        has_delivery_errors = bool(
            db.query(ChatMessage)
            .filter(
                ChatMessage.workspace_id == workspace_id,
                ChatMessage.conversation_id == conv.id,
                ChatMessage.direction == "bot",
                ChatMessage.delivery_state == "failed",
            )
            .first()
        )

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
                has_delivery_errors=has_delivery_errors,
                is_unread=is_unread,
                is_blocked=is_blocked,
                last_activity_id=last_activity_id,
                folder_id=conv.folder_id,
                folder_name=(
                    db.query(ChatFolder.name)
                    .filter(
                        ChatFolder.workspace_id == workspace_id,
                        ChatFolder.id == conv.folder_id,
                    )
                    .scalar()
                    or ""
                )
                if conv.folder_id
                else "",
            )
        )
    # New/unread chats first, then by latest activity.
    items.sort(key=lambda item: (0 if item.is_unread else 1, -item.last_activity_id))
    return items


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
    db.query(OutboxMessage).filter(
        OutboxMessage.workspace_id == ws_id,
        OutboxMessage.conversation_id == conversation_id,
    ).delete()
    db.query(ManagerDispatch).filter(
        ManagerDispatch.workspace_id == ws_id,
        ManagerDispatch.conversation_id == conversation_id,
    ).delete()
    db.query(ConversationMeta).filter(
        ConversationMeta.workspace_id == ws_id,
        ConversationMeta.conversation_id == conversation_id,
    ).delete()
    db.query(ChatMessage).filter(
        ChatMessage.workspace_id == ws_id,
        ChatMessage.conversation_id == conversation_id,
    ).delete()
    db.query(MessageLog).filter(
        MessageLog.workspace_id == ws_id,
        MessageLog.conversation_id == conversation_id,
    ).delete()
    db.delete(conversation)
    db.commit()
    return True


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
    db.commit()
    db.refresh(folder)
    return folder


def assign_conversation_to_folder(
    db: Session,
    *,
    conversation_id: int,
    folder_id: int | None,
    workspace_id: int | None = None,
) -> bool:
    conversation_query = db.query(Conversation).filter(Conversation.id == conversation_id)
    if workspace_id is not None:
        conversation_query = conversation_query.filter(Conversation.workspace_id == workspace_id)
    conversation = conversation_query.first()
    if conversation is None:
        return False
    ws_id = conversation.workspace_id
    if folder_id is not None:
        folder = (
            db.query(ChatFolder)
            .filter(
                ChatFolder.workspace_id == ws_id,
                ChatFolder.id == folder_id,
            )
            .first()
        )
        if folder is None:
            return False
    conversation.folder_id = folder_id
    db.add(conversation)
    db.commit()
    return True


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
) -> list[ChatMessage]:
    query = db.query(ChatMessage).filter(ChatMessage.conversation_id == conversation_id)
    if workspace_id is not None:
        query = query.filter(ChatMessage.workspace_id == workspace_id)
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
    image_path: str | None,
    workspace_id: int | None = None,
    schedule_at_iso: str = "",
) -> bool:
    conversation = get_conversation_by_id(db, conversation_id, workspace_id=workspace_id)
    if conversation is None:
        return False
    scheduled_for = _parse_schedule_at_iso(schedule_at_iso)
    sent_any = False

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

    if image_path:
        image_url = f"{app_settings.public_base_url.rstrip('/')}{image_path}"
        if scheduled_for:
            await queue_only_send_photo(
                db,
                conversation_id=conversation_id,
                target_chat_id=conversation.chat_id,
                target_user_id=conversation.customer_account_id,
                photo_url=image_url,
                caption="Изображение от оператора",
                source="bot_system",
                scheduled_for=scheduled_for,
            )
            ok = True
        else:
            ok = await enqueue_and_process_send_photo(
                db,
                conversation_id=conversation_id,
                target_chat_id=conversation.chat_id,
                target_user_id=conversation.customer_account_id,
                photo_url=image_url,
                caption="Изображение от оператора",
                source="bot_system",
            )
        if ok:
            sent_any = True

    return sent_any


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
    db.delete(msg)
    db.commit()
    return True
