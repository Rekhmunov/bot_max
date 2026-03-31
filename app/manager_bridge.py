from __future__ import annotations

import json
import re
from datetime import UTC, datetime, timedelta
from dataclasses import dataclass
from typing import Optional
from urllib.parse import quote_plus

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.auth import create_manager_mini_token
from app.config import settings
from app.max_client import MaxClient
from app.models import (
    BotSettings,
    ChatMessage,
    ChatFolder,
    Conversation,
    ConversationMeta,
    CustomerProfile,
    ManagerDispatch,
    MessageLog,
    MessageTemplate,
    OutboxMessage,
    QuickReply,
)
from app.schemas import MaxWebhookEvent

TEMPLATE_PRESTART = "prestart_message"
TEMPLATE_START = "start_message"
TEMPLATE_AFTER_PHONE = "after_phone_message"
MANAGER_HELP_TEXT = (
    "Новый клиент пишет в боте.\n"
    "Отвечайте в reply на сообщение с тикетом [T-XXXX], чтобы бот отправил ответ клиенту.\n"
    "Быстрые ответы: /command (например, /price).\n"
    "Если клиент не может отправить контакт: /contact +79990001122 (в reply на нужный тикет).\n"
    "Быстрый поиск в чате: /tickets\n"
    "Ответ без reply: /reply T-1001 ваш текст\n"
    "Быстрый ответ без reply: /reply T-1001 /price\n"
    "Диспетчер: /panel, /new, /mine, /next, /take T-1001, /done T-1001\n"
    "Mini app: /mini"
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


@dataclass
class ForwardResult:
    ok: bool
    message: str


@dataclass
class ChatThreadItem:
    conversation_id: int
    chat_id: str
    ticket_no: int | None
    customer_label: str
    status: str
    phone_verified: bool
    last_message_preview: str
    has_delivery_errors: bool
    is_unread: bool
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


def ensure_default_templates(db: Session) -> None:
    existing = {
        item.template_key: item
        for item in db.query(MessageTemplate)
        .filter(MessageTemplate.template_key.in_(list(DEFAULT_TEMPLATES.keys())))
        .all()
    }
    changed = False
    for key, text in DEFAULT_TEMPLATES.items():
        if key in existing:
            continue
        db.add(MessageTemplate(template_key=key, template_text=text))
        changed = True
    if changed:
        db.commit()


def get_template_text(db: Session, key: str) -> str:
    row = db.query(MessageTemplate).filter(MessageTemplate.template_key == key).first()
    if row and row.template_text.strip():
        return row.template_text
    return DEFAULT_TEMPLATES.get(key, "")


def set_template_text(db: Session, key: str, value: str) -> None:
    row = db.query(MessageTemplate).filter(MessageTemplate.template_key == key).first()
    if row is None:
        row = MessageTemplate(template_key=key, template_text=value.strip())
    else:
        row.template_text = value.strip()
    db.add(row)
    db.commit()


def _get_or_create_conversation(db: Session, chat_id: str, customer_id: str) -> Conversation:
    conversation = db.query(Conversation).filter(Conversation.chat_id == chat_id).first()
    if conversation:
        return conversation
    conversation = Conversation(
        chat_id=chat_id,
        customer_account_id=customer_id,
        manager_added=False,
        is_active=True,
    )
    db.add(conversation)
    db.commit()
    db.refresh(conversation)
    return conversation


def _get_or_create_meta(db: Session, conversation_id: int) -> ConversationMeta:
    meta = db.query(ConversationMeta).filter(ConversationMeta.conversation_id == conversation_id).first()
    if meta:
        return meta
    max_ticket = db.query(func.max(ConversationMeta.ticket_no)).scalar()
    next_ticket = (max_ticket or 1000) + 1
    meta = ConversationMeta(
        conversation_id=conversation_id,
        ticket_no=next_ticket,
        status="new",
        phone_verified=False,
        start_prompt_sent=False,
    )
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
) -> CustomerProfile:
    profile = db.query(CustomerProfile).filter(CustomerProfile.customer_account_id == customer_id).first()
    if profile is None:
        profile = CustomerProfile(
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
) -> OutboxMessage:
    item = OutboxMessage(
        conversation_id=conversation_id,
        chat_message_id=chat_message_id,
        target_chat_id=target_chat_id,
        operation=operation,
        target_user_id=target_user_id or "",
        payload_json=json.dumps(payload, ensure_ascii=False),
        state="queued",
        retry_count=0,
        next_retry_at=_as_naive_utc(_utc_now()),
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
            return await client.send_text(chat_id=target_chat_id, text=str(payload.get("text") or ""))
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
            return await client.send_text_to_user(user_id=target_user_id, text=str(payload.get("text") or ""))
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
        _mark_outbox_sent(item, result)
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


async def process_outbox_queue(db: Session, *, limit: int = 20) -> int:
    now = _as_naive_utc(_utc_now())
    items = (
        db.query(OutboxMessage)
        .filter(
            OutboxMessage.state.in_(["queued", "failed"]),
            OutboxMessage.next_retry_at <= now,
        )
        .order_by(OutboxMessage.next_retry_at.asc(), OutboxMessage.id.asc())
        .limit(limit)
        .all()
    )
    if not items:
        return 0
    client = MaxClient()
    processed = 0
    for item in items:
        await _dispatch_outbox(db, client=client, item=item)
        processed += 1
    return processed


async def retry_failed_outbox_message(db: Session, *, chat_message_id: int) -> bool:
    item = (
        db.query(OutboxMessage)
        .filter(
            OutboxMessage.chat_message_id == chat_message_id,
            OutboxMessage.state == "failed",
        )
        .order_by(OutboxMessage.id.desc())
        .first()
    )
    if item is None:
        return False
    item.state = "queued"
    item.next_retry_at = _as_naive_utc(_utc_now())
    db.add(item)
    db.commit()
    ok, _, _ = await _dispatch_outbox(db, client=MaxClient(), item=item)
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
        payload={"text": text},
    )
    ok, _, _ = await _dispatch_outbox(db, client=MaxClient(), item=item)
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
    ok, _, _ = await _dispatch_outbox(db, client=MaxClient(), item=item)
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
) -> None:
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
    _enqueue_outbox_message(
        db,
        conversation_id=conversation_id,
        chat_message_id=msg.id,
        target_chat_id=target_chat_id,
        target_user_id=target_user_id,
        operation="send_text",
        payload={"text": text},
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
) -> ChatMessage:
    item = ChatMessage(
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
    ok, _, _ = await _dispatch_outbox(db, client=MaxClient(), item=item)
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
        await MaxClient().send_text_to_user(user_id=manager_id, text=msg)
        return {"ok": True, "empty": True, "mode": mode}

    thread, meta = rows[0]
    conversation = db.query(Conversation).filter(Conversation.id == thread.conversation_id).first()
    if conversation is None:
        await MaxClient().send_text_to_user(user_id=manager_id, text="Тикет не найден.")
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
        await MaxClient().send_text_to_user(
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
        payload={"text": full_text, "attachments": attachments},
    )
    ok, _, result = await _dispatch_outbox(db, client=client, item=item)
    if ok:
        return result
    return {"success": False, **result}


async def handle_customer_event(
    db: Session,
    client: MaxClient,
    settings: BotSettings,
    event: MaxWebhookEvent,
) -> dict:
    conversation = _get_or_create_conversation(db, chat_id=event.chat_id, customer_id=event.sender_id)
    meta = _get_or_create_meta(db, conversation_id=conversation.id)
    customer = _upsert_customer_profile(
        db=db,
        customer_id=event.sender_id,
        chat_id=event.chat_id,
        first_name=event.sender_first_name,
        username=event.sender_username,
        phone_number=event.contact_phone,
    )

    db.add(
        MessageLog(
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
    )

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
        )

    update_type = (event.update_type or "").strip().lower()
    is_bot_started = update_type in {"bot_started", "bot_start"}
    is_message_event = update_type in {"", "message_created", "message_callback", "new_message"}

    # Message before Start (custom behavior for message_created before start)
    if not meta.start_prompt_sent and is_message_event and not is_bot_started and not event.contact_phone:
        prestart_text = get_template_text(db, TEMPLATE_PRESTART)
        if prestart_text:
            await queue_only_send_text(
                db,
                conversation_id=conversation.id,
                target_chat_id=event.chat_id,
                target_user_id=event.sender_id,
                text=prestart_text,
                source="bot_system",
            )
            await process_outbox_queue(db, limit=20)
        return {"ok": True, "flow": "prestart"}

    if not meta.start_prompt_sent and (is_bot_started or is_message_event):
        meta.start_prompt_sent = True
        db.add(meta)
        db.commit()
        if meta.phone_verified:
            await queue_only_send_text(
                db,
                conversation_id=conversation.id,
                target_chat_id=event.chat_id,
                target_user_id=event.sender_id,
                text=get_template_text(db, TEMPLATE_AFTER_PHONE),
                source="bot_system",
            )
            await process_outbox_queue(db, limit=20)
            return {"ok": True, "flow": "start_prompt_skipped_phone"}
        start_text = get_template_text(db, TEMPLATE_START)
        await _send_contact_request_prompt(
            db=db,
            conversation_id=conversation.id,
            client=client,
            chat_id=event.chat_id,
            user_id=event.sender_id,
            text=start_text,
        )
        return {"ok": True, "flow": "start_prompt"}

    if phone_just_verified:
        await queue_only_send_text(
            db,
            conversation_id=conversation.id,
            target_chat_id=event.chat_id,
            target_user_id=event.sender_id,
            text=get_template_text(db, TEMPLATE_AFTER_PHONE),
            source="bot_system",
        )
        await process_outbox_queue(db, limit=20)
        await forward_customer_message_to_manager(
            db=db,
            client=client,
            settings=settings,
            conversation=conversation,
            meta=meta,
            customer=customer,
            event=event,
            extra_header="Контакт подтвержден",
        )
        return {"ok": True, "flow": "phone_verified"}

    if not meta.phone_verified and event.text.strip():
        manager_user_id = settings.manager_account_id.strip()
        if manager_user_id:
            ticket_line = _render_ticket_line(meta, customer, event)
            text = event.text.strip()
            manager_text = (
                f"{ticket_line}\n"
                f"Клиент: {text}\n"
                "Для подтверждения контакта отправьте в ответ:\n"
                "/contact +79990001122"
            )
            ok = await enqueue_and_process_send_text(
                db,
                conversation_id=conversation.id,
                target_chat_id="",
                target_user_id=manager_user_id,
                text=manager_text,
                source="bot_system",
            )
            if ok:
                sent_msg = (
                    db.query(ChatMessage)
                    .filter(
                        ChatMessage.conversation_id == conversation.id,
                        ChatMessage.direction == "bot",
                        ChatMessage.source == "bot_system",
                    )
                    .order_by(ChatMessage.id.desc())
                    .first()
                )
                if sent_msg and sent_msg.max_message_mid:
                    db.add(
                        ManagerDispatch(
                            conversation_id=conversation.id,
                            manager_message_mid=sent_msg.max_message_mid,
                            dispatch_type="customer_to_manager",
                        )
                    )
                    db.commit()
        return {"ok": True, "flow": "waiting_contact_confirmation"}

    # Forward customer messages to manager only after phone verification.
    if meta.phone_verified and event.text.strip():
        await forward_customer_message_to_manager(
            db=db,
            client=client,
            settings=settings,
            conversation=conversation,
            meta=meta,
            customer=customer,
            event=event,
            extra_header=None,
        )
        await process_outbox_queue(db, limit=20)
        return {"ok": True, "flow": "forwarded_to_manager"}

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
    manager_user_id = settings.manager_account_id.strip()
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
    manager_user_id = settings.manager_account_id.strip()
    if str(event.sender_id).strip() != str(manager_user_id).strip():
        return {"ok": True, "ignored": "not_manager_sender"}

    text_value = event.text.strip()
    callback_action = _parse_manager_callback_action(event.raw_payload)
    if callback_action:
        action, maybe_ticket = callback_action
        if action in {"panel"}:
            ok = await _send_dispatch_panel(db, manager_id=manager_user_id)
            return {"ok": True, "panel_sent": ok, "via": "callback"}
        if action in {"new", "mine", "next"}:
            mode = "new" if action in {"new", "next"} else "mine"
            return await _send_first_ticket_from_mode(db, manager_id=manager_user_id, mode=mode)
        if action in {"take", "done", "mine", "show"} and maybe_ticket is not None:
            return await _apply_ticket_action(
                db,
                manager_id=manager_user_id,
                action=action,
                ticket_no=maybe_ticket,
            )

    if text_value.lower() in {"/help", "/h"}:
        await client.send_text_to_user(user_id=manager_user_id, text=MANAGER_HELP_TEXT)
        return {"ok": True, "help_sent": True}

    if text_value.lower() in {"/mini", "/app", "/miniapp"}:
        mini_token = create_manager_mini_token(manager_user_id)
        mini_url = f"{settings.public_base_url.rstrip('/')}/mini/manager?token={quote_plus(mini_token)}"
        await client.send_text_to_user(
            user_id=manager_user_id,
            text=(
                "Откройте mini-app менеджера:\n"
                f"{mini_url}"
            ),
        )
        return {"ok": True, "mini_sent": True}

    if text_value.lower() in {"/panel", "/p"}:
        ok = await _send_dispatch_panel(db, manager_id=manager_user_id)
        return {"ok": True, "panel_sent": ok}

    if text_value.lower() == "/new":
        return await _send_first_ticket_from_mode(db, manager_id=manager_user_id, mode="new")

    if text_value.lower() in {"/mine", "/my"}:
        return await _send_first_ticket_from_mode(db, manager_id=manager_user_id, mode="mine")

    if text_value.lower() in {"/next", "/n"}:
        return await _send_first_ticket_from_mode(db, manager_id=manager_user_id, mode="new")

    parsed_ticket_action = _parse_manager_ticket_action(text_value)
    if parsed_ticket_action:
        action, ticket_no = parsed_ticket_action
        return await _apply_ticket_action(
            db,
            manager_id=manager_user_id,
            action=action,
            ticket_no=ticket_no,
        )

    if text_value.lower() in {"/tickets", "/list", "/inbox"}:
        inbox_text = _build_manager_ticket_inbox_text(db)
        await client.send_text_to_user(user_id=manager_user_id, text=inbox_text)
        return {"ok": True, "tickets_sent": True}

    target_conversation = None
    parsed_ticket_reply = _parse_manager_ticket_reply(text_value)
    if parsed_ticket_reply:
        ticket_no, payload_text = parsed_ticket_reply
        target_conversation = _resolve_conversation_by_ticket(db, ticket_no)
        if target_conversation is None:
            await client.send_text_to_user(
                user_id=manager_user_id,
                text=f"Тикет T-{ticket_no} не найден. Проверьте номер через /tickets",
            )
            return {"ok": True, "ignored": "ticket_not_found"}
        sent_ok = await _send_manager_payload_to_conversation(
            db,
            client=client,
            conversation=target_conversation,
            manager_event=event,
            payload_text=payload_text,
        )
        return {"ok": True, "ticket_reply_sent": sent_ok, "ticket_no": ticket_no}

    if event.link_mid:
        dispatch = db.query(ManagerDispatch).filter(ManagerDispatch.manager_message_mid == event.link_mid).first()
        if dispatch:
            target_conversation = (
                db.query(Conversation).filter(Conversation.id == dispatch.conversation_id).first()
            )

    if target_conversation is None:
        # Operational helper to manager chat can stay as direct send.
        await client.send_text_to_user(
            user_id=manager_user_id,
            text=(
                "Нужно отвечать reply на карточку тикета.\n"
                "Или используйте формат: /reply T-1001 ваш текст\n"
                "Список активных тикетов: /tickets"
            ),
        )
        return {"ok": True, "ignored": "reply_required"}

    sent_ok = await _send_manager_payload_to_conversation(
        db,
        client=client,
        conversation=target_conversation,
        manager_event=event,
        payload_text=text_value,
    )
    return {"ok": True, "manager_payload_sent": sent_ok}


async def send_quick_reply_to_customer(
    db: Session,
    client: MaxClient,
    conversation_id: int,
    customer_chat_id: str,
    command_text: str,
    customer_user_id: str | None = None,
    *,
    sender_prefix: str | None = None,
    source: str = "manager",
    image_caption: str = "Менеджер отправил изображение",
) -> bool:
    command = command_text.strip().lstrip("/").strip().lower()
    if not command:
        return False

    quick_reply = (
        db.query(QuickReply)
        .filter(QuickReply.command == command, QuickReply.is_active.is_(True))
        .first()
    )
    if not quick_reply:
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
        )
        if not ok:
            return False

    if quick_reply.image_path:
        image_url = f"{settings.public_base_url.rstrip('/')}{quick_reply.image_path}"
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

    return True


def list_active_quick_replies(db: Session) -> list[QuickReply]:
    return (
        db.query(QuickReply)
        .filter(QuickReply.is_active.is_(True))
        .order_by(QuickReply.command.asc())
        .all()
    )


async def send_admin_quick_reply(
    db: Session,
    *,
    conversation_id: int,
    command_text: str,
) -> bool:
    conversation = get_conversation_by_id(db, conversation_id)
    if conversation is None:
        return False
    client = MaxClient()
    return await send_quick_reply_to_customer(
        db=db,
        client=client,
        conversation_id=conversation_id,
        customer_chat_id=conversation.chat_id,
        customer_user_id=conversation.customer_account_id,
        command_text=command_text,
        sender_prefix=None,
        source="bot_system",
        image_caption="Изображение от оператора",
    )


def load_chat_threads(db: Session, query: str = "") -> list[ChatThreadItem]:
    conversations = db.query(Conversation).order_by(Conversation.id.desc()).all()
    needle = query.strip().lower()
    items: list[ChatThreadItem] = []
    for conv in conversations:
        meta = db.query(ConversationMeta).filter(ConversationMeta.conversation_id == conv.id).first()
        profile = db.query(CustomerProfile).filter(
            CustomerProfile.customer_account_id == conv.customer_account_id
        ).first()
        last_msg = (
            db.query(ChatMessage)
            .filter(ChatMessage.conversation_id == conv.id)
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
        is_unread = status != "read"
        phone_verified = bool(meta.phone_verified) if meta else False
        ticket_no = meta.ticket_no if meta else None
        last_activity_id = last_msg.id if last_msg else conv.id
        has_delivery_errors = bool(
            db.query(ChatMessage)
            .filter(
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
                ticket_no=ticket_no,
                customer_label=label,
                status=status,
                phone_verified=phone_verified,
                last_message_preview=preview,
                has_delivery_errors=has_delivery_errors,
                is_unread=is_unread,
                last_activity_id=last_activity_id,
                folder_id=conv.folder_id,
                folder_name=(db.query(ChatFolder.name).filter(ChatFolder.id == conv.folder_id).scalar() or "")
                if conv.folder_id
                else "",
            )
        )
    # New/unread chats first, then by latest activity.
    items.sort(key=lambda item: (0 if item.is_unread else 1, -item.last_activity_id))
    return items


def delete_conversation(db: Session, conversation_id: int) -> bool:
    conversation = db.query(Conversation).filter(Conversation.id == conversation_id).first()
    if conversation is None:
        return False
    db.query(OutboxMessage).filter(OutboxMessage.conversation_id == conversation_id).delete()
    db.query(ManagerDispatch).filter(ManagerDispatch.conversation_id == conversation_id).delete()
    db.query(ConversationMeta).filter(ConversationMeta.conversation_id == conversation_id).delete()
    db.query(ChatMessage).filter(ChatMessage.conversation_id == conversation_id).delete()
    db.query(MessageLog).filter(MessageLog.conversation_id == conversation_id).delete()
    db.delete(conversation)
    db.commit()
    return True


def get_delivery_metrics(db: Session) -> DeliveryStats:
    total = db.query(OutboxMessage).count()
    success_count = db.query(OutboxMessage).filter(OutboxMessage.state == "sent").count()
    failed_count = db.query(OutboxMessage).filter(OutboxMessage.state == "failed").count()
    permanent_failures = (
        db.query(OutboxMessage)
        .filter(
            OutboxMessage.state == "failed",
            OutboxMessage.is_permanent_failure.is_(True),
        )
        .count()
    )
    retry_sum = int(db.query(func.sum(OutboxMessage.retry_count)).scalar() or 0)
    return DeliveryStats(
        total_sent_attempts=total,
        success_count=success_count,
        failed_count=failed_count,
        retry_sum=retry_sum,
        permanent_failures=permanent_failures,
    )


def get_chat_metrics(db: Session) -> dict[str, int]:
    return {
        "new_count": db.query(ConversationMeta).filter(ConversationMeta.status == "new").count(),
        "in_progress_count": db.query(ConversationMeta).filter(ConversationMeta.status == "in_progress").count(),
        "done_count": db.query(ConversationMeta).filter(ConversationMeta.status == "done").count(),
    }


def get_conversation_meta(db: Session, conversation_id: int) -> ConversationMeta | None:
    return db.query(ConversationMeta).filter(ConversationMeta.conversation_id == conversation_id).first()


def list_conversation_quick_commands(db: Session, conversation_id: int, limit: int = 8) -> list[str]:
    recent_commands = (
        db.query(ChatMessage.text)
        .filter(
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
    return [f"/{item.command}" for item in list_active_quick_replies(db)[:limit]]


def list_chat_folders(db: Session) -> list[ChatFolder]:
    return db.query(ChatFolder).order_by(ChatFolder.sort_order.asc(), ChatFolder.name.asc()).all()


def create_chat_folder(db: Session, folder_name: str) -> ChatFolder:
    normalized = folder_name.strip()
    if not normalized:
        raise ValueError("folder_name is empty")
    exists = db.query(ChatFolder).filter(func.lower(ChatFolder.name) == normalized.lower()).first()
    if exists:
        return exists
    max_sort = db.query(func.max(ChatFolder.sort_order)).scalar()
    folder = ChatFolder(name=normalized, sort_order=(int(max_sort or 0) + 1))
    db.add(folder)
    db.commit()
    db.refresh(folder)
    return folder


def assign_conversation_to_folder(
    db: Session,
    *,
    conversation_id: int,
    folder_id: int | None,
) -> bool:
    conversation = db.query(Conversation).filter(Conversation.id == conversation_id).first()
    if conversation is None:
        return False
    if folder_id is not None:
        folder = db.query(ChatFolder).filter(ChatFolder.id == folder_id).first()
        if folder is None:
            return False
    conversation.folder_id = folder_id
    db.add(conversation)
    db.commit()
    return True


def mark_thread_unread(db: Session, conversation_id: int) -> bool:
    meta = db.query(ConversationMeta).filter(ConversationMeta.conversation_id == conversation_id).first()
    if meta is None:
        return False
    if meta.status == "new":
        return True
    meta.status = "new"
    meta.is_new = True
    db.add(meta)
    db.commit()
    return True


def load_chat_messages(db: Session, conversation_id: int) -> list[ChatMessage]:
    return (
        db.query(ChatMessage)
        .filter(ChatMessage.conversation_id == conversation_id)
        .order_by(ChatMessage.id.asc())
        .all()
    )


def get_conversation_by_id(db: Session, conversation_id: int) -> Conversation | None:
    return db.query(Conversation).filter(Conversation.id == conversation_id).first()


def mark_thread_read(db: Session, conversation_id: int) -> None:
    meta = db.query(ConversationMeta).filter(ConversationMeta.conversation_id == conversation_id).first()
    if meta is None:
        return
    if meta.status != "read":
        meta.status = "read"
        meta.is_new = False
        db.add(meta)
        db.commit()


async def send_admin_chat_message(
    db: Session,
    *,
    conversation_id: int,
    text: str,
    image_path: str | None,
) -> bool:
    conversation = get_conversation_by_id(db, conversation_id)
    if conversation is None:
        return False
    sent_any = False

    if text:
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
        image_url = f"{settings.public_base_url.rstrip('/')}{image_path}"
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
) -> ChatMessage | None:
    msg = db.query(ChatMessage).filter(ChatMessage.id == chat_message_id).first()
    if not msg:
        return None
    text_value = new_text.strip()
    if msg.max_message_mid:
        client = MaxClient()
        await client.edit_message(message_id=msg.max_message_mid, text=text_value)
    msg.text = text_value
    db.add(msg)
    db.commit()
    db.refresh(msg)
    return msg


async def remove_chat_message(db: Session, chat_message_id: int) -> bool:
    msg = db.query(ChatMessage).filter(ChatMessage.id == chat_message_id).first()
    if not msg:
        return False
    if msg.max_message_mid:
        client = MaxClient()
        await client.delete_message(message_id=msg.max_message_mid)
    db.delete(msg)
    db.commit()
    return True
