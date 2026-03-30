from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.max_client import MaxClient
from app.models import (
    BotSettings,
    ChatMessage,
    Conversation,
    ConversationMeta,
    CustomerProfile,
    ManagerDispatch,
    MessageLog,
    MessageTemplate,
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
    "Если клиент не может отправить контакт: /contact +79990001122 (в reply на нужный тикет)."
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
) -> ChatMessage:
    item = ChatMessage(
        conversation_id=conversation_id,
        direction=direction,
        source=source,
        text=text,
        image_url=image_url,
        max_message_mid=max_message_mid,
        link_mid=link_mid,
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


async def _send_contact_request_prompt(
    client: MaxClient,
    chat_id: str,
    text: str,
) -> dict:
    full_text = (
        f"{text}\n\n"
        "Если кнопка контакта не отображается, отправьте номер вручную "
        "сообщением: /contact +79990000000"
    )
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
    return await client.send_message(chat_id=chat_id, text=full_text, attachments=attachments)


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

    is_bot_started = event.update_type == "bot_started"

    # Message before Start (custom behavior for message_created before start)
    if not meta.start_prompt_sent and event.update_type == "message_created" and not event.contact_phone:
        prestart_text = get_template_text(db, TEMPLATE_PRESTART)
        if prestart_text:
            await client.send_text(chat_id=event.chat_id, text=prestart_text)
        return {"ok": True, "flow": "prestart"}

    if not meta.start_prompt_sent and (is_bot_started or event.update_type == "message_created"):
        meta.start_prompt_sent = True
        db.add(meta)
        db.commit()
        if meta.phone_verified:
            await client.send_text(chat_id=event.chat_id, text=get_template_text(db, TEMPLATE_AFTER_PHONE))
            return {"ok": True, "flow": "start_prompt_skipped_phone"}
        start_text = get_template_text(db, TEMPLATE_START)
        await _send_contact_request_prompt(client=client, chat_id=event.chat_id, text=start_text)
        return {"ok": True, "flow": "start_prompt"}

    if phone_just_verified:
        await client.send_text(chat_id=event.chat_id, text=get_template_text(db, TEMPLATE_AFTER_PHONE))
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
        manager_chat_id = settings.manager_account_id.strip()
        if manager_chat_id:
            ticket_line = _render_ticket_line(meta, customer, event)
            text = event.text.strip()
            manager_text = (
                f"{ticket_line}\n"
                f"Клиент: {text}\n"
                "Для подтверждения контакта отправьте в ответ:\n"
                "/contact +79990001122"
            )
            result = await client.send_text(chat_id=manager_chat_id, text=manager_text)
            manager_mid = _extract_sent_mid(result)
            if manager_mid:
                db.add(
                    ManagerDispatch(
                        conversation_id=conversation.id,
                        manager_message_mid=manager_mid,
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
    manager_chat_id = settings.manager_account_id.strip()
    if not manager_chat_id:
        return ForwardResult(ok=False, message="manager_account_id is empty")

    ticket_line = _render_ticket_line(meta, customer, event)
    text = event.text.strip() or "(без текста)"
    header = f"{extra_header}\n" if extra_header else ""
    manager_text = f"{header}{ticket_line}\nКлиент: {text}"

    result = await client.send_text(chat_id=manager_chat_id, text=manager_text)
    if not result.get("success", True) and "message" not in result:
        return ForwardResult(ok=False, message=str(result))

    manager_mid = _extract_sent_mid(result)
    if manager_mid:
        db.add(
            ManagerDispatch(
                conversation_id=conversation.id,
                manager_message_mid=manager_mid,
                dispatch_type="customer_to_manager",
            )
        )
        db.commit()
    _store_chat_message(
        db,
        conversation_id=conversation.id,
        direction="bot",
        source="bot_system",
        text=f"[FORWARD_TO_MANAGER] {manager_text}",
        max_message_mid=manager_mid,
    )
    return ForwardResult(ok=True, message="sent")


async def handle_manager_message(
    db: Session,
    client: MaxClient,
    settings: BotSettings,
    event: MaxWebhookEvent,
) -> dict:
    manager_chat_id = settings.manager_account_id.strip()
    if str(event.chat_id) != str(manager_chat_id):
        return {"ok": True, "ignored": "not_manager_chat"}

    target_conversation = None
    if event.link_mid:
        dispatch = db.query(ManagerDispatch).filter(ManagerDispatch.manager_message_mid == event.link_mid).first()
        if dispatch:
            target_conversation = (
                db.query(Conversation).filter(Conversation.id == dispatch.conversation_id).first()
            )

    if target_conversation is None:
        await client.send_text(
            chat_id=manager_chat_id,
            text=(
                "Нужно отвечать reply на карточку тикета.\n"
                "Нажмите «Ответить» на сообщение вида 🆕 [T-xxxx] ..."
            ),
        )
        return {"ok": True, "ignored": "reply_required"}

    customer_chat_id = target_conversation.chat_id
    text_value = event.text.strip()
    contact_from_text = _extract_contact_from_manager_text(text_value)
    if contact_from_text:
        meta = (
            db.query(ConversationMeta)
            .filter(ConversationMeta.conversation_id == target_conversation.id)
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
            customer_id=target_conversation.customer_account_id,
            chat_id=target_conversation.chat_id,
            first_name=None,
            username=None,
            phone_number=contact_from_text,
        )
        await client.send_text(
            chat_id=customer_chat_id,
            text=get_template_text(db, TEMPLATE_AFTER_PHONE),
        )
        _store_chat_message(
            db,
            conversation_id=target_conversation.id,
            direction="bot",
            source="manager",
            text=get_template_text(db, TEMPLATE_AFTER_PHONE),
        )
        return {"ok": True, "phone_captured_from_manager": True}

    if text_value.startswith("/"):
        sent = await _send_quick_reply_to_customer(
            db=db,
            client=client,
            conversation_id=target_conversation.id,
            customer_chat_id=customer_chat_id,
            command_text=text_value,
        )
        return {"ok": True, "manager_command_sent": sent}

    if text_value:
        text = f"Менеджер: {text_value}"
        result = await client.send_text(chat_id=customer_chat_id, text=text)
        ok = bool(result.get("success", True) or result.get("message"))
        mid = _extract_sent_mid(result)
        _store_chat_message(
            db,
            conversation_id=target_conversation.id,
            direction="bot",
            source="manager",
            text=text,
            max_message_mid=mid,
            link_mid=event.link_mid,
        )
        return {"ok": True, "manager_text_sent": ok}

    return {"ok": True, "ignored": "empty_manager_message"}


async def _send_quick_reply_to_customer(
    db: Session,
    client: MaxClient,
    conversation_id: int,
    customer_chat_id: str,
    command_text: str,
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
        send_result = await client.send_text(chat_id=customer_chat_id, text=f"Менеджер: {quick_reply.text}")
        if not send_result.get("success", True) and "message" not in send_result:
            return False
        _store_chat_message(
            db,
            conversation_id=conversation_id,
            direction="bot",
            source="manager",
            text=f"Менеджер: {quick_reply.text}",
            max_message_mid=_extract_sent_mid(send_result),
        )

    if quick_reply.image_path:
        image_url = f"{settings.public_base_url.rstrip('/')}{quick_reply.image_path}"
        image_result = await client.send_photo(
            chat_id=customer_chat_id,
            photo_url=image_url,
            caption="Менеджер отправил изображение",
        )
        if not image_result.get("success", True) and "message" not in image_result:
            return False
        _store_chat_message(
            db,
            conversation_id=conversation_id,
            direction="bot",
            source="manager",
            text="Менеджер отправил изображение",
            image_url=image_url,
            max_message_mid=_extract_sent_mid(image_result),
        )

    return True


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
        phone_verified = bool(meta.phone_verified) if meta else False
        ticket_no = meta.ticket_no if meta else None

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
            )
        )
    return items


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
    client = MaxClient()
    sent_any = False

    if text:
        result = await client.send_text(chat_id=conversation.chat_id, text=text)
        ok = bool(result.get("success", True) or result.get("message"))
        if ok:
            _store_chat_message(
                db,
                conversation_id=conversation_id,
                direction="bot",
                source="bot_system",
                text=text,
                max_message_mid=_extract_sent_mid(result),
            )
            sent_any = True

    if image_path:
        image_url = f"{settings.public_base_url.rstrip('/')}{image_path}"
        image_result = await client.send_photo(
            chat_id=conversation.chat_id,
            photo_url=image_url,
            caption="Изображение от оператора",
        )
        ok = bool(image_result.get("success", True) or image_result.get("message"))
        if ok:
            _store_chat_message(
                db,
                conversation_id=conversation_id,
                direction="bot",
                source="bot_system",
                text="Изображение от оператора",
                image_url=image_url,
                max_message_mid=_extract_sent_mid(image_result),
            )
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
