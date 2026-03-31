from __future__ import annotations

import re
from typing import Any

from pydantic import BaseModel, Field


def _pick_first(*values: Any) -> Any:
    for value in values:
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        return value
    return None


def _extract_phone_from_vcf(vcf_text: str | None) -> str | None:
    if not vcf_text:
        return None
    # Common VCF form: TEL;TYPE=CELL:+79990001122
    match = re.search(r"TEL[^:]*:([+\d][\d\s\-().]+)", vcf_text)
    if not match:
        return None
    return match.group(1).strip()


class MaxWebhookEvent(BaseModel):
    chat_id: str = Field(..., description="ID чата, где пришло событие")
    sender_id: str = Field(..., description="ID отправителя сообщения")
    text: str = Field(default="", description="Текст сообщения")
    update_type: str | None = Field(default=None, description="Тип события update")
    chat_type: str | None = Field(default=None)
    message_mid: str | None = Field(default=None)
    link_mid: str | None = Field(default=None)
    contact_phone: str | None = Field(default=None)
    sender_first_name: str | None = Field(default=None)
    sender_username: str | None = Field(default=None)
    update_id: str | None = Field(default=None)
    raw_payload: dict[str, Any] = Field(default_factory=dict)
    callback_payload: str | None = Field(default=None)

    def event_uid_value(self) -> str | None:
        if self.update_id:
            return f"update:{self.update_id}"
        if self.message_mid:
            return f"message:{self.message_mid}"
        return None

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "MaxWebhookEvent | None":
        """
        Supports both simplified payload format and official Max Update payload.
        """
        update_type = _pick_first(payload.get("update_type"), payload.get("updateType"))
        update_aliases = {
            "message_callback": "message_created",
            "new_message": "message_created",
            "bot_start": "bot_started",
        }
        if isinstance(update_type, str):
            normalized = update_aliases.get(update_type.strip().lower())
            if normalized:
                update_type = normalized
        message = payload.get("message") if isinstance(payload.get("message"), dict) else {}
        if not message and isinstance(payload.get("body"), dict):
            # Some Max webhook deliveries wrap message object under `body`.
            body_root = payload.get("body")
            if isinstance(body_root.get("message"), dict):
                message = body_root.get("message")
        user = payload.get("user") if isinstance(payload.get("user"), dict) else {}
        sender = message.get("sender") if isinstance(message.get("sender"), dict) else {}
        recipient = message.get("recipient") if isinstance(message.get("recipient"), dict) else {}
        body = message.get("body") if isinstance(message.get("body"), dict) else {}
        link = message.get("link") if isinstance(message.get("link"), dict) else {}
        link_message = link.get("message") if isinstance(link.get("message"), dict) else {}

        attachments = body.get("attachments") if isinstance(body.get("attachments"), list) else []
        contact_phone = None
        for item in attachments:
            if not isinstance(item, dict):
                continue
            if item.get("type") != "contact":
                continue
            payload_item = item.get("payload") if isinstance(item.get("payload"), dict) else {}
            tam_info = payload_item.get("tam_info") if isinstance(payload_item.get("tam_info"), dict) else {}
            contact_phone = _pick_first(
                payload_item.get("vcf_phone"),
                payload_item.get("phone"),
                tam_info.get("phone"),
                _extract_phone_from_vcf(payload_item.get("vcf_info")),
            )
            if contact_phone:
                break

        contact_phone = _pick_first(
            contact_phone,
            payload.get("contact_phone"),
            payload.get("phone"),
            message.get("contact_phone"),
            message.get("phone"),
            body.get("contact_phone"),
            body.get("phone"),
        )

        chat_id = _pick_first(
            payload.get("chat_id"),
            message.get("chat_id"),
            recipient.get("chat_id"),
            recipient.get("chatId"),
        )
        sender_id = _pick_first(
            payload.get("sender_id"),
            message.get("sender_id"),
            message.get("from_user_id"),
            user.get("user_id"),
            sender.get("user_id"),
            sender.get("id"),
        )
        text = _pick_first(
            payload.get("text"),
            message.get("text"),
            body.get("text"),
            "",
        )
        message_mid = _pick_first(
            message.get("mid"),
            body.get("mid"),
        )
        link_mid = _pick_first(
            link.get("mid"),
            link_message.get("mid"),
            link_message.get("message_id"),
        )
        callback_payload = _pick_first(
            message.get("callback_data"),
            message.get("callbackData"),
            body.get("callback_data"),
            body.get("callbackData"),
            body.get("data"),
            body.get("payload"),
            payload.get("callback_data"),
            payload.get("callbackData"),
            payload.get("data"),
            payload.get("payload"),
        )

        if chat_id is None or sender_id is None:
            return None

        event_uid = _pick_first(
            payload.get("update_id"),
            payload.get("updateId"),
            payload.get("event_id"),
            payload.get("eventId"),
        )

        return cls(
            chat_id=str(chat_id),
            sender_id=str(sender_id),
            text=str(text or ""),
            update_type=str(update_type) if update_type is not None else None,
            chat_type=_pick_first(recipient.get("chat_type"), recipient.get("type")),
            message_mid=str(message_mid) if message_mid is not None else None,
            link_mid=str(link_mid) if link_mid is not None else None,
            contact_phone=str(contact_phone) if contact_phone is not None else None,
            sender_first_name=_pick_first(
                sender.get("first_name"),
                sender.get("name"),
                user.get("first_name"),
                user.get("name"),
            ),
            sender_username=_pick_first(sender.get("username"), user.get("username")),
            update_id=str(event_uid) if event_uid is not None else None,
            raw_payload=payload,
            callback_payload=str(callback_payload) if callback_payload is not None else None,
        )
