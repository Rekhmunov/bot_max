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
    raw_payload: dict[str, Any] = Field(default_factory=dict)

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "MaxWebhookEvent | None":
        """
        Supports both simplified payload format and official Max Update payload.
        """
        message = payload.get("message") if isinstance(payload.get("message"), dict) else {}
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

        if chat_id is None or sender_id is None:
            return None

        return cls(
            chat_id=str(chat_id),
            sender_id=str(sender_id),
            text=str(text or ""),
            update_type=payload.get("update_type"),
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
            raw_payload=payload,
        )
