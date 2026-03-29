from __future__ import annotations

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


class MaxWebhookEvent(BaseModel):
    chat_id: str = Field(..., description="ID чата, где пришло событие")
    sender_id: str = Field(..., description="ID отправителя сообщения")
    text: str = Field(default="", description="Текст сообщения")
    update_type: str | None = Field(default=None, description="Тип события update")

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "MaxWebhookEvent | None":
        """
        Supports both simplified payload format and official Max Update payload.
        """
        message = payload.get("message") if isinstance(payload.get("message"), dict) else {}
        sender = message.get("sender") if isinstance(message.get("sender"), dict) else {}
        recipient = message.get("recipient") if isinstance(message.get("recipient"), dict) else {}
        body = message.get("body") if isinstance(message.get("body"), dict) else {}

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
            sender.get("user_id"),
            sender.get("id"),
        )
        text = _pick_first(
            payload.get("text"),
            message.get("text"),
            body.get("text"),
            "",
        )

        if chat_id is None or sender_id is None:
            return None

        return cls(
            chat_id=str(chat_id),
            sender_id=str(sender_id),
            text=str(text or ""),
            update_type=payload.get("update_type"),
        )
