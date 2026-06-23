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


def _extract_start_payload_from_text(text: str | None) -> str | None:
    value = (text or "").strip()
    if not value:
        return None
    # Accept common forms:
    # /start payload
    # /start@bot payload
    # start payload
    match = re.match(r"^/?start(?:@[a-z0-9_]+)?\s+([^\s]+)$", value, flags=re.IGNORECASE)
    if not match:
        return None
    return match.group(1).strip()


def _is_start_command_text(text: str | None) -> bool:
    value = (text or "").strip()
    if not value:
        return False
    return bool(re.match(r"^/?start(?:@[a-z0-9_]+)?(?:\s+.*)?$", value, flags=re.IGNORECASE))


def _normalize_update_type(value: Any) -> str | None:
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    # Convert camelCase / kebab-case to snake_case.
    snake = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", raw)
    snake = snake.replace("-", "_").strip().lower()
    aliases = {
        "new_message": "message_created",
        "message_created": "message_created",
        "message_callback": "message_callback",
        "incoming_message_received": "message_created",
        "message_read": "message_read",
        "messages_read": "message_read",
        "read": "message_read",
        "read_receipt": "message_read",
        "message_seen": "message_read",
        "seen": "message_read",
        "message_opened": "message_read",
        "opened": "message_read",
        "bot_start": "bot_started",
        "bot_started": "bot_started",
    }
    return aliases.get(snake, snake)


def _deep_find_first(root: Any, keys: set[str]) -> Any:
    stack = [root]
    visited: set[int] = set()
    while stack:
        current = stack.pop()
        marker = id(current)
        if marker in visited:
            continue
        visited.add(marker)
        if isinstance(current, dict):
            for key in keys:
                if key in current:
                    value = current.get(key)
                    if value is None:
                        continue
                    if isinstance(value, str) and not value.strip():
                        continue
                    return value
            for value in current.values():
                if isinstance(value, (dict, list)):
                    stack.append(value)
        elif isinstance(current, list):
            for item in current:
                if isinstance(item, (dict, list)):
                    stack.append(item)
    return None


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
    start_payload: str | None = Field(default=None)
    read_message_mid: str | None = Field(default=None)
    image_urls: list[str] = Field(default_factory=list)

    def event_uid_value(self) -> str | None:
        if self.update_id:
            return f"update:{self.update_id}"
        if (self.update_type or "").strip().lower() == "message_read" and self.read_message_mid:
            return f"read:{self.chat_id}:{self.sender_id}:{self.read_message_mid}"
        if self.message_mid:
            return f"message:{self.chat_id}:{self.sender_id}:{self.message_mid}"
        if self.message_mid:
            return f"message:{self.message_mid}"
        return None

    def is_read_event(self) -> bool:
        return (self.update_type or "").strip().lower() == "message_read"

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "MaxWebhookEvent | None":
        """
        Supports both simplified payload format and official Max Update payload.
        """
        body_root = payload.get("body") if isinstance(payload.get("body"), dict) else {}
        update_type = _pick_first(
            payload.get("update_type"),
            payload.get("updateType"),
            payload.get("event_type"),
            payload.get("eventType"),
            payload.get("type"),
            payload.get("type_webhook"),
            payload.get("typeWebhook"),
            body_root.get("update_type"),
            body_root.get("updateType"),
            body_root.get("event_type"),
            body_root.get("eventType"),
            body_root.get("type"),
            body_root.get("type_webhook"),
            body_root.get("typeWebhook"),
        )
        update_type = _normalize_update_type(update_type)
        update_status = _pick_first(
            payload.get("status"),
            body_root.get("status"),
            _deep_find_first(payload, {"status"}),
        )
        update_status_value = str(update_status or "").strip().lower()
        if update_type == "outgoing_message_status" and update_status_value in {"read", "seen", "opened"}:
            # Some providers send read receipts as outgoingMessageStatus + status=read.
            update_type = "message_read"
        message = payload.get("message") if isinstance(payload.get("message"), dict) else {}
        if not message and body_root:
            # Some Max webhook deliveries wrap message object under `body`.
            if isinstance(body_root.get("message"), dict):
                message = body_root.get("message")
        if not message:
            deep_message = _deep_find_first(payload, {"message"})
            if isinstance(deep_message, dict):
                message = deep_message
        # Some MAX integrations provide message payload under messageData/fileMessageData.
        message_data = (
            payload.get("messageData")
            if isinstance(payload.get("messageData"), dict)
            else (
                body_root.get("messageData")
                if isinstance(body_root.get("messageData"), dict)
                else {}
            )
        )
        sender_data = (
            payload.get("senderData")
            if isinstance(payload.get("senderData"), dict)
            else (
                body_root.get("senderData")
                if isinstance(body_root.get("senderData"), dict)
                else {}
            )
        )
        user = payload.get("user") if isinstance(payload.get("user"), dict) else {}
        chat_node = payload.get("chat") if isinstance(payload.get("chat"), dict) else {}
        sender = message.get("sender") if isinstance(message.get("sender"), dict) else {}
        recipient = message.get("recipient") if isinstance(message.get("recipient"), dict) else {}
        body = message.get("body") if isinstance(message.get("body"), dict) else {}
        link = message.get("link") if isinstance(message.get("link"), dict) else {}
        link_message = link.get("message") if isinstance(link.get("message"), dict) else {}

        # For forwarded messages: attachments live under link.message.body
        link_body = link_message.get("body") if isinstance(link_message.get("body"), dict) else {}

        attachments: list[Any] = []
        attachment_candidates = [
            body.get("attachments") if isinstance(body.get("attachments"), list) else None,
            message.get("attachments") if isinstance(message.get("attachments"), list) else None,
            message_data.get("attachments") if isinstance(message_data.get("attachments"), list) else None,
            # Forwarded messages: Max puts original attachments inside link.message.body
            link_body.get("attachments") if isinstance(link_body.get("attachments"), list) else None,
            link_message.get("attachments") if isinstance(link_message.get("attachments"), list) else None,
        ]
        has_structured_message_block = bool(message) or bool(body) or bool(message_data)
        if not has_structured_message_block:
            attachment_candidates.extend(
                [
                    payload.get("attachments") if isinstance(payload.get("attachments"), list) else None,
                    body_root.get("attachments") if isinstance(body_root.get("attachments"), list) else None,
                ]
            )
        for candidate in attachment_candidates:
            if isinstance(candidate, list) and candidate:
                attachments = candidate
                break
        image_urls: list[str] = []
        contact_phone = None
        for item in attachments:
            if not isinstance(item, dict):
                continue
            attachment_type = str(item.get("type") or "").strip().lower()
            _IMAGE_EXTS = (".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp", ".tiff", ".heic")
            _is_file_image = (
                attachment_type == "file"
                and str(item.get("filename") or item.get("name") or "").lower().endswith(_IMAGE_EXTS)
            )
            if attachment_type in {"image", "photo", "image_url", "sticker"} or _is_file_image:
                payload_item = item.get("payload") if isinstance(item.get("payload"), dict) else {}
                image_candidates: list[str] = []
                image_url = _pick_first(
                    payload_item.get("url"),
                    payload_item.get("photo_url"),
                    payload_item.get("original_url"),
                    payload_item.get("src"),
                    payload_item.get("link"),
                    item.get("url"),
                    item.get("photo_url"),
                )
                if image_url:
                    image_candidates.append(str(image_url))
                photos_value = payload_item.get("photos")
                if isinstance(photos_value, list):
                    for photo_item in photos_value:
                        if not isinstance(photo_item, dict):
                            continue
                        photo_url = _pick_first(
                            photo_item.get("url"),
                            photo_item.get("src"),
                            photo_item.get("link"),
                        )
                        if photo_url:
                            image_candidates.append(str(photo_url))
                if image_candidates:
                    for candidate in image_candidates:
                        normalized_candidate = str(candidate or "").strip()
                        if not normalized_candidate:
                            continue
                        if normalized_candidate in image_urls:
                            continue
                        image_urls.append(normalized_candidate)
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

        # MAX incoming media format: messageData.typeMessage + messageData.fileMessageData.downloadUrl
        type_message = str(_pick_first(
            message_data.get("typeMessage"),
            message_data.get("type_message"),
            payload.get("typeMessage"),
        ) or "").strip().lower()
        if type_message in {"imagemessage", "image_message"}:
            file_message_data = (
                message_data.get("fileMessageData")
                if isinstance(message_data.get("fileMessageData"), dict)
                else {}
            )
            media_url = _pick_first(
                file_message_data.get("downloadUrl"),
                file_message_data.get("download_url"),
                file_message_data.get("url"),
                file_message_data.get("fileUrl"),
                file_message_data.get("file_url"),
            )
            if media_url:
                normalized_media_url = str(media_url).strip()
                if normalized_media_url and normalized_media_url not in image_urls:
                    image_urls.append(normalized_media_url)
            caption_text = _pick_first(
                file_message_data.get("caption"),
                message_data.get("caption"),
            )
            if caption_text is not None and str(caption_text).strip():
                text = str(caption_text).strip()

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
            payload.get("chatId"),
            sender_data.get("chatId"),
            sender_data.get("chat_id"),
            message.get("chat_id"),
            message.get("chatId"),
            recipient.get("chat_id"),
            recipient.get("chatId"),
            chat_node.get("chat_id"),
            chat_node.get("chatId"),
            chat_node.get("id"),
            body_root.get("chat_id"),
            body_root.get("chatId"),
            _deep_find_first(payload, {"chat_id", "chatId"}),
        )
        sender_id = _pick_first(
            payload.get("sender_id"),
            payload.get("senderId"),
            sender_data.get("sender"),
            sender_data.get("chatId"),
            message.get("sender_id"),
            message.get("senderId"),
            message.get("from_user_id"),
            sender.get("user_id"),
            sender.get("id"),
            user.get("user_id"),
            user.get("userId"),
            user.get("id"),
            body_root.get("sender_id"),
            body_root.get("senderId"),
            body_root.get("user_id"),
            body_root.get("userId"),
            payload.get("user_id"),
            payload.get("userId"),
            (
                payload.get("instanceData", {}).get("wid")
                if isinstance(payload.get("instanceData"), dict)
                else None
            ),
            _deep_find_first(payload, {"sender_id", "senderId", "user_id", "userId", "from_user_id"}),
        )
        text = _pick_first(
            payload.get("text"),
            message.get("text"),
            body.get("text"),
            body_root.get("text"),
            (
                message_data.get("textMessageData", {}).get("textMessage")
                if isinstance(message_data.get("textMessageData"), dict)
                else None
            ),
            _deep_find_first(payload, {"text", "message_text", "messageText"}),
            "",
        )
        message_mid = _pick_first(
            payload.get("idMessage"),
            payload.get("id_message"),
            payload.get("messageId"),
            payload.get("message_id"),
            message.get("mid"),
            body.get("mid"),
        )
        link_mid = _pick_first(
            link.get("mid"),
            link_message.get("mid"),
            link_message.get("message_id"),
        )
        message_callback = message.get("callback") if isinstance(message.get("callback"), dict) else {}
        if not message_callback:
            message_callback = (
                message.get("message_callback")
                if isinstance(message.get("message_callback"), dict)
                else {}
            )
        payload_callback = payload.get("callback") if isinstance(payload.get("callback"), dict) else {}
        if not payload_callback:
            payload_callback = (
                payload.get("message_callback")
                if isinstance(payload.get("message_callback"), dict)
                else {}
            )
        body_callback = body_root.get("callback") if isinstance(body_root.get("callback"), dict) else {}
        if not body_callback:
            body_callback = (
                body_root.get("message_callback")
                if isinstance(body_root.get("message_callback"), dict)
                else {}
            )
        deep_callback = _deep_find_first(payload, {"callback", "message_callback"})
        deep_callback = deep_callback if isinstance(deep_callback, dict) else {}
        callback_payload = _pick_first(
            message.get("callback_data"),
            message.get("callbackData"),
            message_callback.get("callback_data"),
            message_callback.get("callbackData"),
            message_callback.get("payload"),
            message_callback.get("data"),
            message_callback.get("command"),
            body.get("callback_data"),
            body.get("callbackData"),
            body.get("data"),
            body.get("payload"),
            payload_callback.get("callback_data"),
            payload_callback.get("callbackData"),
            payload_callback.get("payload"),
            payload_callback.get("data"),
            payload_callback.get("command"),
            body_callback.get("callback_data"),
            body_callback.get("callbackData"),
            body_callback.get("payload"),
            body_callback.get("data"),
            body_callback.get("command"),
            deep_callback.get("callback_data"),
            deep_callback.get("callbackData"),
            deep_callback.get("payload"),
            deep_callback.get("data"),
            deep_callback.get("command"),
            payload.get("callback_data"),
            payload.get("callbackData"),
            payload.get("data"),
            payload.get("payload"),
        )
        start_payload = _pick_first(
            payload.get("payload"),
            payload.get("start"),
            payload.get("start_payload"),
            payload.get("startPayload"),
            payload.get("start_param"),
            payload.get("startParam"),
            message.get("payload"),
            message.get("start"),
            message.get("start_payload"),
            message.get("startPayload"),
            body.get("payload"),
            body.get("start"),
            body.get("start_payload"),
            body.get("startPayload"),
            body_root.get("payload"),
            body_root.get("start"),
            body_root.get("start_payload"),
            body_root.get("startPayload"),
            body_root.get("start_param"),
            body_root.get("startParam"),
            _deep_find_first(
                payload,
                {
                    "start_payload",
                    "startPayload",
                    "start_param",
                    "startParam",
                    "payload",
                    "start",
                },
            ),
        )
        read_message_mid = _pick_first(
            payload.get("idMessage"),
            payload.get("id_message"),
            payload.get("messageId"),
            payload.get("message_id"),
            payload.get("read_message_mid"),
            payload.get("readMessageMid"),
            payload.get("last_read_mid"),
            payload.get("lastReadMid"),
            payload.get("last_read_message_mid"),
            payload.get("lastReadMessageMid"),
            payload.get("read_to_mid"),
            payload.get("readToMid"),
            payload.get("read_mid"),
            payload.get("readMid"),
            message.get("idMessage"),
            message.get("id_message"),
            message.get("messageId"),
            message.get("message_id"),
            message.get("read_message_mid"),
            message.get("readMessageMid"),
            message.get("last_read_mid"),
            message.get("lastReadMid"),
            message.get("last_read_message_mid"),
            message.get("lastReadMessageMid"),
            body.get("idMessage"),
            body.get("id_message"),
            body.get("messageId"),
            body.get("message_id"),
            body.get("read_message_mid"),
            body.get("readMessageMid"),
            body.get("last_read_mid"),
            body.get("lastReadMid"),
            body_root.get("idMessage"),
            body_root.get("id_message"),
            body_root.get("messageId"),
            body_root.get("message_id"),
            body_root.get("read_message_mid"),
            body_root.get("readMessageMid"),
            body_root.get("last_read_mid"),
            body_root.get("lastReadMid"),
            _deep_find_first(
                payload,
                {
                    "idMessage",
                    "id_message",
                    "messageId",
                    "message_id",
                    "read_message_mid",
                    "readMessageMid",
                    "last_read_mid",
                    "lastReadMid",
                    "last_read_message_mid",
                    "lastReadMessageMid",
                    "read_to_mid",
                    "readToMid",
                    "read_mid",
                    "readMid",
                },
            ),
        )
        if start_payload is None:
            start_payload = _extract_start_payload_from_text(str(text or ""))
        if update_type is None:
            if start_payload is not None or _is_start_command_text(str(text or "")):
                update_type = "bot_started"
            elif chat_id is not None and sender_id is not None:
                update_type = "message_created"

        is_read_event = (str(update_type or "").strip().lower() == "message_read")
        if is_read_event:
            # Read receipts from providers can be partial: keep the event parseable
            # even when one side identifier is omitted.
            if chat_id is None:
                chat_id = _pick_first(
                    recipient.get("chat_id"),
                    recipient.get("chatId"),
                    chat_node.get("chat_id"),
                    chat_node.get("chatId"),
                    chat_node.get("id"),
                    body_root.get("chat_id"),
                    body_root.get("chatId"),
                    "",
                )
            if sender_id is None:
                sender_id = _pick_first(
                    sender.get("user_id"),
                    sender.get("id"),
                    user.get("user_id"),
                    user.get("userId"),
                    user.get("id"),
                    body_root.get("sender_id"),
                    body_root.get("senderId"),
                    payload.get("sender_id"),
                    payload.get("senderId"),
                    "",
                )

        if not is_read_event and (chat_id is None or sender_id is None):
            return None
        if is_read_event and chat_id is None and sender_id is None:
            return None

        event_uid = _pick_first(
            payload.get("update_id"),
            payload.get("updateId"),
            payload.get("event_id"),
            payload.get("eventId"),
            _deep_find_first(payload, {"update_id", "updateId", "event_id", "eventId"}),
        )

        if update_type is None:
            update_type = _normalize_update_type(
                _deep_find_first(payload, {"update_type", "updateType", "event_type", "eventType"})
            )

        return cls(
            chat_id=str(chat_id or ""),
            sender_id=str(sender_id or ""),
            text=str(text or ""),
            update_type=str(update_type) if update_type is not None else None,
            chat_type=_pick_first(recipient.get("chat_type"), recipient.get("type")),
            message_mid=str(message_mid) if message_mid is not None else None,
            link_mid=str(link_mid) if link_mid is not None else None,
            contact_phone=str(contact_phone) if contact_phone is not None else None,
            sender_first_name=_pick_first(
                sender_data.get("senderName"),
                sender.get("first_name"),
                sender.get("name"),
                user.get("first_name"),
                user.get("name"),
            ),
            sender_username=_pick_first(sender.get("username"), user.get("username")),
            update_id=str(event_uid) if event_uid is not None else None,
            raw_payload=payload,
            callback_payload=str(callback_payload) if callback_payload is not None else None,
            start_payload=str(start_payload) if start_payload is not None else None,
            read_message_mid=str(read_message_mid) if read_message_mid is not None else None,
            image_urls=image_urls,
        )
