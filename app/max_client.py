from __future__ import annotations

from pathlib import Path
from typing import Any
from urllib.parse import quote_plus

import httpx

from app.config import settings


class MaxClient:
    def __init__(self, *, token: str | None = None) -> None:
        self.base_url = settings.max_api_base_url.rstrip("/")
        self.token = (token if token is not None else settings.max_bot_token)

    def _headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self.token:
            token_value = self.token.strip()
            if token_value.lower().startswith("bearer "):
                headers["Authorization"] = token_value
            else:
                headers["Authorization"] = token_value
        return headers

    async def _post(self, endpoint: str, payload: dict[str, Any]) -> dict[str, Any]:
        # Development fallback if API credentials are not configured.
        if not self.token:
            return {"mock": True, "endpoint": endpoint, "payload": payload}

        try:
            async with httpx.AsyncClient(timeout=15) as client:
                response = await client.post(
                    f"{self.base_url}{endpoint}",
                    json=payload,
                    headers=self._headers(),
                )
                try:
                    data: Any = response.json()
                except ValueError:
                    data = {"raw": response.text}

                if response.is_error:
                    return {
                        "success": False,
                        "status_code": response.status_code,
                        "endpoint": endpoint,
                        "response": data,
                    }

                if isinstance(data, dict):
                    return data
                return {"success": True, "data": data}
        except httpx.HTTPError as exc:
            return {
                "success": False,
                "endpoint": endpoint,
                "error": "http_error",
                "details": str(exc),
            }

    async def _post_file(
        self,
        url: str,
        *,
        file_name: str,
        content: bytes,
        mime_type: str,
    ) -> dict[str, Any]:
        if not self.token:
            return {"mock": True, "url": url}
        try:
            async with httpx.AsyncClient(timeout=60) as client:
                response = await client.post(
                    str(url),
                    files={"data": (file_name, content, mime_type)},
                    headers={"Authorization": self._headers().get("Authorization", "")},
                )
                try:
                    data: Any = response.json()
                except ValueError:
                    data = {"raw": response.text}
                if response.is_error:
                    return {
                        "success": False,
                        "status_code": response.status_code,
                        "endpoint": "upload_file",
                        "response": data,
                    }
                if isinstance(data, dict):
                    return data
                return {"success": True, "data": data}
        except httpx.HTTPError as exc:
            return {
                "success": False,
                "endpoint": "upload_file",
                "error": "http_error",
                "details": str(exc),
            }

    async def _put(self, endpoint: str, payload: dict[str, Any]) -> dict[str, Any]:
        if not self.token:
            return {"mock": True, "endpoint": endpoint, "payload": payload}
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                response = await client.put(
                    f"{self.base_url}{endpoint}",
                    json=payload,
                    headers=self._headers(),
                )
                try:
                    data: Any = response.json()
                except ValueError:
                    data = {"raw": response.text}
                if response.is_error:
                    return {
                        "success": False,
                        "status_code": response.status_code,
                        "endpoint": endpoint,
                        "response": data,
                    }
                if isinstance(data, dict):
                    return data
                return {"success": True, "data": data}
        except httpx.HTTPError as exc:
            return {
                "success": False,
                "endpoint": endpoint,
                "error": "http_error",
                "details": str(exc),
            }

    async def _delete(self, endpoint: str) -> dict[str, Any]:
        if not self.token:
            return {"mock": True, "endpoint": endpoint}
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                response = await client.delete(
                    f"{self.base_url}{endpoint}",
                    headers=self._headers(),
                )
                try:
                    data: Any = response.json()
                except ValueError:
                    data = {"raw": response.text}
                if response.is_error:
                    return {
                        "success": False,
                        "status_code": response.status_code,
                        "endpoint": endpoint,
                        "response": data,
                    }
                if isinstance(data, dict):
                    return data
                return {"success": True, "data": data}
        except httpx.HTTPError as exc:
            return {
                "success": False,
                "endpoint": endpoint,
                "error": "http_error",
                "details": str(exc),
            }

    async def send_message(
        self,
        *,
        text: str | None = None,
        chat_id: str | None = None,
        user_id: str | None = None,
        attachments: list[dict[str, Any]] | None = None,
        text_format: str | None = None,
    ) -> dict[str, Any]:
        if not chat_id and not user_id:
            return {"success": False, "error": "chat_id_or_user_id_required"}

        query_part = f"chat_id={chat_id}" if chat_id else f"user_id={user_id}"
        body: dict[str, Any] = {}
        if text is not None:
            body["text"] = text
        if attachments is not None:
            body["attachments"] = attachments
        if text_format in {"markdown", "html"}:
            body["format"] = text_format
        return await self._post(f"/messages?{query_part}", body)

    async def send_text(self, chat_id: str, text: str, text_format: str | None = None) -> dict[str, Any]:
        return await self.send_message(text=text, chat_id=chat_id, text_format=text_format)

    async def send_text_to_user(self, user_id: str, text: str, text_format: str | None = None) -> dict[str, Any]:
        return await self.send_message(text=text, user_id=user_id, text_format=text_format)

    async def send_photo(self, chat_id: str, photo_url: str, caption: str | None = None) -> dict[str, Any]:
        # Backward-compatible path: if caller passes URL instead of bytes,
        # keep legacy behavior. New media-group flow uploads image bytes.
        text = f"{caption}\n{photo_url}" if caption else photo_url
        return await self.send_text(chat_id=chat_id, text=text)

    async def send_photo_to_user(
        self,
        *,
        user_id: str,
        photo_url: str,
        caption: str | None = None,
    ) -> dict[str, Any]:
        # Backward-compatible URL fallback.
        text = f"{caption}\n{photo_url}" if caption else photo_url
        return await self.send_text_to_user(user_id=user_id, text=text)

    @staticmethod
    def _guess_mime_type(file_name: str) -> str:
        ext = Path(file_name or "").suffix.lower()
        mapping = {
            ".jpg": "image/jpeg",
            ".jpeg": "image/jpeg",
            ".png": "image/png",
            ".gif": "image/gif",
            ".webp": "image/webp",
            ".bmp": "image/bmp",
            ".tiff": "image/tiff",
            ".heic": "image/heic",
        }
        return mapping.get(ext, "application/octet-stream")

    async def upload_image_bytes(
        self,
        *,
        file_name: str,
        content: bytes,
    ) -> dict[str, Any]:
        """
        Upload image to MAX and return attachment payload:
        {"type":"image","payload":{"token":"..."}}
        """
        upload_info = await self._post("/uploads?type=image", {})
        if not isinstance(upload_info, dict):
            return {"success": False, "error": "invalid_upload_response"}
        upload_url = str(upload_info.get("url") or "").strip()
        if not upload_url:
            return {"success": False, "error": "upload_url_missing", "response": upload_info}
        mime = self._guess_mime_type(file_name)
        upload_result = await self._post_file(
            upload_url,
            file_name=(file_name or "image.jpg"),
            content=content,
            mime_type=mime,
        )
        if not isinstance(upload_result, dict):
            return {"success": False, "error": "invalid_upload_result"}
        token = str(upload_result.get("token") or "").strip()
        if not token:
            # Some responses can still include token under nested payload.
            payload_obj = upload_result.get("payload")
            if isinstance(payload_obj, dict):
                token = str(payload_obj.get("token") or "").strip()
        if not token:
            return {"success": False, "error": "upload_token_missing", "response": upload_result}
        return {
            "success": True,
            "attachment": {
                "type": "image",
                "payload": {"token": token},
            },
            "token": token,
        }

    async def send_images(
        self,
        *,
        chat_id: str | None = None,
        user_id: str | None = None,
        images: list[tuple[str, bytes]],
        text: str | None = None,
    ) -> dict[str, Any]:
        if not chat_id and not user_id:
            return {"success": False, "error": "chat_id_or_user_id_required"}
        if not images:
            return {"success": False, "error": "images_required"}
        attachments: list[dict[str, Any]] = []
        for file_name, content in images:
            uploaded = await self.upload_image_bytes(
                file_name=(file_name or "image.jpg"),
                content=content,
            )
            if not bool(uploaded.get("success")):
                return uploaded
            attachment = uploaded.get("attachment")
            if isinstance(attachment, dict):
                attachments.append(attachment)
        if not attachments:
            return {"success": False, "error": "image_attachments_missing"}
        return await self.send_message(
            chat_id=chat_id,
            user_id=user_id,
            text=(text if text else None),
            attachments=attachments,
        )

    async def edit_message(
        self,
        *,
        message_id: str,
        text: str | None = None,
        attachments: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {}
        if text is not None:
            payload["text"] = text
        if attachments is not None:
            payload["attachments"] = attachments
        return await self._put(f"/messages?message_id={message_id}", payload)

    async def delete_message(self, *, message_id: str) -> dict[str, Any]:
        return await self._delete(f"/messages?message_id={message_id}")

    async def subscribe_webhook(
        self,
        *,
        url: str,
        update_types: list[str] | None = None,
        secret: str | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {"url": str(url or "").strip()}
        if update_types:
            payload["update_types"] = [str(item).strip() for item in update_types if str(item).strip()]
        if secret:
            payload["secret"] = str(secret).strip()
        return await self._post("/subscriptions", payload)

    async def unsubscribe_webhook(self, *, url: str) -> dict[str, Any]:
        normalized = str(url or "").strip()
        if not normalized:
            return {"success": False, "error": "url_required"}
        return await self._delete(f"/subscriptions?url={quote_plus(normalized)}")

    async def add_member_to_chat(self, chat_id: str, account_id: str) -> dict[str, Any]:
        return await self._post(
            f"/chats/{chat_id}/members",
            {"user_ids": [int(account_id)] if str(account_id).isdigit() else [account_id]},
        )
