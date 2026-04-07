from __future__ import annotations

from pathlib import Path
from typing import Any
from urllib.parse import quote_plus

import asyncio
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
        auth_header = self._headers().get("Authorization", "")

        async def _run_upload(
            *,
            method: str,
            field_name: str | None = None,
            use_auth: bool = False,
        ) -> dict[str, Any]:
            headers: dict[str, str] = {}
            if use_auth and auth_header:
                headers["Authorization"] = auth_header
            if method == "put_raw":
                headers.setdefault("Content-Type", mime_type)
            try:
                async with httpx.AsyncClient(timeout=60) as client:
                    if method == "post_multipart":
                        if not field_name:
                            return {"success": False, "error": "multipart_field_missing"}
                        response = await client.post(
                            str(url),
                            files={field_name: (file_name, content, mime_type)},
                            headers=headers,
                        )
                    elif method == "put_raw":
                        response = await client.put(
                            str(url),
                            content=content,
                            headers=headers,
                        )
                    elif method == "post_raw":
                        response = await client.post(
                            str(url),
                            content=content,
                            headers=headers,
                        )
                    else:
                        return {"success": False, "error": "unsupported_upload_method"}
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

        # Provider/webhook setups differ between bots/workspaces:
        # try multiple common upload variants before returning failure.
        attempts: list[dict[str, str | bool | None]] = [
            {"method": "post_multipart", "field_name": "data", "use_auth": True},
            {"method": "post_multipart", "field_name": "file", "use_auth": True},
            {"method": "post_multipart", "field_name": "data", "use_auth": False},
            {"method": "post_multipart", "field_name": "file", "use_auth": False},
            {"method": "put_raw", "field_name": None, "use_auth": False},
            {"method": "post_raw", "field_name": None, "use_auth": False},
        ]
        last_error: dict[str, Any] = {"success": False, "error": "upload_attempts_exhausted"}
        for attempt in attempts:
            result = await _run_upload(
                method=str(attempt["method"]),
                field_name=(str(attempt["field_name"]) if attempt["field_name"] else None),
                use_auth=bool(attempt["use_auth"]),
            )
            status_code_raw = result.get("status_code") if isinstance(result, dict) else None
            try:
                status_code = int(status_code_raw) if status_code_raw is not None else None
            except (TypeError, ValueError):
                status_code = None
            # Return immediately on success.
            if bool(result.get("success")) or (status_code is not None and status_code < 400):
                return result
            # Some signed upload URLs reject Authorization header (403/401),
            # but work without it; continue fallback attempts.
            last_error = result if isinstance(result, dict) else last_error
        return last_error

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

    @staticmethod
    def _extract_token_from_payload(payload: Any) -> str:
        """Best-effort token extraction for varying MAX upload responses."""
        token_keys = ("token", "upload_token", "attachment_token")
        queue: list[Any] = [payload]
        visited_ids: set[int] = set()

        while queue:
            current = queue.pop(0)
            try:
                object_id = id(current)
            except Exception:
                object_id = 0
            if object_id and object_id in visited_ids:
                continue
            if object_id:
                visited_ids.add(object_id)

            if isinstance(current, dict):
                for key in token_keys:
                    value = current.get(key)
                    if isinstance(value, str) and value.strip():
                        return value.strip()
                for value in current.values():
                    queue.append(value)
                continue

            if isinstance(current, list):
                queue.extend(current)
                continue

            if isinstance(current, str):
                raw = current.strip()
                if not raw:
                    continue
                if raw.startswith("{") or raw.startswith("["):
                    try:
                        import json
                        parsed = json.loads(raw)
                    except Exception:
                        parsed = None
                    if parsed is not None:
                        queue.append(parsed)
                continue

        return ""

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
        upload_url = str(
            upload_info.get("url")
            or upload_info.get("upload_url")
            or upload_info.get("link")
            or upload_info.get("href")
            or ""
        ).strip()
        if not upload_url:
            upload_url = self._extract_upload_url_from_payload(upload_info)
        if not upload_url:
            return {"success": False, "error": "upload_url_missing", "response": upload_info}
        upload_info_token = self._extract_token_from_payload(upload_info)
        mime = self._guess_mime_type(file_name)
        upload_result = await self._post_file(
            upload_url,
            file_name=(file_name or "image.jpg"),
            content=content,
            mime_type=mime,
        )
        if not isinstance(upload_result, dict):
            return {"success": False, "error": "invalid_upload_result"}
        token = self._extract_token_from_payload(upload_result)
        if not token and upload_info_token:
            token = upload_info_token
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

    @staticmethod
    def _extract_upload_url_from_payload(payload: Any) -> str:
        """Best-effort upload URL extraction for varying /uploads responses."""
        url_keys = ("url", "upload_url", "link", "href")
        queue: list[Any] = [payload]
        visited_ids: set[int] = set()

        while queue:
            current = queue.pop(0)
            try:
                object_id = id(current)
            except Exception:
                object_id = 0
            if object_id and object_id in visited_ids:
                continue
            if object_id:
                visited_ids.add(object_id)

            if isinstance(current, dict):
                for key in url_keys:
                    value = current.get(key)
                    if isinstance(value, str):
                        raw = value.strip()
                        if raw.startswith("http://") or raw.startswith("https://"):
                            return raw
                for value in current.values():
                    queue.append(value)
                continue

            if isinstance(current, list):
                queue.extend(current)
                continue

            if isinstance(current, str):
                raw = current.strip()
                if not raw:
                    continue
                if raw.startswith("http://") or raw.startswith("https://"):
                    return raw
                if raw.startswith("{") or raw.startswith("["):
                    try:
                        import json
                        parsed = json.loads(raw)
                    except Exception:
                        parsed = None
                    if parsed is not None:
                        queue.append(parsed)
        return ""

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
        wait_seconds = 0.6
        max_attempts = 4
        for attempt_idx in range(max_attempts):
            result = await self.send_message(
                chat_id=chat_id,
                user_id=user_id,
                text=(text if text else None),
                attachments=attachments,
            )
            response = result.get("response") if isinstance(result, dict) else {}
            code = str(response.get("code") or "").strip().lower() if isinstance(response, dict) else ""
            if code != "attachment.not.ready":
                return result
            if attempt_idx >= max_attempts - 1:
                return result
            await asyncio.sleep(wait_seconds)
            wait_seconds = min(wait_seconds * 2.0, 5.0)
        return {"success": False, "error": "attachment_not_ready_retry_exhausted"}

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
