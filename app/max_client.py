from __future__ import annotations

from typing import Any

import httpx

from app.config import settings


class MaxClient:
    def __init__(self) -> None:
        self.base_url = settings.max_api_base_url.rstrip("/")
        self.token = settings.max_bot_token

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
    ) -> dict[str, Any]:
        if not chat_id and not user_id:
            return {"success": False, "error": "chat_id_or_user_id_required"}

        query_part = f"chat_id={chat_id}" if chat_id else f"user_id={user_id}"
        body: dict[str, Any] = {}
        if text is not None:
            body["text"] = text
        if attachments is not None:
            body["attachments"] = attachments
        return await self._post(f"/messages?{query_part}", body)

    async def send_text(self, chat_id: str, text: str) -> dict[str, Any]:
        return await self.send_message(text=text, chat_id=chat_id)

    async def send_text_to_user(self, user_id: str, text: str) -> dict[str, Any]:
        return await self.send_message(text=text, user_id=user_id)

    async def send_photo(self, chat_id: str, photo_url: str, caption: str | None = None) -> dict[str, Any]:
        # Max API requires media upload, so for now we send URL as text fallback.
        # This keeps quick replies functional even without implementing /uploads flow.
        text = f"{caption}\n{photo_url}" if caption else photo_url
        return await self.send_text(chat_id=chat_id, text=text)

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

    async def add_member_to_chat(self, chat_id: str, account_id: str) -> dict[str, Any]:
        return await self._post(
            f"/chats/{chat_id}/members",
            {"user_ids": [int(account_id)] if str(account_id).isdigit() else [account_id]},
        )
