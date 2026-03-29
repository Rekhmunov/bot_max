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
            headers["Authorization"] = f"Bearer {self.token}"
        return headers

    async def _post(self, endpoint: str, payload: dict[str, Any]) -> dict[str, Any]:
        # Development fallback if API credentials are not configured.
        if not self.token:
            return {"mock": True, "endpoint": endpoint, "payload": payload}

        async with httpx.AsyncClient(timeout=15) as client:
            response = await client.post(
                f"{self.base_url}{endpoint}",
                json=payload,
                headers=self._headers(),
            )
            response.raise_for_status()
            return response.json()

    async def send_text(self, chat_id: str, text: str) -> dict[str, Any]:
        return await self._post("/messages/send", {"chat_id": chat_id, "text": text})

    async def send_photo(self, chat_id: str, photo_url: str, caption: str | None = None) -> dict[str, Any]:
        payload: dict[str, Any] = {"chat_id": chat_id, "photo_url": photo_url}
        if caption:
            payload["caption"] = caption
        return await self._post("/messages/send-photo", payload)

    async def add_member_to_chat(self, chat_id: str, account_id: str) -> dict[str, Any]:
        return await self._post(
            "/chats/add-member",
            {"chat_id": chat_id, "account_id": account_id},
        )
