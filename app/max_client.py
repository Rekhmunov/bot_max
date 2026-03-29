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

    async def send_text(self, chat_id: str, text: str) -> dict[str, Any]:
        return await self._post(f"/messages?chat_id={chat_id}", {"text": text})

    async def send_photo(self, chat_id: str, photo_url: str, caption: str | None = None) -> dict[str, Any]:
        # Max API requires media upload, so for now we send URL as text fallback.
        # This keeps quick replies functional even without implementing /uploads flow.
        text = f"{caption}\n{photo_url}" if caption else photo_url
        return await self.send_text(chat_id=chat_id, text=text)

    async def add_member_to_chat(self, chat_id: str, account_id: str) -> dict[str, Any]:
        return await self._post(
            f"/chats/{chat_id}/members",
            {"user_ids": [int(account_id)] if str(account_id).isdigit() else [account_id]},
        )
