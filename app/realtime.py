from __future__ import annotations

import asyncio
from collections import defaultdict
from typing import Any

from fastapi import WebSocket


class ChatRealtimeHub:
    """In-memory workspace-scoped websocket broadcaster."""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._workspace_connections: dict[int, set[WebSocket]] = defaultdict(set)
        self._ws_workspace: dict[int, int] = {}

    async def connect(self, *, workspace_id: int, websocket: WebSocket) -> None:
        await websocket.accept()
        async with self._lock:
            self._workspace_connections[int(workspace_id)].add(websocket)
            self._ws_workspace[id(websocket)] = int(workspace_id)

    async def disconnect(self, websocket: WebSocket) -> None:
        async with self._lock:
            workspace_id = self._ws_workspace.pop(id(websocket), None)
            if workspace_id is None:
                return
            bucket = self._workspace_connections.get(int(workspace_id))
            if not bucket:
                return
            bucket.discard(websocket)
            if not bucket:
                self._workspace_connections.pop(int(workspace_id), None)

    async def broadcast_workspace(self, *, workspace_id: int, event: dict[str, Any]) -> int:
        async with self._lock:
            sockets = list(self._workspace_connections.get(int(workspace_id), set()))
        if not sockets:
            return 0
        delivered = 0
        stale: list[WebSocket] = []
        for ws in sockets:
            try:
                await ws.send_json(event)
                delivered += 1
            except Exception:
                stale.append(ws)
        for ws in stale:
            await self.disconnect(ws)
        return delivered


chat_realtime_hub = ChatRealtimeHub()

