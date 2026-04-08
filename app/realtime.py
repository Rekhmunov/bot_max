from __future__ import annotations

import asyncio
import time
from collections import defaultdict
from typing import Any

from fastapi import WebSocket


class ChatRealtimeHub:
    """In-memory workspace-scoped websocket broadcaster."""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._workspace_connections: dict[int, set[WebSocket]] = defaultdict(set)
        self._ws_workspace: dict[int, int] = {}
        self._last_incoming_hint_at: dict[tuple[int, int], float] = {}
        self._workspace_hint_window: dict[int, list[float]] = defaultdict(list)

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

    def should_emit_incoming_hint(
        self,
        *,
        workspace_id: int,
        conversation_id: int | None,
        min_interval_ms: int = 200,
        now_monotonic: float | None = None,
    ) -> bool:
        """
        Best-effort in-process dedupe for bursty incoming hints.
        Keeps realtime responsive while reducing redundant ws events.
        """
        ws_id = int(workspace_id or 0)
        conv_id = int(conversation_id or 0)
        key = (ws_id, conv_id)
        now_value = float(now_monotonic if now_monotonic is not None else time.monotonic())
        min_delta = max(0.01, float(min_interval_ms or 0) / 1000.0)
        prev = float(self._last_incoming_hint_at.get(key, 0.0) or 0.0)
        if now_value - prev < min_delta:
            return False
        self._last_incoming_hint_at[key] = now_value
        return True

    def should_emit_workspace_hint_rate_limited(
        self,
        *,
        workspace_id: int,
        now_monotonic: float | None = None,
        window_seconds: float = 1.0,
        max_events_per_window: int = 12,
    ) -> bool:
        """
        Soft workspace-level gate for incoming hint fan-out bursts.
        Keeps responsiveness while preventing websocket flood spikes.
        """
        ws_id = int(workspace_id or 0)
        if ws_id <= 0:
            return True
        now_value = float(now_monotonic if now_monotonic is not None else time.monotonic())
        window = max(0.2, float(window_seconds or 1.0))
        limit = max(1, int(max_events_per_window or 1))
        bucket = self._workspace_hint_window[ws_id]
        cutoff = now_value - window
        while bucket and bucket[0] < cutoff:
            bucket.pop(0)
        if len(bucket) >= limit:
            return False
        bucket.append(now_value)
        return True


chat_realtime_hub = ChatRealtimeHub()

