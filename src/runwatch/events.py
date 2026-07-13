from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

from .storage import RunStore


class EventBus:
    """Persisted event stream with best-effort in-memory fan-out for SSE clients."""

    def __init__(
        self, store: RunStore, run_id: str, *, subscriber_queue_size: int = 256
    ) -> None:
        self.store = store
        self.run_id = run_id
        self.subscriber_queue_size = subscriber_queue_size
        self._subscribers: set[asyncio.Queue[dict[str, Any]]] = set()
        self._lock = asyncio.Lock()

    async def publish(self, event_type: str, payload: dict[str, Any]) -> dict[str, Any]:
        event = self.store.append_event(self.run_id, event_type, payload)
        await self._fan_out(event)
        return event

    async def publish_ephemeral(
        self, event_type: str, payload: dict[str, Any]
    ) -> dict[str, Any]:
        """Fan out a refresh hint without adding high-volume data to SQLite."""

        event = {
            "seq": None,
            "run_id": self.run_id,
            "timestamp": None,
            "type": event_type,
            "payload": payload,
        }
        await self._fan_out(event)
        return event

    async def _fan_out(self, event: dict[str, Any]) -> None:
        async with self._lock:
            stale: list[asyncio.Queue[dict[str, Any]]] = []
            for queue in self._subscribers:
                try:
                    queue.put_nowait(event)
                except asyncio.QueueFull:
                    try:
                        queue.get_nowait()
                        queue.put_nowait(event)
                    except (asyncio.QueueEmpty, asyncio.QueueFull):
                        stale.append(queue)
            for queue in stale:
                self._subscribers.discard(queue)

    @asynccontextmanager
    async def subscribe(self) -> AsyncGenerator[asyncio.Queue[dict[str, Any]], None]:
        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(
            maxsize=self.subscriber_queue_size
        )
        async with self._lock:
            self._subscribers.add(queue)
        try:
            yield queue
        finally:
            async with self._lock:
                self._subscribers.discard(queue)
