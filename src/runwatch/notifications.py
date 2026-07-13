from __future__ import annotations

import asyncio
from typing import Any, cast

import httpx

from .events import EventBus
from .models import NotificationSettings
from .storage import RunStore


def _event_kernel_epoch(payload: dict[str, Any]) -> str:
    value = payload.get("kernel_epoch")
    if value is None:
        return "unknown"
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError("Notification event kernel_epoch must be nonnegative")
    return str(value)


class NotificationManager:
    """Persist and deliver webhook/ntfy notifications without blocking run control."""

    def __init__(
        self,
        *,
        settings: NotificationSettings,
        store: RunStore,
        bus: EventBus,
        run_id: str,
    ) -> None:
        self.settings = settings
        self.store = store
        self.bus = bus
        self.run_id = run_id
        self._listener_task: asyncio.Task[None] | None = None
        self._periodic_task: asyncio.Task[None] | None = None
        self._delivery_task: asyncio.Task[None] | None = None
        self._delivery_wake = asyncio.Event()
        self._client = httpx.AsyncClient(timeout=settings.request_timeout_seconds)

    async def start(self) -> None:
        if self._delivery_task is not None:
            return
        if not self._destinations():
            return
        self.store.recover_notification_deliveries(self.run_id)
        self._delivery_task = asyncio.create_task(
            self._deliver(), name=f"notification-delivery:{self.run_id}"
        )
        self._listener_task = asyncio.create_task(
            self._listen(), name=f"notifications:{self.run_id}"
        )
        if self.settings.periodic_seconds:
            self._periodic_task = asyncio.create_task(
                self._periodic(), name=f"periodic-notifications:{self.run_id}"
            )
        self._delivery_wake.set()

    async def close(self) -> None:
        tasks = (
            self._listener_task,
            self._periodic_task,
            self._delivery_task,
        )
        for task in tasks:
            if task and not task.done():
                task.cancel()
        await asyncio.gather(
            *[task for task in tasks if task],
            return_exceptions=True,
        )
        await self._client.aclose()

    async def send(
        self,
        *,
        title: str,
        message: str,
        data: dict[str, Any] | None = None,
        dedup_key: str | None = None,
    ) -> dict[str, Any] | None:
        """Durably enqueue a notification and wake the delivery worker.

        Delivery is intentionally asynchronous. A successful deduplication key is not
        sent again; a failed key can be explicitly enqueued again to rearm its bounded
        retry sequence.
        """

        destinations = self._destinations()
        if not destinations:
            return None
        intent = self.store.enqueue_notification(
            run_id=self.run_id,
            title=title,
            message=message,
            data=data or {},
            dedup_key=dedup_key,
            destinations=destinations,
        )
        self._delivery_wake.set()
        return intent

    def _destinations(self) -> list[tuple[str, str]]:
        destinations = [("webhook", url) for url in self.settings.webhook_urls]
        if self.settings.ntfy_base_url and self.settings.ntfy_topic:
            destinations.append(
                (
                    "ntfy",
                    f"{self.settings.ntfy_base_url.rstrip('/')}/{self.settings.ntfy_topic}",
                )
            )
        return destinations

    async def _deliver(self) -> None:
        idle_seconds = min(self.settings.retry_initial_seconds, 1.0)
        while True:
            self._delivery_wake.clear()
            claimed: list[dict[str, Any]] = []
            try:
                claimed = self.store.claim_due_notification_deliveries(self.run_id)
                if claimed:
                    await asyncio.gather(
                        *(self._attempt_delivery(item) for item in claimed),
                        return_exceptions=True,
                    )
                await self._report_intent_transitions()
            except asyncio.CancelledError:
                raise
            except Exception as error:
                await self._publish_safely(
                    "notification.worker_error",
                    {"error": f"{type(error).__name__}: {error}"},
                )
            if claimed:
                continue
            try:
                await asyncio.wait_for(self._delivery_wake.wait(), timeout=idle_seconds)
            except TimeoutError:
                pass

    async def _attempt_delivery(self, delivery: dict[str, Any]) -> None:
        error_message: str | None = None
        idempotency_headers = {
            "Idempotency-Key": str(delivery["delivery_id"]),
            "X-Runwatch-Intent-ID": str(delivery["intent_id"]),
        }
        try:
            if delivery["kind"] == "webhook":
                response = await self._client.post(
                    delivery["destination"],
                    json={
                        "title": delivery["title"],
                        "message": delivery["message"],
                        "data": delivery["data"],
                    },
                    headers=idempotency_headers,
                )
            elif delivery["kind"] == "ntfy":
                response = await self._client.post(
                    delivery["destination"],
                    content=str(delivery["message"]).encode("utf-8"),
                    headers={
                        "Title": str(delivery["title"]),
                        "Tags": "computer",
                        **idempotency_headers,
                    },
                )
            else:
                raise ValueError(
                    f"Unsupported notification destination kind {delivery['kind']!r}"
                )
            response.raise_for_status()
        except asyncio.CancelledError:
            raise
        except Exception as error:
            error_message = f"{type(error).__name__}: {error}"

        attempt = int(delivery["attempt_count"])
        retry_delay = min(
            self.settings.retry_initial_seconds * (2 ** max(attempt - 1, 0)),
            self.settings.retry_max_seconds,
        )
        self.store.finish_notification_delivery(
            delivery["delivery_id"],
            succeeded=error_message is None,
            max_attempts=self.settings.max_delivery_attempts,
            retry_delay_seconds=retry_delay,
            error=error_message,
        )
        if error_message is not None:
            self._delivery_wake.set()

    async def _report_intent_transitions(self) -> None:
        for intent in self.store.unreported_notification_intents(self.run_id):
            status = intent["status"]
            deliveries = self.store.notification_deliveries(intent["intent_id"])
            errors = [
                f"{item['kind']}: {item['last_error']}"
                for item in deliveries
                if item["last_error"]
            ]
            payload = {
                "intent_id": intent["intent_id"],
                "title": intent["title"],
                "message": intent["message"],
                "errors": errors,
                "deliveries": [
                    {
                        "kind": item["kind"],
                        "status": item["status"],
                        "attempt_count": item["attempt_count"],
                    }
                    for item in deliveries
                ],
            }
            if status == "succeeded":
                event_type = "notification.sent"
            elif status == "failed":
                event_type = "notification.failed"
            else:
                event_type = "notification.partial_failure"
            await self.bus.publish(event_type, payload)
            self.store.mark_notification_reported(intent["intent_id"], status)

    async def _listen(self) -> None:
        async with self.bus.subscribe() as queue:
            while True:
                try:
                    if await self._drain_persisted_events():
                        continue
                    await queue.get()
                except asyncio.CancelledError:
                    raise
                except Exception as error:
                    await self._publish_safely(
                        "notification.listener_error",
                        {"error": f"{type(error).__name__}: {error}"},
                    )
                    await asyncio.sleep(0.25)

    async def _drain_persisted_events(self) -> bool:
        cursor, repaired = self.store.normalize_notification_event_cursor(self.run_id)
        if repaired:
            await self._publish_safely(
                "notification.cursor_repaired",
                {
                    "cursor": cursor,
                    "reason": "Cursor exceeded the durable event high-water mark",
                },
            )
        events = self.store.events_after(self.run_id, cursor, limit=256)
        for event in events:
            if not await self._consume_event(event):
                await asyncio.sleep(0.25)
                return True
            self.store.advance_notification_event_cursor(self.run_id, int(event["seq"]))
        return bool(events)

    async def _consume_event(self, event: dict[str, Any]) -> bool:
        try:
            await self._handle_event(event)
        except asyncio.CancelledError:
            raise
        except (IndexError, KeyError, TypeError, ValueError) as error:
            await self._publish_event_error("notification.event_rejected", event, error)
        except Exception as error:
            await self._publish_event_error("notification.worker_error", event, error)
            return False
        return True

    async def _publish_event_error(
        self,
        event_type: str,
        event: dict[str, Any],
        error: Exception,
    ) -> None:
        await self._publish_safely(
            event_type,
            {
                "event_type": str(event.get("type", "<missing>")),
                "event_seq": event.get("seq"),
                "error": f"{type(error).__name__}: {error}",
            },
        )

    async def _handle_event(self, event: dict[str, Any]) -> None:
        event_type = event["type"]
        payload_value = event["payload"]
        if not isinstance(event_type, str) or not isinstance(payload_value, dict):
            raise TypeError(
                "Notification events require a string type and object payload"
            )
        payload = cast(dict[str, Any], payload_value)
        if event_type == "cell.failed":
            kernel_epoch = _event_kernel_epoch(payload)
            await self.send(
                title="Runwatch: notebook cell failed",
                message=(
                    f"Cell {payload['cell_index'] + 1} failed: "
                    f"{payload.get('error_name')}: {payload.get('error_value')}"
                ),
                data=payload,
                dedup_key=(
                    f"cell-failed:{kernel_epoch}:"
                    f"{payload['cell_index']}:{payload['attempt']}"
                ),
            )
        elif event_type == "resource.observed" and payload.get("status") == "failed":
            await self.send(
                title="Runwatch: external resource failed",
                message=str(payload.get("message") or payload["internal_id"]),
                data=payload,
                dedup_key=f"resource-failed:{payload['internal_id']}",
            )
        elif event_type == "run.succeeded":
            kernel_epoch = _event_kernel_epoch(payload)
            await self.send(
                title="Runwatch: run completed",
                message="Notebook and blocking resources completed successfully.",
                data=payload,
                dedup_key=f"run-succeeded:{kernel_epoch}",
            )
        elif event_type in {
            "run.failed_external",
            "run.runner_error",
            "run.external_timeout",
        }:
            kernel_epoch = _event_kernel_epoch(payload)
            await self.send(
                title="Runwatch: run failed",
                message=str(payload),
                data=payload,
                dedup_key=f"run-failed:{event_type}:{kernel_epoch}",
            )
        elif event_type == "run.cancelled":
            kernel_epoch = _event_kernel_epoch(payload)
            await self.send(
                title="Runwatch: run cancelled",
                message="The notebook run was cancelled.",
                data=payload,
                dedup_key=f"run-cancelled:{kernel_epoch}",
            )

    async def _periodic(self) -> None:
        assert self.settings.periodic_seconds is not None
        while True:
            await asyncio.sleep(self.settings.periodic_seconds)
            try:
                snapshot = self.store.snapshot(self.run_id)
                run = snapshot["run"]
                if run["status"] in {"succeeded", "failed", "cancelled"}:
                    return
                resources = snapshot["resources"]
                active = sum(not item["terminal"] for item in resources)
                await self.send(
                    title="Runwatch status",
                    message=(
                        f"{run['name']}: {run['status']}; "
                        f"cell {run.get('current_cell_index')}; "
                        f"{active} active resource(s)."
                    ),
                    data={"run": run, "active_resources": active},
                )
            except asyncio.CancelledError:
                raise
            except Exception as error:
                await self._publish_safely(
                    "notification.periodic_error",
                    {"error": f"{type(error).__name__}: {error}"},
                )

    async def _publish_safely(self, event_type: str, payload: dict[str, Any]) -> None:
        try:
            await self.bus.publish(event_type, payload)
        except asyncio.CancelledError:
            raise
        except Exception:
            # Notification diagnostics must never take down run supervision.
            return
