from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from typing import Any, cast

import httpx

from .events import EventBus
from .models import NotificationSettings
from .notification_config import (
    compatible_notification_settings,
    notification_destinations,
)
from .notification_presentation import (
    NotificationDeliveryError,
    NotificationEnvelope,
    NotificationPresenter,
    PresentedNotification,
    safe_delivery_error,
)
from .storage import RunStore


@dataclass(frozen=True)
class NotificationDrainResult:
    """Result of waiting for durable notification routing and delivery."""

    complete: bool
    nonterminal_intents: int = 0
    nonterminal_deliveries: int = 0
    routing_pending: bool = False
    reason: str | None = None


@dataclass(frozen=True)
class _RoutingOutcome:
    consumed: bool
    cursor_advanced: bool = False
    retry_delay_seconds: float = 0.0


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
        self._reported_rejection_sequence: int | None = None
        self._client = httpx.AsyncClient(
            timeout=settings.request_timeout_seconds, follow_redirects=False
        )
        self._presenter = NotificationPresenter(
            store=store, run_id=run_id, settings=settings
        )
        if self._destinations():
            self.store.require_notification_event_routing(self.run_id)

    async def start(self) -> None:
        if self._delivery_task is not None:
            return
        destinations = self._destinations()
        persisted = self.store.notification_configuration(self.run_id)
        current_destinations = (
            notification_destinations(compatible_notification_settings(persisted))
            if persisted is not None
            else None
        )
        self.store.reconcile_notification_configuration(
            self.run_id,
            current_destinations=current_destinations,
            desired_destinations=destinations,
            desired_configuration=self.settings.model_dump(mode="json"),
        )
        if not destinations:
            return
        self._recover_interrupted_deliveries()
        for notification in self._presenter.reconcile_state():
            await self.send(notification, rearm_failed=False)
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
        tasks = [
            self._listener_task,
            self._periodic_task,
            self._delivery_task,
        ]
        for task in tasks:
            if task and not task.done():
                task.cancel()
        existing = [task for task in tasks if task]
        results = await asyncio.gather(*existing, return_exceptions=True)
        recovered = self._recover_interrupted_deliveries()
        for task, result in zip(existing, results, strict=True):
            if isinstance(result, BaseException) and not isinstance(
                result, asyncio.CancelledError
            ):
                await self._publish_safely(
                    "notification.shutdown_error",
                    {
                        "task": task.get_name(),
                        "error": _error_payload(result),
                    },
                )
        if recovered:
            await self._publish_safely(
                "notification.deliveries_recovered",
                {
                    "count": recovered,
                    "reason": "Notification workers stopped before delivery completed",
                },
            )
        await self._client.aclose()

    async def drain(self, timeout_seconds: float) -> NotificationDrainResult:
        """Wait boundedly for persisted events and outbox deliveries to become terminal."""

        if not self._destinations():
            return NotificationDrainResult(complete=True)
        if self._listener_task is None or self._delivery_task is None:
            return NotificationDrainResult(
                complete=False,
                routing_pending=True,
                reason="Notification workers are not running",
            )
        latest = self.store.recent_events(self.run_id, limit=1)
        target_sequence = int(latest[-1]["seq"]) if latest else 0
        loop = asyncio.get_running_loop()
        deadline = loop.time() + max(timeout_seconds, 0.0)
        while True:
            routing_pending, state = self._drain_state(target_sequence)
            if self._drain_is_complete(routing_pending, state):
                return NotificationDrainResult(complete=True)
            if loop.time() >= deadline:
                return self._incomplete_drain_result(
                    target_sequence, routing_pending, state
                )
            self._delivery_wake.set()
            await asyncio.sleep(min(0.05, max(deadline - loop.time(), 0.0)))

    def _drain_state(self, target_sequence: int) -> tuple[bool, dict[str, int]]:
        cursor = self.store.notification_event_cursor(self.run_id)
        return (
            cursor < target_sequence,
            self.store.notification_outbox_state(self.run_id),
        )

    @staticmethod
    def _drain_is_complete(routing_pending: bool, state: dict[str, int]) -> bool:
        return (
            not routing_pending
            and state["nonterminal_intents"] == 0
            and state["nonterminal_deliveries"] == 0
        )

    @staticmethod
    def _incomplete_drain_result(
        target_sequence: int,
        routing_pending: bool,
        state: dict[str, int],
    ) -> NotificationDrainResult:
        reasons: list[str] = []
        if routing_pending:
            reasons.append(
                f"notification routing has not consumed event {target_sequence}"
            )
        if state["nonterminal_deliveries"]:
            reasons.append(
                f"{state['nonterminal_deliveries']} delivery attempt(s) remain pending"
            )
        if state["nonterminal_intents"] and not state["nonterminal_deliveries"]:
            reasons.append(
                f"{state['nonterminal_intents']} notification intent(s) remain "
                "nonterminal"
            )
        return NotificationDrainResult(
            complete=False,
            nonterminal_intents=state["nonterminal_intents"],
            nonterminal_deliveries=state["nonterminal_deliveries"],
            routing_pending=routing_pending,
            reason="; ".join(reasons) or "notification drain did not complete",
        )

    async def send(
        self,
        notification: PresentedNotification,
        *,
        rearm_failed: bool = True,
    ) -> dict[str, Any] | None:
        """Durably enqueue a notification and wake the delivery worker.

        Delivery is intentionally asynchronous. A successful deduplication key is not
        sent again; a failed key can be explicitly enqueued again to rearm its bounded
        retry sequence.
        """

        destinations = self._destinations()
        if notification.destination_kinds is not None:
            allowed = set(notification.destination_kinds)
            destinations = [item for item in destinations if item[0] in allowed]
        if not destinations:
            return None
        payload = notification.envelope.webhook_payload()
        if len(json.dumps(payload, ensure_ascii=False).encode("utf-8")) > (
            self.settings.max_payload_bytes
        ):
            raise ValueError("Notification presentation exceeds max_payload_bytes")
        if notification.rolling:
            if notification.dedup_key is None:
                raise ValueError("A rolling notification requires a deduplication key")
            intent = self.store.enqueue_rolling_notification(
                run_id=self.run_id,
                title=notification.envelope.title,
                message=notification.envelope.message,
                data=payload["data"],
                dedup_key=notification.dedup_key,
                destinations=destinations,
            )
        else:
            intent = self.store.enqueue_notification(
                run_id=self.run_id,
                title=notification.envelope.title,
                message=notification.envelope.message,
                data=payload["data"],
                dedup_key=notification.dedup_key,
                destinations=destinations,
                rearm_failed=rearm_failed,
            )
        self._delivery_wake.set()
        return intent

    def _destinations(self) -> list[tuple[str, str]]:
        return notification_destinations(self.settings)

    async def notify_dashboard_link_changed(self, click_url: str) -> bool:
        """Send the rotated pairing URL to the explicitly configured ntfy topic."""

        destination = next(
            (value for kind, value in self._destinations() if kind == "ntfy"),
            None,
        )
        if destination is None:
            return False
        try:
            request = self._client.build_request(
                "POST",
                destination,
                content=b"Runwatch replaced the Cloudflare dashboard link.",
                headers={
                    "Title": "Runwatch: Cloudflare link changed",
                    "Tags": "link,computer",
                    "Click": click_url,
                },
            )
            response = await self._client.send(
                request, stream=True, follow_redirects=False
            )
            try:
                response.raise_for_status()
            finally:
                await response.aclose()
        except asyncio.CancelledError:
            raise
        except Exception as error:
            await self._publish_safely(
                "notification.dashboard_link_failed",
                {"error": _error_payload(error)},
            )
            return False
        await self._publish_safely("notification.dashboard_link_sent", {})
        return True

    async def _deliver(self) -> None:
        idle_seconds = min(self.settings.retry_initial_seconds, 1.0)
        while True:
            self._delivery_wake.clear()
            claimed: list[dict[str, Any]] = []
            try:
                claimed = self.store.claim_due_notification_deliveries(self.run_id)
                await self._deliver_claimed(claimed)
                await self._report_intent_transitions()
            except asyncio.CancelledError:
                raise
            except Exception as error:
                await self._publish_safely(
                    "notification.worker_error",
                    {"error": _error_payload(error)},
                )
            if claimed:
                continue
            try:
                await asyncio.wait_for(self._delivery_wake.wait(), timeout=idle_seconds)
            except TimeoutError:
                pass

    async def _deliver_claimed(self, claimed: list[dict[str, Any]]) -> None:
        if not claimed:
            return
        try:
            results = await asyncio.gather(
                *(self._attempt_delivery(item) for item in claimed),
                return_exceptions=True,
            )
        except asyncio.CancelledError:
            await self._recover_cancelled_deliveries()
            raise
        for delivery, result in zip(claimed, results, strict=True):
            if isinstance(result, BaseException):
                await self._recover_delivery_task_error(delivery, result)

    async def _recover_cancelled_deliveries(self) -> None:
        recovered = self._recover_interrupted_deliveries()
        if not recovered:
            return
        await self._publish_safely(
            "notification.deliveries_recovered",
            {
                "count": recovered,
                "reason": (
                    "Notification delivery worker stopped before delivery completed"
                ),
            },
        )

    async def _recover_delivery_task_error(
        self, delivery: dict[str, Any], error: BaseException
    ) -> None:
        delivery_error = safe_delivery_error(error)
        error_message = delivery_error.persisted()
        attempt = int(delivery["attempt_count"])
        retry_delay = min(
            self.settings.retry_initial_seconds * (2 ** max(attempt - 1, 0)),
            self.settings.retry_max_seconds,
        )
        try:
            outcome = self.store.recover_claimed_notification_delivery(
                str(delivery["delivery_id"]),
                max_attempts=self.settings.max_delivery_attempts,
                retry_delay_seconds=retry_delay,
                error=error_message,
            )
        except Exception as recovery_error:
            await self._publish_safely(
                "notification.delivery_recovery_failed",
                {
                    "delivery_id": delivery["delivery_id"],
                    "intent_id": delivery["intent_id"],
                    "error": delivery_error.model_dump(mode="json"),
                    "recovery_error": _error_payload(recovery_error),
                },
            )
            return
        await self._publish_safely(
            "notification.delivery_recovered",
            {
                "delivery_id": delivery["delivery_id"],
                "intent_id": delivery["intent_id"],
                "error": delivery_error.model_dump(mode="json"),
                "outcome": outcome or "already_terminal",
            },
        )
        if outcome == "pending":
            self._delivery_wake.set()

    def _recover_interrupted_deliveries(self) -> int:
        error = NotificationDeliveryError(
            code="internal",
            message=(
                "Notification delivery was interrupted before its outcome was persisted"
            ),
        )
        return self.store.recover_notification_deliveries(
            self.run_id,
            max_attempts=self.settings.max_delivery_attempts,
            error=error.persisted(),
        )

    async def _attempt_delivery(self, delivery: dict[str, Any]) -> None:
        delivery_error: NotificationDeliveryError | None = None
        idempotency_headers = {
            "Idempotency-Key": str(delivery["delivery_id"]),
            "X-Runwatch-Intent-ID": str(delivery["intent_id"]),
        }
        try:
            envelope = self._stored_envelope(delivery)
            if delivery["kind"] == "webhook":
                request = self._client.build_request(
                    "POST",
                    delivery["destination"],
                    json=envelope.webhook_payload(),
                    headers=idempotency_headers,
                )
            elif delivery["kind"] == "ntfy":
                request = self._client.build_request(
                    "POST",
                    delivery["destination"],
                    content=envelope.message.encode("utf-8"),
                    headers={
                        "Title": envelope.title,
                        "Tags": "computer",
                        **idempotency_headers,
                    },
                )
            else:
                raise ValueError(
                    f"Unsupported notification destination kind {delivery['kind']!r}"
                )
            response = await self._client.send(
                request, stream=True, follow_redirects=False
            )
            try:
                response.raise_for_status()
            finally:
                await response.aclose()
        except asyncio.CancelledError:
            raise
        except Exception as error:
            delivery_error = safe_delivery_error(error)

        attempt = int(delivery["attempt_count"])
        retry_delay = min(
            self.settings.retry_initial_seconds * (2 ** max(attempt - 1, 0)),
            self.settings.retry_max_seconds,
        )
        self.store.finish_notification_delivery(
            delivery["delivery_id"],
            succeeded=delivery_error is None,
            max_attempts=self.settings.max_delivery_attempts,
            retry_delay_seconds=retry_delay,
            error=delivery_error.persisted() if delivery_error else None,
        )
        if delivery_error is not None:
            self._delivery_wake.set()

    def _stored_envelope(self, delivery: dict[str, Any]) -> NotificationEnvelope:
        data = delivery.get("data")
        if not isinstance(data, dict):
            return self._presenter.legacy()
        values = {
            key: item
            for key, item in cast(dict[object, object], data).items()
            if isinstance(key, str)
        }
        schema_version = values.pop("schema_version", None)
        kind = values.pop("kind", None)
        try:
            return NotificationEnvelope.model_validate(
                {
                    "schema_version": schema_version,
                    "kind": kind,
                    "title": delivery.get("title"),
                    "message": delivery.get("message"),
                    "data": values,
                }
            )
        except (TypeError, ValueError):
            return self._presenter.legacy()

    async def _report_intent_transitions(self) -> None:
        for intent in self.store.unreported_notification_intents(self.run_id):
            status = intent["status"]
            deliveries = self.store.notification_deliveries(intent["intent_id"])
            errors = [_stored_error(item) for item in deliveries if item["last_error"]]
            payload = {
                "intent_id": intent["intent_id"],
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
        consecutive_failures = 0
        async with self.bus.subscribe() as queue:
            while True:
                try:
                    if await self._drain_persisted_events():
                        consecutive_failures = 0
                        continue
                    consecutive_failures = 0
                    await queue.get()
                except asyncio.CancelledError:
                    raise
                except Exception as error:
                    consecutive_failures += 1
                    if consecutive_failures == 1:
                        await self._publish_safely(
                            "notification.listener_error",
                            {"error": _error_payload(error)},
                        )
                    await asyncio.sleep(self._routing_retry_delay(consecutive_failures))

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
            sequence = int(event["seq"])
            outcome = await self._consume_event(event)
            if not outcome.consumed:
                await asyncio.sleep(outcome.retry_delay_seconds)
                return True
            if not outcome.cursor_advanced:
                self.store.advance_notification_event_cursor(self.run_id, sequence)
            if self._reported_rejection_sequence == sequence:
                self._reported_rejection_sequence = None
        return bool(events)

    async def _consume_event(self, event: dict[str, Any]) -> _RoutingOutcome:
        try:
            await self._handle_event(event)
        except asyncio.CancelledError:
            raise
        except (IndexError, KeyError, TypeError, ValueError) as error:
            sequence = int(event["seq"])
            if self._reported_rejection_sequence != sequence:
                await self._publish_event_error(
                    "notification.event_rejected", event, error
                )
                self._reported_rejection_sequence = sequence
        except Exception as error:
            failure = self.store.record_notification_routing_failure(
                self.run_id,
                int(event["seq"]),
                str(event.get("type", "<missing>")),
                type(error).__name__,
                self.settings.max_routing_attempts,
            )
            persisted_event = failure.get("event")
            if isinstance(persisted_event, dict):
                self.bus.fan_out_persisted(cast(dict[str, Any], persisted_event))
            attempt = int(failure["attempt"])
            if attempt == 0:
                return _RoutingOutcome(consumed=True, cursor_advanced=True)
            if bool(failure["dead_lettered"]):
                return _RoutingOutcome(consumed=True, cursor_advanced=True)
            return _RoutingOutcome(
                consumed=False,
                retry_delay_seconds=self._routing_retry_delay(attempt),
            )
        return _RoutingOutcome(consumed=True)

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
                "error": _error_payload(error),
            },
        )

    async def _handle_event(self, event: dict[str, Any]) -> None:
        notification = self._presenter.from_event(event)
        if notification is not None:
            await self.send(notification, rearm_failed=False)

    def _routing_retry_delay(self, attempt: int) -> float:
        return min(
            self.settings.retry_initial_seconds * (2 ** min(max(attempt - 1, 0), 20)),
            self.settings.retry_max_seconds,
        )

    async def _periodic(self) -> None:
        assert self.settings.periodic_seconds is not None
        while True:
            await asyncio.sleep(self.settings.periodic_seconds)
            try:
                notification = self._presenter.periodic()
                if notification is None:
                    return
                await self.send(notification)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                await self._publish_safely(
                    "notification.periodic_error",
                    {"error": _error_payload(error)},
                )

    async def _publish_safely(self, event_type: str, payload: dict[str, Any]) -> None:
        try:
            await self.bus.publish(event_type, payload)
        except asyncio.CancelledError:
            raise
        except Exception:
            # Notification diagnostics must never take down run supervision.
            return


def _error_payload(error: BaseException) -> dict[str, Any]:
    return safe_delivery_error(error).model_dump(mode="json")


def _stored_error(delivery: dict[str, Any]) -> dict[str, Any]:
    try:
        value = json.loads(str(delivery["last_error"]))
        error = NotificationDeliveryError.model_validate(value)
    except (json.JSONDecodeError, TypeError, ValueError):
        error = NotificationDeliveryError(
            code="internal", message="Legacy notification delivery failure"
        )
    return {
        "kind": str(delivery["kind"]),
        **error.model_dump(mode="json"),
    }
