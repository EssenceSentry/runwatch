# pyright: reportMissingTypeArgument=false, reportPrivateUsage=false, reportUnknownArgumentType=false, reportUnknownLambdaType=false, reportUnknownParameterType=false, reportUnknownVariableType=false
from __future__ import annotations

import asyncio
import sqlite3
from collections.abc import Callable, Coroutine
from pathlib import Path
from typing import Any

import httpx
import pytest

from runwatch.events import EventBus
from runwatch.models import NotificationSettings
from runwatch.notifications import NotificationManager
from runwatch.storage import RunStore


def notification_store(root: Path) -> RunStore:
    source = root / "source.ipynb"
    source.write_text("{}", encoding="utf-8")
    store = RunStore(root / "state.sqlite3")
    store.initialize_run(
        run_id="run",
        name="demo",
        notebook_path=source,
        source_path=source,
        output_path=root / "out.ipynb",
        working_dir=root,
        run_dir=root,
        source_digest="digest",
    )
    return store


async def wait_until(predicate: Callable[[], bool], *, timeout: float = 2.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not predicate():
        if loop.time() >= deadline:
            raise AssertionError("Condition was not met before the timeout")
        await asyncio.sleep(0.005)


NotificationHandler = (
    Callable[[httpx.Request], httpx.Response]
    | Callable[[httpx.Request], Coroutine[None, None, httpx.Response]]
)


def replace_client(manager: NotificationManager, handler: NotificationHandler) -> None:
    manager._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))


def notification_intent_has_status(
    store: RunStore, intent_id: str, status: str
) -> bool:
    intent = store.notification_intent(intent_id)
    return intent is not None and intent["status"] == status


def sent_notification_events(store: RunStore) -> list[dict]:
    return [
        event
        for event in store.recent_events("run", limit=1_000)
        if event["type"] == "notification.sent"
    ]


def cell_failure(*, kernel_epoch: int, cell_index: int = 1, attempt: int = 1) -> dict:
    return {
        "kernel_epoch": kernel_epoch,
        "cell_index": cell_index,
        "attempt": attempt,
        "error_name": "ValueError",
        "error_value": "bad input",
    }


@pytest.mark.asyncio
async def test_backlogged_notifiable_event_survives_tiny_retention_budgets(
    tmp_path: Path,
) -> None:
    root = tmp_path / "bounded-run"
    root.mkdir()
    source = root / "source.ipynb"
    source.write_text("{}", encoding="utf-8")
    store = RunStore(
        root / "state.sqlite3",
        max_events_per_run=3,
        max_event_bytes_per_run=96,
    )
    store.initialize_run(
        run_id="run",
        name="demo",
        notebook_path=source,
        source_path=source,
        output_path=root / "out.ipynb",
        working_dir=root,
        run_dir=root,
        source_digest="digest",
    )
    bus = EventBus(store, "run")
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(204)

    manager = NotificationManager(
        settings=NotificationSettings(webhook_urls=["https://hooks.example/backlog"]),
        store=store,
        bus=bus,
        run_id="run",
    )
    await manager._client.aclose()
    replace_client(manager, handler)
    failure = await bus.publish("cell.failed", cell_failure(kernel_epoch=4))
    for index in range(8):
        await bus.publish("probe.noise", {"index": index, "blob": "x" * 40})

    backlog = store.events_after("run", 0, limit=100)
    high_water = int(backlog[-1]["seq"])
    assert len(backlog) == 9
    assert backlog[0]["seq"] == failure["seq"]
    assert backlog[0]["type"] == "cell.failed"

    await manager.start()
    await wait_until(lambda: len(requests) == 1)
    drained = await manager.drain(1)

    assert drained.complete
    assert store.notification_event_cursor("run") >= high_water
    assert len(store.recent_events("run", limit=100)) <= 3
    await manager.close()
    store.close()


@pytest.mark.asyncio
async def test_cell_failure_delivers_webhook_and_ntfy_without_pairing_url(
    tmp_path: Path,
) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(204)

    store = notification_store(tmp_path)
    bus = EventBus(store, "run")
    manager = NotificationManager(
        settings=NotificationSettings(
            webhook_urls=["https://hooks.example/runwatch"],
            ntfy_base_url="https://ntfy.example",
            ntfy_topic="runs",
        ),
        store=store,
        bus=bus,
        run_id="run",
    )
    await manager._client.aclose()
    replace_client(manager, handler)
    await manager.start()
    await asyncio.sleep(0)
    await bus.publish(
        "cell.failed",
        {
            "cell_index": 1,
            "attempt": 2,
            "error_name": "ValueError",
            "error_value": "bad input",
        },
    )
    await wait_until(lambda: len(requests) == 2)

    assert [str(request.url) for request in requests] == [
        "https://hooks.example/runwatch",
        "https://ntfy.example/runs",
    ]
    bodies = b" ".join(request.content for request in requests)
    assert b"ValueError" in bodies
    assert b"token=" not in bodies
    await manager.close()
    store.close()


@pytest.mark.asyncio
async def test_failed_delivery_retries_then_deduplicates_only_after_success(
    tmp_path: Path,
) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if len(requests) < 3:
            return httpx.Response(503, text="try later")
        return httpx.Response(204)

    store = notification_store(tmp_path)
    manager = NotificationManager(
        settings=NotificationSettings(
            webhook_urls=["https://hooks.example/retry"],
            max_delivery_attempts=3,
            retry_initial_seconds=0.01,
            retry_max_seconds=0.02,
        ),
        store=store,
        bus=EventBus(store, "run"),
        run_id="run",
    )
    await manager._client.aclose()
    replace_client(manager, handler)
    await manager.start()

    intent = await manager.send(
        title="Runwatch", message="recoverable", dedup_key="retry-me"
    )
    assert intent is not None
    drained = await manager.drain(1)
    assert drained.complete
    assert len(requests) == 3
    delivery = store.notification_deliveries(intent["intent_id"])[0]
    assert delivery["attempt_count"] == 3

    duplicate = await manager.send(
        title="Runwatch", message="recoverable", dedup_key="retry-me"
    )
    assert duplicate is not None
    assert duplicate["created"] is False
    await asyncio.sleep(0.04)
    assert len(requests) == 3
    await manager.close()
    store.close()


@pytest.mark.asyncio
async def test_delivery_failures_are_bounded_and_persisted(tmp_path: Path) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(500, text="unavailable")

    store = notification_store(tmp_path)
    manager = NotificationManager(
        settings=NotificationSettings(
            webhook_urls=["https://hooks.example/fail"],
            max_delivery_attempts=2,
            retry_initial_seconds=0.01,
            retry_max_seconds=0.01,
        ),
        store=store,
        bus=EventBus(store, "run"),
        run_id="run",
    )
    await manager._client.aclose()
    replace_client(manager, handler)
    await manager.start()
    intent = await manager.send(title="Runwatch", message="still running")
    assert intent is not None
    await wait_until(
        lambda: notification_intent_has_status(store, intent["intent_id"], "failed")
    )

    assert len(requests) == 2
    events = store.recent_events("run")
    event_types = [event["type"] for event in events]
    assert "notification.partial_failure" in event_types
    assert "notification.failed" in event_types
    failed = next(
        event for event in reversed(events) if event["type"] == "notification.failed"
    )
    assert "500" in failed["payload"]["errors"][0]
    await manager.close()
    store.close()


@pytest.mark.asyncio
async def test_malformed_event_does_not_kill_listener(tmp_path: Path) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(204)

    store = notification_store(tmp_path)
    bus = EventBus(store, "run")
    manager = NotificationManager(
        settings=NotificationSettings(webhook_urls=["https://hooks.example/runwatch"]),
        store=store,
        bus=bus,
        run_id="run",
    )
    await manager._client.aclose()
    replace_client(manager, handler)
    await manager.start()
    await asyncio.sleep(0)

    await bus.publish("cell.failed", {"cell_index": "invalid"})
    await wait_until(
        lambda: any(
            event["type"] == "notification.event_rejected"
            for event in store.recent_events("run")
        )
    )
    assert manager._listener_task is not None
    assert not manager._listener_task.done()

    await bus.publish("run.succeeded", {})
    await wait_until(lambda: len(requests) == 1)
    assert manager._listener_task is not None
    assert not manager._listener_task.done()
    await manager.close()
    store.close()


@pytest.mark.asyncio
async def test_slow_failing_destination_does_not_block_fast_destination(
    tmp_path: Path,
) -> None:
    slow_started = asyncio.Event()
    release_slow = asyncio.Event()
    fast_completed = asyncio.Event()

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "slow.example":
            slow_started.set()
            await release_slow.wait()
            return httpx.Response(500, text="slow failure")
        fast_completed.set()
        return httpx.Response(204)

    store = notification_store(tmp_path)
    manager = NotificationManager(
        settings=NotificationSettings(
            webhook_urls=["https://slow.example/hook", "https://fast.example/hook"],
            max_delivery_attempts=1,
        ),
        store=store,
        bus=EventBus(store, "run"),
        run_id="run",
    )
    await manager._client.aclose()
    replace_client(manager, handler)
    await manager.start()
    intent = await manager.send(title="Runwatch", message="fan out")
    assert intent is not None
    drain_task = asyncio.create_task(manager.drain(1))

    await asyncio.wait_for(slow_started.wait(), timeout=1)
    await asyncio.wait_for(fast_completed.wait(), timeout=0.2)
    assert not drain_task.done()
    deliveries = store.notification_deliveries(intent["intent_id"])
    fast = next(item for item in deliveries if "fast.example" in item["destination"])
    assert fast["status"] == "succeeded"

    release_slow.set()
    drained = await drain_task
    assert drained.complete
    assert notification_intent_has_status(store, intent["intent_id"], "failed")
    await manager.close()
    store.close()


@pytest.mark.asyncio
async def test_post_http_persistence_failure_is_requeued_and_diagnosed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(204)

    store = notification_store(tmp_path)
    manager = NotificationManager(
        settings=NotificationSettings(
            webhook_urls=["https://hooks.example/persistence-recovery"],
            max_delivery_attempts=3,
            retry_initial_seconds=0.01,
            retry_max_seconds=0.01,
        ),
        store=store,
        bus=EventBus(store, "run"),
        run_id="run",
    )
    await manager._client.aclose()
    replace_client(manager, handler)
    original_finish = store.finish_notification_delivery
    persistence_calls = 0

    def fail_first_persistence(
        delivery_id: str,
        *,
        succeeded: bool,
        max_attempts: int,
        retry_delay_seconds: float,
        error: str | None = None,
    ) -> dict[str, Any]:
        nonlocal persistence_calls
        persistence_calls += 1
        if persistence_calls == 1:
            raise sqlite3.OperationalError("simulated post-HTTP persistence failure")
        return original_finish(
            delivery_id,
            succeeded=succeeded,
            max_attempts=max_attempts,
            retry_delay_seconds=retry_delay_seconds,
            error=error,
        )

    monkeypatch.setattr(store, "finish_notification_delivery", fail_first_persistence)
    await manager.start()
    intent = await manager.send(title="Runwatch", message="persist me")
    assert intent is not None

    drained = await manager.drain(1)

    assert drained.complete
    assert len(requests) == 2
    assert (
        requests[0].headers["Idempotency-Key"] == requests[1].headers["Idempotency-Key"]
    )
    delivery = store.notification_deliveries(intent["intent_id"])[0]
    assert delivery["status"] == "succeeded"
    assert delivery["attempt_count"] == 2
    recovered = [
        event
        for event in store.recent_events("run", limit=1_000)
        if event["type"] == "notification.delivery_recovered"
    ]
    assert recovered[-1]["payload"]["outcome"] == "pending"
    assert "OperationalError" in recovered[-1]["payload"]["error"]
    await manager.close()
    store.close()


@pytest.mark.asyncio
async def test_repeated_post_http_persistence_failure_is_terminally_bounded(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(204)

    store = notification_store(tmp_path)
    manager = NotificationManager(
        settings=NotificationSettings(
            webhook_urls=["https://hooks.example/persistence-failure"],
            max_delivery_attempts=1,
            retry_initial_seconds=0.01,
            retry_max_seconds=0.01,
        ),
        store=store,
        bus=EventBus(store, "run"),
        run_id="run",
    )
    await manager._client.aclose()
    replace_client(manager, handler)

    def fail_persistence(
        delivery_id: str,
        *,
        succeeded: bool,
        max_attempts: int,
        retry_delay_seconds: float,
        error: str | None = None,
    ) -> dict[str, Any]:
        raise sqlite3.OperationalError("persistent SQLite failure")

    monkeypatch.setattr(store, "finish_notification_delivery", fail_persistence)
    await manager.start()
    intent = await manager.send(title="Runwatch", message="bounded failure")
    assert intent is not None

    drained = await manager.drain(1)

    assert drained.complete
    assert len(requests) == 1
    delivery = store.notification_deliveries(intent["intent_id"])[0]
    assert delivery["status"] == "failed"
    assert delivery["attempt_count"] == 1
    recovered = [
        event
        for event in store.recent_events("run", limit=1_000)
        if event["type"] == "notification.delivery_recovered"
    ]
    assert recovered[-1]["payload"]["outcome"] == "failed"
    await manager.close()
    store.close()


@pytest.mark.asyncio
async def test_drain_timeout_reports_and_close_restores_sending_delivery(
    tmp_path: Path,
) -> None:
    started = asyncio.Event()
    never_release = asyncio.Event()

    async def handler(request: httpx.Request) -> httpx.Response:
        started.set()
        await never_release.wait()
        return httpx.Response(204)

    store = notification_store(tmp_path)
    manager = NotificationManager(
        settings=NotificationSettings(webhook_urls=["https://slow.example/hook"]),
        store=store,
        bus=EventBus(store, "run"),
        run_id="run",
    )
    await manager._client.aclose()
    replace_client(manager, handler)
    await manager.start()
    intent = await manager.send(title="Runwatch", message="retain me")
    assert intent is not None
    await asyncio.wait_for(started.wait(), timeout=1)

    drained = await manager.drain(0.01)

    assert not drained.complete
    assert drained.nonterminal_intents == 1
    assert drained.nonterminal_deliveries == 1
    assert "remain pending" in (drained.reason or "")
    await manager.close()
    delivery = store.notification_deliveries(intent["intent_id"])[0]
    assert delivery["status"] == "pending"
    assert delivery["attempt_count"] == 0
    assert any(
        event["type"] == "notification.deliveries_recovered"
        for event in store.recent_events("run", limit=1_000)
    )
    store.close()


@pytest.mark.asyncio
async def test_interrupted_delivery_is_recovered_after_manager_restart(
    tmp_path: Path,
) -> None:
    first_started = asyncio.Event()
    never_release = asyncio.Event()
    request_count = 0
    idempotency_headers: list[tuple[str, str]] = []

    async def interrupted_handler(request: httpx.Request) -> httpx.Response:
        nonlocal request_count
        request_count += 1
        idempotency_headers.append(
            (
                request.headers["Idempotency-Key"],
                request.headers["X-Runwatch-Intent-ID"],
            )
        )
        first_started.set()
        await never_release.wait()
        return httpx.Response(204)

    store = notification_store(tmp_path)
    bus = EventBus(store, "run")
    settings = NotificationSettings(webhook_urls=["https://hooks.example/recover"])
    first = NotificationManager(settings=settings, store=store, bus=bus, run_id="run")
    await first._client.aclose()
    replace_client(first, interrupted_handler)
    await first.start()
    intent = await first.send(
        title="Runwatch", message="survive restart", dedup_key="durable"
    )
    assert intent is not None
    await asyncio.wait_for(first_started.wait(), timeout=1)
    await wait_until(
        lambda: (
            store.notification_deliveries(intent["intent_id"])[0]["status"] == "sending"
        )
    )
    await first.close()
    store.close()

    reopened = RunStore(tmp_path / "state.sqlite3")
    recovered_bus = EventBus(reopened, "run")

    def recovered_handler(request: httpx.Request) -> httpx.Response:
        nonlocal request_count
        request_count += 1
        idempotency_headers.append(
            (
                request.headers["Idempotency-Key"],
                request.headers["X-Runwatch-Intent-ID"],
            )
        )
        return httpx.Response(204)

    second = NotificationManager(
        settings=settings, store=reopened, bus=recovered_bus, run_id="run"
    )
    await second._client.aclose()
    replace_client(second, recovered_handler)
    await second.start()
    await wait_until(
        lambda: notification_intent_has_status(
            reopened, intent["intent_id"], "succeeded"
        )
    )

    assert request_count == 2
    delivery = reopened.notification_deliveries(intent["intent_id"])[0]
    assert delivery["attempt_count"] == 1
    assert idempotency_headers == [
        (delivery["delivery_id"], intent["intent_id"]),
        (delivery["delivery_id"], intent["intent_id"]),
    ]
    await second.close()
    reopened.close()


@pytest.mark.asyncio
async def test_failure_persisted_before_start_is_replayed_and_delivered_once(
    tmp_path: Path,
) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(204)

    store = notification_store(tmp_path)
    bus = EventBus(store, "run")
    failure = await bus.publish("cell.failed", cell_failure(kernel_epoch=3))
    settings = NotificationSettings(webhook_urls=["https://hooks.example/pre-start"])
    first = NotificationManager(settings=settings, store=store, bus=bus, run_id="run")
    await first._client.aclose()
    replace_client(first, handler)

    await first.start()
    await wait_until(lambda: len(requests) == 1)
    await wait_until(lambda: len(sent_notification_events(store)) == 1)
    await wait_until(
        lambda: store.notification_event_cursor("run") >= int(failure["seq"])
    )

    sent = sent_notification_events(store)
    intent = store.notification_intent(sent[0]["payload"]["intent_id"])
    assert intent is not None
    assert intent["status"] == "succeeded"
    assert len(store.notification_deliveries(intent["intent_id"])) == 1
    await first.close()

    second = NotificationManager(
        settings=settings,
        store=store,
        bus=bus,
        run_id="run",
    )
    await second._client.aclose()
    replace_client(second, handler)
    await second.start()
    await asyncio.sleep(0.05)

    assert len(requests) == 1
    assert len(sent_notification_events(store)) == 1
    await second.close()
    store.close()


@pytest.mark.asyncio
async def test_replay_after_intent_commit_before_cursor_does_not_duplicate_delivery(
    tmp_path: Path,
) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(204)

    path = tmp_path / "state.sqlite3"
    store = notification_store(tmp_path)
    bus = EventBus(store, "run")
    failure = await bus.publish("cell.failed", cell_failure(kernel_epoch=5))
    settings = NotificationSettings(webhook_urls=["https://hooks.example/crash-gap"])

    interrupted = NotificationManager(
        settings=settings,
        store=store,
        bus=bus,
        run_id="run",
    )
    await interrupted._handle_event(failure)
    assert store.notification_event_cursor("run") == 0
    await interrupted.close()
    store.close()

    reopened = RunStore(path)
    recovered_bus = EventBus(reopened, "run")
    recovered = NotificationManager(
        settings=settings,
        store=reopened,
        bus=recovered_bus,
        run_id="run",
    )
    await recovered._client.aclose()
    replace_client(recovered, handler)
    await recovered.start()

    await wait_until(lambda: len(requests) == 1)
    await wait_until(lambda: len(sent_notification_events(reopened)) == 1)
    await wait_until(
        lambda: reopened.notification_event_cursor("run") >= int(failure["seq"])
    )
    await asyncio.sleep(0.05)
    assert len(requests) == 1
    assert len(sent_notification_events(reopened)) == 1

    sent = sent_notification_events(reopened)[0]
    intent = reopened.notification_intent(sent["payload"]["intent_id"])
    assert intent is not None
    assert intent["dedup_key"] == "cell-failed:5:1:1"
    assert len(reopened.notification_deliveries(intent["intent_id"])) == 1
    await recovered.close()
    reopened.close()


@pytest.mark.asyncio
async def test_future_cursor_is_repaired_before_next_failure_is_delivered(
    tmp_path: Path,
) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(204)

    store = notification_store(tmp_path)
    assert store.advance_notification_event_cursor("run", 10_000)
    bus = EventBus(store, "run")
    manager = NotificationManager(
        settings=NotificationSettings(
            webhook_urls=["https://hooks.example/cursor-repair"]
        ),
        store=store,
        bus=bus,
        run_id="run",
    )
    await manager._client.aclose()
    replace_client(manager, handler)
    await manager.start()

    await wait_until(
        lambda: any(
            event["type"] == "notification.cursor_repaired"
            for event in store.recent_events("run")
        )
    )
    repaired = next(
        event
        for event in store.recent_events("run")
        if event["type"] == "notification.cursor_repaired"
    )
    assert repaired["payload"]["cursor"] == 0

    failure = await bus.publish("cell.failed", cell_failure(kernel_epoch=6))
    await wait_until(lambda: len(requests) == 1)
    await wait_until(lambda: len(sent_notification_events(store)) == 1)
    await wait_until(
        lambda: store.notification_event_cursor("run") >= int(failure["seq"])
    )
    assert manager._listener_task is not None
    assert not manager._listener_task.done()
    await manager.close()
    store.close()


@pytest.mark.asyncio
async def test_listener_recovers_after_transient_cursor_store_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(204)

    store = notification_store(tmp_path)
    bus = EventBus(store, "run")
    manager = NotificationManager(
        settings=NotificationSettings(
            webhook_urls=["https://hooks.example/transient-cursor"]
        ),
        store=store,
        bus=bus,
        run_id="run",
    )
    await manager._client.aclose()
    replace_client(manager, handler)

    original_normalize = store.normalize_notification_event_cursor
    normalize_attempts = 0

    def transient_normalize(run_id: str) -> tuple[int, bool]:
        nonlocal normalize_attempts
        normalize_attempts += 1
        if normalize_attempts == 1:
            raise sqlite3.OperationalError("injected transient cursor failure")
        return original_normalize(run_id)

    monkeypatch.setattr(
        store,
        "normalize_notification_event_cursor",
        transient_normalize,
    )
    await manager.start()
    await wait_until(
        lambda: any(
            event["type"] == "notification.listener_error"
            for event in store.recent_events("run")
        )
    )
    assert manager._listener_task is not None
    assert not manager._listener_task.done()

    await bus.publish("cell.failed", cell_failure(kernel_epoch=9))
    await wait_until(lambda: len(requests) == 1)
    await wait_until(lambda: normalize_attempts >= 2)
    assert manager._listener_task is not None
    assert not manager._listener_task.done()
    await manager.close()
    store.close()


@pytest.mark.asyncio
async def test_persisted_replay_survives_subscriber_queue_overflow(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(204)

    store = notification_store(tmp_path)
    bus = EventBus(store, "run", subscriber_queue_size=1)
    manager = NotificationManager(
        settings=NotificationSettings(webhook_urls=["https://hooks.example/overflow"]),
        store=store,
        bus=bus,
        run_id="run",
    )
    await manager._client.aclose()
    replace_client(manager, handler)

    listener_blocked = asyncio.Event()
    release_listener = asyncio.Event()
    original_handle_event = manager._handle_event
    blocked_once = False

    async def lagging_handle_event(event: dict) -> None:
        nonlocal blocked_once
        if event["type"] == "cell.failed" and not blocked_once:
            blocked_once = True
            listener_blocked.set()
            await release_listener.wait()
        await original_handle_event(event)

    monkeypatch.setattr(manager, "_handle_event", lagging_handle_event)
    await manager.start()
    await wait_until(lambda: len(bus._subscribers) == 1)

    failure_count = 25
    await bus.publish(
        "cell.failed", cell_failure(kernel_epoch=4, cell_index=0, attempt=1)
    )
    await asyncio.wait_for(listener_blocked.wait(), timeout=1)
    for attempt in range(2, failure_count + 1):
        await bus.publish(
            "cell.failed",
            cell_failure(kernel_epoch=4, cell_index=0, attempt=attempt),
        )

    subscriber = next(iter(bus._subscribers))
    assert subscriber.qsize() == subscriber.maxsize == 1
    release_listener.set()

    await wait_until(lambda: len(requests) == failure_count)
    await wait_until(lambda: len(sent_notification_events(store)) == failure_count)
    sent = sent_notification_events(store)
    intent_ids = {event["payload"]["intent_id"] for event in sent}
    assert len(intent_ids) == failure_count
    for intent_id in intent_ids:
        intent = store.notification_intent(intent_id)
        assert intent is not None
        assert intent["status"] == "succeeded"
        deliveries = store.notification_deliveries(intent_id)
        assert len(deliveries) == 1
        assert deliveries[0]["status"] == "succeeded"

    await manager.close()
    store.close()


@pytest.mark.asyncio
async def test_cell_failure_deduplication_includes_kernel_epoch(tmp_path: Path) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(204)

    store = notification_store(tmp_path)
    bus = EventBus(store, "run")
    manager = NotificationManager(
        settings=NotificationSettings(webhook_urls=["https://hooks.example/epochs"]),
        store=store,
        bus=bus,
        run_id="run",
    )
    await manager._client.aclose()
    replace_client(manager, handler)
    await manager.start()
    await wait_until(lambda: len(bus._subscribers) == 1)

    payload = cell_failure(kernel_epoch=7, cell_index=2, attempt=3)
    await bus.publish("cell.failed", payload)
    await wait_until(lambda: len(requests) == 1)
    await wait_until(lambda: len(sent_notification_events(store)) == 1)

    duplicate = await bus.publish("cell.failed", payload)
    await wait_until(
        lambda: store.notification_event_cursor("run") >= int(duplicate["seq"])
    )
    await asyncio.sleep(0.03)
    assert len(requests) == 1

    await bus.publish(
        "cell.failed",
        cell_failure(kernel_epoch=8, cell_index=2, attempt=3),
    )
    await wait_until(lambda: len(requests) == 2)
    await wait_until(lambda: len(sent_notification_events(store)) == 2)

    sent = sent_notification_events(store)
    intent_ids = {event["payload"]["intent_id"] for event in sent}
    intents = [store.notification_intent(intent_id) for intent_id in intent_ids]
    assert all(intent is not None for intent in intents)
    assert {intent["dedup_key"] for intent in intents if intent is not None} == {
        "cell-failed:7:2:3",
        "cell-failed:8:2:3",
    }
    assert all(
        len(store.notification_deliveries(intent_id)) == 1 for intent_id in intent_ids
    )

    await manager.close()
    store.close()


@pytest.mark.asyncio
async def test_terminal_notification_deduplication_includes_kernel_epoch(
    tmp_path: Path,
) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(204)

    store = notification_store(tmp_path)
    bus = EventBus(store, "run")
    manager = NotificationManager(
        settings=NotificationSettings(webhook_urls=["https://hooks.example/epochs"]),
        store=store,
        bus=bus,
        run_id="run",
    )
    await manager._client.aclose()
    replace_client(manager, handler)
    await manager.start()
    await wait_until(lambda: len(bus._subscribers) == 1)

    event_types = ("run.succeeded", "run.cancelled", "run.runner_error")
    for kernel_epoch in (11, 12):
        for event_type in event_types:
            await bus.publish(event_type, {"kernel_epoch": kernel_epoch})
    duplicate = await bus.publish("run.succeeded", {"kernel_epoch": 12})

    await wait_until(lambda: len(requests) == 6)
    await wait_until(
        lambda: store.notification_event_cursor("run") >= int(duplicate["seq"])
    )
    assert len(requests) == 6

    intent_ids = {
        event["payload"]["intent_id"] for event in sent_notification_events(store)
    }
    intents = [store.notification_intent(intent_id) for intent_id in intent_ids]
    assert {intent["dedup_key"] for intent in intents if intent is not None} == {
        "run-succeeded:11",
        "run-succeeded:12",
        "run-cancelled:11",
        "run-cancelled:12",
        "run-failed:run.runner_error:11",
        "run-failed:run.runner_error:12",
    }

    await manager.close()
    store.close()
