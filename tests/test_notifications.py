# pyright: reportMissingTypeArgument=false, reportPrivateUsage=false, reportUnknownArgumentType=false, reportUnknownLambdaType=false, reportUnknownParameterType=false, reportUnknownVariableType=false
from __future__ import annotations

import asyncio
import json
import sqlite3
from collections.abc import Callable, Coroutine
from pathlib import Path
from typing import Any

import httpx
import pytest

from runwatch.events import EventBus
from runwatch.models import (
    NotificationSettings,
    ResourceDisposition,
    ResourceEvent,
    ResourceObservation,
    ResourceSpec,
    ResourceStatus,
    RunStatus,
    RunwatchConfig,
)
from runwatch.notification_config import notification_destinations
from runwatch.notification_presentation import (
    NotificationEnvelope,
    NotificationLegacy,
    NotificationPresenter,
    PresentedNotification,
)
from runwatch.notifications import NotificationManager
from runwatch.storage import RunStore


def notification_store(
    root: Path, *, settings: NotificationSettings | None = None
) -> RunStore:
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
        metadata=(
            {
                "config": RunwatchConfig(
                    notifications=settings or NotificationSettings()
                ).model_dump(mode="json")
            }
            if settings is not None
            else None
        ),
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


def presented_notification(
    message: str, *, dedup_key: str | None = None
) -> PresentedNotification:
    return PresentedNotification(
        envelope=NotificationEnvelope(
            kind="legacy",
            title="Runwatch",
            message=message,
            data=NotificationLegacy(),
        ),
        dedup_key=dedup_key,
    )


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
async def test_section_start_notification_is_opt_in_and_ntfy_only(
    tmp_path: Path,
) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(204)

    settings = NotificationSettings(
        webhook_urls=["https://hooks.example/runwatch"],
        ntfy_base_url="https://ntfy.example",
        ntfy_topic="runs",
        ntfy_on_section_start=True,
    )
    store = notification_store(tmp_path, settings=settings)
    bus = EventBus(store, "run")
    manager = NotificationManager(
        settings=settings,
        store=store,
        bus=bus,
        run_id="run",
    )
    await manager._client.aclose()
    replace_client(manager, handler)
    await manager.start()
    await bus.publish(
        "notebook.section_started",
        {
            "heading": "Model evaluation",
            "heading_level": 2,
            "cell_index": 7,
            "kernel_epoch": 3,
        },
    )
    await wait_until(lambda: len(requests) == 1)

    request = requests[0]
    assert str(request.url) == "https://ntfy.example/runs"
    assert request.headers["title"] == "Runwatch: starting notebook section"
    assert request.content == b"Starting section: Model evaluation"
    assert not any("hooks.example" in str(candidate.url) for candidate in requests)

    await manager.close()
    store.close()


def test_section_start_event_is_ignored_when_option_is_disabled(tmp_path: Path) -> None:
    settings = NotificationSettings(
        ntfy_base_url="https://ntfy.example",
        ntfy_topic="runs",
    )
    store = notification_store(tmp_path, settings=settings)
    presenter = NotificationPresenter(store=store, run_id="run", settings=settings)

    notification = presenter.from_event(
        {
            "type": "notebook.section_started",
            "payload": {
                "heading": "Model evaluation",
                "heading_level": 2,
                "cell_index": 7,
                "kernel_epoch": 3,
            },
        }
    )

    assert notification is None
    store.close()


@pytest.mark.asyncio
async def test_ntfy_only_section_intent_supports_same_topology_rotation(
    tmp_path: Path,
) -> None:
    old = NotificationSettings(
        webhook_urls=["https://old-hooks.example/runwatch"],
        ntfy_base_url="https://old-ntfy.example",
        ntfy_topic="runs",
        ntfy_on_section_start=True,
    )
    desired = NotificationSettings(
        webhook_urls=["https://new-hooks.example/runwatch"],
        ntfy_base_url="https://new-ntfy.example",
        ntfy_topic="runs",
        ntfy_on_section_start=True,
    )
    store = notification_store(tmp_path, settings=old)
    first = NotificationManager(
        settings=old,
        store=store,
        bus=EventBus(store, "run"),
        run_id="run",
    )
    notification = first._presenter.from_event(
        {
            "type": "notebook.section_started",
            "payload": {
                "heading": "Model evaluation",
                "heading_level": 2,
                "cell_index": 7,
                "kernel_epoch": 3,
            },
        }
    )
    assert notification is not None
    intent = await first.send(notification)
    assert intent is not None
    assert store.notification_delivery_topology("run") == (("ntfy", 1),)
    await first.close()

    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(204)

    rotated = NotificationManager(
        settings=desired,
        store=store,
        bus=EventBus(store, "run"),
        run_id="run",
    )
    await rotated._client.aclose()
    replace_client(rotated, handler)
    await rotated.start()
    await wait_until(lambda: len(requests) == 1)

    assert str(requests[0].url) == "https://new-ntfy.example/runs"
    assert not any("hooks.example" in str(request.url) for request in requests)
    await rotated.close()
    store.close()


@pytest.mark.asyncio
async def test_rotated_dashboard_link_is_ntfy_click_target_only(
    tmp_path: Path,
) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(204)

    store = notification_store(tmp_path)
    manager = NotificationManager(
        settings=NotificationSettings(
            ntfy_base_url="https://ntfy.example",
            ntfy_topic="runs",
        ),
        store=store,
        bus=EventBus(store, "run"),
        run_id="run",
    )
    await manager._client.aclose()
    replace_client(manager, handler)
    pairing_url = "https://new.trycloudflare.com/?token=secret-token"

    assert await manager.notify_dashboard_link_changed(pairing_url)
    assert len(requests) == 1
    assert str(requests[0].url) == "https://ntfy.example/runs"
    assert requests[0].headers["click"] == pairing_url
    assert requests[0].content == (b"Runwatch replaced the Cloudflare dashboard link.")
    events = store.recent_events("run", limit=100)
    assert events[-1]["type"] == "notification.dashboard_link_sent"
    assert "secret-token" not in str(events)

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
        presented_notification("recoverable", dedup_key="retry-me")
    )
    assert intent is not None
    drained = await manager.drain(1)
    assert drained.complete
    assert len(requests) == 3
    delivery = store.notification_deliveries(intent["intent_id"])[0]
    assert delivery["attempt_count"] == 3

    duplicate = await manager.send(
        presented_notification("recoverable", dedup_key="retry-me")
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
    intent = await manager.send(presented_notification("still running"))
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
    assert failed["payload"]["errors"][0]["status_code"] == 500
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
async def test_poison_event_is_dead_lettered_once_and_routing_continues(
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
            webhook_urls=["https://hooks.example/dead-letter"],
            max_routing_attempts=3,
            retry_initial_seconds=0.01,
            retry_max_seconds=0.01,
        ),
        store=store,
        bus=bus,
        run_id="run",
    )
    await manager._client.aclose()
    replace_client(manager, handler)
    original_handle_event = manager._handle_event

    async def fail_poison_event(event: dict[str, Any]) -> None:
        if event["type"] == "probe.poison":
            raise RuntimeError("deterministic routing failure")
        await original_handle_event(event)

    manager._handle_event = fail_poison_event  # type: ignore[method-assign]
    poison = await bus.publish("probe.poison", {"value": 1})
    await manager.start()

    await wait_until(
        lambda: store.notification_event_cursor("run") >= int(poison["seq"])
    )
    dead_letters = [
        event
        for event in store.recent_events("run", limit=100)
        if event["type"] == "notification.event_dead_lettered"
    ]
    assert len(dead_letters) == 1
    assert not any(
        event["type"] == "notification.worker_error"
        for event in store.recent_events("run", limit=100)
    )

    succeeded = await bus.publish("run.succeeded", {"kernel_epoch": 0})
    await wait_until(lambda: len(requests) == 1)
    await wait_until(
        lambda: store.notification_event_cursor("run") >= int(succeeded["seq"])
    )
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
    intent = await manager.send(presented_notification("fan out"))
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
    intent = await manager.send(presented_notification("persist me"))
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
    assert "OperationalError" in recovered[-1]["payload"]["error"]["message"]
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
    intent = await manager.send(presented_notification("bounded failure"))
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
    intent = await manager.send(presented_notification("retain me"))
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
    assert delivery["attempt_count"] == 1
    assert any(
        event["type"] == "notification.deliveries_recovered"
        for event in store.recent_events("run", limit=1_000)
    )
    store.close()


@pytest.mark.asyncio
async def test_close_terminalizes_ambiguous_delivery_at_retry_limit(
    tmp_path: Path,
) -> None:
    accepted = asyncio.Event()
    never_release = asyncio.Event()
    requests: list[tuple[str, str]] = []

    async def ambiguous_handler(request: httpx.Request) -> httpx.Response:
        requests.append(
            (
                request.headers["Idempotency-Key"],
                request.headers["X-Runwatch-Intent-ID"],
            )
        )
        accepted.set()
        await never_release.wait()
        return httpx.Response(204)

    settings = NotificationSettings(
        webhook_urls=["https://hooks.example/ambiguous"],
        max_delivery_attempts=1,
    )
    store = notification_store(tmp_path, settings=settings)
    first = NotificationManager(
        settings=settings, store=store, bus=EventBus(store, "run"), run_id="run"
    )
    await first._client.aclose()
    replace_client(first, ambiguous_handler)
    await first.start()
    intent = await first.send(
        presented_notification("accepted before shutdown", dedup_key="ambiguous")
    )
    assert intent is not None
    await asyncio.wait_for(accepted.wait(), timeout=1)

    await first.close()

    delivery = store.notification_deliveries(intent["intent_id"])[0]
    assert delivery["status"] == "failed"
    assert delivery["attempt_count"] == 1
    assert requests == [(delivery["delivery_id"], intent["intent_id"])]

    def unexpected_retry(request: httpx.Request) -> httpx.Response:
        requests.append(
            (
                request.headers["Idempotency-Key"],
                request.headers["X-Runwatch-Intent-ID"],
            )
        )
        return httpx.Response(204)

    second = NotificationManager(
        settings=settings, store=store, bus=EventBus(store, "run"), run_id="run"
    )
    await second._client.aclose()
    replace_client(second, unexpected_retry)
    await second.start()
    await asyncio.sleep(0.05)

    assert len(requests) == 1
    assert store.notification_deliveries(intent["intent_id"])[0]["status"] == "failed"
    await second.close()
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
        presented_notification("survive restart", dedup_key="durable")
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
    assert delivery["attempt_count"] == 2
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
async def test_routing_replay_does_not_rearm_failed_delivery_budget(
    tmp_path: Path,
) -> None:
    store = notification_store(tmp_path)
    bus = EventBus(store, "run")
    settings = NotificationSettings(
        webhook_urls=["https://hooks.example/replay-budget"],
        max_delivery_attempts=1,
    )
    manager = NotificationManager(
        settings=settings,
        store=store,
        bus=bus,
        run_id="run",
    )
    failure = await bus.publish("cell.failed", cell_failure(kernel_epoch=6))

    await manager._handle_event(failure)
    claimed = store.claim_due_notification_deliveries("run")
    assert len(claimed) == 1
    store.finish_notification_delivery(
        claimed[0]["delivery_id"],
        succeeded=False,
        max_attempts=1,
        retry_delay_seconds=0,
        error="injected terminal delivery failure",
    )
    intent_id = str(claimed[0]["intent_id"])
    assert notification_intent_has_status(store, intent_id, "failed")

    await manager._handle_event(failure)
    unchanged = store.notification_deliveries(intent_id)[0]
    assert unchanged["status"] == "failed"
    assert unchanged["attempt_count"] == 1
    assert store.claim_due_notification_deliveries("run") == []

    notification = manager._presenter.from_event(failure)
    assert notification is not None
    explicit = await manager.send(notification)
    assert explicit is not None
    assert explicit["rearmed"] is True
    rearmed = store.notification_deliveries(intent_id)[0]
    assert rearmed["status"] == "pending"
    assert rearmed["attempt_count"] == 0
    await manager.close()
    store.close()


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
async def test_persistent_cursor_error_emits_one_listener_diagnostic(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = notification_store(tmp_path)
    manager = NotificationManager(
        settings=NotificationSettings(
            webhook_urls=["https://hooks.example/persistent-cursor"],
            retry_initial_seconds=0.01,
            retry_max_seconds=0.01,
        ),
        store=store,
        bus=EventBus(store, "run"),
        run_id="run",
    )
    normalize_attempts = 0

    def fail_normalize(_run_id: str) -> tuple[int, bool]:
        nonlocal normalize_attempts
        normalize_attempts += 1
        raise sqlite3.OperationalError("injected persistent cursor failure")

    monkeypatch.setattr(store, "normalize_notification_event_cursor", fail_normalize)
    await manager.start()
    await wait_until(lambda: normalize_attempts >= 4)

    listener_errors = [
        event
        for event in store.recent_events("run", limit=100)
        if event["type"] == "notification.listener_error"
    ]
    assert len(listener_errors) == 1
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
    await wait_until(lambda: len(sent_notification_events(store)) == 6)
    assert len(requests) == 6

    intent_ids = {
        event["payload"]["intent_id"] for event in sent_notification_events(store)
    }
    intents = [store.notification_intent(intent_id) for intent_id in intent_ids]
    assert {intent["dedup_key"] for intent in intents if intent is not None} == {
        "run-terminal:succeeded:11",
        "run-terminal:succeeded:12",
        "run-terminal:cancelled:11",
        "run-terminal:cancelled:12",
        "run-terminal:failed:11",
        "run-terminal:failed:12",
    }

    await manager.close()
    store.close()


@pytest.mark.asyncio
async def test_webhook_presentations_exclude_internal_and_canary_payloads(
    tmp_path: Path,
) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(204)

    store = notification_store(tmp_path)
    bus = EventBus(store, "run")
    internal_id, _created = store.register_resource(
        run_id="run",
        event=ResourceEvent(
            resource=ResourceSpec(
                provider="aws",
                type="processing_job",
                id="resource-id",
            )
        ),
        cell_index=0,
        attempt=1,
        kernel_epoch=2,
        supports_stop=False,
    )
    store.update_resource_observation(
        "run",
        internal_id,
        ResourceObservation(
            status=ResourceStatus.FAILED,
            message="PROVIDER_SECRET",
            metrics={"credential": "METRIC_SECRET"},
            log_lines=["LOG_SECRET"],
            raw={"credential": "RAW_SECRET"},
        ),
    )
    manager = NotificationManager(
        settings=NotificationSettings(
            webhook_urls=[
                "https://one.example/hook?token=WEBHOOK_ONE_SECRET",
                "https://two.example/hook?signature=WEBHOOK_TWO_SECRET",
            ],
            ntfy_base_url="https://ntfy.example",
            ntfy_topic="NTFY_TOPIC_SECRET",
        ),
        store=store,
        bus=bus,
        run_id="run",
    )
    await manager._client.aclose()
    replace_client(manager, handler)
    await manager.start()
    await bus.publish(
        "cell.failed",
        {
            **cell_failure(kernel_epoch=2),
            "error_value": "TRACE_SECRET",
            "traceback": ["TRACEBACK_SECRET"],
            "metrics": {"credential": "METRIC_SECRET"},
        },
    )
    await wait_until(lambda: len(requests) == 6)

    serialized = b" ".join(request.content for request in requests)
    for secret in (
        b"WEBHOOK_ONE_SECRET",
        b"WEBHOOK_TWO_SECRET",
        b"NTFY_TOPIC_SECRET",
        b"TRACE_SECRET",
        b"TRACEBACK_SECRET",
        b"METRIC_SECRET",
        b"PROVIDER_SECRET",
        b"LOG_SECRET",
        b"RAW_SECRET",
    ):
        assert secret not in serialized
    await manager.close()
    store.close()


@pytest.mark.asyncio
async def test_failed_resource_presentation_omits_local_and_s3_identifiers(
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
            webhook_urls=["https://hooks.example/resource-failures"]
        ),
        store=store,
        bus=bus,
        run_id="run",
    )
    await manager._client.aclose()
    replace_client(manager, handler)
    await manager.start()

    identifiers = (
        ("local", "line_count", "/private/CLIENT_PATH_SECRET/output.jsonl"),
        ("aws", "s3_prefix", "s3://CLIENT_BUCKET_SECRET/private/prefix"),
    )
    for provider, resource_type, identifier in identifiers:
        internal_id, _created = store.register_resource(
            run_id="run",
            event=ResourceEvent(
                resource=ResourceSpec(
                    provider=provider,
                    type=resource_type,
                    id=identifier,
                    logical_key=identifier,
                )
            ),
            cell_index=0,
            attempt=1,
            kernel_epoch=1,
            supports_stop=False,
        )
        observed = store.update_resource_observation(
            "run",
            internal_id,
            ResourceObservation(
                status=ResourceStatus.FAILED,
                terminal=True,
                message=f"Failure at {identifier}",
            ),
        )
        bus.fan_out_persisted(observed)

    await wait_until(lambda: len(requests) == len(identifiers))

    payloads = [json.loads(request.content) for request in requests]
    assert all(payload["data"]["display_id"] is None for payload in payloads)
    serialized = b" ".join(request.content for request in requests)
    assert b"CLIENT_PATH_SECRET" not in serialized
    assert b"CLIENT_BUCKET_SECRET" not in serialized

    await manager.close()
    store.close()


def test_failed_resource_notifications_require_current_terminal_active_state(
    tmp_path: Path,
) -> None:
    store = notification_store(tmp_path)

    def register(resource_id: str) -> str:
        internal_id, _created = store.register_resource(
            run_id="run",
            event=ResourceEvent(
                resource=ResourceSpec(
                    provider="fake",
                    type="job",
                    id=resource_id,
                )
            ),
            cell_index=0,
            attempt=1,
            kernel_epoch=1,
            supports_stop=False,
        )
        return internal_id

    active = register("active-failed")
    superseded = register("superseded-failed")
    ignored = register("ignored-failed")
    nonterminal = register("still-running")
    recovered = register("recovered")
    for internal_id in (active, superseded, ignored):
        store.update_resource_observation(
            "run",
            internal_id,
            ResourceObservation(status=ResourceStatus.FAILED),
        )
    store.set_resource_disposition(superseded, ResourceDisposition.SUPERSEDED)
    store.set_resource_disposition(ignored, ResourceDisposition.IGNORED)
    store.update_resource_observation(
        "run",
        recovered,
        ResourceObservation(status=ResourceStatus.COMPLETED),
    )

    presenter = NotificationPresenter(
        store=store,
        run_id="run",
        settings=NotificationSettings(),
    )
    candidates = {
        internal_id: presenter.from_event(
            {
                "type": "resource.observed",
                "payload": {"internal_id": internal_id, "status": "failed"},
            }
        )
        for internal_id in (active, superseded, ignored, nonterminal, recovered)
    }
    assert candidates[active] is not None
    assert all(
        candidates[internal_id] is None
        for internal_id in (superseded, ignored, nonterminal, recovered)
    )

    reconciled = presenter.reconcile_state()
    assert [notification.dedup_key for notification in reconciled] == [
        f"resource-failed:{active}"
    ]
    store.close()


@pytest.mark.asyncio
async def test_delivery_error_omits_destination_and_response_body(
    tmp_path: Path,
) -> None:
    class UnreadBody(httpx.AsyncByteStream):
        async def __aiter__(self):
            raise AssertionError("notification response bodies must not be consumed")
            yield b"BODY_SECRET"  # pragma: no cover

        async def aclose(self) -> None:
            return None

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, stream=UnreadBody())

    store = notification_store(tmp_path)
    manager = NotificationManager(
        settings=NotificationSettings(
            webhook_urls=["https://hooks.example/fail?token=DESTINATION_SECRET"],
            max_delivery_attempts=1,
        ),
        store=store,
        bus=EventBus(store, "run"),
        run_id="run",
    )
    await manager._client.aclose()
    replace_client(manager, handler)
    await manager.start()
    intent = await manager.send(presented_notification("bounded response"))
    assert intent is not None
    await wait_until(
        lambda: notification_intent_has_status(store, intent["intent_id"], "failed")
    )

    delivery = store.notification_deliveries(intent["intent_id"])[0]
    events = store.recent_events("run", limit=1_000)
    diagnostics = json.dumps(
        {
            "last_error": delivery["last_error"],
            "notification_events": [
                event for event in events if event["type"].startswith("notification.")
            ],
        }
    )
    assert "HTTP 500" in diagnostics
    assert "DESTINATION_SECRET" not in diagnostics
    assert "BODY_SECRET" not in diagnostics
    await manager.close()
    store.close()


@pytest.mark.asyncio
async def test_notification_delivery_does_not_follow_redirects(tmp_path: Path) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            302,
            headers={"Location": "https://redirect.example/hook?token=REDIRECT_SECRET"},
        )

    store = notification_store(tmp_path)
    manager = NotificationManager(
        settings=NotificationSettings(
            webhook_urls=["https://hooks.example/original"], max_delivery_attempts=1
        ),
        store=store,
        bus=EventBus(store, "run"),
        run_id="run",
    )
    await manager._client.aclose()
    replace_client(manager, handler)
    await manager.start()
    intent = await manager.send(presented_notification("do not redirect"))
    assert intent is not None
    await wait_until(
        lambda: notification_intent_has_status(store, intent["intent_id"], "failed")
    )

    assert len(requests) == 1
    error = store.notification_deliveries(intent["intent_id"])[0]["last_error"]
    assert '"status_code":302' in error
    assert "REDIRECT_SECRET" not in error
    await manager.close()
    store.close()


@pytest.mark.asyncio
async def test_periodic_notification_is_lightweight_and_rolling(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(204)

    store = notification_store(tmp_path)
    manager = NotificationManager(
        settings=NotificationSettings(webhook_urls=["https://hooks.example/periodic"]),
        store=store,
        bus=EventBus(store, "run"),
        run_id="run",
    )
    await manager._client.aclose()
    replace_client(manager, handler)
    await manager.start()
    monkeypatch.setattr(
        store,
        "snapshot",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("periodic notification loaded the full snapshot")
        ),
    )

    def notification_reported(intent_id: str) -> bool:
        stored = store.notification_intent(intent_id)
        return bool(
            stored is not None
            and stored["status"] == "succeeded"
            and stored["last_reported_status"] == "succeeded"
        )

    intent_ids: list[str] = []
    for _index in range(5):
        notification = manager._presenter.periodic()
        assert notification is not None
        intent = await manager.send(notification)
        assert intent is not None
        intent_id = str(intent["intent_id"])
        intent_ids.append(intent_id)
        await wait_until(lambda: notification_reported(intent_id))

    assert len(set(intent_ids)) == 5
    with store._lock:
        count = store._connection.execute(
            "SELECT COUNT(*) FROM notification_intents WHERE run_id = ? "
            "AND dedup_key = 'periodic-status'",
            ("run",),
        ).fetchone()[0]
    assert count == 1
    assert len(requests) == 5
    await manager.close()
    store.close()


@pytest.mark.asyncio
async def test_start_reconciles_terminal_state_to_one_stable_intent(
    tmp_path: Path,
) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(204)

    store = notification_store(tmp_path)
    with store._lock:
        store._connection.execute(
            "UPDATE runs SET status = ?, ended_at = updated_at WHERE run_id = ?",
            (RunStatus.SUCCEEDED.value, "run"),
        )
        store._connection.commit()
    settings = NotificationSettings(webhook_urls=["https://hooks.example/reconcile"])
    bus = EventBus(store, "run")
    first = NotificationManager(settings=settings, store=store, bus=bus, run_id="run")
    await first._client.aclose()
    replace_client(first, handler)
    await first.start()
    await wait_until(lambda: len(requests) == 1)
    await first.close()

    second = NotificationManager(settings=settings, store=store, bus=bus, run_id="run")
    await second._client.aclose()
    replace_client(second, handler)
    await second.start()
    await asyncio.sleep(0.05)
    assert len(requests) == 1
    await second.close()
    store.close()


@pytest.mark.asyncio
async def test_start_reconciles_failed_run_with_persisted_terminal_reason(
    tmp_path: Path,
) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(204)

    store = notification_store(tmp_path)
    store.finish_run(
        "run",
        RunStatus.FAILED,
        message="External resources timed out",
        event_type="run.external_timeout",
        event_payload={"kernel_epoch": 4},
    )
    manager = NotificationManager(
        settings=NotificationSettings(
            webhook_urls=["https://hooks.example/reconcile-reason"]
        ),
        store=store,
        bus=EventBus(store, "run"),
        run_id="run",
    )
    await manager._client.aclose()
    replace_client(manager, handler)
    await manager.start()
    await wait_until(lambda: len(requests) == 1)

    payload = json.loads(requests[0].content)
    assert payload["data"]["kind"] == "run_failed"
    assert payload["data"]["reason"] == "external_timeout"
    await asyncio.sleep(0.03)
    assert len(requests) == 1
    await manager.close()
    store.close()


@pytest.mark.asyncio
async def test_startup_reconciliation_does_not_rearm_failed_delivery_budget(
    tmp_path: Path,
) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(204)

    settings = NotificationSettings(
        webhook_urls=["https://hooks.example/reconcile-budget"],
        max_delivery_attempts=1,
    )
    store = notification_store(tmp_path, settings=settings)
    store.finish_run(
        "run",
        RunStatus.FAILED,
        message="Runner failed",
        event_type="run.runner_error",
        event_payload={},
    )
    manager = NotificationManager(
        settings=settings,
        store=store,
        bus=EventBus(store, "run"),
        run_id="run",
    )
    await manager._client.aclose()
    replace_client(manager, handler)
    notification = manager._presenter.reconcile_state()[0]
    intent = await manager.send(notification)
    assert intent is not None
    claimed = store.claim_due_notification_deliveries("run")
    assert len(claimed) == 1
    store.finish_notification_delivery(
        claimed[0]["delivery_id"],
        succeeded=False,
        max_attempts=1,
        retry_delay_seconds=0,
        error="injected terminal delivery failure",
    )

    await manager.start()
    await wait_until(
        lambda: store.notification_event_cursor("run")
        >= int(store.recent_events("run", limit=1)[-1]["seq"])
    )
    await asyncio.sleep(0.03)
    assert requests == []
    unchanged = store.notification_deliveries(str(intent["intent_id"]))[0]
    assert unchanged["status"] == "failed"
    assert unchanged["attempt_count"] == 1
    await manager.close()
    store.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("canonical_status", ["pending", "failed"])
async def test_terminal_alias_consolidation_prefers_delivered_legacy_intent(
    tmp_path: Path, canonical_status: str
) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(204)

    settings = NotificationSettings(webhook_urls=["https://hooks.example/terminal"])
    store = notification_store(tmp_path, settings=settings)
    legacy = store.enqueue_notification(
        run_id="run",
        title="legacy",
        message="legacy",
        data={},
        dedup_key="run-succeeded:7",
        destinations=notification_destinations(settings),
    )
    canonical = store.enqueue_notification(
        run_id="run",
        title="canonical",
        message="canonical",
        data={},
        dedup_key="run-terminal:succeeded:7",
        destinations=notification_destinations(settings),
    )
    with store._lock:
        store._connection.execute(
            """
            UPDATE notification_deliveries
            SET status = 'succeeded', delivered_at = updated_at
            WHERE intent_id = ?
            """,
            (legacy["intent_id"],),
        )
        store._connection.execute(
            """
            UPDATE notification_intents
            SET status = 'succeeded', completed_at = updated_at,
                last_reported_status = 'succeeded'
            WHERE intent_id = ?
            """,
            (legacy["intent_id"],),
        )
        store._connection.execute(
            "UPDATE notification_intents SET status = ? WHERE intent_id = ?",
            (canonical_status, canonical["intent_id"]),
        )
        store._connection.execute(
            "UPDATE notification_deliveries SET status = ? WHERE intent_id = ?",
            (canonical_status, canonical["intent_id"]),
        )
        store._connection.execute("""
            UPDATE runs SET status = 'succeeded', kernel_epoch = 7,
                ended_at = updated_at WHERE run_id = 'run'
            """)
        store._connection.commit()

    manager = NotificationManager(
        settings=settings, store=store, bus=EventBus(store, "run"), run_id="run"
    )
    await manager._client.aclose()
    replace_client(manager, handler)
    await manager.start()
    await asyncio.sleep(0.05)

    with store._lock:
        remaining = store._connection.execute("""
            SELECT dedup_key, status FROM notification_intents
            WHERE run_id = 'run' AND dedup_key LIKE 'run-%succeeded%'
            """).fetchall()
    assert [(row["dedup_key"], row["status"]) for row in remaining] == [
        ("run-succeeded:7", "succeeded")
    ]
    assert requests == []
    await manager.close()
    store.close()


@pytest.mark.asyncio
async def test_pending_terminal_aliases_consolidate_before_delivery(
    tmp_path: Path,
) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(204)

    settings = NotificationSettings(webhook_urls=["https://hooks.example/terminal"])
    store = notification_store(tmp_path, settings=settings)
    for dedup_key in ("run-succeeded:4", "run-terminal:succeeded:4"):
        store.enqueue_notification(
            run_id="run",
            title="terminal",
            message="terminal",
            data={},
            dedup_key=dedup_key,
            destinations=notification_destinations(settings),
        )
    with store._lock:
        store._connection.execute("""
            UPDATE runs SET status = 'succeeded', kernel_epoch = 4,
                ended_at = updated_at WHERE run_id = 'run'
            """)
        store._connection.commit()

    manager = NotificationManager(
        settings=settings, store=store, bus=EventBus(store, "run"), run_id="run"
    )
    await manager._client.aclose()
    replace_client(manager, handler)
    await manager.start()
    await wait_until(lambda: len(requests) == 1)
    await manager.drain(1)

    with store._lock:
        remaining = store._connection.execute("""
            SELECT dedup_key FROM notification_intents
            WHERE run_id = 'run' AND dedup_key LIKE 'run-%succeeded%'
            """).fetchall()
    assert [row["dedup_key"] for row in remaining] == ["run-terminal:succeeded:4"]
    await manager.close()
    store.close()


@pytest.mark.asyncio
async def test_terminal_alias_consolidation_merges_partial_delivery_success(
    tmp_path: Path,
) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(204)

    settings = NotificationSettings(
        webhook_urls=["https://one.example/hook", "https://two.example/hook"]
    )
    store = notification_store(tmp_path, settings=settings)
    legacy = store.enqueue_notification(
        run_id="run",
        title="legacy",
        message="legacy",
        data={},
        dedup_key="run-succeeded:9",
        destinations=notification_destinations(settings),
    )
    canonical = store.enqueue_notification(
        run_id="run",
        title="canonical",
        message="canonical",
        data={},
        dedup_key="run-terminal:succeeded:9",
        destinations=notification_destinations(settings),
    )
    with store._lock:
        store._connection.execute(
            """
            UPDATE notification_deliveries SET status = 'succeeded'
            WHERE intent_id = ? AND destination LIKE 'https://one.%'
            """,
            (legacy["intent_id"],),
        )
        store._connection.execute(
            """
            UPDATE notification_deliveries SET status = 'succeeded'
            WHERE intent_id = ? AND destination LIKE 'https://two.%'
            """,
            (canonical["intent_id"],),
        )
        store._connection.execute(
            "UPDATE notification_intents SET status = 'partial' WHERE run_id = 'run'"
        )
        store._connection.execute("""
            UPDATE runs SET status = 'succeeded', kernel_epoch = 9,
                ended_at = updated_at WHERE run_id = 'run'
            """)
        store._connection.commit()

    manager = NotificationManager(
        settings=settings, store=store, bus=EventBus(store, "run"), run_id="run"
    )
    await manager._client.aclose()
    replace_client(manager, handler)
    await manager.start()
    await asyncio.sleep(0.05)

    with store._lock:
        intents = store._connection.execute(
            "SELECT intent_id, status FROM notification_intents WHERE run_id = 'run'"
        ).fetchall()
        deliveries = store._connection.execute(
            "SELECT status FROM notification_deliveries WHERE run_id = 'run'"
        ).fetchall()
    assert len(intents) == 1
    assert intents[0]["status"] == "succeeded"
    assert {row["status"] for row in deliveries} == {"succeeded"}
    assert requests == []
    await manager.close()
    store.close()


@pytest.mark.asyncio
async def test_start_reconciles_manifest_first_credential_rotation(
    tmp_path: Path,
) -> None:
    old = NotificationSettings(
        webhook_urls=["https://old.example/hook?token=OLD_WEBHOOK_SECRET"],
        ntfy_base_url="https://old-ntfy.example",
        ntfy_topic="OLD_TOPIC_SECRET",
    )
    desired = NotificationSettings(
        webhook_urls=["https://new.example/hook?token=NEW_WEBHOOK_SECRET"],
        ntfy_base_url="https://new-ntfy.example",
        ntfy_topic="NEW_TOPIC_SECRET",
    )
    store = notification_store(tmp_path, settings=old)
    intent = store.enqueue_notification(
        run_id="run",
        title="OLD_TITLE_SECRET",
        message="OLD_MESSAGE_SECRET",
        data={"legacy": "OLD_DATA_SECRET"},
        dedup_key="legacy-rotation",
        destinations=notification_destinations(old),
    )
    store.append_event(
        "run",
        "notification.failed",
        {"error": "https://old.example/hook?token=OLD_EVENT_SECRET BODY_SECRET"},
    )
    with store._lock:
        store._connection.execute(
            """
            UPDATE notification_deliveries
            SET status = 'failed', last_error = ? WHERE intent_id = ?
            """,
            (
                "https://old.example/hook?token=OLD_ERROR_SECRET RESPONSE_SECRET",
                intent["intent_id"],
            ),
        )
        store._connection.execute(
            "UPDATE notification_intents SET status = 'failed' WHERE intent_id = ?",
            (intent["intent_id"],),
        )
        store._connection.commit()

    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(204)

    manager = NotificationManager(
        settings=desired, store=store, bus=EventBus(store, "run"), run_id="run"
    )
    await manager._client.aclose()
    replace_client(manager, handler)
    await manager.start()
    await wait_until(lambda: len(requests) == 2)
    await manager.drain(1)
    await manager.close()

    request_text = "\n".join(
        f"{request.url}\n{request.content.decode(errors='replace')}"
        for request in requests
    )
    assert "new.example" in request_text
    assert "new-ntfy.example" in request_text
    with store._lock:
        dump = "\n".join(store._connection.iterdump())
    for secret in (
        "OLD_WEBHOOK_SECRET",
        "OLD_TOPIC_SECRET",
        "OLD_TITLE_SECRET",
        "OLD_MESSAGE_SECRET",
        "OLD_DATA_SECRET",
        "OLD_EVENT_SECRET",
        "OLD_ERROR_SECRET",
        "BODY_SECRET",
        "RESPONSE_SECRET",
    ):
        assert secret not in request_text
        assert secret not in dump
    store.close()
    for path in (tmp_path / "state.sqlite3", tmp_path / "state.sqlite3-wal"):
        if path.exists():
            raw = path.read_bytes()
            assert b"OLD_WEBHOOK_SECRET" not in raw
            assert b"OLD_ERROR_SECRET" not in raw


@pytest.mark.asyncio
async def test_same_config_upgrade_scrubs_legacy_diagnostics_once(
    tmp_path: Path,
) -> None:
    settings = NotificationSettings(webhook_urls=["https://hooks.example/same"])
    store = notification_store(tmp_path, settings=settings)
    intent = store.enqueue_notification(
        run_id="run",
        title="legacy",
        message="legacy",
        data={},
        dedup_key="same-config",
        destinations=notification_destinations(settings),
    )
    with store._lock:
        store._connection.execute(
            "UPDATE notification_deliveries SET status = 'succeeded', last_error = ?",
            ("https://hooks.example/same?token=LEGACY_ERROR_SECRET BODY_SECRET",),
        )
        store._connection.execute(
            "UPDATE notification_intents SET status = 'succeeded' WHERE intent_id = ?",
            (intent["intent_id"],),
        )
        store._connection.commit()
    store.append_event("run", "notification.failed", {"error": "LEGACY_EVENT_SECRET"})

    first = NotificationManager(
        settings=settings, store=store, bus=EventBus(store, "run"), run_id="run"
    )
    await first.start()
    await asyncio.sleep(0.02)
    await first.close()
    assert store.notification_deliveries(intent["intent_id"])[0]["last_error"] is None
    migrated = [
        event
        for event in store.recent_events("run", limit=100)
        if event["type"] == "notification.failed"
    ][0]
    assert migrated["payload"] == {}

    store.append_event("run", "notification.sent", {"intent_id": "safe-new-event"})
    second = NotificationManager(
        settings=settings, store=store, bus=EventBus(store, "run"), run_id="run"
    )
    await second.start()
    await asyncio.sleep(0.02)
    await second.close()
    safe = [
        event
        for event in store.recent_events("run", limit=100)
        if event["type"] == "notification.sent"
        and event["payload"].get("intent_id") == "safe-new-event"
    ]
    assert len(safe) == 1
    store.close()
