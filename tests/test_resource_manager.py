# pyright: reportPrivateUsage=false, reportUnknownArgumentType=false, reportUnknownMemberType=false
from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any, ClassVar

import pytest

import runwatch.resources.base as resource_base
from runwatch.events import EventBus
from runwatch.models import (
    ActionKind,
    ActionStatus,
    AwsSettings,
    Ownership,
    ResourceDisposition,
    ResourceEvent,
    ResourceLifecycle,
    ResourceObservation,
    ResourceSpec,
    ResourceStatus,
)
from runwatch.resource_manager import (
    ResourceManager,
    ResourceStopRejected,
    StaleResourceAction,
)
from runwatch.resources.base import (
    AdapterContext,
    AwsClientProvider,
    AwsResourceAdapter,
    ResourceAdapter,
)
from runwatch.storage import RunStore
from runwatch.supervisor import RunSupervisor


class FakeStoppableAdapter(ResourceAdapter):
    provider = "fake"
    resource_type = "job"
    supports_stop = True
    supports_blocking = True
    states: ClassVar[dict[str, str]] = {}

    async def inspect(
        self, resource: dict[str, Any], cursor: dict[str, Any]
    ) -> ResourceObservation:
        cursor["polls"] = int(cursor.get("polls", 0)) + 1
        state = self.states.setdefault(resource["external_id"], "running")
        return ResourceObservation(
            status=(
                ResourceStatus.STOPPED if state == "stopped" else ResourceStatus.RUNNING
            ),
            terminal=state == "stopped",
            metrics={"polls": cursor["polls"]},
        )

    async def stop(self, resource: dict[str, Any]) -> None:
        self.states[resource["external_id"]] = "stopped"


class AlwaysFailingAdapter(ResourceAdapter):
    provider = "fake"
    resource_type = "failing"
    supports_blocking = True

    async def inspect(
        self, resource: dict[str, Any], cursor: dict[str, Any]
    ) -> ResourceObservation:
        cursor["attempts"] = int(cursor.get("attempts", 0)) + 1
        raise RuntimeError("provider unavailable")


class FailOnceAfterCursorMutationAdapter(ResourceAdapter):
    provider = "fake"
    resource_type = "fail_once"
    calls: ClassVar[int] = 0
    received_cursors: ClassVar[list[dict[str, Any]]] = []

    async def inspect(
        self, resource: dict[str, Any], cursor: dict[str, Any]
    ) -> ResourceObservation:
        self.received_cursors.append(dict(cursor))
        cursor["token"] = "page-2"
        self.__class__.calls += 1
        if self.calls == 1:
            raise RuntimeError("failed after reading page")
        return ResourceObservation(
            status=ResourceStatus.RUNNING,
            log_lines=["line-from-page-2"],
        )


class StopRaceAdapter(ResourceAdapter):
    provider = "fake"
    resource_type = "stop_race"
    supports_stop = True
    inspect_started: ClassVar[asyncio.Event]
    release_inspect: ClassVar[asyncio.Event]
    stop_started: ClassVar[asyncio.Event]
    release_stop: ClassVar[asyncio.Event]
    stopped: ClassVar[bool]

    @classmethod
    def reset(cls) -> None:
        cls.inspect_started = asyncio.Event()
        cls.release_inspect = asyncio.Event()
        cls.stop_started = asyncio.Event()
        cls.release_stop = asyncio.Event()
        cls.stopped = False

    async def inspect(
        self, resource: dict[str, Any], cursor: dict[str, Any]
    ) -> ResourceObservation:
        if not self.inspect_started.is_set():
            self.inspect_started.set()
            await self.release_inspect.wait()
            cursor["stale_inspection_committed"] = True
            return ResourceObservation(status=ResourceStatus.RUNNING)
        return ResourceObservation(
            status=(ResourceStatus.STOPPED if self.stopped else ResourceStatus.RUNNING),
            terminal=self.stopped,
        )

    async def stop(self, resource: dict[str, Any]) -> None:
        self.stop_started.set()
        await self.release_stop.wait()
        self.__class__.stopped = True


class DelayedFinalDrainAdapter(ResourceAdapter):
    provider = "fake"
    resource_type = "delayed_final_drain"
    supports_blocking = True
    inspect_calls: ClassVar[int]
    final_inspect_started: ClassVar[asyncio.Event]
    release_final_inspect: ClassVar[asyncio.Event]

    @classmethod
    def reset(cls) -> None:
        cls.inspect_calls = 0
        cls.final_inspect_started = asyncio.Event()
        cls.release_final_inspect = asyncio.Event()

    async def inspect(
        self, resource: dict[str, Any], cursor: dict[str, Any]
    ) -> ResourceObservation:
        self.__class__.inspect_calls += 1
        cursor["inspections"] = self.inspect_calls
        if self.inspect_calls > 1:
            self.final_inspect_started.set()
            await self.release_final_inspect.wait()
            return ResourceObservation(
                status=ResourceStatus.COMPLETED,
                terminal=True,
                log_lines=["final-drain-log"],
            )
        return ResourceObservation(
            status=ResourceStatus.COMPLETED,
            terminal=True,
            log_lines=["terminal-observation"],
        )


def build_store(root: Path, *, max_event_payload_bytes: int = 2_097_152) -> RunStore:
    source = root / "source.ipynb"
    source.write_text("{}", encoding="utf-8")
    store = RunStore(
        root / "state.sqlite3",
        max_event_payload_bytes=max_event_payload_bytes,
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
    return store


@pytest.mark.asyncio
async def test_adapter_context_closes_only_owned_services_once(tmp_path: Path) -> None:
    context = AdapterContext(working_dir=tmp_path, settings={})
    sync_service = object()
    async_service = object()
    borrowed_service = object()
    close_calls: list[tuple[str, object]] = []
    factory_calls = 0

    def factory() -> object:
        nonlocal factory_calls
        factory_calls += 1
        return sync_service

    def close_sync(value: object) -> None:
        close_calls.append(("sync", value))

    async def close_async(value: object) -> None:
        close_calls.append(("async", value))

    assert context.service("sync", factory, close=close_sync) is sync_service
    assert context.service("sync", factory, close=close_sync) is sync_service
    context.register_service("async", async_service, close=close_async)
    context.register_service("borrowed", borrowed_service)
    assert context.service("borrowed", object, close=close_sync) is borrowed_service

    await context.aclose()
    await context.aclose()

    assert factory_calls == 1
    assert close_calls == [("async", async_service), ("sync", sync_service)]
    with pytest.raises(RuntimeError, match="closed"):
        context.service("late", object)


@pytest.mark.asyncio
async def test_direct_aws_adapter_closes_its_factory_provider_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class Provider:
        instances: ClassVar[list[Provider]] = []

        def __init__(self, settings: AwsSettings) -> None:
            self.settings = settings
            self.close_calls = 0
            self.instances.append(self)

        def close(self) -> None:
            self.close_calls += 1

    class DirectAwsAdapter(AwsResourceAdapter):
        provider = "fake"
        resource_type = "direct_aws"

        async def inspect(
            self, resource: dict[str, Any], cursor: dict[str, Any]
        ) -> ResourceObservation:
            return ResourceObservation(status=ResourceStatus.RUNNING)

    monkeypatch.setattr(resource_base, "AwsClientProvider", Provider)
    adapter = DirectAwsAdapter(working_dir=tmp_path, aws_settings=AwsSettings())

    await adapter.close()
    await adapter.close()

    assert len(Provider.instances) == 1
    assert Provider.instances[0].close_calls == 1


@pytest.mark.asyncio
async def test_adapter_does_not_close_supplied_context(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class Provider:
        instances: ClassVar[list[Provider]] = []

        def __init__(self, settings: AwsSettings) -> None:
            self.settings = settings
            self.close_calls = 0
            self.instances.append(self)

        def close(self) -> None:
            self.close_calls += 1

    class ContextAwsAdapter(AwsResourceAdapter):
        provider = "fake"
        resource_type = "context_aws"

        async def inspect(
            self, resource: dict[str, Any], cursor: dict[str, Any]
        ) -> ResourceObservation:
            return ResourceObservation(status=ResourceStatus.RUNNING)

    monkeypatch.setattr(resource_base, "AwsClientProvider", Provider)
    context = AdapterContext(
        working_dir=tmp_path,
        settings={"aws": AwsSettings()},
    )
    adapter = ContextAwsAdapter(context)

    await adapter.close()
    await adapter.close()

    assert len(Provider.instances) == 1
    assert Provider.instances[0].close_calls == 0

    await context.aclose()
    await context.aclose()

    assert Provider.instances[0].close_calls == 1


def test_aws_client_provider_closes_cached_clients_once() -> None:
    class Client:
        close_calls = 0

        def close(self) -> None:
            self.close_calls += 1

    first = Client()
    second = Client()
    provider = AwsClientProvider.__new__(AwsClientProvider)
    provider.settings = AwsSettings()
    provider._clients = {  # noqa: SLF001 - lifecycle unit test
        ("s3", None): first,
        ("logs", None): second,
    }
    provider._closed = False  # noqa: SLF001 - lifecycle unit test

    provider.close()
    provider.close()

    assert first.close_calls == 1
    assert second.close_calls == 1
    with pytest.raises(RuntimeError, match="closed"):
        provider.client("s3")


@pytest.mark.asyncio
async def test_manager_closes_services_after_adapters_and_aggregates_failures(
    tmp_path: Path,
) -> None:
    close_order: list[str] = []

    class Service:
        pass

    def close_service(_service: Service) -> None:
        close_order.append("service")
        raise RuntimeError("service close failed")

    class FailingCloseAdapter(ResourceAdapter):
        provider = "fake"
        resource_type = "failing_close"

        def __init__(self, context: AdapterContext) -> None:
            super().__init__(context)
            self.service = context.service(
                "fake.shared",
                Service,
                close=close_service,
            )

        async def inspect(
            self, resource: dict[str, Any], cursor: dict[str, Any]
        ) -> ResourceObservation:
            return ResourceObservation(status=ResourceStatus.RUNNING)

        async def close(self) -> None:
            close_order.append("adapter")
            raise RuntimeError("adapter close failed")

    store = build_store(tmp_path)
    manager = ResourceManager(
        store=store,
        bus=EventBus(store, "run"),
        run_id="run",
        working_dir=tmp_path,
        aws_settings=AwsSettings(poll_interval_seconds=60),
    )
    manager.register_adapter(FailingCloseAdapter)
    internal_id = await manager.register(
        ResourceEvent(
            resource=ResourceSpec(provider="fake", type="failing_close", id="resource")
        ),
        cell_index=None,
        attempt=None,
        kernel_epoch=None,
    )
    for _ in range(100):
        if internal_id in manager._adapters:  # noqa: SLF001
            break
        await asyncio.sleep(0.001)

    with pytest.raises(RuntimeError, match="Resource cleanup failed") as caught:
        await manager.shutdown()

    assert "adapter close failed" in str(caught.value)
    assert "service close failed" in str(caught.value)
    assert close_order == ["adapter", "service"]
    with pytest.raises(RuntimeError, match="Resource cleanup failed"):
        await manager.shutdown()
    assert close_order == ["adapter", "service"]
    store.close()


@pytest.mark.asyncio
async def test_provider_observation_cannot_overwrite_local_stop_intent(
    tmp_path: Path,
) -> None:
    StopRaceAdapter.reset()
    store = build_store(tmp_path)
    manager = ResourceManager(
        store=store,
        bus=EventBus(store, "run"),
        run_id="run",
        working_dir=tmp_path,
        aws_settings=AwsSettings(poll_interval_seconds=0.001),
    )
    manager.register_adapter(StopRaceAdapter)
    internal_id = await manager.register(
        ResourceEvent(
            resource=ResourceSpec(
                provider="fake",
                type="stop_race",
                id="job",
                ownership=Ownership.EXCLUSIVE,
            ),
            lifecycle=ResourceLifecycle(final_log_drain_seconds=0.001),
        ),
        cell_index=None,
        attempt=None,
        kernel_epoch=None,
    )
    await StopRaceAdapter.inspect_started.wait()

    stop_task = asyncio.create_task(manager.stop_resource(internal_id))
    await StopRaceAdapter.stop_started.wait()
    assert store.get_resource(internal_id)["status"] == "stopping"  # type: ignore[index]

    StopRaceAdapter.release_inspect.set()
    for _ in range(100):
        current = store.get_resource(internal_id)
        if current and current["cursor"].get("stale_inspection_committed"):
            break
        await asyncio.sleep(0.001)
    current = store.get_resource(internal_id)
    assert current is not None
    assert current["cursor"]["stale_inspection_committed"] is True
    assert current["status"] == "stopping"

    StopRaceAdapter.release_stop.set()
    await stop_task
    assert store.get_resource(internal_id)["status"] == "stopped"  # type: ignore[index]
    finalized_events = [
        event
        for event in store.recent_events("run", limit=1_000)
        if event["type"] == "resource.finalized"
    ]
    assert len(finalized_events) == 1
    await manager.shutdown()
    store.close()


@pytest.mark.asyncio
async def test_blocking_resource_waits_for_delayed_final_log_drain(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    DelayedFinalDrainAdapter.reset()
    store = build_store(tmp_path)
    manager = ResourceManager(
        store=store,
        bus=EventBus(store, "run"),
        run_id="run",
        working_dir=tmp_path,
        aws_settings=AwsSettings(poll_interval_seconds=0.001),
    )
    manager.register_adapter(DelayedFinalDrainAdapter)
    internal_id = await manager.register(
        ResourceEvent(
            resource=ResourceSpec(
                provider="fake", type="delayed_final_drain", id="job"
            ),
            lifecycle=ResourceLifecycle(
                blocking=True,
                poll_interval_seconds=0.001,
                final_log_drain_seconds=0.001,
            ),
        ),
        cell_index=None,
        attempt=None,
        kernel_epoch=None,
    )
    wait_task = asyncio.create_task(manager.wait_for_blocking_resources())

    await DelayedFinalDrainAdapter.final_inspect_started.wait()
    pending = store.get_resource(internal_id)
    assert pending is not None
    assert pending["terminal"] is True
    assert pending["monitor_closed"] is False
    assert [item["internal_id"] for item in manager.blocking_summary()["active"]] == [
        internal_id
    ]
    await asyncio.sleep(0)
    assert not wait_task.done()

    publish = manager.bus.publish

    async def reject_oversized_finalization_diagnostic(
        event_type: str, payload: dict[str, Any]
    ) -> dict[str, Any]:
        if event_type == "resource.finalized":
            raise ValueError("Event payload exceeds storage.max_event_payload_bytes")
        return await publish(event_type, payload)

    monkeypatch.setattr(
        manager.bus, "publish", reject_oversized_finalization_diagnostic
    )

    DelayedFinalDrainAdapter.release_final_inspect.set()
    summary = await wait_task
    settled = store.get_resource(internal_id)
    assert settled is not None and settled["monitor_closed"] is True
    assert settled["log_tail"] == ["terminal-observation", "final-drain-log"]
    assert [item["internal_id"] for item in summary["successful"]] == [internal_id]
    await manager.shutdown()
    store.close()


@pytest.mark.asyncio
async def test_restore_resumes_terminal_resource_with_unfinished_final_drain(
    tmp_path: Path,
) -> None:
    DelayedFinalDrainAdapter.reset()
    DelayedFinalDrainAdapter.inspect_calls = 1
    path = tmp_path / "state.sqlite3"
    first = build_store(tmp_path)
    internal_id, _created = first.register_resource(
        run_id="run",
        event=ResourceEvent(
            resource=ResourceSpec(
                provider="fake", type="delayed_final_drain", id="recovered"
            ),
            lifecycle=ResourceLifecycle(
                blocking=True,
                final_log_drain_seconds=0.001,
            ),
        ),
        cell_index=None,
        attempt=None,
        kernel_epoch=None,
        supports_stop=False,
    )
    first.update_resource_observation(
        "run",
        internal_id,
        ResourceObservation(
            status=ResourceStatus.COMPLETED,
            terminal=True,
            log_lines=["before-crash"],
        ),
    )
    assert first.get_resource(internal_id)["monitor_closed"] is False  # type: ignore[index]
    first.close()

    recovered = RunStore(path)
    manager = ResourceManager(
        store=recovered,
        bus=EventBus(recovered, "run"),
        run_id="run",
        working_dir=tmp_path,
        aws_settings=AwsSettings(),
    )
    manager.register_adapter(DelayedFinalDrainAdapter)
    await manager.restore_monitors()
    wait_task = asyncio.create_task(manager.wait_for_blocking_resources())

    await DelayedFinalDrainAdapter.final_inspect_started.wait()
    assert not wait_task.done()
    DelayedFinalDrainAdapter.release_final_inspect.set()
    summary = await wait_task

    settled = recovered.get_resource(internal_id)
    assert settled is not None and settled["monitor_closed"] is True
    assert settled["cursor"] == {"inspections": 2}
    assert settled["log_tail"] == ["before-crash", "final-drain-log"]
    assert [item["internal_id"] for item in summary["successful"]] == [internal_id]
    await manager.shutdown()
    recovered.close()


@pytest.mark.asyncio
async def test_monitor_error_cannot_overwrite_local_stop_intent(tmp_path: Path) -> None:
    store = build_store(tmp_path)
    manager = ResourceManager(
        store=store,
        bus=EventBus(store, "run"),
        run_id="run",
        working_dir=tmp_path,
        aws_settings=AwsSettings(),
    )
    manager.register_adapter(FakeStoppableAdapter)
    internal_id, _ = store.register_resource(
        run_id="run",
        event=ResourceEvent(
            resource=ResourceSpec(
                provider="fake",
                type="job",
                id="error-race",
                ownership=Ownership.EXCLUSIVE,
            ),
            lifecycle=ResourceLifecycle(
                blocking=True,
                max_consecutive_monitor_errors=1,
            ),
        ),
        cell_index=None,
        attempt=None,
        kernel_epoch=None,
        supports_stop=True,
    )
    before_stop = store.get_resource(internal_id)
    assert before_stop is not None
    store.request_resource_stop(internal_id, ResourceDisposition.CANCELLED)

    error = RuntimeError("stale provider error")
    await manager._record_monitor_error(internal_id, error, 1)  # noqa: SLF001
    failed = await manager._fail_blocking_resource_after_monitor_errors(  # noqa: SLF001
        internal_id,
        before_stop,
        error,
        {},
        1,
    )

    current = store.get_resource(internal_id)
    assert current is not None
    assert current["status"] == "stopping"
    assert current["terminal"] is False
    assert failed is False
    event = store.recent_events("run")[-1]
    assert event["type"] == "resource.monitor_error"
    assert event["payload"]["stop_intent_preserved"] is True
    await manager.shutdown()
    store.close()


@pytest.mark.asyncio
async def test_stop_waits_for_terminal_and_persists_cursor(tmp_path: Path) -> None:
    FakeStoppableAdapter.states.clear()
    store = build_store(tmp_path)
    manager = ResourceManager(
        store=store,
        bus=EventBus(store, "run"),
        run_id="run",
        working_dir=tmp_path,
        aws_settings=AwsSettings(poll_interval_seconds=0.01, final_log_drain_seconds=0),
    )
    manager.register_adapter(FakeStoppableAdapter)
    event = ResourceEvent(
        resource=ResourceSpec(
            provider="fake", type="job", id="job-1", ownership=Ownership.EXCLUSIVE
        ),
        lifecycle=ResourceLifecycle(
            blocking=True, stop_on_cancel=True, final_log_drain_seconds=0
        ),
    )
    internal_id = await manager.register(event, cell_index=2, attempt=1, kernel_epoch=1)
    await asyncio.sleep(0.03)
    await manager.stop_resource(internal_id)
    resource = store.get_resource(internal_id)
    assert resource and resource["status"] == "stopped"
    assert resource["cursor"]["polls"] >= 1
    await manager.shutdown()
    store.close()


@pytest.mark.asyncio
async def test_terminal_stop_state_survives_crash_before_confirmation_event(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    FakeStoppableAdapter.states.clear()
    store = build_store(tmp_path)
    manager = ResourceManager(
        store=store,
        bus=EventBus(store, "run"),
        run_id="run",
        working_dir=tmp_path,
        aws_settings=AwsSettings(poll_interval_seconds=0.01),
    )
    manager.register_adapter(FakeStoppableAdapter)
    internal_id = await manager.register(
        ResourceEvent(
            resource=ResourceSpec(
                provider="fake",
                type="job",
                id="crash-gap",
                ownership=Ownership.EXCLUSIVE,
            ),
            lifecycle=ResourceLifecycle(monitor=False),
        ),
        cell_index=None,
        attempt=None,
        kernel_epoch=0,
    )

    async def crash_after_atomic_commit(
        resource_id: str,
        resource: dict[str, Any],
        disposition: ResourceDisposition,
    ) -> dict[str, Any]:
        assert resource_id == internal_id
        assert resource["terminal"] is True
        assert resource["disposition"] == disposition.value == "cancelled"
        raise RuntimeError("simulated process crash after terminal inspection")

    monkeypatch.setattr(
        manager,
        "_confirm_resource_stop",
        crash_after_atomic_commit,
    )
    with pytest.raises(RuntimeError, match="simulated process crash"):
        await manager.stop_resource(internal_id)

    committed = store.get_resource(internal_id)
    assert committed is not None
    assert committed["status"] == "stopped"
    assert committed["terminal"] is True
    assert committed["disposition"] == "cancelled"
    assert committed["monitor_closed"] is True
    assert committed["cursor"]["polls"] >= 1
    await manager.shutdown()
    store.close()


@pytest.mark.asyncio
async def test_recovered_stopping_resource_is_inspected_before_stop_reissue(
    tmp_path: Path,
) -> None:
    class TerminalAfterCrashAdapter(ResourceAdapter):
        provider = "fake"
        resource_type = "terminal_after_crash"
        supports_stop = True
        stop_calls: ClassVar[int] = 0

        async def inspect(
            self, resource: dict[str, Any], cursor: dict[str, Any]
        ) -> ResourceObservation:
            cursor["recovery_inspections"] = (
                int(cursor.get("recovery_inspections", 0)) + 1
            )
            return ResourceObservation(
                status=ResourceStatus.STOPPED,
                terminal=True,
                log_lines=["provider was already terminal"],
            )

        async def stop(self, resource: dict[str, Any]) -> None:
            self.__class__.stop_calls += 1
            raise RuntimeError("provider rejects stop for a terminal resource")

    TerminalAfterCrashAdapter.stop_calls = 0
    store = build_store(tmp_path)
    manager = ResourceManager(
        store=store,
        bus=EventBus(store, "run"),
        run_id="run",
        working_dir=tmp_path,
        aws_settings=AwsSettings(),
    )
    manager.register_adapter(TerminalAfterCrashAdapter)
    internal_id = await manager.register(
        ResourceEvent(
            resource=ResourceSpec(
                provider="fake",
                type="terminal_after_crash",
                id="job",
                ownership=Ownership.EXCLUSIVE,
            ),
            lifecycle=ResourceLifecycle(monitor=False),
        ),
        cell_index=None,
        attempt=None,
        kernel_epoch=0,
    )
    stopping = store.request_resource_stop(internal_id, ResourceDisposition.CANCELLED)
    assert stopping["status"] == "stopping"
    assert stopping["disposition"] == "active"
    cancellation_callbacks = 0

    async def cancel() -> None:
        nonlocal cancellation_callbacks
        cancellation_callbacks += 1

    await manager.stop_resource(
        internal_id,
        allow_stopping=True,
        on_stop_accepted=cancel,
    )

    confirmed = store.get_resource(internal_id)
    assert confirmed is not None
    assert confirmed["status"] == "stopped"
    assert confirmed["terminal"] is True
    assert confirmed["disposition"] == "cancelled"
    assert confirmed["cursor"] == {"recovery_inspections": 2}
    assert confirmed["log_tail"] == [
        "provider was already terminal",
        "provider was already terminal",
    ]
    assert TerminalAfterCrashAdapter.stop_calls == 0
    assert cancellation_callbacks == 1
    await manager.shutdown()
    store.close()


@pytest.mark.asyncio
async def test_logical_key_reconciles_replayed_emission(tmp_path: Path) -> None:
    store = build_store(tmp_path)
    manager = ResourceManager(
        store=store,
        bus=EventBus(store, "run"),
        run_id="run",
        working_dir=tmp_path,
        aws_settings=AwsSettings(final_log_drain_seconds=0),
    )
    manager.register_adapter(FakeStoppableAdapter)
    event = ResourceEvent(
        resource=ResourceSpec(
            provider="fake", type="job", id="job", logical_key="build"
        )
    )
    first = await manager.register(event, cell_index=0, attempt=1, kernel_epoch=1)
    replay = ResourceEvent(
        resource=ResourceSpec(
            provider="fake", type="job", id="job", logical_key="build"
        )
    )
    second = await manager.register(replay, cell_index=0, attempt=2, kernel_epoch=2)
    assert first == second
    assert len(store.list_resources("run")) == 1
    reconciled = store.get_resource(first)
    assert reconciled and reconciled["attempt"] == 2
    assert reconciled["kernel_epoch"] == 2
    await manager.shutdown()
    store.close()


@pytest.mark.asyncio
async def test_logical_key_supersedes_changed_provider_resource(tmp_path: Path) -> None:
    class TrackingAdapter(ResourceAdapter):
        provider = "fake"
        resource_type = "superseded"
        inspected = asyncio.Event()
        closed: ClassVar[list[str]] = []

        def __init__(self, context: AdapterContext) -> None:
            super().__init__(context)
            self.external_id = "uninspected"

        async def inspect(
            self, resource: dict[str, Any], cursor: dict[str, Any]
        ) -> ResourceObservation:
            self.external_id = str(resource["external_id"])
            self.inspected.set()
            return ResourceObservation(status=ResourceStatus.RUNNING)

        async def close(self) -> None:
            self.closed.append(self.external_id)

    store = build_store(tmp_path)
    manager = ResourceManager(
        store=store,
        bus=EventBus(store, "run"),
        run_id="run",
        working_dir=tmp_path,
        aws_settings=AwsSettings(),
    )
    manager.register_adapter(TrackingAdapter)
    first = await manager.register(
        ResourceEvent(
            resource=ResourceSpec(
                provider="fake",
                type="superseded",
                id="job-attempt-1",
                logical_key="build",
            )
        ),
        cell_index=0,
        attempt=1,
        kernel_epoch=1,
    )
    await asyncio.wait_for(TrackingAdapter.inspected.wait(), timeout=1)
    second = await manager.register(
        ResourceEvent(
            resource=ResourceSpec(
                provider="fake",
                type="superseded",
                id="job-attempt-2",
                logical_key="build",
            )
        ),
        cell_index=0,
        attempt=2,
        kernel_epoch=2,
    )
    assert first != second
    resources = {item["internal_id"]: item for item in store.list_resources("run")}
    assert resources[first]["disposition"] == "superseded"
    assert resources[first]["monitor_closed"]
    assert resources[second]["external_id"] == "job-attempt-2"
    assert resources[second]["attempt"] == 2
    assert resources[second]["kernel_epoch"] == 2
    assert first not in manager._monitor_tasks  # noqa: SLF001
    assert first not in manager._adapters  # noqa: SLF001
    assert TrackingAdapter.closed == ["job-attempt-1"]
    await manager.shutdown()
    store.close()


@pytest.mark.asyncio
async def test_cancellation_stops_every_eligible_resource(tmp_path: Path) -> None:
    FakeStoppableAdapter.states.clear()
    store = build_store(tmp_path)
    manager = ResourceManager(
        store=store,
        bus=EventBus(store, "run"),
        run_id="run",
        working_dir=tmp_path,
        aws_settings=AwsSettings(poll_interval_seconds=0.01, final_log_drain_seconds=0),
    )
    manager.register_adapter(FakeStoppableAdapter)
    ids = []
    for name in ("job-a", "job-b"):
        ids.append(
            await manager.register(
                ResourceEvent(
                    resource=ResourceSpec(
                        provider="fake",
                        type="job",
                        id=name,
                        ownership=Ownership.EXCLUSIVE,
                    ),
                    lifecycle=ResourceLifecycle(
                        stop_on_cancel=True, final_log_drain_seconds=0
                    ),
                ),
                cell_index=0,
                attempt=1,
                kernel_epoch=1,
            )
        )
    await asyncio.sleep(0.03)

    stopped = await manager.stop_cancel_resources(first=ids[1])

    assert stopped == [ids[1], ids[0]]
    assert all(store.get_resource(item)["status"] == "stopped" for item in ids)  # type: ignore[index]
    await manager.shutdown()
    store.close()


@pytest.mark.asyncio
async def test_registration_rejection_and_monitor_disabled(tmp_path: Path) -> None:
    store = build_store(tmp_path)
    bus = EventBus(store, "run")
    manager = ResourceManager(
        store=store,
        bus=bus,
        run_id="run",
        working_dir=tmp_path,
        aws_settings=AwsSettings(),
    )
    with pytest.raises(Exception, match="No adapter"):
        await manager.register(
            ResourceEvent(
                resource=ResourceSpec(provider="unknown", type="job", id="x")
            ),
            cell_index=None,
            attempt=None,
            kernel_epoch=None,
        )
    assert store.recent_events("run")[-1]["type"] == "resource.rejected"

    manager.register_adapter(FakeStoppableAdapter)
    internal_id = await manager.register(
        ResourceEvent(
            resource=ResourceSpec(provider="fake", type="job", id="unmonitored"),
            lifecycle=ResourceLifecycle(monitor=False),
        ),
        cell_index=None,
        attempt=None,
        kernel_epoch=None,
    )
    resource = store.get_resource(internal_id)
    assert resource and resource["monitor_closed"]
    await manager.shutdown()
    store.close()


@pytest.mark.asyncio
async def test_resource_registration_and_observation_ignore_fanout_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = build_store(tmp_path)
    bus = EventBus(store, "run")
    manager = ResourceManager(
        store=store,
        bus=bus,
        run_id="run",
        working_dir=tmp_path,
        aws_settings=AwsSettings(poll_interval_seconds=5),
    )
    manager.register_adapter(FakeStoppableAdapter)

    def fail_in_memory_fan_out(event: dict[str, Any]) -> None:
        raise RuntimeError(f"subscriber rejected event {event['seq']}")

    monkeypatch.setattr(bus, "_fan_out_nowait", fail_in_memory_fan_out)
    internal_id = await manager.register(
        ResourceEvent(
            resource=ResourceSpec(provider="fake", type="job", id="fanout-job")
        ),
        cell_index=1,
        attempt=2,
        kernel_epoch=3,
    )

    for _ in range(100):
        resource = store.get_resource(internal_id)
        if resource and resource["status"] == ResourceStatus.RUNNING.value:
            break
        await asyncio.sleep(0.005)

    resource = store.get_resource(internal_id)
    assert resource is not None
    assert resource["status"] == ResourceStatus.RUNNING.value
    assert internal_id in manager._monitor_tasks  # noqa: SLF001
    assert not manager._monitor_tasks[internal_id].done()  # noqa: SLF001
    event_types = [event["type"] for event in store.recent_events("run")]
    assert "resource.registered" in event_types
    assert "resource.observed" in event_types
    await manager.shutdown()
    store.close()


@pytest.mark.asyncio
async def test_stop_rejects_stale_borrowed_and_unsupported_resources(
    tmp_path: Path,
) -> None:
    store = build_store(tmp_path)
    manager = ResourceManager(
        store=store,
        bus=EventBus(store, "run"),
        run_id="run",
        working_dir=tmp_path,
        aws_settings=AwsSettings(),
    )
    manager.register_adapter(FakeStoppableAdapter)
    borrowed = await manager.register(
        ResourceEvent(
            resource=ResourceSpec(
                provider="fake",
                type="job",
                id="borrowed",
                ownership=Ownership.BORROWED,
            )
        ),
        cell_index=None,
        attempt=None,
        kernel_epoch=None,
    )
    current = store.get_resource(borrowed)
    assert current is not None
    with pytest.raises(StaleResourceAction, match="changed"):
        await manager.stop_resource(borrowed, expected_version=current["version"] + 1)
    with pytest.raises(ResourceStopRejected, match="exclusive"):
        await manager.stop_resource(borrowed)

    class ObservedAdapter(ResourceAdapter):
        provider = "fake"
        resource_type = "observed"

        async def inspect(
            self, resource: dict[str, Any], cursor: dict[str, Any]
        ) -> ResourceObservation:
            return ResourceObservation(status=ResourceStatus.RUNNING)

    manager.register_adapter(ObservedAdapter)
    observed = await manager.register(
        ResourceEvent(
            resource=ResourceSpec(
                provider="fake",
                type="observed",
                id="observed",
                ownership=Ownership.EXCLUSIVE,
            )
        ),
        cell_index=None,
        attempt=None,
        kernel_epoch=None,
    )
    with pytest.raises(ResourceStopRejected, match="No stop action"):
        await manager.stop_resource(observed)
    await manager.shutdown()
    store.close()


@pytest.mark.asyncio
async def test_stale_or_inactive_stop_never_runs_cancellation_callback(
    tmp_path: Path,
) -> None:
    FakeStoppableAdapter.states.clear()
    store = build_store(tmp_path)
    manager = ResourceManager(
        store=store,
        bus=EventBus(store, "run"),
        run_id="run",
        working_dir=tmp_path,
        aws_settings=AwsSettings(),
    )
    manager.register_adapter(FakeStoppableAdapter)
    internal_id = await manager.register(
        ResourceEvent(
            resource=ResourceSpec(
                provider="fake",
                type="job",
                id="exclusive",
                ownership=Ownership.EXCLUSIVE,
            )
        ),
        cell_index=None,
        attempt=None,
        kernel_epoch=None,
    )
    resource = store.get_resource(internal_id)
    assert resource is not None
    callbacks = 0

    async def cancelled() -> None:
        nonlocal callbacks
        callbacks += 1

    with pytest.raises(StaleResourceAction):
        await manager.stop_resource(
            internal_id,
            expected_version=resource["version"] + 1,
            on_stop_accepted=cancelled,
        )
    assert callbacks == 0
    assert store.get_resource(internal_id)["status"] == "registered"  # type: ignore[index]

    store.set_resource_disposition(internal_id, ResourceDisposition.SUPERSEDED)
    with pytest.raises(ResourceStopRejected, match="active"):
        await manager.stop_resource(internal_id, on_stop_accepted=cancelled)
    assert callbacks == 0
    assert FakeStoppableAdapter.states == {}
    await manager.shutdown()
    store.close()


@pytest.mark.asyncio
async def test_stop_timeout_reports_last_inspection_error(tmp_path: Path) -> None:
    class InspectionFailureAdapter(ResourceAdapter):
        provider = "fake"
        resource_type = "inspection_failure"
        supports_stop = True

        async def inspect(
            self, resource: dict[str, Any], cursor: dict[str, Any]
        ) -> ResourceObservation:
            raise RuntimeError("provider unavailable")

        async def stop(self, resource: dict[str, Any]) -> None:
            return None

    store = build_store(tmp_path)
    manager = ResourceManager(
        store=store,
        bus=EventBus(store, "run"),
        run_id="run",
        working_dir=tmp_path,
        aws_settings=AwsSettings(stop_timeout_seconds=0.02),
    )
    manager.register_adapter(InspectionFailureAdapter)
    internal_id = await manager.register(
        ResourceEvent(
            resource=ResourceSpec(
                provider="fake",
                type="inspection_failure",
                id="job",
                ownership=Ownership.EXCLUSIVE,
            )
        ),
        cell_index=None,
        attempt=None,
        kernel_epoch=None,
    )
    with pytest.raises(TimeoutError, match="provider unavailable"):
        await manager.stop_resource(internal_id)
    resource = store.get_resource(internal_id)
    assert resource is not None and "provider unavailable" in resource["message"]
    assert resource["status"] != "stopping"
    manager.validate_stop_eligibility(internal_id)
    assert any(
        event["type"] == "resource.stop_inspection_error"
        for event in store.recent_events("run")
    )
    timeout_event = next(
        event
        for event in store.recent_events("run")
        if event["type"] == "resource.stop_timeout"
    )
    assert timeout_event["payload"]["provider_acknowledged"] is True
    await manager.shutdown()
    store.close()


@pytest.mark.asyncio
async def test_hung_provider_stop_times_out_and_finishes_action(tmp_path: Path) -> None:
    class HungStopAdapter(ResourceAdapter):
        provider = "fake"
        resource_type = "hung_stop"
        supports_stop = True

        async def inspect(
            self, resource: dict[str, Any], cursor: dict[str, Any]
        ) -> ResourceObservation:
            return ResourceObservation(status=ResourceStatus.RUNNING)

        async def stop(self, resource: dict[str, Any]) -> None:
            await asyncio.Event().wait()

    store = build_store(tmp_path)
    bus = EventBus(store, "run")
    manager = ResourceManager(
        store=store,
        bus=bus,
        run_id="run",
        working_dir=tmp_path,
        aws_settings=AwsSettings(stop_timeout_seconds=0.02),
    )
    manager.register_adapter(HungStopAdapter)
    internal_id = await manager.register(
        ResourceEvent(
            resource=ResourceSpec(
                provider="fake",
                type="hung_stop",
                id="job",
                ownership=Ownership.EXCLUSIVE,
            ),
            lifecycle=ResourceLifecycle(monitor=False),
        ),
        cell_index=None,
        attempt=None,
        kernel_epoch=0,
    )
    resource = store.get_resource(internal_id)
    assert resource is not None
    action_id = store.create_action(
        "run",
        ActionKind.STOP_RESOURCE,
        payload={
            "internal_id": internal_id,
            "expected_version": resource["version"],
        },
        expected_kernel_epoch=0,
    )

    class Runner:
        cancelled = False

        async def cancel(self) -> None:
            self.cancelled = True

    supervisor = RunSupervisor.__new__(RunSupervisor)
    supervisor.run_id = "run"
    supervisor.store = store
    supervisor.bus = bus
    supervisor.resources = manager
    runner = Runner()
    supervisor.runner = runner  # type: ignore[assignment]
    task = asyncio.create_task(supervisor._action_loop())  # noqa: SLF001
    try:
        for _ in range(100):
            action = store.get_action(action_id)
            if action and ActionStatus(action["status"]).terminal:
                break
            await asyncio.sleep(0.005)
        action = store.get_action(action_id)
        assert action is not None and action["status"] == "failed"
        assert "Timed out" in action["message"]
        assert runner.cancelled is False
        current = store.get_resource(internal_id)
        assert current is not None and current["status"] == "registered"
        assert "timed out" in current["message"]
        assert "provider outcome is unknown" in current["message"]
        assert manager.validate_stop_eligibility(internal_id) == current
        timeout_event = next(
            event
            for event in store.recent_events("run")
            if event["type"] == "resource.stop_timeout"
        )
        assert timeout_event["payload"]["retryable"] is True
        assert timeout_event["payload"]["provider_acknowledged"] is False
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await manager.shutdown()
        store.close()


@pytest.mark.asyncio
async def test_stale_stop_action_is_rejected_without_cancelling_run(
    tmp_path: Path,
) -> None:
    store = build_store(tmp_path)
    bus = EventBus(store, "run")
    manager = ResourceManager(
        store=store,
        bus=bus,
        run_id="run",
        working_dir=tmp_path,
        aws_settings=AwsSettings(),
    )
    manager.register_adapter(FakeStoppableAdapter)
    internal_id = await manager.register(
        ResourceEvent(
            resource=ResourceSpec(
                provider="fake",
                type="job",
                id="job",
                ownership=Ownership.EXCLUSIVE,
            )
        ),
        cell_index=None,
        attempt=None,
        kernel_epoch=0,
    )
    confirmed = store.get_resource(internal_id)
    assert confirmed is not None
    action_id = store.create_action(
        "run",
        ActionKind.STOP_RESOURCE,
        payload={
            "internal_id": internal_id,
            "expected_version": confirmed["version"],
        },
        expected_kernel_epoch=0,
    )
    store.update_resource_observation(
        "run",
        internal_id,
        ResourceObservation(status=ResourceStatus.RUNNING),
    )

    class Runner:
        cancelled = False

        async def cancel(self) -> None:
            self.cancelled = True

    supervisor = RunSupervisor.__new__(RunSupervisor)
    supervisor.run_id = "run"
    supervisor.store = store
    supervisor.bus = bus
    supervisor.resources = manager
    runner = Runner()
    supervisor.runner = runner  # type: ignore[assignment]
    task = asyncio.create_task(supervisor._action_loop())  # noqa: SLF001
    try:
        for _ in range(100):
            action = store.get_action(action_id)
            if action and ActionStatus(action["status"]).terminal:
                break
            await asyncio.sleep(0.01)
        action = store.get_action(action_id)
        assert action is not None and action["status"] == "rejected"
        assert runner.cancelled is False
        assert store.get_run("run")["status"] == "created"
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await manager.shutdown()
        store.close()


@pytest.mark.asyncio
async def test_blocking_summary_and_cancel_event(tmp_path: Path) -> None:
    store = build_store(tmp_path)
    manager = ResourceManager(
        store=store,
        bus=EventBus(store, "run"),
        run_id="run",
        working_dir=tmp_path,
        aws_settings=AwsSettings(),
    )
    manager.register_adapter(FakeStoppableAdapter)
    active = await manager.register(
        ResourceEvent(
            resource=ResourceSpec(provider="fake", type="job", id="active"),
            lifecycle=ResourceLifecycle(blocking=True),
        ),
        cell_index=None,
        attempt=None,
        kernel_epoch=None,
    )
    summary = manager.blocking_summary()
    assert [item["internal_id"] for item in summary["active"]] == [active]
    cancel = asyncio.Event()
    cancel.set()
    cancelled_summary = await manager.wait_for_blocking_resources(cancel_event=cancel)
    assert cancelled_summary["active"]
    await manager.close_nonblocking_monitors()
    await manager.shutdown()
    store.close()


@pytest.mark.asyncio
async def test_cancellation_skips_ineligible_persisted_resource(tmp_path: Path) -> None:
    store = build_store(tmp_path)
    manager = ResourceManager(
        store=store,
        bus=EventBus(store, "run"),
        run_id="run",
        working_dir=tmp_path,
        aws_settings=AwsSettings(),
    )
    internal_id, _ = store.register_resource(
        run_id="run",
        event=ResourceEvent(
            resource=ResourceSpec(provider="fake", type="job", id="borrowed"),
            lifecycle=ResourceLifecycle(stop_on_cancel=True),
        ),
        cell_index=None,
        attempt=None,
        kernel_epoch=None,
        supports_stop=False,
    )
    assert await manager.stop_cancel_resources() == []
    assert store.get_resource(internal_id)["disposition"] == "active"  # type: ignore[index]
    await manager.shutdown()
    store.close()


@pytest.mark.asyncio
async def test_blocking_monitor_fails_after_configured_error_limit(
    tmp_path: Path,
) -> None:
    store = build_store(tmp_path)
    manager = ResourceManager(
        store=store,
        bus=EventBus(store, "run"),
        run_id="run",
        working_dir=tmp_path,
        aws_settings=AwsSettings(poll_interval_seconds=0.001),
    )
    manager.register_adapter(AlwaysFailingAdapter)
    internal_id = await manager.register(
        ResourceEvent(
            resource=ResourceSpec(provider="fake", type="failing", id="broken"),
            lifecycle=ResourceLifecycle(
                blocking=True,
                poll_interval_seconds=0.001,
                max_consecutive_monitor_errors=2,
            ),
        ),
        cell_index=None,
        attempt=None,
        kernel_epoch=None,
    )

    for _ in range(100):
        resource = store.get_resource(internal_id)
        if resource and resource["terminal"]:
            break
        await asyncio.sleep(0.005)

    resource = store.get_resource(internal_id)
    assert resource and resource["status"] == "failed"
    assert resource["terminal"]
    assert resource["cursor"] == {}
    assert any(
        event["type"] == "resource.monitor_failed"
        for event in store.recent_events("run")
    )
    await manager.shutdown()
    store.close()


@pytest.mark.asyncio
async def test_long_monitor_diagnostic_at_minimum_cap_preserves_retry_threshold(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class NoisyFailingAdapter(ResourceAdapter):
        provider = "fake"
        resource_type = "noisy_failure"
        supports_blocking = True
        calls = 0

        async def inspect(
            self, resource: dict[str, Any], cursor: dict[str, Any]
        ) -> ResourceObservation:
            self.__class__.calls += 1
            raise RuntimeError("provider unavailable: " + ("x" * 4_000))

    store = build_store(tmp_path, max_event_payload_bytes=1_024)
    bus = EventBus(store, "run")
    original_publish = bus.publish
    rejected_first_diagnostic = False

    async def reject_first_monitor_diagnostic(
        event_type: str, payload: dict[str, Any]
    ) -> dict[str, Any]:
        nonlocal rejected_first_diagnostic
        if event_type == "resource.monitor_error" and not rejected_first_diagnostic:
            rejected_first_diagnostic = True
            raise OSError("injected diagnostic persistence failure")
        return await original_publish(event_type, payload)

    monkeypatch.setattr(bus, "publish", reject_first_monitor_diagnostic)
    manager = ResourceManager(
        store=store,
        bus=bus,
        run_id="run",
        working_dir=tmp_path,
        aws_settings=AwsSettings(
            poll_interval_seconds=0.001,
            final_log_drain_seconds=0,
        ),
    )
    manager.register_adapter(NoisyFailingAdapter)
    internal_id = await manager.register(
        ResourceEvent(
            resource=ResourceSpec(provider="fake", type="noisy_failure", id="broken"),
            lifecycle=ResourceLifecycle(
                blocking=True,
                poll_interval_seconds=0.001,
                max_consecutive_monitor_errors=2,
            ),
        ),
        cell_index=None,
        attempt=None,
        kernel_epoch=None,
    )

    for _ in range(100):
        resource = store.get_resource(internal_id)
        if resource and resource["terminal"]:
            break
        await asyncio.sleep(0.005)

    resource = store.get_resource(internal_id)
    assert rejected_first_diagnostic is True
    assert NoisyFailingAdapter.calls == 2
    assert resource is not None and resource["status"] == "failed"
    assert resource["terminal"] is True
    events = store.recent_events("run")
    monitor_errors = [
        event for event in events if event["type"] == "resource.monitor_error"
    ]
    assert len(monitor_errors) == 1
    assert monitor_errors[0]["payload"]["consecutive_errors"] == 2
    assert monitor_errors[0]["payload"]["error"].endswith("…")
    assert any(event["type"] == "resource.monitor_failed" for event in events)
    assert not any(event["type"] == "resource.monitor_task_failed" for event in events)
    await manager.shutdown()
    store.close()


@pytest.mark.asyncio
async def test_monitor_error_backoff_never_accelerates_long_base_interval(
    tmp_path: Path,
) -> None:
    store = build_store(tmp_path)
    manager = ResourceManager(
        store=store,
        bus=EventBus(store, "run"),
        run_id="run",
        working_dir=tmp_path,
        aws_settings=AwsSettings(poll_interval_seconds=300),
    )

    assert (
        manager._poll_interval(  # noqa: SLF001
            {"lifecycle": {"poll_interval_seconds": 300}}, 1
        )
        == 300
    )
    assert (
        manager._poll_interval(  # noqa: SLF001
            {"lifecycle": {"poll_interval_seconds": 15}}, 1
        )
        == 30
    )
    assert (
        manager._poll_interval(  # noqa: SLF001
            {"lifecycle": {"poll_interval_seconds": 15}}, 2
        )
        == 60
    )
    await manager.shutdown()
    store.close()


@pytest.mark.asyncio
async def test_unexpected_monitor_task_failure_terminalizes_resource(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = build_store(tmp_path)
    manager = ResourceManager(
        store=store,
        bus=EventBus(store, "run"),
        run_id="run",
        working_dir=tmp_path,
        aws_settings=AwsSettings(poll_interval_seconds=0.001),
    )
    manager.register_adapter(FakeStoppableAdapter)
    publish_calls = 0
    publish_observation = manager._publish_observation  # noqa: SLF001

    def fail_first_observation_fan_out(event: dict[str, Any]) -> None:
        nonlocal publish_calls
        publish_calls += 1
        if publish_calls == 1:
            raise RuntimeError("fan-out exploded")
        publish_observation(event)

    monkeypatch.setattr(manager, "_publish_observation", fail_first_observation_fan_out)
    internal_id = await manager.register(
        ResourceEvent(
            resource=ResourceSpec(provider="fake", type="job", id="monitor-crash"),
            lifecycle=ResourceLifecycle(
                blocking=True,
                poll_interval_seconds=0.001,
            ),
        ),
        cell_index=None,
        attempt=None,
        kernel_epoch=None,
    )

    summary = await manager.wait_for_blocking_resources(timeout_seconds=2)

    resource = store.get_resource(internal_id)
    assert resource is not None
    assert resource["terminal"] is True
    assert resource["status"] == "failed"
    assert resource["metrics"] == {"monitor_task_error": "RuntimeError"}
    assert "fan-out exploded" in resource["message"]
    assert [item["internal_id"] for item in summary["failures"]] == [internal_id]
    assert any(
        event["type"] == "resource.monitor_task_failed"
        for event in store.recent_events("run")
    )
    await manager.shutdown()
    store.close()


@pytest.mark.asyncio
async def test_blocking_wait_detects_failed_monitor_terminalization(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = build_store(tmp_path)
    manager = ResourceManager(
        store=store,
        bus=EventBus(store, "run"),
        run_id="run",
        working_dir=tmp_path,
        aws_settings=AwsSettings(poll_interval_seconds=0.001),
    )
    manager.register_adapter(FakeStoppableAdapter)

    def fail_observation_fan_out(event: dict[str, Any]) -> None:
        raise RuntimeError(f"fan-out failed for event {event['seq']}")

    def fail_terminalization(
        run_id: str,
        internal_id: str,
        observation: ResourceObservation,
    ) -> dict[str, Any]:
        raise OSError(
            f"cannot terminalize {run_id}/{internal_id}/{observation.status.value}"
        )

    monkeypatch.setattr(manager, "_publish_observation", fail_observation_fan_out)
    monkeypatch.setattr(store, "update_resource_observation", fail_terminalization)
    internal_id = await manager.register(
        ResourceEvent(
            resource=ResourceSpec(
                provider="fake", type="job", id="terminalization-crash"
            ),
            lifecycle=ResourceLifecycle(
                blocking=True,
                poll_interval_seconds=0.001,
            ),
        ),
        cell_index=None,
        attempt=None,
        kernel_epoch=None,
    )

    with pytest.raises(
        RuntimeError,
        match=f"Blocking resource monitor {internal_id} stopped unexpectedly",
    ) as caught:
        await manager.wait_for_blocking_resources(timeout_seconds=2)

    assert isinstance(caught.value.__cause__, OSError)
    resource = store.get_resource(internal_id)
    assert resource is not None
    assert resource["terminal"] is False
    await manager.shutdown()
    store.close()


@pytest.mark.asyncio
async def test_failed_inspection_retries_from_last_committed_cursor(
    tmp_path: Path,
) -> None:
    FailOnceAfterCursorMutationAdapter.calls = 0
    FailOnceAfterCursorMutationAdapter.received_cursors = []
    store = build_store(tmp_path)
    manager = ResourceManager(
        store=store,
        bus=EventBus(store, "run"),
        run_id="run",
        working_dir=tmp_path,
        aws_settings=AwsSettings(poll_interval_seconds=0.001),
    )
    manager.register_adapter(FailOnceAfterCursorMutationAdapter)
    internal_id = await manager.register(
        ResourceEvent(
            resource=ResourceSpec(provider="fake", type="fail_once", id="stream"),
            lifecycle=ResourceLifecycle(monitor=False),
        ),
        cell_index=None,
        attempt=None,
        kernel_epoch=None,
    )
    store.save_resource_cursor(internal_id, {"token": "page-1"})
    active_cursor = store.resource_cursor(internal_id)

    error_count = await manager._monitor_cycle(  # noqa: SLF001
        internal_id, active_cursor, 0
    )
    assert error_count == 1
    assert active_cursor == {"token": "page-1"}
    assert store.resource_cursor(internal_id) == {"token": "page-1"}

    error_count = await manager._monitor_cycle(  # noqa: SLF001
        internal_id, active_cursor, error_count
    )
    assert error_count == 0
    assert FailOnceAfterCursorMutationAdapter.received_cursors == [
        {"token": "page-1"},
        {"token": "page-1"},
    ]
    resource = store.get_resource(internal_id)
    assert resource is not None
    assert resource["cursor"] == {"token": "page-2"}
    assert resource["log_tail"] == ["line-from-page-2"]
    await manager.shutdown()
    store.close()
