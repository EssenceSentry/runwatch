# pyright: reportPrivateUsage=false, reportUnknownArgumentType=false, reportUnknownMemberType=false
from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any, ClassVar

import pytest

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
from runwatch.resources.base import ResourceAdapter
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


def build_store(root: Path) -> RunStore:
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
            )
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
    await manager.shutdown()
    store.close()


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
    assert confirmed["cursor"] == {"recovery_inspections": 1}
    assert confirmed["log_tail"] == ["provider was already terminal"]
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
    store = build_store(tmp_path)
    manager = ResourceManager(
        store=store,
        bus=EventBus(store, "run"),
        run_id="run",
        working_dir=tmp_path,
        aws_settings=AwsSettings(),
    )
    manager.register_adapter(FakeStoppableAdapter)
    first = await manager.register(
        ResourceEvent(
            resource=ResourceSpec(
                provider="fake", type="job", id="job-attempt-1", logical_key="build"
            )
        ),
        cell_index=0,
        attempt=1,
        kernel_epoch=1,
    )
    second = await manager.register(
        ResourceEvent(
            resource=ResourceSpec(
                provider="fake", type="job", id="job-attempt-2", logical_key="build"
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
