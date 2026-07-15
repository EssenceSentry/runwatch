# pyright: reportPrivateUsage=false
from __future__ import annotations

import asyncio
import json
import os
import stat
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any, ClassVar

import nbformat
import pytest

from runwatch.models import (
    ActionKind,
    ActionStatus,
    Ownership,
    ResourceDisposition,
    ResourceEvent,
    ResourceLifecycle,
    ResourceObservation,
    ResourceSpec,
    ResourceStatus,
    RunStatus,
    RunwatchConfig,
)
from runwatch.resources.base import ResourceAdapter
from runwatch.storage import source_hash
from runwatch.supervisor import RunSupervisor


def build_supervisor(root: Path) -> RunSupervisor:
    notebook_path = root / "empty.ipynb"
    nbformat.write(nbformat.v4.new_notebook(), notebook_path)
    return RunSupervisor(
        notebook_path=notebook_path,
        output_path=root / "out.ipynb",
        working_dir=root,
        run_dir=root / "run",
        config=RunwatchConfig(),
    )


class SupervisorDelayedFinalDrainAdapter(ResourceAdapter):
    provider = "fake"
    resource_type = "supervisor_delayed_final_drain"
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
        if self.inspect_calls > 1:
            self.final_inspect_started.set()
            await self.release_final_inspect.wait()
            return ResourceObservation(
                status=ResourceStatus.COMPLETED,
                terminal=True,
                log_lines=["supervisor-final-drain"],
            )
        return ResourceObservation(
            status=ResourceStatus.COMPLETED,
            terminal=True,
            log_lines=["supervisor-terminal"],
        )


def manifest_payload(*, schema_version: object = 3) -> dict[str, Any]:
    return {
        "schema_version": schema_version,
        "run_id": "run-1",
        "name": "demo",
        "notebook_path": "/tmp/demo.ipynb",
        "source_path": "/tmp/run/source.ipynb",
        "output_path": "/tmp/out.ipynb",
        "working_dir": "/tmp",
        "cleanup_on_success": True,
        "config": RunwatchConfig().model_dump(mode="json"),
    }


def test_new_run_manifest_has_an_independent_v3_schema(tmp_path: Path) -> None:
    supervisor = build_supervisor(tmp_path)

    manifest = RunSupervisor.read_manifest(supervisor.run_dir)

    assert manifest["schema_version"] == 3
    assert manifest["config"]["schema_version"] == 2
    supervisor.store.close()


def test_legacy_v2_manifest_without_cleanup_policy_is_conservative(
    tmp_path: Path,
) -> None:
    payload = manifest_payload(schema_version=2)
    payload.pop("cleanup_on_success")
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    (run_dir / "run-manifest.json").write_text(json.dumps(payload), encoding="utf-8")

    manifest = RunSupervisor.read_manifest(run_dir)

    assert manifest["cleanup_on_success"] is False


def test_legacy_v2_manifest_upgrades_old_notification_policy(tmp_path: Path) -> None:
    payload = manifest_payload(schema_version=2)
    notifications = payload["config"]["notifications"]
    notifications.pop("allow_insecure_http")
    notifications["webhook_urls"] = [
        "http://hooks.example/legacy?token=OLD#ignored-fragment"
    ]
    notifications["ntfy_base_url"] = "http://ntfy.example/root#ignored"
    notifications["ntfy_topic"] = "runs"
    notifications["periodic_seconds"] = 5
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    (run_dir / "run-manifest.json").write_text(json.dumps(payload), encoding="utf-8")

    manifest = RunSupervisor.read_manifest(run_dir)

    upgraded = manifest["config"]["notifications"]
    assert upgraded["allow_insecure_http"] is True
    assert upgraded["periodic_seconds"] == 60
    assert upgraded["webhook_urls"] == ["http://hooks.example/legacy?token=OLD"]
    assert upgraded["ntfy_base_url"] == "http://ntfy.example/root"


@pytest.mark.parametrize(
    "notifications",
    [
        {
            "webhook_urls": ["http://hooks.example/current"],
            "periodic_seconds": 60,
        },
        {
            "webhook_urls": ["https://hooks.example/current#fragment"],
            "periodic_seconds": 60,
        },
        {
            "webhook_urls": ["https://hooks.example/current"],
            "periodic_seconds": 5,
        },
    ],
)
def test_v3_manifest_does_not_apply_legacy_notification_policy(
    tmp_path: Path, notifications: dict[str, object]
) -> None:
    payload = manifest_payload()
    payload["config"]["notifications"] = notifications
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    (run_dir / "run-manifest.json").write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(RuntimeError):
        RunSupervisor.read_manifest(run_dir)


@pytest.mark.parametrize("value", ["false", 0, 1, None])
def test_manifest_cleanup_policy_rejects_boolean_coercion(
    tmp_path: Path, value: object
) -> None:
    payload = manifest_payload()
    payload["cleanup_on_success"] = value
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    (run_dir / "run-manifest.json").write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(RuntimeError, match="cleanup_on_success"):
        RunSupervisor.read_manifest(run_dir)


def test_current_manifest_requires_explicit_cleanup_policy(tmp_path: Path) -> None:
    payload = manifest_payload()
    payload.pop("cleanup_on_success")
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    (run_dir / "run-manifest.json").write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(RuntimeError, match="cleanup_on_success"):
        RunSupervisor.read_manifest(run_dir)


@pytest.mark.parametrize("schema_version", ["3", 3.0, True, 4, None])
def test_manifest_schema_dispatch_rejects_coercion_and_unknown_versions(
    tmp_path: Path, schema_version: object
) -> None:
    payload = manifest_payload(schema_version=schema_version)
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    (run_dir / "run-manifest.json").write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(RuntimeError, match="Unsupported Runwatch manifest schema"):
        RunSupervisor.read_manifest(run_dir)


def test_manifest_rejects_unknown_fields(tmp_path: Path) -> None:
    payload = manifest_payload()
    payload["future_policy"] = True
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    (run_dir / "run-manifest.json").write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(RuntimeError, match="future_policy"):
        RunSupervisor.read_manifest(run_dir)


def test_run_state_is_created_with_private_permissions(tmp_path: Path) -> None:
    old_umask = os.umask(0o000)
    try:
        supervisor = build_supervisor(tmp_path)
    finally:
        os.umask(old_umask)

    assert stat.S_IMODE(supervisor.run_dir.stat().st_mode) == 0o700
    for path in (
        supervisor.input_snapshot_path,
        supervisor.source_path,
        supervisor.run_dir / "run-manifest.json",
        supervisor.store.path,
    ):
        assert stat.S_IMODE(path.stat().st_mode) == 0o600
    for suffix in ("-wal", "-shm"):
        sidecar = Path(f"{supervisor.store.path}{suffix}")
        if sidecar.exists():
            assert stat.S_IMODE(sidecar.stat().st_mode) == 0o600
    supervisor.store.close()


@pytest.mark.parametrize(
    "output_name",
    ["original", "input.ipynb", "source.ipynb", "executed.partial.ipynb"],
)
def test_output_path_cannot_overwrite_runwatch_owned_notebooks(
    tmp_path: Path, output_name: str
) -> None:
    notebook_path = tmp_path / "input-notebook.ipynb"
    nbformat.write(nbformat.v4.new_notebook(), notebook_path)
    run_dir = tmp_path / "run"
    output_path = notebook_path if output_name == "original" else run_dir / output_name

    with pytest.raises(ValueError, match="would overwrite"):
        RunSupervisor(
            notebook_path=notebook_path,
            output_path=output_path,
            working_dir=tmp_path,
            run_dir=run_dir,
            config=RunwatchConfig(),
        )

    assert not run_dir.exists()
    assert nbformat.read(notebook_path, as_version=4).cells == []


@pytest.mark.asyncio
async def test_start_rejects_a_closed_supervisor(tmp_path: Path) -> None:
    supervisor = build_supervisor(tmp_path)
    await supervisor.close()

    with pytest.raises(RuntimeError, match="Cannot start a closed supervisor"):
        await supervisor.start()


@pytest.mark.asyncio
async def test_start_rejects_a_second_start_even_after_runner_finishes(
    tmp_path: Path,
) -> None:
    supervisor = build_supervisor(tmp_path)
    await supervisor.start()
    assert (await supervisor.wait()).value == "succeeded"

    with pytest.raises(RuntimeError, match="already been started"):
        await supervisor.start()

    await supervisor.close()


@pytest.mark.asyncio
async def test_wait_marks_finalization_only_after_after_run_returns_normally(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    supervisor = build_supervisor(tmp_path)
    supervisor.store.finish_run(
        supervisor.run_id,
        RunStatus.SUCCEEDED,
        message="completed",
        event_type="run.succeeded",
        event_payload={"kernel_epoch": 0},
    )

    async def completed() -> RunStatus:
        return RunStatus.SUCCEEDED

    async def fail_after_run(status: RunStatus) -> None:
        assert status is RunStatus.SUCCEEDED
        raise RuntimeError("injected post-run failure")

    supervisor._runner_task = asyncio.create_task(completed())  # noqa: SLF001
    monkeypatch.setattr(supervisor, "_after_run", fail_after_run)

    with pytest.raises(RuntimeError, match="injected post-run failure"):
        await supervisor.wait()

    unfinished = supervisor.store.get_run(supervisor.run_id)
    assert unfinished["status"] == RunStatus.SUCCEEDED.value
    assert unfinished["finalization_complete"] is False
    assert unfinished["finalized_at"] is None
    assert supervisor.wait_completed_normally is False

    async def complete_after_run(status: RunStatus) -> None:
        assert status is RunStatus.SUCCEEDED

    monkeypatch.setattr(supervisor, "_after_run", complete_after_run)
    assert await supervisor.wait() is RunStatus.SUCCEEDED

    finalized = supervisor.store.get_run(supervisor.run_id)
    assert finalized["finalization_complete"] is True
    assert finalized["finalized_at"] is not None
    assert supervisor.wait_completed_normally is True
    await supervisor.close()


@pytest.mark.asyncio
async def test_wait_finalizes_only_after_blocking_resource_final_log_drain(
    tmp_path: Path,
) -> None:
    SupervisorDelayedFinalDrainAdapter.reset()
    supervisor = build_supervisor(tmp_path)
    supervisor.resources.register_adapter(SupervisorDelayedFinalDrainAdapter)
    internal_id = await supervisor.resources.register(
        ResourceEvent(
            resource=ResourceSpec(
                provider="fake",
                type="supervisor_delayed_final_drain",
                id="job",
            ),
            lifecycle=ResourceLifecycle(
                blocking=True,
                poll_interval_seconds=0.001,
                final_log_drain_seconds=0.001,
            ),
        ),
        cell_index=None,
        attempt=None,
        kernel_epoch=0,
    )

    async def complete_after_resources() -> RunStatus:
        outcome = await supervisor.runner._wait_for_blocking_resources()  # noqa: SLF001
        assert outcome is None
        event = supervisor.store.finish_run(
            supervisor.run_id,
            RunStatus.SUCCEEDED,
            message="completed after resource drain",
            event_type="run.succeeded",
            event_payload={"kernel_epoch": 0},
        )
        supervisor.bus.fan_out_persisted(event)
        return RunStatus.SUCCEEDED

    supervisor._runner_task = asyncio.create_task(  # noqa: SLF001
        complete_after_resources()
    )
    wait_task = asyncio.create_task(supervisor.wait())

    await SupervisorDelayedFinalDrainAdapter.final_inspect_started.wait()
    pending = supervisor.store.get_resource(internal_id)
    assert pending is not None
    assert pending["terminal"] is True
    assert pending["monitor_closed"] is False
    assert supervisor.store.get_run(supervisor.run_id)["finalization_complete"] is False
    await asyncio.sleep(0)
    assert not wait_task.done()

    SupervisorDelayedFinalDrainAdapter.release_final_inspect.set()
    assert await wait_task is RunStatus.SUCCEEDED
    settled = supervisor.store.get_resource(internal_id)
    assert settled is not None and settled["monitor_closed"] is True
    finalized = supervisor.store.get_run(supervisor.run_id)
    assert finalized["finalization_complete"] is True
    assert finalized["finalized_at"] is not None
    await supervisor.close()


@pytest.mark.asyncio
async def test_wait_stops_runner_and_surfaces_action_loop_exception(
    tmp_path: Path,
) -> None:
    supervisor = build_supervisor(tmp_path)
    supervisor.store.update_run_status(
        supervisor.run_id, RunStatus.RUNNING, started=True
    )
    cancelled = asyncio.Event()
    never = asyncio.Event()

    async def running() -> RunStatus:
        try:
            await never.wait()
        finally:
            cancelled.set()
        return RunStatus.SUCCEEDED

    async def failed_actions() -> None:
        await asyncio.sleep(0)
        raise ValueError("injected action-loop failure")

    supervisor._runner_task = asyncio.create_task(running())  # noqa: SLF001
    supervisor._action_task = asyncio.create_task(failed_actions())  # noqa: SLF001

    with pytest.raises(
        RuntimeError, match=r"action loop failed unexpectedly \(ValueError\)"
    ) as raised:
        await supervisor.wait()

    assert isinstance(raised.value.__cause__, ValueError)
    assert cancelled.is_set()
    assert supervisor._runner_task.cancelled()  # noqa: SLF001
    run = supervisor.store.get_run(supervisor.run_id)
    assert run["status"] == RunStatus.PAUSED.value
    assert run["finalization_complete"] is False
    assert supervisor.wait_completed_normally is False
    await supervisor.close()


@pytest.mark.asyncio
async def test_wait_stops_runner_when_action_loop_exits_normally(
    tmp_path: Path,
) -> None:
    supervisor = build_supervisor(tmp_path)
    supervisor.store.update_run_status(
        supervisor.run_id, RunStatus.RUNNING, started=True
    )
    cancelled = asyncio.Event()
    never = asyncio.Event()

    async def running() -> RunStatus:
        try:
            await never.wait()
        finally:
            cancelled.set()
        return RunStatus.SUCCEEDED

    async def exited_actions() -> None:
        return None

    supervisor._runner_task = asyncio.create_task(running())  # noqa: SLF001
    supervisor._action_task = asyncio.create_task(exited_actions())  # noqa: SLF001

    with pytest.raises(RuntimeError, match="action loop exited unexpectedly"):
        await supervisor.wait()

    assert cancelled.is_set()
    assert supervisor._runner_task.cancelled()  # noqa: SLF001
    run = supervisor.store.get_run(supervisor.run_id)
    assert run["status"] == RunStatus.PAUSED.value
    assert run["finalization_complete"] is False
    assert supervisor.wait_completed_normally is False
    await supervisor.close()


@pytest.mark.asyncio
async def test_wait_finalizes_normally_when_runner_finishes_first(
    tmp_path: Path,
) -> None:
    supervisor = build_supervisor(tmp_path)
    supervisor.store.finish_run(
        supervisor.run_id,
        RunStatus.SUCCEEDED,
        message="completed",
        event_type="run.succeeded",
        event_payload={"kernel_epoch": 0},
    )
    action_cancelled = asyncio.Event()
    never = asyncio.Event()

    async def completed() -> RunStatus:
        return RunStatus.SUCCEEDED

    async def running_actions() -> None:
        try:
            await never.wait()
        finally:
            action_cancelled.set()

    supervisor._runner_task = asyncio.create_task(completed())  # noqa: SLF001
    supervisor._action_task = asyncio.create_task(running_actions())  # noqa: SLF001

    assert await supervisor.wait() is RunStatus.SUCCEEDED
    assert not supervisor._action_task.done()  # noqa: SLF001
    run = supervisor.store.get_run(supervisor.run_id)
    assert run["finalization_complete"] is True
    assert supervisor.wait_completed_normally is True

    await supervisor.quiesce()
    assert action_cancelled.is_set()
    settled = supervisor.store.get_run(supervisor.run_id)
    assert settled["status"] == RunStatus.SUCCEEDED.value
    assert not any(
        event["type"] == "run.process_stopped"
        for event in supervisor.store.recent_events(supervisor.run_id)
    )
    await supervisor.close()


@pytest.mark.parametrize("raises", [False, True], ids=["normal-exit", "exception"])
@pytest.mark.asyncio
async def test_linger_health_failure_revokes_normal_wait_cleanup_gate(
    tmp_path: Path, raises: bool
) -> None:
    supervisor = build_supervisor(tmp_path)
    supervisor.store.finish_run(
        supervisor.run_id,
        RunStatus.SUCCEEDED,
        message="completed",
        event_type="run.succeeded",
        event_payload={"kernel_epoch": 0},
    )
    supervisor.store.mark_run_finalized(supervisor.run_id, RunStatus.SUCCEEDED)
    supervisor._wait_completed_normally = True  # noqa: SLF001

    async def terminate_actions() -> None:
        await asyncio.sleep(0)
        if raises:
            raise ValueError("injected linger failure")

    supervisor._action_task = asyncio.create_task(terminate_actions())  # noqa: SLF001

    expected = (
        r"action loop failed unexpectedly \(ValueError\)"
        if raises
        else "action loop exited unexpectedly"
    )
    with pytest.raises(RuntimeError, match=expected):
        await supervisor.wait_for_action_loop_failure()

    assert supervisor.wait_completed_normally is False
    assert supervisor.store.get_run(supervisor.run_id)["finalization_complete"] is True
    await supervisor.close()


@pytest.mark.asyncio
async def test_close_attempts_all_cleanup_after_multiple_failures(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    supervisor = build_supervisor(tmp_path)
    calls: list[str] = []
    stop_runtime = supervisor._stop_runtime_tasks  # noqa: SLF001
    close_notifications = supervisor.notifications.close
    shutdown_resources = supervisor.resources.shutdown
    clear_controller = supervisor._clear_controller_registration  # noqa: SLF001
    close_store = supervisor.store.close

    async def failing_async(name: str, cleanup: Callable[[], Awaitable[None]]) -> None:
        calls.append(name)
        await cleanup()
        raise RuntimeError(f"{name} failed")

    async def fail_runtime() -> None:
        await failing_async("runtime", stop_runtime)

    async def fail_notifications() -> None:
        await failing_async("notifications", close_notifications)

    async def fail_resources() -> None:
        await failing_async("resources", shutdown_resources)

    def fail_controller() -> None:
        calls.append("controller")
        clear_controller()
        raise RuntimeError("controller failed")

    def fail_store() -> None:
        calls.append("store")
        close_store()
        raise RuntimeError("store failed")

    monkeypatch.setattr(supervisor, "_stop_runtime_tasks", fail_runtime)
    monkeypatch.setattr(supervisor.notifications, "close", fail_notifications)
    monkeypatch.setattr(supervisor.resources, "shutdown", fail_resources)
    monkeypatch.setattr(supervisor, "_clear_controller_registration", fail_controller)
    monkeypatch.setattr(supervisor.store, "close", fail_store)

    with pytest.raises(RuntimeError, match="Runwatch cleanup failed") as raised:
        await supervisor.close()

    assert calls == ["runtime", "resources", "notifications", "controller", "store"]
    assert all(
        message in str(raised.value)
        for message in (
            "runtime failed",
            "notifications failed",
            "resources failed",
            "controller failed",
            "store failed",
        )
    )

    completed_calls = list(calls)
    await supervisor.close()
    assert calls == completed_calls


@pytest.mark.asyncio
async def test_quiesce_is_idempotent_and_keeps_notifications_and_store_open(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    supervisor = build_supervisor(tmp_path)
    shutdown_calls = 0
    original_shutdown = supervisor.resources.shutdown

    async def counted_shutdown() -> None:
        nonlocal shutdown_calls
        shutdown_calls += 1
        await original_shutdown()

    monkeypatch.setattr(supervisor.resources, "shutdown", counted_shutdown)

    await supervisor.quiesce()
    await supervisor.quiesce()

    assert shutdown_calls == 1
    assert supervisor.store.get_run(supervisor.run_id)["status"] == "created"
    assert not supervisor.notifications._client.is_closed
    await supervisor.close()
    assert supervisor.notifications._client.is_closed


@pytest.mark.asyncio
async def test_close_cancels_and_joins_kernel_escalation_before_store_close(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    supervisor = build_supervisor(tmp_path)
    store_closed = False
    joined = asyncio.Event()
    wait_forever = asyncio.Event()
    original_store_close = supervisor.store.close

    def tracked_store_close() -> None:
        nonlocal store_closed
        store_closed = True
        original_store_close()

    async def escalation() -> None:
        try:
            await wait_forever.wait()
        finally:
            assert not store_closed
            await supervisor.bus.publish("notebook.cancel_shutdown_joined", {})
            joined.set()

    monkeypatch.setattr(supervisor.store, "close", tracked_store_close)
    task = asyncio.create_task(escalation())
    supervisor.runner._cancel_escalation_task = task
    await asyncio.sleep(0)

    await supervisor.close()

    assert joined.is_set()
    assert task.cancelled()
    assert supervisor.runner._cancel_escalation_task is None
    assert store_closed
    await supervisor.runner.shutdown()


@pytest.mark.asyncio
async def test_only_one_live_recovery_command_is_queued_for_failed_cell(
    tmp_path: Path,
) -> None:
    supervisor = build_supervisor(tmp_path)
    supervisor.store.update_run_status(
        supervisor.run_id,
        RunStatus.PAUSED,
        current_cell_index=0,
        failed_cell_index=0,
        failed_attempt=1,
    )
    supervisor.runner._paused.set()  # noqa: SLF001
    first_id = supervisor.create_recovery_action(ActionKind.RESUME)
    second_id = supervisor.create_recovery_action(ActionKind.RESTART)
    first = supervisor.store.claim_action(first_id)
    assert first is not None
    await supervisor._dispatch_recovery_action(first)  # noqa: SLF001
    second = supervisor.store.claim_action(second_id)
    assert second is not None
    await supervisor._dispatch_recovery_action(second)  # noqa: SLF001

    assert supervisor.runner.command_queue.qsize() == 1
    rejected = supervisor.store.get_action(second_id)
    assert rejected is not None
    assert rejected["status"] == ActionStatus.REJECTED.value
    assert "already in flight" in rejected["message"]
    await supervisor.close()


@pytest.mark.asyncio
async def test_live_recovery_is_bound_to_failed_cell_identity(tmp_path: Path) -> None:
    supervisor = build_supervisor(tmp_path)
    supervisor.store.update_run_status(
        supervisor.run_id,
        RunStatus.PAUSED,
        current_cell_index=0,
        failed_cell_index=0,
        failed_attempt=1,
    )
    supervisor.runner._paused.set()  # noqa: SLF001
    action_id = supervisor.store.create_action(
        supervisor.run_id,
        ActionKind.RESUME,
        payload={"from_cell": 0, "failed_cell_index": 1},
        expected_kernel_epoch=0,
        expected_cell_attempt=1,
        expected_source_hash=source_hash(supervisor.source_path),
    )
    action = supervisor.store.claim_action(action_id)
    assert action is not None

    await supervisor._dispatch_recovery_action(action)  # noqa: SLF001

    rejected = supervisor.store.get_action(action_id)
    assert rejected is not None
    assert rejected["status"] == ActionStatus.REJECTED.value
    assert "identity changed" in rejected["message"]
    assert supervisor.runner.command_queue.empty()
    await supervisor.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("crash_status", [RunStatus.CANCELLING, RunStatus.CANCELLED])
async def test_recovered_confirmed_stop_cancels_without_restarting_notebook(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    crash_status: RunStatus,
) -> None:
    first = build_supervisor(tmp_path)
    event = ResourceEvent(
        resource=ResourceSpec(
            provider="fake",
            type="job",
            id="job",
            ownership=Ownership.EXCLUSIVE,
        )
    )
    resource_id, _created = first.store.register_resource(
        run_id=first.run_id,
        event=event,
        cell_index=None,
        attempt=None,
        kernel_epoch=0,
        supports_stop=True,
    )
    resource = first.store.get_resource(resource_id)
    assert resource is not None
    stop_action_id = first.store.create_action(
        first.run_id,
        ActionKind.STOP_RESOURCE,
        payload={
            "internal_id": resource_id,
            "expected_version": resource["version"],
        },
        expected_kernel_epoch=0,
    )
    assert first.store.claim_action(stop_action_id) is not None
    first.store.record_resource_stop_inspection(
        first.run_id,
        resource_id,
        ResourceObservation(
            status=ResourceStatus.STOPPED,
            terminal=True,
            log_lines=["final provider log"],
        ),
        {"next_token": "terminal"},
        ResourceDisposition.CANCELLED,
    )
    recovered_payload = {"payload": {"recovered": True}}
    assert first._recover_confirmed_stop(recovered_payload, resource_id)  # noqa: SLF001
    confirmed = first.store.get_resource(resource_id)
    assert confirmed is not None
    assert confirmed["cursor"] == {"next_token": "terminal"}
    assert confirmed["log_tail"] == ["final provider log"]
    assert confirmed["monitor_closed"] is False
    if crash_status.terminal:
        first.store.finish_run(
            first.run_id,
            crash_status,
            message="Cancellation confirmed before controller crash",
            event_type="run.cancelled",
            event_payload={"kernel_epoch": 0},
        )
    else:
        first.store.update_run_status(first.run_id, crash_status)
    bootstrap_id = first.store.create_action(
        first.run_id,
        ActionKind.RESTART,
        payload={"from_cell": 0, "failed_cell_index": None},
        expected_kernel_epoch=0,
        expected_cell_attempt=None,
        expected_source_hash=source_hash(first.source_path),
    )
    first.store.close()

    reopened = RunSupervisor.reopen(first.run_dir, bootstrap_action_id=bootstrap_id)

    async def forbidden_run() -> RunStatus:
        raise AssertionError("recovered cancellation must not restart the notebook")

    async def forbidden_provider_stop(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("confirmed stop recovery must not reissue provider stop")

    monkeypatch.setattr(reopened.runner, "run", forbidden_run)
    monkeypatch.setattr(
        reopened.resources,
        "stop_resource",
        forbidden_provider_stop,
    )
    await reopened.start()
    status = await reopened.wait()

    assert status is RunStatus.CANCELLED
    run = reopened.store.get_run(reopened.run_id)
    assert run["status"] == RunStatus.CANCELLED.value
    assert run["kernel_epoch"] == 0
    action = reopened.store.get_action(stop_action_id)
    assert action is not None and action["status"] == ActionStatus.COMPLETED.value
    await reopened.close()


@pytest.mark.asyncio
async def test_terminal_run_wins_race_with_provider_stop(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    supervisor = build_supervisor(tmp_path)
    action_id = supervisor.store.create_action(
        supervisor.run_id,
        ActionKind.STOP_RESOURCE,
        payload={"internal_id": "job", "expected_version": 1},
        expected_kernel_epoch=0,
    )
    action = supervisor.store.claim_action(action_id)
    assert action is not None
    cascaded = False

    async def stop_resource(
        internal_id: str,
        *,
        expected_version: int | None,
        on_stop_accepted: Any,
        allow_stopping: bool,
    ) -> None:
        assert internal_id == "job"
        supervisor.store.finish_run(
            supervisor.run_id,
            RunStatus.SUCCEEDED,
            message="completed during stop race",
            event_type="run.succeeded",
            event_payload={"kernel_epoch": 0},
        )
        await on_stop_accepted()

    async def stop_cancel_resources() -> list[str]:
        nonlocal cascaded
        cascaded = True
        return []

    monkeypatch.setattr(supervisor.resources, "stop_resource", stop_resource)
    monkeypatch.setattr(
        supervisor.resources, "stop_cancel_resources", stop_cancel_resources
    )

    await supervisor._dispatch_stop_action(action)  # noqa: SLF001

    run = supervisor.store.get_run(supervisor.run_id)
    completed = supervisor.store.get_action(action_id)
    assert run["status"] == RunStatus.SUCCEEDED.value
    assert completed is not None
    assert completed["status"] == ActionStatus.COMPLETED.value
    assert completed["result"]["final_run_status"] == RunStatus.SUCCEEDED.value
    assert completed["result"]["cancellation_requested"] is False
    assert cascaded is False
    await supervisor.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("outcome", "failed_event_type", "expected_status"),
    [
        ("completed", "action.completed", ActionStatus.COMPLETED),
        ("rejected", "action.rejected", ActionStatus.REJECTED),
    ],
)
async def test_action_diagnostic_publish_failure_does_not_stop_later_actions(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    outcome: str,
    failed_event_type: str,
    expected_status: ActionStatus,
) -> None:
    supervisor = build_supervisor(tmp_path)
    if outcome == "completed":
        action_ids = [
            supervisor.store.create_action(
                supervisor.run_id,
                ActionKind.STOP_RESOURCE,
                payload={"internal_id": f"job-{index}", "expected_version": 1},
                expected_kernel_epoch=0,
            )
            for index in range(2)
        ]

        async def complete(action: dict[str, Any]) -> None:
            await supervisor._complete_stop_action(  # noqa: SLF001
                action, [str(action["payload"]["internal_id"])]
            )

        monkeypatch.setattr(supervisor, "_dispatch_stop_action", complete)
    else:
        action_ids = [
            supervisor.create_recovery_action(ActionKind.RESUME) for _ in range(2)
        ]

    original_publish = supervisor.bus.publish
    failed_once = False

    async def flaky_publish(event_type: str, payload: dict[str, Any]) -> dict[str, Any]:
        nonlocal failed_once
        if event_type == failed_event_type and not failed_once:
            failed_once = True
            raise RuntimeError("injected diagnostic persistence failure")
        return await original_publish(event_type, payload)

    monkeypatch.setattr(supervisor.bus, "publish", flaky_publish)
    action_task = asyncio.create_task(supervisor._action_loop())  # noqa: SLF001
    try:
        for _ in range(200):
            actions = [
                supervisor.store.get_action(action_id) for action_id in action_ids
            ]
            if all(
                action is not None and ActionStatus(action["status"]).terminal
                for action in actions
            ):
                break
            await asyncio.sleep(0.005)

        actions = [supervisor.store.get_action(action_id) for action_id in action_ids]
        assert failed_once
        assert [action["status"] for action in actions if action is not None] == [
            expected_status.value,
            expected_status.value,
        ]
        assert not action_task.done()
        assert any(
            event["type"] == failed_event_type
            and event["payload"]["action_id"] == action_ids[1]
            for event in supervisor.store.recent_events(supervisor.run_id)
        )
    finally:
        action_task.cancel()
        await asyncio.gather(action_task, return_exceptions=True)
        await supervisor.close()


@pytest.mark.asyncio
async def test_post_terminal_dispatch_error_preserves_action_and_loop(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    supervisor = build_supervisor(tmp_path)
    action_ids = [
        supervisor.store.create_action(
            supervisor.run_id,
            ActionKind.STOP_RESOURCE,
            payload={"internal_id": f"job-{index}", "expected_version": 1},
            expected_kernel_epoch=0,
        )
        for index in range(2)
    ]

    async def finish_then_maybe_fail(action: dict[str, Any]) -> None:
        supervisor.store.finish_action(
            action["action_id"], ActionStatus.COMPLETED, message="provider stopped"
        )
        if action["action_id"] == action_ids[0]:
            raise RuntimeError("failure after terminal transition")

    monkeypatch.setattr(supervisor, "_dispatch_stop_action", finish_then_maybe_fail)
    action_task = asyncio.create_task(supervisor._action_loop())  # noqa: SLF001
    try:
        for _ in range(200):
            second = supervisor.store.get_action(action_ids[1])
            if second is not None and ActionStatus(second["status"]).terminal:
                break
            await asyncio.sleep(0.005)

        first = supervisor.store.get_action(action_ids[0])
        second = supervisor.store.get_action(action_ids[1])
        assert first is not None and first["status"] == ActionStatus.COMPLETED.value
        assert second is not None and second["status"] == ActionStatus.COMPLETED.value
        assert not action_task.done()
        assert any(
            event["type"] == "action.post_terminal_error"
            and event["payload"]["action_id"] == action_ids[0]
            for event in supervisor.store.recent_events(supervisor.run_id)
        )
    finally:
        action_task.cancel()
        await asyncio.gather(action_task, return_exceptions=True)
        await supervisor.close()
