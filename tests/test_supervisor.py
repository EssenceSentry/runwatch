# pyright: reportPrivateUsage=false
from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

import nbformat
import pytest

from runwatch.models import (
    ActionKind,
    ActionStatus,
    Ownership,
    ResourceDisposition,
    ResourceEvent,
    ResourceObservation,
    ResourceSpec,
    ResourceStatus,
    RunStatus,
    RunwatchConfig,
)
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

    assert calls[0] == "runtime"
    assert set(calls[1:3]) == {"notifications", "resources"}
    assert calls[3:] == ["controller", "store"]
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
    assert confirmed["monitor_closed"] is True
    first.store.update_run_status(
        first.run_id, crash_status, ended=crash_status.terminal
    )
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
        supervisor.store.update_run_status(
            supervisor.run_id, RunStatus.SUCCEEDED, ended=True
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
