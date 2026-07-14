# pyright: reportMissingTypeArgument=false, reportPrivateUsage=false, reportUnknownArgumentType=false, reportUnknownParameterType=false
from __future__ import annotations

import asyncio
import stat
import threading
from collections.abc import Callable
from pathlib import Path
from typing import Any

import nbformat
import pytest

import runwatch.notebook as notebook_module
from runwatch.models import (
    ActionKind,
    ActionStatus,
    NotebookSettings,
    RunnerCommand,
    RunStatus,
    RunwatchConfig,
)
from runwatch.notebook import write_notebook_atomic
from runwatch.storage import RunStore, source_hash
from runwatch.supervisor import RunSupervisor


def write_notebook(path: Path, sources: list[str]) -> None:
    notebook = nbformat.v4.new_notebook(
        cells=[nbformat.v4.new_code_cell(source) for source in sources],
        metadata={
            "kernelspec": {
                "name": "python3",
                "display_name": "Python 3",
                "language": "python",
            }
        },
    )
    nbformat.write(notebook, path)


def config() -> RunwatchConfig:
    return RunwatchConfig(
        notebook=NotebookSettings(
            kernel_name="python3",
            checkpoint_interval_seconds=0.05,
            wait_for_blocking_resources=False,
        )
    )


def timeout_config() -> RunwatchConfig:
    return RunwatchConfig(
        notebook=NotebookSettings(
            kernel_name="python3",
            timeout_seconds=1,
            startup_timeout_seconds=5,
            checkpoint_interval_seconds=0.05,
            wait_for_blocking_resources=False,
        )
    )


def immediate_cancel_config() -> RunwatchConfig:
    return RunwatchConfig(
        notebook=NotebookSettings(
            kernel_name="python3",
            checkpoint_interval_seconds=0.05,
            cancel_interrupt_grace_seconds=0,
            cancel_shutdown_grace_seconds=0,
            cancel_terminate_grace_seconds=0,
            cancel_kill_grace_seconds=0,
            capture_tqdm=False,
            wait_for_blocking_resources=False,
        )
    )


def test_atomic_notebook_write_preserves_mode_and_defaults_to_private(
    tmp_path: Path,
) -> None:
    existing = tmp_path / "existing.ipynb"
    write_notebook(existing, ["print('before')"])
    existing.chmod(0o600)
    replacement = nbformat.v4.new_notebook(
        cells=[nbformat.v4.new_code_cell("print('after')")]
    )

    write_notebook_atomic(replacement, existing)

    assert stat.S_IMODE(existing.stat().st_mode) == 0o600
    assert nbformat.read(existing, as_version=4).cells[0].source == "print('after')"

    created = tmp_path / "created.ipynb"
    write_notebook_atomic(replacement, created)
    assert stat.S_IMODE(created.stat().st_mode) == 0o600


@pytest.mark.asyncio
async def test_failed_cell_can_be_edited_with_nbformat_and_resumed(
    tmp_path: Path,
) -> None:
    notebook_path = tmp_path / "failure.ipynb"
    write_notebook(notebook_path, ["x = 2", "y = x + missing", "print(y * 3)"])
    supervisor = RunSupervisor(
        notebook_path=notebook_path,
        output_path=tmp_path / "executed.ipynb",
        working_dir=tmp_path,
        run_dir=tmp_path / "run",
        config=config(),
    )
    await supervisor.start()
    await asyncio.wait_for(supervisor.runner.wait_until_paused(), timeout=20)
    source = nbformat.read(supervisor.source_path, as_version=4)
    source.cells[1].source = "y = x + 4"
    nbformat.write(source, supervisor.source_path)
    action_id = supervisor.create_recovery_action(ActionKind.RESUME)
    status = await asyncio.wait_for(supervisor.wait(), timeout=20)
    assert status.value == "succeeded"
    assert supervisor.store.get_action(action_id)["status"] == "completed"  # type: ignore[index]
    executed = nbformat.read(supervisor.output_path, as_version=4)
    assert executed.cells[2].outputs[0].text.strip() == "18"
    updated = nbformat.read(notebook_path, as_version=4)
    assert updated.cells[1].source == "y = x + 4"
    assert updated.cells[2].outputs[0].text.strip() == "18"
    await supervisor.close()


@pytest.mark.asyncio
async def test_settled_cell_outputs_are_written_back_before_recovery(
    tmp_path: Path,
) -> None:
    notebook_path = tmp_path / "failure.ipynb"
    write_notebook(
        notebook_path,
        ["print('first')", "raise RuntimeError('stop')"],
    )
    supervisor = RunSupervisor(
        notebook_path=notebook_path,
        output_path=tmp_path / "executed.ipynb",
        working_dir=tmp_path,
        run_dir=tmp_path / "run",
        config=config(),
    )
    await supervisor.start()
    await asyncio.wait_for(supervisor.runner.wait_until_paused(), timeout=20)

    deadline = asyncio.get_running_loop().time() + 5
    updated = nbformat.read(notebook_path, as_version=4)
    while not updated.cells[1].outputs:
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError("Failed-cell output was not written back")
        await asyncio.sleep(0.05)
        updated = nbformat.read(notebook_path, as_version=4)

    assert updated.cells[0].outputs[0].text.strip() == "first"
    assert updated.cells[1].outputs[-1].output_type == "error"
    assert updated.cells[1].outputs[-1].ename == "RuntimeError"
    await supervisor.close()


@pytest.mark.asyncio
async def test_external_notebook_edit_prevents_writeback_and_retains_checkpoint(
    tmp_path: Path,
) -> None:
    notebook_path = tmp_path / "input.ipynb"
    write_notebook(notebook_path, ["print('runwatch source')"])
    supervisor = RunSupervisor(
        notebook_path=notebook_path,
        output_path=tmp_path / "executed.ipynb",
        working_dir=tmp_path,
        run_dir=tmp_path / "run",
        config=config(),
    )
    externally_edited = nbformat.read(notebook_path, as_version=4)
    externally_edited.cells[0].source = "print('external edit')"
    nbformat.write(externally_edited, notebook_path)

    await supervisor.start()
    status = await asyncio.wait_for(supervisor.wait(), timeout=20)

    assert status.value == "failed"
    assert (
        "refusing to overwrite"
        in supervisor.store.get_run(supervisor.run_id)["message"]
    )
    unchanged = nbformat.read(notebook_path, as_version=4)
    assert unchanged.cells[0].source == "print('external edit')"
    checkpoint = nbformat.read(supervisor.partial_output_path, as_version=4)
    assert checkpoint.cells[0].outputs[0].text.strip() == "runwatch source"
    await supervisor.close()


@pytest.mark.asyncio
async def test_checkpoint_generation_preserves_request_arriving_during_worker_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    notebook_path = tmp_path / "input.ipynb"
    write_notebook(notebook_path, ["print('checkpoint')"])
    supervisor = RunSupervisor(
        notebook_path=notebook_path,
        output_path=tmp_path / "executed.ipynb",
        working_dir=tmp_path,
        run_dir=tmp_path / "run",
        config=config(),
    )
    runner = supervisor.runner
    first_write_started = threading.Event()
    release_first_write = threading.Event()
    snapshots: list[bytes] = []
    original_write = runner._write_notebook_bytes_atomic

    def controlled_write(snapshot: bytes, path: Path) -> None:
        snapshots.append(snapshot)
        if len(snapshots) == 1:
            first_write_started.set()
            assert release_first_write.wait(timeout=5)
        original_write(snapshot, path)

    monkeypatch.setattr(runner, "_write_notebook_bytes_atomic", controlled_write)
    runner.notebook.cells[0].outputs = [
        nbformat.v4.new_output("stream", name="stdout", text="first\n")
    ]
    runner._request_checkpoint()
    first_checkpoint = asyncio.create_task(runner._checkpoint(force=False))
    assert await asyncio.to_thread(first_write_started.wait, 2)

    runner.notebook.cells[0].outputs.append(
        nbformat.v4.new_output("stream", name="stdout", text="second\n")
    )
    runner._request_checkpoint()
    release_first_write.set()
    await first_checkpoint

    first_snapshot = nbformat.reads(snapshots[0].decode("utf-8"), as_version=4)
    assert [output.text for output in first_snapshot.cells[0].outputs] == ["first\n"]
    assert runner._checkpoint_persisted_generation == 1
    assert runner._checkpoint_requested_generation == 2
    assert runner._checkpoint_event.is_set()

    await runner._checkpoint(force=False)
    persisted = nbformat.read(runner.partial_output_path, as_version=4)
    assert [output.text for output in persisted.cells[0].outputs] == [
        "first\n",
        "second\n",
    ]
    assert runner._checkpoint_persisted_generation == 2
    assert not runner._checkpoint_event.is_set()
    await supervisor.close()


@pytest.mark.asyncio
async def test_periodic_checkpoint_retries_transient_failure_and_records_recovery(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    notebook_path = tmp_path / "input.ipynb"
    write_notebook(notebook_path, ["print('checkpoint')"])
    supervisor = RunSupervisor(
        notebook_path=notebook_path,
        output_path=tmp_path / "executed.ipynb",
        working_dir=tmp_path,
        run_dir=tmp_path / "run",
        config=config(),
    )
    runner = supervisor.runner
    attempts = 0
    original_write = runner._write_notebook_bytes_atomic

    def flaky_write(snapshot: bytes, path: Path) -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise OSError("injected checkpoint failure")
        original_write(snapshot, path)

    monkeypatch.setattr(runner, "_write_notebook_bytes_atomic", flaky_write)
    runner._checkpoint_task = asyncio.create_task(runner._checkpoint_loop())
    runner._request_checkpoint()
    deadline = asyncio.get_running_loop().time() + 5
    while not runner.partial_output_path.exists():
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError("checkpoint worker did not recover")
        await asyncio.sleep(0.02)

    event_types = {
        event["type"] for event in supervisor.store.recent_events(supervisor.run_id)
    }
    assert attempts >= 2
    assert "notebook.checkpoint_failed" in event_types
    assert "notebook.checkpoint_recovered" in event_types
    assert runner._checkpoint_task is not None
    assert not runner._checkpoint_task.done()
    runner._checkpoint_task.cancel()
    await asyncio.gather(runner._checkpoint_task, return_exceptions=True)
    runner._checkpoint_task = None
    await supervisor.close()


@pytest.mark.asyncio
async def test_writeback_rechecks_original_immediately_before_replace(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    notebook_path = tmp_path / "input.ipynb"
    write_notebook(notebook_path, ["print('runwatch')"])
    supervisor = RunSupervisor(
        notebook_path=notebook_path,
        output_path=tmp_path / "executed.ipynb",
        working_dir=tmp_path,
        run_dir=tmp_path / "run",
        config=config(),
    )
    runner = supervisor.runner
    runner.notebook.cells[0].outputs = [
        nbformat.v4.new_output("stream", name="stdout", text="executed\n")
    ]
    real_atomic_write = notebook_module.atomic_write_bytes
    injected = False

    def inject_external_save(
        path: Path,
        data: bytes,
        *,
        preserve_mode: bool = True,
        mode: int = 0o600,
        before_replace: Callable[[], None] | None = None,
    ) -> None:
        nonlocal injected
        if path == notebook_path and before_replace is not None and not injected:
            injected = True
            external = nbformat.v4.new_notebook(
                cells=[nbformat.v4.new_code_cell("print('external')")]
            )
            real_atomic_write(path, nbformat.writes(external).encode("utf-8"))
        real_atomic_write(
            path,
            data,
            preserve_mode=preserve_mode,
            mode=mode,
            before_replace=before_replace,
        )

    monkeypatch.setattr(notebook_module, "atomic_write_bytes", inject_external_save)
    with pytest.raises(
        RuntimeError, match="changed while Runwatch prepared write-back"
    ):
        await runner._checkpoint(force=True, writeback=True)

    external = nbformat.read(notebook_path, as_version=4)
    assert external.cells[0].source == "print('external')"
    checkpoint = nbformat.read(runner.partial_output_path, as_version=4)
    assert checkpoint.cells[0].outputs[0].text == "executed\n"
    await supervisor.close()


@pytest.mark.asyncio
async def test_timed_out_cell_is_paused_and_resumes_in_synchronized_kernel(
    tmp_path: Path,
) -> None:
    notebook_path = tmp_path / "timeout.ipynb"
    write_notebook(
        notebook_path,
        [
            "x = 2",
            "import time\ntime.sleep(5)\ny = x + 4",
            "print(y * 3)",
        ],
    )
    supervisor = RunSupervisor(
        notebook_path=notebook_path,
        output_path=tmp_path / "executed.ipynb",
        working_dir=tmp_path,
        run_dir=tmp_path / "run",
        config=timeout_config(),
    )
    await supervisor.start()
    await asyncio.wait_for(supervisor.runner.wait_until_paused(), timeout=20)

    paused = supervisor.snapshot()
    assert paused["run"]["status"] == "paused"
    assert paused["run"]["kernel_epoch"] == 1
    assert paused["cells"][1]["status"] == "failed"
    assert paused["cells"][1]["error_name"] == "CellTimeoutError"
    assert "timed out" in paused["cells"][1]["error_value"]

    source = nbformat.read(supervisor.source_path, as_version=4)
    source.cells[1].source = "y = x + 4"
    nbformat.write(source, supervisor.source_path)
    action_id = supervisor.create_recovery_action(ActionKind.RESUME)

    status = await asyncio.wait_for(supervisor.wait(), timeout=20)
    assert status.value == "succeeded"
    assert supervisor.store.get_action(action_id)["status"] == "completed"  # type: ignore[index]
    assert supervisor.snapshot()["run"]["kernel_epoch"] == 1
    executed = nbformat.read(supervisor.output_path, as_version=4)
    assert executed.cells[2].outputs[0].text.strip() == "18"
    await supervisor.close()


@pytest.mark.asyncio
async def test_high_volume_cell_output_is_coalesced_and_not_event_persisted(
    tmp_path: Path,
) -> None:
    notebook_path = tmp_path / "output.ipynb"
    write_notebook(notebook_path, ["pass"])
    supervisor = RunSupervisor(
        notebook_path=notebook_path,
        output_path=tmp_path / "executed.ipynb",
        working_dir=tmp_path,
        run_dir=tmp_path / "run",
        config=config(),
    )

    async with supervisor.bus.subscribe() as queue:
        for index in range(100):
            output = nbformat.v4.new_output(
                "stream", name="stdout", text=f"line {index}\n"
            )
            supervisor.runner._on_output(output, 0, 1)
        await supervisor.runner._drain_output_tasks()
        event = await asyncio.wait_for(queue.get(), timeout=1)
        supervisor.runner._on_output(
            nbformat.v4.new_output("stream", name="stdout", text="later\n"), 0, 1
        )
        delayed_event = await asyncio.wait_for(queue.get(), timeout=1)

    cell = supervisor.snapshot()["cells"][0]
    assert len(cell["output_tail"]) == 2
    assert cell["output_tail"][0]["coalesced_messages"] == 100
    assert cell["output_tail"][0]["text"].endswith("line 99\n")
    assert cell["output_tail"][1]["text"] == "later\n"
    assert event["type"] == "cell.output"
    assert event["seq"] is None
    assert delayed_event["type"] == "cell.output"
    assert delayed_event["seq"] is None
    assert not any(
        item["type"] == "cell.output"
        for item in supervisor.store.recent_events(supervisor.run_id)
    )
    await supervisor.close()


@pytest.mark.asyncio
async def test_completed_output_side_effect_failure_is_not_silently_lost(
    tmp_path: Path,
) -> None:
    notebook_path = tmp_path / "output.ipynb"
    write_notebook(notebook_path, ["pass"])
    supervisor = RunSupervisor(
        notebook_path=notebook_path,
        output_path=tmp_path / "executed.ipynb",
        working_dir=tmp_path,
        run_dir=tmp_path / "run",
        config=config(),
    )

    async def fail() -> None:
        raise OSError("disk unavailable")

    supervisor.runner._schedule(fail())
    await asyncio.sleep(0)
    with pytest.raises(RuntimeError, match="disk unavailable"):
        await supervisor.runner._drain_output_tasks()
    await supervisor.close()


@pytest.mark.asyncio
async def test_change_to_executed_cell_requires_restart(tmp_path: Path) -> None:
    notebook_path = tmp_path / "failure.ipynb"
    write_notebook(notebook_path, ["x = 2", "raise ValueError('stop')"])
    supervisor = RunSupervisor(
        notebook_path=notebook_path,
        output_path=tmp_path / "executed.ipynb",
        working_dir=tmp_path,
        run_dir=tmp_path / "run",
        config=config(),
    )
    await supervisor.start()
    await asyncio.wait_for(supervisor.runner.wait_until_paused(), timeout=20)
    source = nbformat.read(supervisor.source_path, as_version=4)
    source.cells[0].source = "x = 3"
    nbformat.write(source, supervisor.source_path)
    action_id = supervisor.create_recovery_action(ActionKind.RESUME)
    deadline = asyncio.get_running_loop().time() + 5
    action: dict[str, Any] | None = None
    while asyncio.get_running_loop().time() < deadline:
        action = supervisor.store.get_action(action_id)
        if action and action["status"] == "rejected":
            break
        await asyncio.sleep(0.05)
    assert action and "restart is required" in action["message"]
    await supervisor.close()


@pytest.mark.asyncio
async def test_stopped_process_can_reopen_and_replay_from_selected_cell(
    tmp_path: Path,
) -> None:
    notebook_path = tmp_path / "failure.ipynb"
    write_notebook(notebook_path, ["ignored = missing", "print('replayed')"])
    first = RunSupervisor(
        notebook_path=notebook_path,
        output_path=tmp_path / "executed.ipynb",
        working_dir=tmp_path,
        run_dir=tmp_path / "run",
        config=config(),
    )
    await first.start()
    await asyncio.wait_for(first.runner.wait_until_paused(), timeout=20)
    await first.close()
    recovery_store = RunStore(tmp_path / "run" / "runwatch.sqlite3")
    run = recovery_store.get_run(first.run_id)
    action_id = recovery_store.create_action(
        first.run_id,
        ActionKind.RESTART,
        payload={
            "from_cell": 1,
            "failed_cell_index": run.get("failed_cell_index"),
            "offline_recovery": True,
        },
        expected_kernel_epoch=run["kernel_epoch"],
        expected_cell_attempt=run.get("failed_attempt"),
        expected_source_hash=source_hash(tmp_path / "run" / "source.ipynb"),
    )
    recovery_store.close()
    reopened = RunSupervisor.reopen(
        tmp_path / "run", from_cell=1, bootstrap_action_id=action_id
    )
    await reopened.start()
    status = await asyncio.wait_for(reopened.wait(), timeout=20)
    assert status.value == "succeeded"
    snapshot = reopened.snapshot()
    assert snapshot["run"]["kernel_epoch"] == 2
    assert snapshot["cells"][0]["status"] == "not_replayed"
    assert reopened.store.get_action(action_id)["status"] == "completed"  # type: ignore[index]
    await reopened.close()


@pytest.mark.asyncio
async def test_cancellation_interrupts_paused_notebook_and_finishes_cancelled(
    tmp_path: Path,
) -> None:
    notebook_path = tmp_path / "failure.ipynb"
    write_notebook(notebook_path, ["raise RuntimeError('pause')"])
    supervisor = RunSupervisor(
        notebook_path=notebook_path,
        output_path=tmp_path / "executed.ipynb",
        working_dir=tmp_path,
        run_dir=tmp_path / "run",
        config=config(),
    )
    await supervisor.start()
    await asyncio.wait_for(supervisor.runner.wait_until_paused(), timeout=20)

    await supervisor.runner.cancel()
    status = await asyncio.wait_for(supervisor.wait(), timeout=20)

    assert status.value == "cancelled"
    assert supervisor.snapshot()["run"]["status"] == "cancelled"
    await supervisor.close()


@pytest.mark.asyncio
async def test_cancellation_retries_when_durable_status_update_fails_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    notebook_path = tmp_path / "cancel.ipynb"
    write_notebook(notebook_path, ["pass"])
    supervisor = RunSupervisor(
        notebook_path=notebook_path,
        output_path=tmp_path / "executed.ipynb",
        working_dir=tmp_path,
        run_dir=tmp_path / "run",
        config=config(),
    )
    runner = supervisor.runner
    original_update = supervisor.store.update_run_status
    attempts = 0

    def fail_once(run_id: str, status: RunStatus, **kwargs: Any) -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise OSError("injected status persistence failure")
        original_update(run_id, status, **kwargs)

    monkeypatch.setattr(supervisor.store, "update_run_status", fail_once)
    with pytest.raises(OSError, match="injected status persistence failure"):
        await runner.cancel()
    assert not runner._cancel_requested.is_set()
    assert supervisor.store.get_run(supervisor.run_id)["status"] == "created"

    await runner.cancel()
    assert attempts == 2
    assert runner._cancel_requested.is_set()
    assert supervisor.store.get_run(supervisor.run_id)["status"] == "cancelling"
    await supervisor.close()


@pytest.mark.asyncio
async def test_cancellation_interrupts_active_cell_and_persists_terminal_state(
    tmp_path: Path,
) -> None:
    notebook_path = tmp_path / "active.ipynb"
    write_notebook(notebook_path, ["import time\ntime.sleep(60)"])
    supervisor = RunSupervisor(
        notebook_path=notebook_path,
        output_path=tmp_path / "executed.ipynb",
        working_dir=tmp_path,
        run_dir=tmp_path / "run",
        config=config(),
    )
    await supervisor.start()
    deadline = asyncio.get_running_loop().time() + 20
    while not supervisor.runner._running_cell:
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError("notebook cell did not start")
        await asyncio.sleep(0.01)

    await asyncio.wait_for(supervisor.runner.cancel(), timeout=20)
    status = await asyncio.wait_for(supervisor.wait(), timeout=20)

    assert status.value == "cancelled"
    snapshot = supervisor.snapshot()
    assert snapshot["run"]["status"] == "cancelled"
    assert snapshot["cells"][0]["status"] == "interrupted"
    cancelled = next(
        event
        for event in supervisor.store.recent_events(supervisor.run_id)
        if event["type"] == "run.cancelled"
    )
    assert cancelled["payload"]["kernel_state_lost"] is False
    assert not any(
        event["type"] == "notebook.kernel_state_lost"
        for event in supervisor.store.recent_events(supervisor.run_id)
    )
    await supervisor.close()


@pytest.mark.asyncio
async def test_cancellation_escalates_and_abandons_unresponsive_cell_without_hanging(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    notebook_path = tmp_path / "cancel.ipynb"
    write_notebook(notebook_path, ["while True: pass"])
    supervisor = RunSupervisor(
        notebook_path=notebook_path,
        output_path=tmp_path / "executed.ipynb",
        working_dir=tmp_path,
        run_dir=tmp_path / "run",
        config=immediate_cancel_config(),
    )
    calls: list[str] = []

    class FakeProvisioner:
        async def terminate(self, *, restart: bool = False) -> None:
            assert restart is False
            calls.append("terminate")

        async def kill(self, *, restart: bool = False) -> None:
            assert restart is False
            calls.append("kill")

    class FakeKernelManager:
        provisioner = FakeProvisioner()

        async def interrupt_kernel(self) -> None:
            calls.append("interrupt")

        async def shutdown_kernel(
            self, *, now: bool = False, restart: bool = False
        ) -> None:
            assert restart is False
            calls.append("kill-shutdown" if now else "shutdown")

    class UnresponsiveClient:
        km = FakeKernelManager()
        kc = object()
        code_cells_executed = 0
        current_attempt = 0

        class KernelContext:
            async def __aenter__(self) -> None:
                return None

            async def __aexit__(self, *args: Any) -> None:
                del args

        def reset_execution_trackers(self) -> None:
            self.code_cells_executed = 0

        def async_setup_kernel(self, *, cwd: str) -> KernelContext:
            del cwd
            return self.KernelContext()

        async def async_execute_cell(
            self,
            cell: Any,
            index: int,
            *,
            execution_count: int,
        ) -> Any:
            del cell, index, execution_count
            await asyncio.Event().wait()

    runner = supervisor.runner
    client = UnresponsiveClient()
    monkeypatch.setattr(runner, "_make_client", lambda: client)
    await supervisor.start()
    deadline = asyncio.get_running_loop().time() + 2
    while not runner._cell_execution_active():
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError("fake cell did not start")
        await asyncio.sleep(0)

    await asyncio.wait_for(runner.cancel(), timeout=2)
    assert await asyncio.wait_for(supervisor.wait(), timeout=2) is RunStatus.CANCELLED
    assert calls == ["interrupt", "shutdown", "terminate", "kill"]
    cell = supervisor.snapshot()["cells"][0]
    assert cell["status"] == "interrupted"
    assert cell["error_name"] == "_CancellationEscalated"
    run = supervisor.store.get_run(supervisor.run_id)
    assert run["status"] == RunStatus.CANCELLED.value
    assert "kernel state was lost" in run["message"]
    events = supervisor.store.recent_events(supervisor.run_id)
    event_types = {event["type"] for event in events}
    assert "notebook.interrupt_requested" in event_types
    assert "notebook.kernel_state_lost" in event_types
    assert "notebook.cancel_execution_abandoned" in event_types
    assert "cell.interrupted" in event_types
    state_lost = next(
        event for event in events if event["type"] == "notebook.kernel_state_lost"
    )
    assert state_lost["payload"]["kernel_state_lost"] is True
    cancelled = next(event for event in events if event["type"] == "run.cancelled")
    assert cancelled["payload"]["kernel_state_lost"] is True
    interrupted = next(event for event in events if event["type"] == "cell.interrupted")
    assert interrupted["payload"]["kernel_state_lost"] is True

    # Repeated cancellation remains a no-op after the run is terminal.
    await runner.cancel()
    assert calls == ["interrupt", "shutdown", "terminate", "kill"]
    await supervisor.close()


@pytest.mark.asyncio
async def test_recovery_rejection_event_failure_does_not_fail_runner(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    notebook_path = tmp_path / "input.ipynb"
    write_notebook(notebook_path, ["print('unused')"])
    supervisor = RunSupervisor(
        notebook_path=notebook_path,
        output_path=tmp_path / "executed.ipynb",
        working_dir=tmp_path,
        run_dir=tmp_path / "run",
        config=config(),
    )
    action_id = supervisor.store.create_action(
        supervisor.run_id,
        ActionKind.RESUME,
        expected_kernel_epoch=supervisor.runner.kernel_epoch,
    )
    assert supervisor.store.claim_action(action_id) is not None

    async def fail_publish(event_type: str, payload: dict) -> dict:
        raise RuntimeError("injected event persistence failure")

    monkeypatch.setattr(supervisor.bus, "publish", fail_publish)
    await supervisor.runner.enqueue(
        RunnerCommand(
            action_id=action_id,
            kind="resume",
            expected_kernel_epoch=supervisor.runner.kernel_epoch + 1,
            expected_failed_attempt=1,
        )
    )
    await supervisor.runner.enqueue(
        RunnerCommand(
            action_id="internal-cancel",
            kind="cancel",
            expected_kernel_epoch=supervisor.runner.kernel_epoch,
        )
    )

    assert await supervisor.runner._recovery_loop(0, 1) == "cancel"  # noqa: SLF001
    action = supervisor.store.get_action(action_id)
    assert action is not None
    assert action["status"] == ActionStatus.REJECTED.value
    await supervisor.close()
