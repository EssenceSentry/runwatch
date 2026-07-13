# pyright: reportMissingTypeArgument=false, reportPrivateUsage=false, reportUnknownArgumentType=false, reportUnknownParameterType=false
from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import nbformat
import pytest

from runwatch.models import (
    ActionKind,
    ActionStatus,
    NotebookSettings,
    RunnerCommand,
    RunwatchConfig,
)
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
