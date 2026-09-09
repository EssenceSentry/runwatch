# pyright: reportPrivateUsage=false
from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

import pytest

from runwatch._marimo_runner import MarimoRunner
from runwatch.models import NotebookSettings, RunStatus, RunwatchConfig
from runwatch.supervisor import RunSupervisor


def notebook(path: Path, bodies: list[str]) -> None:
    cells: list[str] = []
    for body in bodies:
        cells.append(
            "@app.cell\ndef _():\n"
            + "\n".join("    " + line for line in body.splitlines())
        )
    path.write_text(
        "import marimo\n\napp = marimo.App()\n\n"
        + "\n\n".join(cells)
        + '\n\nif __name__ == "__main__":\n    app.run()\n'
    )


def supervisor_for(path: Path, **settings: object) -> RunSupervisor:
    return RunSupervisor(
        notebook_path=path,
        output_path=path.with_suffix(".executed.html"),
        working_dir=path.parent,
        run_dir=path.parent / "run",
        config=RunwatchConfig(
            notebook=NotebookSettings(
                checkpoint_interval_seconds=0.1,
                wait_for_blocking_resources=False,
                **settings,  # pyright: ignore[reportArgumentType]
            )
        ),
        cleanup_on_success=False,
    )


@pytest.mark.asyncio
async def test_marimo_execution_snapshot_survives_shutdown(tmp_path: Path) -> None:
    path = tmp_path / "simple.py"
    notebook(path, ["value = 42\nreturn (value,)", "print(value)\nreturn"])
    supervisor = supervisor_for(path)
    assert isinstance(supervisor.runner, MarimoRunner)
    try:
        await supervisor.start()
        status = await asyncio.wait_for(supervisor.wait(), 45)
        assert status == RunStatus.SUCCEEDED, supervisor.snapshot()
        payload = json.loads(path.with_suffix(".executed.session.json").read_text())
        assert payload["backend"] == "marimo"
        assert all(c["status"] == "succeeded" for c in payload["cells"])
        assert "42" in json.dumps(payload["cells"][1]["outputs"])
        html = path.with_suffix(".executed.html").read_text()
        assert "42" in html
        assert "<script" not in html
    finally:
        await supervisor.close()


async def wait_paused(supervisor: RunSupervisor) -> None:
    paused = asyncio.create_task(supervisor.runner.wait_until_paused())
    finished = asyncio.create_task(supervisor.runner.wait_finished())
    try:
        await asyncio.wait_for(
            asyncio.wait({paused, finished}, return_when=asyncio.FIRST_COMPLETED), 45
        )
        assert supervisor.runner.paused, supervisor.snapshot()["run"]["message"]
    finally:
        paused.cancel()
        finished.cancel()
        await asyncio.gather(paused, finished, return_exceptions=True)


@pytest.mark.asyncio
async def test_repair_preserves_independent_success_and_applied_source(
    tmp_path: Path,
) -> None:
    from runwatch.models import ActionKind

    path = tmp_path / "repair.py"
    notebook(
        path,
        [
            "a = 1 / 0\nreturn (a,)",
            "print(a)\nreturn",
            "from pathlib import Path\nPath('branch').write_text('once')\nprint('independent')\nreturn",
        ],
    )
    supervisor = supervisor_for(path)
    try:
        await supervisor.start()
        await wait_paused(supervisor)
        cells = supervisor.snapshot()["cells"]
        assert [c["status"] for c in cells] == ["failed", "blocked", "succeeded"]
        snapshot = json.loads(supervisor.partial_output_path.read_text())
        supervisor.source_path.write_text(
            supervisor.source_path.read_text().replace("1 / 0", "123")
        )
        assert json.loads(supervisor.partial_output_path.read_text()) == snapshot
        action = supervisor.create_recovery_action(ActionKind.RESUME)
        assert (
            await asyncio.wait_for(supervisor.wait(), 45) == RunStatus.SUCCEEDED
        ), supervisor.snapshot()
        assert supervisor.store.get_action(action)["status"] == "completed"  # type: ignore[index]
        cells = supervisor.snapshot()["cells"]
        assert [c["attempt"] for c in cells] == [2, 1, 1]
        assert (tmp_path / "branch").read_text() == "once"
        assert "123" in path.read_text()
        assert "1 / 0" in supervisor.input_snapshot_path.read_text()
        assert "123" in path.with_suffix(".executed.html").read_text()
    finally:
        await supervisor.close()


@pytest.mark.asyncio
async def test_multiple_failures_and_restart_selection(tmp_path: Path) -> None:
    from runwatch.models import ActionKind

    path = tmp_path / "selection.py"
    notebook(
        path,
        [
            "print(a + b)\nreturn",
            "a = 2\nreturn (a,)",
            "b = 3\nreturn (b,)",
            "raise ValueError('branch1')\nreturn",
            "raise RuntimeError('branch2')\nreturn",
        ],
    )
    supervisor = supervisor_for(path)
    try:
        await supervisor.start()
        await wait_paused(supervisor)
        assert len(supervisor.snapshot()["run"]["failed_cell_ids"]) == 2
        supervisor.create_recovery_action(ActionKind.RESTART, from_cell=0)
        assert await asyncio.wait_for(supervisor.wait(), 45) == RunStatus.SUCCEEDED
        cells = supervisor.snapshot()["cells"]
        assert [c["status"] for c in cells] == ["succeeded"] * 3 + ["not_replayed"] * 2
        assert supervisor.snapshot()["run"]["kernel_epoch"] == 2
    finally:
        await supervisor.close()


@pytest.mark.asyncio
async def test_disabled_stopped_and_async_cells(tmp_path: Path) -> None:
    path = tmp_path / "stop.py"
    notebook(
        path,
        [
            "import marimo as mo\nreturn (mo,)",
            "mo.stop(True, mo.md('stopped'))\na = 1\nreturn (a,)",
            "print(a)\nreturn",
            "raise AssertionError('disabled')\nreturn",
            "import asyncio\nawait asyncio.sleep(0.01)\nprint('async done')\nreturn",
        ],
    )
    path.write_text(
        path.read_text()
        .replace(
            "@app.cell\ndef _():\n    raise AssertionError",
            "@app.cell(disabled=True)\ndef _():\n    raise AssertionError",
        )
        .replace(
            "@app.cell\ndef _():\n    import asyncio",
            "@app.cell\nasync def _():\n    import asyncio",
        )
    )
    supervisor = supervisor_for(path)
    try:
        await supervisor.start()
        assert (
            await asyncio.wait_for(supervisor.wait(), 45) == RunStatus.SUCCEEDED
        ), supervisor.snapshot()
        assert [c["status"] for c in supervisor.snapshot()["cells"]] == [
            "succeeded",
            "skipped",
            "skipped",
            "skipped",
            "succeeded",
        ]
    finally:
        await supervisor.close()


@pytest.mark.asyncio
async def test_progress_resources_and_static_outputs(tmp_path: Path) -> None:
    path = tmp_path / "outputs.py"
    notebook(
        path,
        [
            "import marimo as mo\nfrom runwatch import emit\nfrom runwatch.models import ResourceEvent, ResourceSpec\nfrom tqdm import tqdm\nimport time\nreturn (mo, emit, ResourceEvent, ResourceSpec, tqdm, time)",
            "emit.emit_resource(ResourceEvent(resource=ResourceSpec(provider='local', type='file_count', id='.', metadata={'pattern': '*.py'})), text='files')\nfor _i in tqdm(range(3), desc='tqdm progress'):\n    time.sleep(0.05)\nfor _j in mo.status.progress_bar(range(3), title='native progress'):\n    time.sleep(0.05)\nmo.vstack([mo.md('# Results'), mo.Html('<table><tr><td>table-result</td></tr></table>'), mo.image(bytes.fromhex('89504e470d0a1a0a0000000d49484452000000010000000108060000001f15c4890000000b49444154789c636000020000050001a5f645400000000049454e44ae426082'))])\nreturn",
        ],
    )
    supervisor = supervisor_for(path)
    try:
        await supervisor.start()
        done = asyncio.create_task(supervisor.wait())
        settled = asyncio.create_task(supervisor.runner.wait_finished())
        paused = asyncio.create_task(supervisor.runner.wait_until_paused())
        await asyncio.wait_for(
            asyncio.wait({settled, paused}, return_when=asyncio.FIRST_COMPLETED), 40
        )
        paused.cancel()
        settled.cancel()
        await asyncio.gather(paused, settled, return_exceptions=True)
        assert not supervisor.runner.paused, supervisor.snapshot()["cells"]
        assert await done == RunStatus.SUCCEEDED, supervisor.snapshot()["run"][
            "message"
        ]
        snapshot = supervisor.snapshot()
        assert snapshot["resources"]
        events = supervisor.store.recent_events(supervisor.run_id, limit=1000)
        progress = [e["payload"] for e in events if e["type"] == "notebook.progress"]
        assert {p["metrics"]["source"] for p in progress} >= {"tqdm", "marimo"}
        assert all(p["cell_index"] == 1 and p["attempt"] == 1 for p in progress)
        html = supervisor.output_path.read_text()
        assert "table-result" in html
        assert "data:image/png;base64" in html
        assert "./@file/" not in html
    finally:
        await supervisor.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("force", [False, True])
async def test_cancellation_is_bounded_and_publishes_outputs(
    tmp_path: Path, force: bool
) -> None:
    path = tmp_path / "cancel.py"
    body = "import time\nprint('running', flush=True)\ntime.sleep(60)\nreturn"
    if force:
        body = (
            "import signal\nsignal.signal(signal.SIGINT, signal.SIG_IGN)\nsignal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
            + body
        )
    notebook(path, [body])
    supervisor = supervisor_for(
        path,
        cancel_interrupt_grace_seconds=0.2,
        cancel_shutdown_grace_seconds=0.2,
        cancel_terminate_grace_seconds=0.2,
        cancel_kill_grace_seconds=0.2,
    )
    try:
        await supervisor.start()

        async def active() -> None:
            while not supervisor.snapshot()["run"]["active_cell_ids"]:
                await asyncio.sleep(0.05)

        await asyncio.wait_for(active(), 30)
        await asyncio.sleep(0.2)
        await supervisor.runner.cancel()
        assert (
            await asyncio.wait_for(supervisor.wait(), 15) == RunStatus.CANCELLED
        ), supervisor.snapshot()
        assert supervisor.output_path.exists()
        assert not (supervisor.run_dir / "marimo-runtime/process.json").exists()
    finally:
        await supervisor.close()


@pytest.mark.asyncio
async def test_timeout_can_resume_in_same_epoch(tmp_path: Path) -> None:
    from runwatch.models import ActionKind

    path = tmp_path / "timeout.py"
    notebook(path, ["import time\ntime.sleep(60)\nprint('finished')\nreturn"])
    supervisor = supervisor_for(
        path, timeout_seconds=1, cancel_interrupt_grace_seconds=2
    )
    try:
        await supervisor.start()
        await wait_paused(supervisor)
        assert supervisor.snapshot()["cells"][0]["error_name"] == "CellTimeoutError"
        supervisor.source_path.write_text(
            supervisor.source_path.read_text().replace(
                "time.sleep(60)", "time.sleep(0)"
            )
        )
        supervisor.create_recovery_action(ActionKind.RESUME)
        assert await asyncio.wait_for(supervisor.wait(), 30) == RunStatus.SUCCEEDED
        assert supervisor.snapshot()["run"]["kernel_epoch"] == 1
    finally:
        await supervisor.close()


async def action_finished(
    supervisor: RunSupervisor, action_id: str
) -> dict[str, object]:
    async def wait() -> dict[str, object]:
        while True:
            action = supervisor.store.get_action(action_id)
            if action and action["status"] in {"completed", "rejected", "failed"}:
                return action
            await asyncio.sleep(0.05)

    return await asyncio.wait_for(wait(), 20)


@pytest.mark.asyncio
async def test_stale_actions_and_imported_module_changes_require_fresh_recovery(
    tmp_path: Path,
) -> None:
    from runwatch.models import ActionKind

    path = tmp_path / "guards.py"
    helper = tmp_path / "runwatch_marimo_helper.py"
    helper.write_text("value = 1\n")
    notebook(
        path,
        ["import runwatch_marimo_helper as helper\na = helper.value / 0\nreturn (a,)"],
    )
    supervisor = supervisor_for(path)
    try:
        await supervisor.start()
        await wait_paused(supervisor)
        stale = supervisor.create_recovery_action(ActionKind.RESUME)
        supervisor.source_path.write_text(
            supervisor.source_path.read_text().replace("/ 0", "/ 1")
        )
        assert (await action_finished(supervisor, stale))["status"] == "rejected"
        helper.write_text("value = 42\n")
        action = supervisor.create_recovery_action(ActionKind.RESUME)
        result = await action_finished(supervisor, action)
        assert result["status"] == "rejected"
        assert "Imported Python source changed" in str(result["message"])
        supervisor.create_recovery_action(ActionKind.RESTART)
        assert await asyncio.wait_for(supervisor.wait(), 30) == RunStatus.SUCCEEDED
        assert supervisor.snapshot()["run"]["kernel_epoch"] == 2
    finally:
        await supervisor.close()


@pytest.mark.asyncio
async def test_ambiguous_execution_waits_for_explicit_reconstruction(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from runwatch._marimo_session import MarimoSession, MarimoTransportError
    from runwatch.models import ActionKind

    path = tmp_path / "transport.py"
    notebook(
        path,
        [
            "from pathlib import Path\n_counter = Path('count')\n_counter.write_text(str(int(_counter.read_text()) + 1) if _counter.exists() else '1')\nreturn"
        ],
    )
    execute = MarimoSession.execute
    injected = False

    async def ambiguous(session: MarimoSession, code: str) -> tuple[bool, str]:
        nonlocal injected
        result = await execute(session, code)
        if "ctx.run_cell(" in code and not injected:
            injected = True
            raise MarimoTransportError("Injected loss after the server executed")
        return result

    monkeypatch.setattr(MarimoSession, "execute", ambiguous)
    supervisor = supervisor_for(path)
    try:
        await supervisor.start()
        await wait_paused(supervisor)
        assert (tmp_path / "count").read_text() == "1"
        await asyncio.sleep(0.2)
        assert (tmp_path / "count").read_text() == "1"
        supervisor.create_recovery_action(ActionKind.RESUME)
        assert await asyncio.wait_for(supervisor.wait(), 30) == RunStatus.SUCCEEDED
        assert supervisor.snapshot()["run"]["kernel_epoch"] == 2
        assert (tmp_path / "count").read_text() == "2"
    finally:
        await supervisor.close()


@pytest.mark.asyncio
async def test_stopped_controller_reopens_native_source_and_replays(
    tmp_path: Path,
) -> None:
    path = tmp_path / "reopen.py"
    notebook(path, ["a = 1 / 0\nreturn (a,)", "print(a)\nreturn"])
    first = supervisor_for(path)
    await first.start()
    await wait_paused(first)
    await first.close()
    first.source_path.write_text(first.source_path.read_text().replace("1 / 0", "42"))
    reopened = RunSupervisor.reopen(first.run_dir)
    try:
        await reopened.start()
        assert await asyncio.wait_for(reopened.wait(), 30) == RunStatus.SUCCEEDED
        assert reopened.snapshot()["run"]["backend"] == "marimo"
        assert reopened.snapshot()["run"]["kernel_epoch"] == 2
        assert "42" in reopened.output_path.read_text()
    finally:
        await reopened.close()


@pytest.mark.asyncio
async def test_publication_failure_retains_recoverable_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import runwatch._marimo_runner as implementation

    path = tmp_path / "publication.py"
    notebook(path, ["print('result')\nreturn"])
    original = implementation.atomic_write_bytes

    def broken(path: Path, data: bytes, **kwargs: Any) -> None:
        if path.suffix == ".html":
            raise OSError("Injected publication failure")
        original(path, data, **kwargs)

    monkeypatch.setattr(implementation, "atomic_write_bytes", broken)
    supervisor = supervisor_for(path)
    supervisor.cleanup_on_success = True
    try:
        await supervisor.start()
        assert await asyncio.wait_for(supervisor.wait(), 30) == RunStatus.FAILED
        assert supervisor.partial_output_path.exists()
        assert "publication failure" in supervisor.snapshot()["run"]["message"]
    finally:
        await supervisor.close()
    assert supervisor.run_dir.exists()


@pytest.mark.asyncio
async def test_kernel_death_pauses_and_requires_explicit_replay(tmp_path: Path) -> None:
    import os
    import signal

    from runwatch.models import ActionKind

    path = tmp_path / "kernel-death.py"
    notebook(path, ["import time\ntime.sleep(60)\nreturn"])
    supervisor = supervisor_for(path)
    try:
        await supervisor.start()

        async def running() -> None:
            while not supervisor.snapshot()["run"]["active_cell_ids"]:
                await asyncio.sleep(0.05)

        await asyncio.wait_for(running(), 30)
        pid = supervisor.store.get_run(supervisor.run_id)["kernel_pid"]
        os.kill(pid, signal.SIGKILL)
        await wait_paused(supervisor)
        assert supervisor.snapshot()["cells"][0]["status"] == "interrupted"
        supervisor.source_path.write_text(
            supervisor.source_path.read_text().replace(
                "time.sleep(60)", "time.sleep(0)"
            )
        )
        supervisor.create_recovery_action(ActionKind.RESUME)
        assert await asyncio.wait_for(supervisor.wait(), 30) == RunStatus.SUCCEEDED
        assert supervisor.snapshot()["run"]["kernel_epoch"] == 2
    finally:
        await supervisor.close()


@pytest.mark.asyncio
async def test_source_conflict_retains_external_edit_and_checkpoint(
    tmp_path: Path,
) -> None:
    from runwatch.models import ActionKind

    path = tmp_path / "conflict.py"
    notebook(path, ["value = 1 / 0\nreturn (value,)"])
    supervisor = supervisor_for(path)
    try:
        await supervisor.start()
        await wait_paused(supervisor)
        external = path.read_text().replace("1 / 0", "999")
        path.write_text(external)
        supervisor.source_path.write_text(
            supervisor.source_path.read_text().replace("1 / 0", "42")
        )
        supervisor.create_recovery_action(ActionKind.RESUME)
        assert await asyncio.wait_for(supervisor.wait(), 30) == RunStatus.FAILED
        assert path.read_text() == external
        assert "42" in supervisor.partial_output_path.read_text()
    finally:
        await supervisor.close()
    assert supervisor.run_dir.exists()
