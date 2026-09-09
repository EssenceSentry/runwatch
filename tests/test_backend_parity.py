# pyright: reportPrivateUsage=false
from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

import httpx
import nbformat
import pytest

from runwatch.cli import _automatic_cleanup_decision
from runwatch.models import (
    ActionKind,
    NotebookSettings,
    NotificationSettings,
    RunStatus,
    RunwatchConfig,
)
from runwatch.supervisor import RunSupervisor


def write(path: Path, bodies: list[str]) -> None:
    if path.suffix == ".ipynb":
        node = (
            nbformat.read(path, as_version=4)
            if path.exists()
            else nbformat.v4.new_notebook(
                cells=[nbformat.v4.new_code_cell(body) for body in bodies]
            )
        )
        for cell, body in zip(node.cells, bodies, strict=True):
            cell.source = body
        nbformat.write(node, path)
        return
    cells = [
        "@app.cell\ndef _():\n" + "\n".join("    " + line for line in body.splitlines())
        for body in bodies
    ]
    path.write_text("import marimo\napp=marimo.App()\n\n" + "\n\n".join(cells) + "\n")


@pytest.mark.asyncio
@pytest.mark.parametrize("backend", ["jupyter", "marimo"])
@pytest.mark.parametrize("transition", ["complete", "resume", "restart", "cancel"])
async def test_shared_durable_lifecycle(
    tmp_path: Path, backend: str, transition: str
) -> None:
    path = tmp_path / ("parity.py" if backend == "marimo" else "parity.ipynb")
    first = "from runwatch import local, emit_progress\nlocal.emit_line_count('ready.txt', expected_lines=1, blocking=True, poll_interval_seconds=0.1)\nemit_progress(1, total=1, message='ready')\ntoken=1"
    second = "assert token == 1\nprint('done')"
    if transition in {"resume", "restart"}:
        second = "assert token == 1\nraise ValueError('repair')"
    elif transition == "cancel":
        second = "assert token == 1\nimport time\ntime.sleep(60)"
    bodies = [first, second]
    write(path, bodies)
    (tmp_path / "ready.txt").write_text("ready\n")
    delivered: list[dict[str, Any]] = []

    def receive(request: httpx.Request) -> httpx.Response:
        delivered.append(json.loads(request.content))
        return httpx.Response(200)

    supervisor = RunSupervisor(
        notebook_path=path,
        output_path=tmp_path
        / ("result.html" if backend == "marimo" else "result.ipynb"),
        working_dir=tmp_path,
        run_dir=tmp_path / "run",
        config=RunwatchConfig(
            notebook=NotebookSettings(checkpoint_interval_seconds=0.1),
            notifications=NotificationSettings(webhook_urls=["http://127.0.0.1/hook"]),
        ),
    )
    await supervisor.notifications._client.aclose()
    supervisor.notifications._client = httpx.AsyncClient(
        transport=httpx.MockTransport(receive)
    )
    try:
        await supervisor.start()
        if transition in {"resume", "restart"}:
            await asyncio.wait_for(supervisor.runner.wait_until_paused(), 30)
            assert supervisor.partial_output_path.exists()
            bodies[1] = "assert token == 1\nprint('repaired')"
            write(supervisor.source_path, bodies)
            supervisor.create_recovery_action(
                ActionKind.RESUME if transition == "resume" else ActionKind.RESTART
            )
        elif transition == "cancel":

            async def active() -> None:
                while supervisor.snapshot()["cells"][1]["status"] != "running":
                    await asyncio.sleep(0.05)

            await asyncio.wait_for(active(), 30)
            await supervisor.runner.cancel()
        expected = (
            RunStatus.CANCELLED if transition == "cancel" else RunStatus.SUCCEEDED
        )
        assert await asyncio.wait_for(supervisor.wait(), 30) == expected
        assert supervisor.partial_output_path.exists()
        assert supervisor.notebook_path.exists()
        if transition != "cancel" or backend == "marimo":
            assert supervisor.output_path.exists()
        snapshot = supervisor.snapshot()
        assert snapshot["run"]["backend"] == backend
        assert snapshot["run"]["kernel_epoch"] == (2 if transition == "restart" else 1)
        assert len(snapshot["resources"]) == 1
        assert snapshot["resources"][0]["status"] == "completed"
        assert snapshot["run"]["finalization_complete"]
        assert await _automatic_cleanup_decision(supervisor) == (True, None)
        assert delivered
        assert any(
            "completed" in str(item).lower() or "cancel" in str(item).lower()
            for item in delivered
        )
    finally:
        await supervisor.close()
