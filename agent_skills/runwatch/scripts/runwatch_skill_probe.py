#!/usr/bin/env python
"""Forward probes for the Runwatch recovery skill."""

from __future__ import annotations

import asyncio
import json
import tempfile
from pathlib import Path

import nbformat

from runwatch.models import ActionKind, NotebookSettings, RunwatchConfig
from runwatch.supervisor import RunSupervisor


def _write_notebook(path: Path, sources: list[str]) -> None:
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


def _config() -> RunwatchConfig:
    return RunwatchConfig(
        notebook=NotebookSettings(
            kernel_name="python3",
            checkpoint_interval_seconds=0.05,
            wait_for_blocking_resources=False,
        )
    )


async def _cell_local_repair(root: Path) -> dict[str, object]:
    notebook_path = root / "cell-repair.ipynb"
    _write_notebook(notebook_path, ["x = 2", "y = x + missing", "print(y * 3)"])
    supervisor = RunSupervisor(
        notebook_path=notebook_path,
        output_path=root / "cell-repair-executed.ipynb",
        working_dir=root,
        run_dir=root / "cell-repair-run",
        config=_config(),
    )
    try:
        await supervisor.start()
        await asyncio.wait_for(supervisor.runner.wait_until_paused(), timeout=20)
        source = nbformat.read(supervisor.source_path, as_version=4)
        source.cells[1].source = "y = x + 4"
        nbformat.write(source, supervisor.source_path)
        action_id = supervisor.create_recovery_action(ActionKind.RESUME)
        status = await asyncio.wait_for(supervisor.wait(), timeout=20)
        action = supervisor.store.get_action(action_id)
        executed = nbformat.read(supervisor.output_path, as_version=4)
        output = executed.cells[2].outputs[0].text.strip()
        if status.value != "succeeded" or output != "18":
            raise AssertionError("Cell-local repair did not resume in the live kernel")
        if action is None or action["status"] != "completed":
            raise AssertionError("Resume action did not complete")
        return {"status": status.value, "output": output}
    finally:
        await supervisor.close()


async def _imported_source_restart(root: Path) -> dict[str, object]:
    helper = root / "runwatch_probe_helper.py"
    helper.write_text(
        "def value():\n    raise RuntimeError('repair me')\n", encoding="utf-8"
    )
    notebook_path = root / "source-restart.ipynb"
    _write_notebook(
        notebook_path,
        ["from runwatch_probe_helper import value\nresult = value()", "print(result)"],
    )
    supervisor = RunSupervisor(
        notebook_path=notebook_path,
        output_path=root / "source-restart-executed.ipynb",
        working_dir=root,
        run_dir=root / "source-restart-run",
        config=_config(),
    )
    try:
        await supervisor.start()
        await asyncio.wait_for(supervisor.runner.wait_until_paused(), timeout=20)
        helper.write_text("def value():\n    return 7\n", encoding="utf-8")
        action_id = supervisor.create_recovery_action(ActionKind.RESTART)
        status = await asyncio.wait_for(supervisor.wait(), timeout=20)
        action = supervisor.store.get_action(action_id)
        executed = nbformat.read(supervisor.output_path, as_version=4)
        output = executed.cells[1].outputs[0].text.strip()
        if status.value != "succeeded" or output != "7":
            raise AssertionError("Imported-source repair did not restart and replay")
        if action is None or action["status"] != "completed":
            raise AssertionError("Restart action did not complete")
        return {
            "status": status.value,
            "output": output,
            "kernel_epoch": supervisor.snapshot()["run"]["kernel_epoch"],
        }
    finally:
        await supervisor.close()


async def _main() -> None:
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        results = {
            "cell_local_repair": await _cell_local_repair(root),
            "imported_source_restart": await _imported_source_restart(root),
        }
    print(json.dumps(results, indent=2, default=str))


if __name__ == "__main__":
    asyncio.run(_main())
