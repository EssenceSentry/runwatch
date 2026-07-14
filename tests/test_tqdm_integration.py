# pyright: reportPrivateUsage=false, reportUnknownArgumentType=false, reportUnknownMemberType=false
from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from types import ModuleType
from typing import Any

import nbformat
import pytest

from runwatch._tqdm import tqdm_bootstrap_code
from runwatch.emit import EVENT_MIME_TYPE
from runwatch.models import ActionKind, NotebookSettings, RunwatchConfig
from runwatch.supervisor import RunSupervisor


class _FakeStandardTqdm:
    def __init__(self) -> None:
        self.n = 0
        self.total = 10
        self.unit = "rows"
        self.desc = "Loading"
        self.pos = -2
        self.disable = False
        self.native_calls = 0

    @property
    def format_dict(self) -> dict[str, Any]:
        return {
            "n": self.n,
            "total": self.total,
            "unit": self.unit,
            "prefix": self.desc,
            "rate": 4.0,
            "elapsed": 2.5,
            "postfix": None,
        }

    def display(self, *_args: object, **_kwargs: object) -> str:
        self.native_calls += 1
        return "standard display"


class _FakeNotebookTqdm(_FakeStandardTqdm):
    def display(self, *_args: object, **_kwargs: object) -> str:
        self.native_calls += 1
        return "notebook display"


def _install_fake_modules(
    monkeypatch: pytest.MonkeyPatch,
    captured: list[tuple[str, dict[str, Any], str]],
    failure: dict[str, bool],
) -> None:
    display_module = ModuleType("IPython.display")

    def display(bundle: dict[str, Any], *, raw: bool, display_id: str) -> None:
        assert raw is True
        captured.append(("display", bundle, display_id))

    def update_display(bundle: dict[str, Any], *, raw: bool, display_id: str) -> None:
        assert raw is True
        if failure["update"]:
            raise RuntimeError("frontend unavailable")
        captured.append(("update", bundle, display_id))

    setattr(display_module, "display", display)
    setattr(display_module, "update_display", update_display)
    monkeypatch.setitem(sys.modules, "IPython.display", display_module)

    tqdm_package = ModuleType("tqdm")
    setattr(tqdm_package, "__path__", [])
    std_module = ModuleType("tqdm.std")
    notebook_module = ModuleType("tqdm.notebook")
    setattr(std_module, "tqdm", _FakeStandardTqdm)
    setattr(notebook_module, "tqdm", _FakeNotebookTqdm)
    monkeypatch.setitem(sys.modules, "tqdm", tqdm_package)
    monkeypatch.setitem(sys.modules, "tqdm.std", std_module)
    monkeypatch.setitem(sys.modules, "tqdm.notebook", notebook_module)


def test_tqdm_bootstrap_preserves_frontends_throttles_and_updates_in_place(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: list[tuple[str, dict[str, Any], str]] = []
    failure = {"update": False}
    _install_fake_modules(monkeypatch, captured, failure)

    code = tqdm_bootstrap_code(60)
    assert "__RUNWATCH_TQDM_MIN_INTERVAL_SECONDS__" not in code
    exec(code, {})

    standard = _FakeStandardTqdm()
    assert standard.display() == "standard display"
    standard.n = 3
    assert standard.display() == "standard display"
    standard.n = 10
    standard.disable = True
    assert standard.display() == "standard display"
    assert standard.native_calls == 3

    assert [item[0] for item in captured] == ["display", "update"]
    first_payload = captured[0][1][EVENT_MIME_TYPE]
    final_payload = captured[1][1][EVENT_MIME_TYPE]
    assert first_payload["completed"] == 0
    assert final_payload["completed"] == 10
    assert final_payload["total"] == 10
    assert final_payload["unit"] == "rows"
    assert final_payload["message"] == "Loading"
    assert final_payload["metrics"] == {
        "source": "tqdm",
        "progress_id": first_payload["metrics"]["progress_id"],
        "position": 2,
        "closed": True,
        "rate": 4.0,
        "elapsed_seconds": 2.5,
    }
    assert captured[0][2] == captured[1][2]

    notebook = _FakeNotebookTqdm()
    assert notebook.display() == "notebook display"
    notebook.n = notebook.total
    assert notebook.display(bar_style="success") == "notebook display"
    assert notebook.native_calls == 2
    assert [item[0] for item in captured[-2:]] == ["display", "update"]

    failure["update"] = True
    resilient = _FakeStandardTqdm()
    assert resilient.display() == "standard display"
    resilient.n = resilient.total
    resilient.disable = True
    assert resilient.display() == "standard display"
    assert resilient.native_calls == 2


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


def _config(*, capture_tqdm: bool = True) -> RunwatchConfig:
    return RunwatchConfig(
        notebook=NotebookSettings(
            kernel_name="python3",
            checkpoint_interval_seconds=0.05,
            capture_tqdm=capture_tqdm,
            tqdm_min_interval_seconds=60,
            wait_for_blocking_resources=False,
        )
    )


@pytest.mark.asyncio
async def test_tqdm_progress_survives_restart_and_keeps_one_structured_output(
    tmp_path: Path,
) -> None:
    notebook_path = tmp_path / "progress.ipynb"
    _write_notebook(
        notebook_path,
        [
            "raise RuntimeError('restart me')",
            """
from tqdm.auto import tqdm

bar = tqdm(total=4, desc="Loading rows", unit="rows")
bar.update(2)
bar.update(2)
bar.close()
""".strip(),
        ],
    )
    supervisor = RunSupervisor(
        notebook_path=notebook_path,
        output_path=tmp_path / "executed.ipynb",
        working_dir=tmp_path,
        run_dir=tmp_path / "run",
        config=_config(),
    )
    await supervisor.start()
    await asyncio.wait_for(supervisor.runner.wait_until_paused(), timeout=20)

    source = nbformat.read(supervisor.source_path, as_version=4)
    source.cells[0].source = "print('restart ready')"
    nbformat.write(source, supervisor.source_path)
    action_id = supervisor.create_recovery_action(ActionKind.RESTART)

    status = await asyncio.wait_for(supervisor.wait(), timeout=20)
    assert status.value == "succeeded"
    assert supervisor.store.get_action(action_id)["status"] == "completed"  # type: ignore[index]
    assert supervisor.snapshot()["run"]["kernel_epoch"] == 2

    progress_events = [
        event
        for event in supervisor.store.recent_events(supervisor.run_id)
        if event["type"] == "notebook.progress"
    ]
    assert len(progress_events) == 2
    assert [event["payload"]["completed"] for event in progress_events] == [0, 4]
    final_payload = progress_events[-1]["payload"]
    assert final_payload["cell_index"] == 1
    assert final_payload["attempt"] == 1
    assert final_payload["message"] == "Loading rows"
    assert final_payload["metrics"]["source"] == "tqdm"
    assert final_payload["metrics"]["closed"] is True

    executed = nbformat.read(supervisor.output_path, as_version=4)
    outputs = executed.cells[1].outputs
    structured = [
        output for output in outputs if EVENT_MIME_TYPE in output.get("data", {})
    ]
    assert len(structured) == 1
    assert structured[0].data[EVENT_MIME_TYPE]["completed"] == 4
    native_output = "".join(
        str(output.get("text", ""))
        for output in outputs
        if output.output_type == "stream"
    )
    assert "Loading rows" in native_output
    assert "4/4" in native_output
    assert all(
        EVENT_MIME_TYPE not in item.get("mime_types", [])
        for item in supervisor.snapshot()["cells"][1]["output_tail"]
    )
    await supervisor.close()


@pytest.mark.asyncio
async def test_tqdm_capture_can_be_disabled_without_starting_a_kernel(
    tmp_path: Path,
) -> None:
    notebook_path = tmp_path / "disabled.ipynb"
    _write_notebook(notebook_path, ["pass"])
    supervisor = RunSupervisor(
        notebook_path=notebook_path,
        output_path=tmp_path / "executed.ipynb",
        working_dir=tmp_path,
        run_dir=tmp_path / "run",
        config=_config(capture_tqdm=False),
    )

    assert supervisor.runner.client is None
    await supervisor.runner._install_tqdm_instrumentation()
    assert supervisor.runner.client is None
    await supervisor.close()
