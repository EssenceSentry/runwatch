# pyright: reportMissingParameterType=false, reportUnknownMemberType=false, reportUnknownParameterType=false
from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path
from types import ModuleType

import nbformat


def _load_launcher() -> ModuleType:
    repo_root = Path(__file__).resolve().parents[1]
    path = repo_root / "web_artifacts_fake_sessions" / "runwatch" / "run.py"
    spec = importlib.util.spec_from_file_location("runwatch_fake_session", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_fake_session_defaults_to_lan_and_ntfy(monkeypatch) -> None:
    monkeypatch.delenv("RUNWATCH_NTFY_BASE_URL", raising=False)
    monkeypatch.delenv("RUNWATCH_NTFY_TOPIC", raising=False)
    monkeypatch.setattr(sys, "argv", ["run.py"])

    args = _load_launcher().parse_args()

    assert args.share == "lan"
    assert args.ntfy is True
    assert args.ntfy_base_url == "https://ntfy.sh"
    assert args.ntfy_topic is None
    assert args.batches == 300
    assert args.delay_seconds == 1.0


def test_fake_session_supports_local_only_replay(monkeypatch) -> None:
    monkeypatch.setattr(sys, "argv", ["run.py", "--share", "none", "--no-ntfy"])

    args = _load_launcher().parse_args()

    assert args.share == "none"
    assert args.ntfy is False


def test_vscode_active_notebook_task_uses_cloudflared() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    tasks_path = repo_root / ".vscode" / "tasks.json"
    tasks = json.loads(tasks_path.read_text(encoding="utf-8"))["tasks"]
    active_notebook = next(
        task for task in tasks if task["label"] == "notebook: run active notebook"
    )

    assert active_notebook["type"] == "process"
    assert active_notebook["command"] == (
        "${workspaceFolder}/scripts/run_active_notebook.sh"
    )
    assert active_notebook["args"] == ["${file}"]

    launcher_path = repo_root / "scripts" / "run_active_notebook.sh"
    assert os.access(launcher_path, os.X_OK)
    launcher = launcher_path.read_text(encoding="utf-8")
    assert "--share cloudflared" in launcher


def test_notebook_workspace_resolver_accepts_relative_paths(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    notebook = workspace / "models" / "example.ipynb"
    notebook.parent.mkdir(parents=True)
    notebook.touch()
    (workspace / "pyproject.toml").touch()
    resolver = (
        Path(__file__).resolve().parents[1]
        / "scripts"
        / "resolve_notebook_workspace.sh"
    )

    result = subprocess.run(
        [str(resolver), str(notebook.relative_to(tmp_path))],
        cwd=tmp_path,
        check=True,
        capture_output=True,
        text=True,
    )

    assert result.stdout.strip() == str(workspace)


def test_fake_session_registers_dependency_free_linked_results_dashboard() -> None:
    session_root = (
        Path(__file__).resolve().parents[1] / "web_artifacts_fake_sessions" / "runwatch"
    )
    notebook = nbformat.read(session_root / "session.ipynb", as_version=4)
    source = "\n".join(str(cell.source) for cell in notebook.cells)

    assert "local.emit_dashboard(" in source
    assert "RUNWATCH_SIMULATION_DASHBOARD_URL" in source
    assert "import pandas" not in source
    assert (session_root / "linked_dashboard.html").is_file()


def test_fake_session_executes_a_runtime_notebook_copy() -> None:
    launcher = (
        Path(__file__).resolve().parents[1]
        / "web_artifacts_fake_sessions"
        / "runwatch"
        / "run.py"
    ).read_text(encoding="utf-8")

    assert (
        'replay_notebook_path = runtime_root / f"session-{replay_id[:8]}.ipynb"'
        in launcher
    )
    assert "shutil.copyfile(NOTEBOOK_PATH, replay_notebook_path)" in launcher
    assert launcher.count("str(replay_notebook_path)") == 2
    assert "replay_notebook_path.unlink(missing_ok=True)" in launcher
    assert '"linked-dashboard" / replay_id[:8]' in launcher
    assert "shutil.rmtree(linked_dashboard_root, ignore_errors=True)" in launcher
    assert 'environment["RUNWATCH_MASCOT_SHOWCASE"] = "1"' in launcher


def test_fake_session_forwards_interrupt_and_waits_for_replay(monkeypatch) -> None:
    launcher = _load_launcher()
    handlers: dict[object, object] = {
        launcher.signal.SIGINT: launcher.signal.SIG_DFL,
        launcher.signal.SIGTERM: launcher.signal.SIG_DFL,
    }
    popen_options: dict[str, object] = {}
    forwarded: list[tuple[int, object]] = []

    class FakeReplay:
        pid = 4242
        wait_calls = 0

        def wait(self, timeout=None) -> int:
            self.wait_calls += 1
            if self.wait_calls == 1:
                handler = handlers[launcher.signal.SIGINT]
                assert callable(handler)
                handler(int(launcher.signal.SIGINT), None)
                raise launcher.subprocess.TimeoutExpired("runwatch", timeout)
            return 130

    replay = FakeReplay()

    def fake_popen(command: list[str], **kwargs: object) -> FakeReplay:
        popen_options["command"] = command
        popen_options.update(kwargs)
        return replay

    def get_handler(signum: object) -> object:
        return handlers[signum]

    def set_handler(signum: object, handler: object) -> object:
        previous = handlers[signum]
        handlers[signum] = handler
        return previous

    monkeypatch.setattr(launcher.subprocess, "Popen", fake_popen)

    def record_forward(pid: int, signum: object) -> None:
        forwarded.append((pid, signum))

    monkeypatch.setattr(launcher.signal, "getsignal", get_handler)
    monkeypatch.setattr(launcher.signal, "signal", set_handler)
    monkeypatch.setattr(launcher.os, "killpg", record_forward)

    result = launcher.run_replay(["runwatch", "execute"], {"DEMO": "1"})

    assert result == 130
    assert popen_options["start_new_session"] is True
    assert popen_options["env"] == {"DEMO": "1"}
    assert forwarded == [(replay.pid, launcher.signal.SIGINT)]
    assert handlers[launcher.signal.SIGINT] is launcher.signal.SIG_DFL
    assert handlers[launcher.signal.SIGTERM] is launcher.signal.SIG_DFL
