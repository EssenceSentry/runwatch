# pyright: reportMissingParameterType=false, reportUnknownMemberType=false, reportUnknownParameterType=false
from __future__ import annotations

import importlib.util
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


def test_fake_session_supports_local_only_replay(monkeypatch) -> None:
    monkeypatch.setattr(sys, "argv", ["run.py", "--share", "none", "--no-ntfy"])

    args = _load_launcher().parse_args()

    assert args.share == "none"
    assert args.ntfy is False


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
