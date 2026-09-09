"""Internal document and execution boundaries; optional engines load on demand."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Literal, Protocol

from nbformat import NotebookNode

from .models import NotebookSettings, RunnerCommand, RunStatus

BackendName = Literal["jupyter", "marimo"]


class NotebookDocument(Protocol):
    @property
    def source(self) -> str: ...

    def view(self) -> NotebookNode: ...

    def records(self) -> list[dict[str, Any]]: ...

    def validate_resume(self, fresh: NotebookDocument) -> None: ...


class ExecutionBackend(Protocol):
    @property
    def paused(self) -> bool: ...

    async def run(self) -> RunStatus: ...

    async def enqueue(self, command: RunnerCommand) -> None: ...

    async def interrupt(self) -> bool: ...

    async def cancel(self) -> None: ...

    async def shutdown(self) -> None: ...


def resolve_backend(path: Path, settings: NotebookSettings) -> BackendName:
    if settings.backend != "auto":
        backend = settings.backend
    elif path.suffix == ".ipynb":
        backend = "jupyter"
    elif path.suffix == ".py":
        backend = "marimo"
    else:
        raise ValueError("Expected a Jupyter .ipynb or Marimo .py notebook")
    expected = ".py" if backend == "marimo" else ".ipynb"
    if path.suffix != expected:
        raise ValueError(f"The {backend} backend requires {expected} source")
    if backend == "marimo" and settings.kernel_name is not None:
        raise ValueError(
            "kernel_name is Jupyter-specific; use marimo.python_executable"
        )
    if backend == "jupyter" and (
        settings.marimo.python_executable is not None
        or not settings.marimo.capture_native_progress
    ):
        raise ValueError("notebook.marimo settings require the Marimo backend")
    return backend


def python_executable(settings: NotebookSettings) -> str:
    return settings.marimo.python_executable or sys.executable


def source_path_for_run(run_dir: Path) -> Path:
    from .manifest import read_run_manifest

    manifest = read_run_manifest(run_dir / "run-manifest.json")
    return Path(manifest.source_path)


def default_output(path: Path, run_dir: Path, backend: BackendName) -> Path:
    if backend == "marimo":
        return path.with_suffix(".executed.html")
    return run_dir / "executed.ipynb"
