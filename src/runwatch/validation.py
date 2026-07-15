from __future__ import annotations

import shutil
import warnings as warning_control
from pathlib import Path
from typing import Any, TypedDict

import nbformat
from jupyter_client.kernelspec import KernelSpecManager, NoSuchKernel
from nbformat import NotebookNode

from .adapters import default_adapter_registry
from .models import ResourceEvent, RunwatchConfig


class ValidationReport(TypedDict):
    """Structured result returned by notebook execution preflight."""

    valid: bool
    notebook: str
    working_dir: str
    kernel_name: str
    cell_count: int
    code_cell_count: int
    configured_resources: list[dict[str, Any]]
    errors: list[str]
    warnings: list[str]


def _read_notebook(notebook_path: Path, errors: list[str]) -> NotebookNode | None:
    try:
        notebook = nbformat.read(notebook_path, as_version=4)
        nbformat.validate(notebook)
        return notebook
    except Exception as error:
        errors.append(
            f"Notebook is not valid nbformat v4: {type(error).__name__}: {error}"
        )
        return None


def _validate_working_dir(working_dir: Path, errors: list[str]) -> None:
    if not working_dir.exists():
        errors.append(f"Working directory does not exist: {working_dir}")
    elif not working_dir.is_dir():
        errors.append(f"Working directory is not a directory: {working_dir}")


def _validate_kernel(kernel_name: str, errors: list[str]) -> None:
    try:
        with warning_control.catch_warnings():
            warning_control.filterwarnings(
                "ignore", message=r"IPython dir .* is not a writable location"
            )
            KernelSpecManager().get_kernel_spec(kernel_name)
    except NoSuchKernel:
        errors.append(f"Kernel {kernel_name!r} is not installed")
    except Exception as error:
        errors.append(
            f"Could not inspect kernel {kernel_name!r}: {type(error).__name__}: {error}"
        )


def _validate_resources(
    config: RunwatchConfig, errors: list[str]
) -> list[dict[str, Any]]:
    resources: list[dict[str, Any]] = []
    registry = default_adapter_registry()
    for index, registration in enumerate(config.resources):
        event = ResourceEvent(
            resource=registration.resource,
            lifecycle=registration.lifecycle,
        )
        adapter = None
        try:
            adapter = registry.validate(event)
        except Exception as error:
            errors.append(f"Configured resource {index + 1}: {error}")
        resources.append(
            {
                "provider": registration.resource.provider,
                "type": registration.resource.type,
                "id": registration.resource.id,
                "blocking": registration.lifecycle.blocking,
                "stop_on_cancel": registration.lifecycle.stop_on_cancel,
                "supports_stop": bool(adapter and adapter.supports_stop),
            }
        )
    return resources


def _validate_sharing(
    config: RunwatchConfig, errors: list[str], warning_messages: list[str]
) -> None:
    if (
        config.server.share == "cloudflared"
        and shutil.which(config.server.cloudflared_binary) is None
    ):
        errors.append(
            f"cloudflared binary {config.server.cloudflared_binary!r} was not found"
        )
    if config.server.share == "lan" and config.server.host not in {
        "0.0.0.0",
        "127.0.0.1",
        "localhost",
    }:
        warning_messages.append(
            f"LAN sharing will bind configured host {config.server.host!r}"
        )


def validate_execution(
    notebook_path: Path,
    config: RunwatchConfig,
    *,
    working_dir: Path,
) -> ValidationReport:
    """Validate a planned run without starting a kernel, server, or provider resource."""

    errors: list[str] = []
    warning_messages: list[str] = []
    notebook = _read_notebook(notebook_path, errors)
    _validate_working_dir(working_dir, errors)

    kernel_name = config.notebook.kernel_name
    if kernel_name is None and notebook is not None:
        kernel_name = notebook.metadata.get("kernelspec", {}).get("name")
    kernel_name = str(kernel_name or "python3")
    _validate_kernel(kernel_name, errors)
    resources = _validate_resources(config, errors)
    _validate_sharing(config, errors, warning_messages)
    warning_messages.append(
        "Resources emitted dynamically by notebook cells cannot be known until those cells run"
    )
    if config.notebook.timeout_seconds is None:
        warning_messages.append("Notebook cells have no execution timeout")
    if (
        config.notebook.wait_for_blocking_resources
        and config.notebook.resource_completion_timeout_seconds is None
    ):
        warning_messages.append(
            "Blocking resources, including dynamically emitted resources, have no "
            "overall completion timeout"
        )

    cells = list(notebook.cells) if notebook is not None else []
    return {
        "valid": not errors,
        "notebook": str(notebook_path.resolve()),
        "working_dir": str(working_dir.resolve()),
        "kernel_name": kernel_name,
        "cell_count": len(cells),
        "code_cell_count": sum(cell.cell_type == "code" for cell in cells),
        "configured_resources": resources,
        "errors": errors,
        "warnings": warning_messages,
    }
