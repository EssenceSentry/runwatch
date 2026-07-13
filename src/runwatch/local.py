from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Literal

from .emit import emit_resource
from .models import Ownership, ResourceEvent, ResourceLifecycle, ResourceSpec
from .resources.base import ResourceAdapter
from .resources.dashboard import DashboardAdapter
from .resources.local import (
    FileCountAdapter,
    LineCountAdapter,
    SystemMetricsAdapter,
)


def _validate_event(adapter: type[ResourceAdapter], event: ResourceEvent) -> None:
    try:
        adapter.validate_registration(event)
    except RuntimeError as error:
        raise ValueError(str(error)) from error


def emit_system_metrics(
    *,
    include_host: bool = True,
    include_kernel: bool = True,
    gpu: Literal["all", "none"] = "all",
    logical_key: str = "system",
    poll_interval_seconds: float | None = 5.0,
) -> dict[str, Any]:
    if not include_host and not include_kernel and gpu == "none":
        raise ValueError("at least one system metric scope must be enabled")
    event = ResourceEvent(
        resource=ResourceSpec(
            provider="local",
            type="system_metrics",
            id=logical_key,
            logical_key=logical_key,
            ownership=Ownership.BORROWED,
            metadata={
                "include_host": include_host,
                "include_kernel": include_kernel,
                "gpu": gpu,
                "kernel_pid": os.getpid(),
            },
        ),
        lifecycle=ResourceLifecycle(
            blocking=False,
            retain_logs=False,
            poll_interval_seconds=poll_interval_seconds,
        ),
    )
    _validate_event(SystemMetricsAdapter, event)
    return emit_resource(event, text="Local system metrics")


def emit_dashboard(
    url: str,
    *,
    name: str | None = None,
    health_path: str | None = None,
    expected_status_code: int | None = None,
    request_timeout_seconds: float = 5.0,
    logical_key: str | None = None,
    poll_interval_seconds: float | None = 5.0,
) -> dict[str, Any]:
    """Register a localhost dashboard for authenticated Runwatch sharing.

    Parameters
    ----------
    url:
        Localhost or loopback HTTP URL served on the Runwatch host.
    name:
        Human-readable label shown on the resource card.
    health_path:
        Optional absolute path used for availability checks.
    expected_status_code:
        Optional exact healthy HTTP response code. By default, 2xx and 3xx are healthy.
    request_timeout_seconds:
        Timeout for each availability check.
    logical_key:
        Stable key used to reconcile replayed registrations.
    poll_interval_seconds:
        Availability polling interval.

    Returns
    -------
    dict[str, Any]
        The structured resource event written to notebook output.
    """
    event = ResourceEvent(
        resource=ResourceSpec(
            provider="local",
            type="dashboard",
            id=url,
            logical_key=logical_key or url,
            ownership=Ownership.EXTERNAL,
            metadata={
                "name": name,
                "health_path": health_path,
                "expected_status_code": expected_status_code,
                "request_timeout_seconds": request_timeout_seconds,
            },
        ),
        lifecycle=ResourceLifecycle(
            blocking=False,
            stop_on_cancel=False,
            retain_logs=False,
            poll_interval_seconds=poll_interval_seconds,
        ),
    )
    _validate_event(DashboardAdapter, event)
    return emit_resource(event, text=f"Local dashboard: {name or url}")


def emit_file_count(
    path: str | Path,
    *,
    pattern: str = "*",
    recursive: bool = False,
    logical_key: str | None = None,
    expected_count: int | None = None,
    completion_marker: str | None = None,
    settled_seconds: float | None = None,
    blocking: bool = False,
    poll_interval_seconds: float | None = 2.0,
) -> dict[str, Any]:
    value = str(path)
    if expected_count is not None and expected_count < 0:
        raise ValueError("expected_count must be nonnegative")
    if settled_seconds is not None and settled_seconds <= 0:
        raise ValueError("settled_seconds must be positive")
    if (
        blocking
        and expected_count is None
        and not completion_marker
        and settled_seconds is None
    ):
        raise ValueError(
            "blocking file-count monitors require expected_count, completion_marker, or settled_seconds"
        )
    event = ResourceEvent(
        resource=ResourceSpec(
            provider="local",
            type="file_count",
            id=value,
            logical_key=logical_key or f"{value}:{pattern}",
            ownership=Ownership.BORROWED,
            metadata={
                "pattern": pattern,
                "recursive": recursive,
                "expected_count": expected_count,
                "completion_marker": completion_marker,
                "settled_seconds": settled_seconds,
            },
        ),
        lifecycle=ResourceLifecycle(
            blocking=blocking,
            retain_logs=False,
            poll_interval_seconds=poll_interval_seconds,
        ),
    )
    _validate_event(FileCountAdapter, event)
    return emit_resource(event, text=f"Local files: {value}/{pattern}")


def emit_line_count(
    path: str | Path,
    *,
    logical_key: str | None = None,
    expected_lines: int | None = None,
    tail_lines: int = 100,
    blocking: bool = False,
    poll_interval_seconds: float | None = 2.0,
) -> dict[str, Any]:
    value = str(path)
    if expected_lines is not None and expected_lines < 0:
        raise ValueError("expected_lines must be nonnegative")
    if tail_lines < 0:
        raise ValueError("tail_lines must be nonnegative")
    if blocking and expected_lines is None:
        raise ValueError("blocking line-count monitors require expected_lines")
    event = ResourceEvent(
        resource=ResourceSpec(
            provider="local",
            type="line_count",
            id=value,
            logical_key=logical_key or value,
            ownership=Ownership.BORROWED,
            metadata={"expected_lines": expected_lines, "tail_lines": tail_lines},
        ),
        lifecycle=ResourceLifecycle(
            blocking=blocking,
            retain_logs=tail_lines > 0,
            poll_interval_seconds=poll_interval_seconds,
        ),
    )
    _validate_event(LineCountAdapter, event)
    return emit_resource(event, text=f"Local line count: {value}")
