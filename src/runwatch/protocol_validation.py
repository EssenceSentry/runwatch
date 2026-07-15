"""Dependency-light validation shared by emitters and supervisor adapters."""

from __future__ import annotations

import ipaddress
import math
from pathlib import Path
from typing import Any, cast
from urllib.parse import urljoin, urlparse, urlsplit

_CLOUDWATCH_STATISTICS = {
    "Average",
    "Sum",
    "Minimum",
    "Maximum",
    "SampleCount",
}


def parse_s3_uri(uri: str) -> tuple[str, str]:
    """Parse an S3 URI without importing an AWS SDK."""

    parsed = urlparse(uri)
    if (
        parsed.scheme != "s3"
        or not parsed.netloc
        or parsed.params
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError(f"Invalid S3 URI: {uri!r}")
    return parsed.netloc, parsed.path.lstrip("/")


def _required_text(metadata: dict[str, Any], field: str, kind: str) -> None:
    value = metadata.get(field)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{kind} {field} must not be empty")


def _positive_integer(
    metadata: dict[str, Any], field: str, default: int, kind: str
) -> int:
    value = metadata.get(field, default)
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{kind} {field} must be a positive integer")
    return value


def validate_cloudwatch_metric_configuration(metadata: dict[str, Any]) -> None:
    """Validate CloudWatch metric metadata without importing an AWS SDK."""

    kind = "aws.cloudwatch_metric"
    for field in ("namespace", "metric_name"):
        _required_text(metadata, field, kind)
    statistic = metadata.get("statistic", "Average")
    if statistic not in _CLOUDWATCH_STATISTICS:
        raise ValueError(f"{kind} statistic is not supported")
    period = _positive_integer(metadata, "period_seconds", 60, kind)
    if period not in {1, 5, 10, 20, 30} and period % 60:
        raise ValueError(
            f"{kind} period_seconds must be 1, 5, 10, 20, 30, or a multiple of 60"
        )
    lookback = _positive_integer(metadata, "lookback_seconds", 900, kind)
    if (lookback + period - 1) // period > 1_440:
        raise ValueError(f"{kind} lookback produces more than 1440 datapoints")
    dimensions = metadata.get("dimensions", {})
    if not isinstance(dimensions, dict):
        raise ValueError(f"{kind} dimensions must be a mapping with at most 30 entries")
    typed_dimensions = cast(dict[Any, Any], dimensions)
    if len(typed_dimensions) > 30:
        raise ValueError(f"{kind} dimensions must be a mapping with at most 30 entries")
    if any(
        not isinstance(key, str)
        or not key.strip()
        or not isinstance(value, str)
        or not value.strip()
        for key, value in typed_dimensions.items()
    ):
        raise ValueError(f"{kind} dimension names and values must not be empty")


def validate_cloudwatch_logs_configuration(metadata: dict[str, Any]) -> int | None:
    """Validate CloudWatch Logs metadata without importing an AWS SDK."""

    log_group = metadata.get("log_group")
    if not isinstance(log_group, str) or not log_group.strip():
        raise ValueError("aws.cloudwatch_logs log_group must not be empty")
    configured = metadata.get("max_streams")
    if configured is None:
        return None
    if isinstance(configured, bool) or not isinstance(configured, int):
        raise ValueError("aws.cloudwatch_logs max_streams must be an integer")
    if not 1 <= configured <= 100:
        raise ValueError("aws.cloudwatch_logs max_streams must be between 1 and 100")
    return configured


def validate_dashboard_url(value: str) -> str:
    """Validate and return a loopback HTTP dashboard URL."""

    candidate = value.strip()
    parts = urlsplit(candidate)
    if parts.scheme not in {"http", "https"}:
        raise ValueError("local.dashboard requires an http:// or https:// URL")
    if not parts.hostname or parts.username or parts.password:
        raise ValueError("local.dashboard requires a host without embedded credentials")
    hostname = parts.hostname.rstrip(".").lower()
    if hostname != "localhost":
        try:
            address = ipaddress.ip_address(hostname)
        except ValueError as error:
            raise ValueError(
                "local.dashboard accepts only localhost or a loopback IP address"
            ) from error
        if not address.is_loopback:
            raise ValueError(
                "local.dashboard accepts only localhost or a loopback IP address"
            )
    try:
        parts.port
    except ValueError as error:
        raise ValueError("local.dashboard has an invalid port") from error
    if parts.fragment:
        raise ValueError("local.dashboard URLs cannot contain a fragment")
    return candidate


def dashboard_health_url(origin: str, health_path: str | None) -> str:
    """Return a dashboard health endpoint from its validated origin."""

    if health_path is None:
        return origin
    if not health_path.startswith("/") or urlsplit(health_path).netloc:
        raise ValueError("health_path must be an absolute URL path")
    parts = urlsplit(origin)
    root = f"{parts.scheme}://{parts.netloc}/"
    return urljoin(root, health_path.lstrip("/"))


def validate_dashboard_configuration(
    url: str, metadata: dict[str, Any]
) -> tuple[str, str]:
    """Validate dashboard registration values used by both process roles."""

    origin = validate_dashboard_url(url)
    health_path = metadata.get("health_path")
    health_url = dashboard_health_url(
        origin, str(health_path) if health_path is not None else None
    )
    name = metadata.get("name")
    if name is not None and (not isinstance(name, str) or not name.strip()):
        raise ValueError("dashboard name must be non-empty")
    timeout_seconds = metadata.get("request_timeout_seconds", 5.0)
    if (
        not isinstance(timeout_seconds, (int, float))
        or isinstance(timeout_seconds, bool)
        or not math.isfinite(float(timeout_seconds))
        or timeout_seconds <= 0
    ):
        raise ValueError("request_timeout_seconds must be a positive finite number")
    expected = metadata.get("expected_status_code")
    if expected is not None and (
        not isinstance(expected, int)
        or isinstance(expected, bool)
        or expected < 100
        or expected > 599
    ):
        raise ValueError("expected_status_code must be between 100 and 599")
    return origin, health_url


def validate_file_count_configuration(metadata: dict[str, Any]) -> None:
    """Validate dependency-free file-count registration metadata."""

    expected = metadata.get("expected_count")
    if expected is not None and (
        isinstance(expected, bool) or not isinstance(expected, int) or expected < 0
    ):
        raise ValueError("local.file_count expected_count must be nonnegative")
    settled = metadata.get("settled_seconds")
    if settled is not None and (
        isinstance(settled, bool)
        or not isinstance(settled, (int, float))
        or not math.isfinite(float(settled))
        or settled <= 0
    ):
        raise ValueError(
            "local.file_count settled_seconds must be a positive finite number"
        )
    pattern = metadata.get("pattern", "*")
    if not isinstance(pattern, str) or not pattern:
        raise ValueError("local.file_count pattern must not be empty")
    _validate_relative_path(pattern, "pattern")
    marker = metadata.get("completion_marker")
    if marker is not None:
        if not isinstance(marker, str):
            raise ValueError("local.file_count completion_marker must be a string")
        if marker:
            _validate_relative_path(marker, "completion_marker")


def _validate_relative_path(value: str, field: str) -> None:
    path = Path(value)
    if path.is_absolute() or ".." in path.parts:
        raise ValueError(
            f"local.file_count {field} must stay within the monitored directory"
        )


def validate_line_count_configuration(metadata: dict[str, Any]) -> None:
    """Validate dependency-free line-count registration metadata."""

    expected = metadata.get("expected_lines")
    if expected is not None and (
        isinstance(expected, bool) or not isinstance(expected, int) or expected < 0
    ):
        raise ValueError("local.line_count expected_lines must be nonnegative")
    tail_lines = metadata.get("tail_lines", 100)
    if (
        isinstance(tail_lines, bool)
        or not isinstance(tail_lines, int)
        or tail_lines < 0
    ):
        raise ValueError("local.line_count tail_lines must be nonnegative")
