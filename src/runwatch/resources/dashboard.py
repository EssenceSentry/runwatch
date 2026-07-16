from __future__ import annotations

import ipaddress
import time
from pathlib import Path
from typing import cast
from urllib.parse import urljoin, urlsplit

import httpx

from ..models import AwsSettings, ResourceEvent, ResourceObservation, ResourceStatus
from .base import (
    AdapterContext,
    AwsClientProvider,
    ResourceAdapter,
    ResourceConfigurationError,
)


def validate_dashboard_url(value: str) -> str:
    """Validate and return a loopback HTTP dashboard URL."""
    candidate = value.strip()
    parts = urlsplit(candidate)
    if parts.scheme not in {"http", "https"}:
        raise ResourceConfigurationError(
            "local.dashboard requires an http:// or https:// URL"
        )
    if not parts.hostname or parts.username or parts.password:
        raise ResourceConfigurationError(
            "local.dashboard requires a host without embedded credentials"
        )
    hostname = parts.hostname.rstrip(".").lower()
    if hostname != "localhost":
        try:
            address = ipaddress.ip_address(hostname)
        except ValueError as error:
            raise ResourceConfigurationError(
                "local.dashboard accepts only localhost or a loopback IP address"
            ) from error
        if not address.is_loopback:
            raise ResourceConfigurationError(
                "local.dashboard accepts only localhost or a loopback IP address"
            )
    try:
        parts.port
    except ValueError as error:
        raise ResourceConfigurationError(
            "local.dashboard has an invalid port"
        ) from error
    if parts.fragment:
        raise ResourceConfigurationError(
            "local.dashboard URLs cannot contain a fragment"
        )
    return candidate


def dashboard_health_url(origin: str, health_path: str | None) -> str:
    """Return the health endpoint for a validated dashboard origin."""
    if health_path is None:
        return origin
    if not health_path.startswith("/") or urlsplit(health_path).netloc:
        raise ResourceConfigurationError("health_path must be an absolute URL path")
    parts = urlsplit(origin)
    root = f"{parts.scheme}://{parts.netloc}/"
    return urljoin(root, health_path.lstrip("/"))


class DashboardAdapter(ResourceAdapter):
    """Monitor the availability of a linked localhost dashboard."""

    provider = "local"
    resource_type = "dashboard"

    def __init__(
        self,
        context: AdapterContext | None = None,
        *,
        aws: AwsClientProvider | None = None,
        aws_settings: AwsSettings | None = None,
        working_dir: Path | None = None,
    ) -> None:
        super().__init__(
            context,
            aws=aws,
            aws_settings=aws_settings,
            working_dir=working_dir,
        )
        self._client = httpx.AsyncClient(follow_redirects=False)

    @classmethod
    def validate_registration(cls, event: ResourceEvent) -> None:
        super().validate_registration(event)
        metadata = event.resource.metadata
        validate_dashboard_url(event.resource.id)
        dashboard_health_url(
            event.resource.id,
            (
                str(metadata["health_path"])
                if metadata.get("health_path") is not None
                else None
            ),
        )
        name = metadata.get("name")
        if name is not None and (not isinstance(name, str) or not name.strip()):
            raise ResourceConfigurationError("dashboard name must be non-empty")
        timeout_seconds = metadata.get("request_timeout_seconds", 5.0)
        if (
            not isinstance(timeout_seconds, (int, float))
            or isinstance(timeout_seconds, bool)
            or timeout_seconds <= 0
        ):
            raise ResourceConfigurationError("request_timeout_seconds must be positive")
        expected = metadata.get("expected_status_code")
        if expected is not None and (
            not isinstance(expected, int)
            or isinstance(expected, bool)
            or expected < 100
            or expected > 599
        ):
            raise ResourceConfigurationError(
                "expected_status_code must be between 100 and 599"
            )

    async def inspect(
        self, resource: dict[str, object], cursor: dict[str, object]
    ) -> ResourceObservation:
        raw_metadata = resource.get("metadata", {})
        if not isinstance(raw_metadata, dict):
            raise ResourceConfigurationError("dashboard metadata must be an object")
        metadata = cast(dict[str, object], raw_metadata)
        origin = validate_dashboard_url(str(resource["external_id"]))
        configured_health = metadata.get("health_path")
        health_url = dashboard_health_url(
            origin, str(configured_health) if configured_health is not None else None
        )
        timeout_value = metadata.get("request_timeout_seconds", 5.0)
        if not isinstance(timeout_value, (int, float)) or isinstance(
            timeout_value, bool
        ):
            raise ResourceConfigurationError("request_timeout_seconds must be numeric")
        timeout_seconds = float(timeout_value)
        started = time.perf_counter()
        try:
            async with self._client.stream(
                "GET", health_url, timeout=timeout_seconds
            ) as response:
                status_code = response.status_code
        except httpx.RequestError as error:
            elapsed = time.perf_counter() - started
            return ResourceObservation(
                status=ResourceStatus.UNKNOWN,
                message=f"Dashboard unavailable: {type(error).__name__}: {error}",
                metrics={
                    "reachable": False,
                    "healthy": False,
                    "response_time_seconds": elapsed,
                },
            )

        elapsed = time.perf_counter() - started
        expected = metadata.get("expected_status_code")
        if expected is not None and (
            not isinstance(expected, int) or isinstance(expected, bool)
        ):
            raise ResourceConfigurationError("expected_status_code must be an integer")
        healthy = (
            status_code == expected
            if expected is not None
            else 200 <= status_code < 400
        )
        return ResourceObservation(
            status=ResourceStatus.RUNNING if healthy else ResourceStatus.UNKNOWN,
            message=(
                f"Dashboard reachable (HTTP {status_code})"
                if healthy
                else f"Dashboard health check returned HTTP {status_code}"
            ),
            metrics={
                "reachable": True,
                "healthy": healthy,
                "http_status": status_code,
                "response_time_seconds": elapsed,
            },
        )

    async def close(self) -> None:
        failures: list[Exception] = []
        try:
            await self._client.aclose()
        except Exception as error:
            failures.append(error)
        try:
            await super().close()
        except Exception as error:
            failures.append(error)
        if len(failures) == 1:
            raise failures[0]
        if failures:
            raise RuntimeError(
                "Dashboard adapter cleanup failed: "
                + "; ".join(str(error) for error in failures)
            ) from failures[0]
