# pyright: reportArgumentType=false, reportUnknownMemberType=false
from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from runwatch.models import AwsSettings, ResourceStatus
from runwatch.resources.base import ResourceConfigurationError
from runwatch.resources.dashboard import (
    DashboardAdapter,
    dashboard_health_url,
    validate_dashboard_url,
)


@pytest.mark.parametrize(
    "url",
    [
        "http://localhost:8501",
        "https://127.0.0.1:9443/app",
        "http://[::1]:3000",
    ],
)
def test_dashboard_url_accepts_only_explicit_loopback_targets(url: str) -> None:
    assert validate_dashboard_url(url) == url


@pytest.mark.parametrize(
    ("url", "message"),
    [
        ("https://example.com", "localhost or a loopback"),
        ("http://192.168.1.12:8000", "localhost or a loopback"),
        ("ftp://localhost/file", "http:// or https://"),
        ("http://user:password@localhost:8000", "embedded credentials"),
        ("http://localhost:8000/#secret", "fragment"),
        ("http://localhost:99999", "invalid port"),
    ],
)
def test_dashboard_url_rejects_unsafe_or_ambiguous_targets(
    url: str, message: str
) -> None:
    with pytest.raises(ResourceConfigurationError, match=message):
        validate_dashboard_url(url)


def test_dashboard_health_path_stays_on_registered_origin() -> None:
    assert (
        dashboard_health_url("http://localhost:8501/app?theme=dark", "/health")
        == "http://localhost:8501/health"
    )
    with pytest.raises(ResourceConfigurationError, match="absolute URL path"):
        dashboard_health_url("http://localhost:8501", "health")


async def _serve_status(status: int) -> tuple[asyncio.Server, str]:
    async def respond(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        await reader.readuntil(b"\r\n\r\n")
        writer.write(
            f"HTTP/1.1 {status} Test\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".encode()
        )
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_server(respond, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    return server, f"http://127.0.0.1:{port}"


@pytest.mark.asyncio
async def test_dashboard_adapter_reports_health_and_exact_expected_status(
    tmp_path: Path,
) -> None:
    server, origin = await _serve_status(204)
    monitor = DashboardAdapter(
        aws=None,
        aws_settings=AwsSettings(),
        working_dir=tmp_path,
    )
    try:
        healthy = await monitor.inspect(
            {
                "external_id": origin,
                "metadata": {"expected_status_code": 204},
            },
            {},
        )
        unexpected = await monitor.inspect(
            {
                "external_id": origin,
                "metadata": {"expected_status_code": 200},
            },
            {},
        )
    finally:
        server.close()
        await server.wait_closed()
        await monitor.close()

    assert healthy.status is ResourceStatus.RUNNING
    assert healthy.metrics["reachable"] is True
    assert healthy.metrics["http_status"] == 204
    assert unexpected.status is ResourceStatus.UNKNOWN
    assert unexpected.metrics["reachable"] is True
    assert unexpected.metrics["healthy"] is False


@pytest.mark.asyncio
async def test_dashboard_adapter_degrades_cleanly_when_target_disappears(
    tmp_path: Path,
) -> None:
    server, origin = await _serve_status(200)
    server.close()
    await server.wait_closed()
    monitor = DashboardAdapter(
        aws=None,
        aws_settings=AwsSettings(),
        working_dir=tmp_path,
    )
    try:
        observation = await monitor.inspect(
            {
                "external_id": origin,
                "metadata": {"request_timeout_seconds": 0.1},
            },
            {},
        )
    finally:
        await monitor.close()

    assert observation.status is ResourceStatus.UNKNOWN
    assert observation.metrics["reachable"] is False
    assert observation.metrics["healthy"] is False
    assert "unavailable" in (observation.message or "").lower()
