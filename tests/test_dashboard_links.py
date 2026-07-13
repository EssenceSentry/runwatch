# pyright: reportArgumentType=false, reportUnknownMemberType=false, reportUnknownVariableType=false
from __future__ import annotations

import asyncio
from pathlib import Path
from urllib.parse import urlencode, urlsplit, urlunsplit

import httpx
import pytest
from websockets.asyncio.client import connect
from websockets.asyncio.server import ServerConnection, serve

from runwatch.dashboard_links import DashboardLinkManager
from runwatch.events import EventBus
from runwatch.storage import RunStore


def _store(root: Path) -> RunStore:
    source = root / "source.ipynb"
    source.write_text("{}", encoding="utf-8")
    store = RunStore(root / "state.sqlite3")
    store.initialize_run(
        run_id="run",
        name="demo",
        notebook_path=source,
        source_path=source,
        output_path=root / "out.ipynb",
        working_dir=root,
        run_dir=root,
        source_digest="digest",
    )
    return store


def _resource(origin: str, *, internal_id: str = "dashboard-1") -> dict[str, object]:
    return {
        "internal_id": internal_id,
        "provider": "local",
        "resource_type": "dashboard",
        "external_id": origin,
        "metadata": {"name": "Training UI"},
        "disposition": "active",
    }


async def _wait_for_link(
    manager: DashboardLinkManager, internal_id: str = "dashboard-1"
) -> dict[str, object]:
    for _ in range(250):
        description = manager.describe(internal_id)
        if description is not None and description["status"] != "starting":
            return description
        await asyncio.sleep(0.01)
    raise AssertionError("dashboard link did not become ready")


@pytest.mark.asyncio
async def test_none_link_is_durable_safe_and_recreatable(tmp_path: Path) -> None:
    store = _store(tmp_path)
    bus = EventBus(store, "run")
    resource = _resource("http://127.0.0.1:8501")
    first = DashboardLinkManager(
        access_token="secret",
        share="none",
        cloudflared_binary="cloudflared",
        bus=bus,
    )
    await first.reconcile([resource])
    description = await _wait_for_link(first)

    assert description == {
        "label": "Training UI",
        "status": "ready",
        "message": None,
        "href": "/api/resources/dashboard-1/open",
    }
    assert "secret" not in str(description)
    assert "8501" not in str(description)
    assert first.open_target("dashboard-1") == (
        "http://127.0.0.1:8501",
        False,
    )
    await first.close()

    recovered = DashboardLinkManager(
        access_token="new-secret",
        share="none",
        cloudflared_binary="cloudflared",
        bus=bus,
    )
    await recovered.reconcile([resource])
    assert (await _wait_for_link(recovered))["status"] == "ready"
    await recovered.close()
    store.close()


async def _http_origin() -> tuple[asyncio.Server, str, list[str]]:
    requests: list[str] = []
    origin = ""

    async def respond(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        raw = await reader.readuntil(b"\r\n\r\n")
        request = raw.decode("latin-1")
        requests.append(request)
        target = request.split(" ", 2)[1]
        if target.startswith("/redirect"):
            response = (
                "HTTP/1.1 302 Found\r\n"
                f"Location: {origin}/next?ok=1\r\n"
                "Content-Length: 0\r\nConnection: close\r\n\r\n"
            ).encode()
        else:
            body = request.encode()
            response = (
                "HTTP/1.1 200 OK\r\n"
                f"Content-Length: {len(body)}\r\n"
                "Content-Type: text/plain\r\n"
                "Set-Cookie: target_session=works; Domain=127.0.0.1; Path=/\r\n"
                "Set-Cookie: runwatch_access=upstream-evil; Path=/\r\n"
                "Connection: close\r\n\r\n"
            ).encode() + body
        writer.write(response)
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_server(respond, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    origin = f"http://127.0.0.1:{port}"
    return server, origin, requests


@pytest.mark.asyncio
async def test_lan_proxy_authenticates_and_does_not_leak_runwatch_credentials(
    tmp_path: Path,
) -> None:
    origin_server, origin, requests = await _http_origin()
    store = _store(tmp_path)
    manager = DashboardLinkManager(
        access_token="pairing-secret",
        share="lan",
        cloudflared_binary="cloudflared",
        bus=EventBus(store, "run"),
        lan_ip=lambda: "127.0.0.1",
    )
    await manager.reconcile([_resource(origin)])
    assert (await _wait_for_link(manager))["status"] == "ready"
    public_base, requires_token = manager.open_target("dashboard-1")
    assert requires_token is True

    async with httpx.AsyncClient(follow_redirects=False) as client:
        unauthorized = await client.get(public_base)
        client.cookies.set("upstream", "kept")
        paired = await client.get(
            f"{public_base}/hello?token=pairing-secret&keep=yes",
            headers={"authorization": "Bearer pairing-secret"},
        )
        forwarded = await client.get(f"{public_base}{paired.headers['location']}")
        redirected = await client.get(f"{public_base}/redirect")

    assert unauthorized.status_code == 401
    assert paired.status_code == 303
    assert paired.headers["location"] == "/hello?keep=yes"
    assert "runwatch_access=pairing-secret" in paired.headers["set-cookie"]
    assert forwarded.status_code == 200
    assert all(
        "Domain=" not in value for value in forwarded.headers.get_list("set-cookie")
    )
    assert all(
        "upstream-evil" not in value
        for value in forwarded.headers.get_list("set-cookie")
    )
    assert "GET /hello?keep=yes HTTP/1.1" in forwarded.text
    assert "upstream=kept" in forwarded.text
    assert "pairing-secret" not in requests[0]
    assert redirected.status_code == 302
    assert redirected.headers["location"] == f"{public_base}/next?ok=1"

    await manager.close()
    origin_server.close()
    await origin_server.wait_closed()
    store.close()


@pytest.mark.asyncio
async def test_lan_proxy_relays_websockets(tmp_path: Path) -> None:
    async def echo(connection: ServerConnection) -> None:
        async for message in connection:
            await connection.send(f"echo:{message}")

    upstream = await serve(echo, "127.0.0.1", 0)
    upstream_port = upstream.sockets[0].getsockname()[1]
    store = _store(tmp_path)
    manager = DashboardLinkManager(
        access_token="pairing-secret",
        share="lan",
        cloudflared_binary="cloudflared",
        bus=EventBus(store, "run"),
        lan_ip=lambda: "127.0.0.1",
    )
    await manager.reconcile([_resource(f"http://127.0.0.1:{upstream_port}")])
    await _wait_for_link(manager)
    public_base, _requires_token = manager.open_target("dashboard-1")
    parts = urlsplit(public_base)
    websocket_url = urlunsplit(
        ("ws", parts.netloc, "/socket", urlencode({"token": "pairing-secret"}), "")
    )

    async with connect(websocket_url, proxy=None) as websocket:
        await websocket.send("hello")
        assert await websocket.recv() == "echo:hello"

    await manager.close()
    upstream.close()
    await upstream.wait_closed()
    store.close()


class _FakeTunnel:
    def __init__(self) -> None:
        self.local_url: str | None = None
        self.closed = False

    async def start(self, local_url: str, *, timeout_seconds: float = 30.0) -> str:
        self.local_url = local_url
        return "https://share.trycloudflare.com"

    async def close(self) -> None:
        self.closed = True


@pytest.mark.asyncio
async def test_cloudflare_link_owns_and_closes_its_tunnel(tmp_path: Path) -> None:
    tunnel = _FakeTunnel()
    store = _store(tmp_path)
    manager = DashboardLinkManager(
        access_token="secret",
        share="cloudflared",
        cloudflared_binary="custom-cloudflared",
        bus=EventBus(store, "run"),
        tunnel_factory=lambda _binary: tunnel,
    )
    await manager.reconcile([_resource("http://127.0.0.1:8501")])
    assert (await _wait_for_link(manager))["status"] == "ready"

    assert tunnel.local_url is not None
    assert tunnel.local_url.startswith("http://127.0.0.1:")
    assert manager.open_target("dashboard-1") == (
        "https://share.trycloudflare.com",
        True,
    )
    await manager.reconcile([])
    assert manager.describe("dashboard-1") is None
    assert tunnel.closed is True
    await manager.close()
    store.close()
