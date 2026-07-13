from __future__ import annotations

import asyncio
import hmac
import re
import socket
from collections.abc import AsyncGenerator, Callable
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass
from typing import Any, Literal, Protocol, cast
from urllib.parse import parse_qsl, quote, urlencode, urljoin, urlsplit, urlunsplit

import httpx
import uvicorn
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from starlette.background import BackgroundTask
from starlette.datastructures import Headers
from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed
from websockets.typing import Subprotocol

from .events import EventBus
from .models import ResourceDisposition
from .resources.dashboard import validate_dashboard_url
from .tunnel import CloudflaredTunnel, discover_lan_ip

DASHBOARD_ACCESS_COOKIE = "runwatch_access"
ShareMode = Literal["none", "lan", "cloudflared"]
_HOP_BY_HOP_HEADERS = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
}
_COOKIE_DOMAIN = re.compile(r";\s*domain=[^;]+", re.IGNORECASE)


class Tunnel(Protocol):
    """Runtime interface required from a Cloudflare tunnel."""

    async def start(self, local_url: str, *, timeout_seconds: float = 30.0) -> str: ...

    async def close(self) -> None: ...


TunnelFactory = Callable[[str], Tunnel]


def _valid_token(candidate: str | None, expected: str) -> bool:
    return candidate is not None and hmac.compare_digest(candidate, expected)


def _cookie_value(headers: Headers, name: str) -> str | None:
    for part in headers.get("cookie", "").split(";"):
        key, separator, value = part.strip().partition("=")
        if separator and key == name:
            return value
    return None


def _without_access_cookie(value: str) -> str:
    retained = [
        part.strip()
        for part in value.split(";")
        if part.strip().partition("=")[0] != DASHBOARD_ACCESS_COOKIE
    ]
    return "; ".join(part for part in retained if part)


def _is_authenticated(headers: Headers, query_token: str | None, token: str) -> bool:
    return _valid_token(query_token, token) or _valid_token(
        _cookie_value(headers, DASHBOARD_ACCESS_COOKIE), token
    )


def _target_url(
    origin: str,
    path: str,
    query_items: list[tuple[str, str]],
    *,
    websocket: bool = False,
) -> str:
    parts = urlsplit(origin)
    origin_path = parts.path.rstrip("/") + "/"
    encoded_path = quote(path, safe="/%:@-._~!$&'()*+,;=")
    target_path = urljoin(origin_path, encoded_path)
    query = urlencode(
        [*parse_qsl(parts.query, keep_blank_values=True), *query_items], doseq=True
    )
    scheme = ("wss" if parts.scheme == "https" else "ws") if websocket else parts.scheme
    return urlunsplit((scheme, parts.netloc, target_path, query, ""))


def _forward_headers(
    headers: Headers, token: str, origin: str
) -> list[tuple[str, str]]:
    forwarded: list[tuple[str, str]] = []
    for raw_key, raw_value in headers.raw:
        key = raw_key.decode("latin-1")
        value = raw_value.decode("latin-1")
        lowered = key.lower()
        if _drop_forwarded_header(lowered):
            continue
        sanitized = _sanitize_forwarded_value(lowered, value, token, origin)
        if sanitized is not None:
            forwarded.append((key, sanitized))
    return forwarded


def _drop_forwarded_header(name: str) -> bool:
    return name in _HOP_BY_HOP_HEADERS or name in {
        "host",
        "referer",
        "sec-websocket-accept",
        "sec-websocket-extensions",
        "sec-websocket-key",
        "sec-websocket-protocol",
        "sec-websocket-version",
    }


def _sanitize_forwarded_value(
    name: str, value: str, token: str, origin: str
) -> str | None:
    if name == "authorization" and value.lower().startswith("bearer "):
        if _valid_token(value[7:].strip(), token):
            return None
    if name == "cookie":
        return _without_access_cookie(value) or None
    if name == "origin":
        origin_parts = urlsplit(origin)
        return f"{origin_parts.scheme}://{origin_parts.netloc}"
    return value


def _rewrite_location(location: str, origin: str, request: Request) -> str:
    location_parts = urlsplit(location)
    origin_parts = urlsplit(origin)
    if not location_parts.netloc:
        return location
    if location_parts.scheme not in {"", "http", "https"} or (
        location_parts.netloc != origin_parts.netloc
    ):
        return location
    public = urlsplit(str(request.base_url))
    return urlunsplit(
        (
            public.scheme,
            public.netloc,
            location_parts.path,
            location_parts.query,
            location_parts.fragment,
        )
    )


def _copy_response_headers(
    upstream: httpx.Response, response: StreamingResponse, origin: str, request: Request
) -> None:
    for key, value in upstream.headers.multi_items():
        lowered = key.lower()
        if lowered in _HOP_BY_HOP_HEADERS:
            continue
        if lowered == "location":
            value = _rewrite_location(value, origin, request)
        elif lowered == "set-cookie":
            cookie_name = value.partition(";")[0].partition("=")[0].strip()
            if cookie_name == DASHBOARD_ACCESS_COOKIE:
                continue
            value = _COOKIE_DOMAIN.sub("", value)
        response.headers.append(key, value)


def _set_access_cookie(
    response: HTMLResponse | RedirectResponse | StreamingResponse,
    token: str,
    *,
    secure: bool,
) -> None:
    response.set_cookie(
        DASHBOARD_ACCESS_COOKIE,
        token,
        httponly=True,
        samesite="strict",
        secure=secure,
        max_age=60 * 60 * 24,
    )


async def _client_to_upstream(client: WebSocket, upstream: ClientConnection) -> None:
    try:
        while True:
            message = await client.receive()
            if message["type"] == "websocket.disconnect":
                return
            data = message.get("bytes")
            await upstream.send(
                data if data is not None else str(message.get("text", ""))
            )
    except WebSocketDisconnect:
        return


async def _upstream_to_client(upstream: ClientConnection, client: WebSocket) -> None:
    try:
        async for message in upstream:
            if isinstance(message, bytes):
                await client.send_bytes(message)
            else:
                await client.send_text(message)
    except ConnectionClosed:
        return


async def _relay_websocket(client: WebSocket, upstream: ClientConnection) -> None:
    tasks = {
        asyncio.create_task(_client_to_upstream(client, upstream)),
        asyncio.create_task(_upstream_to_client(upstream, client)),
    }
    _done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    for task in pending:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


def _query_without_token(items: list[tuple[str, str]]) -> list[tuple[str, str]]:
    return [(key, value) for key, value in items if key != "token"]


class _DashboardProxy:
    def __init__(self, origin: str, access_token: str, *, secure_cookie: bool) -> None:
        self.origin = validate_dashboard_url(origin)
        self.access_token = access_token
        self.secure_cookie = secure_cookie
        self.client = httpx.AsyncClient(follow_redirects=False, timeout=None)

    async def close(self) -> None:
        await self.client.aclose()

    async def http(
        self, request: Request, path: str = ""
    ) -> HTMLResponse | RedirectResponse | StreamingResponse:
        query_token = request.query_params.get("token")
        if not _is_authenticated(request.headers, query_token, self.access_token):
            return HTMLResponse(
                "<h1>Runwatch</h1><p>This linked dashboard requires its pairing URL.</p>",
                status_code=401,
            )
        query = _query_without_token(request.query_params.multi_items())
        if _valid_token(query_token, self.access_token):
            return self._pairing_redirect(request, query)
        return await self._forward_http(request, path, query)

    def _pairing_redirect(
        self, request: Request, query: list[tuple[str, str]]
    ) -> RedirectResponse:
        clean_location = urlunsplit(
            ("", "", request.url.path or "/", urlencode(query, doseq=True), "")
        )
        response = RedirectResponse(clean_location, status_code=303)
        _set_access_cookie(response, self.access_token, secure=self.secure_cookie)
        return response

    async def _forward_http(
        self, request: Request, path: str, query: list[tuple[str, str]]
    ) -> HTMLResponse | StreamingResponse:
        target = _target_url(self.origin, path, query)
        try:
            upstream_request = self.client.build_request(
                request.method,
                target,
                headers=_forward_headers(
                    request.headers, self.access_token, self.origin
                ),
                content=request.stream(),
            )
            upstream = await self.client.send(upstream_request, stream=True)
        except httpx.RequestError as error:
            return HTMLResponse(
                f"<h1>Linked dashboard unavailable</h1><p>{type(error).__name__}</p>",
                status_code=502,
            )

        response = StreamingResponse(
            upstream.aiter_raw(),
            status_code=upstream.status_code,
            background=BackgroundTask(upstream.aclose),
        )
        _copy_response_headers(upstream, response, self.origin, request)
        return response

    async def websocket(self, websocket: WebSocket, path: str = "") -> None:
        query_token = websocket.query_params.get("token")
        if not _is_authenticated(websocket.headers, query_token, self.access_token):
            await websocket.close(code=1008)
            return
        query = _query_without_token(websocket.query_params.multi_items())
        target = _target_url(self.origin, path, query, websocket=True)
        try:
            await self._forward_websocket(websocket, target, query_token)
        except Exception:
            with suppress(RuntimeError):
                await websocket.close(code=1011)

    async def _forward_websocket(
        self, websocket: WebSocket, target: str, query_token: str | None
    ) -> None:
        requested_protocols = [
            Subprotocol(value.strip())
            for value in websocket.headers.get("sec-websocket-protocol", "").split(",")
            if value.strip()
        ]
        async with connect(
            target,
            additional_headers=_forward_headers(
                websocket.headers, self.access_token, self.origin
            ),
            subprotocols=requested_protocols or None,
            proxy=None,
            max_size=None,
        ) as upstream:
            await websocket.accept(
                subprotocol=upstream.subprotocol,
                headers=self._websocket_pairing_headers(query_token),
            )
            await _relay_websocket(websocket, upstream)

    def _websocket_pairing_headers(
        self, query_token: str | None
    ) -> list[tuple[bytes, bytes]] | None:
        if not _valid_token(query_token, self.access_token):
            return None
        cookie = (
            f"{DASHBOARD_ACCESS_COOKIE}={self.access_token}; Path=/; "
            "Max-Age=86400; HttpOnly; SameSite=Strict"
            + ("; Secure" if self.secure_cookie else "")
        )
        return [(b"set-cookie", cookie.encode("latin-1"))]


def create_dashboard_proxy_app(
    origin: str,
    access_token: str,
    *,
    secure_cookie: bool,
) -> FastAPI:
    """Create an authenticated root-mounted reverse proxy for one dashboard."""
    proxy = _DashboardProxy(origin, access_token, secure_cookie=secure_cookie)

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncGenerator[None, None]:
        try:
            yield
        finally:
            await proxy.close()

    app = FastAPI(docs_url=None, redoc_url=None, lifespan=lifespan)
    methods = ["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"]
    app.add_api_route("/", proxy.http, methods=methods, response_model=None)
    app.add_api_route("/{path:path}", proxy.http, methods=methods, response_model=None)
    app.add_api_websocket_route("/", proxy.websocket)
    app.add_api_websocket_route("/{path:path}", proxy.websocket)
    return app


async def _wait_for_server(server: uvicorn.Server, task: asyncio.Task[None]) -> None:
    while not server.started:
        if task.done():
            await task
            raise RuntimeError("Linked dashboard proxy exited during startup")
        await asyncio.sleep(0.02)


class DashboardProxyRuntime:
    """Own one authenticated proxy and its optional Cloudflare tunnel."""

    def __init__(
        self,
        *,
        origin: str,
        access_token: str,
        share: ShareMode,
        cloudflared_binary: str,
        lan_ip: Callable[[], str],
        tunnel_factory: TunnelFactory,
    ) -> None:
        self.origin = origin
        self.access_token = access_token
        self.share: ShareMode = share
        self.cloudflared_binary = cloudflared_binary
        self.lan_ip = lan_ip
        self.tunnel_factory = tunnel_factory
        self.server: uvicorn.Server | None = None
        self.server_task: asyncio.Task[None] | None = None
        self.tunnel: Tunnel | None = None
        self.public_base: str | None = None

    async def start(self) -> str:
        bind_host = "0.0.0.0" if self.share == "lan" else "127.0.0.1"
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind((bind_host, 0))
        listener.listen(128)
        listener.setblocking(False)
        port = int(listener.getsockname()[1])
        app = create_dashboard_proxy_app(
            self.origin,
            self.access_token,
            secure_cookie=self.share == "cloudflared",
        )
        self.server = uvicorn.Server(
            uvicorn.Config(
                app,
                host=bind_host,
                port=port,
                log_level="warning",
                access_log=False,
                proxy_headers=True,
                forwarded_allow_ips="*",
            )
        )
        self.server.install_signal_handlers = lambda: None  # type: ignore[method-assign]
        self.server_task = asyncio.create_task(
            self.server.serve(sockets=[listener]), name="runwatch-dashboard-proxy"
        )
        try:
            await _wait_for_server(self.server, self.server_task)
            local_base = f"http://127.0.0.1:{port}"
            if self.share == "lan":
                self.public_base = f"http://{self.lan_ip()}:{port}"
            else:
                self.tunnel = self.tunnel_factory(self.cloudflared_binary)
                self.public_base = await self.tunnel.start(local_base)
            return self.public_base
        except BaseException:
            await self.close()
            raise

    async def close(self) -> None:
        if self.tunnel is not None:
            with suppress(Exception):
                await self.tunnel.close()
            self.tunnel = None
        if self.server is not None:
            self.server.should_exit = True
        if self.server_task is not None:
            await asyncio.gather(self.server_task, return_exceptions=True)
            self.server_task = None
        self.server = None
        self.public_base = None


@dataclass
class _ManagedLink:
    internal_id: str
    origin: str
    label: str
    status: Literal["starting", "ready", "failed"] = "starting"
    message: str | None = None
    public_base: str | None = None
    requires_token: bool = False
    runtime: DashboardProxyRuntime | None = None
    task: asyncio.Task[None] | None = None


class DashboardLinkManager:
    """Reconcile durable dashboard resources with ephemeral authenticated shares."""

    def __init__(
        self,
        *,
        access_token: str,
        share: ShareMode,
        cloudflared_binary: str,
        bus: EventBus,
        lan_ip: Callable[[], str] = discover_lan_ip,
        tunnel_factory: TunnelFactory = CloudflaredTunnel,
    ) -> None:
        self.access_token = access_token
        self.share: ShareMode = share
        self.cloudflared_binary = cloudflared_binary
        self.bus = bus
        self.lan_ip = lan_ip
        self.tunnel_factory = tunnel_factory
        self._links: dict[str, _ManagedLink] = {}
        self._closing = False

    async def reconcile(self, resources: list[dict[str, Any]]) -> None:
        active = {
            str(resource["internal_id"]): resource
            for resource in resources
            if resource["provider"] == "local"
            and resource["resource_type"] == "dashboard"
            and resource["disposition"] == ResourceDisposition.ACTIVE.value
        }
        for internal_id in set(self._links) - set(active):
            await self._remove(internal_id)
        for internal_id, resource in active.items():
            metadata = cast(dict[str, Any], resource.get("metadata", {}))
            label = str(metadata.get("name") or resource["external_id"])
            origin = validate_dashboard_url(str(resource["external_id"]))
            existing = self._links.get(internal_id)
            if existing is not None and existing.origin == origin:
                existing.label = label
                continue
            if existing is not None:
                await self._remove(internal_id)
            link = _ManagedLink(
                internal_id=internal_id,
                origin=origin,
                label=label,
            )
            self._links[internal_id] = link
            link.task = asyncio.create_task(
                self._start(link), name=f"dashboard-link:{internal_id}"
            )

    async def _start(self, link: _ManagedLink) -> None:
        try:
            if self.share == "none":
                link.public_base = link.origin
                link.requires_token = False
            else:
                runtime = DashboardProxyRuntime(
                    origin=link.origin,
                    access_token=self.access_token,
                    share=self.share,
                    cloudflared_binary=self.cloudflared_binary,
                    lan_ip=self.lan_ip,
                    tunnel_factory=self.tunnel_factory,
                )
                link.runtime = runtime
                link.public_base = await runtime.start()
                link.requires_token = True
            link.status = "ready"
            link.message = None
            await self._publish("resource.link_ready", link)
        except asyncio.CancelledError:
            if link.runtime is not None:
                await link.runtime.close()
            raise
        except Exception as error:
            if link.runtime is not None:
                await link.runtime.close()
            link.status = "failed"
            link.message = f"{type(error).__name__}: {error}"
            await self._publish("resource.link_failed", link)

    async def _publish(self, event_type: str, link: _ManagedLink) -> None:
        if self._closing:
            return
        try:
            await self.bus.publish(
                event_type,
                {
                    "internal_id": link.internal_id,
                    "status": link.status,
                    "message": link.message,
                },
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            # Link availability must not depend on optional event delivery.
            return

    async def _remove(self, internal_id: str) -> None:
        link = self._links.pop(internal_id, None)
        if link is None:
            return
        if link.task is not None and not link.task.done():
            link.task.cancel()
            await asyncio.gather(link.task, return_exceptions=True)
        if link.runtime is not None:
            await link.runtime.close()

    def describe(self, internal_id: str) -> dict[str, Any] | None:
        link = self._links.get(internal_id)
        if link is None:
            return None
        if (
            link.status == "ready"
            and link.runtime is not None
            and link.runtime.server_task is not None
            and link.runtime.server_task.done()
        ):
            link.status = "failed"
            link.message = "Linked dashboard proxy stopped unexpectedly"
            link.public_base = None
        return {
            "label": link.label,
            "status": link.status,
            "message": link.message,
            "href": (
                f"/api/resources/{internal_id}/open" if link.status == "ready" else None
            ),
        }

    def open_target(self, internal_id: str) -> tuple[str, bool]:
        link = self._links.get(internal_id)
        if link is None:
            raise KeyError(internal_id)
        if link.status != "ready" or link.public_base is None:
            raise RuntimeError(link.message or "Linked dashboard share is not ready")
        return link.public_base, link.requires_token

    async def close(self) -> None:
        self._closing = True
        for internal_id in list(self._links):
            await self._remove(internal_id)
