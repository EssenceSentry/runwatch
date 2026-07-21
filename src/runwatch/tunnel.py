from __future__ import annotations

import asyncio
import re
import socket
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Literal
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import httpx

from ._compat import timeout

_CLOUDFLARED_URL = re.compile(r"https://[a-z0-9-]+\.trycloudflare\.com")

DashboardShareStatus = Literal["starting", "ready", "rotating", "degraded", "closed"]


@dataclass(frozen=True)
class DashboardShareSnapshot:
    status: DashboardShareStatus
    public_url: str | None
    generation: int
    message: str | None


class DashboardShareState:
    """Current public dashboard address, shared with the local web app."""

    def __init__(self) -> None:
        self._status: DashboardShareStatus = "starting"
        self._public_url: str | None = None
        self._generation = 0
        self._message: str | None = "Cloudflare tunnel is starting."

    def snapshot(self) -> DashboardShareSnapshot:
        return DashboardShareSnapshot(
            status=self._status,
            public_url=self._public_url,
            generation=self._generation,
            message=self._message,
        )

    def ready(self, public_url: str) -> None:
        self._status = "ready"
        self._public_url = public_url.rstrip("/")
        self._generation += 1
        self._message = None

    def rotating(self) -> None:
        self._status = "rotating"
        self._message = "Cloudflare link stopped responding; replacing it."

    def degraded(self) -> None:
        self._status = "degraded"
        self._message = "Cloudflare link replacement failed; Runwatch will retry."

    def close(self) -> None:
        self._status = "closed"
        self._message = "Cloudflare sharing has stopped."


def discover_lan_ip() -> str:
    """Best-effort discovery of the LAN address used for outbound traffic."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # UDP connect does not send traffic; it only asks the OS to select a route.
        sock.connect(("8.8.8.8", 80))
        return str(sock.getsockname()[0])
    except OSError:
        try:
            return socket.gethostbyname(socket.gethostname())
        except OSError:
            return "127.0.0.1"
    finally:
        sock.close()


def with_token(url: str, token: str) -> str:
    parts = urlsplit(url)
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    query["token"] = token
    return urlunsplit(
        (
            parts.scheme,
            parts.netloc,
            parts.path or "/",
            urlencode(query),
            parts.fragment,
        )
    )


class CloudflaredTunnel:
    """Own a cloudflared quick-tunnel process for the lifetime of a run.

    cloudflared keeps writing diagnostics after it announces the public URL. The
    drain task is essential for long runs: leaving stdout unread can eventually
    fill the subprocess pipe and stall cloudflared.
    """

    def __init__(
        self, binary: str = "cloudflared", *, retained_lines: int = 100
    ) -> None:
        self.binary = binary
        self.process: asyncio.subprocess.Process | None = None
        self.output: deque[str] = deque(maxlen=retained_lines)
        self._drain_task: asyncio.Task[None] | None = None

    @property
    def running(self) -> bool:
        return self.process is not None and self.process.returncode is None

    async def start(self, local_url: str, *, timeout_seconds: float = 30.0) -> str:
        if self.process is not None:
            raise RuntimeError("cloudflared tunnel has already been started")

        try:
            self.process = await asyncio.create_subprocess_exec(
                self.binary,
                "tunnel",
                "--url",
                local_url,
                "--no-autoupdate",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )
        except FileNotFoundError as error:
            raise RuntimeError(
                f"cloudflared binary {self.binary!r} was not found; install it or use "
                "--share lan/none"
            ) from error

        assert self.process.stdout is not None
        try:
            async with timeout(timeout_seconds):
                while line := await self.process.stdout.readline():
                    text = line.decode("utf-8", errors="replace").rstrip()
                    self.output.append(text)
                    match = _CLOUDFLARED_URL.search(text)
                    if match:
                        self._drain_task = asyncio.create_task(
                            self._drain_output(), name="runwatch-cloudflared-output"
                        )
                        return match.group(0)
                    if self.process.returncode is not None:
                        break
        except asyncio.CancelledError:
            await self.close()
            raise
        except TimeoutError:
            await self.close()
            raise RuntimeError(
                "cloudflared did not publish a tunnel URL; recent output: "
                + " | ".join(self.output)
            ) from None

        await self.close()
        raise RuntimeError(
            "cloudflared exited before publishing a URL: " + " | ".join(self.output)
        )

    async def _drain_output(self) -> None:
        process = self.process
        if process is None or process.stdout is None:
            return
        try:
            while line := await process.stdout.readline():
                self.output.append(line.decode("utf-8", errors="replace").rstrip())
        except asyncio.CancelledError:
            raise
        except (OSError, RuntimeError):
            # The process is already being torn down. Diagnostics are best effort.
            return

    async def close(self) -> None:
        process = self.process
        if process is None:
            return

        if process.returncode is None:
            process.terminate()
            try:
                async with timeout(5):
                    await process.wait()
            except TimeoutError:
                process.kill()
                await process.wait()

        if self._drain_task is not None:
            try:
                async with timeout(1):
                    await self._drain_task
            except TimeoutError:
                self._drain_task.cancel()
                await asyncio.gather(self._drain_task, return_exceptions=True)
            self._drain_task = None
        self.process = None


class CloudflaredShare:
    """Keep a Quick Tunnel healthy while preserving the local Runwatch server."""

    def __init__(
        self,
        *,
        local_url: str,
        binary: str = "cloudflared",
        state: DashboardShareState | None = None,
        on_rotation: Callable[[str], Awaitable[None]] | None = None,
        health_interval_seconds: float = 30.0,
        failure_threshold: int = 3,
        health_check: Callable[[str], Awaitable[bool]] | None = None,
        tunnel_factory: Callable[[], CloudflaredTunnel] | None = None,
    ) -> None:
        self.local_url = local_url
        self.state = state or DashboardShareState()
        self.on_rotation = on_rotation
        self.health_interval_seconds = health_interval_seconds
        self.failure_threshold = failure_threshold
        self._health_check = health_check
        self._tunnel_factory = tunnel_factory or (lambda: CloudflaredTunnel(binary))
        self._tunnel: CloudflaredTunnel | None = None
        self._monitor_task: asyncio.Task[None] | None = None
        self._client = httpx.AsyncClient(timeout=10.0, follow_redirects=False)

    async def start(self) -> str:
        public_url = await self._replace(initial=True)
        self._monitor_task = asyncio.create_task(
            self._monitor(), name="runwatch-cloudflared-health"
        )
        return public_url

    async def _monitor(self) -> None:
        failures = 0
        while True:
            await asyncio.sleep(self.health_interval_seconds)
            if await self._healthy():
                failures = 0
                continue
            failures += 1
            if failures < self.failure_threshold:
                continue
            self.state.rotating()
            try:
                await self._replace(initial=False)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.state.degraded()
                failures = self.failure_threshold - 1
            else:
                failures = 0

    async def _healthy(self) -> bool:
        snapshot = self.state.snapshot()
        if self._tunnel is None or not self._tunnel.running:
            return False
        if snapshot.public_url is None:
            return False
        try:
            if self._health_check is not None:
                return await self._health_check(snapshot.public_url)
            response = await self._client.get(f"{snapshot.public_url}/health")
            return response.status_code == 200
        except asyncio.CancelledError:
            raise
        except Exception:
            return False

    async def _replace(self, *, initial: bool) -> str:
        candidate = self._tunnel_factory()
        public_url = await candidate.start(self.local_url)
        previous = self._tunnel
        self._tunnel = candidate
        self.state.ready(public_url)
        if previous is not None:
            await previous.close()
        if not initial and self.on_rotation is not None:
            try:
                await self.on_rotation(public_url)
            except asyncio.CancelledError:
                raise
            except Exception:
                pass
        return public_url

    async def close(self) -> None:
        if self._monitor_task is not None:
            self._monitor_task.cancel()
            await asyncio.gather(self._monitor_task, return_exceptions=True)
            self._monitor_task = None
        if self._tunnel is not None:
            await self._tunnel.close()
            self._tunnel = None
        await self._client.aclose()
        self.state.close()
