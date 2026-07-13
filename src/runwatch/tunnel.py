from __future__ import annotations

import asyncio
import re
import socket
from collections import deque
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from ._compat import timeout

_CLOUDFLARED_URL = re.compile(r"https://[a-z0-9-]+\.trycloudflare\.com")


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
