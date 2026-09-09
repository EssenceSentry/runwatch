"""Owned Marimo subprocess, authenticated session transport, and bounded control."""

from __future__ import annotations

import asyncio
import base64
import contextlib
import html
import json
import os
import re
import secrets
import signal
import socket
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any, BinaryIO, cast

import httpx
import websockets.asyncio.client

from ._backends import python_executable
from ._fs import atomic_write_bytes, ensure_private_directory
from ._marimo_code import CAPABILITIES, INVENTORY, parse_result
from ._marimo_document import WORKER
from .models import NotebookSettings
from .storage import process_is_alive, process_start_time


class MarimoTransportError(RuntimeError):
    """The runtime outcome is uncertain; execution must not be retried implicitly."""


class MarimoSession:
    def __init__(
        self,
        run_dir: Path,
        working_dir: Path,
        settings: NotebookSettings,
        on_message: Callable[[dict[str, Any]], Awaitable[None]],
    ) -> None:
        self.directory = run_dir / "marimo-runtime"
        self.working_dir = working_dir
        self.settings = settings
        self.on_message = on_message
        self.source_path = self.directory / "runtime.py"
        self.identity_path = self.directory / "process.json"
        self.process: asyncio.subprocess.Process | None = None
        self.client: httpx.AsyncClient | None = None
        self.socket: websockets.asyncio.client.ClientConnection | None = None
        self.reader: asyncio.Task[None] | None = None
        self.log: BinaryIO | None = None
        self._secret = secrets.token_urlsafe(32)
        self._session_id = secrets.token_hex(16)
        self._closing = False
        self._identity: dict[str, Any] | None = None
        self._assets: dict[str, str] = {}
        self._asset_bytes = 0
        self._execution_lock = asyncio.Lock()
        self._barriers: dict[str, asyncio.Event] = {}

    async def start(self, source: str) -> dict[str, Any]:
        ensure_private_directory(self.directory)
        await self.reap_previous()
        atomic_write_bytes(self.source_path, source.encode(), preserve_mode=False)
        token_path = self.directory / "token"
        atomic_write_bytes(token_path, self._secret.encode(), preserve_mode=False)
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            port = reservation.getsockname()[1]
        log_path = self.directory / "server.log"
        atomic_write_bytes(log_path, b"", preserve_mode=False)
        self.log = log_path.open("ab")
        self.process = await asyncio.create_subprocess_exec(
            python_executable(self.settings),
            str(WORKER),
            "serve",
            "edit",
            str(self.source_path),
            "--headless",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--token-password-file",
            str(token_path),
            "--no-sandbox",
            cwd=self.working_dir,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=self.log,
            stderr=self.log,
            start_new_session=True,
        )
        identity = self._identity = {
            "pid": self.process.pid,
            "started_at": process_start_time(self.process.pid),
        }
        atomic_write_bytes(
            self.identity_path, json.dumps(identity).encode(), preserve_mode=False
        )
        self.client = httpx.AsyncClient(
            base_url=f"http://127.0.0.1:{port}",
            trust_env=False,
            headers={
                "Authorization": f"Bearer {self._secret}",
                "Marimo-Session-Id": self._session_id,
            },
            timeout=httpx.Timeout(10.0, read=None),
        )
        await asyncio.wait_for(
            self._connect(port), self.settings.startup_timeout_seconds
        )
        await self.execute("import marimo._code_mode as cm; help(cm)")
        success, _ = await self.execute(CAPABILITIES)
        if not success:
            raise MarimoTransportError(
                "Marimo code-mode capabilities do not match this adapter"
            )
        inventory = await self.inventory()
        identity["kernel_pid"] = int(inventory["pid"])
        identity["kernel_started_at"] = process_start_time(identity["kernel_pid"])
        atomic_write_bytes(
            self.identity_path, json.dumps(identity).encode(), preserve_mode=False
        )
        return inventory

    async def _connect(self, port: int) -> None:
        assert self.client is not None
        while True:
            if self.process is not None and self.process.returncode is not None:
                raise MarimoTransportError("Marimo server exited during startup")
            try:
                page = await self.client.get("/")
                page.raise_for_status()
                break
            except (httpx.TransportError, httpx.HTTPStatusError):
                await asyncio.sleep(0.1)
        match = re.search(r'<marimo-server-token data-token="([^"]+)"', page.text)
        if match is None:
            raise MarimoTransportError(
                "Marimo server protocol is missing its version token"
            )
        self.client.headers["Marimo-Server-Token"] = html.unescape(match[1])
        self.socket = await websockets.asyncio.client.connect(
            f"ws://127.0.0.1:{port}/ws?session_id={self._session_id}",
            additional_headers={"Authorization": f"Bearer {self._secret}"},
            max_size=50 * 1024 * 1024,
        )
        while True:
            message = json.loads(await self.socket.recv())
            if message["op"] == "kernel-ready":
                break
        self.reader = asyncio.create_task(self._read_messages())

    async def _read_messages(self) -> None:
        assert self.socket is not None
        async for raw in self.socket:
            message = json.loads(raw)
            await self.on_message(message)
            if (
                message.get("op") == "cell-op"
                and message["data"].get("cell_id") == "__scratch__"
            ):
                for marker, event in self._barriers.items():
                    if marker in str(message["data"].get("console", "")):
                        event.set()
        if not self._closing:
            raise MarimoTransportError("Marimo event stream disconnected")

    async def execute(self, code: str) -> tuple[bool, str]:
        assert self.client is not None
        async with self._execution_lock:
            marker = "__RUNWATCH_BARRIER_" + secrets.token_hex(16)
            event = self._barriers[marker] = asyncio.Event()
            try:
                success, output = await self._execute(code + f"\nprint({marker!r})")
                if marker in output:
                    await asyncio.wait_for(
                        event.wait(), self.settings.startup_timeout_seconds
                    )
                return success, output
            finally:
                self._barriers.pop(marker, None)

    async def _execute(self, code: str) -> tuple[bool, str]:
        assert self.client is not None
        output: list[str] = []
        length = 0
        event_type = ""
        try:
            async with self.client.stream(
                "POST", "/api/kernel/execute", json={"code": code}
            ) as response:
                response.raise_for_status()
                async for line in response.aiter_lines():
                    if line.startswith("event:"):
                        event_type = line[6:].strip()
                    elif line.startswith("data:"):
                        value = json.loads(line[5:])
                        if event_type == "done":
                            return bool(value["success"]), "".join(output)
                        if event_type == "stdout":
                            chunk = str(value.get("data", ""))
                            length += len(chunk)
                            if length <= 16 * 1024 * 1024:
                                output.append(chunk)
        except (httpx.HTTPError, ValueError, KeyError) as error:
            # httpx errors can contain authenticated URLs; report only the category.
            raise MarimoTransportError(
                f"Marimo execution transport failed ({type(error).__name__}); outcome is uncertain"
            ) from None
        raise MarimoTransportError(
            "Marimo execution stream ended without completion; outcome is uncertain"
        )

    async def inventory(self) -> dict[str, Any]:
        success, stdout = await self.execute(INVENTORY)
        if not success:
            raise MarimoTransportError("Marimo graph inspection failed")
        return cast(dict[str, Any], parse_result(stdout))

    async def interrupt(self) -> None:
        assert self.client is not None
        response = await self.client.post("/api/kernel/interrupt", json={})
        if response.status_code != 200:
            raise MarimoTransportError("Marimo rejected the interrupt request")

    async def materialize(self, value: str) -> str:
        assert self.client is not None
        for url in set(re.findall(r"\./@file/[A-Za-z0-9._~%/-]+", value)):
            if url not in self._assets:
                async with self.client.stream("GET", url[1:], timeout=10.0) as response:
                    response.raise_for_status()
                    data = bytearray()
                    async for chunk in response.aiter_bytes():
                        data.extend(chunk)
                        if len(data) > 50 * 1024 * 1024:
                            raise ValueError("Marimo output asset exceeds 50 MiB")
                    mime = response.headers.get(
                        "content-type", "application/octet-stream"
                    ).split(";")[0]
                self._assets[url] = (
                    f"data:{mime};base64,{base64.b64encode(data).decode()}"
                )
                self._asset_bytes += len(self._assets[url])
                while self._asset_bytes > 64 * 1024 * 1024 and len(self._assets) > 1:
                    oldest = next(iter(self._assets))
                    self._asset_bytes -= len(self._assets.pop(oldest))
            value = value.replace(url, self._assets[url])
        return value

    def _owned_processes(self) -> list[tuple[int, float]]:
        identity = self._identity
        if identity is None:
            return []
        return [
            (int(identity[pid_key]), float(identity[start_key]))
            for pid_key, start_key in (
                ("pid", "started_at"),
                ("kernel_pid", "kernel_started_at"),
            )
            if identity.get(pid_key) and identity.get(start_key) is not None
        ]

    async def signal(self, sig: signal.Signals) -> None:
        for pid, started_at in self._owned_processes():
            if process_is_alive(pid, started_at):
                with contextlib.suppress(ProcessLookupError):
                    group = os.getpgid(pid)
                    identity = self._identity
                    assert identity is not None
                    if group == int(identity["pid"]):
                        os.killpg(group, sig)
                    else:
                        os.kill(pid, sig)

    async def reap_previous(self) -> None:
        if self._identity is None and self.identity_path.exists():
            self._identity = json.loads(self.identity_path.read_text())
        await self.signal(signal.SIGTERM)
        if await self._wait_owned_stopped(1.0):
            self._remove_identity()
            return
        await self.signal(signal.SIGKILL)
        if not await self._wait_owned_stopped(5.0):
            raise MarimoTransportError(
                "Owned Marimo processes survived shutdown; refusing reconstruction"
            )
        self._remove_identity()

    async def _wait_owned_stopped(self, seconds: float) -> bool:
        deadline = asyncio.get_running_loop().time() + seconds
        while any(
            process_is_alive(pid, started) for pid, started in self._owned_processes()
        ):
            if asyncio.get_running_loop().time() >= deadline:
                return False
            await asyncio.sleep(0.05)
        return True

    def kernel_is_alive(self) -> bool:
        identity = self._identity or {}
        return process_is_alive(
            identity.get("kernel_pid"), identity.get("kernel_started_at")
        )

    def _remove_identity(self) -> None:
        if (
            self.identity_path.exists()
            and json.loads(self.identity_path.read_text()) == self._identity
        ):
            self.identity_path.unlink(missing_ok=True)

    async def close(self) -> None:
        if self._closing:
            return
        self._closing = True
        if self.socket is not None:
            await self.socket.close()
        if self.reader is not None:
            self.reader.cancel()
            await asyncio.gather(self.reader, return_exceptions=True)
        await self.signal(signal.SIGTERM)
        if self.process is not None:
            try:
                await asyncio.wait_for(
                    self.process.wait(), self.settings.cancel_shutdown_grace_seconds
                )
            except TimeoutError:
                await self.signal(signal.SIGKILL)
                await asyncio.wait_for(self.process.wait(), 5)
        await self.reap_previous()
        if self.client is not None:
            await self.client.aclose()
        if self.log is not None:
            self.log.close()
        self._remove_identity()
