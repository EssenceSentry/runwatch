# pyright: reportMissingParameterType=false, reportPrivateUsage=false, reportUnknownLambdaType=false, reportUnknownMemberType=false, reportUnknownParameterType=false
from __future__ import annotations

import asyncio
from collections import deque

import pytest

from runwatch.tunnel import (
    CloudflaredTunnel,
    discover_lan_ip,
    with_token,
)


def test_pairing_token_preserves_existing_public_url_query() -> None:
    value = with_token("https://dashboard.example/run?mode=phone", "secret")
    assert value == "https://dashboard.example/run?mode=phone&token=secret"


def test_lan_discovery_uses_selected_route(monkeypatch) -> None:
    class FakeSocket:
        def connect(self, address) -> None:
            assert address == ("8.8.8.8", 80)

        def getsockname(self):
            return ("192.168.1.42", 54321)

        def close(self) -> None:
            pass

    monkeypatch.setattr("runwatch.tunnel.socket.socket", lambda *args: FakeSocket())
    assert discover_lan_ip() == "192.168.1.42"


@pytest.mark.asyncio
async def test_missing_cloudflared_has_curated_error(monkeypatch) -> None:
    async def missing(*args, **kwargs):
        raise FileNotFoundError

    monkeypatch.setattr(asyncio, "create_subprocess_exec", missing)
    tunnel = CloudflaredTunnel("not-installed")
    with pytest.raises(RuntimeError, match=r"not-installed.*was not found"):
        await tunnel.start("http://127.0.0.1:8765")


def test_lan_discovery_falls_back_to_hostname_then_loopback(monkeypatch) -> None:
    class BrokenSocket:
        def connect(self, address) -> None:
            raise OSError("no route")

        def close(self) -> None:
            pass

    monkeypatch.setattr("runwatch.tunnel.socket.socket", lambda *args: BrokenSocket())
    monkeypatch.setattr("runwatch.tunnel.socket.gethostname", lambda: "host")
    monkeypatch.setattr(
        "runwatch.tunnel.socket.gethostbyname",
        lambda hostname: "192.168.2.9",
    )
    assert discover_lan_ip() == "192.168.2.9"

    def unavailable(hostname: str) -> str:
        raise OSError("no dns")

    monkeypatch.setattr("runwatch.tunnel.socket.gethostbyname", unavailable)
    assert discover_lan_ip() == "127.0.0.1"


def test_pairing_token_adds_root_path_and_replaces_old_token() -> None:
    assert with_token("https://dashboard.example", "new") == (
        "https://dashboard.example/?token=new"
    )
    assert with_token("https://dashboard.example/?token=old#status", "new") == (
        "https://dashboard.example/?token=new#status"
    )


class FakeStream:
    def __init__(self, lines: list[bytes]) -> None:
        self.lines = deque(lines)

    async def readline(self) -> bytes:
        await asyncio.sleep(0)
        return self.lines.popleft() if self.lines else b""


class FakeProcess:
    def __init__(self, lines: list[bytes], *, returncode: int | None = None) -> None:
        self.stdout = FakeStream(lines)
        self.returncode = returncode
        self.terminated = False
        self.killed = False

    def terminate(self) -> None:
        self.terminated = True
        self.returncode = 0

    def kill(self) -> None:
        self.killed = True
        self.returncode = -9

    async def wait(self) -> int:
        return self.returncode or 0


@pytest.mark.asyncio
async def test_cloudflared_success_drains_output_and_closes(monkeypatch) -> None:
    process = FakeProcess(
        [
            b"starting tunnel\n",
            b"visit https://mobile-run.trycloudflare.com now\n",
            b"after announcement\n",
        ]
    )

    async def create(*args, **kwargs):
        assert args[0] == "cloudflared"
        assert "http://127.0.0.1:8765" in args
        return process

    monkeypatch.setattr(asyncio, "create_subprocess_exec", create)
    tunnel = CloudflaredTunnel(retained_lines=2)
    url = await tunnel.start("http://127.0.0.1:8765")
    assert url == "https://mobile-run.trycloudflare.com"
    await asyncio.sleep(0)
    await tunnel.close()
    assert process.terminated
    assert list(tunnel.output)[-1] == "after announcement"
    assert tunnel.process is None


@pytest.mark.asyncio
async def test_cloudflared_early_exit_reports_recent_output(monkeypatch) -> None:
    process = FakeProcess([b"configuration rejected\n"], returncode=1)

    async def create(*args, **kwargs):
        return process

    monkeypatch.setattr(asyncio, "create_subprocess_exec", create)
    tunnel = CloudflaredTunnel()
    with pytest.raises(RuntimeError, match="configuration rejected"):
        await tunnel.start("http://127.0.0.1:8765")
    assert tunnel.process is None


@pytest.mark.asyncio
async def test_drain_without_process_is_noop() -> None:
    tunnel = CloudflaredTunnel()
    await tunnel._drain_output()
    await tunnel.close()
