# pyright: reportPrivateUsage=false, reportUnknownArgumentType=false
from __future__ import annotations

import asyncio
import ctypes
from pathlib import Path
from typing import Any

import nbformat
import pytest

import runwatch._sleep_inhibition as sleep_module
from runwatch.models import HostSettings, RunStatus, RunwatchConfig
from runwatch.supervisor import RunSupervisor


class FakeIOKitBindings:
    def __init__(self) -> None:
        self.created_strings: list[bytes] = []
        self.released_objects: list[int] = []
        self.released_assertions: list[int] = []

    def create_cf_string(self, _allocator: object, value: bytes, _encoding: int) -> int:
        self.created_strings.append(value)
        return 100 + len(self.created_strings)

    def release_cf_object(self, value: int) -> None:
        self.released_objects.append(value)

    def create_assertion(
        self,
        _assertion_type: int,
        level: int,
        _reason: int,
        assertion_id: Any,
    ) -> int:
        assert level == 255
        ctypes.cast(assertion_id, ctypes.POINTER(ctypes.c_uint32)).contents.value = 73
        return 0

    def release_assertion(self, assertion_id: int) -> int:
        self.released_assertions.append(assertion_id)
        return 0


class FakeStreamReader:
    def __init__(self, payload: bytes = b"") -> None:
        self.payload = payload

    async def read(self, _limit: int) -> bytes:
        return self.payload


class FakeStreamWriter:
    def __init__(self, process: FakeProcess) -> None:
        self.process = process

    def close(self) -> None:
        self.process.finish(0)

    async def wait_closed(self) -> None:
        return


class FakeProcess:
    def __init__(self, *, returncode: int | None = None, error: bytes = b"") -> None:
        self.returncode = returncode
        self.stderr = FakeStreamReader(error)
        self._finished = asyncio.Event()
        if returncode is not None:
            self._finished.set()
        self.stdin = FakeStreamWriter(self)

    def finish(self, returncode: int) -> None:
        self.returncode = returncode
        self._finished.set()

    async def wait(self) -> int:
        await self._finished.wait()
        assert self.returncode is not None
        return self.returncode

    def terminate(self) -> None:
        self.finish(-15)

    def kill(self) -> None:
        self.finish(-9)


class FakeSleepInhibitor:
    backend = "fake"

    def __init__(self) -> None:
        self.active = False
        self.calls: list[str] = []

    async def acquire(self) -> None:
        self.calls.append("acquire")
        self.active = True

    async def release(self) -> None:
        if not self.active:
            return
        self.calls.append("release")
        self.active = False


@pytest.mark.asyncio
async def test_iokit_assertion_is_idempotent_and_released(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bindings = FakeIOKitBindings()
    monkeypatch.setattr(sleep_module, "_IOKitBindings", lambda: bindings)
    inhibitor = sleep_module._IOKitSleepInhibitor()

    await inhibitor.acquire()
    await inhibitor.acquire()

    assert inhibitor.active
    assert bindings.created_strings == [
        b"NoIdleSleepAssertion",
        b"Runwatch notebook execution",
    ]
    assert bindings.released_objects == [102, 101]

    await inhibitor.release()
    await inhibitor.release()

    assert not inhibitor.active
    assert bindings.released_assertions == [73]


@pytest.mark.asyncio
async def test_logind_inhibitor_holds_until_stdin_closes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process = FakeProcess()
    command: tuple[object, ...] | None = None

    async def create_subprocess(*args: object, **_kwargs: object) -> FakeProcess:
        nonlocal command
        command = args
        return process

    monkeypatch.setattr(asyncio, "create_subprocess_exec", create_subprocess)
    inhibitor = sleep_module._LogindSleepInhibitor("/usr/bin/systemd-inhibit")

    await inhibitor.acquire()

    assert inhibitor.active
    assert command is not None
    assert "--what=idle:sleep" in command
    assert "/bin/cat" in command

    await inhibitor.release()

    assert not inhibitor.active
    assert process.returncode == 0


@pytest.mark.asyncio
async def test_logind_startup_failure_is_reported(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process = FakeProcess(returncode=1, error=b"Failed to connect to bus")

    async def create_subprocess(*_args: object, **_kwargs: object) -> FakeProcess:
        return process

    monkeypatch.setattr(asyncio, "create_subprocess_exec", create_subprocess)
    inhibitor = sleep_module._LogindSleepInhibitor("/usr/bin/systemd-inhibit")

    with pytest.raises(RuntimeError, match="Failed to connect to bus"):
        await inhibitor.acquire()

    assert not inhibitor.active


@pytest.mark.asyncio
async def test_supervisor_holds_inhibitor_through_finalization(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    notebook_path = tmp_path / "empty.ipynb"
    nbformat.write(nbformat.v4.new_notebook(), notebook_path)
    inhibitor = FakeSleepInhibitor()
    supervisor = RunSupervisor(
        notebook_path=notebook_path,
        output_path=tmp_path / "out.ipynb",
        working_dir=tmp_path,
        run_dir=tmp_path / "run",
        config=RunwatchConfig(host=HostSettings(prevent_system_sleep=True)),
        sleep_inhibitor=inhibitor,
    )

    async def restore_runtime_services() -> None:
        return

    async def completed() -> RunStatus:
        return RunStatus.SUCCEEDED

    def create_runtime_tasks() -> None:
        supervisor.store.finish_run(
            supervisor.run_id,
            RunStatus.SUCCEEDED,
            message="completed",
            event_type="run.succeeded",
            event_payload={"kernel_epoch": 0},
        )
        supervisor._runner_task = asyncio.create_task(completed())

    monkeypatch.setattr(
        supervisor, "_restore_runtime_services", restore_runtime_services
    )
    monkeypatch.setattr(supervisor, "_create_runtime_tasks", create_runtime_tasks)

    await supervisor.start()
    assert inhibitor.active
    assert inhibitor.calls == ["acquire"]

    assert await supervisor.wait() is RunStatus.SUCCEEDED
    assert not inhibitor.active
    assert inhibitor.calls == ["acquire", "release"]
    event_types = [
        event["type"]
        for event in supervisor.store.recent_events(supervisor.run_id, limit=100)
    ]
    assert "host.sleep_inhibition_started" in event_types
    assert "host.sleep_inhibition_stopped" in event_types

    await supervisor.close()
    assert inhibitor.calls == ["acquire", "release"]


def test_enabled_sleep_inhibition_rejects_unsupported_platform_before_writes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    notebook_path = tmp_path / "empty.ipynb"
    nbformat.write(nbformat.v4.new_notebook(), notebook_path)

    def unsupported() -> Any:
        raise RuntimeError("unsupported test platform")

    monkeypatch.setattr("runwatch.supervisor.create_sleep_inhibitor", unsupported)

    with pytest.raises(RuntimeError, match="unsupported test platform"):
        RunSupervisor(
            notebook_path=notebook_path,
            output_path=tmp_path / "out.ipynb",
            working_dir=tmp_path,
            run_dir=tmp_path / "run",
            config=RunwatchConfig(host=HostSettings(prevent_system_sleep=True)),
        )

    assert not (tmp_path / "run").exists()
