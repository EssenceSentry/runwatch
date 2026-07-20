# pyright: reportMissingParameterType=false, reportPrivateUsage=false, reportUnknownArgumentType=false, reportUnknownLambdaType=false, reportUnknownMemberType=false, reportUnknownParameterType=false
from __future__ import annotations

import asyncio
import json
import os
import re
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import httpx
import nbformat
import pytest
from typer.testing import CliRunner

import runwatch.cli as cli
from runwatch.models import (
    ActionKind,
    ActionStatus,
    NotificationSettings,
    RunStatus,
    RunwatchConfig,
)
from runwatch.notification_config import notification_destinations
from runwatch.storage import RunStore
from runwatch.supervisor import RunSupervisor

runner = CliRunner()


def _notebook(path: Path, source: str = "print('ok')") -> None:
    nbformat.write(
        nbformat.v4.new_notebook(cells=[nbformat.v4.new_code_cell(source)]), path
    )


def _supervisor(root: Path) -> RunSupervisor:
    root.mkdir(parents=True, exist_ok=True)
    notebook = root / "input.ipynb"
    _notebook(notebook)
    return RunSupervisor(
        notebook_path=notebook,
        output_path=root / "executed.ipynb",
        working_dir=root,
        run_dir=root / "run",
        config=RunwatchConfig(),
        name="demo",
    )


def test_lock_token_default_directory_and_server_overrides(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    lock = cli.RunLock(run_dir)
    lock.acquire()
    assert run_dir.stat().st_mode & 0o777 == 0o700
    lock_record = json.loads(lock.path.read_text(encoding="utf-8"))
    assert lock_record["pid"] == os.getpid()
    assert lock_record["started_at"] > 0
    assert lock_record["controller_token"]
    assert lock_record["hostname"]
    assert lock_record["boot_id"]
    with pytest.raises(RuntimeError, match="already owns"):
        cli.RunLock(run_dir).acquire()
    lock.release()
    assert not lock.path.exists()

    lock.path.write_text("not-a-pid", encoding="utf-8")
    lock.acquire()
    assert lock.held
    lock.release()

    token = "new-token-that-is-long-enough-for-runwatch"
    monkeypatch.setattr(cli.secrets, "token_urlsafe", lambda size: token)
    assert cli._token(run_dir) == token
    assert cli._token(run_dir) == token
    (run_dir / "access-token.txt").write_text("", encoding="utf-8")
    with pytest.raises(RuntimeError, match="empty or invalid"):
        cli._token(run_dir)
    generated = cli._default_run_dir(tmp_path, tmp_path / "demo.ipynb")
    assert generated.parent == tmp_path / ".runwatch" / "runs"
    assert "demo" in generated.name

    config = cli._override_server(
        RunwatchConfig(),
        host="0.0.0.0",
        port=9999,
        share="lan",
        open_browser=True,
        show_qr=False,
    )
    assert config.server.host == "0.0.0.0"
    assert config.server.port == 9999
    assert config.server.share == "lan"
    assert config.server.open_browser is True
    assert config.server.show_qr is False


def test_finished_cleanup_removes_empty_runwatch_parents(tmp_path: Path) -> None:
    run_dir = tmp_path / ".runwatch" / "runs" / "successful-run"
    run_dir.mkdir(parents=True)
    (run_dir / "run-manifest.json").write_text("{}", encoding="utf-8")
    (run_dir / "runwatch.sqlite3").write_bytes(b"state")

    cli._cleanup_finished_run(run_dir, tmp_path)

    assert not run_dir.exists()
    assert not (tmp_path / ".runwatch").exists()


def test_finished_cleanup_preserves_other_runwatch_state(tmp_path: Path) -> None:
    runs_dir = tmp_path / ".runwatch" / "runs"
    run_dir = runs_dir / "successful-run"
    retained = runs_dir / "failed-run"
    run_dir.mkdir(parents=True)
    retained.mkdir()
    (run_dir / "run-manifest.json").write_text("{}", encoding="utf-8")
    (run_dir / "runwatch.sqlite3").write_bytes(b"state")

    cli._cleanup_finished_run(run_dir, tmp_path)

    assert not run_dir.exists()
    assert retained.is_dir()
    assert runs_dir.is_dir()


@pytest.mark.asyncio
async def test_automatic_cleanup_requires_normal_wait_and_durable_finalization(
    tmp_path: Path,
) -> None:
    supervisor = _supervisor(tmp_path)
    supervisor.cleanup_on_success = True
    supervisor.store.finish_run(
        supervisor.run_id,
        RunStatus.SUCCEEDED,
        message="completed",
        event_type="run.succeeded",
        event_payload={"kernel_epoch": 0},
    )

    assert await cli._automatic_cleanup_decision(supervisor) == (False, None)

    supervisor._wait_completed_normally = True  # noqa: SLF001 - cleanup gate probe
    assert await cli._automatic_cleanup_decision(supervisor) == (False, None)

    supervisor.store.mark_run_finalized(supervisor.run_id, RunStatus.SUCCEEDED)
    supervisor._wait_completed_normally = False  # noqa: SLF001 - cleanup gate probe
    assert await cli._automatic_cleanup_decision(supervisor) == (False, None)

    supervisor._wait_completed_normally = True  # noqa: SLF001 - cleanup gate probe
    assert await cli._automatic_cleanup_decision(supervisor) == (True, None)
    await supervisor.close()


@pytest.mark.asyncio
async def test_automatic_cleanup_accepts_cancelled_and_preserves_failed(
    tmp_path: Path,
) -> None:
    cancelled = _supervisor(tmp_path / "cancelled")
    cancelled.store.finish_run(
        cancelled.run_id,
        RunStatus.CANCELLED,
        message="cancelled",
        event_type="run.cancelled",
        event_payload={"kernel_epoch": 0},
    )
    cancelled.store.mark_run_finalized(cancelled.run_id, RunStatus.CANCELLED)
    cancelled._wait_completed_normally = True  # noqa: SLF001 - cleanup gate probe
    assert await cli._automatic_cleanup_decision(cancelled) == (True, None)
    await cancelled.close()

    failed = _supervisor(tmp_path / "failed")
    failed.store.finish_run(
        failed.run_id,
        RunStatus.FAILED,
        message="failed",
        event_type="run.runner_error",
        event_payload={"kernel_epoch": 0, "error_type": "Error", "error": "boom"},
    )
    failed.store.mark_run_finalized(failed.run_id, RunStatus.FAILED)
    failed._wait_completed_normally = True  # noqa: SLF001 - cleanup gate probe
    assert await cli._automatic_cleanup_decision(failed) == (False, None)
    await failed.close()


def test_run_lock_is_atomically_published_and_release_is_token_fenced(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    published: list[dict[str, Any]] = []
    original_link = cli.os.link

    def observed_link(source: Path, destination: Path) -> None:
        assert not Path(destination).exists()
        original_link(source, destination)
        published.append(json.loads(Path(destination).read_text(encoding="utf-8")))

    monkeypatch.setattr(cli.os, "link", observed_link)
    lock = cli.RunLock(tmp_path, controller_token="owner-token")
    lock.acquire()
    assert published[0]["controller_token"] == "owner-token"
    assert published[0]["pid"] == os.getpid()
    assert published[0]["started_at"] > 0

    lock.path.unlink()
    replacement = {
        "pid": os.getpid(),
        "started_at": cli.process_start_time(os.getpid()),
        "controller_token": "replacement-token",
    }
    lock.path.write_text(json.dumps(replacement), encoding="utf-8")
    lock.release()

    assert json.loads(lock.path.read_text(encoding="utf-8")) == replacement


def test_run_lock_owns_published_lock_if_temporary_cleanup_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    original_unlink = Path.unlink

    def fail_temporary_cleanup(path: Path, missing_ok: bool = False) -> None:
        if path.name.startswith(".runwatch.lock.") and path.name.endswith(".tmp"):
            raise OSError("simulated cleanup failure")
        original_unlink(path, missing_ok=missing_ok)

    monkeypatch.setattr(Path, "unlink", fail_temporary_cleanup)
    lock = cli.RunLock(tmp_path)

    lock.acquire()

    assert lock.held
    assert lock.path.exists()
    with pytest.raises(RuntimeError, match="already owns"):
        cli.RunLock(tmp_path).acquire()
    lock.release()
    assert not lock.path.exists()


def test_run_lock_recovers_legacy_and_local_stale_records_but_fences_foreign_host(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    monkeypatch.setattr(cli, "_machine_identity", lambda: ("local-host", "boot-a"))
    monkeypatch.setattr(cli, "process_is_alive", lambda *args: False)

    legacy = {
        "pid": 111,
        "started_at": 1.0,
        "controller_token": "legacy",
    }
    (run_dir / "runwatch.lock").write_text(json.dumps(legacy), encoding="utf-8")
    legacy_owner = cli.RunLock(run_dir)
    legacy_owner.acquire()
    legacy_owner.release()

    local = {
        **legacy,
        "controller_token": "local",
        "hostname": "local-host",
        "boot_id": "boot-a",
    }
    (run_dir / "runwatch.lock").write_text(json.dumps(local), encoding="utf-8")
    local_owner = cli.RunLock(run_dir)
    local_owner.acquire()
    local_owner.release()

    previous_boot = {**local, "controller_token": "previous-boot", "boot_id": "boot-z"}
    (run_dir / "runwatch.lock").write_text(json.dumps(previous_boot), encoding="utf-8")
    monkeypatch.setattr(cli, "process_is_alive", lambda *args: True)
    rebooted_owner = cli.RunLock(run_dir)
    rebooted_owner.acquire()
    rebooted_owner.release()

    foreign = {
        **local,
        "controller_token": "foreign",
        "hostname": "remote-host",
    }
    (run_dir / "runwatch.lock").write_text(json.dumps(foreign), encoding="utf-8")
    with pytest.raises(RuntimeError, match="host remote-host already owns"):
        cli.RunLock(run_dir).acquire()
    assert (
        json.loads((run_dir / "runwatch.lock").read_text(encoding="utf-8"))[
            "controller_token"
        ]
        == "foreign"
    )


def test_cleanup_guard_fences_successor_after_run_directory_is_deleted(
    tmp_path: Path,
) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    owner = cli.RunLock(run_dir)
    owner.acquire()
    owner.begin_cleanup()
    cli.shutil.rmtree(run_dir)

    successor = cli.RunLock(run_dir)
    with pytest.raises(RuntimeError, match="while it cleans"):
        successor.acquire()
    assert not run_dir.exists()

    owner.release()
    successor.acquire()
    assert successor.held
    successor.release()


def test_cleanup_helper_refuses_an_active_unfenced_owner(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    (run_dir / "run-manifest.json").write_text("{}", encoding="utf-8")
    (run_dir / "runwatch.sqlite3").write_bytes(b"state")
    owner = cli.RunLock(run_dir)
    owner.acquire()
    try:
        with pytest.raises(RuntimeError, match="actively owned"):
            cli._cleanup_finished_run(run_dir, tmp_path)
        assert run_dir.is_dir()
    finally:
        owner.release()


@pytest.mark.asyncio
async def test_dashboard_url_selection_and_server_start_failure(monkeypatch) -> None:
    public = RunwatchConfig.model_validate(
        {"server": {"public_url": "https://example.test/root/"}}
    )
    assert await cli._dashboard_base(public, "http://local") == (
        "https://example.test/root",
        None,
    )

    lan = RunwatchConfig.model_validate({"server": {"share": "lan", "port": 9876}})
    monkeypatch.setattr(cli, "discover_lan_ip", lambda: "192.0.2.3")
    assert await cli._dashboard_base(lan, "http://local") == (
        "http://192.0.2.3:9876",
        None,
    )

    class FakeTunnel:
        def __init__(self, binary: str) -> None:
            assert binary == "cloudflared"

        async def start(self, local: str) -> str:
            assert local == "http://local"
            return "https://tunnel.example"

    monkeypatch.setattr(cli, "CloudflaredTunnel", FakeTunnel)
    cloud = RunwatchConfig.model_validate({"server": {"share": "cloudflared"}})
    base, tunnel = await cli._dashboard_base(cloud, "http://local")
    assert base == "https://tunnel.example"
    assert isinstance(tunnel, FakeTunnel)

    server = SimpleNamespace(started=False)

    async def fail() -> None:
        raise RuntimeError("server failed")

    task = asyncio.create_task(fail())
    await asyncio.sleep(0)
    with pytest.raises(RuntimeError, match="server failed"):
        await cli._wait_for_server(server, task)  # type: ignore[arg-type]


def test_terminal_qr_uses_compact_error_correction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: dict[str, object] = {}

    class FakeQrCode:
        def __init__(self, **kwargs: object) -> None:
            calls["options"] = kwargs

        def add_data(self, value: str) -> None:
            calls["value"] = value

        def make(self, *, fit: bool) -> None:
            calls["fit"] = fit

        def print_ascii(self, *, invert: bool) -> None:
            calls["invert"] = invert

    monkeypatch.setattr(cli.qrcode, "QRCode", FakeQrCode)

    cli._print_qr("https://example.test/?token=secret")

    assert calls == {
        "options": {
            "border": 1,
            "error_correction": cli.ERROR_CORRECT_L,
        },
        "value": "https://example.test/?token=secret",
        "fit": True,
        "invert": True,
    }


@pytest.mark.asyncio
async def test_announce_and_run_status_exit_codes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    opened: list[str] = []
    qr: list[str] = []
    monkeypatch.setattr(cli.webbrowser, "open", opened.append)
    monkeypatch.setattr(cli, "_print_qr", qr.append)
    supervisor = SimpleNamespace(
        run_dir=tmp_path / "run",
        source_path=tmp_path / "source.ipynb",
        output_path=tmp_path / "out.ipynb",
        config=RunwatchConfig.model_validate(
            {"server": {"show_qr": True, "open_browser": True, "linger_seconds": 0}}
        ),
        start=lambda: None,
    )
    public_url = "https://example.test/?token=x"
    local_url = "http://127.0.0.1:8765/?token=x"
    cli._announce_run(cast(RunSupervisor, supervisor), public_url, local_url)
    assert opened == ["https://example.test/?token=x"]
    assert qr == opened
    output = capsys.readouterr().out
    assert f"Dashboard: {public_url}" in output
    assert f"Local dashboard: {local_url}" in output

    class FakeSupervisor:
        def __init__(self, status: RunStatus) -> None:
            self.status = status
            self.notebook_path = tmp_path / "input.ipynb"
            self.output_path = tmp_path / "out.ipynb"
            self.config = RunwatchConfig.model_validate(
                {"server": {"linger_seconds": 0}}
            )
            self.started = False

        async def start(self) -> None:
            self.started = True

        async def wait(self) -> RunStatus:
            return self.status

    success = FakeSupervisor(RunStatus.SUCCEEDED)
    failed = FakeSupervisor(RunStatus.FAILED)
    assert await cli._run_and_linger(cast(RunSupervisor, success), start_run=True) == 0
    assert await cli._run_and_linger(cast(RunSupervisor, failed), start_run=True) == 1
    assert success.started and failed.started


@pytest.mark.asyncio
async def test_serve_signal_requests_process_stop_and_uses_shell_exit_code(
    tmp_path: Path,
) -> None:
    cancellation_started = asyncio.Event()

    class FakeRunner:
        def request_process_stop(self) -> asyncio.Task[None]:
            async def cancel() -> None:
                cancellation_started.set()

            return asyncio.create_task(cancel())

    supervisor = SimpleNamespace(runner=FakeRunner(), run_dir=tmp_path)
    stop = cli._ServeSignalState(cast(RunSupervisor, supervisor), start_run=True)

    stop.request(cli.signal.SIGINT)
    await stop.wait_for_cancellation()

    assert stop.event.is_set()
    assert stop.exit_code == 130
    assert cancellation_started.is_set()


@pytest.mark.asyncio
async def test_run_and_linger_uses_default_observation_grace(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    slept: list[float] = []

    async def record_sleep(seconds: float) -> None:
        slept.append(seconds)

    class FakeSupervisor:
        notebook_path = tmp_path / "input.ipynb"
        output_path = tmp_path / "out.ipynb"
        config = RunwatchConfig()

        async def start(self) -> None:
            return None

        async def wait(self) -> RunStatus:
            return RunStatus.SUCCEEDED

        async def wait_for_action_loop_failure(self) -> None:
            await asyncio.Future()

    monkeypatch.setattr(cli.asyncio, "sleep", record_sleep)

    result = await cli._run_and_linger(
        cast(RunSupervisor, FakeSupervisor()), start_run=True
    )

    assert result == 0
    assert slept == [90.0]
    assert (
        "Dashboard remains available for 90 seconds before closing."
        in capsys.readouterr().out
    )


@pytest.mark.asyncio
async def test_run_and_linger_allows_explicit_indefinite_linger(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    waited: list[bool] = []

    class ReturningEvent:
        async def wait(self) -> None:
            waited.append(True)

    class FakeSupervisor:
        notebook_path = tmp_path / "input.ipynb"
        output_path = tmp_path / "out.ipynb"
        config = RunwatchConfig.model_validate({"server": {"linger_seconds": None}})

        async def start(self) -> None:
            return None

        async def wait(self) -> RunStatus:
            return RunStatus.SUCCEEDED

        async def wait_for_action_loop_failure(self) -> None:
            await asyncio.Future()

    monkeypatch.setattr(cli.asyncio, "Event", ReturningEvent)

    result = await cli._run_and_linger(
        cast(RunSupervisor, FakeSupervisor()), start_run=True
    )

    assert result == 0
    assert waited == [True]
    assert (
        "Dashboard remains available; press Ctrl+C to close it."
        in capsys.readouterr().out
    )


@pytest.mark.parametrize("linger_seconds", [30.0, None], ids=["finite", "indefinite"])
@pytest.mark.parametrize(
    "failure_message",
    [
        "Runwatch action loop exited unexpectedly",
        "Runwatch action loop failed unexpectedly (ValueError)",
    ],
    ids=["normal-exit", "exception"],
)
@pytest.mark.asyncio
async def test_run_and_linger_aborts_when_action_loop_terminates(
    tmp_path: Path,
    linger_seconds: float | None,
    failure_message: str,
) -> None:
    class FakeSupervisor:
        notebook_path = tmp_path / "input.ipynb"
        output_path = tmp_path / "out.ipynb"
        config = RunwatchConfig.model_validate(
            {"server": {"linger_seconds": linger_seconds}}
        )

        async def start(self) -> None:
            return None

        async def wait(self) -> RunStatus:
            return RunStatus.SUCCEEDED

        async def wait_for_action_loop_failure(self) -> None:
            await asyncio.sleep(0)
            raise RuntimeError(failure_message)

    with pytest.raises(RuntimeError, match=re.escape(failure_message)):
        await asyncio.wait_for(
            cli._run_and_linger(cast(RunSupervisor, FakeSupervisor()), start_run=True),
            timeout=1,
        )


@pytest.mark.asyncio
async def test_serve_releases_lock_when_startup_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class FakeSupervisor:
        def __init__(self) -> None:
            self.run_dir = tmp_path
            self.config = RunwatchConfig()
            self.controller_token = "controller-token"
            self.closed = False

        async def close(self) -> None:
            self.closed = True

        async def quiesce(self) -> None:
            return None

    supervisor = FakeSupervisor()
    monkeypatch.setattr(
        cli,
        "_token",
        lambda run_dir: (_ for _ in ()).throw(RuntimeError("token failed")),
    )
    with pytest.raises(RuntimeError, match="token failed"):
        await cli._serve(cast(RunSupervisor, supervisor), start_run=True)
    assert supervisor.closed
    assert not (tmp_path / "runwatch.lock").exists()

    supervisor.closed = False
    held_lock = cli.RunLock(tmp_path, controller_token=supervisor.controller_token)
    held_lock.acquire()
    with pytest.raises(RuntimeError, match="token failed"):
        await cli._serve(
            cast(RunSupervisor, supervisor),
            start_run=True,
            run_lock=held_lock,
        )
    assert supervisor.closed
    assert not held_lock.held
    assert not held_lock.path.exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("start_run", [True, False], ids=["execute", "open"])
async def test_serve_stops_work_when_server_crashes_after_startup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    start_run: bool,
) -> None:
    class FakeServer:
        def __init__(self, config: object) -> None:
            self.started = False
            self.should_exit = False
            self.install_signal_handlers: object | None = None

        async def serve(self) -> None:
            self.started = True
            await work_started.wait()
            raise RuntimeError("post-start server crash")

    class FakeSupervisor:
        run_dir = tmp_path
        controller_token = "controller-token"
        bus = object()
        config = RunwatchConfig.model_validate(
            {"server": {"open_browser": False, "show_qr": False}}
        )

        def attach_dashboard_links(self, manager: object) -> None:
            return None

        async def start(self) -> None:
            return None

    work_started = asyncio.Event()
    work_cancelled = asyncio.Event()
    finish_called = asyncio.Event()

    async def wait_for_server_failure(
        supervisor: RunSupervisor,
        *,
        start_run: bool,
        stop: cli._ServeSignalState | None = None,
    ) -> int:
        del supervisor, start_run, stop
        work_started.set()
        try:
            await asyncio.Event().wait()
        finally:
            work_cancelled.set()
        return 0

    async def record_finish(supervisor: RunSupervisor, lock: cli.RunLock) -> None:
        finish_called.set()
        lock.release()

    monkeypatch.setattr(cli, "_run_and_linger", wait_for_server_failure)
    monkeypatch.setattr(cli, "_finish_serve", record_finish)
    monkeypatch.setattr(cli, "_announce_run", lambda *args: None)
    monkeypatch.setattr(cli, "create_app", lambda *args: object())
    monkeypatch.setattr(cli, "DashboardLinkManager", lambda **kwargs: object())
    monkeypatch.setattr(cli.uvicorn, "Config", lambda *args, **kwargs: object())
    monkeypatch.setattr(cli.uvicorn, "Server", FakeServer)

    with pytest.raises(RuntimeError, match="web server exited unexpectedly") as raised:
        await cli._serve(cast(RunSupervisor, FakeSupervisor()), start_run=start_run)

    assert isinstance(raised.value.__cause__, RuntimeError)
    assert "post-start server crash" in str(raised.value.__cause__)
    assert work_started.is_set()
    assert work_cancelled.is_set()
    assert finish_called.is_set()
    assert not (tmp_path / "runwatch.lock").exists()


@pytest.mark.asyncio
async def test_server_crash_pauses_real_supervisor_after_runner_is_cancelled(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class CrashingServer:
        def __init__(self, config: object) -> None:
            self.started = False
            self.should_exit = False
            self.install_signal_handlers: object | None = None

        async def serve(self) -> None:
            self.started = True
            await runner_started.wait()
            raise RuntimeError("injected post-start server crash")

    notebook = tmp_path / "input.ipynb"
    _notebook(notebook)
    run_dir = tmp_path / ".runwatch" / "runs" / "server-crash"
    supervisor = RunSupervisor(
        notebook_path=notebook,
        output_path=run_dir / "executed.ipynb",
        working_dir=tmp_path,
        run_dir=run_dir,
        config=RunwatchConfig.model_validate(
            {"server": {"open_browser": False, "show_qr": False}}
        ),
    )
    run_id = supervisor.run_id
    runner_started = asyncio.Event()
    runner_cancelled = asyncio.Event()

    async def run_until_server_crashes() -> RunStatus:
        supervisor.store.update_run_status(
            run_id,
            RunStatus.RUNNING,
            message="Notebook executing",
            started=True,
        )
        runner_started.set()
        try:
            await asyncio.Event().wait()
        finally:
            runner_cancelled.set()
        return RunStatus.SUCCEEDED

    monkeypatch.setattr(supervisor.runner, "run", run_until_server_crashes)
    monkeypatch.setattr(cli, "_announce_run", lambda *args: None)
    monkeypatch.setattr(cli, "create_app", lambda *args: object())
    monkeypatch.setattr(cli.uvicorn, "Config", lambda *args, **kwargs: object())
    monkeypatch.setattr(cli.uvicorn, "Server", CrashingServer)

    with pytest.raises(RuntimeError, match="web server exited unexpectedly") as raised:
        await cli._serve(supervisor, start_run=True)

    assert isinstance(raised.value.__cause__, RuntimeError)
    assert "injected post-start server crash" in str(raised.value.__cause__)
    assert runner_cancelled.is_set()
    assert not (run_dir / "runwatch.lock").exists()

    reopened = RunStore(run_dir / "runwatch.sqlite3")
    try:
        run = reopened.get_run(run_id)
        assert run["status"] == RunStatus.PAUSED.value
        assert run["finalization_complete"] is False
        assert run["process_pid"] is None
        assert run["process_token"] is None
        assert run["server_port"] is None
        assert any(
            event["type"] == "run.process_stopped"
            for event in reopened.recent_events(run_id)
        )
    finally:
        reopened.close()


@pytest.mark.asyncio
async def test_serve_writes_back_and_cleans_successful_default_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class FakeServer:
        def __init__(self, config: object) -> None:
            self.config = config
            self.started = False
            self.should_exit = False
            self.install_signal_handlers: object | None = None

        async def serve(self) -> None:
            self.started = True
            while not self.should_exit:
                await asyncio.sleep(0.01)

    notebook = tmp_path / "input.ipynb"
    _notebook(notebook, "print('published')")
    run_dir = tmp_path / ".runwatch" / "runs" / "successful-run"
    config = RunwatchConfig.model_validate(
        {
            "notebook": {
                "kernel_name": "python3",
                "wait_for_blocking_resources": False,
            },
            "server": {
                "open_browser": False,
                "show_qr": False,
                "linger_seconds": 0,
            },
            "notifications": {
                "webhook_urls": ["https://hooks.example/terminal"],
            },
        }
    )
    supervisor = RunSupervisor(
        notebook_path=notebook,
        output_path=run_dir / "executed.ipynb",
        working_dir=tmp_path,
        run_dir=run_dir,
        config=config,
    )
    requests: list[httpx.Request] = []

    def notification_handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(204)

    await supervisor.notifications._client.aclose()
    supervisor.notifications._client = httpx.AsyncClient(
        transport=httpx.MockTransport(notification_handler)
    )
    server_options: dict[str, object] = {}

    def fake_server_config(*args: object, **kwargs: object) -> object:
        server_options.update(kwargs)
        return object()

    monkeypatch.setattr(cli.uvicorn, "Config", fake_server_config)
    monkeypatch.setattr(cli.uvicorn, "Server", FakeServer)
    monkeypatch.setattr(cli, "create_app", lambda *args: object())

    assert await cli._serve(supervisor, start_run=True) == 0

    updated = nbformat.read(notebook, as_version=4)
    assert updated.cells[0].outputs[0].text.strip() == "published"
    assert server_options["lifespan"] == "off"
    assert server_options["timeout_graceful_shutdown"] == 5
    assert len(requests) == 1
    assert not run_dir.exists()
    assert not (tmp_path / ".runwatch").exists()


@pytest.mark.asyncio
async def test_success_cleanup_retains_state_when_terminal_notification_is_pending(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    class FakeServer:
        def __init__(self, config: object) -> None:
            self.started = False
            self.should_exit = False
            self.install_signal_handlers: object | None = None

        async def serve(self) -> None:
            self.started = True
            while not self.should_exit:
                await asyncio.sleep(0.001)

    notebook = tmp_path / "input.ipynb"
    _notebook(notebook)
    run_dir = tmp_path / ".runwatch" / "runs" / "retained-run"
    config = RunwatchConfig.model_validate(
        {
            "server": {
                "open_browser": False,
                "show_qr": False,
                "linger_seconds": 0,
            },
            "notifications": {
                "webhook_urls": ["https://slow.example/terminal"],
                "terminal_drain_timeout_seconds": 0.01,
            },
        }
    )
    supervisor = RunSupervisor(
        notebook_path=notebook,
        output_path=run_dir / "executed.ipynb",
        working_dir=tmp_path,
        run_dir=run_dir,
        config=config,
    )
    delivery_started = asyncio.Event()
    never_release = asyncio.Event()

    async def slow_handler(request: httpx.Request) -> httpx.Response:
        delivery_started.set()
        await never_release.wait()
        return httpx.Response(204)

    await supervisor.notifications._client.aclose()
    supervisor.notifications._client = httpx.AsyncClient(
        transport=httpx.MockTransport(slow_handler)
    )

    async def finish_without_kernel(
        active: RunSupervisor,
        *,
        start_run: bool,
        stop: cli._ServeSignalState | None = None,
    ) -> int:
        del stop
        assert start_run
        await active.notifications.start()
        event = active.store.finish_run(
            active.run_id,
            RunStatus.SUCCEEDED,
            message="completed",
            event_type="run.succeeded",
            event_payload={"kernel_epoch": 0},
        )
        active.bus.fan_out_persisted(event)
        active.store.mark_run_finalized(active.run_id, RunStatus.SUCCEEDED)
        active._wait_completed_normally = True  # noqa: SLF001 - cleanup gate setup
        await asyncio.wait_for(delivery_started.wait(), timeout=1)
        return 0

    monkeypatch.setattr(cli, "_run_and_linger", finish_without_kernel)
    monkeypatch.setattr(cli, "create_app", lambda *args: object())
    monkeypatch.setattr(cli.uvicorn, "Config", lambda *args, **kwargs: object())
    monkeypatch.setattr(cli.uvicorn, "Server", FakeServer)
    assert await cli._serve(supervisor, start_run=True) == 0

    output = capsys.readouterr().out
    assert "Retained Runwatch state" in output
    assert "delivery attempt(s) remain pending" in output
    assert f"runwatch open {run_dir}" in output
    assert run_dir.is_dir()
    manifest = RunSupervisor.read_manifest(run_dir)
    reopened = RunStore(run_dir / "runwatch.sqlite3")
    try:
        state = reopened.notification_outbox_state(manifest["run_id"])
        assert state["nonterminal_intents"] == 1
        assert state["pending_deliveries"] == 1
        retained = [
            event
            for event in reopened.recent_events(manifest["run_id"], limit=1_000)
            if event["type"] == "run.cleanup_retained"
        ]
        assert retained[-1]["payload"]["recovery_command"] == (
            f"runwatch open {run_dir}"
        )
    finally:
        reopened.close()

    recovery = RunSupervisor.reopen(run_dir)
    recovery.config.notifications.terminal_drain_timeout_seconds = 1.0
    requests: list[httpx.Request] = []

    def recovery_handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(204)

    await recovery.notifications._client.aclose()
    recovery.notifications._client = httpx.AsyncClient(
        transport=httpx.MockTransport(recovery_handler)
    )
    await recovery.notifications.start()
    recovery_lock = cli.RunLock(run_dir, controller_token=recovery.controller_token)
    recovery_lock.acquire()

    await cli._finish_serve(recovery, recovery_lock)

    assert len(requests) == 1
    assert run_dir.exists()
    assert (tmp_path / ".runwatch").exists()


@pytest.mark.asyncio
async def test_open_preserves_successful_keep_run_state(tmp_path: Path) -> None:
    notebook = tmp_path / "input.ipynb"
    _notebook(notebook)
    run_dir = tmp_path / ".runwatch" / "runs" / "kept-run"
    supervisor = RunSupervisor(
        notebook_path=notebook,
        output_path=run_dir / "executed.ipynb",
        working_dir=tmp_path,
        run_dir=run_dir,
        config=RunwatchConfig(),
        cleanup_on_success=False,
    )
    supervisor.store.finish_run(
        supervisor.run_id,
        RunStatus.SUCCEEDED,
        message="completed",
        event_type="run.succeeded",
        event_payload={"kernel_epoch": 0},
    )
    await supervisor.close()

    recovery = RunSupervisor.reopen(run_dir)
    recovery_lock = cli.RunLock(run_dir, controller_token=recovery.controller_token)
    recovery_lock.acquire()

    await cli._finish_serve(recovery, recovery_lock)

    assert run_dir.is_dir()
    assert RunSupervisor.read_manifest(run_dir)["cleanup_on_success"] is False


@pytest.mark.asyncio
async def test_cleanup_quiesces_producers_before_terminal_notification_drain(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    notebook = tmp_path / "input.ipynb"
    _notebook(notebook)
    run_dir = tmp_path / ".runwatch" / "runs" / "late-event-run"
    config = RunwatchConfig.model_validate(
        {
            "notifications": {
                "webhook_urls": ["https://hooks.example/terminal"],
                "terminal_drain_timeout_seconds": 1.0,
            }
        }
    )
    supervisor = RunSupervisor(
        notebook_path=notebook,
        output_path=run_dir / "executed.ipynb",
        working_dir=tmp_path,
        run_dir=run_dir,
        config=config,
    )
    requests: list[httpx.Request] = []

    def notification_handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(204)

    await supervisor.notifications._client.aclose()
    supervisor.notifications._client = httpx.AsyncClient(
        transport=httpx.MockTransport(notification_handler)
    )
    await supervisor.notifications.start()
    succeeded = supervisor.store.finish_run(
        supervisor.run_id,
        RunStatus.SUCCEEDED,
        message="completed",
        event_type="run.succeeded",
        event_payload={"kernel_epoch": 0},
    )
    supervisor.bus.fan_out_persisted(succeeded)
    supervisor.store.mark_run_finalized(supervisor.run_id, RunStatus.SUCCEEDED)
    supervisor._wait_completed_normally = True  # noqa: SLF001 - cleanup gate setup
    order: list[str] = []
    original_shutdown = supervisor.resources.shutdown

    async def shutdown_with_late_event() -> None:
        order.append("producer")
        await supervisor.bus.publish(
            "cell.failed",
            {
                "kernel_epoch": 0,
                "cell_index": 0,
                "attempt": 1,
                "error_name": "ValueError",
                "error_value": "late failure",
            },
        )
        await original_shutdown()

    monkeypatch.setattr(supervisor.resources, "shutdown", shutdown_with_late_event)
    original_drain = supervisor.notifications.drain

    async def recorded_drain(timeout_seconds: float):
        order.append("drain")
        return await original_drain(timeout_seconds)

    monkeypatch.setattr(supervisor.notifications, "drain", recorded_drain)
    lock = cli.RunLock(run_dir, controller_token=supervisor.controller_token)
    lock.acquire()

    await cli._finish_serve(supervisor, lock)

    assert order == ["producer", "drain"]
    assert len(requests) == 2
    assert not run_dir.exists()


def test_wait_action_completes_and_times_out(monkeypatch: pytest.MonkeyPatch) -> None:
    class Store:
        def __init__(self) -> None:
            self.calls = 0

        def get_action(self, action_id: str) -> dict[str, Any] | None:
            self.calls += 1
            if self.calls < 2:
                return None
            return {"status": ActionStatus.COMPLETED.value}

    store = Store()
    monkeypatch.setattr(cli.time, "sleep", lambda seconds: None)
    assert cli._wait_action(cast(RunStore, store), "action", 1)["status"] == "completed"

    clock = iter([0.0, 0.0, 2.0])
    monkeypatch.setattr(cli.time, "monotonic", lambda: next(clock))
    with pytest.raises(TimeoutError, match="action"):
        cli._wait_action(
            cast(RunStore, SimpleNamespace(get_action=lambda action_id: None)),
            "action",
            1,
        )


def test_basic_cli_commands_and_preflight(tmp_path: Path, monkeypatch) -> None:
    version = runner.invoke(cli.app, ["version"])
    assert version.exit_code == 0
    assert version.stdout.strip()

    config_path = tmp_path / "runwatch.yaml"
    created = runner.invoke(cli.app, ["init-config", str(config_path)])
    assert created.exit_code == 0
    duplicate = runner.invoke(cli.app, ["init-config", str(config_path)])
    assert duplicate.exit_code != 0
    forced = runner.invoke(cli.app, ["init-config", str(config_path), "--force"])
    assert forced.exit_code == 0

    notebook = tmp_path / "input.ipynb"
    _notebook(notebook)
    monkeypatch.setattr(
        cli,
        "validate_execution",
        lambda *args, **kwargs: {
            "valid": True,
            "notebook": str(notebook),
            "working_dir": str(tmp_path),
            "kernel_name": "python3",
            "cell_count": 1,
            "code_cell_count": 1,
            "configured_resources": [],
            "errors": [],
            "warnings": ["dynamic resources unknown"],
        },
    )
    text = runner.invoke(cli.app, ["validate", str(notebook)])
    assert text.exit_code == 0
    assert "1 cells" in text.stdout
    assert "dynamic resources unknown" in text.stdout
    structured = runner.invoke(cli.app, ["validate", str(notebook), "--json"])
    assert structured.exit_code == 0
    assert json.loads(structured.stdout)["valid"] is True

    monkeypatch.setattr(
        cli, "load_config", lambda path: (_ for _ in ()).throw(ValueError("bad yaml"))
    )
    invalid = runner.invoke(cli.app, ["validate", str(notebook), "--json"])
    assert invalid.exit_code == 1
    assert "bad yaml" in invalid.stdout


def test_execute_rejects_output_that_aliases_input_before_writing(
    tmp_path: Path,
) -> None:
    notebook = tmp_path / "input.ipynb"
    _notebook(notebook)
    original = notebook.read_bytes()
    run_dir = tmp_path / "run"

    result = runner.invoke(
        cli.app,
        [
            "execute",
            str(notebook),
            "--output",
            str(notebook),
            "--run-dir",
            str(run_dir),
        ],
    )

    assert result.exit_code != 0
    assert "would overwrite" in str(result.exception)
    assert notebook.read_bytes() == original
    assert not run_dir.exists()


def test_execute_keep_run_persists_cleanup_policy(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    notebook = tmp_path / "input.ipynb"
    run_dir = tmp_path / "kept-run"
    _notebook(notebook)

    async def fake_serve(
        supervisor: RunSupervisor,
        *,
        start_run: bool,
        run_lock: cli.RunLock | None = None,
    ) -> int:
        assert start_run
        assert run_lock is not None and run_lock.held
        assert run_lock.controller_token == supervisor.controller_token
        assert supervisor.cleanup_on_success is False
        await supervisor.close()
        return 0

    monkeypatch.setattr(cli, "_serve", fake_serve)
    result = runner.invoke(
        cli.app,
        [
            "execute",
            str(notebook),
            "--run-dir",
            str(run_dir),
            "--keep-run",
            "--no-browser",
            "--no-qr",
        ],
    )

    assert result.exit_code == 0
    assert RunSupervisor.read_manifest(run_dir)["cleanup_on_success"] is False


def test_execute_does_not_initialize_an_explicit_directory_owned_by_another_controller(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    notebook = tmp_path / "input.ipynb"
    _notebook(notebook)
    run_dir = tmp_path / "contended-run"
    owner = cli.RunLock(run_dir)
    owner.acquire()
    constructed = False

    def unexpected_supervisor(*args, **kwargs):
        nonlocal constructed
        constructed = True
        raise AssertionError("RunSupervisor must not be constructed without the lock")

    monkeypatch.setattr(cli, "RunSupervisor", unexpected_supervisor)
    try:
        result = runner.invoke(
            cli.app,
            ["execute", str(notebook), "--run-dir", str(run_dir)],
        )
    finally:
        owner.release()

    assert result.exit_code != 0
    assert "already owns" in str(result.exception)
    assert not constructed


def test_open_does_not_reopen_state_owned_by_another_controller(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    supervisor = _supervisor(tmp_path / "existing")
    run_dir = supervisor.run_dir
    database = run_dir / "runwatch.sqlite3"
    supervisor.store.close()
    owner = cli.RunLock(run_dir)
    owner.acquire()
    run_dir.chmod(0o755)
    database_before = database.read_bytes()
    constructed = False

    def unexpected_reopen(*args: object, **kwargs: object) -> RunSupervisor:
        nonlocal constructed
        constructed = True
        raise AssertionError("RunSupervisor must not be reopened without the lock")

    monkeypatch.setattr(cli.RunSupervisor, "reopen", unexpected_reopen)
    try:
        result = runner.invoke(cli.app, ["open", str(run_dir)])
    finally:
        owner.release()

    assert result.exit_code != 0
    assert "already owns" in str(result.exception)
    assert not constructed
    assert run_dir.stat().st_mode & 0o777 == 0o755
    assert database.read_bytes() == database_before


def test_execute_resume_restart_and_open_dispatch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    notebook = tmp_path / "input.ipynb"
    _notebook(notebook)
    served: list[tuple[bool, int]] = []

    async def fake_serve(
        supervisor: RunSupervisor,
        *,
        start_run: bool,
        run_lock: cli.RunLock | None = None,
    ) -> int:
        served.append((start_run, supervisor.runner._initial_from_cell))
        if run_lock is not None:
            assert run_lock.held
            assert run_lock.controller_token == supervisor.controller_token
        await supervisor.close()
        return 0

    monkeypatch.setattr(cli, "_serve", fake_serve)
    executed = runner.invoke(
        cli.app,
        [
            "execute",
            str(notebook),
            "--run-dir",
            str(tmp_path / "execute-run"),
            "--no-browser",
            "--no-qr",
        ],
    )
    assert executed.exit_code == 0
    assert served[-1] == (True, 0)

    occupied = tmp_path / "occupied-run"
    occupied.mkdir()
    (occupied / "keep.txt").write_text("owned", encoding="utf-8")
    rejected = runner.invoke(
        cli.app, ["execute", str(notebook), "--run-dir", str(occupied)]
    )
    assert rejected.exit_code != 0
    assert "is not" in rejected.output
    assert "empty; choose a new directory" in rejected.output
    assert (occupied / "keep.txt").read_text(encoding="utf-8") == "owned"

    supervisor = _supervisor(tmp_path / "existing")
    run_dir = supervisor.run_dir
    source = nbformat.read(supervisor.source_path, as_version=4)
    source.cells.append(nbformat.v4.new_code_cell("print('second')"))
    nbformat.write(source, supervisor.source_path)
    supervisor.store.close()
    monkeypatch.setattr(cli, "_queue_live_recovery", lambda *args: False)
    resumed = runner.invoke(cli.app, ["resume", str(run_dir)])
    restarted = runner.invoke(cli.app, ["restart", str(run_dir), "--from-cell", "1"])
    opened = runner.invoke(cli.app, ["open", str(run_dir)])
    assert resumed.exit_code == restarted.exit_code == opened.exit_code == 0
    assert served[-3:] == [(True, 0), (True, 1), (False, 0)]

    monkeypatch.setattr(cli, "_queue_live_recovery", lambda *args: True)
    assert runner.invoke(cli.app, ["resume", str(run_dir)]).exit_code == 0
    assert runner.invoke(cli.app, ["restart", str(run_dir)]).exit_code == 0


def test_restart_rejects_out_of_range_from_cell(tmp_path: Path, monkeypatch) -> None:
    supervisor = _supervisor(tmp_path)
    run_dir = supervisor.run_dir
    supervisor.store.close()
    monkeypatch.setattr(cli, "_queue_live_recovery", lambda *args: False)
    result = runner.invoke(cli.app, ["restart", str(run_dir), "--from-cell", "2"])
    assert result.exit_code != 0
    assert isinstance(result.exception, ValueError)
    assert "outside the notebook" in str(result.exception)


def test_dead_terminal_run_rejects_resume_but_restart_reruns(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    supervisor = _supervisor(tmp_path)
    supervisor.store.finish_run(
        supervisor.run_id,
        RunStatus.SUCCEEDED,
        message="completed",
        event_type="run.succeeded",
        event_payload={"kernel_epoch": 0},
    )
    run_dir = supervisor.run_dir
    supervisor.store.close()
    monkeypatch.setattr(cli, "_queue_live_recovery", lambda *args: False)
    observed_epochs: list[int] = []

    async def run_without_server(
        reopened: RunSupervisor,
        *,
        start_run: bool,
        run_lock: cli.RunLock | None = None,
    ) -> int:
        assert start_run
        assert run_lock is not None and run_lock.held
        await reopened.start()
        assert await reopened.wait() is RunStatus.SUCCEEDED
        observed_epochs.append(reopened.store.get_run(reopened.run_id)["kernel_epoch"])
        await reopened.close()
        return 0

    monkeypatch.setattr(cli, "_serve", run_without_server)

    resumed = runner.invoke(cli.app, ["resume", str(run_dir)])
    restarted = runner.invoke(cli.app, ["restart", str(run_dir)])

    assert resumed.exit_code != 0
    assert "cannot be resumed" in str(resumed.exception)
    assert restarted.exit_code == 0
    assert observed_epochs == [1]


def test_status_context_and_events_commands(tmp_path: Path) -> None:
    supervisor = _supervisor(tmp_path)
    asyncio.run(supervisor.bus.publish("probe.event", {"value": 3}))
    supervisor.store.update_run_status(
        supervisor.run_id, RunStatus.PAUSED, message="Needs repair"
    )
    supervisor.store.close()

    status_json = runner.invoke(cli.app, ["status", str(supervisor.run_dir), "--json"])
    assert status_json.exit_code == 0
    assert json.loads(status_json.stdout)["run"]["status"] == "paused"
    status_text = runner.invoke(cli.app, ["status", str(supervisor.run_dir)])
    assert "demo: paused" in status_text.stdout

    context_json = runner.invoke(
        cli.app, ["context", str(supervisor.run_dir), "--format", "json"]
    )
    assert context_json.exit_code == 0
    assert json.loads(context_json.stdout)["source_path"].endswith("source.ipynb")
    context_text = runner.invoke(cli.app, ["context", str(supervisor.run_dir)])
    assert "# Runwatch context: demo" in context_text.stdout

    events_json = runner.invoke(cli.app, ["events", str(supervisor.run_dir), "--json"])
    assert events_json.exit_code == 0
    assert any(
        json.loads(line)["type"] == "probe.event"
        for line in events_json.stdout.splitlines()
    )
    events_text = runner.invoke(cli.app, ["events", str(supervisor.run_dir)])
    assert "probe.event" in events_text.stdout


def test_status_context_and_events_exclude_internal_canaries(tmp_path: Path) -> None:
    root = tmp_path / "canary"
    root.mkdir()
    notebook = root / "input.ipynb"
    _notebook(notebook)
    supervisor = RunSupervisor(
        notebook_path=notebook,
        output_path=root / "executed.ipynb",
        working_dir=root,
        run_dir=root / "run",
        config=RunwatchConfig(
            notifications=NotificationSettings(
                webhook_urls=["https://hooks.example/run?token=WEBHOOK_CONFIG_SECRET"],
                ntfy_base_url="https://ntfy.example",
                ntfy_topic="NTFY_CONFIG_SECRET",
            )
        ),
        name="demo",
    )
    supervisor.store.update_process(
        supervisor.run_id,
        process_pid=os.getpid(),
        process_started_at=cli.process_start_time(os.getpid()),
        process_token="CONTROLLER_TOKEN_SECRET",
        server_port=8765,
    )
    supervisor.store.update_run_status(
        supervisor.run_id,
        RunStatus.PAUSED,
        message="Cell failed with CELL_ERROR_SECRET",
        failed_cell_index=0,
        failed_attempt=1,
    )
    asyncio.run(
        supervisor.bus.publish(
            "probe.event",
            {
                "credential": "EVENT_PAYLOAD_SECRET",
                "message": "PROVIDER_MESSAGE_SECRET",
            },
        )
    )
    supervisor.store.close()

    results = [
        runner.invoke(cli.app, ["status", str(supervisor.run_dir), "--json"]),
        runner.invoke(cli.app, ["status", str(supervisor.run_dir)]),
        runner.invoke(
            cli.app, ["context", str(supervisor.run_dir), "--format", "json"]
        ),
        runner.invoke(cli.app, ["context", str(supervisor.run_dir)]),
        runner.invoke(cli.app, ["events", str(supervisor.run_dir), "--json"]),
        runner.invoke(cli.app, ["events", str(supervisor.run_dir)]),
    ]
    assert all(result.exit_code == 0 for result in results)
    serialized = "\n".join(result.stdout for result in results)
    for secret in (
        "WEBHOOK_CONFIG_SECRET",
        "NTFY_CONFIG_SECRET",
        "CONTROLLER_TOKEN_SECRET",
        "CELL_ERROR_SECRET",
        "EVENT_PAYLOAD_SECRET",
        "PROVIDER_MESSAGE_SECRET",
    ):
        assert secret not in serialized
    status_payload = json.loads(results[0].stdout)
    assert status_payload["schema_version"] == 1
    assert set(status_payload["run"]).isdisjoint(
        {"metadata", "process_token", "kernel_id", "notebook_path", "working_dir"}
    )
    event_payload = json.loads(results[4].stdout.splitlines()[-1])
    assert event_payload["data"] == {}


def test_notification_rotate_and_purge_scrub_persisted_credentials(
    tmp_path: Path,
) -> None:
    root = tmp_path / "notification-maintenance"
    root.mkdir()
    notebook = root / "input.ipynb"
    _notebook(notebook)
    old = NotificationSettings(
        webhook_urls=["https://old.example/hook?token=OLD_MANIFEST_SECRET"],
        ntfy_base_url="https://old-ntfy.example",
        ntfy_topic="OLD_TOPIC_SECRET",
    )
    supervisor = RunSupervisor(
        notebook_path=notebook,
        output_path=root / "executed.ipynb",
        working_dir=root,
        run_dir=root / "run",
        config=RunwatchConfig(notifications=old),
        name="demo",
    )
    intent = supervisor.store.enqueue_notification(
        run_id=supervisor.run_id,
        title="OLD_TITLE_SECRET",
        message="OLD_MESSAGE_SECRET",
        data={"legacy": "OLD_DATA_SECRET"},
        dedup_key="credential-maintenance",
        destinations=notification_destinations(old),
    )
    supervisor.store.append_event(
        supervisor.run_id,
        "notification.failed",
        {"error": "OLD_EVENT_SECRET https://old.example/response"},
    )
    with supervisor.store._lock:
        supervisor.store._connection.execute(
            "UPDATE notification_deliveries SET last_error = ? WHERE intent_id = ?",
            ("OLD_ERROR_SECRET response body", intent["intent_id"]),
        )
        supervisor.store._connection.commit()
    asyncio.run(supervisor.notifications._client.aclose())
    supervisor.store.close()

    desired = NotificationSettings(
        webhook_urls=["https://new.example/hook?token=NEW_MANIFEST_SECRET"],
        ntfy_base_url="https://new-ntfy.example",
        ntfy_topic="NEW_TOPIC_SECRET",
    )
    config_path = root / "rotated.json"
    config_path.write_text(
        json.dumps(RunwatchConfig(notifications=desired).model_dump(mode="json")),
        encoding="utf-8",
    )

    rotated = runner.invoke(
        cli.app,
        [
            "notifications",
            "rotate",
            str(supervisor.run_dir),
            "--config",
            str(config_path),
        ],
    )
    assert rotated.exit_code == 0, rotated.output
    manifest_text = (supervisor.run_dir / "run-manifest.json").read_text(
        encoding="utf-8"
    )
    assert "NEW_MANIFEST_SECRET" in manifest_text
    for old_secret in (
        "OLD_MANIFEST_SECRET",
        "OLD_TOPIC_SECRET",
        "OLD_TITLE_SECRET",
        "OLD_MESSAGE_SECRET",
        "OLD_DATA_SECRET",
        "OLD_EVENT_SECRET",
        "OLD_ERROR_SECRET",
    ):
        assert old_secret not in manifest_text

    refused = runner.invoke(
        cli.app, ["notifications", "purge", str(supervisor.run_dir)]
    )
    assert refused.exit_code != 0
    purged = runner.invoke(
        cli.app, ["notifications", "purge", str(supervisor.run_dir), "--yes"]
    )
    assert purged.exit_code == 0, purged.output

    final_manifest = (supervisor.run_dir / "run-manifest.json").read_text(
        encoding="utf-8"
    )
    for secret in (
        "OLD_MANIFEST_SECRET",
        "OLD_TOPIC_SECRET",
        "NEW_MANIFEST_SECRET",
        "NEW_TOPIC_SECRET",
    ):
        assert secret not in final_manifest
    reopened = RunStore(supervisor.run_dir / "runwatch.sqlite3")
    with reopened._lock:
        dump = "\n".join(reopened._connection.iterdump())
    assert reopened.notification_delivery_topology(supervisor.run_id) == ()
    metadata = reopened.get_run(supervisor.run_id)["metadata"]
    assert metadata["_notification_routing_required"] is False
    assert metadata["_notification_event_cursor"] >= 1
    diagnostics = [
        event
        for event in reopened.recent_events(supervisor.run_id, limit=100)
        if event["type"].startswith("notification.")
    ]
    assert diagnostics and all(event["payload"] == {} for event in diagnostics)
    reopened.close()
    for secret in (
        "OLD_MANIFEST_SECRET",
        "OLD_TOPIC_SECRET",
        "OLD_TITLE_SECRET",
        "OLD_MESSAGE_SECRET",
        "OLD_DATA_SECRET",
        "OLD_EVENT_SECRET",
        "OLD_ERROR_SECRET",
        "NEW_MANIFEST_SECRET",
        "NEW_TOPIC_SECRET",
    ):
        assert secret not in dump
        for path in (
            supervisor.run_dir / "runwatch.sqlite3",
            supervisor.run_dir / "runwatch.sqlite3-wal",
        ):
            if path.exists():
                assert secret.encode() not in path.read_bytes()


def test_notification_maintenance_rejects_live_run_lock(tmp_path: Path) -> None:
    supervisor = _supervisor(tmp_path)
    config_path = tmp_path / "config.json"
    config_path.write_text(
        json.dumps(RunwatchConfig().model_dump(mode="json")), encoding="utf-8"
    )
    asyncio.run(supervisor.notifications._client.aclose())
    supervisor.store.close()
    owner = cli.RunLock(supervisor.run_dir)
    owner.acquire()
    try:
        rotate = runner.invoke(
            cli.app,
            [
                "notifications",
                "rotate",
                str(supervisor.run_dir),
                "--config",
                str(config_path),
            ],
        )
        purge = runner.invoke(
            cli.app,
            ["notifications", "purge", str(supervisor.run_dir), "--yes"],
        )
        assert rotate.exit_code != 0
        assert purge.exit_code != 0
    finally:
        owner.release()


def test_live_recovery_queue_and_resource_stop_paths(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    supervisor = _supervisor(tmp_path)
    run_dir = supervisor.run_dir
    supervisor.store.update_process(
        supervisor.run_id, process_pid=os.getpid(), server_port=8765
    )
    supervisor.store.update_run_status(supervisor.run_id, RunStatus.PAUSED)
    supervisor.store.close()

    monkeypatch.setattr(
        cli,
        "_wait_action",
        lambda store, action_id, timeout: {
            "status": "completed",
            "action_id": action_id,
        },
    )
    assert cli._queue_live_recovery(run_dir, ActionKind.RESUME, 0)

    monkeypatch.setattr(cli, "controller_is_alive", lambda run: False)
    assert not cli._queue_live_recovery(run_dir, ActionKind.RESUME, 0)

    offline_calls: list[tuple[Path, str]] = []

    async def offline(path: Path, action_id: str) -> None:
        offline_calls.append((path, action_id))

    monkeypatch.setattr(cli, "_stop_resources_offline", offline)
    result = runner.invoke(cli.app, ["resource", "stop", str(run_dir), "missing"])
    assert result.exit_code != 0

    class FakeStore:
        def get_run(self, run_id: str) -> dict[str, Any]:
            return {
                "process_pid": None,
                "kernel_epoch": 0,
                "status": RunStatus.CREATED.value,
            }

        def get_resource(self, resource_id: str) -> dict[str, Any] | None:
            return {"version": 2}

        def recover_incomplete_actions(self, run_id: str) -> int:
            return 0

        def list_actions(self, run_id: str, limit: int) -> list[dict[str, Any]]:
            return []

        def create_action(self, *args: Any, **kwargs: Any) -> str:
            return "offline-action"

        def close(self) -> None:
            pass

    monkeypatch.setattr(
        cli,
        "_run_store",
        lambda path: (
            {"run_id": "run"},
            RunwatchConfig(),
            FakeStore(),
        ),
    )
    result = runner.invoke(cli.app, ["resource", "stop", str(run_dir), "resource"])
    assert result.exit_code == 0
    assert offline_calls == [(run_dir, "offline-action")]


def test_offline_resource_stop_holds_lock_and_rechecks_controller(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    calls: list[str] = []

    class Store:
        def get_run(self, run_id: str) -> dict[str, Any]:
            calls.append("get-run")
            return {
                "process_pid": None,
                "kernel_epoch": 0,
                "status": RunStatus.CREATED.value,
            }

        def get_resource(self, resource_id: str) -> dict[str, Any] | None:
            return {"version": 1}

        def recover_incomplete_actions(self, run_id: str) -> int:
            calls.append("recover")
            return 0

        def list_actions(self, run_id: str, limit: int) -> list[dict[str, Any]]:
            return []

        def create_action(self, *args: Any, **kwargs: Any) -> str:
            calls.append("create")
            return "action"

        def close(self) -> None:
            pass

    class Lock:
        held = False

        def __init__(self, path: Path) -> None:
            assert path == run_dir

        def acquire(self) -> None:
            self.held = True
            calls.append("lock")

        def release(self) -> None:
            self.held = False
            calls.append("unlock")

    lock = Lock(run_dir)
    monkeypatch.setattr(cli, "RunLock", lambda path: lock)
    monkeypatch.setattr(
        cli,
        "_run_store",
        lambda path: ({"run_id": "run"}, RunwatchConfig(), Store()),
    )
    alive_checks = 0

    def controller_alive(run: dict[str, Any]) -> bool:
        nonlocal alive_checks
        alive_checks += 1
        assert lock.held is (alive_checks == 2)
        return False

    async def offline(path: Path, action_id: str) -> None:
        assert lock.held
        calls.append("provider-stop")

    monkeypatch.setattr(cli, "controller_is_alive", controller_alive)
    monkeypatch.setattr(cli, "_stop_resources_offline", offline)

    result = runner.invoke(cli.app, ["resource", "stop", str(run_dir), "resource"])

    assert result.exit_code == 0
    assert alive_checks == 2
    assert calls.index("lock") < calls.index("recover") < calls.index("provider-stop")
    assert calls[-1] == "unlock"


def test_offline_resource_stop_aborts_if_controller_appears_under_lock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    created = False
    stopped = False

    class Store:
        def get_run(self, run_id: str) -> dict[str, Any]:
            return {
                "process_pid": None,
                "kernel_epoch": 0,
                "status": RunStatus.CREATED.value,
            }

        def get_resource(self, resource_id: str) -> dict[str, Any] | None:
            return {"version": 1}

        def create_action(self, *args: Any, **kwargs: Any) -> str:
            nonlocal created
            created = True
            return "action"

        def close(self) -> None:
            pass

    class Lock:
        held = False

        def __init__(self, path: Path) -> None:
            pass

        def acquire(self) -> None:
            self.held = True

        def release(self) -> None:
            self.held = False

    lock = Lock(run_dir)
    checks = iter([False, True])
    monkeypatch.setattr(cli, "RunLock", lambda path: lock)
    monkeypatch.setattr(
        cli,
        "_run_store",
        lambda path: ({"run_id": "run"}, RunwatchConfig(), Store()),
    )
    monkeypatch.setattr(cli, "controller_is_alive", lambda run: next(checks))

    async def offline(path: Path, action_id: str) -> None:
        nonlocal stopped
        stopped = True

    monkeypatch.setattr(cli, "_stop_resources_offline", offline)

    result = runner.invoke(cli.app, ["resource", "stop", str(run_dir), "resource"])

    assert result.exit_code != 0
    assert "became active" in str(result.exception)
    assert created is False
    assert stopped is False
    assert lock.held is False


def test_offline_recovery_action_is_durable_and_reused(tmp_path: Path) -> None:
    supervisor = _supervisor(tmp_path)
    run_dir = supervisor.run_dir
    supervisor.store.update_run_status(supervisor.run_id, RunStatus.PAUSED)
    supervisor.store.close()

    lock = cli.RunLock(run_dir)
    lock.acquire()
    try:
        action_id = cli._offline_recovery_action(
            run_dir, ActionKind.RESTART, 0, run_lock=lock
        )
        assert (
            cli._offline_recovery_action(run_dir, ActionKind.RESTART, 0, run_lock=lock)
            == action_id
        )
    finally:
        lock.release()
    manifest, _config, store = cli._run_store(run_dir)
    try:
        action = store.get_action(action_id)
        assert action is not None
        assert action["status"] == ActionStatus.REQUESTED.value
        assert action["payload"]["offline_recovery"] is True
        claimed = store.claim_action(action_id)
        assert claimed is not None
        assert store.recover_incomplete_actions(manifest["run_id"]) == 1
        recovered = store.get_action(action_id)
        assert recovered is not None and recovered["payload"]["recovered"] is True
    finally:
        store.close()


@pytest.mark.parametrize(
    "command",
    [
        ["resume"],
        ["restart", "--from-cell", "0"],
    ],
)
def test_stopped_recovery_rechecks_controller_under_lock_before_journal_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    command: list[str],
) -> None:
    supervisor = _supervisor(tmp_path)
    supervisor.store.update_run_status(supervisor.run_id, RunStatus.PAUSED)
    sentinel_id = supervisor.store.create_action(
        supervisor.run_id,
        ActionKind.STOP_RESOURCE,
        payload={"internal_id": "sentinel", "expected_version": 1},
        expected_kernel_epoch=0,
    )
    assert supervisor.store.claim_action(sentinel_id) is not None
    run_dir = supervisor.run_dir
    supervisor.store.close()
    checks = iter([False, True])
    monkeypatch.setattr(cli, "controller_is_alive", lambda run: next(checks))

    result = runner.invoke(cli.app, [*command, str(run_dir)])

    assert result.exit_code != 0
    assert "became active" in str(result.exception)
    manifest, _config, store = cli._run_store(run_dir)
    try:
        actions = store.list_actions(manifest["run_id"], limit=10)
        assert [action["action_id"] for action in actions] == [sentinel_id]
        assert actions[0]["status"] == ActionStatus.EXECUTING.value
    finally:
        store.close()
    assert not (run_dir / "runwatch.lock").exists()


def test_crash_interrupted_offline_stop_is_recovered(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    supervisor = _supervisor(tmp_path)
    action_id = supervisor.store.create_action(
        supervisor.run_id,
        ActionKind.STOP_RESOURCE,
        payload={
            "internal_id": "resource",
            "expected_version": 1,
            "offline": True,
        },
        expected_kernel_epoch=0,
    )
    assert supervisor.store.claim_action(action_id) is not None
    supervisor.store.close()
    calls: list[str] = []

    class FakeManager:
        def __init__(self, **kwargs: Any) -> None:
            pass

        async def restore_monitors(self) -> None:
            calls.append("restore")

        async def stop_resource(
            self,
            resource_id: str,
            *,
            expected_version: int | None,
            on_stop_accepted: Any,
            allow_stopping: bool,
        ) -> None:
            assert expected_version is None
            assert allow_stopping is True
            calls.append(resource_id)
            await on_stop_accepted()

        async def stop_cancel_resources(self) -> list[str]:
            calls.append("cascade")
            return []

        async def shutdown(self) -> None:
            calls.append("shutdown")

    monkeypatch.setattr(cli, "ResourceManager", FakeManager)
    asyncio.run(cli._stop_resources_offline(supervisor.run_dir, action_id))

    manifest, _config, store = cli._run_store(supervisor.run_dir)
    try:
        action = store.get_action(action_id)
        assert action is not None and action["status"] == "completed"
        assert action["payload"]["recovered"] is True
        assert store.get_run(manifest["run_id"])["status"] == "cancelled"
        cancelled = next(
            event
            for event in store.recent_events(manifest["run_id"], limit=20)
            if event["type"] == "run.cancelled"
        )
        assert cancelled["payload"] == {
            "offline": True,
            "kernel_epoch": 0,
            "projection_truncated": False,
        }
        assert calls == ["restore", "resource", "cascade", "shutdown"]
    finally:
        store.close()


def test_offline_stop_recovery_requires_confirmed_cancelled_disposition() -> None:
    action = {"payload": {"recovered": True}}
    active: Any = SimpleNamespace(
        get_resource=lambda resource_id: {
            "terminal": True,
            "disposition": "active",
        }
    )
    cancelled: Any = SimpleNamespace(
        get_resource=lambda resource_id: {
            "terminal": True,
            "disposition": "cancelled",
        }
    )

    assert not cli._recover_confirmed_offline_stop(active, action, "resource")
    assert cli._recover_confirmed_offline_stop(cancelled, action, "resource")
