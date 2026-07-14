# pyright: reportMissingParameterType=false, reportPrivateUsage=false, reportUnknownArgumentType=false, reportUnknownLambdaType=false, reportUnknownMemberType=false, reportUnknownParameterType=false
from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import nbformat
import pytest
from typer.testing import CliRunner

import runwatch.cli as cli
from runwatch.models import (
    ActionKind,
    ActionStatus,
    RunStatus,
    RunwatchConfig,
)
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
    lock_record = json.loads(lock.path.read_text(encoding="utf-8"))
    assert lock_record["pid"] == os.getpid()
    assert lock_record["started_at"] > 0
    assert lock_record["controller_token"]
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


def test_success_cleanup_removes_empty_runwatch_parents(tmp_path: Path) -> None:
    run_dir = tmp_path / ".runwatch" / "runs" / "successful-run"
    run_dir.mkdir(parents=True)
    (run_dir / "run-manifest.json").write_text("{}", encoding="utf-8")
    (run_dir / "runwatch.sqlite3").write_bytes(b"state")

    cli._cleanup_successful_run(run_dir, tmp_path)

    assert not run_dir.exists()
    assert not (tmp_path / ".runwatch").exists()


def test_success_cleanup_preserves_other_runwatch_state(tmp_path: Path) -> None:
    runs_dir = tmp_path / ".runwatch" / "runs"
    run_dir = runs_dir / "successful-run"
    retained = runs_dir / "failed-run"
    run_dir.mkdir(parents=True)
    retained.mkdir()
    (run_dir / "run-manifest.json").write_text("{}", encoding="utf-8")
    (run_dir / "runwatch.sqlite3").write_bytes(b"state")

    cli._cleanup_successful_run(run_dir, tmp_path)

    assert not run_dir.exists()
    assert retained.is_dir()
    assert runs_dir.is_dir()


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


@pytest.mark.asyncio
async def test_announce_and_run_status_exit_codes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
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
    cli._announce_run(cast(RunSupervisor, supervisor), "https://example.test/?token=x")
    assert opened == ["https://example.test/?token=x"]
    assert qr == opened

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
        }
    )
    supervisor = RunSupervisor(
        notebook_path=notebook,
        output_path=run_dir / "executed.ipynb",
        working_dir=tmp_path,
        run_dir=run_dir,
        config=config,
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
    assert not run_dir.exists()
    assert not (tmp_path / ".runwatch").exists()


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
        assert run_lock is None
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
    assert "not empty" in rejected.output
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
    supervisor.store.update_run_status(
        supervisor.run_id, RunStatus.SUCCEEDED, ended=True
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
        assert cancelled["payload"] == {"offline": True, "kernel_epoch": 0}
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
