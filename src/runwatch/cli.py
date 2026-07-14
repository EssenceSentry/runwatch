from __future__ import annotations

import asyncio
import contextlib
import json
import os
import secrets
import shutil
import time
import webbrowser
from datetime import datetime, timezone
from pathlib import Path
from typing import Annotated, Any, Literal, cast
from uuid import uuid4

import qrcode
import typer
import uvicorn

from . import __version__
from .config import dump_default_config, load_config
from .dashboard_links import DashboardLinkManager
from .events import EventBus
from .models import (
    ActionKind,
    ActionStatus,
    RunStatus,
    RunwatchConfig,
)
from .resource_manager import ResourceManager, ResourceStopRejected
from .storage import (
    RunStore,
    controller_is_alive,
    process_is_alive,
    process_start_time,
    source_hash,
)
from .supervisor import RunSupervisor
from .tunnel import CloudflaredTunnel, discover_lan_ip, with_token
from .validation import ValidationReport, validate_execution
from .web import create_app

app = typer.Typer(
    name="runwatch",
    help="Durable, observable nbclient notebook execution with resource monitoring.",
    no_args_is_help=True,
)
resource_app = typer.Typer(
    help="Inspect and control typed resources.", no_args_is_help=True
)
app.add_typer(resource_app, name="resource")


class RunLock:
    def __init__(self, run_dir: Path, *, controller_token: str | None = None) -> None:
        self.path = run_dir / "runwatch.lock"
        self.held = False
        self.controller_token = controller_token or str(uuid4())
        self._fd: int | None = None

    def acquire(self) -> None:
        if self.held:
            raise RuntimeError(f"Runwatch lock {self.path} is already held")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        while True:
            try:
                self._fd = self._publish_lock_record()
            except FileExistsError:
                self._remove_stale_lock_or_raise()
                continue
            self.held = True
            self._fsync_parent()
            return

    def _publish_lock_record(self) -> int:
        temporary = self.path.with_name(f".{self.path.name}.{uuid4().hex}.tmp")
        fd = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o600)
        record = json.dumps(
            {
                "pid": os.getpid(),
                "started_at": process_start_time(os.getpid()),
                "controller_token": self.controller_token,
            },
            separators=(",", ":"),
        ).encode("utf-8")
        try:
            self._write_all(fd, record)
            os.fsync(fd)
            os.link(temporary, self.path)
        except BaseException:
            os.close(fd)
            raise
        finally:
            with contextlib.suppress(OSError):
                temporary.unlink(missing_ok=True)
        return fd

    @staticmethod
    def _write_all(fd: int, value: bytes) -> None:
        remaining = memoryview(value)
        while remaining:
            written = os.write(fd, remaining)
            if written <= 0:
                raise OSError("Could not write Runwatch lock record")
            remaining = remaining[written:]

    def _remove_stale_lock_or_raise(self) -> None:
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        try:
            fd = os.open(self.path, flags)
        except FileNotFoundError:
            return
        try:
            pid, started_at, _token = self._read_lock_record(fd)
            if process_is_alive(pid, started_at):
                self._raise_lock_owned(pid)
            if self._path_matches_fd(fd):
                self.path.unlink(missing_ok=True)
                self._fsync_parent()
        finally:
            os.close(fd)

    @staticmethod
    def _read_lock_record(fd: int) -> tuple[int | None, float | None, str | None]:
        try:
            os.lseek(fd, 0, os.SEEK_SET)
            raw = b""
            while chunk := os.read(fd, 4096):
                raw += chunk
                if len(raw) > 64 * 1024:
                    raise ValueError("lock record is too large")
            value = json.loads(raw.decode("utf-8"))
            pid = int(value["pid"])
            started_value = value.get("started_at")
            started_at = float(started_value) if started_value is not None else None
            token = str(value["controller_token"])
            return pid, started_at, token
        except (
            OSError,
            UnicodeError,
            TypeError,
            ValueError,
            KeyError,
            json.JSONDecodeError,
        ):
            return None, None, None

    def _raise_lock_owned(self, pid: int | None) -> None:
        owner = f"process {pid}" if pid is not None else "another process"
        raise RuntimeError(f"Runwatch {owner} already owns {self.path.parent}")

    def _path_matches_fd(self, fd: int) -> bool:
        try:
            current = self.path.stat(follow_symlinks=False)
        except FileNotFoundError:
            return False
        opened = os.fstat(fd)
        return (current.st_dev, current.st_ino) == (opened.st_dev, opened.st_ino)

    def _fsync_parent(self) -> None:
        with contextlib.suppress(OSError):
            fd = os.open(self.path.parent, os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)

    def release(self) -> None:
        if not self.held:
            return
        fd = self._fd
        try:
            if fd is not None and self._path_matches_fd(fd):
                _pid, _started_at, token = self._read_lock_record(fd)
                if token == self.controller_token:
                    self.path.unlink(missing_ok=True)
                    self._fsync_parent()
        finally:
            if fd is not None:
                os.close(fd)
            self._fd = None
            self.held = False


def _print_qr(url: str) -> None:
    qr = qrcode.QRCode(border=1)
    qr.add_data(url)
    qr.make(fit=True)
    qr.print_ascii(invert=True)


def _default_run_dir(working_dir: Path, notebook: Path) -> Path:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return (
        working_dir
        / ".runwatch"
        / "runs"
        / f"{timestamp}-{notebook.stem}-{uuid4().hex[:8]}"
    )


def _cleanup_successful_run(run_dir: Path, working_dir: Path) -> None:
    run_dir = run_dir.resolve()
    required = (run_dir / "run-manifest.json", run_dir / "runwatch.sqlite3")
    if not run_dir.exists():
        return
    if not all(path.is_file() for path in required):
        raise RuntimeError(
            f"Refusing to remove unrecognized Runwatch directory {run_dir}"
        )
    shutil.rmtree(run_dir)

    runs_root = (working_dir.resolve() / ".runwatch" / "runs").resolve()
    if run_dir.parent != runs_root:
        return
    for path in (runs_root, runs_root.parent):
        with contextlib.suppress(OSError):
            path.rmdir()


def _override_server(
    config: RunwatchConfig,
    *,
    host: str | None = None,
    port: int | None = None,
    share: Literal["none", "lan", "cloudflared"] | None = None,
    open_browser: bool | None = None,
    show_qr: bool | None = None,
) -> RunwatchConfig:
    values = config.server.model_dump()
    for key, value in (
        ("host", host),
        ("port", port),
        ("share", share),
        ("open_browser", open_browser),
        ("show_qr", show_qr),
    ):
        if value is not None:
            values[key] = value
    return config.model_copy(update={"server": config.server.model_validate(values)})


def _token(run_dir: Path) -> str:
    path = run_dir / "access-token.txt"
    try:
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError:
        value = path.read_text(encoding="utf-8").strip()
        if len(value) < 32 or any(character.isspace() for character in value):
            raise RuntimeError(
                f"Runwatch access token {path} is empty or invalid; delete it to rotate"
            ) from None
        with contextlib.suppress(OSError):
            os.chmod(path, 0o600)
        return value
    value = secrets.token_urlsafe(32)
    try:
        os.write(fd, value.encode("ascii"))
        os.fsync(fd)
    finally:
        os.close(fd)
    return value


def _require_new_run_dir(run_dir: Path) -> None:
    if run_dir.exists() and any(run_dir.iterdir()):
        raise typer.BadParameter(
            f"Run directory {run_dir} is not empty; choose a new directory"
        )


async def _wait_for_server(server: uvicorn.Server, task: asyncio.Task[Any]) -> None:
    while not server.started:
        if task.done():
            await task
            raise RuntimeError("Runwatch web server exited during startup")
        await asyncio.sleep(0.05)


async def _dashboard_base(
    config: RunwatchConfig, local_base: str
) -> tuple[str, CloudflaredTunnel | None]:
    if config.server.public_url:
        return config.server.public_url.rstrip("/"), None
    if config.server.share == "cloudflared":
        tunnel = CloudflaredTunnel(config.server.cloudflared_binary)
        return await tunnel.start(local_base), tunnel
    if config.server.share == "lan":
        return f"http://{discover_lan_ip()}:{config.server.port}", None
    return local_base, None


def _announce_run(supervisor: RunSupervisor, pairing_url: str) -> None:
    typer.echo(f"\nRunwatch run directory: {supervisor.run_dir}")
    typer.echo(f"Editable notebook: {supervisor.source_path}")
    typer.echo(f"Dashboard: {pairing_url}\n")
    if supervisor.config.server.show_qr:
        _print_qr(pairing_url)
    if supervisor.config.server.open_browser:
        webbrowser.open(pairing_url)


async def _run_and_linger(supervisor: RunSupervisor, *, start_run: bool) -> int:
    if not start_run:
        await asyncio.Event().wait()
        return 0
    await supervisor.start()
    status = await supervisor.wait()
    typer.echo(f"\nRun finished with status: {status.value}")
    typer.echo(f"Executed notebook: {supervisor.output_path}")
    if status is RunStatus.SUCCEEDED:
        typer.echo(f"Updated notebook: {supervisor.notebook_path}")
    linger = supervisor.config.server.linger_seconds
    if linger is None:
        typer.echo("Dashboard remains available; press Ctrl+C to close it.")
        await asyncio.Event().wait()
    elif linger > 0:
        typer.echo(
            f"Dashboard remains available for {linger:g} seconds before closing."
        )
        await asyncio.sleep(linger)
    return 0 if status is RunStatus.SUCCEEDED else 1


async def _serve(
    supervisor: RunSupervisor,
    *,
    start_run: bool,
    run_lock: RunLock | None = None,
) -> int:
    config = supervisor.config
    run_dir = supervisor.run_dir
    lock = _serve_lock(supervisor, run_lock)
    server: uvicorn.Server | None = None
    server_task: asyncio.Task[Any] | None = None
    tunnel: CloudflaredTunnel | None = None
    try:
        token = _token(run_dir)
        supervisor.attach_dashboard_links(
            DashboardLinkManager(
                access_token=token,
                share=config.server.share,
                cloudflared_binary=config.server.cloudflared_binary,
                bus=supervisor.bus,
            )
        )
        host = config.server.host
        if config.server.share == "lan" and host in {"127.0.0.1", "localhost"}:
            host = "0.0.0.0"
        local_host = "127.0.0.1" if host == "0.0.0.0" else host
        local_base = f"http://{local_host}:{config.server.port}"
        server = uvicorn.Server(
            uvicorn.Config(
                create_app(supervisor, token),
                host=host,
                port=config.server.port,
                log_level="warning",
                access_log=False,
                lifespan="off",
            )
        )
        server.install_signal_handlers = lambda: None  # type: ignore[method-assign]
        server_task = asyncio.create_task(server.serve(), name="runwatch-web-server")
        await _wait_for_server(server, server_task)
        public_base, tunnel = await _dashboard_base(config, local_base)
        pairing_url = with_token(public_base, token)
        _announce_run(supervisor, pairing_url)
        return await _run_and_linger(supervisor, start_run=start_run)
    finally:
        if tunnel:
            with contextlib.suppress(Exception):
                await tunnel.close()
        if server is not None:
            server.should_exit = True
        if server_task is not None:
            await asyncio.gather(server_task, return_exceptions=True)
        cleanup_successful_run = False
        if start_run and getattr(supervisor, "cleanup_on_success", True):
            with contextlib.suppress(Exception):
                run = supervisor.store.get_run(supervisor.run_id)
                cleanup_successful_run = RunStatus(run["status"]) is RunStatus.SUCCEEDED
        try:
            await supervisor.close()
        finally:
            lock.release()
        if cleanup_successful_run:
            successful_run_dir = supervisor.run_dir
            _cleanup_successful_run(successful_run_dir, supervisor.working_dir)
            typer.echo(f"Removed successful Runwatch state: {successful_run_dir}")


def _serve_lock(supervisor: RunSupervisor, run_lock: RunLock | None) -> RunLock:
    if run_lock is None:
        created = RunLock(
            supervisor.run_dir,
            controller_token=getattr(supervisor, "controller_token", None),
        )
        created.acquire()
        return created
    if not run_lock.held or run_lock.path.parent.resolve() != supervisor.run_dir:
        raise RuntimeError("Stopped-process recovery did not retain the run lock")
    if run_lock.controller_token != supervisor.controller_token:
        raise RuntimeError("Run lock and supervisor controller identities differ")
    return run_lock


def _run_store(run_dir: Path) -> tuple[dict[str, Any], RunwatchConfig, RunStore]:
    manifest = RunSupervisor.read_manifest(run_dir)
    config = RunwatchConfig.model_validate(manifest["config"])
    store = RunStore(
        run_dir.resolve() / "runwatch.sqlite3",
        max_observations_per_resource=config.storage.max_observations_per_resource,
        max_log_lines_per_resource=config.storage.max_log_lines_per_resource,
        max_events_per_run=config.storage.max_events_per_run,
    )
    return manifest, config, store


def _wait_action(
    store: RunStore, action_id: str, timeout_seconds: float
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        action = store.get_action(action_id)
        if action and ActionStatus(action["status"]).terminal:
            return action
        time.sleep(0.25)
    raise TimeoutError(f"Timed out waiting for action {action_id}")


def _queue_live_recovery(run_dir: Path, kind: ActionKind, from_cell: int) -> bool:
    manifest, _config, store = _run_store(run_dir)
    try:
        run = store.get_run(manifest["run_id"])
        if not controller_is_alive(run):
            return False
        action_id = store.create_action(
            manifest["run_id"],
            kind,
            payload={
                "from_cell": from_cell,
                "failed_cell_index": run.get("failed_cell_index"),
            },
            expected_kernel_epoch=int(run["kernel_epoch"]),
            expected_cell_attempt=run.get("failed_attempt"),
            expected_source_hash=source_hash(run_dir.resolve() / "source.ipynb"),
        )
        action = _wait_action(store, action_id, 60)
        if action["status"] != ActionStatus.COMPLETED.value:
            raise RuntimeError(action.get("message") or f"Action {action['status']}")
        typer.echo(json.dumps(action, indent=2))
        return True
    finally:
        store.close()


def _offline_recovery_action(
    run_dir: Path,
    kind: ActionKind,
    from_cell: int,
    *,
    run_lock: RunLock,
) -> str:
    if not run_lock.held or run_lock.path.parent.resolve() != run_dir.resolve():
        raise RuntimeError("Stopped-process recovery requires the held run lock")
    manifest, _config, store = _run_store(run_dir)
    try:
        run_id = str(manifest["run_id"])
        run = store.get_run(run_id)
        if controller_is_alive(run):
            raise RuntimeError(
                "A Runwatch controller became active while preparing "
                "stopped-process recovery; retry the command"
            )
        store.recover_incomplete_actions(run_id)
        digest = source_hash(run_dir.resolve() / "source.ipynb")
        if kind is ActionKind.RESUME and RunStatus(run["status"]).terminal:
            raise RuntimeError(
                "A terminal run cannot be resumed; use restart to rerun it"
            )
        failed_cell_index = run.get("failed_cell_index")
        failed_attempt = run.get("failed_attempt")
        for action in store.list_actions(run_id, limit=10_000):
            if action["status"] != ActionStatus.REQUESTED.value or action[
                "kind"
            ] not in {ActionKind.RESUME.value, ActionKind.RESTART.value}:
                continue
            matches = (
                action["kind"] == kind.value
                and int(action["payload"].get("from_cell", 0)) == from_cell
                and action.get("expected_source_hash") == digest
                and action.get("expected_kernel_epoch") == run["kernel_epoch"]
                and action.get("expected_cell_attempt") == failed_attempt
                and action["payload"].get("failed_cell_index") == failed_cell_index
            )
            if matches:
                return str(action["action_id"])
            store.finish_action(
                action["action_id"],
                ActionStatus.REJECTED,
                message="Superseded by a newer stopped-process recovery request",
            )
        return store.create_action(
            run_id,
            kind,
            payload={
                "from_cell": from_cell,
                "failed_cell_index": failed_cell_index,
                "offline_recovery": True,
            },
            expected_kernel_epoch=int(run["kernel_epoch"]),
            expected_cell_attempt=run.get("failed_attempt"),
            expected_source_hash=digest,
        )
    finally:
        store.close()


def _serve_stopped_recovery(run_dir: Path, kind: ActionKind, from_cell: int) -> int:
    lock = RunLock(run_dir)
    lock.acquire()
    try:
        action_id = _offline_recovery_action(run_dir, kind, from_cell, run_lock=lock)
        supervisor = RunSupervisor.reopen(
            run_dir,
            from_cell=from_cell,
            bootstrap_action_id=action_id,
        )
        supervisor.controller_token = lock.controller_token
        return asyncio.run(_serve(supervisor, start_run=True, run_lock=lock))
    finally:
        lock.release()


@app.command()
def execute(
    notebook: Annotated[
        Path, typer.Argument(exists=True, dir_okay=False, readable=True)
    ],
    config_path: Annotated[
        Path | None,
        typer.Option("--config", "-c", exists=True, dir_okay=False, readable=True),
    ] = None,
    output: Annotated[
        Path | None, typer.Option("--output", "-o", dir_okay=False)
    ] = None,
    run_dir: Annotated[Path | None, typer.Option("--run-dir", file_okay=False)] = None,
    working_dir: Annotated[
        Path | None, typer.Option("--working-dir", "-w", file_okay=False)
    ] = None,
    name: Annotated[str | None, typer.Option("--name")] = None,
    host: Annotated[str | None, typer.Option("--host")] = None,
    port: Annotated[int | None, typer.Option("--port", min=1, max=65535)] = None,
    share: Annotated[
        Literal["none", "lan", "cloudflared"] | None, typer.Option("--share")
    ] = None,
    browser: Annotated[bool | None, typer.Option("--browser/--no-browser")] = None,
    qr: Annotated[bool | None, typer.Option("--qr/--no-qr")] = None,
    keep_run: Annotated[
        bool,
        typer.Option(
            "--keep-run",
            help="Retain successful Runwatch state after the dashboard closes.",
        ),
    ] = False,
) -> None:
    """Execute NOTEBOOK and expose its durable Runwatch dashboard."""
    loaded = _override_server(
        load_config(config_path),
        host=host,
        port=port,
        share=share,
        open_browser=browser,
        show_qr=qr,
    )
    notebook = notebook.resolve()
    working = (working_dir or notebook.parent).resolve()
    target_run_dir = (run_dir or _default_run_dir(working, notebook)).resolve()
    _require_new_run_dir(target_run_dir)
    supervisor = RunSupervisor(
        notebook_path=notebook,
        output_path=(output or target_run_dir / "executed.ipynb").resolve(),
        working_dir=working,
        run_dir=target_run_dir,
        config=loaded,
        name=name,
        cleanup_on_success=not keep_run,
    )
    try:
        raise typer.Exit(asyncio.run(_serve(supervisor, start_run=True)))
    except KeyboardInterrupt:
        if target_run_dir.exists():
            typer.echo(
                "\nRunwatch process stopped. Resume with: runwatch resume "
                + str(target_run_dir)
            )
        else:
            typer.echo("\nRunwatch dashboard closed after successful cleanup.")
        raise typer.Exit(130) from None


@app.command()
def validate(
    notebook: Annotated[
        Path, typer.Argument(exists=True, dir_okay=False, readable=True)
    ],
    config_path: Annotated[
        Path | None,
        typer.Option("--config", "-c", exists=True, dir_okay=False, readable=True),
    ] = None,
    working_dir: Annotated[
        Path | None, typer.Option("--working-dir", "-w", file_okay=False)
    ] = None,
    json_output: Annotated[bool, typer.Option("--json")] = False,
) -> None:
    """Preflight NOTEBOOK and configuration without executing or starting a server."""
    notebook = notebook.resolve()
    working = (working_dir or notebook.parent).resolve()
    report: ValidationReport
    try:
        loaded = load_config(config_path)
        report = validate_execution(notebook, loaded, working_dir=working)
    except Exception as error:
        report = {
            "valid": False,
            "notebook": str(notebook),
            "working_dir": str(working),
            "kernel_name": "unknown",
            "cell_count": 0,
            "code_cell_count": 0,
            "configured_resources": [],
            "errors": [f"Configuration is invalid: {error}"],
            "warnings": [],
        }
    if json_output:
        typer.echo(json.dumps(report, indent=2, default=str))
    else:
        typer.echo("Runwatch preflight: " + ("valid" if report["valid"] else "invalid"))
        if report.get("kernel_name"):
            typer.echo(f"Kernel: {report['kernel_name']}")
        if "cell_count" in report:
            typer.echo(
                f"Notebook: {report['cell_count']} cells "
                f"({report['code_cell_count']} code)"
            )
        typer.echo(
            f"Configured resources: {len(report.get('configured_resources', []))}"
        )
        for warning in report.get("warnings", []):
            typer.echo(f"Warning: {warning}")
        for error in report.get("errors", []):
            typer.echo(f"Error: {error}", err=True)
    if not report["valid"]:
        raise typer.Exit(1)


@app.command()
def resume(
    run_dir: Annotated[Path, typer.Argument(exists=True, file_okay=False)],
) -> None:
    """Resume a paused live run or reconstruct a stopped run from cell zero."""
    if _queue_live_recovery(run_dir, ActionKind.RESUME, 0):
        return
    try:
        raise typer.Exit(_serve_stopped_recovery(run_dir, ActionKind.RESUME, 0))
    except KeyboardInterrupt:
        raise typer.Exit(130) from None


@app.command()
def restart(
    run_dir: Annotated[Path, typer.Argument(exists=True, file_okay=False)],
    from_cell: Annotated[int, typer.Option("--from-cell", min=0)] = 0,
) -> None:
    """Create a new kernel epoch and replay from FROM_CELL (zero-based)."""
    if _queue_live_recovery(run_dir, ActionKind.RESTART, from_cell):
        return
    try:
        raise typer.Exit(
            _serve_stopped_recovery(run_dir, ActionKind.RESTART, from_cell)
        )
    except KeyboardInterrupt:
        raise typer.Exit(130) from None


@app.command()
def status(
    run_dir: Annotated[Path, typer.Argument(exists=True, file_okay=False)],
    json_output: Annotated[bool, typer.Option("--json")] = False,
) -> None:
    """Show persisted run status without starting Runwatch."""
    manifest, config, store = _run_store(run_dir)
    try:
        snapshot = store.snapshot(
            manifest["run_id"], chart_points=config.storage.dashboard_chart_points
        )
        if json_output:
            typer.echo(json.dumps(snapshot, indent=2, default=str))
        else:
            run = snapshot["run"]
            typer.echo(f"{run['name']}: {run['status']} — {run.get('message') or ''}")
            typer.echo(f"Source: {run['source_path']}")
            typer.echo(f"Resources: {len(snapshot['resources'])}")
    finally:
        store.close()


@app.command()
def context(
    run_dir: Annotated[Path, typer.Argument(exists=True, file_okay=False)],
    json_output: Annotated[bool, typer.Option("--json")] = False,
    output_format: Annotated[
        Literal["markdown", "json"], typer.Option("--format")
    ] = "markdown",
) -> None:
    """Print a bounded agent-oriented run dossier."""
    manifest, config, store = _run_store(run_dir)
    try:
        snapshot = store.snapshot(
            manifest["run_id"], chart_points=config.storage.dashboard_chart_points
        )
        run = snapshot["run"]
        failed = next(
            (
                cell
                for cell in snapshot["cells"]
                if cell["cell_index"] == run.get("failed_cell_index")
            ),
            None,
        )
        dossier = {
            "run": run,
            "failed_cell": failed,
            "resources": snapshot["resources"],
            "recent_events": snapshot["events"][-40:],
            "source_path": str(run_dir.resolve() / "source.ipynb"),
            "suggested_commands": {
                "resume": f"runwatch resume {run_dir.resolve()}",
                "restart": f"runwatch restart {run_dir.resolve()}",
            },
        }
        if json_output or output_format == "json":
            typer.echo(json.dumps(dossier, indent=2, default=str))
            return
        typer.echo(f"# Runwatch context: {run['name']}\n")
        typer.echo(f"- Status: `{run['status']}`")
        typer.echo(f"- Message: {run.get('message') or '—'}")
        typer.echo(f"- Kernel epoch: {run['kernel_epoch']}")
        typer.echo(f"- Editable notebook: `{dossier['source_path']}`")
        if failed:
            typer.echo(f"\n## Failed cell {failed['cell_index'] + 1}\n")
            typer.echo(f"- Attempt: {failed['attempt']}")
            typer.echo(
                f"- Error: `{failed.get('error_name')}: {failed.get('error_value')}`"
            )
            typer.echo("\n```python\n" + failed["source"] + "\n```")
        resources = dossier.get("resources")
        resource_count = (
            len(cast(list[Any], resources)) if isinstance(resources, list) else 0
        )
        typer.echo(f"\n## Resources\n\n{resource_count} tracked resource(s).")
    finally:
        store.close()


@app.command()
def events(
    run_dir: Annotated[Path, typer.Argument(exists=True, file_okay=False)],
    follow: Annotated[bool, typer.Option("--follow")] = False,
    json_output: Annotated[bool, typer.Option("--json")] = False,
) -> None:
    """Print persisted events and optionally follow new events."""
    manifest, _config, store = _run_store(run_dir)
    try:
        seen = 0
        while True:
            values = store.recent_events(manifest["run_id"], limit=500)
            for event in values:
                if event["seq"] <= seen:
                    continue
                seen = event["seq"]
                typer.echo(
                    json.dumps(event, default=str)
                    if json_output
                    else f"{event['timestamp']} {event['type']} {event['payload']}"
                )
            if not follow:
                return
            time.sleep(0.5)
    finally:
        store.close()


@resource_app.command("stop")
def resource_stop(
    run_dir: Annotated[Path, typer.Argument(exists=True, file_okay=False)],
    resource_id: Annotated[str, typer.Argument()],
) -> None:
    """Stop an exclusive stoppable resource and cancel the run."""
    manifest, config, store = _run_store(run_dir)
    try:
        run = store.get_run(manifest["run_id"])
        resource = store.get_resource(resource_id)
        if resource is None:
            raise typer.BadParameter(f"Unknown resource {resource_id}")
        if controller_is_alive(run):
            action_id = store.create_action(
                manifest["run_id"],
                ActionKind.STOP_RESOURCE,
                payload={
                    "internal_id": resource_id,
                    "expected_version": resource["version"],
                },
                expected_kernel_epoch=run["kernel_epoch"],
            )
            action = _wait_action(
                store, action_id, config.aws.stop_timeout_seconds + 30
            )
            typer.echo(json.dumps(action, indent=2))
            if action["status"] != ActionStatus.COMPLETED.value:
                raise typer.Exit(1)
            return
    finally:
        store.close()
    lock = RunLock(run_dir)
    lock.acquire()
    try:
        action_id = _offline_stop_action(run_dir, resource_id)
        asyncio.run(_stop_resources_offline(run_dir, action_id))
    finally:
        lock.release()


def _offline_stop_action(run_dir: Path, resource_id: str) -> str:
    manifest, _config, store = _run_store(run_dir)
    try:
        run_id = str(manifest["run_id"])
        run = store.get_run(run_id)
        if controller_is_alive(run):
            raise RuntimeError(
                "A Runwatch controller became active while preparing the offline stop; "
                "retry the command"
            )
        resource = store.get_resource(resource_id)
        if resource is None:
            raise typer.BadParameter(f"Unknown resource {resource_id}")
        store.recover_incomplete_actions(run_id)
        existing = _matching_offline_stop_action(store, run_id, resource_id)
        if existing is not None:
            return existing
        if RunStatus(run["status"]).terminal:
            raise ResourceStopRejected("The run is already terminal")
        return store.create_action(
            run_id,
            ActionKind.STOP_RESOURCE,
            payload={
                "internal_id": resource_id,
                "expected_version": resource["version"],
                "offline": True,
            },
            expected_kernel_epoch=run["kernel_epoch"],
        )
    finally:
        store.close()


def _matching_offline_stop_action(
    store: RunStore, run_id: str, resource_id: str
) -> str | None:
    return next(
        (
            str(action["action_id"])
            for action in store.list_actions(run_id, limit=10_000)
            if action["status"] == ActionStatus.REQUESTED.value
            and action["kind"] == ActionKind.STOP_RESOURCE.value
            and action["payload"].get("internal_id") == resource_id
        ),
        None,
    )


async def _stop_resources_offline(run_dir: Path, action_id: str) -> None:
    manifest, config, store = _run_store(run_dir)
    bus = EventBus(store, manifest["run_id"])
    manager = ResourceManager(
        store=store,
        bus=bus,
        run_id=manifest["run_id"],
        working_dir=Path(manifest["working_dir"]),
        aws_settings=config.aws,
    )
    try:
        action = _claim_offline_stop_action(store, manifest["run_id"], action_id)
        if action is None:
            return
        resource_id = str(action["payload"]["internal_id"])
        await _execute_offline_stop(
            store=store,
            bus=bus,
            manager=manager,
            run_id=manifest["run_id"],
            action=action,
            resource_id=resource_id,
        )
    except ResourceStopRejected as error:
        _finish_offline_stop_error(store, action_id, ActionStatus.REJECTED, error)
        raise
    except Exception as error:
        _finish_offline_stop_error(store, action_id, ActionStatus.FAILED, error)
        raise
    finally:
        await manager.shutdown()
        store.close()


def _claim_offline_stop_action(
    store: RunStore, run_id: str, action_id: str
) -> dict[str, Any] | None:
    store.recover_incomplete_actions(run_id)
    action = store.claim_action(action_id)
    if action is None:
        existing = store.get_action(action_id)
        if existing and existing["status"] == ActionStatus.COMPLETED.value:
            return None
        raise RuntimeError(f"Offline stop action {action_id} is not requestable")
    run = store.get_run(run_id)
    if action.get("expected_kernel_epoch") == run["kernel_epoch"]:
        return action
    message = "Kernel epoch changed after the offline stop was requested"
    store.finish_action(action_id, ActionStatus.REJECTED, message=message)
    raise ResourceStopRejected(message)


async def _execute_offline_stop(
    *,
    store: RunStore,
    bus: EventBus,
    manager: ResourceManager,
    run_id: str,
    action: dict[str, Any],
    resource_id: str,
) -> None:
    recovered_stop = _recover_confirmed_offline_stop(store, action, resource_id)
    if _finish_terminal_offline_stop(
        store, action["action_id"], resource_id, recovered_stop, run_id
    ):
        return
    await manager.restore_monitors()

    async def cancel_run() -> None:
        await _cancel_offline_run(store, bus, run_id)

    if not recovered_stop:
        await manager.stop_resource(
            resource_id,
            expected_version=(
                None
                if action["payload"].get("recovered")
                else int(action["payload"]["expected_version"])
            ),
            on_stop_accepted=cancel_run,
            allow_stopping=bool(action["payload"].get("recovered", False)),
        )
    await cancel_run()
    if _finish_terminal_offline_stop(
        store, action["action_id"], resource_id, True, run_id
    ):
        return
    await manager.stop_cancel_resources()
    store.update_run_status(
        run_id, RunStatus.CANCELLED, message="Run cancelled", ended=True
    )
    kernel_epoch = int(store.get_run(run_id)["kernel_epoch"])
    await bus.publish(
        "run.cancelled",
        {"offline": True, "kernel_epoch": kernel_epoch},
    )
    _finish_offline_stop_action(store, action["action_id"], resource_id)


async def _cancel_offline_run(store: RunStore, bus: EventBus, run_id: str) -> bool:
    current = RunStatus(store.get_run(run_id)["status"])
    if current.terminal:
        return False
    if current is RunStatus.CANCELLING:
        return True
    store.update_run_status(
        run_id,
        RunStatus.CANCELLING,
        message="Offline resource stop accepted; cancelling run",
    )
    await bus.publish("run.cancel_requested", {"offline": True})
    return True


def _finish_terminal_offline_stop(
    store: RunStore,
    action_id: str,
    resource_id: str,
    stop_was_confirmed: bool,
    run_id: str,
) -> bool:
    terminal_status = _run_terminal_status(store, run_id)
    if terminal_status is None:
        return False
    if not stop_was_confirmed:
        raise ResourceStopRejected("The run is already terminal")
    _finish_offline_stop_action(
        store,
        action_id,
        resource_id,
        terminal_status=terminal_status,
    )
    return True


def _finish_offline_stop_error(
    store: RunStore,
    action_id: str,
    status: ActionStatus,
    error: Exception,
) -> None:
    current = store.get_action(action_id)
    if current and not ActionStatus(current["status"]).terminal:
        store.finish_action(action_id, status, message=str(error))


def _recover_confirmed_offline_stop(
    store: RunStore, action: dict[str, Any], resource_id: str
) -> bool:
    if not action["payload"].get("recovered"):
        return False
    resource = store.get_resource(resource_id)
    return bool(
        resource and resource["terminal"] and resource["disposition"] == "cancelled"
    )


def _run_terminal_status(store: RunStore, run_id: str) -> RunStatus | None:
    status = RunStatus(store.get_run(run_id)["status"])
    return status if status.terminal else None


def _finish_offline_stop_action(
    store: RunStore,
    action_id: str,
    resource_id: str,
    *,
    terminal_status: RunStatus | None = None,
    cancellation_requested: bool = True,
) -> None:
    result: dict[str, Any] = {
        "stopped_resource_ids": [resource_id],
        "cancellation_requested": cancellation_requested and terminal_status is None,
    }
    if terminal_status is None:
        message = "Resource stopped and offline run cancellation completed"
    else:
        result["final_run_status"] = terminal_status.value
        message = (
            "Resource stop completed after the run reached "
            f"{terminal_status.value}; terminal run state preserved"
        )
    store.finish_action(
        action_id,
        ActionStatus.COMPLETED,
        message=message,
        result=result,
    )


@app.command()
def open(
    run_dir: Annotated[Path, typer.Argument(exists=True, file_okay=False)],
) -> None:
    """Open a persisted run as a read-only dashboard."""
    supervisor = RunSupervisor.reopen(run_dir)
    try:
        asyncio.run(_serve(supervisor, start_run=False))
    except KeyboardInterrupt:
        raise typer.Exit(130) from None


@app.command("init-config")
def init_config(
    path: Annotated[Path, typer.Argument(dir_okay=False)] = Path("runwatch.yaml"),
    force: Annotated[bool, typer.Option("--force")] = False,
) -> None:
    if path.exists() and not force:
        raise typer.BadParameter(f"{path} already exists; pass --force to replace it")
    dump_default_config(path)
    typer.echo(f"Wrote {path}")


@app.command()
def version() -> None:
    typer.echo(__version__)
