from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import os
import re
import time
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any, cast
from uuid import uuid4

import nbformat
from nbclient import NotebookClient
from nbclient.exceptions import CellExecutionError, CellTimeoutError, DeadKernelError
from nbformat import NotebookNode

from ._tqdm import tqdm_bootstrap_code
from .emit import EVENT_MIME_TYPE, FALLBACK_PREFIX, RESOURCE_MIME_TYPE
from .events import EventBus
from .models import (
    ActionStatus,
    CellStatus,
    NotebookSettings,
    ProgressEvent,
    ResourceEvent,
    RunnerCommand,
    RunStatus,
)
from .resource_manager import ResourceManager
from .storage import RunStore, source_hash

_ANSI_RE = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
_OUTPUT_PERSIST_INTERVAL_SECONDS = 0.25
_TIMEOUT_RECOVERY_MAX_SECONDS = 10.0
_WRITEBACK_STATE_FILENAME = "writeback-state.json"


def _strip_ansi(value: str) -> str:
    return _ANSI_RE.sub("", value)


def _source_digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _cell_label(notebook: NotebookNode, index: int) -> str:
    cell = notebook.cells[index]
    configured = cell.metadata.get("runwatch", {}).get("label")
    if configured:
        return str(configured)
    for previous in reversed(notebook.cells[:index]):
        if previous.cell_type != "markdown":
            continue
        for line in str(previous.source).splitlines():
            stripped = line.strip()
            if stripped.startswith("#"):
                return stripped.lstrip("#").strip()[:120]
    first = next(
        (line.strip() for line in str(cell.source).splitlines() if line.strip()), ""
    )
    return first[:120] or f"{cell.cell_type.title()} cell {index + 1}"


def ensure_cell_ids(notebook: NotebookNode) -> None:
    for cell in notebook.cells:
        if not cell.get("id"):
            cell["id"] = uuid4().hex[:12]


def clear_notebook_outputs(notebook: NotebookNode) -> None:
    for cell in notebook.cells:
        if cell.cell_type == "code":
            cell.outputs = []
            cell.execution_count = None


def write_notebook_atomic(notebook: NotebookNode, path: Path) -> None:
    """Write a notebook through an fsynced temporary file and atomic replace."""

    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        nbformat.write(notebook, handle)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def _cell_records(notebook: NotebookNode) -> list[dict[str, Any]]:
    return [
        {
            "cell_index": index,
            "cell_id": cell.id,
            "cell_type": cell.cell_type,
            "label": _cell_label(notebook, index),
            "source": str(cell.source),
            "source_hash": _source_digest(str(cell.source)),
        }
        for index, cell in enumerate(notebook.cells)
    ]


def summarize_output(output: NotebookNode, *, max_chars: int = 6_000) -> dict[str, Any]:
    output_type = str(output.get("output_type", "unknown"))
    if output_type == "stream":
        return {
            "output_type": output_type,
            "name": output.get("name"),
            "text": str(output.get("text", ""))[-max_chars:],
        }
    if output_type == "error":
        return {
            "output_type": output_type,
            "ename": output.get("ename"),
            "evalue": output.get("evalue"),
            "traceback": [
                _strip_ansi(str(line)) for line in output.get("traceback", [])[-30:]
            ],
        }
    data = cast(dict[str, Any], output.get("data", {}))
    text = data.get("text/plain")
    return {
        "output_type": output_type,
        "mime_types": sorted(data),
        "text": str(text)[-max_chars:] if text is not None else None,
    }


class MonitoredNotebookClient(NotebookClient):
    def __init__(
        self,
        *args: Any,
        output_callback: Callable[[NotebookNode, int, int], None],
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.output_callback = output_callback
        self.current_attempt = 0
        self._output_poll_task: asyncio.Task[None] | None = None

    async def _async_poll_output_msg(
        self, parent_msg_id: str, cell: NotebookNode, cell_index: int
    ) -> None:
        task = asyncio.current_task()
        assert task is not None
        self._output_poll_task = task
        try:
            await super()._async_poll_output_msg(parent_msg_id, cell, cell_index)
        finally:
            if self._output_poll_task is task:
                self._output_poll_task = None

    async def synchronize_after_interrupt(self, timeout_seconds: float) -> None:
        """Wait until an interrupted execution has fully released the kernel."""
        if self.kc is None:
            raise RuntimeError("Kernel client is unavailable after the timeout")

        output_poll_task = self._output_poll_task
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout_seconds
        try:
            kernel_info_result = self.kc.kernel_info()
            kernel_info_id = (
                await kernel_info_result
                if inspect.isawaitable(kernel_info_result)
                else kernel_info_result
            )
            while True:
                remaining = deadline - loop.time()
                if remaining <= 0:
                    raise TimeoutError("Kernel did not answer after being interrupted")
                message = await asyncio.wait_for(
                    self.kc.shell_channel.get_msg(timeout=None), timeout=remaining
                )
                if message["parent_header"].get("msg_id") == kernel_info_id:
                    break

            if output_poll_task is not None and not output_poll_task.done():
                remaining = deadline - loop.time()
                if remaining <= 0:
                    raise TimeoutError(
                        "Kernel output did not become idle after being interrupted"
                    )
                await asyncio.wait_for(output_poll_task, timeout=remaining)
        except BaseException:
            if output_poll_task is not None and not output_poll_task.done():
                output_poll_task.cancel()
                await asyncio.gather(output_poll_task, return_exceptions=True)
            raise

    def process_message(
        self, msg: dict[str, Any], cell: NotebookNode, cell_index: int
    ) -> NotebookNode | None:
        output = super().process_message(msg, cell, cell_index)
        if output is not None:
            self.output_callback(output, cell_index, self.current_attempt)
        elif msg.get("msg_type") == "update_display_data":
            content = cast(dict[str, Any], msg.get("content", {}))
            data = content.get("data")
            data_payload = cast(dict[str, Any], data) if isinstance(data, dict) else {}
            if data_payload and any(
                isinstance(data_payload.get(mime_type), dict)
                for mime_type in (RESOURCE_MIME_TYPE, EVENT_MIME_TYPE)
            ):
                metadata = content.get("metadata", {})
                metadata_payload = (
                    cast(dict[str, Any], metadata) if isinstance(metadata, dict) else {}
                )
                updated_output = nbformat.v4.new_output(
                    output_type="display_data",
                    data=data_payload,
                    metadata=metadata_payload,
                )
                self.output_callback(
                    updated_output,
                    cell_index,
                    self.current_attempt,
                )
        return output


class NotebookRunner:
    def __init__(
        self,
        *,
        run_id: str,
        notebook_path: Path,
        source_path: Path,
        output_path: Path,
        run_dir: Path,
        working_dir: Path,
        settings: NotebookSettings,
        store: RunStore,
        bus: EventBus,
        resources: ResourceManager,
        initialize_cells: bool,
        initial_from_cell: int = 0,
    ) -> None:
        self.run_id = run_id
        self.notebook_path = notebook_path
        self.source_path = source_path
        self.output_path = output_path
        self.run_dir = run_dir
        self.working_dir = working_dir
        self.settings = settings
        self.store = store
        self.bus = bus
        self.resources = resources
        self.notebook = self._read_source()
        self._validate_start_index(self.notebook, initial_from_cell)
        self.client: MonitoredNotebookClient | None = None
        self.current_cell_index: int | None = None
        self.current_attempt: int | None = None
        self.failed_cell_index: int | None = None
        self.failed_attempt: int | None = None
        self.kernel_epoch = 0
        self.command_queue: asyncio.Queue[RunnerCommand] = asyncio.Queue()
        self._pending_output_tasks: set[asyncio.Task[Any]] = set()
        self._pending_output_errors: list[Exception] = []
        self._pending_cell_outputs: dict[int, dict[str, Any]] = {}
        self._pending_cell_output_counts: dict[int, int] = {}
        self._output_flush_tasks: dict[int, asyncio.Task[None]] = {}
        self._checkpoint_event = asyncio.Event()
        self._checkpoint_task: asyncio.Task[None] | None = None
        self._checkpoint_lock = asyncio.Lock()
        self._finished = asyncio.Event()
        self._paused = asyncio.Event()
        self._cancel_requested = asyncio.Event()
        self._running_cell = False
        self._kernel_dead = False
        self._initial_from_cell = initial_from_cell
        self._resume_existing = not initialize_cells
        self.partial_output_path = run_dir / "executed.partial.ipynb"
        self.writeback_state_path = run_dir / _WRITEBACK_STATE_FILENAME
        self._expected_notebook_hash = self._load_writeback_hash()
        if initialize_cells:
            self.store.initialize_cells(run_id, _cell_records(self.notebook))

    @property
    def paused(self) -> bool:
        return self._paused.is_set()

    async def wait_until_paused(self) -> None:
        await self._paused.wait()

    async def wait_finished(self) -> None:
        await self._finished.wait()

    async def enqueue(self, command: RunnerCommand) -> None:
        await self.command_queue.put(command)

    async def interrupt(self) -> bool:
        client = self.client
        if client is None or client.km is None or not self._running_cell:
            return False
        kernel_manager = cast(Any, client.km)
        result = kernel_manager.interrupt_kernel()
        if inspect.isawaitable(result):
            await result
        await self.bus.publish(
            "notebook.interrupt_requested",
            {"cell_index": self.current_cell_index, "attempt": self.current_attempt},
        )
        return True

    async def cancel(self) -> None:
        if self._cancel_requested.is_set():
            return
        run_status = RunStatus(self.store.get_run(self.run_id)["status"])
        if run_status.terminal:
            return
        self._cancel_requested.set()
        self.store.update_run_status(
            self.run_id, RunStatus.CANCELLING, message="Cancellation requested"
        )
        await self.bus.publish("run.cancel_requested", {})
        if self.paused:
            await self.enqueue(
                RunnerCommand(
                    action_id="internal-cancel",
                    kind="cancel",
                    expected_kernel_epoch=self.kernel_epoch,
                )
            )
        else:
            await self.interrupt()

    async def finalize_cancelled_without_execution(self) -> RunStatus:
        """Finish crash-recovered cancellation without creating a new kernel."""

        self._cancel_requested.set()
        try:
            return await self._finish_cancelled()
        finally:
            self._finished.set()

    async def run(self) -> RunStatus:
        self.store.update_run_status(
            self.run_id,
            RunStatus.STARTING,
            message="Starting notebook kernel",
            started=True,
        )
        await self.bus.publish("run.started", {"working_dir": str(self.working_dir)})
        self._checkpoint_task = asyncio.create_task(
            self._checkpoint_loop(), name=f"checkpoint:{self.run_id}"
        )
        try:
            return await self._run_sessions()
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self.store.update_run_status(
                self.run_id,
                RunStatus.FAILED,
                message=f"Runner failure: {type(error).__name__}: {error}",
                ended=True,
            )
            await self.bus.publish(
                "run.runner_error",
                {
                    "kernel_epoch": self.kernel_epoch,
                    "error_type": type(error).__name__,
                    "error": str(error),
                },
            )
            return RunStatus.FAILED
        finally:
            await self._drain_output_tasks()
            await self._checkpoint(force=True)
            if self._checkpoint_task:
                self._checkpoint_task.cancel()
                await asyncio.gather(self._checkpoint_task, return_exceptions=True)
            self._finished.set()

    async def _run_sessions(self) -> RunStatus:
        await self._run_kernel_epochs()
        if self._cancel_requested.is_set():
            return await self._finish_cancelled()
        self._write_notebook_atomic(self.notebook, self.output_path)
        blocking_status = await self._wait_for_blocking_resources()
        if blocking_status is not None:
            return blocking_status
        await self._checkpoint(force=True, writeback=True)
        return await self._finish_succeeded()

    async def _run_kernel_epochs(self) -> None:
        restart = True
        start_index = self._initial_from_cell
        first_session = True
        while restart and not self._cancel_requested.is_set():
            self._prepare_kernel_epoch(
                start_index,
                replace_cells=self._resume_existing
                or not first_session
                or start_index > 0,
            )
            first_session = False
            outcome = await self._run_kernel_epoch(start_index)
            restart = outcome == "restart"
            if restart:
                start_index = self._initial_from_cell
            elif outcome == "cancel":
                self._cancel_requested.set()

    def _prepare_kernel_epoch(self, start_index: int, *, replace_cells: bool) -> None:
        self.notebook = self._read_source()
        self._validate_start_index(self.notebook, start_index)
        self.kernel_epoch = self.store.begin_kernel_epoch(self.run_id)
        records = _cell_records(self.notebook)
        self.store.update_source(self.run_id, source_hash(self.source_path), records)
        if replace_cells:
            self.store.replace_cells_for_restart(
                self.run_id, records, self.kernel_epoch, start_index
            )
        self.client = self._make_client()
        self.client.reset_execution_trackers()
        self._kernel_dead = False

    async def _run_kernel_epoch(self, start_index: int) -> str:
        assert self.client is not None
        async with self.client.async_setup_kernel(cwd=str(self.working_dir)):
            self._record_kernel_identity()
            await self._install_tqdm_instrumentation()
            self.store.update_run_status(
                self.run_id,
                RunStatus.RUNNING,
                message="Notebook executing",
                current_cell_index=start_index,
                failed_cell_index=None,
                failed_attempt=None,
            )
            await self.bus.publish(
                "notebook.kernel_started",
                {**self._kernel_identity(), "kernel_epoch": self.kernel_epoch},
            )
            try:
                return await self._execute_cells(start_index)
            finally:
                await self.bus.publish(
                    "notebook.kernel_stopped",
                    {**self._kernel_identity(), "kernel_epoch": self.kernel_epoch},
                )

    async def _install_tqdm_instrumentation(self) -> None:
        if not self.settings.capture_tqdm or not self._uses_python_kernel():
            return
        assert self.client is not None
        assert self.client.kc is not None
        try:
            execution = cast(Any, self.client.kc).execute_interactive(
                tqdm_bootstrap_code(self.settings.tqdm_min_interval_seconds),
                silent=True,
                store_history=False,
                allow_stdin=False,
                stop_on_error=False,
                timeout=float(self.settings.startup_timeout_seconds),
                output_hook=self._ignore_kernel_message,
            )
            reply = await execution if inspect.isawaitable(execution) else execution
            content = cast(dict[str, Any], reply.get("content", {}))
            if content.get("status") != "ok":
                raise RuntimeError(
                    str(content.get("evalue") or "kernel rejected instrumentation")
                )
        except asyncio.CancelledError:
            raise
        except Exception as error:
            try:
                await self.bus.publish(
                    "notebook.tqdm_instrumentation_failed",
                    {
                        "kernel_epoch": self.kernel_epoch,
                        "error": f"{type(error).__name__}: {error}",
                    },
                )
            except Exception:
                return

    def _uses_python_kernel(self) -> bool:
        metadata = cast(dict[str, Any], self.notebook.get("metadata", {}))
        kernelspec_value = metadata.get("kernelspec")
        if not isinstance(kernelspec_value, dict):
            return True
        kernelspec = cast(dict[str, Any], kernelspec_value)
        language = kernelspec.get("language")
        return language is None or str(language).lower().startswith("python")

    @staticmethod
    def _ignore_kernel_message(_message: dict[str, Any]) -> None:
        return

    async def _execute_cells(self, start_index: int) -> str:
        index = start_index
        while index < len(self.notebook.cells):
            if self._cancel_requested.is_set():
                return "cancel"
            cell = self.notebook.cells[index]
            self.current_cell_index = index
            if cell.cell_type != "code" or not str(cell.source).strip():
                self.store.mark_cell_skipped(self.run_id, index)
                index += 1
                continue
            outcome = await self._execute_cell_until_resolved(index)
            if outcome != "next":
                return outcome
            index += 1
        return "complete"

    async def _finish_cancelled(self) -> RunStatus:
        await self.resources.stop_cancel_resources()
        self.store.update_run_status(
            self.run_id,
            RunStatus.CANCELLED,
            message="Run cancelled",
            current_cell_index=None,
            ended=True,
        )
        await self.bus.publish("run.cancelled", {"kernel_epoch": self.kernel_epoch})
        return RunStatus.CANCELLED

    async def _wait_for_blocking_resources(self) -> RunStatus | None:
        if not self.settings.wait_for_blocking_resources:
            return None
        summary = self.resources.blocking_summary()
        if summary["active"]:
            await self._announce_blocking_wait(summary["active"])
        try:
            summary = await self.resources.wait_for_blocking_resources(
                self.settings.resource_completion_timeout_seconds,
                cancel_event=self._cancel_requested,
            )
        except TimeoutError:
            self.store.update_run_status(
                self.run_id,
                RunStatus.FAILED,
                message="Timed out waiting for blocking resources",
                ended=True,
            )
            await self.bus.publish(
                "run.external_timeout", {"kernel_epoch": self.kernel_epoch}
            )
            return RunStatus.FAILED
        if self._cancel_requested.is_set():
            return await self._finish_cancelled()
        if summary["failures"]:
            return await self._finish_external_failure(summary["failures"])
        return None

    async def _announce_blocking_wait(self, active: list[dict[str, Any]]) -> None:
        self.store.update_run_status(
            self.run_id,
            RunStatus.WAITING_EXTERNAL,
            message=f"Waiting for {len(active)} external resource(s)",
            current_cell_index=None,
        )
        await self.bus.publish(
            "run.waiting_external",
            {"resource_ids": [item["internal_id"] for item in active]},
        )

    async def _finish_external_failure(
        self, failures: list[dict[str, Any]]
    ) -> RunStatus:
        if self._cancel_requested.is_set():
            return await self._finish_cancelled()
        self.store.update_run_status(
            self.run_id,
            RunStatus.FAILED,
            message=f"{len(failures)} blocking resource(s) did not succeed",
            ended=True,
        )
        await self.bus.publish(
            "run.failed_external",
            {
                "kernel_epoch": self.kernel_epoch,
                "resource_ids": [item["internal_id"] for item in failures],
            },
        )
        return RunStatus.FAILED

    async def _finish_succeeded(self) -> RunStatus:
        if self._cancel_requested.is_set():
            return await self._finish_cancelled()
        self.store.update_run_status(
            self.run_id,
            RunStatus.SUCCEEDED,
            message="Notebook and blocking resources completed",
            current_cell_index=None,
            failed_cell_index=None,
            failed_attempt=None,
            ended=True,
        )
        await self.bus.publish(
            "run.succeeded",
            {
                "kernel_epoch": self.kernel_epoch,
                "output_path": str(self.output_path),
            },
        )
        return RunStatus.SUCCEEDED

    async def _execute_cell_until_resolved(self, index: int) -> str:
        while True:
            cell = self.notebook.cells[index]
            digest = _source_digest(str(cell.source))
            attempt = self.store.begin_cell_attempt(
                self.run_id, index, str(cell.source), digest, self.kernel_epoch
            )
            self.current_attempt = attempt
            assert self.client is not None
            self.client.current_attempt = attempt
            self._paused.clear()
            self.store.update_run_status(
                self.run_id,
                RunStatus.RUNNING,
                message=f"Executing cell {index + 1}",
                current_cell_index=index,
                failed_cell_index=None,
                failed_attempt=None,
            )
            await self.bus.publish(
                "cell.started",
                {
                    "cell_index": index,
                    "cell_id": cell.id,
                    "attempt": attempt,
                    "kernel_epoch": self.kernel_epoch,
                    "label": _cell_label(self.notebook, index),
                },
            )
            started = time.monotonic()
            self._running_cell = True
            try:
                await self.client.async_execute_cell(
                    cell, index, execution_count=self.client.code_cells_executed + 1
                )
                await self._drain_output_tasks()
                elapsed = time.monotonic() - started
                self.store.complete_cell(
                    self.run_id,
                    index,
                    status=CellStatus.SUCCEEDED,
                    elapsed_seconds=elapsed,
                )
                await self.bus.publish(
                    "cell.succeeded",
                    {
                        "cell_index": index,
                        "attempt": attempt,
                        "elapsed_seconds": elapsed,
                    },
                )
                await self._checkpoint(force=True, writeback=True)
                return "next"
            except (CellExecutionError, CellTimeoutError, DeadKernelError) as error:
                timeout_synchronized = True
                if isinstance(error, CellTimeoutError):
                    timeout_synchronized = await self._recover_timed_out_kernel()
                await self._drain_output_tasks()
                elapsed = time.monotonic() - started
                error_name, error_value, traceback = self._extract_error(cell, error)
                self._kernel_dead = isinstance(error, DeadKernelError) or not (
                    timeout_synchronized
                )
                if self._cancel_requested.is_set():
                    self.store.complete_cell(
                        self.run_id,
                        index,
                        status=CellStatus.INTERRUPTED,
                        elapsed_seconds=elapsed,
                        error_name=error_name,
                        error_value=error_value,
                        traceback=traceback,
                    )
                    return "cancel"
                self.failed_cell_index = index
                self.failed_attempt = attempt
                self.store.complete_cell(
                    self.run_id,
                    index,
                    status=CellStatus.FAILED,
                    elapsed_seconds=elapsed,
                    error_name=error_name,
                    error_value=error_value,
                    traceback=traceback,
                )
                self.store.update_run_status(
                    self.run_id,
                    RunStatus.PAUSED,
                    message=f"Cell {index + 1} failed: {error_name}: {error_value}",
                    current_cell_index=index,
                    failed_cell_index=index,
                    failed_attempt=attempt,
                )
                self._paused.set()
                await self._checkpoint(force=True, writeback=True)
                await self.bus.publish(
                    "cell.failed",
                    {
                        "cell_index": index,
                        "attempt": attempt,
                        "kernel_epoch": self.kernel_epoch,
                        "error_name": error_name,
                        "error_value": error_value,
                        "traceback": traceback[-20:],
                        "kernel_dead": self._kernel_dead,
                    },
                )
                decision = await self._recovery_loop(index, attempt)
                if decision == "resume":
                    continue
                return decision
            finally:
                self._running_cell = False

    async def _recover_timed_out_kernel(self) -> bool:
        """Interrupt and synchronize a timed-out kernel before offering live resume."""
        try:
            if not await self.interrupt():
                raise RuntimeError("Timed-out kernel could not be interrupted")
            assert self.client is not None
            await self.client.synchronize_after_interrupt(
                min(
                    float(self.settings.startup_timeout_seconds),
                    _TIMEOUT_RECOVERY_MAX_SECONDS,
                )
            )
            await self.bus.publish(
                "notebook.timeout_recovered",
                {
                    "cell_index": self.current_cell_index,
                    "attempt": self.current_attempt,
                    "kernel_epoch": self.kernel_epoch,
                },
            )
            return True
        except Exception as error:
            await self.bus.publish(
                "notebook.timeout_recovery_failed",
                {
                    "cell_index": self.current_cell_index,
                    "attempt": self.current_attempt,
                    "kernel_epoch": self.kernel_epoch,
                    "error_type": type(error).__name__,
                    "error": str(error),
                },
            )
            return False

    async def _recovery_loop(self, cell_index: int, attempt: int) -> str:
        while True:
            command = await self.command_queue.get()
            if command.kind == "cancel":
                return "cancel"
            try:
                if command.expected_kernel_epoch != self.kernel_epoch:
                    raise RuntimeError(
                        "Kernel epoch changed before the action executed"
                    )
                if command.expected_failed_attempt != attempt:
                    raise RuntimeError(
                        "Failed-cell attempt changed before the action executed"
                    )
                if source_hash(self.source_path) != command.requested_source_hash:
                    raise RuntimeError(
                        "source.ipynb changed after the action was requested"
                    )
                if command.kind == "resume" and not self._kernel_dead:
                    fresh = self._read_source()
                    self._validate_live_resume(fresh, cell_index)
                    self._apply_fresh_sources(fresh, cell_index)
                    self._paused.clear()
                    self.store.finish_action(
                        command.action_id,
                        ActionStatus.COMPLETED,
                        message="Source reloaded; failed cell will resume in the live kernel",
                    )
                    return "resume"
                fresh = self._read_source()
                self._validate_start_index(fresh, command.from_cell)
                self._initial_from_cell = command.from_cell
                self._paused.clear()
                self.store.update_run_status(
                    self.run_id,
                    RunStatus.RESTARTING,
                    message=f"Restarting kernel and replaying from cell {command.from_cell + 1}",
                )
                self.store.finish_action(
                    command.action_id,
                    ActionStatus.COMPLETED,
                    message="Kernel restart accepted",
                    result={"from_cell": command.from_cell},
                )
                return "restart"
            except Exception as error:
                self.store.finish_action(
                    command.action_id, ActionStatus.REJECTED, message=str(error)
                )
                await self._publish_action_rejection_safely(command, error)

    async def _publish_action_rejection_safely(
        self, command: RunnerCommand, error: Exception
    ) -> None:
        try:
            await self.bus.publish(
                "action.rejected",
                {"action_id": command.action_id, "error": str(error)},
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            # The durable action state is authoritative; a diagnostic event failure
            # must not turn a rejected recovery command into a runner failure.
            return

    @staticmethod
    def _validate_start_index(notebook: NotebookNode, from_cell: int) -> None:
        if from_cell < 0:
            raise ValueError("from-cell must be nonnegative")
        if from_cell > 0 and from_cell >= len(notebook.cells):
            raise ValueError(
                f"from-cell {from_cell} is outside the notebook's "
                f"{len(notebook.cells)} cell(s)"
            )

    def _validate_live_resume(self, fresh: NotebookNode, failed_index: int) -> None:
        if len(fresh.cells) != len(self.notebook.cells):
            raise RuntimeError("Cell additions/removals require restart")
        for index, (old, new) in enumerate(
            zip(self.notebook.cells, fresh.cells, strict=True)
        ):
            if old.id != new.id:
                raise RuntimeError("Cell reordering or replacement requires restart")
            if index < failed_index and str(old.source) != str(new.source):
                raise RuntimeError(
                    f"Already-executed cell {index + 1} changed; restart is required"
                )

    def _apply_fresh_sources(self, fresh: NotebookNode, from_index: int) -> None:
        for index in range(from_index, len(self.notebook.cells)):
            self.notebook.cells[index].source = fresh.cells[index].source
            self.notebook.cells[index].metadata = fresh.cells[index].metadata
        failed = self.notebook.cells[from_index]
        failed.outputs = []
        failed.execution_count = None
        self.store.update_source(
            self.run_id, source_hash(self.source_path), _cell_records(fresh)
        )

    def _read_source(self) -> NotebookNode:
        notebook = nbformat.read(self.source_path, as_version=4)
        ensure_cell_ids(notebook)
        clear_notebook_outputs(notebook)
        return notebook

    def _on_output(self, output: NotebookNode, cell_index: int, attempt: int) -> None:
        structured_payloads = self._structured_payloads(output)
        if not self._is_hidden_structured_output(output):
            summary = summarize_output(output)
            self._buffer_cell_output(cell_index, summary)
            self._schedule_output_flush(cell_index, attempt)
        for mime_type, payload in structured_payloads:
            if mime_type == RESOURCE_MIME_TYPE:
                try:
                    event = ResourceEvent.model_validate(payload)
                except Exception as error:
                    self._schedule(
                        self.bus.publish(
                            "notebook.invalid_resource_event",
                            {
                                "cell_index": cell_index,
                                "attempt": attempt,
                                "error": str(error),
                            },
                        )
                    )
                else:
                    self._schedule(
                        self.resources.register(
                            event,
                            cell_index=cell_index,
                            attempt=attempt,
                            kernel_epoch=self.kernel_epoch,
                        )
                    )
            elif mime_type == EVENT_MIME_TYPE:
                try:
                    progress = ProgressEvent.model_validate(payload)
                    self._schedule(
                        self.bus.publish(
                            "notebook.progress",
                            {
                                "cell_index": cell_index,
                                "attempt": attempt,
                                **progress.model_dump(mode="json"),
                            },
                        )
                    )
                except Exception as error:
                    self._schedule(
                        self.bus.publish(
                            "notebook.invalid_event",
                            {
                                "cell_index": cell_index,
                                "attempt": attempt,
                                "error": str(error),
                            },
                        )
                    )
        self._request_checkpoint()

    @staticmethod
    def _is_hidden_structured_output(output: NotebookNode) -> bool:
        if output.get("output_type") not in {"display_data", "execute_result"}:
            return False
        data = cast(dict[str, Any], output.get("data", {}))
        return bool(data) and set(data).issubset({RESOURCE_MIME_TYPE, EVENT_MIME_TYPE})

    def _buffer_cell_output(self, cell_index: int, summary: dict[str, Any]) -> None:
        previous = self._pending_cell_outputs.get(cell_index)
        count = self._pending_cell_output_counts.get(cell_index, 0) + 1
        self._pending_cell_output_counts[cell_index] = count
        if (
            previous is not None
            and previous.get("output_type") == "stream"
            and summary.get("output_type") == "stream"
            and previous.get("name") == summary.get("name")
        ):
            self._pending_cell_outputs[cell_index] = {
                **summary,
                "text": (str(previous.get("text", "")) + str(summary.get("text", "")))[
                    -6_000:
                ],
            }
        else:
            self._pending_cell_outputs[cell_index] = summary

    def _schedule_output_flush(self, cell_index: int, attempt: int) -> None:
        task = self._output_flush_tasks.get(cell_index)
        if task is not None and not task.done():
            return
        flush_task = asyncio.ensure_future(
            self._delayed_output_flush(cell_index, attempt)
        )
        self._output_flush_tasks[cell_index] = flush_task
        self._track_output_task(flush_task)

    async def _delayed_output_flush(self, cell_index: int, attempt: int) -> None:
        task = asyncio.current_task()
        try:
            await asyncio.sleep(_OUTPUT_PERSIST_INTERVAL_SECONDS)
            self._flush_cell_output(cell_index, attempt)
        finally:
            if self._output_flush_tasks.get(cell_index) is task:
                self._output_flush_tasks.pop(cell_index, None)

    def _flush_cell_output(self, cell_index: int, attempt: int) -> None:
        summary = self._pending_cell_outputs.pop(cell_index, None)
        count = self._pending_cell_output_counts.pop(cell_index, 0)
        if summary is None:
            return
        if count > 1:
            summary = {**summary, "coalesced_messages": count}
        self.store.append_cell_output(self.run_id, cell_index, summary)
        self._schedule(
            self.bus.publish_ephemeral(
                "cell.output",
                {"cell_index": cell_index, "attempt": attempt, "output": summary},
            )
        )

    @staticmethod
    def _structured_payloads(output: NotebookNode) -> list[tuple[str, dict[str, Any]]]:
        data = cast(dict[str, Any], output.get("data", {}))
        values = NotebookRunner._mime_payloads(data)
        if output.get("output_type") != "stream":
            return values
        for line in str(output.get("text", "")).splitlines():
            fallback = NotebookRunner._fallback_payload(line)
            if fallback is not None:
                values.append(fallback)
        return values

    @staticmethod
    def _mime_payloads(data: dict[str, Any]) -> list[tuple[str, dict[str, Any]]]:
        values: list[tuple[str, dict[str, Any]]] = []
        for mime_type in (RESOURCE_MIME_TYPE, EVENT_MIME_TYPE):
            payload = data.get(mime_type)
            if isinstance(payload, dict):
                values.append((mime_type, cast(dict[str, Any], payload)))
        return values

    @staticmethod
    def _fallback_payload(line: str) -> tuple[str, dict[str, Any]] | None:
        if not line.startswith(FALLBACK_PREFIX):
            return None
        try:
            decoded = json.loads(line[len(FALLBACK_PREFIX) :])
            wrapper = cast(dict[str, Any], decoded)
            mime_type = str(wrapper["mime_type"])
            payload = wrapper["payload"]
            if mime_type not in {RESOURCE_MIME_TYPE, EVENT_MIME_TYPE}:
                return None
            if not isinstance(payload, dict):
                return None
            return mime_type, cast(dict[str, Any], payload)
        except (KeyError, TypeError, ValueError, json.JSONDecodeError):
            return None

    def _schedule(self, awaitable: Awaitable[Any]) -> None:
        task = asyncio.ensure_future(awaitable)
        self._track_output_task(task)

    def _track_output_task(self, task: asyncio.Task[Any]) -> None:
        self._pending_output_tasks.add(task)
        task.add_done_callback(self._output_task_finished)

    def _output_task_finished(self, task: asyncio.Task[Any]) -> None:
        if task not in self._pending_output_tasks:
            return
        self._pending_output_tasks.discard(task)
        try:
            task.result()
        except asyncio.CancelledError:
            return
        except Exception as error:
            self._pending_output_errors.append(error)

    async def _drain_output_tasks(self) -> None:
        flush_tasks = list(self._output_flush_tasks.values())
        for task in flush_tasks:
            task.cancel()
        if flush_tasks:
            await asyncio.gather(*flush_tasks, return_exceptions=True)
            self._pending_output_tasks.difference_update(flush_tasks)
        for cell_index in list(self._pending_cell_outputs):
            self._flush_cell_output(cell_index, self.current_attempt or 0)
        while self._pending_output_tasks:
            tasks = list(self._pending_output_tasks)
            await asyncio.gather(*tasks, return_exceptions=True)
            for task in tasks:
                self._output_task_finished(task)
        if self._pending_output_errors:
            error = self._pending_output_errors.pop(0)
            self._pending_output_errors.clear()
            raise RuntimeError(
                "Notebook output side effect failed: "
                f"{type(error).__name__}: {error}"
            ) from error

    def _make_client(self) -> MonitoredNotebookClient:
        kwargs: dict[str, Any] = {
            "output_callback": self._on_output,
            "timeout": self.settings.timeout_seconds,
            "startup_timeout": self.settings.startup_timeout_seconds,
            "resources": {"metadata": {"path": str(self.working_dir)}},
            "allow_errors": False,
            "record_timing": True,
            "store_widget_state": True,
        }
        if self.settings.kernel_name:
            kwargs["kernel_name"] = self.settings.kernel_name
        return MonitoredNotebookClient(self.notebook, **kwargs)

    def _record_kernel_identity(self) -> None:
        identity = self._kernel_identity()
        self.store.update_kernel(
            self.run_id,
            kernel_id=identity.get("kernel_id"),
            kernel_pid=identity.get("kernel_pid"),
        )

    def _kernel_identity(self) -> dict[str, Any]:
        if self.client is None or self.client.km is None:
            return {"kernel_id": None, "kernel_pid": None}
        provisioner = getattr(self.client.km, "provisioner", None)
        return {
            "kernel_id": getattr(self.client.km, "kernel_id", None),
            "kernel_pid": getattr(provisioner, "pid", None),
        }

    @staticmethod
    def _extract_error(
        cell: NotebookNode, error: Exception
    ) -> tuple[str, str, list[str]]:
        if isinstance(error, CellTimeoutError):
            for output in reversed(cell.get("outputs", [])):
                if output.get("output_type") == "error":
                    traceback = [
                        _strip_ansi(str(line)) for line in output.get("traceback", [])
                    ]
                    return type(error).__name__, str(error), traceback
            return type(error).__name__, str(error), [_strip_ansi(str(error))]
        for output in reversed(cell.get("outputs", [])):
            if output.get("output_type") == "error":
                return (
                    str(output.get("ename", type(error).__name__)),
                    str(output.get("evalue", error)),
                    [_strip_ansi(str(line)) for line in output.get("traceback", [])],
                )
        return type(error).__name__, str(error), [_strip_ansi(str(error))]

    def _request_checkpoint(self) -> None:
        self._checkpoint_event.set()

    async def _checkpoint_loop(self) -> None:
        while True:
            await self._checkpoint_event.wait()
            await asyncio.sleep(self.settings.checkpoint_interval_seconds)
            await self._checkpoint(force=True)

    async def _checkpoint(self, *, force: bool, writeback: bool = False) -> None:
        if not force and not self._checkpoint_event.is_set():
            return
        async with self._checkpoint_lock:
            await asyncio.to_thread(
                self._write_notebook_atomic, self.notebook, self.partial_output_path
            )
            if writeback:
                await asyncio.to_thread(self._write_back_notebook)
            self._checkpoint_event.clear()

    def _load_writeback_hash(self) -> str:
        if not self.writeback_state_path.exists():
            digest = source_hash(self.notebook_path)
            self._write_writeback_state(digest)
            return digest
        try:
            payload = json.loads(self.writeback_state_path.read_text(encoding="utf-8"))
            path = Path(str(payload["notebook_path"])).resolve()
            digest = str(payload["sha256"])
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
            raise RuntimeError(
                f"Invalid Runwatch write-back state: {self.writeback_state_path}"
            ) from error
        if path != self.notebook_path:
            raise RuntimeError(
                "Runwatch write-back state targets a different notebook: "
                f"{path} != {self.notebook_path}"
            )
        if not re.fullmatch(r"[0-9a-f]{64}", digest):
            raise RuntimeError(f"Invalid notebook hash in {self.writeback_state_path}")
        return digest

    def _write_back_notebook(self) -> None:
        try:
            current_hash = source_hash(self.notebook_path)
        except FileNotFoundError as error:
            raise RuntimeError(
                f"Original notebook disappeared during execution: {self.notebook_path}"
            ) from error
        if current_hash != self._expected_notebook_hash:
            checkpoint_hash = source_hash(self.partial_output_path)
            if current_hash == checkpoint_hash:
                self._record_writeback_hash(current_hash)
                return
            raise RuntimeError(
                "Original notebook changed outside Runwatch during execution; "
                f"refusing to overwrite {self.notebook_path}. The executed state "
                f"is preserved at {self.partial_output_path}."
            )
        write_notebook_atomic(self.notebook, self.notebook_path)
        self._record_writeback_hash(source_hash(self.notebook_path))

    def _record_writeback_hash(self, digest: str) -> None:
        self._write_writeback_state(digest)
        self._expected_notebook_hash = digest

    def _write_writeback_state(self, digest: str) -> None:
        payload = json.dumps(
            {
                "notebook_path": str(self.notebook_path),
                "sha256": digest,
            },
            indent=2,
        )
        temporary = self.writeback_state_path.with_name(
            f".{self.writeback_state_path.name}.{uuid4().hex}.tmp"
        )
        with temporary.open("w", encoding="utf-8") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, self.writeback_state_path)

    @staticmethod
    def _write_notebook_atomic(notebook: NotebookNode, path: Path) -> None:
        write_notebook_atomic(notebook, path)
