from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import os
import re
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast
from uuid import uuid4

import nbformat
from nbclient import NotebookClient
from nbclient.exceptions import CellExecutionError, CellTimeoutError, DeadKernelError
from nbclient.util import run_sync
from nbformat import NotebookNode

from ._fs import atomic_write_bytes
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
_CHECKPOINT_RETRY_MAX_SECONDS = 30.0
_WRITEBACK_STATE_FILENAME = "writeback-state.json"
_EVENT_TEXT_MAX_CHARS = 120
_EVENT_RESOURCE_ID_MAX_CHARS = 64
_EVENT_RESOURCE_SAMPLE_SIZE = 5


@dataclass(frozen=True)
class _FileFingerprint:
    digest: str
    device: int
    inode: int
    size: int
    modified_ns: int


class _CancellationEscalated(RuntimeError):
    pass


def _notebook_bytes(notebook: NotebookNode) -> bytes:
    return nbformat.writes(notebook).encode("utf-8")


def _file_fingerprint(path: Path) -> _FileFingerprint:
    """Read a stable identity and digest for a regular file."""

    with path.open("rb") as handle:
        before = os.fstat(handle.fileno())
        payload = handle.read()
        after = os.fstat(handle.fileno())
    if (
        before.st_dev,
        before.st_ino,
        before.st_size,
        before.st_mtime_ns,
    ) != (
        after.st_dev,
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
    ):
        raise RuntimeError(f"File changed while Runwatch was reading it: {path}")
    return _FileFingerprint(
        digest=hashlib.sha256(payload).hexdigest(),
        device=after.st_dev,
        inode=after.st_ino,
        size=after.st_size,
        modified_ns=after.st_mtime_ns,
    )


def _strip_ansi(value: str) -> str:
    return _ANSI_RE.sub("", value)


def _source_digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _cell_label(notebook: NotebookNode, index: int) -> str:
    cell = notebook.cells[index]
    configured = cell.metadata.get("runwatch", {}).get("label")
    if configured:
        return str(configured)[:_EVENT_TEXT_MAX_CHARS]
    for previous in reversed(notebook.cells[:index]):
        if previous.cell_type != "markdown":
            continue
        for line in str(previous.source).splitlines():
            stripped = line.strip()
            if stripped.startswith("#"):
                return stripped.lstrip("#").strip()[:_EVENT_TEXT_MAX_CHARS]
    first = next(
        (line.strip() for line in str(cell.source).splitlines() if line.strip()), ""
    )
    return first[:_EVENT_TEXT_MAX_CHARS] or f"{cell.cell_type.title()} cell {index + 1}"


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
    """Atomically write a notebook, preserving an existing destination's mode."""

    atomic_write_bytes(path, _notebook_bytes(notebook))


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
        output_callback: Callable[[NotebookNode, int, int], bool | None],
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.output_callback = output_callback
        self.current_attempt = 0
        self._output_poll_task: asyncio.Task[None] | None = None
        self._kernel_cleanup_lock = asyncio.Lock()

    async def _async_cleanup_kernel(self) -> None:
        """Serialize signal and context-manager cleanup of the owned kernel."""

        async with self._kernel_cleanup_lock:
            if self.km is None:
                return
            await super()._async_cleanup_kernel()

    _cleanup_kernel = run_sync(_async_cleanup_kernel)

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
            keep_output = self.output_callback(output, cell_index, self.current_attempt)
            if keep_output is False:
                cell.outputs = [item for item in cell.outputs if item is not output]
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
        self._checkpoint_requested_generation = 0
        self._checkpoint_persisted_generation = 0
        self._checkpoint_consecutive_failures = 0
        self._finished = asyncio.Event()
        self._paused = asyncio.Event()
        self._cancel_requested = asyncio.Event()
        self._cancel_lock = asyncio.Lock()
        self._cancel_escalation_task: asyncio.Task[None] | None = None
        self._cancel_command_enqueued = False
        self._cancel_abandon_execution = asyncio.Event()
        self._running_cell = False
        self._cell_stopped = asyncio.Event()
        self._cell_stopped.set()
        self._cell_execution_task: asyncio.Future[NotebookNode] | None = None
        self._kernel_dead = False
        self._kernel_state_lost = any(
            event["type"] == "notebook.kernel_state_lost"
            for event in self.store.recent_events(self.run_id, limit=1_000)
        )
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

    async def shutdown(self) -> None:
        """Cancel and join runner-owned background work before services close."""

        async with self._cancel_lock:
            escalation = self._cancel_escalation_task
            self._cancel_escalation_task = None
        if escalation is None or escalation is asyncio.current_task():
            return
        if not escalation.done():
            escalation.cancel()
        await asyncio.gather(escalation, return_exceptions=True)

    def _cell_execution_active(self) -> bool:
        execution = self._cell_execution_task
        return execution is not None and not execution.done()

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
        run_status = RunStatus(self.store.get_run(self.run_id)["status"])
        if run_status.terminal:
            return
        if not self._cancel_requested.is_set():
            event = self.store.request_run_cancellation(
                self.run_id,
                message="Cancellation requested",
                event_payload={},
            )
            self._cancel_requested.set()
            if event is not None:
                self.bus.fan_out_persisted(event)

        escalation_task: asyncio.Task[None] | None = None
        async with self._cancel_lock:
            if self.paused and not self._cancel_command_enqueued:
                await self.enqueue(
                    RunnerCommand(
                        action_id="internal-cancel",
                        kind="cancel",
                        expected_kernel_epoch=self.kernel_epoch,
                    )
                )
                self._cancel_command_enqueued = True
            elif self._cell_execution_active():
                current = self._cancel_escalation_task
                if current is None or current.done():
                    current = asyncio.create_task(
                        self._escalate_cancellation(),
                        name=f"cancel-kernel:{self.run_id}",
                    )
                    self._cancel_escalation_task = current
                escalation_task = current
        if escalation_task is not None:
            await asyncio.shield(escalation_task)

    async def _escalate_cancellation(self) -> None:
        client = self.client
        if client is None or client.km is None or not self._cell_execution_active():
            return
        kernel_manager = cast(Any, client.km)
        stages: list[tuple[str, Callable[[], Any] | None, float]] = [
            (
                "interrupt",
                getattr(kernel_manager, "interrupt_kernel", None),
                self.settings.cancel_interrupt_grace_seconds,
            ),
            (
                "shutdown",
                self._kernel_shutdown_callable(kernel_manager, now=False),
                self.settings.cancel_shutdown_grace_seconds,
            ),
            (
                "terminate",
                self._provisioner_callable(kernel_manager, "terminate"),
                self.settings.cancel_terminate_grace_seconds,
            ),
            (
                "kill",
                self._provisioner_callable(kernel_manager, "kill")
                or self._kernel_shutdown_callable(kernel_manager, now=True),
                self.settings.cancel_kill_grace_seconds,
            ),
        ]
        for stage, action, grace_seconds in stages:
            if not self._cell_execution_active():
                return
            if action is None:
                continue
            if stage != "interrupt":
                await self._mark_kernel_state_lost(stage)
            await self._publish_cancellation_event_safely(
                "notebook.cancel_escalated",
                {
                    "stage": stage,
                    "kernel_state_lost": self._kernel_state_lost,
                    "cell_index": self.current_cell_index,
                    "attempt": self.current_attempt,
                    "kernel_epoch": self.kernel_epoch,
                },
            )
            if stage == "interrupt":
                await self._publish_cancellation_event_safely(
                    "notebook.interrupt_requested",
                    {
                        "cell_index": self.current_cell_index,
                        "attempt": self.current_attempt,
                    },
                )
            await self._run_cancellation_stage(stage, action, grace_seconds)
            if not self._cell_execution_active():
                return

        # A provider implementation may ignore cancellation even after its process is
        # gone. Let the runner leave that await instead of hanging forever.
        self._cancel_abandon_execution.set()
        await self._publish_cancellation_event_safely(
            "notebook.cancel_execution_abandoned",
            {
                "cell_index": self.current_cell_index,
                "attempt": self.current_attempt,
                "kernel_epoch": self.kernel_epoch,
                "kernel_state_lost": self._kernel_state_lost,
            },
        )

    async def _mark_kernel_state_lost(self, stage: str) -> None:
        self._kernel_dead = True
        if self._kernel_state_lost:
            return
        self._kernel_state_lost = True
        await self._publish_cancellation_event_safely(
            "notebook.kernel_state_lost",
            {
                "stage": stage,
                "kernel_epoch": self.kernel_epoch,
                "cell_index": self.current_cell_index,
                "attempt": self.current_attempt,
                "kernel_state_lost": True,
            },
        )

    async def _run_cancellation_stage(
        self,
        stage: str,
        action: Callable[[], Any],
        grace_seconds: float,
    ) -> None:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + grace_seconds
        try:
            result = action()
            if inspect.isawaitable(result):
                await asyncio.wait_for(
                    result,
                    timeout=max(0.001, deadline - loop.time()),
                )
        except asyncio.CancelledError:
            raise
        except Exception as error:
            await self._publish_cancellation_event_safely(
                "notebook.cancel_stage_failed",
                {
                    "stage": stage,
                    "error_type": type(error).__name__,
                    "error": str(error),
                    "kernel_epoch": self.kernel_epoch,
                },
            )
        remaining = max(0.0, deadline - loop.time())
        if remaining > 0 and self._cell_execution_active():
            try:
                await asyncio.wait_for(self._cell_stopped.wait(), timeout=remaining)
            except TimeoutError:
                pass

    @staticmethod
    def _kernel_shutdown_callable(
        kernel_manager: Any, *, now: bool
    ) -> Callable[[], Any] | None:
        shutdown = getattr(kernel_manager, "shutdown_kernel", None)
        if shutdown is None:
            return None
        return lambda: shutdown(now=now)

    @staticmethod
    def _provisioner_callable(
        kernel_manager: Any, method_name: str
    ) -> Callable[[], Any] | None:
        provisioner = getattr(kernel_manager, "provisioner", None)
        method = getattr(provisioner, method_name, None)
        if method is None:
            return None
        return lambda: method(restart=False)

    async def _publish_cancellation_event_safely(
        self, event_type: str, payload: dict[str, Any]
    ) -> None:
        try:
            await self.bus.publish(event_type, payload)
        except asyncio.CancelledError:
            raise
        except Exception:
            # The durable cancelling state remains authoritative if diagnostics fail.
            return

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
        working_dir = str(self.working_dir)
        bounded_working_dir = working_dir[:_EVENT_TEXT_MAX_CHARS]
        await self.bus.publish(
            "run.started",
            {
                "working_dir": bounded_working_dir,
                "projection_truncated": bounded_working_dir != working_dir,
            },
        )
        self._checkpoint_task = asyncio.create_task(
            self._checkpoint_loop(), name=f"checkpoint:{self.run_id}"
        )
        try:
            return await self._run_sessions()
        except asyncio.CancelledError:
            raise
        except Exception as error:
            checkpoint_error: Exception | None = None
            try:
                await self._drain_output_tasks()
                await self._checkpoint(force=True)
            except Exception as artifact_error:
                checkpoint_error = artifact_error
            message = f"Runner failure: {type(error).__name__}: {error}"
            if checkpoint_error is not None:
                message += (
                    "; final checkpoint failed: "
                    f"{type(checkpoint_error).__name__}: {checkpoint_error}"
                )
            event = self.store.finish_run(
                self.run_id,
                RunStatus.FAILED,
                message=message,
                event_type="run.runner_error",
                event_payload={
                    "kernel_epoch": self.kernel_epoch,
                    "error_type": type(error).__name__,
                    "error": str(error),
                },
            )
            self.bus.fan_out_persisted(event)
            return RunStatus.FAILED
        finally:
            if self._checkpoint_task:
                self._checkpoint_task.cancel()
                await asyncio.gather(self._checkpoint_task, return_exceptions=True)
            self._finished.set()

    async def _run_sessions(self) -> RunStatus:
        await self._run_kernel_epochs()
        if self._cancel_requested.is_set():
            return await self._finish_cancelled()
        output_snapshot = _notebook_bytes(self.notebook)
        await asyncio.to_thread(
            self._write_notebook_bytes_atomic,
            output_snapshot,
            self.output_path,
        )
        blocking_status = await self._wait_for_blocking_resources()
        if blocking_status is not None:
            return blocking_status
        self.store.update_run_status(
            self.run_id,
            RunStatus.FINALIZING,
            message="Persisting final notebook state",
            current_cell_index=None,
        )
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
        if self._checkpoint_task is not None:
            await self._drain_output_tasks()
            await self._checkpoint(force=True)
        message = (
            "Run cancelled; in-memory kernel state was lost during escalation"
            if self._kernel_state_lost
            else "Run cancelled"
        )
        event = self.store.finish_run(
            self.run_id,
            RunStatus.CANCELLED,
            message=message,
            current_cell_index=None,
            event_type="run.cancelled",
            event_payload={
                "kernel_epoch": self.kernel_epoch,
                "kernel_state_lost": self._kernel_state_lost,
            },
        )
        self.bus.fan_out_persisted(event)
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
            await self._drain_output_tasks()
            await self._checkpoint(force=True)
            event = self.store.finish_run(
                self.run_id,
                RunStatus.FAILED,
                message="Timed out waiting for blocking resources",
                event_type="run.external_timeout",
                event_payload={"kernel_epoch": self.kernel_epoch},
            )
            self.bus.fan_out_persisted(event)
            return RunStatus.FAILED
        if self._cancel_requested.is_set():
            return await self._finish_cancelled()
        if summary["failures"]:
            return await self._finish_external_failure(summary["failures"])
        return None

    async def _announce_blocking_wait(self, active: list[dict[str, Any]]) -> None:
        resource_ids = [str(item["internal_id"]) for item in active]
        resource_ids_sample = [
            resource_id[:_EVENT_RESOURCE_ID_MAX_CHARS]
            for resource_id in resource_ids[:_EVENT_RESOURCE_SAMPLE_SIZE]
        ]
        self.store.update_run_status(
            self.run_id,
            RunStatus.WAITING_EXTERNAL,
            message=f"Waiting for {len(active)} external resource(s)",
            current_cell_index=None,
        )
        await self.bus.publish(
            "run.waiting_external",
            {
                "resource_count": len(resource_ids),
                "resource_ids_sample": resource_ids_sample,
                "projection_truncated": (
                    len(resource_ids) > len(resource_ids_sample)
                    or resource_ids_sample != resource_ids[: len(resource_ids_sample)]
                ),
            },
        )

    async def _finish_external_failure(
        self, failures: list[dict[str, Any]]
    ) -> RunStatus:
        if self._cancel_requested.is_set():
            return await self._finish_cancelled()
        await self._drain_output_tasks()
        await self._checkpoint(force=True)
        event = self.store.finish_run(
            self.run_id,
            RunStatus.FAILED,
            message=f"{len(failures)} blocking resource(s) did not succeed",
            event_type="run.failed_external",
            event_payload={
                "kernel_epoch": self.kernel_epoch,
                "resource_ids": [item["internal_id"] for item in failures],
            },
        )
        self.bus.fan_out_persisted(event)
        return RunStatus.FAILED

    async def _finish_succeeded(self) -> RunStatus:
        if self._cancel_requested.is_set():
            return await self._finish_cancelled()
        event = self.store.finish_run(
            self.run_id,
            RunStatus.SUCCEEDED,
            message="Notebook and blocking resources completed",
            current_cell_index=None,
            failed_cell_index=None,
            failed_attempt=None,
            event_type="run.succeeded",
            event_payload={
                "kernel_epoch": self.kernel_epoch,
                "output_path": str(self.output_path),
            },
        )
        self.bus.fan_out_persisted(event)
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
            self._cell_stopped.clear()
            self._cancel_abandon_execution.clear()
            try:
                outcome = await self._execute_cell_attempt(
                    cell, index, attempt, started
                )
                if outcome == "resume":
                    continue
                return outcome
            finally:
                self._running_cell = False
                self._cell_stopped.set()

    async def _execute_cell_attempt(
        self,
        cell: NotebookNode,
        index: int,
        attempt: int,
        started: float,
    ) -> str:
        try:
            await self._execute_client_cell(cell, index)
            return await self._complete_successful_cell(index, attempt, started)
        except (CellExecutionError, CellTimeoutError, DeadKernelError) as error:
            return await self._handle_execution_failure(
                cell, index, attempt, started, error
            )
        except _CancellationEscalated as error:
            return await self._handle_cancelled_execution(
                index, attempt, started, error
            )
        except Exception as error:
            if not self._cancel_requested.is_set():
                raise
            return await self._handle_cancelled_execution(
                index, attempt, started, error
            )

    async def _complete_successful_cell(
        self, index: int, attempt: int, started: float
    ) -> str:
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

    async def _handle_execution_failure(
        self,
        cell: NotebookNode,
        index: int,
        attempt: int,
        started: float,
        error: CellExecutionError | CellTimeoutError | DeadKernelError,
    ) -> str:
        timeout_synchronized = True
        if isinstance(error, CellTimeoutError):
            timeout_synchronized = await self._recover_timed_out_kernel()
        await self._drain_output_tasks()
        elapsed = time.monotonic() - started
        error_name, error_value, traceback = self._extract_error(cell, error)
        self._kernel_dead = (
            isinstance(error, DeadKernelError) or not timeout_synchronized
        )
        if self._cancel_requested.is_set():
            await self._record_interrupted_cell(
                index,
                attempt,
                elapsed,
                error_name=error_name,
                error_value=error_value,
                traceback=traceback,
            )
            return "cancel"
        await self._pause_failed_cell(
            index,
            attempt,
            elapsed,
            error_name=error_name,
            error_value=error_value,
            traceback=traceback,
        )
        return await self._recovery_loop(index, attempt)

    async def _pause_failed_cell(
        self,
        index: int,
        attempt: int,
        elapsed: float,
        *,
        error_name: str,
        error_value: str,
        traceback: list[str],
    ) -> None:
        self.failed_cell_index = index
        self.failed_attempt = attempt
        await self._checkpoint(force=True, writeback=True)
        event = self.store.pause_failed_cell(
            self.run_id,
            index,
            attempt=attempt,
            kernel_epoch=self.kernel_epoch,
            elapsed_seconds=elapsed,
            error_name=error_name,
            error_value=error_value,
            traceback=traceback,
            kernel_dead=self._kernel_dead,
        )
        self._paused.set()
        self.bus.fan_out_persisted(event)

    async def _handle_cancelled_execution(
        self, index: int, attempt: int, started: float, error: Exception
    ) -> str:
        await self._drain_output_tasks()
        await self._record_interrupted_cell(
            index,
            attempt,
            time.monotonic() - started,
            error_name=type(error).__name__,
            error_value=str(error),
            traceback=[str(error)],
        )
        return "cancel"

    async def _execute_client_cell(
        self, cell: NotebookNode, index: int
    ) -> NotebookNode:
        assert self.client is not None
        execution = asyncio.ensure_future(
            self.client.async_execute_cell(
                cell,
                index,
                execution_count=self.client.code_cells_executed + 1,
            )
        )
        abandon = asyncio.create_task(self._cancel_abandon_execution.wait())
        self._cell_execution_task = execution
        try:
            done, _pending = await asyncio.wait(
                {execution, abandon}, return_when=asyncio.FIRST_COMPLETED
            )
            if abandon in done and not execution.done():
                execution.cancel()
                execution.add_done_callback(self._consume_execution_result)
                raise _CancellationEscalated(
                    "Kernel execution did not stop after cancellation escalation"
                )
            return await execution
        except asyncio.CancelledError:
            execution.cancel()
            await asyncio.gather(execution, return_exceptions=True)
            raise
        finally:
            abandon.cancel()
            await asyncio.gather(abandon, return_exceptions=True)
            if self._cell_execution_task is execution:
                self._cell_execution_task = None

    @staticmethod
    def _consume_execution_result(execution: asyncio.Future[NotebookNode]) -> None:
        try:
            execution.result()
        except (asyncio.CancelledError, Exception):
            return

    async def _record_interrupted_cell(
        self,
        index: int,
        attempt: int,
        elapsed_seconds: float,
        *,
        error_name: str,
        error_value: str,
        traceback: list[str],
    ) -> None:
        self.store.complete_cell(
            self.run_id,
            index,
            status=CellStatus.INTERRUPTED,
            elapsed_seconds=elapsed_seconds,
            error_name=error_name,
            error_value=error_value,
            traceback=traceback,
        )
        await self._publish_cancellation_event_safely(
            "cell.interrupted",
            {
                "cell_index": index,
                "attempt": attempt,
                "elapsed_seconds": elapsed_seconds,
                "error_name": error_name,
                "error_value": error_value,
                "kernel_epoch": self.kernel_epoch,
                "kernel_state_lost": self._kernel_state_lost,
            },
        )

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

    def _on_output(self, output: NotebookNode, cell_index: int, attempt: int) -> bool:
        structured_payloads = self._structured_payloads(output)
        keep_output = self._strip_fallback_protocol_lines(output)
        if keep_output and not self._is_hidden_structured_output(output):
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
        return keep_output

    @staticmethod
    def _strip_fallback_protocol_lines(output: NotebookNode) -> bool:
        """Remove valid fallback envelopes while preserving neighboring stream text."""

        if output.get("output_type") != "stream":
            return True
        original = str(output.get("text", ""))
        retained: list[str] = []
        removed = False
        for line in original.splitlines(keepends=True):
            candidate = line.rstrip("\r\n")
            if NotebookRunner._fallback_payload(candidate) is None:
                retained.append(line)
            else:
                removed = True
        if not removed:
            return True
        output["text"] = "".join(retained)
        return bool(output["text"])

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
        self._checkpoint_requested_generation += 1
        self._checkpoint_event.set()

    async def _checkpoint_loop(self) -> None:
        delay_before_attempt = True
        while True:
            await self._checkpoint_event.wait()
            if delay_before_attempt:
                await asyncio.sleep(self.settings.checkpoint_interval_seconds)
            try:
                await self._checkpoint(force=False)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                self._checkpoint_consecutive_failures += 1
                retry_seconds = min(
                    _CHECKPOINT_RETRY_MAX_SECONDS,
                    max(0.1, self.settings.checkpoint_interval_seconds)
                    * (2 ** min(self._checkpoint_consecutive_failures - 1, 8)),
                )
                await self._publish_checkpoint_diagnostic_safely(
                    "notebook.checkpoint_failed",
                    {
                        "attempt": self._checkpoint_consecutive_failures,
                        "error_type": type(error).__name__,
                        "error": str(error),
                        "retry_seconds": retry_seconds,
                        "requested_generation": (self._checkpoint_requested_generation),
                        "persisted_generation": (self._checkpoint_persisted_generation),
                    },
                )
                await asyncio.sleep(retry_seconds)
                delay_before_attempt = False
            else:
                if self._checkpoint_consecutive_failures:
                    await self._publish_checkpoint_diagnostic_safely(
                        "notebook.checkpoint_recovered",
                        {
                            "failed_attempts": self._checkpoint_consecutive_failures,
                            "persisted_generation": (
                                self._checkpoint_persisted_generation
                            ),
                        },
                    )
                    self._checkpoint_consecutive_failures = 0
                delay_before_attempt = (
                    self._checkpoint_persisted_generation
                    >= self._checkpoint_requested_generation
                )

    async def _checkpoint(self, *, force: bool, writeback: bool = False) -> None:
        if (
            not force
            and self._checkpoint_persisted_generation
            >= self._checkpoint_requested_generation
        ):
            return
        async with self._checkpoint_lock:
            target_generation = self._checkpoint_requested_generation
            if not force and self._checkpoint_persisted_generation >= target_generation:
                return
            # nbclient mutates the notebook on the event-loop thread. Serialize before
            # crossing into a worker so that worker I/O sees an immutable generation.
            snapshot = _notebook_bytes(self.notebook)
            await asyncio.to_thread(
                self._write_notebook_bytes_atomic,
                snapshot,
                self.partial_output_path,
            )
            if writeback:
                await asyncio.to_thread(self._write_back_notebook_bytes, snapshot)
            self._checkpoint_persisted_generation = max(
                self._checkpoint_persisted_generation, target_generation
            )
            # No await is allowed between the generation comparison and clear. A new
            # output callback therefore either precedes this check or remains pending.
            if (
                self._checkpoint_persisted_generation
                >= self._checkpoint_requested_generation
            ):
                self._checkpoint_event.clear()
            else:
                self._checkpoint_event.set()

    async def _publish_checkpoint_diagnostic_safely(
        self, event_type: str, payload: dict[str, Any]
    ) -> None:
        try:
            await self.bus.publish(event_type, payload)
        except asyncio.CancelledError:
            raise
        except Exception:
            # A diagnostic failure must not terminate the retry worker. A later forced
            # checkpoint remains authoritative and still propagates its own failure.
            return

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

    def _write_back_notebook_bytes(self, snapshot: bytes) -> None:
        """Best-effort compare-and-swap the original notebook.

        Portable filesystems do not expose an atomic content-compare-and-replace. The
        destination is fingerprinted, the replacement is fully written and fsynced,
        and the fingerprint is checked again immediately before ``os.replace``. This
        narrows the unavoidable final check/rename window while refusing every change
        that can be observed portably.
        """

        try:
            current = _file_fingerprint(self.notebook_path)
        except FileNotFoundError as error:
            raise RuntimeError(
                f"Original notebook disappeared during execution: {self.notebook_path}"
            ) from error
        snapshot_hash = hashlib.sha256(snapshot).hexdigest()
        if current.digest != self._expected_notebook_hash:
            if current.digest == snapshot_hash:
                self._record_writeback_hash(current.digest)
                return
            raise RuntimeError(
                "Original notebook changed outside Runwatch during execution; "
                f"refusing to overwrite {self.notebook_path}. The executed state "
                f"is preserved at {self.partial_output_path}."
            )

        def verify_unchanged() -> None:
            try:
                latest = _file_fingerprint(self.notebook_path)
            except FileNotFoundError as error:
                raise RuntimeError(
                    "Original notebook disappeared while Runwatch prepared write-back: "
                    f"{self.notebook_path}"
                ) from error
            if latest != current:
                raise RuntimeError(
                    "Original notebook changed while Runwatch prepared write-back; "
                    f"refusing to overwrite {self.notebook_path}. The executed state "
                    f"is preserved at {self.partial_output_path}."
                )

        atomic_write_bytes(
            self.notebook_path,
            snapshot,
            before_replace=verify_unchanged,
        )
        self._record_writeback_hash(snapshot_hash)

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
        ).encode("utf-8")
        atomic_write_bytes(self.writeback_state_path, payload)

    @staticmethod
    def _write_notebook_atomic(notebook: NotebookNode, path: Path) -> None:
        write_notebook_atomic(notebook, path)

    @staticmethod
    def _write_notebook_bytes_atomic(snapshot: bytes, path: Path) -> None:
        atomic_write_bytes(path, snapshot)
