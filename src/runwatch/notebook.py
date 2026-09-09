from __future__ import annotations

import asyncio
import atexit
import inspect
import re
import time
from collections.abc import AsyncGenerator, Callable
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, cast
from uuid import uuid4

import nbformat
from jupyter_client.connect import KernelConnectionInfo
from jupyter_client.manager import AsyncKernelManager
from nbclient import NotebookClient
from nbclient.exceptions import CellExecutionError, CellTimeoutError, DeadKernelError
from nbclient.util import run_hook, run_sync
from nbformat import NotebookNode
from traitlets.config import Config

from ._fs import atomic_write_bytes
from ._jupyter_document import JupyterDocument
from ._runner_core import (
    RunnerCore,
    cell_label,
    cell_records,
    source_digest,
    strip_ansi,
)
from ._runner_core import summarize_output as summarize_output
from ._runner_core import write_notebook_atomic as write_notebook_atomic
from ._tqdm import tqdm_bootstrap_code
from .emit import EVENT_MIME_TYPE, RESOURCE_MIME_TYPE
from .emit import FALLBACK_PREFIX as FALLBACK_PREFIX
from .models import (
    ActionStatus,
    CellStatus,
    RunStatus,
)
from .storage import source_hash

_ANSI_RE = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
_OUTPUT_PERSIST_INTERVAL_SECONDS = 0.25
_TIMEOUT_RECOVERY_MAX_SECONDS = 10.0
_CHECKPOINT_RETRY_MAX_SECONDS = 30.0
_WRITEBACK_STATE_FILENAME = "writeback-state.json"
_EVENT_TEXT_MAX_CHARS = 120
_EVENT_RESOURCE_ID_MAX_CHARS = 64
_EVENT_RESOURCE_SAMPLE_SIZE = 5
_ATX_HEADING_RE = re.compile(r"^[ \\t]{0,3}(#{1,6})[ \\t]+(.+?)[ \\t]*$")
_SETEXT_HEADING_RE = re.compile(r"^[ \\t]{0,3}(=+|-+)[ \\t]*$")


_cell_label = cell_label


class _CancellationEscalated(RuntimeError):
    pass


def _markdown_headings(source: str) -> list[tuple[int, str]]:
    headings: list[tuple[int, str]] = []
    lines = source.splitlines()
    for index, line in enumerate(lines):
        atx = _ATX_HEADING_RE.match(line)
        if atx is not None:
            title = re.sub(r"[ \\t]+#+[ \\t]*$", "", atx.group(2)).strip()
            if title:
                headings.append((len(atx.group(1)), title))
            continue
        if index == 0:
            continue
        setext = _SETEXT_HEADING_RE.match(line)
        title = lines[index - 1].strip()
        if setext is not None and title:
            headings.append((1 if setext.group(1).startswith("=") else 2, title))
    return headings


def _section_heading_between(
    notebook: NotebookNode, previous_code_index: int, next_code_index: int
) -> tuple[int, str] | None:
    latest: tuple[int, str] | None = None
    for cell in notebook.cells[previous_code_index + 1 : next_code_index]:
        if cell.cell_type != "markdown":
            continue
        headings = _markdown_headings(str(cell.source))
        if headings:
            latest = headings[-1]
    if latest is None:
        return None
    level, title = latest
    return level, title[:_EVENT_TEXT_MAX_CHARS]


def ensure_cell_ids(notebook: NotebookNode) -> None:
    for cell in notebook.cells:
        if not cell.get("id"):
            cell["id"] = uuid4().hex[:12]


def clear_notebook_outputs(notebook: NotebookNode) -> None:
    for cell in notebook.cells:
        if cell.cell_type == "code":
            cell.outputs = []
            cell.execution_count = None


class _CurveAsyncKernelManager(AsyncKernelManager):
    """Create clients with the binary Curve keys expected by pyzmq.

    jupyter_client 8.9 serializes these keys as text in connection dictionaries,
    while its kernel client traits still require bytes. Keep the compatibility
    conversion at the manager boundary until the upstream types and behavior agree.
    """

    def get_connection_info(self, session: bool = False) -> KernelConnectionInfo:
        info = super().get_connection_info(session=session)
        if not session:
            return info
        client_info = cast(dict[str, Any], info)
        for name in ("curve_publickey", "curve_secretkey"):
            value = client_info.get(name)
            if isinstance(value, str):
                client_info[name] = value.encode("ascii")
        return info


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

    @asynccontextmanager
    async def async_setup_kernel(self, **kwargs: Any) -> AsyncGenerator[None]:
        """Own kernel cleanup without nbclient's competing process signals.

        Runwatch coordinates SIGINT and SIGTERM at the supervisor boundary so an
        active cell can settle before its ZeroMQ channels are closed.  nbclient's
        default context manager installs handlers that start cleanup immediately,
        which can race an in-flight execute reply.
        """

        cleanup_kc = kwargs.pop("cleanup_kc", self.owns_km)
        if self.km is None:
            self.km = self.create_kernel_manager()

        atexit.register(self._cleanup_kernel)
        if not self.km.has_kernel:
            await self.async_start_new_kernel(**kwargs)
        if self.kc is None:
            await self.async_start_new_kernel_client()

        try:
            yield
        except RuntimeError:
            await run_hook(self.on_notebook_error, notebook=self.nb)
            raise
        finally:
            if cleanup_kc:
                await self._async_cleanup_kernel()
            await run_hook(self.on_notebook_complete, notebook=self.nb)
            atexit.unregister(self._cleanup_kernel)

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


class NotebookRunner(RunnerCore):
    client: MonitoredNotebookClient | None = None

    def _atomic_write(
        self,
        path: Path,
        payload: bytes,
        *,
        before_replace: Callable[[], None] | None = None,
    ) -> None:
        atomic_write_bytes(path, payload, before_replace=before_replace)

    async def shutdown(self) -> None:
        """Cancel and join runner-owned background work before services close."""

        async with self._cancel_lock:
            escalation = self._cancel_escalation_task
            self._cancel_escalation_task = None
        if escalation is not None and escalation is not asyncio.current_task():
            if not escalation.done():
                escalation.cancel()
            await asyncio.gather(escalation, return_exceptions=True)
        process_stop = self._process_stop_task
        if process_stop is not None and process_stop is not asyncio.current_task():
            await asyncio.gather(process_stop, return_exceptions=True)

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
        records = cell_records(self.notebook)
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
        previous_code_index: int | None = None
        while index < len(self.notebook.cells):
            if self._cancel_requested.is_set():
                return "cancel"
            cell = self.notebook.cells[index]
            self.current_cell_index = index
            if cell.cell_type != "code" or not str(cell.source).strip():
                self.store.mark_cell_skipped(self.run_id, index)
                index += 1
                continue
            if previous_code_index is not None:
                heading = _section_heading_between(
                    self.notebook, previous_code_index, index
                )
                if heading is not None:
                    heading_level, heading_text = heading
                    await self.bus.publish(
                        "notebook.section_started",
                        {
                            "heading": heading_text,
                            "heading_level": heading_level,
                            "cell_index": index,
                            "kernel_epoch": self.kernel_epoch,
                        },
                    )
            outcome = await self._execute_cell_until_resolved(index)
            if outcome != "next":
                return outcome
            previous_code_index = index
            index += 1
        return "complete"

    async def _execute_cell_until_resolved(self, index: int) -> str:
        while True:
            cell = self.notebook.cells[index]
            digest = source_digest(str(cell.source))
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
                    "label": cell_label(self.notebook, index),
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
        if (
            self._process_stop_requested.is_set()
            and not self._cancel_requested.is_set()
        ):
            await self.cancel()
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
                from_cell = command.from_cell or 0
                self._validate_start_index(fresh, from_cell)
                self._initial_from_cell = from_cell
                self._paused.clear()
                self.store.update_run_status(
                    self.run_id,
                    RunStatus.RESTARTING,
                    message=f"Restarting kernel and replaying from cell {from_cell + 1}",
                )
                self.store.finish_action(
                    command.action_id,
                    ActionStatus.COMPLETED,
                    message="Kernel restart accepted",
                    result={"from_cell": from_cell},
                )
                return "restart"
            except Exception as error:
                self.store.finish_action(
                    command.action_id, ActionStatus.REJECTED, message=str(error)
                )
                await self._publish_action_rejection_safely(command, error)

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
        JupyterDocument(self.notebook).validate_resume(
            JupyterDocument(fresh), failed_index
        )

    def _apply_fresh_sources(self, fresh: NotebookNode, from_index: int) -> None:
        for index in range(from_index, len(self.notebook.cells)):
            self.notebook.cells[index].source = fresh.cells[index].source
            self.notebook.cells[index].metadata = fresh.cells[index].metadata
        failed = self.notebook.cells[from_index]
        failed.outputs = []
        failed.execution_count = None
        self.store.update_source(
            self.run_id, source_hash(self.source_path), cell_records(fresh)
        )

    def _read_source(self) -> NotebookNode:
        notebook = JupyterDocument.read(self.source_path).view()
        ensure_cell_ids(notebook)
        clear_notebook_outputs(notebook)
        return notebook

    def _make_client(self) -> MonitoredNotebookClient:
        kwargs: dict[str, Any] = {
            "output_callback": self._on_output,
            "timeout": self.settings.timeout_seconds,
            "startup_timeout": self.settings.startup_timeout_seconds,
            "resources": {"metadata": {"path": str(self.working_dir)}},
            "allow_errors": False,
            "record_timing": True,
            "store_widget_state": True,
            "config": Config({"KernelManager": {"transport_encryption": "required"}}),
            "kernel_manager_class": _CurveAsyncKernelManager,
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
                        strip_ansi(str(line)) for line in output.get("traceback", [])
                    ]
                    return type(error).__name__, str(error), traceback
            return type(error).__name__, str(error), [strip_ansi(str(error))]
        for output in reversed(cell.get("outputs", [])):
            if output.get("output_type") == "error":
                return (
                    str(output.get("ename", type(error).__name__)),
                    str(output.get("evalue", error)),
                    [strip_ansi(str(line)) for line in output.get("traceback", [])],
                )
        return type(error).__name__, str(error), [strip_ansi(str(error))]
