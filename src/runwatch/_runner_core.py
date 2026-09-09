"""Shared durable notebook lifecycle; engines supply execution and serialization."""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import os
import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, cast

import nbformat
from nbformat import NotebookNode

from ._eta import CacheAwareBayesianETA
from ._fs import atomic_write_bytes
from .emit import EVENT_MIME_TYPE, FALLBACK_PREFIX, RESOURCE_MIME_TYPE
from .events import EventBus
from .models import (
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
_CHECKPOINT_RETRY_MAX_SECONDS = 30.0
_WRITEBACK_STATE_FILENAME = "writeback-state.json"
_EVENT_TEXT_MAX_CHARS = 120
_EVENT_RESOURCE_ID_MAX_CHARS = 64
_EVENT_RESOURCE_SAMPLE_SIZE = 5


def _with_bayesian_tqdm_eta(
    progress: ProgressEvent,
    estimators: dict[str, CacheAwareBayesianETA],
) -> ProgressEvent:
    metrics = progress.metrics
    elapsed = metrics.get("elapsed_seconds")
    progress_id = metrics.get("progress_id")
    if (
        metrics.get("source") not in {"tqdm", "marimo"}
        or not isinstance(progress_id, str)
        or not isinstance(elapsed, (int, float))
    ):
        return progress
    eta, status, change_probability = estimators.setdefault(
        progress_id, CacheAwareBayesianETA()
    ).update(progress.completed, progress.total, float(elapsed))
    updated_metrics = {**metrics, "bayesian_finish_status": status}
    if eta:
        observed_at = datetime.now(timezone.utc)
        updated_metrics["bayesian_finish_times"] = {
            key: (observed_at + timedelta(seconds=seconds)).isoformat()
            for key, seconds in eta.items()
        }
    if change_probability is not None:
        updated_metrics["bayesian_change_probability"] = change_probability
    return progress.model_copy(update={"metrics": updated_metrics})


@dataclass(frozen=True)
class _FileFingerprint:
    digest: str
    device: int
    inode: int
    size: int
    modified_ns: int


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


def strip_ansi(value: str) -> str:
    return _ANSI_RE.sub("", value)


def source_digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def cell_label(notebook: NotebookNode, index: int) -> str:
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


def write_notebook_atomic(notebook: NotebookNode, path: Path) -> None:
    """Atomically write a notebook, preserving an existing destination's mode."""

    atomic_write_bytes(path, _notebook_bytes(notebook))


def cell_records(notebook: NotebookNode) -> list[dict[str, Any]]:
    return [
        {
            "cell_index": index,
            "cell_id": cell.id,
            "cell_type": cell.cell_type,
            "label": cell_label(notebook, index),
            "source": str(cell.source),
            "source_hash": source_digest(str(cell.source)),
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
                strip_ansi(str(line)) for line in output.get("traceback", [])[-30:]
            ],
        }
    data = cast(dict[str, Any], output.get("data", {}))
    text = data.get("text/plain")
    return {
        "output_type": output_type,
        "mime_types": sorted(data),
        "text": str(text)[-max_chars:] if text is not None else None,
    }


class RunnerCore:
    async def _run_kernel_epochs(self) -> None:
        raise NotImplementedError

    async def _escalate_cancellation(self) -> None:
        raise NotImplementedError

    def _read_source(self) -> NotebookNode:
        raise NotImplementedError

    @staticmethod
    def _validate_start_index(notebook: NotebookNode, from_cell: int) -> None:
        if from_cell < 0 or (from_cell > 0 and from_cell >= len(notebook.cells)):
            raise ValueError("from-cell is outside the notebook")

    def _serialize_checkpoint(self) -> bytes:
        return _notebook_bytes(self.notebook)

    def _writeback_bytes(self, snapshot: bytes) -> bytes:
        return snapshot

    def _atomic_write(
        self,
        path: Path,
        payload: bytes,
        *,
        before_replace: Callable[[], None] | None = None,
    ) -> None:
        atomic_write_bytes(path, payload, before_replace=before_replace)

    async def _publish_final_output(self) -> None:
        snapshot = self._serialize_checkpoint()
        await asyncio.to_thread(
            self._write_notebook_bytes_atomic, snapshot, self.output_path
        )

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
        initial_from_cell: int | None = None,
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
        self._validate_start_index(self.notebook, initial_from_cell or 0)
        self.current_cell_index: int | None = None
        self.current_attempt: int | None = None
        self.failed_cell_index: int | None = None
        self.failed_attempt: int | None = None
        self.kernel_epoch = 0
        self.command_queue: asyncio.Queue[RunnerCommand] = asyncio.Queue()
        self._pending_output_tasks: set[asyncio.Task[Any]] = set()
        self._bayesian_tqdm_etas: dict[str, CacheAwareBayesianETA] = {}
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
        self._process_stop_task: asyncio.Task[None] | None = None
        self._process_stop_requested = asyncio.Event()
        self._cancel_command_enqueued = False
        self._cancel_abandon_execution = asyncio.Event()
        self._running_cell = False
        self._cell_stopped = asyncio.Event()
        self._cell_stopped.set()
        self._cell_execution_task: asyncio.Future[Any] | None = None
        self._kernel_dead = False
        self._kernel_state_lost = any(
            event["type"] == "notebook.kernel_state_lost"
            for event in self.store.recent_events(self.run_id, limit=1_000)
        )
        self._initial_from_cell = initial_from_cell or 0
        self._selected_from_cell = initial_from_cell
        self._resume_existing = not initialize_cells
        self.partial_output_path = run_dir / "executed.partial.ipynb"
        self.writeback_state_path = run_dir / _WRITEBACK_STATE_FILENAME
        self._expected_notebook_hash = self._load_writeback_hash()
        if initialize_cells:
            self.store.initialize_cells(run_id, cell_records(self.notebook))

    @property
    def paused(self) -> bool:
        return self._paused.is_set()

    async def wait_until_paused(self) -> None:
        await self._paused.wait()

    async def wait_finished(self) -> None:
        await self._finished.wait()

    async def enqueue(self, command: RunnerCommand) -> None:
        await self.command_queue.put(command)

    @property
    def process_stop_requested(self) -> bool:
        return self._process_stop_requested.is_set()

    def request_process_stop(self) -> asyncio.Task[None]:
        """Convert a process signal into Runwatch's durable cancellation flow."""

        self._process_stop_requested.set()
        current = self._process_stop_task
        if current is None or current.done():
            current = asyncio.create_task(
                self.cancel(), name=f"process-stop:{self.run_id}"
            )
            self._process_stop_task = current
        return current

    def _cell_execution_active(self) -> bool:
        execution = self._cell_execution_task
        return execution is not None and not execution.done()

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
        await self._publish_final_output()
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
                    progress = _with_bayesian_tqdm_eta(
                        progress,
                        self._bayesian_tqdm_etas,
                    )
                    self._schedule(
                        self.bus.publish(
                            "notebook.progress",
                            {
                                "cell_index": cell_index,
                                "attempt": attempt,
                                **progress.model_dump(mode="json"),
                                **self._progress_metadata(cell_index),
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

    def _progress_metadata(self, cell_index: int) -> dict[str, Any]:
        return {"kernel_epoch": self.kernel_epoch}

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
            if RunnerCore._fallback_payload(candidate) is None:
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
        values = RunnerCore._mime_payloads(data)
        if output.get("output_type") != "stream":
            return values
        for line in str(output.get("text", "")).splitlines():
            fallback = RunnerCore._fallback_payload(line)
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
            snapshot = self._serialize_checkpoint()
            await asyncio.to_thread(
                self._write_notebook_bytes_atomic,
                snapshot,
                self.partial_output_path,
            )
            if writeback:
                await asyncio.to_thread(
                    self._write_back_notebook_bytes, self._writeback_bytes(snapshot)
                )
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

        self._atomic_write(
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
