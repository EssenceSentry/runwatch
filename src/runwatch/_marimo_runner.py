"""Marimo graph execution using Runwatch's shared durable run lifecycle."""

from __future__ import annotations

import ast
import asyncio
import json
import signal
import time
from pathlib import Path
from typing import Any, cast

from nbformat import NotebookNode

from ._fs import atomic_write_bytes
from ._marimo_code import STOP_BOOTSTRAP, execute_cells, native_progress_bootstrap
from ._marimo_document import MarimoDocument
from ._marimo_output import MarimoOutputs
from ._marimo_session import MarimoSession, MarimoTransportError
from ._marimo_snapshot import render_snapshot
from ._runner_core import RunnerCore, source_digest
from ._tqdm import tqdm_bootstrap_code
from .models import ActionStatus, CellStatus, RunnerCommand, RunStatus
from .storage import process_start_time, source_hash


class MarimoRunner(RunnerCore):
    def __init__(self, **kwargs: Any) -> None:
        self.document: MarimoDocument
        super().__init__(**kwargs)
        self.partial_output_path = self.run_dir / "executed.partial.session.json"
        self.session: MarimoSession | None = None
        self._publication_lock = asyncio.Lock()
        self.outputs = MarimoOutputs()
        self.attempts: dict[str, int] = {}
        self.statuses: dict[str, str] = {}
        self.errors: dict[str, list[dict[str, Any]]] = {}
        self.skip_reasons: dict[str, str] = {}
        self.active: dict[str, float] = {}
        self.elapsed: dict[str, float] = {}
        self._settled_attempts: set[tuple[str, int, str]] = set()
        self.modules: dict[str, str] = {}
        self.generation = 0
        self.batch = 0
        self._selected: set[str] = set()
        self._sections: set[str] = set()
        self._timed_out: set[str] = set()
        self._runtime_error: str | None = None

    def _read_source(self) -> NotebookNode:
        self.document = MarimoDocument.read(
            self.source_path, self.settings, self.working_dir
        )
        return self.document.view()

    async def _run_kernel_epochs(self) -> None:
        outcome = "restart"
        while outcome == "restart" and not self._cancel_requested.is_set():
            self.notebook = self._read_source()
            self._selected = self.document.selection(self._selected_from_cell)
            self.kernel_epoch = self.store.begin_kernel_epoch(self.run_id)
            self.generation += 1
            self.store.replace_cells_for_restart(
                self.run_id, self.document.records(), self.kernel_epoch, 0
            )
            self.store.update_source(
                self.run_id, source_hash(self.source_path), self.document.records()
            )
            self.attempts.clear()
            self.statuses = {
                c["id"]: "pending" if c["id"] in self._selected else "not_replayed"
                for c in self.document.cells
            }
            self.elapsed.clear()
            self._settled_attempts.clear()
            self.skip_reasons.clear()
            self.errors.clear()
            self.active.clear()
            self.outputs = MarimoOutputs()
            self._kernel_dead = False
            self.session = MarimoSession(
                self.run_dir, self.working_dir, self.settings, self._on_message
            )
            try:
                await asyncio.wait_for(
                    self._start_runtime(), self.settings.startup_timeout_seconds
                )
                outcome = await self._execute_graph()
            finally:
                await self.session.close()
                await self.bus.publish(
                    "notebook.kernel_stopped", {"kernel_epoch": self.kernel_epoch}
                )

    async def _start_runtime(self) -> None:
        assert self.session is not None
        inventory = await self.session.start(self.document.source)
        if [c["id"] for c in inventory["cells"]] != [
            c["id"] for c in self.document.cells
        ]:
            raise RuntimeError("Marimo cell identity differs from the validated source")
        pid = int(inventory["pid"])
        self.store.update_kernel(
            self.run_id, kernel_id=f"marimo:{self.kernel_epoch}", kernel_pid=pid
        )
        self._persist_notebook_state(kernel_started_at=process_start_time(pid))
        await self.bus.publish(
            "notebook.kernel_started",
            {"kernel_epoch": self.kernel_epoch, "backend": "marimo"},
        )
        success, _ = await self.session.execute(
            "import sys as _rw_sys; _rw_sys.path.insert(0, "
            + repr(str(self.working_dir))
            + ")\n"
            + STOP_BOOTSTRAP
        )
        if not success:
            raise RuntimeError("Marimo stop instrumentation is incompatible")
        await self._install_progress()

    async def _install_progress(self) -> None:
        assert self.session is not None
        codes: list[str] = []
        if self.settings.capture_tqdm:
            codes.append(
                tqdm_bootstrap_code(
                    self.settings.tqdm_min_interval_seconds, stream=True
                )
            )
        if self.settings.marimo.capture_native_progress:
            codes.append(
                native_progress_bootstrap(self.settings.tqdm_min_interval_seconds)
            )
        for code in codes:
            success, _ = await self.session.execute(code)
            if not success:
                await self.bus.publish(
                    "notebook.tqdm_instrumentation_failed",
                    {
                        "kernel_epoch": self.kernel_epoch,
                        "error": "Marimo rejected progress instrumentation",
                    },
                )

    async def _execute_graph(self) -> str:
        targets = set(self._selected)
        while not self._cancel_requested.is_set():
            await self._run_batch(targets)
            if self._cancel_requested.is_set():
                return "cancel"
            failed = [
                c["id"]
                for c in self.document.cells
                if self.statuses.get(c["id"])
                in {"failed", "interrupted", "blocked", "stale"}
                and c["id"] in self._selected
            ]
            if not failed and self._runtime_error is None:
                return "complete"
            await self._pause_graph(failed)
            outcome = await self._recover_graph()
            if outcome != "resume":
                return outcome
            targets = {
                cid
                for cid in self._selected
                if self.statuses.get(cid)
                in {"failed", "interrupted", "blocked", "stale", "pending"}
            }
        return "cancel"

    async def _run_batch(self, targets: set[str]) -> None:
        assert self.session is not None
        self.batch += 1
        self._sections.clear()
        self._timed_out.clear()
        self._runtime_error = None
        self._paused.clear()
        self.store.update_run_status(
            self.run_id,
            RunStatus.RUNNING,
            message="Executing Marimo graph",
            failed_cell_index=None,
            failed_attempt=None,
        )
        self._running_cell = True
        self._cell_stopped.clear()
        execution = asyncio.create_task(
            self.session.execute(execute_cells(sorted(targets)))
        )
        self._cell_execution_task = execution
        try:
            await self._watch_execution(execution)
            await execution
            inventory = await self.session.inventory()
            self.modules = inventory["modules"]
            await self._reconcile(inventory["cells"])
        except asyncio.CancelledError:
            if not self._cancel_requested.is_set():
                raise
            self._kernel_dead = True
            self._interrupt_active()
        except (MarimoTransportError, TimeoutError, OSError) as error:
            self._runtime_error = str(error)
            self._kernel_dead = True
            self._interrupt_active()
            await self.session.signal(signal.SIGKILL)
        finally:
            if not execution.done():
                execution.cancel()
            await asyncio.gather(execution, return_exceptions=True)
            self._running_cell = False
            self._cell_stopped.set()
            await self._drain_output_tasks()
            self._persist_notebook_state()
            await self._checkpoint(force=True, writeback=True)

    async def _watch_execution(self, execution: asyncio.Task[tuple[bool, str]]) -> None:
        assert self.session is not None
        while not execution.done():
            if not self.session.kernel_is_alive():
                raise MarimoTransportError(
                    "Marimo kernel exited; explicit reconstruction is required"
                )
            if self.session.reader is not None and self.session.reader.done():
                error = self.session.reader.exception()
                raise MarimoTransportError(
                    f"Marimo monitoring stopped ({type(error).__name__})"
                )
            expired = self._expired_cells()
            if expired:
                self._timed_out.update(expired)
                await self.session.interrupt()
                try:
                    await asyncio.wait_for(
                        asyncio.shield(execution),
                        max(0.1, self.settings.cancel_interrupt_grace_seconds),
                    )
                except TimeoutError:
                    await self._mark_kernel_state_lost("timeout")
                    raise MarimoTransportError(
                        "Timed-out Marimo kernel could not be synchronized"
                    ) from None
            await asyncio.wait({execution}, timeout=0.05)

    def _expired_cells(self) -> set[str]:
        timeout = self.settings.timeout_seconds
        if timeout is None:
            return set()
        now = time.monotonic()
        return {cid for cid, started in self.active.items() if now - started >= timeout}

    async def _on_message(self, message: dict[str, Any]) -> None:
        if message.get("op") != "cell-op":
            return
        value = message["data"]
        cid = value.get("cell_id")
        index = next(
            (i for i, cell in enumerate(self.document.cells) if cell["id"] == cid), None
        )
        if index is None:
            return
        if value.get("status") == "running":
            await self._begin_attempt(cid, index)
        console = value.get("console")
        items: list[Any] = (
            cast(list[Any], console) if isinstance(console, list) else [console]
        )
        for item in items:
            if item is not None:
                await self._accept_output(cid, index, item, console=True)
        if value.get("output") is not None:
            await self._accept_output(cid, index, value["output"], console=False)
        if value.get("status") == "idle" and cid in self.active:
            self.elapsed[cid] = time.monotonic() - self.active.pop(cid)
            self.statuses[cid] = "failed" if self.errors.get(cid) else "settling"
            self._store_cell(index, cid, CellStatus(self.statuses[cid]))
            for output in self.outputs.flush(cid):
                self._accept_normalized(cid, index, output)
            self._request_checkpoint()

    async def _begin_attempt(self, cid: str, index: int) -> None:
        code = self.document.cells[index]["code"]
        attempt = self.store.begin_cell_attempt(
            self.run_id, index, code, source_digest(code), self.kernel_epoch
        )
        self.attempts[cid] = attempt
        self.outputs.attempts[cid] = attempt
        self.active[cid] = time.monotonic()
        self.statuses[cid] = "running"
        self.errors.pop(cid, None)
        self.notebook.cells[index].outputs = []
        self.current_cell_index, self.current_attempt = index, attempt
        self.store.update_run_status(
            self.run_id, RunStatus.RUNNING, current_cell_index=index
        )
        await self._announce_section(index)
        await self.bus.publish(
            "cell.started",
            {
                "cell_index": index,
                "cell_id": cid,
                "attempt": attempt,
                "kernel_epoch": self.kernel_epoch,
            },
        )

    async def _accept_output(
        self, cid: str, index: int, item: dict[str, Any], *, console: bool
    ) -> None:
        if item.get("channel") == "marimo-error":
            self.errors[cid] = [
                {"type": "MarimoError", "message": str(item.get("data"))}
            ]
        if not console:
            # Native cell output replaces the previous display; streams remain.
            self.notebook.cells[index].outputs = [
                o
                for o in self.notebook.cells[index].outputs
                if o.get("output_type") == "stream"
            ]
        if (
            item.get("mimetype") in {"text/html", "image/svg+xml"}
            and isinstance(item.get("data"), str)
            and self.session is not None
        ):
            item = {**item, "data": await self.session.materialize(item["data"])}
        for output in self.outputs.normalize(cid, item):
            self._accept_normalized(cid, index, output)

    def _accept_normalized(self, cid: str, index: int, output: NotebookNode) -> None:
        attempt = self.attempts.get(cid, 0)
        if self._on_output(
            output, index, attempt
        ) and not self._is_hidden_structured_output(output):
            self.notebook.cells[index].outputs.append(output)

    def _progress_metadata(self, cell_index: int) -> dict[str, Any]:
        return {
            "kernel_epoch": self.kernel_epoch,
            "cell_id": self.document.cells[cell_index]["id"],
            "source_generation": self.generation,
            "execution_batch": self.batch,
        }

    async def _reconcile(self, cells: list[dict[str, Any]]) -> None:
        stopped = {
            c["id"]
            for c in cells
            if any(e["type"] == "MarimoStopError" for e in c["errors"])
        }
        for index, cell in enumerate(cells):
            cid = cell["id"]
            self.document.cells[index]["parents"] = cell["parents"]
            status = self._settled_status(cell, stopped)
            self.statuses[cid] = status.value
            self.errors[cid] = cell["errors"]
            if cid in self._timed_out:
                status = CellStatus.FAILED
                self.statuses[cid] = status.value
                self.errors[cid] = [
                    {
                        "type": "CellTimeoutError",
                        "message": "Marimo cell exceeded timeout_seconds",
                    }
                ]
            self._store_cell(index, cid, status)
            key = (cid, self.attempts.get(cid, 0), status.value)
            if cid in self.attempts and key not in self._settled_attempts:
                self._settled_attempts.add(key)
                await self.bus.publish(
                    (
                        "cell.completed"
                        if status == CellStatus.SUCCEEDED
                        else "cell.settled"
                    ),
                    {
                        "cell_index": index,
                        "attempt": self.attempts[cid],
                        "kernel_epoch": self.kernel_epoch,
                        "status": status.value,
                    },
                )
        self.active.clear()

    def _interrupt_active(self) -> None:
        for cid, started in self.active.items():
            self.elapsed[cid] = time.monotonic() - started
            self.statuses[cid] = "interrupted"
            index = next(i for i, c in enumerate(self.document.cells) if c["id"] == cid)
            self._store_cell(index, cid, CellStatus.INTERRUPTED)
        self.active.clear()

    def _store_cell(self, index: int, cid: str, status: CellStatus) -> None:
        errors = self.errors.get(cid, [])
        reason = None
        if status == CellStatus.SKIPPED:
            reason = self.skip_reasons.get(cid, "Disabled or empty cell")
        self.store.complete_cell(
            self.run_id,
            index,
            status=status,
            elapsed_seconds=self.elapsed.get(cid, 0),
            error_name=errors[0]["type"] if errors else None,
            error_value=errors[0]["message"] if errors else reason,
            traceback=[str(e["message"]) for e in errors],
        )

    def _settled_status(self, cell: dict[str, Any], stopped: set[str]) -> CellStatus:
        cid, status = cell["id"], cell["status"]
        if cid not in self._selected:
            self.skip_reasons[cid] = "Outside the selected replay graph"
            return CellStatus.NOT_REPLAYED
        disabled = {c["id"] for c in self.document.cells if c["config"].get("disabled")}
        if status == "disabled" or self._has_stopped_ancestor(cid, disabled):
            self.skip_reasons[cid] = "Disabled cell or disabled ancestor"
            return CellStatus.SKIPPED
        if cid in stopped or (
            status == "cancelled" and self._has_stopped_ancestor(cid, stopped)
        ):
            self.skip_reasons[cid] = "Intentional mo.stop branch"
            return CellStatus.SKIPPED
        self.skip_reasons.pop(cid, None)
        if status == "idle" and cell["errors"]:
            return CellStatus.FAILED
        return {
            "idle": CellStatus.SUCCEEDED,
            "None": CellStatus.SKIPPED,
            "cancelled": CellStatus.BLOCKED,
            "exception": CellStatus.FAILED,
            "marimo-error": CellStatus.FAILED,
            "interrupted": CellStatus.INTERRUPTED,
            "stale": CellStatus.STALE,
        }.get(status, CellStatus.PENDING)

    def _has_stopped_ancestor(self, cid: str, stopped: set[str]) -> bool:
        parents = {c["id"]: set(c["parents"]) for c in self.document.cells}
        pending = list(parents[cid])
        visited: set[str] = set()
        while pending:
            parent = pending.pop()
            if parent in stopped:
                return True
            if parent not in visited:
                visited.add(parent)
                pending.extend(parents.get(parent, ()))
        return False

    async def _pause_graph(self, failed: list[str]) -> None:
        primary = next(
            (cid for cid in failed if self.statuses[cid] in {"failed", "interrupted"}),
            failed[0] if failed else None,
        )
        index = next(
            (i for i, c in enumerate(self.document.cells) if c["id"] == primary), None
        )
        attempt = self.attempts.get(primary, 0) if primary is not None else None
        self.failed_cell_index, self.failed_attempt = index, attempt
        self.store.update_run_status(
            self.run_id,
            RunStatus.PAUSED,
            failed_cell_index=index,
            failed_attempt=attempt,
            current_cell_index=None,
            message=self._runtime_error
            or f"Marimo graph paused: {len(failed)} failed or blocked cell(s)",
        )
        await self.bus.publish(
            "cell.failed",
            {
                "cell_index": index,
                "attempt": attempt,
                "kernel_epoch": self.kernel_epoch,
                "error_name": "MarimoExecutionError",
            },
        )
        self._persist_notebook_state()
        await self._checkpoint(force=True, writeback=True)
        self._paused.set()

    async def _recover_graph(self) -> str:
        while True:
            command = await self.command_queue.get()
            if command.kind == "cancel":
                return "cancel"
            try:
                self._check_command(command)
                fresh = await asyncio.to_thread(
                    MarimoDocument.read,
                    self.source_path,
                    self.settings,
                    self.working_dir,
                )
                if command.kind == "restart" or self._kernel_dead:
                    fresh.selection(command.from_cell)
                    self._selected_from_cell = command.from_cell
                    outcome = "restart"
                else:
                    await self._apply_repair(fresh)
                    outcome = "resume"
                self._paused.clear()
                self.store.finish_action(
                    command.action_id,
                    ActionStatus.COMPLETED,
                    message=f"Marimo {outcome} accepted",
                )
                return outcome
            except Exception as error:
                self.store.finish_action(
                    command.action_id, ActionStatus.REJECTED, message=str(error)
                )
                await self._publish_action_rejection_safely(command, error)

    def _check_command(self, command: RunnerCommand) -> None:
        if (
            command.expected_kernel_epoch != self.kernel_epoch
            or command.expected_failed_attempt != self.failed_attempt
        ):
            raise RuntimeError("Kernel epoch or failed-cell attempt changed")
        if source_hash(self.source_path) != command.requested_source_hash:
            raise RuntimeError("source.py changed after recovery was requested")

    async def _apply_repair(self, fresh: MarimoDocument) -> None:
        assert self.session is not None
        self.document.validate_resume(fresh)
        for name, digest in self.modules.items():
            if not Path(name).is_file() or source_hash(Path(name)) != digest:
                raise RuntimeError(
                    "Imported Python source changed; restart is required"
                )
        edits = {
            old["id"]: new["code"]
            for old, new in zip(self.document.cells, fresh.cells, strict=True)
            if old["code"] != new["code"]
        }
        try:
            success, _ = await self.session.execute(execute_cells([], edits))
            if not success:
                raise MarimoTransportError("Marimo rejected the source repair")
            inventory = await self.session.inventory()
        except (MarimoTransportError, TimeoutError, OSError):
            self._kernel_dead = True
            await self.session.signal(signal.SIGKILL)
            raise
        self.document = fresh
        self.generation += 1
        for index, cell in enumerate(fresh.cells):
            self.notebook.cells[index].source = cell["code"]
        await self._reconcile(inventory["cells"])
        self.store.update_source(
            self.run_id, source_digest(fresh.source), fresh.records()
        )
        self._persist_notebook_state()

    async def _announce_section(self, index: int) -> None:
        headings: list[str] = []
        for cell in self.document.cells[: index + 1]:
            tree = ast.parse(cell["code"])
            for node in ast.walk(tree):
                if (
                    isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Attribute)
                    and node.func.attr == "md"
                    and node.args
                ):
                    argument = node.args[0]
                    if isinstance(argument, ast.Constant) and isinstance(
                        argument.value, str
                    ):
                        headings.extend(
                            line.strip()
                            for line in argument.value.splitlines()
                            if line.strip().startswith("#")
                        )
        if not headings or headings[-1] in self._sections:
            return
        heading = headings[-1]
        self._sections.add(heading)
        level = min(6, len(heading) - len(heading.lstrip("#")))
        await self.bus.publish(
            "notebook.section_started",
            {
                "heading": heading.lstrip("#").strip()[:120],
                "heading_level": level,
                "execution_batch": self.batch,
                "cell_index": index,
                "kernel_epoch": self.kernel_epoch,
            },
        )

    def _persist_notebook_state(self, **extra: Any) -> None:
        state = {
            "backend": "marimo",
            "version": self.document.version,
            "generation": self.generation,
            "batch": self.batch,
            "cells": [
                {
                    "id": c["id"],
                    "parents": c["parents"],
                    "skip_reason": self.skip_reasons.get(c["id"]),
                }
                for c in self.document.cells
            ],
            **extra,
        }
        self.store.update_notebook_state(self.run_id, state)

    def _serialize_checkpoint(self) -> bytes:
        cells: list[dict[str, Any]] = []
        for index, cell in enumerate(self.document.cells):
            cells.append(
                {
                    **cell,
                    "index": index,
                    "status": self.statuses.get(cell["id"], "pending"),
                    "skip_reason": self.skip_reasons.get(cell["id"]),
                    "attempt": self.attempts.get(cell["id"], 0),
                    "elapsed_seconds": self.elapsed.get(cell["id"], 0),
                    "errors": self.errors.get(cell["id"], []),
                    "outputs": self.notebook.cells[index].outputs,
                }
            )
        return json.dumps(
            {
                "schema_version": 1,
                "backend": "marimo",
                "source": self.document.source,
                "source_hash": source_digest(self.document.source),
                "kernel_epoch": self.kernel_epoch,
                "generation": self.generation,
                "version": self.document.version,
                "cells": cells,
            }
        ).encode()

    def _writeback_bytes(self, snapshot: bytes) -> bytes:
        return str(json.loads(snapshot)["source"]).encode()

    async def _checkpoint(self, *, force: bool, writeback: bool = False) -> None:
        await super()._checkpoint(force=force, writeback=writeback)
        if force:
            await self._publish_final_output()

    async def _publish_final_output(self) -> None:
        async with self._publication_lock:
            await self._publish_artifacts()

    async def _publish_artifacts(self) -> None:
        snapshot = self._serialize_checkpoint()
        html = await asyncio.to_thread(render_snapshot, snapshot)
        await asyncio.to_thread(
            atomic_write_bytes, self.output_path.with_suffix(".session.json"), snapshot
        )
        await asyncio.to_thread(atomic_write_bytes, self.output_path, html.encode())

    async def interrupt(self) -> bool:
        if self.session is None or not self._running_cell:
            return False
        await self.session.interrupt()
        await self.bus.publish(
            "notebook.interrupt_requested", {"kernel_epoch": self.kernel_epoch}
        )
        return True

    async def _escalate_cancellation(self) -> None:
        assert self.session is not None
        session = self.session
        stages = [
            (
                "interrupt",
                self.session.interrupt,
                self.settings.cancel_interrupt_grace_seconds,
            ),
            (
                "shutdown",
                lambda: session.signal(signal.SIGTERM),
                self.settings.cancel_shutdown_grace_seconds,
            ),
            (
                "terminate",
                lambda: session.signal(signal.SIGTERM),
                self.settings.cancel_terminate_grace_seconds,
            ),
            (
                "kill",
                lambda: session.signal(signal.SIGKILL),
                self.settings.cancel_kill_grace_seconds,
            ),
        ]
        for stage, action, grace in stages:
            if not self._cell_execution_active():
                return
            if stage != "interrupt":
                await self._mark_kernel_state_lost(stage)
            await self._run_cancellation_stage(stage, action, grace)
        if (
            self._cell_execution_task is not None
            and not self._cell_execution_task.done()
        ):
            self._cell_execution_task.cancel()

    async def shutdown(self) -> None:
        if self.session is not None:
            await self.session.close()
        if self._cancel_escalation_task is not None:
            await asyncio.gather(self._cancel_escalation_task, return_exceptions=True)
        await self._drain_output_tasks()
