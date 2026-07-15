from __future__ import annotations

import asyncio
import json
import os
import shutil
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import TYPE_CHECKING, Any, NoReturn
from uuid import uuid4

import nbformat

from ._fs import atomic_write_bytes, ensure_private_directory
from .events import EventBus
from .manifest import read_run_manifest
from .models import (
    ActionKind,
    ActionStatus,
    ResourceEvent,
    ResourceRegistration,
    RunnerCommand,
    RunStatus,
    RunwatchConfig,
)
from .notebook import (
    NotebookRunner,
    clear_notebook_outputs,
    ensure_cell_ids,
    write_notebook_atomic,
)
from .notifications import NotificationManager
from .resource_manager import ResourceManager, ResourceStopRejected
from .schema_versions import RUN_MANIFEST_SCHEMA_VERSION
from .storage import RunStore, controller_is_alive, process_start_time, source_hash

if TYPE_CHECKING:
    from .dashboard_links import DashboardLinkManager


class RunSupervisor:
    def __init__(
        self,
        *,
        notebook_path: Path,
        output_path: Path,
        working_dir: Path,
        run_dir: Path,
        config: RunwatchConfig,
        name: str | None = None,
        run_id: str | None = None,
        reopen: bool = False,
        initial_from_cell: int = 0,
        bootstrap_action_id: str | None = None,
        cleanup_on_success: bool = True,
    ) -> None:
        self.notebook_path = notebook_path.resolve()
        self.output_path = output_path.resolve()
        self.working_dir = working_dir.resolve()
        self.run_dir = run_dir.resolve()
        self.config = config
        self.name = name or notebook_path.stem
        self.run_id = run_id or str(uuid4())
        self.initial_from_cell = initial_from_cell
        self.cleanup_on_success = cleanup_on_success
        self.source_path = self.run_dir / "source.ipynb"
        self.input_snapshot_path = self.run_dir / "input.ipynb"
        self.partial_output_path = self.run_dir / "executed.partial.ipynb"
        self._validate_output_path_ownership()
        ensure_private_directory(self.run_dir)

        if not reopen:
            original = nbformat.read(self.notebook_path, as_version=4)
            ensure_cell_ids(original)
            write_notebook_atomic(original, self.input_snapshot_path)
            editable = nbformat.from_dict(original)
            clear_notebook_outputs(editable)
            write_notebook_atomic(editable, self.source_path)

        self.store = RunStore(
            self.run_dir / "runwatch.sqlite3",
            max_observations_per_resource=config.storage.max_observations_per_resource,
            max_observation_bytes_per_resource=(
                config.storage.max_observation_bytes_per_resource
            ),
            max_log_lines_per_resource=config.storage.max_log_lines_per_resource,
            max_log_bytes_per_resource=config.storage.max_log_bytes_per_resource,
            max_events_per_run=config.storage.max_events_per_run,
            max_event_bytes_per_run=config.storage.max_event_bytes_per_run,
            max_event_payload_bytes=config.storage.max_event_payload_bytes,
            max_resource_payload_bytes=config.storage.max_resource_payload_bytes,
            max_notification_record_bytes=(
                config.storage.max_notification_record_bytes
            ),
            max_delivery_error_bytes=config.storage.max_delivery_error_bytes,
        )
        if not reopen:
            self.store.initialize_run(
                run_id=self.run_id,
                name=self.name,
                notebook_path=self.notebook_path,
                source_path=self.source_path,
                output_path=self.output_path,
                working_dir=self.working_dir,
                run_dir=self.run_dir,
                source_digest=source_hash(self.source_path),
                metadata={"config": config.model_dump(mode="json")},
            )
            self._write_manifest()

        self.bus = EventBus(self.store, self.run_id)
        self.resources = ResourceManager(
            store=self.store,
            bus=self.bus,
            run_id=self.run_id,
            working_dir=self.working_dir,
            aws_settings=config.aws,
        )
        self.notifications = NotificationManager(
            settings=config.notifications,
            store=self.store,
            bus=self.bus,
            run_id=self.run_id,
        )
        self.runner = NotebookRunner(
            run_id=self.run_id,
            notebook_path=self.notebook_path,
            source_path=self.source_path,
            output_path=self.output_path,
            run_dir=self.run_dir,
            working_dir=self.working_dir,
            settings=config.notebook,
            store=self.store,
            bus=self.bus,
            resources=self.resources,
            initialize_cells=not reopen,
            initial_from_cell=initial_from_cell,
        )
        self._runner_task: asyncio.Task[RunStatus] | None = None
        self._action_task: asyncio.Task[None] | None = None
        self._action_task_termination_observed = False
        self._finalized = False
        self._wait_completed_normally = False
        self._quiesced = False
        self._quiesce_error: BaseException | None = None
        self._closed = False
        self._reopen = reopen
        self._bootstrap_action_id = bootstrap_action_id
        self._bootstrap_kind: ActionKind | None = None
        self._recovered_stop_pending = False
        self.controller_token = str(uuid4())
        self.controller_started_at = process_start_time(os.getpid())
        self._process_registered = False
        self._dashboard_links: DashboardLinkManager | None = None

    def attach_dashboard_links(self, manager: DashboardLinkManager) -> None:
        """Attach authenticated sharing for ``local.dashboard`` resources."""
        if self._dashboard_links is not None:
            raise RuntimeError("Dashboard links are already attached")
        self._dashboard_links = manager
        self.resources.attach_dashboard_links(manager)

    @property
    def wait_completed_normally(self) -> bool:
        """Whether runner and supervisor finalization both returned normally."""

        return self._wait_completed_normally

    def _validate_output_path_ownership(self) -> None:
        reserved = {
            self.notebook_path: "the original notebook",
            self.input_snapshot_path: "Runwatch's immutable input snapshot",
            self.source_path: "Runwatch's editable source notebook",
            self.partial_output_path: "Runwatch's partial execution checkpoint",
        }
        description = reserved.get(self.output_path)
        if description is not None:
            raise ValueError(
                f"Output path {self.output_path} would overwrite {description}; "
                "choose a separate output notebook"
            )

    @classmethod
    def reopen(
        cls,
        run_dir: Path,
        *,
        from_cell: int = 0,
        bootstrap_action_id: str | None = None,
    ) -> RunSupervisor:
        manifest = cls.read_manifest(run_dir)
        config = RunwatchConfig.model_validate(manifest["config"])
        return cls(
            notebook_path=Path(manifest["notebook_path"]),
            output_path=Path(manifest["output_path"]),
            working_dir=Path(manifest["working_dir"]),
            run_dir=run_dir,
            config=config,
            name=manifest["name"],
            run_id=manifest["run_id"],
            reopen=True,
            initial_from_cell=from_cell,
            bootstrap_action_id=bootstrap_action_id,
            cleanup_on_success=manifest["cleanup_on_success"],
        )

    @staticmethod
    def read_manifest(run_dir: Path) -> dict[str, Any]:
        path = run_dir.resolve() / "run-manifest.json"
        return read_run_manifest(path).model_dump(mode="json")

    def _write_manifest(self) -> None:
        path = self.run_dir / "run-manifest.json"
        payload = json.dumps(
            {
                "schema_version": RUN_MANIFEST_SCHEMA_VERSION,
                "run_id": self.run_id,
                "name": self.name,
                "notebook_path": str(self.notebook_path),
                "source_path": str(self.source_path),
                "output_path": str(self.output_path),
                "working_dir": str(self.working_dir),
                "cleanup_on_success": self.cleanup_on_success,
                "config": self.config.model_dump(mode="json"),
            },
            indent=2,
        )
        atomic_write_bytes(path, payload.encode("utf-8"), preserve_mode=False)

    async def start(self) -> None:
        if self._closed:
            raise RuntimeError("Cannot start a closed supervisor")
        if self._runner_task is not None or self._action_task is not None:
            raise RuntimeError("Supervisor has already been started")
        recovered_actions = self.store.recover_incomplete_actions(self.run_id)
        self._recovered_stop_pending = self._has_unfinished_stop_actions()
        bootstrap_action = self._claim_bootstrap_action()
        await self._restore_runtime_services()
        self._register_controller_process()
        if recovered_actions:
            await self.bus.publish("actions.recovered", {"count": recovered_actions})
        self._ensure_bootstrap_source_current(bootstrap_action)
        self._create_runtime_tasks()
        if bootstrap_action is not None:
            await self._complete_bootstrap_action(bootstrap_action)

    def _claim_bootstrap_action(self) -> dict[str, Any] | None:
        if self._bootstrap_action_id is None:
            return None
        action = self.store.claim_action(self._bootstrap_action_id)
        if action is None:
            raise RuntimeError(
                f"Bootstrap action {self._bootstrap_action_id} is not requestable"
            )
        if action["kind"] not in {ActionKind.RESUME.value, ActionKind.RESTART.value}:
            raise RuntimeError("Only resume/restart actions can bootstrap a runner")
        self._bootstrap_kind = ActionKind(action["kind"])
        rejection = self._bootstrap_rejection(action, self.store.get_run(self.run_id))
        if rejection is not None:
            self.store.finish_action(
                action["action_id"], ActionStatus.REJECTED, message=rejection
            )
            raise RuntimeError(rejection)
        return action

    def _bootstrap_rejection(
        self, action: dict[str, Any], run: dict[str, Any]
    ) -> str | None:
        run_status = RunStatus(run["status"])
        if run_status.terminal and action["kind"] == ActionKind.RESUME.value:
            return "A terminal run cannot be resumed; use restart to rerun it"
        if action.get("expected_kernel_epoch") != run["kernel_epoch"]:
            return "Kernel epoch changed before stopped-process recovery"
        if action.get("expected_cell_attempt") != run.get("failed_attempt"):
            return "Failed-cell attempt changed before stopped-process recovery"
        if action.get("expected_source_hash") != source_hash(self.source_path):
            return "source.ipynb changed after recovery was requested"
        if action["payload"].get("failed_cell_index") != run.get("failed_cell_index"):
            return "Failed-cell identity changed before stopped-process recovery"
        if int(action["payload"].get("from_cell", 0)) != self.initial_from_cell:
            return "Recovery start cell no longer matches the requested action"
        return None

    async def _restore_runtime_services(self) -> None:
        await self.notifications.start()
        await self.resources.restore_monitors()
        if self._reopen:
            return
        for registration in self.config.resources:
            await self._register_config_resource(registration)

    def _register_controller_process(self) -> None:
        self.store.update_process(
            self.run_id,
            process_pid=os.getpid(),
            process_started_at=self.controller_started_at,
            process_token=self.controller_token,
            server_port=self.config.server.port,
        )
        self._process_registered = True

    def _ensure_bootstrap_source_current(self, action: dict[str, Any] | None) -> None:
        if action is None or action.get("expected_source_hash") == source_hash(
            self.source_path
        ):
            return
        message = "source.ipynb changed while stopped-process recovery was starting"
        self.store.finish_action(
            action["action_id"], ActionStatus.REJECTED, message=message
        )
        raise RuntimeError(message)

    def _create_runtime_tasks(self) -> None:
        self._action_task = asyncio.create_task(
            self._action_loop(), name=f"actions:{self.run_id}"
        )
        self._runner_task = asyncio.create_task(
            self._run_after_recovered_stop_actions(),
            name=f"notebook-runner:{self.run_id}",
        )

    async def _complete_bootstrap_action(self, action: dict[str, Any]) -> None:
        self.store.finish_action(
            action["action_id"],
            ActionStatus.COMPLETED,
            message="Stopped-process recovery accepted; a new kernel epoch is starting",
            result={"from_cell": int(action["payload"].get("from_cell", 0))},
        )
        await self._publish_action_event_safely(
            "action.completed",
            {"action_id": action["action_id"], "bootstrap": True},
        )

    async def _run_after_recovered_stop_actions(self) -> RunStatus:
        recovered_stop_was_pending = (
            self._recovered_stop_pending or self._has_unfinished_stop_actions()
        )
        while self._has_unfinished_stop_actions():
            await asyncio.sleep(0.05)
        status = RunStatus(self.store.get_run(self.run_id)["status"])
        if status is RunStatus.CANCELLING:
            return await self.runner.finalize_cancelled_without_execution()
        if status.terminal and (
            recovered_stop_was_pending or self._bootstrap_kind is not ActionKind.RESTART
        ):
            return status
        return await self.runner.run()

    def _has_unfinished_stop_actions(self) -> bool:
        unfinished = {ActionStatus.REQUESTED.value, ActionStatus.EXECUTING.value}
        return any(
            action["kind"] == ActionKind.STOP_RESOURCE.value
            and action["status"] in unfinished
            for action in self.store.list_actions(self.run_id, limit=10_000)
        )

    async def wait(self) -> RunStatus:
        if self._runner_task is None:
            raise RuntimeError("Supervisor has not been started")
        status = await self._wait_for_runtime_completion()
        if not self._finalized:
            await self._after_run(status)
            await self._raise_if_action_loop_terminated()
            self.store.mark_run_finalized(self.run_id, status)
            self._finalized = True
        self._wait_completed_normally = True
        return status

    async def _wait_for_runtime_completion(self) -> RunStatus:
        runner_task = self._runner_task
        if runner_task is None:
            raise RuntimeError("Supervisor has not been started")
        action_task = self._action_task
        if action_task is None:
            return await runner_task
        completed, _pending = await asyncio.wait(
            {runner_task, action_task}, return_when=asyncio.FIRST_COMPLETED
        )
        if action_task in completed and not self._quiesced:
            await self._raise_unexpected_action_loop_termination(action_task)
        return await runner_task

    async def _raise_if_action_loop_terminated(self) -> None:
        action_task = self._action_task
        if (
            action_task is not None
            and action_task.done()
            and not self._quiesced
            and not self._action_task_termination_observed
        ):
            await self._raise_unexpected_action_loop_termination(action_task)

    async def _raise_unexpected_action_loop_termination(
        self, action_task: asyncio.Task[None]
    ) -> NoReturn:
        self._wait_completed_normally = False
        failure, cause = self._action_loop_termination_failure(action_task)
        self._action_task_termination_observed = True
        try:
            await self.quiesce()
        except BaseException as quiesce_error:
            combined = RuntimeError(
                f"{failure}; runtime shutdown also failed "
                f"({type(quiesce_error).__name__})"
            )
            raise combined from (cause or quiesce_error)
        if cause is not None:
            raise failure from cause
        raise failure

    async def wait_for_action_loop_failure(self) -> NoReturn:
        """Wait until the live control loop terminates, then surface that failure.

        This remains active after notebook finalization while the dashboard lingers.
        Cancelling the waiter does not cancel the underlying action loop.

        Raises
        ------
        RuntimeError
            If the supervisor has not started or the action loop terminates while the
            runtime is expected to remain available.
        """

        action_task = self._action_task
        if action_task is None:
            raise RuntimeError("Supervisor has not been started")
        await asyncio.wait({action_task})
        await self._raise_unexpected_action_loop_termination(action_task)

    @staticmethod
    def _action_loop_termination_failure(
        action_task: asyncio.Task[None],
    ) -> tuple[RuntimeError, BaseException | None]:
        if action_task.cancelled():
            return (
                RuntimeError("Runwatch action loop was cancelled unexpectedly"),
                asyncio.CancelledError(),
            )
        cause = action_task.exception()
        if cause is None:
            return RuntimeError("Runwatch action loop exited unexpectedly"), None
        return (
            RuntimeError(
                "Runwatch action loop failed unexpectedly " f"({type(cause).__name__})"
            ),
            cause,
        )

    async def quiesce(self) -> None:
        """Stop event producers while leaving notifications and storage available."""

        if self._quiesced:
            if self._quiesce_error is not None:
                raise self._quiesce_error
            return
        self._quiesced = True
        runtime_error = await self._capture_async_cleanup(self._stop_runtime_tasks)
        resource_error = await self._capture_async_cleanup(self.resources.shutdown)
        try:
            self._raise_cleanup_errors([runtime_error, resource_error])
        except BaseException as error:
            self._quiesce_error = error
            raise

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        quiesce_error = await self._capture_async_cleanup(self.quiesce)
        notification_error = await self._capture_async_cleanup(self.notifications.close)
        controller_error = self._capture_sync_cleanup(
            self._clear_controller_registration
        )
        store_error = self._capture_sync_cleanup(self.store.close)
        self._raise_cleanup_errors(
            [quiesce_error, notification_error, controller_error, store_error]
        )

    async def _stop_runtime_tasks(self) -> None:
        runner_existed = self._runner_task is not None
        action_failure: tuple[RuntimeError, BaseException | None] | None = None
        if (
            self._action_task is not None
            and self._action_task.done()
            and not self._action_task_termination_observed
        ):
            action_failure = self._action_loop_termination_failure(self._action_task)
            self._action_task_termination_observed = True
        tasks: list[asyncio.Task[Any]] = [
            task
            for task in (self._action_task, self._runner_task)
            if task is not None and not task.done()
        ]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        await self.runner.shutdown()
        if runner_existed:
            await self._pause_after_process_stop()
        if action_failure is not None:
            failure, cause = action_failure
            if cause is not None:
                raise failure from cause
            raise failure

    async def _pause_after_process_stop(self) -> None:
        run = self.store.get_run(self.run_id)
        if run["status"] in {status.value for status in RunStatus if status.terminal}:
            return
        self.store.update_run_status(
            self.run_id,
            RunStatus.PAUSED,
            message=(
                "Runwatch process stopped; use runwatch resume to reconstruct the run"
            ),
        )
        try:
            await self.bus.publish("run.process_stopped", {})
        except asyncio.CancelledError:
            raise
        except Exception:
            # PAUSED is already durable. A diagnostic event failure must not mask the
            # runtime/server error that initiated shutdown.
            return

    async def _capture_async_cleanup(
        self, cleanup: Callable[[], Awaitable[None]]
    ) -> BaseException | None:
        try:
            await cleanup()
        except BaseException as error:
            return error
        return None

    @staticmethod
    def _capture_sync_cleanup(cleanup: Callable[[], None]) -> BaseException | None:
        try:
            cleanup()
        except BaseException as error:
            return error
        return None

    def _clear_controller_registration(self) -> None:
        if not self._process_registered:
            return
        self.store.clear_process(self.run_id, process_token=self.controller_token)
        self._process_registered = False

    @staticmethod
    def _raise_cleanup_errors(errors: list[BaseException | None]) -> None:
        failures = [error for error in errors if error is not None]
        if not failures:
            return
        if isinstance(failures[0], asyncio.CancelledError):
            raise failures[0]
        raise RuntimeError(
            "Runwatch cleanup failed: " + "; ".join(str(error) for error in failures)
        ) from failures[0]

    def snapshot(self) -> dict[str, Any]:
        snapshot = self.store.snapshot(
            self.run_id, chart_points=self.config.storage.dashboard_chart_points
        )
        snapshot["capabilities"] = {
            "paused": self.runner.paused,
            "source_path": str(self.source_path),
            "remote_mutation": "stop_resource_only",
            "controller_live": controller_is_alive(snapshot["run"]),
        }
        if self._dashboard_links is not None:
            for resource in snapshot["resources"]:
                link = self._dashboard_links.describe(str(resource["internal_id"]))
                if link is not None:
                    resource["link"] = link
        return snapshot

    def dashboard_link_target(self, internal_id: str) -> tuple[str, bool]:
        """Return a ready linked-dashboard target and whether it needs pairing."""
        if self._dashboard_links is None:
            raise RuntimeError("Linked dashboard sharing is not active")
        return self._dashboard_links.open_target(internal_id)

    def create_recovery_action(self, kind: ActionKind, *, from_cell: int = 0) -> str:
        if kind not in {ActionKind.RESUME, ActionKind.RESTART}:
            raise ValueError(kind)
        run = self.store.get_run(self.run_id)
        return self.store.create_action(
            self.run_id,
            kind,
            payload={
                "from_cell": from_cell,
                "failed_cell_index": run.get("failed_cell_index"),
            },
            expected_kernel_epoch=int(run["kernel_epoch"]),
            expected_cell_attempt=run.get("failed_attempt"),
            expected_source_hash=source_hash(self.source_path),
        )

    def create_stop_action(self, internal_id: str, *, expected_version: int) -> str:
        run = self.store.get_run(self.run_id)
        return self.store.create_action(
            self.run_id,
            ActionKind.STOP_RESOURCE,
            payload={"internal_id": internal_id, "expected_version": expected_version},
            expected_kernel_epoch=int(run["kernel_epoch"]),
        )

    async def _action_loop(self) -> None:
        while True:
            action = self.store.claim_next_action(self.run_id)
            if action is None:
                await asyncio.sleep(0.25)
                continue
            await self._publish_action_event_safely(
                "action.executing",
                {"action_id": action["action_id"], "kind": action["kind"]},
            )
            try:
                if action["kind"] in {
                    ActionKind.RESUME.value,
                    ActionKind.RESTART.value,
                }:
                    await self._dispatch_recovery_action(action)
                elif action["kind"] == ActionKind.STOP_RESOURCE.value:
                    await self._dispatch_stop_action(action)
                else:
                    raise RuntimeError(f"Unsupported action kind {action['kind']!r}")
            except asyncio.CancelledError:
                raise
            except Exception as error:
                await self._record_action_dispatch_failure(action, error)

    async def _record_action_dispatch_failure(
        self, action: dict[str, Any], error: Exception
    ) -> None:
        current = self.store.get_action(str(action["action_id"]))
        if current is None:
            raise RuntimeError(
                f"Action {action['action_id']} disappeared during dispatch"
            ) from error
        current_status = ActionStatus(current["status"])
        if current_status.terminal:
            await self._publish_action_event_safely(
                "action.post_terminal_error",
                {
                    "action_id": action["action_id"],
                    "status": current_status.value,
                    "error": str(error),
                },
            )
            return
        rejected = isinstance(error, ResourceStopRejected)
        status = ActionStatus.REJECTED if rejected else ActionStatus.FAILED
        try:
            self.store.finish_action(action["action_id"], status, message=str(error))
        except RuntimeError:
            raced = self.store.get_action(str(action["action_id"]))
            if raced is None or not ActionStatus(raced["status"]).terminal:
                raise
            await self._publish_action_event_safely(
                "action.post_terminal_error",
                {
                    "action_id": action["action_id"],
                    "status": raced["status"],
                    "error": str(error),
                },
            )
            return
        await self._publish_action_event_safely(
            "action.rejected" if rejected else "action.failed",
            {"action_id": action["action_id"], "error": str(error)},
        )

    async def _publish_action_event_safely(
        self, event_type: str, payload: dict[str, Any]
    ) -> None:
        try:
            await self.bus.publish(event_type, payload)
        except asyncio.CancelledError:
            raise
        except Exception:
            # The action journal is authoritative. Diagnostic event persistence must not
            # rewrite a terminal action or stop processing later control requests.
            return

    async def _dispatch_recovery_action(self, action: dict[str, Any]) -> None:
        run = self.store.get_run(self.run_id)
        rejection = self._live_recovery_rejection(action, run)
        if rejection is not None:
            self.store.finish_action(
                action["action_id"],
                ActionStatus.REJECTED,
                message=rejection,
            )
            await self._publish_action_event_safely(
                "action.rejected",
                {"action_id": action["action_id"], "error": rejection},
            )
            return
        command = RunnerCommand(
            action_id=action["action_id"],
            kind=action["kind"],
            from_cell=int(action["payload"].get("from_cell", 0)),
            expected_kernel_epoch=int(action["expected_kernel_epoch"]),
            expected_failed_attempt=action.get("expected_cell_attempt"),
            requested_source_hash=str(action["expected_source_hash"]),
        )
        await self.runner.enqueue(command)

    def _live_recovery_rejection(
        self, action: dict[str, Any], run: dict[str, Any]
    ) -> str | None:
        if run["status"] != RunStatus.PAUSED.value or not self.runner.paused:
            return "The live run is not paused at a failed cell"
        if action.get("expected_kernel_epoch") != run["kernel_epoch"]:
            return "Kernel epoch changed before the recovery action executed"
        if action.get("expected_cell_attempt") != run.get("failed_attempt"):
            return "Failed-cell attempt changed before the recovery action executed"
        if action["payload"].get("failed_cell_index") != run.get("failed_cell_index"):
            return "Failed-cell identity changed before the recovery action executed"
        if action.get("expected_source_hash") != source_hash(self.source_path):
            return "source.ipynb changed after the recovery action was requested"
        if self._another_recovery_is_executing(str(action["action_id"])):
            return "Another recovery action is already in flight for this failure"
        return None

    def _another_recovery_is_executing(self, action_id: str) -> bool:
        recovery_kinds = {ActionKind.RESUME.value, ActionKind.RESTART.value}
        return any(
            candidate["action_id"] != action_id
            and candidate["kind"] in recovery_kinds
            and candidate["status"] == ActionStatus.EXECUTING.value
            for candidate in self.store.list_actions(self.run_id, limit=10_000)
        )

    async def _dispatch_stop_action(self, action: dict[str, Any]) -> None:
        run = self.store.get_run(self.run_id)
        internal_id = str(action["payload"]["internal_id"])
        if run["status"] in {
            RunStatus.SUCCEEDED.value,
            RunStatus.FAILED.value,
            RunStatus.CANCELLED.value,
        }:
            terminal_status = RunStatus(run["status"])
            if self._recover_confirmed_stop(action, internal_id):
                await self.resources.finalize_terminal_resource(internal_id)
                await self._complete_stop_action(
                    action,
                    [internal_id],
                    terminal_status=terminal_status,
                )
                return
            raise ResourceStopRejected("The run is already terminal")
        expected_epoch = action.get("expected_kernel_epoch")
        if expected_epoch is not None and int(expected_epoch) != int(
            run["kernel_epoch"]
        ):
            message = "Kernel epoch changed after the stop action was requested"
            self.store.finish_action(
                action["action_id"], ActionStatus.REJECTED, message=message
            )
            await self._publish_action_event_safely(
                "action.rejected", {"action_id": action["action_id"], "error": message}
            )
            return
        recovered = bool(action["payload"].get("recovered", False))
        if not self._recover_confirmed_stop(action, internal_id):
            await self.resources.stop_resource(
                internal_id,
                expected_version=(
                    None if recovered else int(action["payload"]["expected_version"])
                ),
                on_stop_accepted=self.runner.cancel,
                allow_stopping=recovered,
            )
        else:
            await self.resources.finalize_terminal_resource(internal_id)
        await self.runner.cancel()
        terminal_status = self._terminal_run_status()
        if terminal_status is not None:
            await self._complete_stop_action(
                action,
                [internal_id],
                terminal_status=terminal_status,
            )
            return
        stopped = await self.resources.stop_cancel_resources()
        if internal_id not in stopped:
            stopped.insert(0, internal_id)
        await self._complete_stop_action(action, stopped)

    def _recover_confirmed_stop(self, action: dict[str, Any], internal_id: str) -> bool:
        if not action["payload"].get("recovered"):
            return False
        resource = self.store.get_resource(internal_id)
        return bool(
            resource and resource["terminal"] and resource["disposition"] == "cancelled"
        )

    def _terminal_run_status(self) -> RunStatus | None:
        status = RunStatus(self.store.get_run(self.run_id)["status"])
        return status if status.terminal else None

    async def _complete_stop_action(
        self,
        action: dict[str, Any],
        stopped: list[str],
        *,
        terminal_status: RunStatus | None = None,
    ) -> None:
        if terminal_status is None:
            message = "Resource stopped and run cancellation requested"
        else:
            message = (
                "Resource stop completed after the run reached "
                f"{terminal_status.value}; terminal run state preserved"
            )
        result: dict[str, Any] = {
            "stopped_resource_ids": stopped,
            "cancellation_requested": terminal_status is None,
        }
        if terminal_status is not None:
            result["final_run_status"] = terminal_status.value
        self.store.finish_action(
            action["action_id"],
            ActionStatus.COMPLETED,
            message=message,
            result=result,
        )
        await self._publish_action_event_safely(
            "action.completed",
            {
                "action_id": action["action_id"],
                "stopped_resource_ids": stopped,
                "final_run_status": (
                    terminal_status.value if terminal_status is not None else None
                ),
            },
        )

    async def _register_config_resource(
        self, registration: ResourceRegistration
    ) -> None:
        await self.resources.register(
            ResourceEvent(
                resource=registration.resource, lifecycle=registration.lifecycle
            ),
            cell_index=None,
            attempt=None,
            kernel_epoch=None,
        )

    async def _after_run(self, status: RunStatus) -> None:
        await self.resources.close_nonblocking_monitors()


def copy_source_notebook(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, destination)
