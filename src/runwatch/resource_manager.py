from __future__ import annotations

import asyncio
import copy
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from ._compat import timeout
from .events import EventBus
from .models import (
    AwsSettings,
    Ownership,
    ResourceDisposition,
    ResourceEvent,
    ResourceObservation,
    ResourceStatus,
)
from .resources import (
    BUILTIN_ADAPTERS,
    AwsClientProvider,
    ResourceAdapter,
    validate_resource_event,
)
from .storage import RunStore

if TYPE_CHECKING:
    from .dashboard_links import DashboardLinkManager


class ResourceStopRejected(RuntimeError):
    """A stop request that failed a side-effect-free eligibility check."""


class StaleResourceAction(ResourceStopRejected):
    """A stop request whose confirmed resource version is no longer current."""


@dataclass
class _StopConfirmation:
    last_error: Exception | None = None
    provider_acknowledged: bool = False


class ResourceManager:
    def __init__(
        self,
        *,
        store: RunStore,
        bus: EventBus,
        run_id: str,
        working_dir: Path,
        aws_settings: AwsSettings,
        aws_provider: AwsClientProvider | None = None,
    ) -> None:
        self.store = store
        self.bus = bus
        self.run_id = run_id
        self.working_dir = working_dir
        self.aws_settings = aws_settings
        self.aws = aws_provider or AwsClientProvider(aws_settings)
        self._adapter_types: dict[tuple[str, str], type[ResourceAdapter]] = {
            (adapter.provider, adapter.resource_type): adapter
            for adapter in BUILTIN_ADAPTERS
        }
        self._adapters: dict[str, ResourceAdapter] = {}
        self._monitor_tasks: dict[str, asyncio.Task[None]] = {}
        self._inspect_locks: dict[str, asyncio.Lock] = {}
        self._stop_locks: dict[str, asyncio.Lock] = {}
        self._stop_dispositions: dict[str, ResourceDisposition] = {}
        self._dashboard_links: DashboardLinkManager | None = None
        self._closing = False

    def attach_dashboard_links(self, manager: DashboardLinkManager) -> None:
        """Attach the runtime that exposes registered localhost dashboards."""
        if self._dashboard_links is not None:
            raise RuntimeError("A dashboard link manager is already attached")
        self._dashboard_links = manager

    def register_adapter(self, adapter_type: type[ResourceAdapter]) -> None:
        self._adapter_types[(adapter_type.provider, adapter_type.resource_type)] = (
            adapter_type
        )

    async def restore_monitors(self) -> None:
        resources = self.store.list_resources(self.run_id)
        if self._dashboard_links is not None:
            await self._dashboard_links.reconcile(resources)
        for resource in resources:
            if (
                resource.get("lifecycle", {}).get("monitor", True)
                and resource["disposition"] == ResourceDisposition.ACTIVE.value
                and not resource["terminal"]
                and resource["status"] != ResourceStatus.STOPPING.value
            ):
                self._start_monitor(resource["internal_id"])

    async def register(
        self,
        event: ResourceEvent,
        *,
        cell_index: int | None,
        attempt: int | None,
        kernel_epoch: int | None,
    ) -> str:
        adapter_type = self._adapter_types.get(
            (event.resource.provider, event.resource.type)
        )
        try:
            if adapter_type is None:
                validate_resource_event(event)
            else:
                adapter_type.validate_registration(event)
        except Exception as error:
            await self.bus.publish(
                "resource.rejected",
                {
                    "event_id": event.event_id,
                    "provider": event.resource.provider,
                    "resource_type": event.resource.type,
                    "error": str(error),
                },
            )
            raise
        supports_stop = bool(adapter_type and adapter_type.supports_stop)
        internal_id, created = self.store.register_resource(
            run_id=self.run_id,
            event=event,
            cell_index=cell_index,
            attempt=attempt,
            kernel_epoch=kernel_epoch,
            supports_stop=supports_stop,
        )
        if not created:
            await self.bus.publish(
                "resource.reconciled",
                {
                    "internal_id": internal_id,
                    "logical_key": event.resource.logical_key,
                    "cell_index": cell_index,
                    "attempt": attempt,
                    "kernel_epoch": kernel_epoch,
                },
            )
            if self._dashboard_links is not None:
                await self._dashboard_links.reconcile(
                    self.store.list_resources(self.run_id)
                )
            return internal_id
        await self.bus.publish(
            "resource.registered",
            {
                "internal_id": internal_id,
                "event_id": event.event_id,
                "cell_index": cell_index,
                "attempt": attempt,
                "kernel_epoch": kernel_epoch,
                "resource": event.resource.model_dump(mode="json"),
                "lifecycle": event.lifecycle.model_dump(mode="json"),
                "supports_stop": supports_stop,
            },
        )
        if event.lifecycle.monitor:
            self._start_monitor(internal_id)
        else:
            self.store.mark_resource_monitor_closed(internal_id)
        if self._dashboard_links is not None:
            await self._dashboard_links.reconcile(
                self.store.list_resources(self.run_id)
            )
        return internal_id

    def _adapter_for(self, resource: dict[str, Any]) -> ResourceAdapter | None:
        internal_id = resource["internal_id"]
        if internal_id in self._adapters:
            return self._adapters[internal_id]
        adapter_type = self._adapter_types.get(
            (resource["provider"], resource["resource_type"])
        )
        if adapter_type is None:
            return None
        adapter = adapter_type(
            aws=self.aws,
            aws_settings=self.aws_settings,
            working_dir=self.working_dir,
        )
        self._adapters[internal_id] = adapter
        return adapter

    def _start_monitor(self, internal_id: str) -> None:
        existing = self._monitor_tasks.get(internal_id)
        if existing and not existing.done():
            return
        self._monitor_tasks[internal_id] = asyncio.create_task(
            self._monitor(internal_id), name=f"resource-monitor:{internal_id}"
        )

    async def _record_unsupported(
        self, internal_id: str, resource: dict[str, Any]
    ) -> None:
        observation = ResourceObservation(
            status=ResourceStatus.UNKNOWN,
            terminal=True,
            message=f"No adapter for {resource['provider']}.{resource['resource_type']}",
        )
        self.store.update_resource_observation(self.run_id, internal_id, observation)
        await self.bus.publish(
            "resource.unsupported",
            {"internal_id": internal_id, "message": observation.message},
        )

    def _persist_observation(
        self,
        internal_id: str,
        observation: ResourceObservation,
        candidate_cursor: dict[str, Any],
        active_cursor: dict[str, Any] | None,
        terminal_disposition: ResourceDisposition | None = None,
    ) -> None:
        if terminal_disposition is None:
            self.store.record_resource_inspection(
                self.run_id,
                internal_id,
                observation,
                candidate_cursor,
            )
        else:
            self.store.record_resource_stop_inspection(
                self.run_id,
                internal_id,
                observation,
                candidate_cursor,
                terminal_disposition,
            )
        if active_cursor is not None:
            active_cursor.clear()
            active_cursor.update(copy.deepcopy(candidate_cursor))

    async def _publish_observation(
        self, internal_id: str, observation: ResourceObservation
    ) -> None:
        await self.bus.publish(
            "resource.observed",
            {
                "internal_id": internal_id,
                "status": observation.status.value,
                "terminal": observation.terminal,
                "message": observation.message,
                "metrics": observation.metrics,
                "new_log_lines": observation.log_lines[-30:],
            },
        )

    async def _record_monitor_error(
        self,
        internal_id: str,
        error: Exception,
        consecutive_errors: int,
    ) -> None:
        self.store.set_resource_status(
            internal_id,
            ResourceStatus.MONITOR_ERROR,
            message=f"{type(error).__name__}: {error}",
        )
        await self.bus.publish(
            "resource.monitor_error",
            {
                "internal_id": internal_id,
                "error_type": type(error).__name__,
                "error": str(error),
                "consecutive_errors": consecutive_errors,
            },
        )

    async def _fail_blocking_resource_after_monitor_errors(
        self,
        internal_id: str,
        resource: dict[str, Any],
        error: Exception,
        cursor: dict[str, Any],
        consecutive_errors: int,
    ) -> bool:
        lifecycle = resource.get("lifecycle", {})
        limit = lifecycle.get("max_consecutive_monitor_errors", 12)
        if not lifecycle.get("blocking", False) or limit is None:
            return False
        if consecutive_errors < int(limit):
            return False
        message = (
            f"Resource monitoring failed {consecutive_errors} consecutive times; "
            f"last error: {type(error).__name__}: {error}"
        )
        observation = ResourceObservation(
            status=ResourceStatus.FAILED,
            terminal=True,
            message=message,
            metrics={"consecutive_monitor_errors": consecutive_errors},
        )
        self.store.update_resource_observation(self.run_id, internal_id, observation)
        cursor.clear()
        cursor.update(self.store.resource_cursor(internal_id))
        await self._publish_observation(internal_id, observation)
        await self.bus.publish(
            "resource.monitor_failed",
            {
                "internal_id": internal_id,
                "consecutive_errors": consecutive_errors,
                "error_type": type(error).__name__,
                "error": str(error),
            },
        )
        return True

    def _poll_interval(self, resource: dict[str, Any], error_count: int) -> float:
        interval = float(
            resource.get("lifecycle", {}).get("poll_interval_seconds")
            or self.aws_settings.poll_interval_seconds
        )
        if error_count:
            return min(60.0, interval * (2 ** min(error_count, 4)))
        return interval

    def _active_resource(self, internal_id: str) -> dict[str, Any] | None:
        resource = self.store.get_resource(internal_id)
        if resource is None:
            return None
        if resource["disposition"] != ResourceDisposition.ACTIVE.value:
            return None
        return resource

    def _close_terminal_monitor(self, internal_id: str) -> None:
        resource = self.store.get_resource(internal_id)
        if resource and resource.get("terminal"):
            self.store.mark_resource_monitor_closed(internal_id)

    async def _monitor(self, internal_id: str) -> None:
        cursor = self.store.resource_cursor(internal_id)
        consecutive_errors = 0
        try:
            while not self._closing:
                next_error_count = await self._monitor_cycle(
                    internal_id, cursor, consecutive_errors
                )
                if next_error_count is None:
                    return
                consecutive_errors = next_error_count
        finally:
            self._close_terminal_monitor(internal_id)

    async def _monitor_cycle(
        self,
        internal_id: str,
        cursor: dict[str, Any],
        consecutive_errors: int,
    ) -> int | None:
        resource = self._active_resource(internal_id)
        if resource is None:
            return None
        adapter = self._adapter_for(resource)
        if adapter is None:
            await self._record_unsupported(internal_id, resource)
            return None
        try:
            observation = await self._inspect_and_persist(
                internal_id, adapter, resource, cursor
            )
            consecutive_errors = 0
            await self._publish_observation(internal_id, observation)
            if observation.terminal:
                await self._finalize_terminal_resource(
                    internal_id, resource, adapter, cursor
                )
                return None
        except asyncio.CancelledError:
            raise
        except Exception as error:
            consecutive_errors += 1
            await self._record_monitor_error(internal_id, error, consecutive_errors)
            failed = await self._fail_blocking_resource_after_monitor_errors(
                internal_id,
                resource,
                error,
                cursor,
                consecutive_errors,
            )
            if failed:
                return None
        latest = self.store.get_resource(internal_id)
        if latest is None:
            return None
        await asyncio.sleep(self._poll_interval(latest, consecutive_errors))
        return consecutive_errors

    async def _inspect_and_persist(
        self,
        internal_id: str,
        adapter: ResourceAdapter,
        resource: dict[str, Any],
        active_cursor: dict[str, Any] | None,
        terminal_disposition: ResourceDisposition | None = None,
    ) -> ResourceObservation:
        lock = self._inspect_locks.setdefault(internal_id, asyncio.Lock())
        async with lock:
            committed_cursor = self.store.resource_cursor(internal_id)
            candidate_cursor = copy.deepcopy(committed_cursor)
            observation = await adapter.inspect(resource, candidate_cursor)
            stop_disposition = terminal_disposition or self._stop_dispositions.get(
                internal_id
            )
            self._persist_observation(
                internal_id,
                observation,
                candidate_cursor,
                active_cursor,
                stop_disposition,
            )
            return observation

    async def _finalize_terminal_resource(
        self,
        internal_id: str,
        resource: dict[str, Any],
        adapter: ResourceAdapter,
        cursor: dict[str, Any],
    ) -> None:
        lifecycle = resource.get("lifecycle", {})
        drain = lifecycle.get("final_log_drain_seconds")
        if drain is None:
            drain = self.aws_settings.final_log_drain_seconds
        if lifecycle.get("retain_logs", True) and float(drain) > 0:
            await asyncio.sleep(float(drain))
            latest = self.store.get_resource(internal_id)
            if latest is not None:
                try:
                    final = await self._inspect_and_persist(
                        internal_id, adapter, latest, cursor
                    )
                    await self._publish_observation(internal_id, final)
                    await self.bus.publish(
                        "resource.finalized",
                        {
                            "internal_id": internal_id,
                            "status": final.status.value,
                            "metrics": final.metrics,
                            "new_log_lines": final.log_lines[-30:],
                        },
                    )
                    if final.metrics.get("log_drain_truncated"):
                        await self.bus.publish(
                            "resource.finalization_warning",
                            {
                                "internal_id": internal_id,
                                "error": "Terminal log catch-up reached its configured bound; logs may be incomplete",
                            },
                        )
                except Exception as error:
                    await self.bus.publish(
                        "resource.finalization_warning",
                        {"internal_id": internal_id, "error": str(error)},
                    )
        self.store.mark_resource_monitor_closed(internal_id)
        await self.bus.publish(
            "resource.monitor_closed",
            {"internal_id": internal_id, "reason": "terminal"},
        )

    async def stop_resource(
        self,
        internal_id: str,
        *,
        expected_version: int | None = None,
        disposition: ResourceDisposition = ResourceDisposition.CANCELLED,
        on_stop_accepted: Callable[[], Awaitable[None]] | None = None,
        allow_stopping: bool = False,
    ) -> dict[str, Any]:
        lock = self._stop_locks.setdefault(internal_id, asyncio.Lock())
        async with lock:
            resource = self.validate_stop_eligibility(
                internal_id,
                expected_version=expected_version,
                allow_stopping=allow_stopping,
            )
            adapter = self._adapter_for(resource)
            assert adapter is not None and adapter.supports_stop
            confirmation = _StopConfirmation()
            deadline = timeout(self.aws_settings.stop_timeout_seconds)
            self._stop_dispositions[internal_id] = disposition
            try:
                async with deadline:
                    await self._request_resource_stop(
                        internal_id,
                        resource,
                        adapter,
                        on_stop_accepted,
                        disposition,
                        confirmation,
                    )
                    return await self._wait_for_resource_stop(
                        internal_id,
                        adapter,
                        disposition,
                        confirmation,
                    )
            except TimeoutError as error:
                if not deadline.expired:
                    raise
                await self._record_stop_timeout(
                    internal_id,
                    previous_status=str(resource["status"]),
                    confirmation=confirmation,
                )
                detail = self._stop_timeout_detail(confirmation.last_error)
                raise TimeoutError(
                    f"Timed out waiting for resource {internal_id} to stop{detail}"
                ) from (confirmation.last_error or error)
            finally:
                self._stop_dispositions.pop(internal_id, None)

    async def _request_resource_stop(
        self,
        internal_id: str,
        resource: dict[str, Any],
        adapter: ResourceAdapter,
        on_stop_accepted: Callable[[], Awaitable[None]] | None,
        disposition: ResourceDisposition,
        confirmation: _StopConfirmation,
    ) -> None:
        already_stopping = resource["status"] == ResourceStatus.STOPPING.value
        requested = self.store.request_resource_stop(internal_id, disposition)
        if not already_stopping:
            await self.bus.publish(
                "resource.stop_requested",
                {
                    "internal_id": internal_id,
                    "external_id": resource["external_id"],
                    "already_terminal": requested["terminal"],
                },
            )
        terminal_confirmed = bool(requested["terminal"])
        if already_stopping and not terminal_confirmed:
            terminal_confirmed = await self._inspect_stopping_resource(
                internal_id,
                adapter,
                requested,
                disposition,
                confirmation,
            )
        try:
            if not terminal_confirmed:
                await adapter.stop(resource)
            confirmation.provider_acknowledged = True
        except Exception as error:
            if not already_stopping:
                self.store.set_resource_status(
                    internal_id,
                    ResourceStatus(resource["status"]),
                    message=f"Stop request failed: {type(error).__name__}: {error}",
                )
            raise
        if on_stop_accepted is not None:
            await on_stop_accepted()
        if not terminal_confirmed:
            self._start_monitor(internal_id)

    async def _wait_for_resource_stop(
        self,
        internal_id: str,
        adapter: ResourceAdapter,
        disposition: ResourceDisposition,
        confirmation: _StopConfirmation,
    ) -> dict[str, Any]:
        while True:
            latest = self.store.get_resource(internal_id)
            if latest is None:
                raise KeyError(internal_id)
            if latest["terminal"]:
                return await self._confirm_resource_stop(
                    internal_id, latest, disposition
                )
            terminal = await self._inspect_stopping_resource(
                internal_id,
                adapter,
                latest,
                disposition,
                confirmation,
            )
            if terminal:
                confirmed = self.store.get_resource(internal_id)
                if confirmed is None:
                    raise KeyError(internal_id)
                return await self._confirm_resource_stop(
                    internal_id, confirmed, disposition
                )
            await asyncio.sleep(1)

    async def _record_stop_timeout(
        self,
        internal_id: str,
        *,
        previous_status: str,
        confirmation: _StopConfirmation,
    ) -> None:
        current = self.store.get_resource(internal_id)
        confirmed = bool(
            current
            and current["terminal"]
            and current["disposition"] != ResourceDisposition.ACTIVE.value
        )
        retry_status = (
            ResourceStatus(str(current["status"]))
            if confirmed and current is not None
            else ResourceStatus(previous_status)
        )
        if retry_status is ResourceStatus.STOPPING and not confirmed:
            retry_status = ResourceStatus.RUNNING
        detail = self._stop_timeout_detail(confirmation.last_error)
        phase = (
            "after provider acknowledgement; confirmation is incomplete"
            if confirmation.provider_acknowledged
            else "before provider acknowledgement; provider outcome is unknown"
        )
        message = f"Stop operation timed out {phase}; retry is allowed{detail}"
        self.store.set_resource_status(
            internal_id,
            retry_status,
            message=message,
        )
        await self.bus.publish(
            "resource.stop_timeout",
            {
                "internal_id": internal_id,
                "error": message,
                "provider_acknowledged": confirmation.provider_acknowledged,
                "retryable": True,
            },
        )

    async def _confirm_resource_stop(
        self,
        internal_id: str,
        resource: dict[str, Any],
        disposition: ResourceDisposition,
    ) -> dict[str, Any]:
        if resource["disposition"] != disposition.value:
            self.store.set_resource_disposition(internal_id, disposition)
        await self.bus.publish(
            "resource.stop_confirmed",
            {"internal_id": internal_id, "status": resource["status"]},
        )
        return resource

    async def _inspect_stopping_resource(
        self,
        internal_id: str,
        adapter: ResourceAdapter,
        resource: dict[str, Any],
        disposition: ResourceDisposition,
        confirmation: _StopConfirmation,
    ) -> bool:
        try:
            observation = await self._inspect_and_persist(
                internal_id,
                adapter,
                resource,
                None,
                disposition,
            )
            confirmation.last_error = None
            return observation.terminal
        except Exception as error:
            confirmation.last_error = error
            message = (
                f"Stop confirmation inspection failed: {type(error).__name__}: {error}"
            )
            self.store.set_resource_status(
                internal_id, ResourceStatus.STOPPING, message=message
            )
            await self.bus.publish(
                "resource.stop_inspection_error",
                {
                    "internal_id": internal_id,
                    "error_type": type(error).__name__,
                    "error": str(error),
                },
            )
            return False

    @staticmethod
    def _stop_timeout_detail(error: Exception | None) -> str:
        if error is None:
            return ""
        return f"; last inspection error: {type(error).__name__}: {error}"

    def validate_stop_eligibility(
        self,
        internal_id: str,
        *,
        expected_version: int | None = None,
        allow_stopping: bool = False,
    ) -> dict[str, Any]:
        """Return an eligible resource without performing any side effects."""

        resource = self.store.get_resource(internal_id)
        if resource is None:
            raise ResourceStopRejected(f"Unknown resource {internal_id}")
        if expected_version is not None and resource["version"] != expected_version:
            raise StaleResourceAction(
                "Resource changed after the stop confirmation was shown"
            )
        if resource["disposition"] != ResourceDisposition.ACTIVE.value:
            raise ResourceStopRejected("Only active resources can be stopped")
        if resource["terminal"]:
            raise ResourceStopRejected("Terminal resources cannot be stopped")
        if resource["status"] == ResourceStatus.STOPPING.value and not allow_stopping:
            raise ResourceStopRejected("Resource is already stopping")
        if resource["ownership"] != Ownership.EXCLUSIVE.value:
            raise ResourceStopRejected("Only exclusive resources can be stopped")
        adapter = self._adapter_for(resource)
        if (
            not resource["supports_stop"]
            or adapter is None
            or not adapter.supports_stop
        ):
            raise ResourceStopRejected(
                f"No stop action for {resource['provider']}.{resource['resource_type']}"
            )
        return resource

    async def stop_cancel_resources(self, *, first: str | None = None) -> list[str]:
        resources = self.store.list_resources(self.run_id)
        resources.sort(key=lambda item: item["internal_id"] != first)
        stopped: list[str] = []
        for resource in resources:
            lifecycle = resource.get("lifecycle", {})
            if (
                resource["disposition"] == ResourceDisposition.ACTIVE.value
                and lifecycle.get("stop_on_cancel", False)
                and not resource["terminal"]
                and resource["status"] != ResourceStatus.STOPPING.value
                and resource["ownership"] == Ownership.EXCLUSIVE.value
                and resource["supports_stop"]
            ):
                try:
                    await self.stop_resource(resource["internal_id"])
                    stopped.append(resource["internal_id"])
                except Exception as error:
                    await self.bus.publish(
                        "resource.cancel_stop_failed",
                        {"internal_id": resource["internal_id"], "error": str(error)},
                    )
        return stopped

    def blocking_summary(self) -> dict[str, Any]:
        active: list[dict[str, Any]] = []
        failures: list[dict[str, Any]] = []
        successful: list[dict[str, Any]] = []
        for resource in self.store.list_resources(self.run_id):
            if not resource.get("lifecycle", {}).get("blocking", False):
                continue
            if resource["disposition"] != ResourceDisposition.ACTIVE.value:
                continue
            if not resource["terminal"]:
                active.append(resource)
            elif resource["status"] == ResourceStatus.COMPLETED.value:
                successful.append(resource)
            else:
                failures.append(resource)
        return {"active": active, "failures": failures, "successful": successful}

    async def wait_for_blocking_resources(
        self,
        timeout_seconds: float | None = None,
        cancel_event: asyncio.Event | None = None,
    ) -> dict[str, Any]:
        async def wait() -> dict[str, Any]:
            while True:
                if cancel_event and cancel_event.is_set():
                    return self.blocking_summary()
                summary = self.blocking_summary()
                if not summary["active"]:
                    return summary
                await asyncio.sleep(0.5)

        if timeout_seconds is None:
            return await wait()
        async with timeout(timeout_seconds):
            return await wait()

    async def close_nonblocking_monitors(self) -> None:
        resources = {
            item["internal_id"]: item for item in self.store.list_resources(self.run_id)
        }
        cancelled: list[asyncio.Task[None]] = []
        for internal_id, task in self._monitor_tasks.items():
            resource = resources.get(internal_id)
            if resource and not resource.get("lifecycle", {}).get("blocking", False):
                if not task.done():
                    task.cancel()
                    cancelled.append(task)
                self.store.mark_resource_monitor_closed(internal_id)
        if cancelled:
            await asyncio.gather(*cancelled, return_exceptions=True)

    async def shutdown(self) -> None:
        self._closing = True
        for task in self._monitor_tasks.values():
            if not task.done():
                task.cancel()
        await asyncio.gather(*self._monitor_tasks.values(), return_exceptions=True)
        cleanup: list[Awaitable[None]] = [
            adapter.close() for adapter in self._adapters.values()
        ]
        if self._dashboard_links is not None:
            cleanup.append(self._dashboard_links.close())
        results = await asyncio.gather(*cleanup, return_exceptions=True)
        failures = [result for result in results if isinstance(result, BaseException)]
        if failures:
            raise RuntimeError(
                "Resource cleanup failed: " + "; ".join(str(item) for item in failures)
            ) from failures[0]
