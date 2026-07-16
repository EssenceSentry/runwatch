from __future__ import annotations

import asyncio
import copy
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

from ._compat import timeout
from .adapters import AdapterRegistry, default_adapter_registry
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
    AdapterContext,
    AwsClientProvider,
    ResourceAdapter,
)
from .storage import RunStore, json_dumps

if TYPE_CHECKING:
    from .dashboard_links import DashboardLinkManager


class ResourceStopRejected(RuntimeError):
    """A stop request that failed a side-effect-free eligibility check."""


class StaleResourceAction(ResourceStopRejected):
    """A stop request whose confirmed resource version is no longer current."""


_DIAGNOSTIC_IDENTIFIER_JSON_BYTES = 128
_DIAGNOSTIC_NAME_JSON_BYTES = 96
_DIAGNOSTIC_ERROR_JSON_BYTES = 384
_RESOURCE_MESSAGE_JSON_BYTES = 512


def _bounded_json_text(value: str, max_bytes: int) -> str:
    if len(json_dumps(value).encode("utf-8")) <= max_bytes:
        return value
    suffix = "…"
    low = 0
    high = len(value)
    best = ""
    while low <= high:
        middle = (low + high) // 2
        candidate = value[:middle] + suffix
        if len(json_dumps(candidate).encode("utf-8")) <= max_bytes:
            best = candidate
            low = middle + 1
        else:
            high = middle - 1
    return best


def _exception_parts(error: Exception) -> tuple[str, str]:
    error_type = _bounded_json_text(type(error).__name__, _DIAGNOSTIC_NAME_JSON_BYTES)
    try:
        detail = str(error)
    except Exception:
        detail = "<error detail unavailable>"
    return error_type, _bounded_json_text(detail, _DIAGNOSTIC_ERROR_JSON_BYTES)


def _resource_message(value: str) -> str:
    return _bounded_json_text(value, _RESOURCE_MESSAGE_JSON_BYTES)


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
        adapter_registry: AdapterRegistry | None = None,
    ) -> None:
        self.store = store
        self.bus = bus
        self.run_id = run_id
        self.working_dir = working_dir
        self.aws_settings = aws_settings
        self._adapter_registry = (adapter_registry or default_adapter_registry()).copy()
        self._adapter_context = AdapterContext(
            working_dir=working_dir,
            settings={"aws": aws_settings},
        )
        if aws_provider is not None:
            self._adapter_context.register_service("aws.client_provider", aws_provider)
        self._adapters: dict[str, ResourceAdapter] = {}
        self._monitor_tasks: dict[str, asyncio.Task[None]] = {}
        self._inspect_locks: dict[str, asyncio.Lock] = {}
        self._finalize_locks: dict[str, asyncio.Lock] = {}
        self._stop_locks: dict[str, asyncio.Lock] = {}
        self._stop_dispositions: dict[str, ResourceDisposition] = {}
        self._dashboard_links: DashboardLinkManager | None = None
        self._retirement_failures: list[Exception] = []
        self._closing = False
        self._shutdown_complete = False
        self._shutdown_error: RuntimeError | None = None
        self._shutdown_lock = asyncio.Lock()

    def attach_dashboard_links(self, manager: DashboardLinkManager) -> None:
        """Attach the runtime that exposes registered localhost dashboards."""
        if self._dashboard_links is not None:
            raise RuntimeError("A dashboard link manager is already attached")
        self._dashboard_links = manager

    def register_adapter(self, adapter_type: type[ResourceAdapter]) -> None:
        self._adapter_registry.register(adapter_type)

    async def restore_monitors(self) -> None:
        resources = self.store.list_resources(self.run_id)
        if self._dashboard_links is not None:
            await self._dashboard_links.reconcile(resources)
        for resource in resources:
            lifecycle = resource.get("lifecycle", {})
            terminal_finalization_pending = bool(
                resource["terminal"] and not resource["monitor_closed"]
            )
            active_monitor_pending = bool(
                lifecycle.get("monitor", True)
                and resource["disposition"] == ResourceDisposition.ACTIVE.value
                and not resource["terminal"]
                and resource["status"] != ResourceStatus.STOPPING.value
            )
            if terminal_finalization_pending or active_monitor_pending:
                self._start_monitor(resource["internal_id"])

    async def register(
        self,
        event: ResourceEvent,
        *,
        cell_index: int | None,
        attempt: int | None,
        kernel_epoch: int | None,
    ) -> str:
        try:
            adapter_type = self._adapter_registry.validate(event)
        except Exception as error:
            _, error_detail = _exception_parts(error)
            await self._publish_diagnostic_safely(
                "resource.rejected",
                {
                    "event_id": _bounded_json_text(
                        event.event_id, _DIAGNOSTIC_IDENTIFIER_JSON_BYTES
                    ),
                    "provider": _bounded_json_text(
                        event.resource.provider, _DIAGNOSTIC_NAME_JSON_BYTES
                    ),
                    "resource_type": _bounded_json_text(
                        event.resource.type, _DIAGNOSTIC_NAME_JSON_BYTES
                    ),
                    "error": error_detail,
                },
            )
            raise
        supports_stop = adapter_type.supports_stop
        internal_id, created, persisted_event = self.store.register_resource_with_event(
            run_id=self.run_id,
            event=event,
            cell_index=cell_index,
            attempt=attempt,
            kernel_epoch=kernel_epoch,
            supports_stop=supports_stop,
        )
        self.bus.fan_out_persisted(persisted_event)
        payload_value: object = persisted_event.get("payload")
        payload = (
            cast(dict[str, object], payload_value)
            if isinstance(payload_value, dict)
            else {}
        )
        superseded_internal_id = payload.get("superseded_internal_id")
        if isinstance(superseded_internal_id, str):
            await self._cancel_and_retire_resource(superseded_internal_id)
        if not created:
            if self._dashboard_links is not None:
                await self._dashboard_links.reconcile(
                    self.store.list_resources(self.run_id)
                )
            return internal_id
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
        adapter_type = self._adapter_registry.resolve(
            str(resource["provider"]), str(resource["resource_type"])
        )
        if adapter_type is None:
            return None
        adapter = adapter_type(self._adapter_context)
        self._adapters[internal_id] = adapter
        return adapter

    def _start_monitor(self, internal_id: str) -> None:
        existing = self._monitor_tasks.get(internal_id)
        if existing and not existing.done():
            return
        self._monitor_tasks[internal_id] = asyncio.create_task(
            self._monitor(internal_id), name=f"resource-monitor:{internal_id}"
        )

    async def _cancel_and_retire_resource(self, internal_id: str) -> None:
        task = self._monitor_tasks.get(internal_id)
        current = asyncio.current_task()
        if task is not None and task is not current and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await self._retire_resource_if_settled(internal_id)

    async def _retire_resource_if_settled(self, internal_id: str) -> None:
        if self._resource_retirement_is_blocked(internal_id):
            return
        self._monitor_tasks.pop(internal_id, None)
        await self._close_retired_adapter(internal_id)
        self._prune_retired_resource_state(internal_id)

    def _resource_retirement_is_blocked(self, internal_id: str) -> bool:
        if self._closing:
            return True
        resource = self.store.get_resource(internal_id)
        if resource is not None and not (
            resource["monitor_closed"]
            or resource["disposition"] != ResourceDisposition.ACTIVE.value
        ):
            return True
        stop_lock = self._stop_locks.get(internal_id)
        if stop_lock is not None and stop_lock.locked():
            return True
        task = self._monitor_tasks.get(internal_id)
        current = asyncio.current_task()
        return task is not None and task is not current and not task.done()

    async def _close_retired_adapter(self, internal_id: str) -> None:
        adapter = self._adapters.pop(internal_id, None)
        if adapter is None:
            return
        try:
            await adapter.close()
        except Exception as error:
            self._retirement_failures.append(error)
            error_type, detail = _exception_parts(error)
            await self._publish_diagnostic_safely(
                "resource.adapter_close_error",
                {
                    "internal_id": internal_id,
                    "error_type": error_type,
                    "error": detail,
                },
            )

    def _prune_retired_resource_state(self, internal_id: str) -> None:
        inspect_lock = self._inspect_locks.get(internal_id)
        if inspect_lock is None or not inspect_lock.locked():
            self._inspect_locks.pop(internal_id, None)
        finalize_lock = self._finalize_locks.get(internal_id)
        if finalize_lock is None or not finalize_lock.locked():
            self._finalize_locks.pop(internal_id, None)
        self._stop_locks.pop(internal_id, None)
        self._stop_dispositions.pop(internal_id, None)

    async def _record_unsupported(
        self, internal_id: str, resource: dict[str, Any]
    ) -> None:
        message = _resource_message(
            f"No adapter for {resource['provider']}.{resource['resource_type']}"
        )
        observation = ResourceObservation(
            status=ResourceStatus.UNKNOWN,
            terminal=True,
            message=message,
        )
        observed = self.store.update_resource_observation(
            self.run_id, internal_id, observation
        )
        self.bus.fan_out_persisted(observed)
        await self._publish_diagnostic_safely(
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
    ) -> dict[str, Any]:
        if terminal_disposition is None:
            event = self.store.record_resource_inspection(
                self.run_id,
                internal_id,
                observation,
                candidate_cursor,
            )
        else:
            event = self.store.record_resource_stop_inspection(
                self.run_id,
                internal_id,
                observation,
                candidate_cursor,
                terminal_disposition,
            )
        if active_cursor is not None:
            active_cursor.clear()
            active_cursor.update(copy.deepcopy(candidate_cursor))
        return event

    def _publish_observation(self, event: dict[str, Any]) -> None:
        self.bus.fan_out_persisted(event)

    async def _record_monitor_error(
        self,
        internal_id: str,
        error: Exception,
        consecutive_errors: int,
    ) -> None:
        error_type, detail = _exception_parts(error)
        current = self.store.get_resource(internal_id)
        stop_intent_preserved = bool(
            current and current["status"] == ResourceStatus.STOPPING.value
        )
        if not stop_intent_preserved:
            self.store.set_resource_status(
                internal_id,
                ResourceStatus.MONITOR_ERROR,
                message=_resource_message(f"{error_type}: {detail}"),
            )
        await self._publish_diagnostic_safely(
            "resource.monitor_error",
            {
                "internal_id": internal_id,
                "error_type": error_type,
                "error": detail,
                "consecutive_errors": consecutive_errors,
                "stop_intent_preserved": stop_intent_preserved,
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
        current = self.store.get_resource(internal_id)
        if current and current["status"] == ResourceStatus.STOPPING.value:
            return False
        error_type, detail = _exception_parts(error)
        message = _resource_message(
            f"Resource monitoring failed {consecutive_errors} consecutive times; "
            f"last error: {error_type}: {detail}"
        )
        observation = ResourceObservation(
            status=ResourceStatus.FAILED,
            terminal=True,
            message=message,
            metrics={"consecutive_monitor_errors": consecutive_errors},
        )
        observed = self.store.update_resource_observation(
            self.run_id, internal_id, observation
        )
        cursor.clear()
        cursor.update(self.store.resource_cursor(internal_id))
        self._publish_observation(observed)
        self.store.mark_resource_monitor_closed(internal_id)
        await self._publish_diagnostic_safely(
            "resource.monitor_failed",
            {
                "internal_id": internal_id,
                "consecutive_errors": consecutive_errors,
                "error_type": error_type,
                "error": detail,
            },
        )
        return True

    def _poll_interval(self, resource: dict[str, Any], error_count: int) -> float:
        interval = float(
            resource.get("lifecycle", {}).get("poll_interval_seconds")
            or self.aws_settings.poll_interval_seconds
        )
        if error_count:
            return max(
                interval,
                min(60.0, interval * (2 ** min(error_count, 4))),
            )
        return interval

    def _active_resource(self, internal_id: str) -> dict[str, Any] | None:
        resource = self.store.get_resource(internal_id)
        if resource is None:
            return None
        if resource["disposition"] != ResourceDisposition.ACTIVE.value:
            return None
        return resource

    @staticmethod
    def _monitor_failure_detail(error: Exception) -> str:
        error_type, detail = _exception_parts(error)
        return _resource_message(f"{error_type}: {detail}")

    async def _terminalize_unexpected_monitor_failure(
        self, internal_id: str, error: Exception
    ) -> None:
        resource = self._active_resource(internal_id)
        if resource is None or resource["terminal"]:
            return
        if resource["status"] == ResourceStatus.STOPPING.value:
            # A failed monitor cannot prove that the provider honored a stop request.
            # Leave STOPPING retryable for the dedicated stop-confirmation path and
            # let wait_for_blocking_resources surface the failed task if necessary.
            raise RuntimeError(
                f"Resource monitor for {internal_id} failed while stop confirmation "
                "was still pending"
            ) from error

        error_type, detail = _exception_parts(error)
        message = _resource_message(
            f"Resource monitor stopped unexpectedly: {error_type}: {detail}"
        )
        observed = self.store.update_resource_observation(
            self.run_id,
            internal_id,
            ResourceObservation(
                status=ResourceStatus.FAILED,
                terminal=True,
                message=message,
                metrics={"monitor_task_error": error_type},
            ),
        )
        self.store.mark_resource_monitor_closed(internal_id)
        try:
            self._publish_observation(observed)
        except Exception:
            # The state transition and event are already durable. In-memory SSE
            # fan-out must not turn a terminal resource back into an endless wait.
            pass
        try:
            await self._publish_diagnostic_safely(
                "resource.monitor_task_failed",
                {
                    "internal_id": internal_id,
                    "error_type": error_type,
                    "error": detail,
                },
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            # The atomic resource.observed event above remains the durable diagnostic.
            pass

    async def _monitor(self, internal_id: str) -> None:
        cursor = self.store.resource_cursor(internal_id)
        consecutive_errors = 0
        try:
            restored = self.store.get_resource(internal_id)
            if restored and restored["terminal"] and not restored["monitor_closed"]:
                await self.finalize_terminal_resource(internal_id)
                return
            while not self._closing:
                next_error_count = await self._monitor_cycle(
                    internal_id, cursor, consecutive_errors
                )
                if next_error_count is None:
                    return
                consecutive_errors = next_error_count
        except asyncio.CancelledError:
            raise
        except Exception as error:
            await self._terminalize_unexpected_monitor_failure(internal_id, error)
            await self.finalize_terminal_resource(internal_id)
        finally:
            await self._retire_resource_if_settled(internal_id)

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
            await self.finalize_terminal_resource(internal_id)
            return None
        try:
            observation, observed = await self._inspect_and_persist(
                internal_id, adapter, resource, cursor
            )
            consecutive_errors = 0
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
                await self._finalize_terminal_resource(
                    internal_id, resource, adapter, cursor
                )
                return None
        else:
            self._publish_observation(observed)
            if observation.terminal:
                await self._finalize_terminal_resource(
                    internal_id, resource, adapter, cursor
                )
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
        preserve_terminal: bool = False,
    ) -> tuple[ResourceObservation, dict[str, Any]]:
        lock = self._inspect_locks.setdefault(internal_id, asyncio.Lock())
        async with lock:
            committed_cursor = self.store.resource_cursor(internal_id)
            candidate_cursor = copy.deepcopy(committed_cursor)
            observation = await adapter.inspect(resource, candidate_cursor)
            latest = self.store.get_resource(internal_id)
            if (
                preserve_terminal
                and latest is not None
                and latest["terminal"]
                and not observation.terminal
            ):
                observation = observation.model_copy(
                    update={
                        "status": ResourceStatus(latest["status"]),
                        "terminal": True,
                        "message": latest.get("message") or observation.message,
                    }
                )
            if (
                latest is not None
                and latest["status"] == ResourceStatus.STOPPING.value
                and not observation.terminal
            ):
                # A provider inspection can begin before a local stop request is
                # persisted and complete afterwards.  Keep the durable local intent
                # authoritative until the provider reaches a terminal state or the
                # stop path explicitly restores a retryable status.
                observation = observation.model_copy(
                    update={"status": ResourceStatus.STOPPING}
                )
            stop_disposition = terminal_disposition or self._stop_dispositions.get(
                internal_id
            )
            event = self._persist_observation(
                internal_id,
                observation,
                candidate_cursor,
                active_cursor,
                stop_disposition,
            )
            return observation, event

    async def _finalize_terminal_resource(
        self,
        internal_id: str,
        resource: dict[str, Any],
        adapter: ResourceAdapter,
        cursor: dict[str, Any],
    ) -> None:
        lock = self._finalize_locks.setdefault(internal_id, asyncio.Lock())
        async with lock:
            current = self.store.get_resource(internal_id)
            if current is None or not current["terminal"] or current["monitor_closed"]:
                return
            lifecycle = current.get("lifecycle", resource.get("lifecycle", {}))
            drain = lifecycle.get("final_log_drain_seconds")
            if drain is None:
                drain = self.aws_settings.final_log_drain_seconds
            finalization_payload: dict[str, Any] | None = None
            warning: str | None = None
            if lifecycle.get("retain_logs", True) and float(drain) > 0:
                await asyncio.sleep(float(drain))
                latest = self.store.get_resource(internal_id)
                if latest is not None:
                    try:
                        final, observed = await self._inspect_and_persist(
                            internal_id,
                            adapter,
                            latest,
                            cursor,
                            preserve_terminal=True,
                        )
                        self._publish_observation(observed)
                        finalization_payload = {
                            "internal_id": internal_id,
                            "status": final.status.value,
                            "metric_count": len(final.metrics),
                            "new_log_line_count": len(final.log_lines),
                            "log_drain_truncated": bool(
                                final.metrics.get("log_drain_truncated")
                            ),
                        }
                        if final.metrics.get("log_drain_truncated"):
                            warning = (
                                "Terminal log catch-up reached its configured bound; "
                                "logs may be incomplete"
                            )
                    except Exception as error:
                        warning = self._monitor_failure_detail(error)
            self.store.mark_resource_monitor_closed(internal_id)
            if finalization_payload is not None:
                await self._publish_diagnostic_safely(
                    "resource.finalized", finalization_payload
                )
            if warning is not None:
                await self._publish_diagnostic_safely(
                    "resource.finalization_warning",
                    {"internal_id": internal_id, "error": warning},
                )
            await self._publish_diagnostic_safely(
                "resource.monitor_closed",
                {"internal_id": internal_id, "reason": "terminal"},
            )

    async def _publish_diagnostic_safely(
        self, event_type: str, payload: dict[str, Any]
    ) -> None:
        try:
            await self.bus.publish(event_type, payload)
        except asyncio.CancelledError:
            raise
        except Exception:
            # Authoritative state and control decisions already succeeded. Optional
            # diagnostics must not rewrite their retry or finalization flow.
            return

    async def finalize_terminal_resource(self, internal_id: str) -> dict[str, Any]:
        """Finish a durable terminal resource before considering it settled."""

        resource = self.store.get_resource(internal_id)
        if resource is None:
            raise KeyError(internal_id)
        if not resource["terminal"] or resource["monitor_closed"]:
            return resource
        adapter = self._adapter_for(resource)
        if adapter is None:
            lock = self._finalize_locks.setdefault(internal_id, asyncio.Lock())
            async with lock:
                current = self.store.get_resource(internal_id)
                if current is None:
                    raise KeyError(internal_id)
                if not current["terminal"] or current["monitor_closed"]:
                    return current
                self.store.mark_resource_monitor_closed(internal_id)
            await self._publish_diagnostic_safely(
                "resource.finalization_warning",
                {
                    "internal_id": internal_id,
                    "error": (
                        "Terminal resource finalization could not inspect the provider "
                        "because no adapter is available"
                    ),
                },
            )
            await self._publish_diagnostic_safely(
                "resource.monitor_closed",
                {"internal_id": internal_id, "reason": "terminal"},
            )
            return self.store.get_resource(internal_id) or resource
        await self._finalize_terminal_resource(
            internal_id,
            resource,
            adapter,
            self.store.resource_cursor(internal_id),
        )
        return self.store.get_resource(internal_id) or resource

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
        try:
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
        finally:
            await self._retire_resource_if_settled(internal_id)

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
        requested, stop_event = self.store.request_resource_stop_with_event(
            internal_id, disposition
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
                error_type, detail = _exception_parts(error)
                self.store.set_resource_status(
                    internal_id,
                    ResourceStatus(resource["status"]),
                    message=_resource_message(
                        f"Stop request failed: {error_type}: {detail}"
                    ),
                )
            raise
        if stop_event is not None:
            self.bus.fan_out_persisted(stop_event)
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
                latest = await self.finalize_terminal_resource(internal_id)
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
                confirmed = await self.finalize_terminal_resource(internal_id)
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
        message = _resource_message(
            f"Stop operation timed out {phase}; retry is allowed{detail}"
        )
        self.store.set_resource_status(
            internal_id,
            retry_status,
            message=message,
        )
        await self._publish_diagnostic_safely(
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
        await self._publish_diagnostic_safely(
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
            observation, observed = await self._inspect_and_persist(
                internal_id,
                adapter,
                resource,
                None,
                disposition,
            )
            self._publish_observation(observed)
            confirmation.last_error = None
            return observation.terminal
        except Exception as error:
            confirmation.last_error = error
            error_type, detail = _exception_parts(error)
            message = _resource_message(
                f"Stop confirmation inspection failed: {error_type}: {detail}"
            )
            self.store.set_resource_status(
                internal_id, ResourceStatus.STOPPING, message=message
            )
            await self._publish_diagnostic_safely(
                "resource.stop_inspection_error",
                {
                    "internal_id": internal_id,
                    "error_type": error_type,
                    "error": detail,
                },
            )
            return False

    @staticmethod
    def _stop_timeout_detail(error: Exception | None) -> str:
        if error is None:
            return ""
        error_type, detail = _exception_parts(error)
        return _resource_message(f"; last inspection error: {error_type}: {detail}")

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
                and resource["ownership"] == Ownership.EXCLUSIVE.value
                and resource["supports_stop"]
            ):
                try:
                    await self.stop_resource(
                        resource["internal_id"],
                        allow_stopping=(
                            resource["status"] == ResourceStatus.STOPPING.value
                        ),
                    )
                    stopped.append(resource["internal_id"])
                except Exception as error:
                    error_type, detail = _exception_parts(error)
                    await self._publish_diagnostic_safely(
                        "resource.cancel_stop_failed",
                        {
                            "internal_id": resource["internal_id"],
                            "error_type": error_type,
                            "error": detail,
                        },
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
            if not resource["terminal"] or not resource["monitor_closed"]:
                active.append(resource)
            elif resource["status"] == ResourceStatus.COMPLETED.value:
                successful.append(resource)
            else:
                failures.append(resource)
        return {"active": active, "failures": failures, "successful": successful}

    def _raise_for_stopped_blocking_monitors(self, summary: dict[str, Any]) -> None:
        for resource in summary["active"]:
            internal_id = str(resource["internal_id"])
            latest = self.store.get_resource(internal_id)
            if latest is None:
                continue
            if latest["terminal"] and latest["monitor_closed"]:
                continue
            task = self._monitor_tasks.get(internal_id)
            if task is None:
                raise RuntimeError(
                    f"Blocking resource {internal_id} has no active monitor task"
                )
            if not task.done():
                continue
            if task.cancelled():
                raise RuntimeError(
                    f"Blocking resource monitor {internal_id} was cancelled unexpectedly"
                )
            error = task.exception()
            if error is not None:
                raise RuntimeError(
                    f"Blocking resource monitor {internal_id} stopped unexpectedly"
                ) from error
            raise RuntimeError(
                f"Blocking resource monitor {internal_id} exited before the resource "
                "became terminal"
            )

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
                self._raise_for_stopped_blocking_monitors(summary)
                await asyncio.sleep(0.5)

        if timeout_seconds is None:
            return await wait()
        async with timeout(timeout_seconds):
            return await wait()

    async def close_nonblocking_monitors(self) -> None:
        resources = {
            item["internal_id"]: item for item in self.store.list_resources(self.run_id)
        }
        cancelled: list[tuple[str, asyncio.Task[None]]] = []
        for internal_id, task in self._monitor_tasks.items():
            resource = resources.get(internal_id)
            if resource and not resource.get("lifecycle", {}).get("blocking", False):
                if not task.done():
                    task.cancel()
                    cancelled.append((internal_id, task))
                self.store.mark_resource_monitor_closed(internal_id)
        if cancelled:
            await asyncio.gather(
                *(task for _internal_id, task in cancelled),
                return_exceptions=True,
            )
        for internal_id, resource in resources.items():
            if not resource.get("lifecycle", {}).get("blocking", False):
                await self._retire_resource_if_settled(internal_id)

    async def shutdown(self) -> None:
        async with self._shutdown_lock:
            if self._shutdown_complete:
                if self._shutdown_error is not None:
                    raise self._shutdown_error
                return
            self._closing = True
            tasks = list(self._monitor_tasks.values())
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            self._monitor_tasks.clear()

            adapters = list(self._adapters.values())
            self._adapters.clear()
            adapter_results = await asyncio.gather(
                *(adapter.close() for adapter in adapters),
                return_exceptions=True,
            )
            failures: list[BaseException] = [
                *self._retirement_failures,
                *(
                    result
                    for result in adapter_results
                    if isinstance(result, BaseException)
                ),
            ]
            self._retirement_failures.clear()

            if self._dashboard_links is not None:
                dashboard_result = await asyncio.gather(
                    self._dashboard_links.close(), return_exceptions=True
                )
                failures.extend(
                    result
                    for result in dashboard_result
                    if isinstance(result, BaseException)
                )
            service_result = await asyncio.gather(
                self._adapter_context.aclose(), return_exceptions=True
            )
            failures.extend(
                result for result in service_result if isinstance(result, BaseException)
            )

            self._shutdown_complete = True
            if failures:
                self._shutdown_error = RuntimeError(
                    "Resource cleanup failed: "
                    + "; ".join(str(item) for item in failures)
                )
                raise self._shutdown_error from failures[0]
