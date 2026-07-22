from __future__ import annotations

from datetime import datetime
from typing import Any, Literal, cast

import httpx
from pydantic import BaseModel, ConfigDict, Field

from .egress import SecretRedactor
from .models import (
    NotificationSettings,
    ResourceDisposition,
    ResourceStatus,
    RunStatus,
)
from .schema_versions import NOTIFICATION_SCHEMA_VERSION
from .storage import RunStore

NOTIFICATION_TITLE_MAX_CHARS = 160
NOTIFICATION_MESSAGE_MAX_CHARS = 1_024
NOTIFICATION_ERROR_MAX_CHARS = 512


class NotificationRunSummary(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(max_length=200)
    status: str = Field(max_length=32)
    current_cell_index: int | None = None
    active_resource_count: int = Field(default=0, ge=0)
    elapsed_seconds: float | None = Field(default=None, ge=0)


class NotificationCellFailure(BaseModel):
    model_config = ConfigDict(extra="forbid")

    cell_index: int = Field(ge=0)
    error_type: str | None = Field(default=None, max_length=160)


class NotificationSectionStart(BaseModel):
    model_config = ConfigDict(extra="forbid")

    heading: str = Field(max_length=200)
    heading_level: int = Field(ge=1, le=6)
    cell_index: int = Field(ge=0)


class NotificationResourceFailure(BaseModel):
    model_config = ConfigDict(extra="forbid")

    provider: str = Field(max_length=80)
    resource_type: str = Field(max_length=120)
    display_id: str | None = Field(default=None, max_length=200)
    status: Literal["failed"] = "failed"


class NotificationTerminal(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: Literal["succeeded", "failed", "cancelled"]
    reason: str | None = Field(default=None, max_length=80)
    elapsed_seconds: float | None = Field(default=None, ge=0)


class NotificationLegacy(BaseModel):
    model_config = ConfigDict(extra="forbid")

    note: Literal["Retained notification details were removed for safety"] = (
        "Retained notification details were removed for safety"
    )


NotificationData = (
    NotificationRunSummary
    | NotificationCellFailure
    | NotificationSectionStart
    | NotificationResourceFailure
    | NotificationTerminal
    | NotificationLegacy
)


class NotificationEnvelope(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1] = NOTIFICATION_SCHEMA_VERSION
    kind: Literal[
        "periodic_status",
        "cell_failed",
        "section_started",
        "resource_failed",
        "run_succeeded",
        "run_failed",
        "run_cancelled",
        "legacy",
    ]
    title: str = Field(max_length=NOTIFICATION_TITLE_MAX_CHARS)
    message: str = Field(max_length=NOTIFICATION_MESSAGE_MAX_CHARS)
    data: NotificationData

    def webhook_payload(self) -> dict[str, Any]:
        value = self.model_dump(mode="json")
        data = cast(dict[str, Any], value["data"])
        return {
            "title": value["title"],
            "message": value["message"],
            "data": {
                "schema_version": value["schema_version"],
                "kind": value["kind"],
                **data,
            },
        }


class PresentedNotification(BaseModel):
    model_config = ConfigDict(extra="forbid")

    envelope: NotificationEnvelope
    dedup_key: str | None = Field(default=None, max_length=512)
    rolling: bool = False
    destination_kinds: tuple[Literal["webhook", "ntfy"], ...] | None = None


class NotificationDeliveryError(BaseModel):
    model_config = ConfigDict(extra="forbid")

    code: Literal["timeout", "connect", "protocol", "http_status", "internal"]
    message: str = Field(max_length=NOTIFICATION_ERROR_MAX_CHARS)
    status_code: int | None = Field(default=None, ge=100, le=599)

    def persisted(self) -> str:
        return self.model_dump_json()


def safe_delivery_error(error: BaseException) -> NotificationDeliveryError:
    """Map an exception to a URL- and response-body-free persisted diagnostic."""

    if isinstance(error, httpx.HTTPStatusError):
        status_code = int(error.response.status_code)
        return NotificationDeliveryError(
            code="http_status",
            status_code=status_code,
            message=f"Notification endpoint returned HTTP {status_code}",
        )
    if isinstance(error, httpx.TimeoutException):
        return NotificationDeliveryError(
            code="timeout", message="Notification request timed out"
        )
    if isinstance(error, httpx.ConnectError):
        return NotificationDeliveryError(
            code="connect", message="Notification endpoint connection failed"
        )
    if isinstance(error, httpx.RequestError):
        return NotificationDeliveryError(
            code="protocol", message="Notification transport failed"
        )
    return NotificationDeliveryError(
        code="internal",
        message=f"Notification worker failed ({type(error).__name__})",
    )


def notification_redactor(
    settings: NotificationSettings, run: dict[str, Any] | None = None
) -> SecretRedactor:
    values: list[object] = [
        *settings.webhook_urls,
        settings.ntfy_base_url,
        settings.ntfy_topic,
    ]
    if run:
        values.extend((run.get("process_token"), run.get("kernel_id")))
    return SecretRedactor.from_values(values)


class NotificationPresenter:
    """Translate durable internal state into a small outbound schema."""

    def __init__(
        self,
        *,
        store: RunStore,
        run_id: str,
        settings: NotificationSettings,
    ) -> None:
        self.store = store
        self.run_id = run_id
        self.settings = settings

    def from_event(self, event: dict[str, Any]) -> PresentedNotification | None:
        event_type = event.get("type")
        payload_value = event.get("payload")
        if not isinstance(event_type, str) or not isinstance(payload_value, dict):
            raise TypeError(
                "Notification events require a string type and object payload"
            )
        payload = cast(dict[str, Any], payload_value)
        run = self.store.get_run(self.run_id)
        redactor = notification_redactor(self.settings, run)
        if event_type == "cell.failed":
            cell_index = _nonnegative_int(payload.get("cell_index"), "cell_index")
            attempt = _nonnegative_int(payload.get("attempt"), "attempt")
            epoch = _kernel_epoch(payload)
            error_type = _optional_text(
                redactor, payload.get("error_name"), max_chars=160
            )
            return PresentedNotification(
                envelope=NotificationEnvelope(
                    kind="cell_failed",
                    title="Runwatch: notebook cell failed",
                    message=f"Cell {cell_index + 1} failed"
                    + (f" ({error_type})." if error_type else "."),
                    data=NotificationCellFailure(
                        cell_index=cell_index, error_type=error_type
                    ),
                ),
                dedup_key=f"cell-failed:{epoch}:{cell_index}:{attempt}",
            )
        if event_type == "notebook.section_started":
            return self._section_start(payload, redactor)
        if event_type == "resource.observed" and payload.get("status") == "failed":
            internal_id = str(payload["internal_id"])
            resource = self.store.get_resource(internal_id)
            if resource is None or not _notifiable_failed_resource(resource):
                return None
            provider = redactor.text(resource.get("provider", "external"), max_chars=80)
            resource_type = redactor.text(
                resource.get("resource_type", "resource"), max_chars=120
            )
            return PresentedNotification(
                envelope=NotificationEnvelope(
                    kind="resource_failed",
                    title="Runwatch: external resource failed",
                    message=f"A {provider} {resource_type} resource failed.",
                    data=NotificationResourceFailure(
                        provider=provider,
                        resource_type=resource_type,
                        # Provider-neutral logical keys are often full local paths or
                        # S3 URIs. They are durable reconciliation identifiers, not
                        # safe outbound presentation labels.
                        display_id=None,
                    ),
                ),
                dedup_key=f"resource-failed:{internal_id}",
            )
        if event_type == "run.succeeded":
            epoch = _kernel_epoch(payload)
            return self._terminal(
                kind="run_succeeded",
                status="succeeded",
                title="Runwatch: run completed",
                message="Notebook and blocking resources completed successfully.",
                reason=None,
                dedup_key=f"run-terminal:succeeded:{epoch}",
                legacy_dedup_keys=(f"run-succeeded:{epoch}",),
                run=run,
            )
        if event_type in {
            "run.failed_external",
            "run.runner_error",
            "run.external_timeout",
        }:
            reason = {
                "run.failed_external": "external_resource_failure",
                "run.runner_error": "runner_error",
                "run.external_timeout": "external_timeout",
            }[event_type]
            epoch = _kernel_epoch(payload)
            return self._terminal(
                kind="run_failed",
                status="failed",
                title="Runwatch: run failed",
                message="The notebook run failed. Inspect retained Runwatch state for details.",
                reason=reason,
                dedup_key=f"run-terminal:failed:{epoch}",
                legacy_dedup_keys=tuple(
                    f"run-failed:{legacy_event}:{epoch}"
                    for legacy_event in (
                        event_type,
                        "run.runner_error",
                        "run.failed_external",
                        "run.external_timeout",
                    )
                ),
                run=run,
            )
        if event_type == "run.cancelled":
            epoch = _kernel_epoch(payload)
            return self._terminal(
                kind="run_cancelled",
                status="cancelled",
                title="Runwatch: run cancelled",
                message="The notebook run was cancelled.",
                reason=None,
                dedup_key=f"run-terminal:cancelled:{epoch}",
                legacy_dedup_keys=(f"run-cancelled:{epoch}",),
                run=run,
            )
        return None

    def reconcile_state(self) -> list[PresentedNotification]:
        """Build deduplicated notifications implied by current durable state."""

        run = self.store.get_run(self.run_id)
        epoch = max(0, int(run.get("kernel_epoch", 0)))
        events: list[dict[str, Any]] = []
        status = str(run.get("status", ""))
        if status == "succeeded":
            events.append({"type": "run.succeeded", "payload": {"kernel_epoch": epoch}})
        elif status == "failed":
            terminal_event = self.store.terminal_event_for_state(
                self.run_id, RunStatus.FAILED, epoch
            )
            events.append(
                terminal_event
                or {"type": "run.runner_error", "payload": {"kernel_epoch": epoch}}
            )
        elif status == "cancelled":
            events.append({"type": "run.cancelled", "payload": {"kernel_epoch": epoch}})
        failed_cell_index = run.get("failed_cell_index")
        failed_attempt = run.get("failed_attempt")
        if failed_cell_index is not None and failed_attempt is not None:
            events.append(
                {
                    "type": "cell.failed",
                    "payload": {
                        "cell_index": int(failed_cell_index),
                        "attempt": int(failed_attempt),
                        "kernel_epoch": epoch,
                    },
                }
            )
        for resource in self.store.list_resources(self.run_id):
            if _notifiable_failed_resource(resource):
                events.append(
                    {
                        "type": "resource.observed",
                        "payload": {
                            "internal_id": resource["internal_id"],
                            "status": "failed",
                        },
                    }
                )
        notifications: list[PresentedNotification] = []
        for event in events:
            notification = self.from_event(event)
            if notification is not None:
                notifications.append(notification)
        return notifications

    def periodic(self) -> PresentedNotification | None:
        summary = self.store.notification_run_summary(self.run_id)
        if summary["status"] in {"succeeded", "failed", "cancelled"}:
            return None
        run = self.store.get_run(self.run_id)
        redactor = notification_redactor(self.settings, run)
        name = redactor.text(summary["name"], max_chars=200)
        status = redactor.text(summary["status"], max_chars=32)
        cell_index = summary.get("current_cell_index")
        active = int(summary["active_resource_count"])
        cell_text = "no active cell" if cell_index is None else f"cell {cell_index + 1}"
        return PresentedNotification(
            envelope=NotificationEnvelope(
                kind="periodic_status",
                title="Runwatch status",
                message=(
                    f"{name}: {status}; {cell_text}; {active} active resource(s)."
                ),
                data=NotificationRunSummary(
                    name=name,
                    status=status,
                    current_cell_index=cell_index,
                    active_resource_count=active,
                    elapsed_seconds=summary.get("elapsed_seconds"),
                ),
            ),
            dedup_key="periodic-status",
            rolling=True,
        )

    def legacy(self) -> NotificationEnvelope:
        return NotificationEnvelope(
            kind="legacy",
            title="Runwatch notification",
            message="A retained Runwatch notification is ready.",
            data=NotificationLegacy(),
        )

    def _section_start(
        self, payload: dict[str, Any], redactor: SecretRedactor
    ) -> PresentedNotification | None:
        if not self.settings.ntfy_on_section_start:
            return None
        cell_index = _nonnegative_int(payload.get("cell_index"), "cell_index")
        heading_level = _heading_level(payload.get("heading_level"))
        heading = redactor.text(payload.get("heading", ""), max_chars=200)
        if not heading:
            raise ValueError("Notification section heading must not be empty")
        epoch = _kernel_epoch(payload)
        return PresentedNotification(
            envelope=NotificationEnvelope(
                kind="section_started",
                title="Runwatch: starting notebook section",
                message=f"Starting section: {heading}",
                data=NotificationSectionStart(
                    heading=heading,
                    heading_level=heading_level,
                    cell_index=cell_index,
                ),
            ),
            dedup_key=f"section-started:{epoch}:{cell_index}",
            destination_kinds=("ntfy",),
        )

    def _terminal(
        self,
        *,
        kind: Literal["run_succeeded", "run_failed", "run_cancelled"],
        status: Literal["succeeded", "failed", "cancelled"],
        title: str,
        message: str,
        reason: str | None,
        dedup_key: str,
        legacy_dedup_keys: tuple[str, ...],
        run: dict[str, Any],
    ) -> PresentedNotification:
        compatible_key = self.store.existing_notification_dedup_key(
            self.run_id, (dedup_key, *legacy_dedup_keys)
        )
        return PresentedNotification(
            envelope=NotificationEnvelope(
                kind=kind,
                title=title,
                message=message,
                data=NotificationTerminal(
                    status=status,
                    reason=reason,
                    elapsed_seconds=_elapsed_seconds(run),
                ),
            ),
            dedup_key=compatible_key or dedup_key,
        )


def _kernel_epoch(payload: dict[str, Any]) -> str:
    value = payload.get("kernel_epoch")
    if value is None:
        return "unknown"
    return str(_nonnegative_int(value, "kernel_epoch"))


def _notifiable_failed_resource(resource: dict[str, Any] | None) -> bool:
    return bool(
        resource is not None
        and resource.get("status") == ResourceStatus.FAILED.value
        and resource.get("terminal") is True
        and resource.get("disposition")
        not in {
            ResourceDisposition.SUPERSEDED.value,
            ResourceDisposition.IGNORED.value,
        }
    )


def _nonnegative_int(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"Notification event {field} must be nonnegative")
    return value


def _heading_level(value: object) -> int:
    level = _nonnegative_int(value, "heading_level")
    if level < 1 or level > 6:
        raise ValueError("Notification event heading_level must be between 1 and 6")
    return level


def _optional_text(
    redactor: SecretRedactor, value: object, *, max_chars: int
) -> str | None:
    if value is None:
        return None
    text = redactor.text(value, max_chars=max_chars).strip()
    return text or None


def _elapsed_seconds(run: dict[str, Any]) -> float | None:
    started = run.get("started_at")
    ended = run.get("ended_at") or run.get("updated_at")
    if not started or not ended:
        return None
    try:
        return max(
            0.0,
            (
                datetime.fromisoformat(str(ended))
                - datetime.fromisoformat(str(started))
            ).total_seconds(),
        )
    except ValueError:
        return None
