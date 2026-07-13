from __future__ import annotations

import math
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Literal, cast
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

SCHEMA_VERSION = 2
JSONDict = dict[str, Any]


def _validate_json_value(value: Any, *, path: str = "value") -> None:
    if value is None or isinstance(value, (str, bool, int)):
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"{path} must contain only finite numbers")
        return
    if isinstance(value, list):
        for index, item in enumerate(cast(list[Any], value)):
            _validate_json_value(item, path=f"{path}[{index}]")
        return
    if isinstance(value, dict):
        for key, item in cast(dict[Any, Any], value).items():
            if not isinstance(key, str):
                raise ValueError(f"{path} keys must be strings")
            _validate_json_value(item, path=f"{path}.{key}")
        return
    raise ValueError(f"{path} contains unsupported {type(value).__name__}")


class StrEnum(str, Enum):
    """Python 3.10-compatible string enum."""

    def __str__(self) -> str:
        return self.value


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class RunStatus(StrEnum):
    CREATED = "created"
    STARTING = "starting"
    RUNNING = "running"
    PAUSED = "paused"
    RESTARTING = "restarting"
    WAITING_EXTERNAL = "waiting_external"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLING = "cancelling"
    CANCELLED = "cancelled"

    @property
    def terminal(self) -> bool:
        return self in {self.SUCCEEDED, self.FAILED, self.CANCELLED}


class CellStatus(StrEnum):
    PENDING = "pending"
    NOT_REPLAYED = "not_replayed"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    INTERRUPTED = "interrupted"
    SKIPPED = "skipped"


class ResourceStatus(StrEnum):
    REGISTERED = "registered"
    DISCOVERING = "discovering"
    PENDING = "pending"
    RUNNING = "running"
    STOPPING = "stopping"
    COMPLETED = "completed"
    FAILED = "failed"
    STOPPED = "stopped"
    UNKNOWN = "unknown"
    MONITOR_ERROR = "monitor_error"

    @property
    def terminal(self) -> bool:
        return self in {self.COMPLETED, self.FAILED, self.STOPPED}


class ResourceDisposition(StrEnum):
    ACTIVE = "active"
    SUPERSEDED = "superseded"
    CANCELLED = "cancelled"
    IGNORED = "ignored"


class Ownership(StrEnum):
    EXCLUSIVE = "exclusive"
    BORROWED = "borrowed"
    EXTERNAL = "external"


class ActionKind(StrEnum):
    RESUME = "resume"
    RESTART = "restart"
    STOP_RESOURCE = "stop_resource"


class ActionStatus(StrEnum):
    REQUESTED = "requested"
    EXECUTING = "executing"
    COMPLETED = "completed"
    REJECTED = "rejected"
    FAILED = "failed"

    @property
    def terminal(self) -> bool:
        return self in {self.COMPLETED, self.REJECTED, self.FAILED}


class ResourceSpec(BaseModel):
    """Identify an AWS or local resource monitored by Runwatch.

    Attributes
    ----------
    provider:
        Resource provider namespace, such as ``aws`` or ``local``.
    type:
        Adapter-specific resource type.
    id:
        External provider identifier or local path.
    ownership:
        Whether Runwatch may mutate the resource during cancellation.
    metadata:
        Adapter-specific monitoring configuration.
    """

    model_config = ConfigDict(extra="forbid")

    provider: str
    type: str
    id: str
    logical_key: str | None = None
    region: str | None = None
    account_id: str | None = None
    ownership: Ownership = Ownership.BORROWED
    metadata: JSONDict = Field(default_factory=dict)

    @field_validator("provider", "type", "id")
    @classmethod
    def non_empty(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("must not be empty")
        return value

    @field_validator("metadata")
    @classmethod
    def metadata_is_json(cls, value: JSONDict) -> JSONDict:
        _validate_json_value(value, path="metadata")
        return value


class ResourceLifecycle(BaseModel):
    """Configure monitoring and cancellation behavior for a resource.

    Attributes
    ----------
    monitor:
        Whether Runwatch polls the resource.
    blocking:
        Whether successful run completion waits for the resource.
    stop_on_cancel:
        Whether cancellation requests provider stop when supported.
    retain_logs:
        Whether the adapter retains bounded log output.
    """

    model_config = ConfigDict(extra="forbid")

    monitor: bool = True
    blocking: bool = False
    stop_on_cancel: bool = False
    retain_logs: bool = True
    poll_interval_seconds: float | None = Field(default=None, gt=0)
    final_log_drain_seconds: float | None = Field(default=None, ge=0)
    max_consecutive_monitor_errors: int | None = Field(default=12, ge=1)

    @model_validator(mode="after")
    def blocking_requires_monitoring(self) -> ResourceLifecycle:
        if self.blocking and not self.monitor:
            raise ValueError("blocking resources must be monitored")
        return self


class ResourceEvent(BaseModel):
    """Register a typed resource emitted by a notebook cell.

    Attributes
    ----------
    resource:
        Provider identity and adapter metadata.
    lifecycle:
        Monitoring, blocking, and cancellation policy.
    event_id:
        Unique identifier used to deduplicate the emitted event.
    """

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[2] = 2
    event_id: str = Field(default_factory=lambda: str(uuid4()))
    event: Literal["resource_created"] = "resource_created"
    resource: ResourceSpec
    lifecycle: ResourceLifecycle = Field(default_factory=ResourceLifecycle)


class ProgressEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[2] = 2
    event_id: str = Field(default_factory=lambda: str(uuid4()))
    event: Literal["progress"] = "progress"
    completed: float = Field(ge=0)
    total: float | None = Field(default=None, gt=0)
    unit: str | None = None
    message: str | None = None
    metrics: JSONDict = Field(default_factory=dict)

    @field_validator("metrics")
    @classmethod
    def metrics_are_json(cls, value: JSONDict) -> JSONDict:
        _validate_json_value(value, path="metrics")
        return value

    @model_validator(mode="after")
    def completed_not_above_total(self) -> ProgressEvent:
        if self.total is not None and self.completed > self.total:
            raise ValueError("completed must not exceed total")
        return self


class ResourceRegistration(BaseModel):
    resource: ResourceSpec
    lifecycle: ResourceLifecycle = Field(default_factory=ResourceLifecycle)


class NotebookSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kernel_name: str | None = None
    timeout_seconds: int | None = Field(default=None, gt=0)
    startup_timeout_seconds: int = Field(default=60, gt=0)
    checkpoint_interval_seconds: float = Field(default=2.0, gt=0)
    wait_for_blocking_resources: bool = True
    resource_completion_timeout_seconds: float | None = Field(default=None, gt=0)


class AwsSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    profile_name: str | None = None
    region_name: str | None = None
    poll_interval_seconds: float = Field(default=15.0, gt=0)
    stop_timeout_seconds: float = Field(default=300.0, gt=0)
    final_log_drain_seconds: float = Field(default=3.0, ge=0)
    final_log_drain_max_pages: int = Field(default=20, ge=1, le=1_000)
    max_log_lines_per_poll: int = Field(default=250, ge=1, le=10_000)
    max_log_streams: int = Field(default=8, ge=1, le=100)


class ServerSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    host: str = "127.0.0.1"
    port: int = Field(default=8765, ge=1, le=65535)
    share: Literal["none", "lan", "cloudflared"] = "none"
    public_url: str | None = None
    open_browser: bool = True
    show_qr: bool = True
    cloudflared_binary: str = "cloudflared"
    linger_seconds: float | None = Field(default=None, ge=0)


class NotificationSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    webhook_urls: list[str] = Field(default_factory=list)
    ntfy_base_url: str | None = None
    ntfy_topic: str | None = None
    periodic_seconds: float | None = Field(default=None, gt=0)
    request_timeout_seconds: float = Field(default=15.0, gt=0)
    max_delivery_attempts: int = Field(default=4, ge=1, le=20)
    retry_initial_seconds: float = Field(default=1.0, gt=0)
    retry_max_seconds: float = Field(default=60.0, gt=0)

    @model_validator(mode="after")
    def complete_ntfy_pair(self) -> NotificationSettings:
        if bool(self.ntfy_base_url) != bool(self.ntfy_topic):
            raise ValueError("ntfy_base_url and ntfy_topic must be configured together")
        if self.retry_max_seconds < self.retry_initial_seconds:
            raise ValueError(
                "retry_max_seconds must be greater than or equal to "
                "retry_initial_seconds"
            )
        return self


class StorageSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    max_observations_per_resource: int = Field(default=10_000, ge=100)
    max_log_lines_per_resource: int = Field(default=2_000, ge=100)
    max_events_per_run: int = Field(default=10_000, ge=500)
    dashboard_chart_points: int = Field(default=300, ge=20, le=2_000)


class RunwatchConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[2] = 2
    notebook: NotebookSettings = Field(default_factory=NotebookSettings)
    aws: AwsSettings = Field(default_factory=AwsSettings)
    server: ServerSettings = Field(default_factory=ServerSettings)
    notifications: NotificationSettings = Field(default_factory=NotificationSettings)
    storage: StorageSettings = Field(default_factory=StorageSettings)
    resources: list[ResourceRegistration] = Field(
        default_factory=lambda: list[ResourceRegistration]()
    )


class ResourceObservation(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: ResourceStatus
    terminal: bool = False
    message: str | None = None
    metrics: JSONDict = Field(default_factory=dict)
    log_lines: list[str] = Field(default_factory=list)
    raw: JSONDict = Field(default_factory=dict)

    @field_validator("metrics", "raw")
    @classmethod
    def payloads_are_json(cls, value: JSONDict) -> JSONDict:
        _validate_json_value(value, path="resource observation")
        return value


class RunnerCommand(BaseModel):
    model_config = ConfigDict(extra="forbid")

    action_id: str
    kind: Literal["resume", "restart", "cancel"]
    from_cell: int = Field(default=0, ge=0)
    expected_kernel_epoch: int
    expected_failed_attempt: int | None = None
    requested_source_hash: str = ""


class RunAction(BaseModel):
    model_config = ConfigDict(extra="forbid")

    action_id: str
    run_id: str
    kind: ActionKind
    status: ActionStatus
    payload: JSONDict
    expected_kernel_epoch: int | None = None
    expected_cell_attempt: int | None = None
    expected_source_hash: str | None = None
    requested_at: datetime
    started_at: datetime | None = None
    finished_at: datetime | None = None
    message: str | None = None
    result: JSONDict = Field(default_factory=dict)
