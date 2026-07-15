from __future__ import annotations

from pathlib import Path
from typing import Any, Literal, cast

from pydantic import BaseModel, ConfigDict, Field

from .egress import SecretRedactor
from .models import NotificationSettings
from .schema_versions import CLI_PRESENTATION_SCHEMA_VERSION
from .storage import controller_is_alive


class CliRun(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(max_length=200)
    status: str = Field(max_length=32)
    message: str | None = Field(default=None, max_length=240)
    current_cell_index: int | None = None
    failed_cell_index: int | None = None
    kernel_epoch: int
    created_at: str
    updated_at: str
    started_at: str | None = None
    ended_at: str | None = None


class CliResourceCounts(BaseModel):
    model_config = ConfigDict(extra="forbid")

    total: int = Field(ge=0)
    active: int = Field(ge=0)


class CliStatus(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1] = CLI_PRESENTATION_SCHEMA_VERSION
    run: CliRun
    source_path: str
    resources: CliResourceCounts
    controller_live: bool


class CliFailedCell(BaseModel):
    model_config = ConfigDict(extra="forbid")

    cell_index: int = Field(ge=0)
    label: str | None = Field(default=None, max_length=160)
    status: str = Field(max_length=32)
    attempt: int = Field(ge=0)
    kernel_epoch: int = Field(ge=0)
    error_type: str | None = Field(default=None, max_length=160)


class CliResource(BaseModel):
    model_config = ConfigDict(extra="forbid")

    internal_id: str = Field(max_length=200)
    logical_key: str | None = Field(default=None, max_length=200)
    provider: str = Field(max_length=80)
    resource_type: str = Field(max_length=120)
    display_label: str = Field(max_length=240)
    ownership: str = Field(max_length=32)
    status: str = Field(max_length=32)
    terminal: bool
    supports_stop: bool


class CliEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1] = CLI_PRESENTATION_SCHEMA_VERSION
    seq: int = Field(ge=0)
    timestamp: str
    type: str = Field(max_length=160)
    data: dict[str, Any] = Field(default_factory=dict)


class CliSuggestedCommands(BaseModel):
    model_config = ConfigDict(extra="forbid")

    resume: str
    restart: str


class CliContext(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1] = CLI_PRESENTATION_SCHEMA_VERSION
    run: CliRun
    failed_cell: CliFailedCell | None = None
    resources: list[CliResource]
    recent_events: list[CliEvent]
    source_path: str
    suggested_commands: CliSuggestedCommands


class CliPresenter:
    """Allowlist persisted state for local human and agent CLI output."""

    def __init__(
        self,
        *,
        snapshot: dict[str, Any],
        settings: NotificationSettings,
        run_dir: Path,
    ) -> None:
        self.snapshot = snapshot
        self.run_dir = run_dir.resolve()
        run = cast(dict[str, Any], snapshot["run"])
        self.redactor = SecretRedactor.from_values(
            [
                *settings.webhook_urls,
                settings.ntfy_base_url,
                settings.ntfy_topic,
                run.get("process_token"),
                run.get("kernel_id"),
            ]
        )

    def status(self) -> CliStatus:
        run = cast(dict[str, Any], self.snapshot["run"])
        resources = cast(list[dict[str, Any]], self.snapshot.get("resources", []))
        return CliStatus(
            run=self._run(run),
            source_path=str(self.run_dir / "source.ipynb"),
            resources=CliResourceCounts(
                total=len(resources),
                active=sum(
                    not bool(resource.get("terminal"))
                    and resource.get("disposition") == "active"
                    for resource in resources
                ),
            ),
            controller_live=controller_is_alive(run),
        )

    def context(self) -> CliContext:
        run = cast(dict[str, Any], self.snapshot["run"])
        cells = cast(list[dict[str, Any]], self.snapshot.get("cells", []))
        failed = next(
            (
                cell
                for cell in cells
                if cell.get("cell_index") == run.get("failed_cell_index")
            ),
            None,
        )
        events = cast(list[dict[str, Any]], self.snapshot.get("events", []))[-40:]
        resources = cast(list[dict[str, Any]], self.snapshot.get("resources", []))
        source_path = str(self.run_dir / "source.ipynb")
        return CliContext(
            run=self._run(run),
            failed_cell=self._failed_cell(failed) if failed else None,
            resources=[self._resource(resource) for resource in resources],
            recent_events=[self.event(event) for event in events],
            source_path=source_path,
            suggested_commands=CliSuggestedCommands(
                resume=f"runwatch resume {self.run_dir}",
                restart=f"runwatch restart {self.run_dir}",
            ),
        )

    def event(self, event: dict[str, Any]) -> CliEvent:
        event_type = self.redactor.text(event.get("type", "event"), max_chars=160)
        payload = event.get("payload")
        data = self._event_data(
            event_type,
            cast(dict[str, Any], payload) if isinstance(payload, dict) else {},
        )
        return CliEvent(
            seq=max(0, int(event.get("seq", 0))),
            timestamp=self.redactor.text(event.get("timestamp", ""), max_chars=80),
            type=event_type,
            data=data,
        )

    def _run(self, run: dict[str, Any]) -> CliRun:
        return CliRun(
            name=self.redactor.text(run.get("name", "run"), max_chars=200),
            status=self.redactor.text(run.get("status", "unknown"), max_chars=32),
            message=_run_message(run),
            current_cell_index=run.get("current_cell_index"),
            failed_cell_index=run.get("failed_cell_index"),
            kernel_epoch=max(0, int(run.get("kernel_epoch", 0))),
            created_at=str(run.get("created_at", "")),
            updated_at=str(run.get("updated_at", "")),
            started_at=run.get("started_at"),
            ended_at=run.get("ended_at"),
        )

    def _failed_cell(self, cell: dict[str, Any]) -> CliFailedCell:
        error_name = cell.get("error_name")
        return CliFailedCell(
            cell_index=max(0, int(cell.get("cell_index", 0))),
            label=(
                self.redactor.text(cell["label"], max_chars=160)
                if isinstance(cell.get("label"), str)
                else None
            ),
            status=self.redactor.text(cell.get("status", "failed"), max_chars=32),
            attempt=max(0, int(cell.get("attempt", 0))),
            kernel_epoch=max(0, int(cell.get("kernel_epoch", 0))),
            error_type=(
                self.redactor.text(error_name, max_chars=160)
                if error_name is not None
                else None
            ),
        )

    def _resource(self, resource: dict[str, Any]) -> CliResource:
        provider = self.redactor.text(
            resource.get("provider", "external"), max_chars=80
        )
        resource_type = self.redactor.text(
            resource.get("resource_type", "resource"), max_chars=120
        )
        raw_logical_key = resource.get("logical_key")
        logical_key = (
            self.redactor.text(raw_logical_key, max_chars=200)
            if isinstance(raw_logical_key, str) and raw_logical_key.strip()
            else None
        )
        return CliResource(
            internal_id=self.redactor.text(
                resource.get("internal_id", "resource"), max_chars=200
            ),
            logical_key=logical_key,
            provider=provider,
            resource_type=resource_type,
            display_label=logical_key or f"{provider}:{resource_type}",
            ownership=self.redactor.text(
                resource.get("ownership", "borrowed"), max_chars=32
            ),
            status=self.redactor.text(resource.get("status", "unknown"), max_chars=32),
            terminal=bool(resource.get("terminal")),
            supports_stop=bool(resource.get("supports_stop")),
        )

    def _event_data(self, event_type: str, payload: dict[str, Any]) -> dict[str, Any]:
        allowed: set[str]
        if event_type.startswith("cell."):
            allowed = {"cell_index", "attempt", "kernel_epoch", "status", "error_name"}
        elif event_type.startswith("resource."):
            allowed = {"internal_id", "status", "terminal"}
        elif event_type.startswith("run."):
            allowed = {"kernel_epoch", "status"}
        elif event_type.startswith("action"):
            allowed = {"action_id", "kind", "status"}
        elif event_type.startswith("notification."):
            allowed = {"intent_id", "count", "outcome"}
        elif event_type == "notebook.progress":
            allowed = {"cell_index", "attempt", "completed", "total", "unit"}
        else:
            allowed = set()
        value = {key: payload[key] for key in allowed if key in payload}
        redacted = self.redactor.json(
            value, max_depth=2, max_items=12, max_string_chars=160
        )
        return cast(dict[str, Any], redacted) if isinstance(redacted, dict) else {}


def _run_message(run: dict[str, Any]) -> str | None:
    status = str(run.get("status", "unknown"))
    if status == "paused" and run.get("failed_cell_index") is not None:
        return "Run paused after a cell failure"
    messages = {
        "created": "Run created",
        "starting": "Notebook kernel is starting",
        "running": "Notebook is running",
        "restarting": "Notebook kernel is restarting",
        "waiting_external": "Waiting for external resources",
        "finalizing": "Final notebook state is being persisted",
        "succeeded": "Run completed successfully",
        "failed": "Run failed",
        "cancelling": "Cancellation is in progress",
        "cancelled": "Run was cancelled",
    }
    return messages.get(status)
