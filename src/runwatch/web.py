from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import os
from collections.abc import AsyncGenerator
from pathlib import Path
from sysconfig import get_path
from typing import Annotated, Any, Literal, cast
from urllib.parse import quote, urlsplit

from fastapi import (
    Cookie,
    Depends,
    FastAPI,
    Header,
    HTTPException,
    Query,
    Request,
    status,
)
from fastapi.responses import (
    FileResponse,
    HTMLResponse,
    JSONResponse,
    RedirectResponse,
    StreamingResponse,
)
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

from ._notebook_snapshot import (
    NotebookSnapshotChanged,
    NotebookSnapshotDescription,
    NotebookSnapshotRenderer,
    NotebookSnapshotRenderError,
    NotebookSnapshotTooLarge,
    NotebookSnapshotUnavailable,
)
from .dashboard_links import DASHBOARD_ACCESS_COOKIE
from .models import NotificationSettings
from .resource_manager import ResourceStopRejected
from .schema_versions import DASHBOARD_SCHEMA_VERSION
from .supervisor import RunSupervisor
from .tunnel import with_token

_COOKIE_NAME = DASHBOARD_ACCESS_COOKIE
_MASCOT_ASSET_NAMES = frozenset(
    {
        "alert.png",
        "confused.png",
        "inspecting.png",
        "phrases.json",
        "ready.png",
        "running.png",
        "sleeping.png",
        "success.png",
        "waiting.png",
    }
)
_REQUIRED_WEB_ARTIFACTS = (
    Path("common/neumorphic-gloss-components.css"),
    Path("runwatch/index.html"),
    Path("runwatch/notebook.html"),
    Path("runwatch/notebook.js"),
    Path("runwatch/app.js"),
    Path("runwatch/styles.css"),
    *(
        Path("runwatch/mascot") / asset_name
        for asset_name in sorted(_MASCOT_ASSET_NAMES)
    ),
)


class StopRequest(BaseModel):
    confirmation: str
    expected_version: int


class DashboardRun(BaseModel):
    name: str
    status: str
    message: str | None = None
    current_cell_index: int | None = None
    created_at: str
    updated_at: str
    started_at: str | None = None
    ended_at: str | None = None


class DashboardCell(BaseModel):
    cell_index: int
    cell_type: str
    label: str | None = None
    status: str
    attempt: int
    started_at: str | None = None
    ended_at: str | None = None
    elapsed_seconds: float | None = None
    error_name: str | None = None
    error_value: str | None = None
    traceback: list[str]
    output_tail: list[dict[str, Any]]


class DashboardObservation(BaseModel):
    timestamp: str
    status: str
    message: str | None = None
    metrics: dict[str, Any]


class DashboardLink(BaseModel):
    status: str
    href: str | None = None
    label: str | None = None
    message: str | None = None


class DashboardResource(BaseModel):
    internal_id: str
    cell_index: int | None = None
    attempt: int | None = None
    provider: str
    resource_type: str
    external_id: str
    region: str | None = None
    ownership: str
    lifecycle: dict[str, Any]
    metadata: dict[str, Any]
    supports_stop: bool
    status: str
    terminal: bool
    monitor_closed: bool
    disposition: str
    version: int
    message: str | None = None
    metrics: dict[str, Any]
    log_tail: list[str]
    created_at: str
    updated_at: str
    observations: list[DashboardObservation]
    link: DashboardLink | None = None


class DashboardEvent(BaseModel):
    seq: int
    timestamp: str
    type: str
    payload: dict[str, Any]


class DashboardCapabilities(BaseModel):
    controller_live: bool


class _NotebookSnapshotPresentation(BaseModel):
    available: bool
    kind: Literal["source", "checkpoint", "final"] | None = None
    updated_at: str | None = None
    settled_code_cells: int
    code_cell_count: int
    current_cell_number: int | None = None
    current_cell_incomplete: bool


class DashboardState(BaseModel):
    schema_version: Literal[1] = DASHBOARD_SCHEMA_VERSION
    run: DashboardRun
    cells: list[DashboardCell]
    resources: list[DashboardResource]
    events: list[DashboardEvent]
    capabilities: DashboardCapabilities


_RESOURCE_METRIC_FIELDS: dict[str, frozenset[str]] = {
    "sagemaker_processing_job": frozenset(
        {
            "instance_count",
            "processing_instance_count",
            "cluster_instance_count",
            "instance_type",
            "processing_instance_type",
            "cluster_instance_type",
            "volume_size_gb",
            "volume_size_in_gb",
            "processing_volume_size_gb",
            "start_time",
            "creation_time",
            "end_time",
        }
    ),
    "file_count": frozenset(
        {"file_count", "expected_count", "total_bytes", "latest_modified_at"}
    ),
    "line_count": frozenset(
        {
            "line_count",
            "expected_lines",
            "lines_per_second",
            "bytes",
            "modified_at",
            "partial_line_pending",
        }
    ),
    "s3_prefix": frozenset(
        {
            "object_count",
            "expected_count",
            "total_bytes",
            "scan_in_progress",
            "latest_object_time",
            "bucket",
            "prefix",
        }
    ),
    "s3_manifest": frozenset({"completed", "total", "exists", "key"}),
    "system_metrics": frozenset(
        {
            "host_cpu_percent",
            "host_memory_percent",
            "kernel_cpu_percent",
            "kernel_memory_rss_bytes",
            "disk_percent",
            "gpu_0_utilization_percent",
        }
    ),
    "cloudwatch_metric": frozenset(
        {
            "latest_value",
            "latest_unit",
            "latest_timestamp",
            "statistic",
            "metric_name",
            "series",
        }
    ),
    "cloudwatch_logs": frozenset({"stream_count"}),
}
_GENERIC_METRIC_FIELDS = frozenset(
    {"completed", "total", "unit", "count", "rate", "throughput", "percent"}
)
_PROGRESS_METRIC_FIELDS = frozenset(
    {
        "source",
        "position",
        "progress_id",
        "closed",
        "rate",
        "items_per_second",
        "lines_per_second",
        "rows_per_second",
        "batches_per_second",
        "throughput",
    }
)


class DashboardAuth:
    def __init__(self, token: str) -> None:
        self.token = token

    def valid(self, candidate: str | None) -> bool:
        return candidate is not None and hmac.compare_digest(candidate, self.token)


def _web_artifacts_root(explicit_root: Path | None = None) -> Path:
    if explicit_root is not None:
        candidates = [explicit_root]
    else:
        install_data = get_path("data")
        candidates = [Path(__file__).resolve().parents[2] / "web_artifacts"]
        if install_data:
            candidates.append(
                Path(install_data) / "share" / "runwatch" / "web_artifacts"
            )
        candidates.append(Path.cwd() / "web_artifacts")

    checked: list[Path] = []
    for candidate in candidates:
        resolved = candidate.resolve()
        if resolved in checked:
            continue
        checked.append(resolved)
        if all(
            resolved.joinpath(relative).is_file()
            for relative in _REQUIRED_WEB_ARTIFACTS
        ):
            return resolved

    locations = ", ".join(str(path) for path in checked)
    raise RuntimeError(
        "Runwatch web artifacts are incomplete or missing. " f"Checked: {locations}"
    )


def _supervisor(request: Request) -> RunSupervisor:
    return cast(RunSupervisor, request.app.state.runwatch_supervisor)


def _auth(request: Request) -> DashboardAuth:
    return cast(DashboardAuth, request.app.state.runwatch_auth)


def _notebook_renderer(request: Request) -> NotebookSnapshotRenderer:
    return cast(
        NotebookSnapshotRenderer,
        request.app.state.runwatch_notebook_renderer,
    )


def _ntfy_deep_link(settings: NotificationSettings) -> str | None:
    if not settings.ntfy_base_url or not settings.ntfy_topic:
        return None
    parts = urlsplit(settings.ntfy_base_url)
    if parts.scheme not in {"http", "https"} or not parts.hostname:
        return None
    host = f"[{parts.hostname}]" if ":" in parts.hostname else parts.hostname
    try:
        port = parts.port
    except ValueError:
        return None
    authority = f"{host}:{port}" if port is not None else host
    base_path = quote(parts.path.rstrip("/"), safe="/-._~%")
    topic = quote(settings.ntfy_topic, safe="-_")
    secure = "" if parts.scheme == "https" else "?secure=false"
    return f"ntfy://{authority}{base_path}/{topic}{secure}"


async def _require_auth(
    request: Request,
    cookie: Annotated[str | None, Cookie(alias=_COOKIE_NAME)] = None,
    authorization: Annotated[str | None, Header()] = None,
    token: Annotated[str | None, Query()] = None,
) -> None:
    bearer = None
    if authorization and authorization.lower().startswith("bearer "):
        bearer = authorization[7:].strip()
    if not any(_auth(request).valid(value) for value in (cookie, bearer, token)):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Unauthorized"
        )


async def _health(_request: Request) -> dict[str, str]:
    return {"status": "ok"}


async def _dashboard(
    request: Request,
    token: str | None = Query(default=None),
    cookie: Annotated[str | None, Cookie(alias=_COOKIE_NAME)] = None,
) -> Any:
    auth = _auth(request)
    pairing = auth.valid(token)
    if not pairing and not auth.valid(cookie):
        return HTMLResponse(
            "<h1>Runwatch</h1><p>This dashboard requires its pairing URL.</p>",
            status_code=401,
        )
    supervisor = _supervisor(request)
    templates = cast(Jinja2Templates, request.app.state.runwatch_templates)
    response = templates.TemplateResponse(
        request=request,
        name="index.html",
        context={
            "run_id": supervisor.run_id,
            "run_name": supervisor.name,
            "asset_version": request.app.state.runwatch_asset_version,
            "mascot_showcase": request.app.state.runwatch_mascot_showcase,
            "ntfy_enabled": _ntfy_deep_link(supervisor.config.notifications)
            is not None,
        },
    )
    response.headers["Cache-Control"] = "no-store"
    if pairing:
        response.set_cookie(
            _COOKIE_NAME,
            auth.token,
            httponly=True,
            samesite="strict",
            secure=request.url.scheme == "https",
            max_age=60 * 60 * 24,
        )
    return response


def _bounded_text(value: Any, *, limit: int = 32_768) -> str:
    text = "" if value is None else str(value)
    if len(text) <= limit:
        return text
    return text[-limit:]


def _dashboard_output(output: dict[str, Any]) -> dict[str, Any]:
    output_type = str(output.get("output_type", "output"))
    if output_type == "error":
        return {
            "output_type": output_type,
            "traceback": [
                _bounded_text(line) for line in output.get("traceback", [])[-50:]
            ],
        }
    text = output.get("text")
    if text is None and isinstance(output.get("data"), dict):
        text = output["data"].get("text/plain")
    return {"output_type": output_type, "text": _bounded_text(text)}


def _dashboard_cell_label(cell: dict[str, Any]) -> str | None:
    label = cell.get("label")
    if not isinstance(label, str):
        return None
    source = str(cell.get("source", ""))
    first_source_line = next(
        (line.strip() for line in source.splitlines() if line.strip()), ""
    )
    if cell.get("cell_type") == "code" and label == first_source_line[:120]:
        return None
    return label


def _dashboard_cell(cell: dict[str, Any]) -> DashboardCell:
    return DashboardCell(
        cell_index=int(cell["cell_index"]),
        cell_type=str(cell["cell_type"]),
        label=_dashboard_cell_label(cell),
        status=str(cell["status"]),
        attempt=int(cell["attempt"]),
        started_at=cell.get("started_at"),
        ended_at=cell.get("ended_at"),
        elapsed_seconds=cell.get("elapsed_seconds"),
        error_name=cell.get("error_name"),
        error_value=cell.get("error_value"),
        traceback=[_bounded_text(line) for line in cell.get("traceback", [])[-50:]],
        output_tail=[
            _dashboard_output(output) for output in cell.get("output_tail", [])[-20:]
        ],
    )


def _dashboard_metrics(resource_type: str, metrics: dict[str, Any]) -> dict[str, Any]:
    allowed = _RESOURCE_METRIC_FIELDS.get(resource_type, _GENERIC_METRIC_FIELDS)
    sanitized = {key: value for key, value in metrics.items() if key in allowed}
    series = sanitized.get("series")
    if isinstance(series, list):
        points: list[dict[str, Any]] = []
        for raw_point in cast(list[object], series)[-2_000:]:
            point = _string_dict(raw_point)
            if point:
                points.append(
                    {
                        key: point[key]
                        for key in ("timestamp", "value", "unit")
                        if key in point
                    }
                )
        sanitized["series"] = points
    return sanitized


def _dashboard_external_id(resource: dict[str, Any]) -> str:
    external_id = str(resource.get("external_id", ""))
    if resource.get("provider") != "local":
        return external_id
    resource_type = resource.get("resource_type")
    if resource_type == "dashboard":
        return str(resource.get("logical_key") or "dashboard")
    if resource_type in {"file_count", "line_count"}:
        return Path(external_id).name
    return str(resource_type or "local")


def _dashboard_metadata(resource: dict[str, Any]) -> dict[str, Any]:
    metadata = _string_dict(resource.get("metadata"))
    if not metadata:
        return {}
    allowed = (
        {"name"}
        if resource.get("resource_type") == "dashboard"
        else {"instance_count", "instance_type", "volume_size_gb"}
    )
    return {key: value for key, value in metadata.items() if key in allowed}


def _dashboard_resource(resource: dict[str, Any]) -> DashboardResource:
    resource_type = str(resource["resource_type"])
    observations = [
        DashboardObservation(
            timestamp=str(observation["timestamp"]),
            status=str(observation["status"]),
            message=observation.get("message"),
            metrics=_dashboard_metrics(resource_type, observation.get("metrics", {})),
        )
        for observation in resource.get("observations", [])
    ]
    lifecycle = _string_dict(resource.get("lifecycle"))
    link = _string_dict(resource.get("link"))
    dashboard_link = (
        DashboardLink(
            status=str(link["status"]),
            href=link.get("href"),
            label=link.get("label"),
            message=link.get("message"),
        )
        if "status" in link
        else None
    )
    return DashboardResource(
        internal_id=str(resource["internal_id"]),
        cell_index=resource.get("cell_index"),
        attempt=resource.get("attempt"),
        provider=str(resource["provider"]),
        resource_type=resource_type,
        external_id=_dashboard_external_id(resource),
        region=resource.get("region"),
        ownership=str(resource["ownership"]),
        lifecycle={"stop_on_cancel": bool(lifecycle.get("stop_on_cancel", False))},
        metadata=_dashboard_metadata(resource),
        supports_stop=bool(resource["supports_stop"]),
        status=str(resource["status"]),
        terminal=bool(resource["terminal"]),
        monitor_closed=bool(resource["monitor_closed"]),
        disposition=str(resource["disposition"]),
        version=int(resource["version"]),
        message=resource.get("message"),
        metrics=_dashboard_metrics(resource_type, resource.get("metrics", {})),
        log_tail=[_bounded_text(line) for line in resource.get("log_tail", [])[-35:]],
        created_at=str(resource["created_at"]),
        updated_at=str(resource["updated_at"]),
        observations=observations,
        link=dashboard_link,
    )


def _dashboard_event_payload(event: dict[str, Any]) -> dict[str, Any]:
    payload = _string_dict(event.get("payload"))
    if not payload:
        return {}
    event_type = str(event.get("type", ""))
    if event_type == "notebook.progress":
        value = {
            key: payload[key]
            for key in (
                "cell_index",
                "attempt",
                "completed",
                "total",
                "unit",
                "message",
            )
            if key in payload
        }
        metrics = _string_dict(payload.get("metrics"))
        if metrics:
            value["metrics"] = {
                key: metric
                for key, metric in metrics.items()
                if key in _PROGRESS_METRIC_FIELDS
            }
        return value
    allowed = {"internal_id", "status", "message", "error"}
    value = {key: payload[key] for key in allowed if key in payload}
    for key in ("message", "error"):
        if key in value:
            value[key] = _bounded_text(value[key])
    return value


def _string_dict(value: object) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    mapping = cast(dict[object, object], value)
    return {key: item for key, item in mapping.items() if isinstance(key, str)}


_SETTLED_CELL_STATUSES = frozenset({"succeeded", "failed", "interrupted", "skipped"})
_FINAL_NOTEBOOK_RUN_STATUSES = frozenset(
    {"waiting_external", "finalizing", "succeeded"}
)


def _use_final_notebook(raw: dict[str, Any]) -> bool:
    run = _string_dict(raw.get("run"))
    return run.get("status") in _FINAL_NOTEBOOK_RUN_STATUSES


def _dashboard_notebook_snapshot(
    raw: dict[str, Any],
    description: NotebookSnapshotDescription | None,
) -> _NotebookSnapshotPresentation:
    cells = [
        _string_dict(cast(object, cell))
        for cell in cast(list[object], raw.get("cells", []))
        if isinstance(cell, dict)
    ]
    code_cells = [cell for cell in cells if cell.get("cell_type") == "code"]
    settled = sum(cell.get("status") in _SETTLED_CELL_STATUSES for cell in code_cells)
    run = _string_dict(raw.get("run"))
    current_cell_index = run.get("current_cell_index")
    current = next(
        (cell for cell in code_cells if cell.get("cell_index") == current_cell_index),
        None,
    )
    return _NotebookSnapshotPresentation(
        available=description is not None,
        kind=description.kind if description is not None else None,
        updated_at=description.updated_at if description is not None else None,
        settled_code_cells=settled,
        code_cell_count=len(code_cells),
        current_cell_number=(
            int(current_cell_index) + 1 if isinstance(current_cell_index, int) else None
        ),
        current_cell_incomplete=(
            current is not None and current.get("status") not in _SETTLED_CELL_STATUSES
        ),
    )


def _dashboard_state(supervisor: RunSupervisor) -> DashboardState:
    raw = supervisor.snapshot()
    run = raw["run"]
    return DashboardState(
        run=DashboardRun(
            name=str(run["name"]),
            status=str(run["status"]),
            message=run.get("message"),
            current_cell_index=run.get("current_cell_index"),
            created_at=str(run["created_at"]),
            updated_at=str(run["updated_at"]),
            started_at=run.get("started_at"),
            ended_at=run.get("ended_at"),
        ),
        cells=[_dashboard_cell(cell) for cell in raw["cells"]],
        resources=[_dashboard_resource(resource) for resource in raw["resources"]],
        events=[
            DashboardEvent(
                seq=int(event["seq"]),
                timestamp=str(event["timestamp"]),
                type=str(event["type"]),
                payload=_dashboard_event_payload(event),
            )
            for event in raw["events"]
        ],
        capabilities=DashboardCapabilities(
            controller_live=bool(raw["capabilities"]["controller_live"])
        ),
    )


async def _state_snapshot(request: Request) -> JSONResponse:
    state = _dashboard_state(_supervisor(request))
    return JSONResponse(
        state.model_dump(mode="json"), headers={"Cache-Control": "no-store"}
    )


async def _notebook_snapshot_view(request: Request) -> Any:
    supervisor = _supervisor(request)
    raw = supervisor.snapshot()
    content_href: str | None = None
    error_message: str | None = None
    response_status = status.HTTP_200_OK
    try:
        rendered = await _notebook_renderer(request).render(
            use_final=_use_final_notebook(raw)
        )
    except NotebookSnapshotUnavailable:
        description = None
        error_message = "No notebook snapshot is available yet."
        response_status = status.HTTP_404_NOT_FOUND
    except NotebookSnapshotTooLarge:
        description = None
        error_message = "This notebook is too large to render safely."
        response_status = status.HTTP_413_REQUEST_ENTITY_TOO_LARGE
    except NotebookSnapshotRenderError:
        description = None
        error_message = "Runwatch could not render this notebook snapshot."
        response_status = status.HTTP_422_UNPROCESSABLE_CONTENT
    else:
        description = rendered.description
        content_href = f"/api/notebook/render?digest={rendered.digest}"
    snapshot = _dashboard_notebook_snapshot(raw, description)
    templates = cast(Jinja2Templates, request.app.state.runwatch_templates)
    response = templates.TemplateResponse(
        request=request,
        name="notebook.html",
        context={
            "run_name": supervisor.name,
            "asset_version": request.app.state.runwatch_asset_version,
            "snapshot": snapshot,
            "content_href": content_href,
            "error_message": error_message,
        },
        status_code=response_status,
    )
    response.headers["Cache-Control"] = "no-store"
    return response


async def _render_notebook_snapshot(
    request: Request,
    digest: Annotated[
        str,
        Query(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$"),
    ],
) -> HTMLResponse:
    raw = _supervisor(request).snapshot()
    try:
        rendered = await _notebook_renderer(request).render(
            use_final=_use_final_notebook(raw),
            expected_digest=digest,
        )
    except NotebookSnapshotUnavailable:
        return HTMLResponse(
            "<h1>Notebook snapshot unavailable</h1>",
            status_code=404,
            headers={"Cache-Control": "no-store"},
        )
    except NotebookSnapshotChanged:
        return HTMLResponse(
            "<h1>A newer notebook snapshot is available</h1>"
            "<p>Refresh the snapshot page to view it.</p>",
            status_code=409,
            headers={"Cache-Control": "no-store"},
        )
    except NotebookSnapshotTooLarge:
        return HTMLResponse(
            "<h1>Notebook snapshot is too large to render safely</h1>",
            status_code=413,
            headers={"Cache-Control": "no-store"},
        )
    except NotebookSnapshotRenderError:
        return HTMLResponse(
            "<h1>Notebook snapshot could not be rendered</h1>",
            status_code=422,
            headers={"Cache-Control": "no-store"},
        )
    return HTMLResponse(
        rendered.html,
        headers={"Cache-Control": "no-store"},
    )


async def _open_ntfy(request: Request) -> RedirectResponse:
    deep_link = _ntfy_deep_link(_supervisor(request).config.notifications)
    if deep_link is None:
        raise HTTPException(404, "ntfy notifications are not configured")
    return RedirectResponse(
        deep_link, status_code=303, headers={"Cache-Control": "no-store"}
    )


async def _open_resource_dashboard(
    request: Request, internal_id: str
) -> RedirectResponse:
    supervisor = _supervisor(request)
    resource = supervisor.store.get_resource(internal_id)
    if resource is None:
        raise HTTPException(404, "Resource not found")
    if resource["provider"] != "local" or resource["resource_type"] != "dashboard":
        raise HTTPException(404, "Resource is not a linked dashboard")
    if resource["disposition"] != "active":
        raise HTTPException(409, "Only active dashboards can be opened")
    try:
        target, requires_token = supervisor.dashboard_link_target(internal_id)
    except KeyError as error:
        raise HTTPException(404, "Linked dashboard is not registered") from error
    except RuntimeError as error:
        raise HTTPException(409, str(error)) from error
    if requires_token:
        target = with_token(target, _auth(request).token)
    return RedirectResponse(
        target, status_code=303, headers={"Cache-Control": "no-store"}
    )


def _event_cursor(request: Request, supervisor: RunSupervisor) -> tuple[int, bool]:
    last_event_id = request.headers.get("last-event-id")
    try:
        requested = max(0, int(last_event_id or "0"))
    except ValueError:
        requested = 0
    latest = supervisor.store.recent_events(supervisor.run_id, limit=1)
    maximum = int(latest[-1]["seq"]) if latest else 0
    return min(requested, maximum), last_event_id is not None


def _event_message(event: dict[str, Any]) -> str:
    sequence = event.get("seq")
    prefix = "" if sequence is None else f"id: {int(sequence)}\n"
    return f"{prefix}event: runwatch\ndata: {json.dumps(event)}\n\n"


def _dashboard_event_signal(event: dict[str, Any]) -> dict[str, Any]:
    return {
        key: event[key]
        for key in ("seq", "timestamp", "type")
        if event.get(key) is not None
    }


def _events_after(
    supervisor: RunSupervisor,
    cursor: int,
    *,
    through: int | None = None,
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    while True:
        page = supervisor.store.events_after(supervisor.run_id, cursor, limit=1_000)
        if not page:
            return events
        for event in page:
            sequence = int(event["seq"])
            if through is not None and sequence > through:
                return events
            events.append(event)
            cursor = sequence
        if len(page) < 1_000:
            return events


def _delivery_batch(
    supervisor: RunSupervisor,
    event: dict[str, Any],
    cursor: int,
) -> list[dict[str, Any]]:
    sequence_value = event.get("seq")
    if sequence_value is None:
        return [event]
    sequence = int(sequence_value)
    if sequence <= cursor:
        return []
    if sequence == cursor + 1:
        return [event]
    recovered = _events_after(supervisor, cursor, through=sequence)
    if recovered and int(recovered[-1]["seq"]) >= sequence:
        return recovered
    return [*recovered, event]


async def _event_stream(request: Request) -> AsyncGenerator[str, None]:
    supervisor = _supervisor(request)
    cursor, should_replay = _event_cursor(request, supervisor)
    async with supervisor.bus.subscribe() as queue:
        yield _event_message({"type": "connected"})
        if should_replay:
            for event in _events_after(supervisor, cursor):
                cursor = int(event["seq"])
                yield _event_message(_dashboard_event_signal(event))
        while not await request.is_disconnected():
            try:
                event = await asyncio.wait_for(queue.get(), timeout=15)
                for delivered in _delivery_batch(supervisor, event, cursor):
                    if delivered.get("seq") is not None:
                        cursor = int(delivered["seq"])
                    yield _event_message(_dashboard_event_signal(delivered))
            except TimeoutError:
                yield ": keepalive\n\n"


async def _stream_events(request: Request) -> StreamingResponse:
    return StreamingResponse(
        _event_stream(request),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-store", "X-Accel-Buffering": "no"},
    )


async def _stop_resource(
    request: Request, internal_id: str, body: StopRequest
) -> dict[str, str]:
    supervisor = _supervisor(request)
    if body.confirmation != "STOP RESOURCE AND CANCEL RUN":
        raise HTTPException(400, "Invalid stop confirmation")
    resource = supervisor.store.get_resource(internal_id)
    if resource is None:
        raise HTTPException(404, "Resource not found")
    try:
        supervisor.resources.validate_stop_eligibility(
            internal_id, expected_version=body.expected_version
        )
    except ResourceStopRejected as error:
        raise HTTPException(409, str(error)) from error
    if not supervisor.snapshot()["capabilities"]["controller_live"]:
        raise HTTPException(409, "Runwatch is not live; use the local CLI")
    action_id = supervisor.create_stop_action(
        internal_id, expected_version=body.expected_version
    )
    return {"status": "accepted", "action_id": action_id}


def create_app(
    supervisor: RunSupervisor,
    access_token: str,
    *,
    web_artifacts_root: Path | None = None,
    mascot_showcase: bool | None = None,
) -> FastAPI:
    if mascot_showcase is None:
        mascot_showcase = os.environ.get("RUNWATCH_MASCOT_SHOWCASE") == "1"
    artifacts_root = _web_artifacts_root(web_artifacts_root)
    runwatch_assets = artifacts_root / "runwatch"
    asset_digest = hashlib.sha256()
    for relative in _REQUIRED_WEB_ARTIFACTS:
        asset_digest.update(relative.as_posix().encode())
        asset_digest.update(artifacts_root.joinpath(relative).read_bytes())
    templates = Jinja2Templates(directory=str(runwatch_assets))
    app = FastAPI(title="Runwatch", docs_url=None, redoc_url=None)
    app.state.runwatch_supervisor = supervisor
    app.state.runwatch_auth = DashboardAuth(access_token)
    app.state.runwatch_notebook_renderer = NotebookSnapshotRenderer(
        source_path=supervisor.source_path,
        partial_output_path=supervisor.partial_output_path,
        output_path=supervisor.output_path,
    )
    app.state.runwatch_templates = templates
    app.state.runwatch_asset_version = asset_digest.hexdigest()[:12]
    app.state.runwatch_mascot_showcase = mascot_showcase

    async def security_headers(request: Request, call_next: Any) -> Any:
        response = await call_next(request)
        notebook_content = request.url.path == "/api/notebook/render"
        if notebook_content:
            response.headers["Content-Security-Policy"] = (
                "sandbox allow-same-origin; default-src 'none'; script-src 'none'; "
                "style-src 'unsafe-inline'; img-src data:; font-src data:; "
                "connect-src 'none'; media-src data:; object-src 'none'; "
                "base-uri 'none'; form-action 'none'; frame-src 'none'; "
                "frame-ancestors 'self'"
            )
        else:
            response.headers["Content-Security-Policy"] = (
                "default-src 'self'; script-src 'self'; style-src 'self'; "
                "img-src 'self' data:; connect-src 'self'; object-src 'none'; "
                "base-uri 'none'; frame-ancestors 'none'; form-action 'self'"
            )
        response.headers["Referrer-Policy"] = "no-referrer"
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = (
            "SAMEORIGIN" if notebook_content else "DENY"
        )
        response.headers["Permissions-Policy"] = (
            "camera=(), microphone=(), geolocation=()"
        )
        return response

    app.middleware("http")(security_headers)

    async def common_stylesheet() -> FileResponse:
        return FileResponse(artifacts_root / "common/neumorphic-gloss-components.css")

    async def runwatch_stylesheet() -> FileResponse:
        return FileResponse(runwatch_assets / "styles.css")

    async def runwatch_script() -> FileResponse:
        return FileResponse(runwatch_assets / "app.js")

    async def runwatch_notebook_script() -> FileResponse:
        return FileResponse(runwatch_assets / "notebook.js")

    async def runwatch_mascot_asset(asset_name: str) -> FileResponse:
        if asset_name not in _MASCOT_ASSET_NAMES:
            raise HTTPException(404, "Mascot asset not found")
        return FileResponse(runwatch_assets / "mascot" / asset_name)

    auth_dependency = [Depends(_require_auth)]
    app.add_api_route("/health", _health, methods=["GET"])
    app.add_api_route(
        "/static/common/neumorphic-gloss-components.css",
        common_stylesheet,
        methods=["GET"],
    )
    app.add_api_route(
        "/static/runwatch/styles.css", runwatch_stylesheet, methods=["GET"]
    )
    app.add_api_route("/static/runwatch/app.js", runwatch_script, methods=["GET"])
    app.add_api_route(
        "/static/runwatch/notebook.js",
        runwatch_notebook_script,
        methods=["GET"],
    )
    app.add_api_route(
        "/static/runwatch/mascot/{asset_name}",
        runwatch_mascot_asset,
        methods=["GET"],
    )
    app.add_api_route("/", _dashboard, methods=["GET"], response_class=HTMLResponse)
    app.add_api_route(
        "/notebook",
        _notebook_snapshot_view,
        methods=["GET"],
        dependencies=auth_dependency,
        response_class=HTMLResponse,
    )
    app.add_api_route(
        "/api/notebook/render",
        _render_notebook_snapshot,
        methods=["GET"],
        dependencies=auth_dependency,
        response_class=HTMLResponse,
    )
    app.add_api_route(
        "/api/state", _state_snapshot, methods=["GET"], dependencies=auth_dependency
    )
    app.add_api_route(
        "/api/events", _stream_events, methods=["GET"], dependencies=auth_dependency
    )
    app.add_api_route(
        "/notifications/ntfy/open",
        _open_ntfy,
        methods=["GET"],
        dependencies=auth_dependency,
    )
    app.add_api_route(
        "/api/resources/{internal_id}/stop",
        _stop_resource,
        methods=["POST"],
        dependencies=auth_dependency,
        status_code=status.HTTP_202_ACCEPTED,
    )
    app.add_api_route(
        "/api/resources/{internal_id}/open",
        _open_resource_dashboard,
        methods=["GET"],
        dependencies=auth_dependency,
    )
    return app
