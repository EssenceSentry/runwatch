from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
from collections.abc import AsyncGenerator
from pathlib import Path
from sysconfig import get_path
from typing import Annotated, Any, cast
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
    RedirectResponse,
    StreamingResponse,
)
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

from .dashboard_links import DASHBOARD_ACCESS_COOKIE
from .models import NotificationSettings
from .resource_manager import ResourceStopRejected
from .supervisor import RunSupervisor
from .tunnel import with_token

_COOKIE_NAME = DASHBOARD_ACCESS_COOKIE
_REQUIRED_WEB_ARTIFACTS = (
    Path("common/neumorphic-gloss-components.css"),
    Path("runwatch/index.html"),
    Path("runwatch/app.js"),
    Path("runwatch/styles.css"),
)


class StopRequest(BaseModel):
    confirmation: str
    expected_version: int


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


async def _state_snapshot(request: Request) -> dict[str, Any]:
    return _supervisor(request).snapshot()


async def _open_ntfy(request: Request) -> RedirectResponse:
    deep_link = _ntfy_deep_link(_supervisor(request).config.notifications)
    if deep_link is None:
        raise HTTPException(404, "ntfy notifications are not configured")
    return RedirectResponse(deep_link, status_code=303)


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
    return RedirectResponse(target, status_code=303)


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
                yield _event_message(event)
        while not await request.is_disconnected():
            try:
                event = await asyncio.wait_for(queue.get(), timeout=15)
                for delivered in _delivery_batch(supervisor, event, cursor):
                    if delivered.get("seq") is not None:
                        cursor = int(delivered["seq"])
                    yield _event_message(delivered)
            except TimeoutError:
                yield ": keepalive\n\n"


async def _stream_events(request: Request) -> StreamingResponse:
    return StreamingResponse(
        _event_stream(request),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
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
) -> FastAPI:
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
    app.state.runwatch_templates = templates
    app.state.runwatch_asset_version = asset_digest.hexdigest()[:12]

    async def security_headers(request: Request, call_next: Any) -> Any:
        response = await call_next(request)
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; script-src 'self'; style-src 'self'; "
            "img-src 'self' data:; connect-src 'self'; object-src 'none'; "
            "base-uri 'none'; frame-ancestors 'none'; form-action 'self'"
        )
        response.headers["Referrer-Policy"] = "no-referrer"
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
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
    app.add_api_route("/", _dashboard, methods=["GET"], response_class=HTMLResponse)
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
