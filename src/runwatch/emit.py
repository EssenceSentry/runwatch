from __future__ import annotations

import json
from importlib import import_module
from typing import Any, Protocol, cast

from pydantic import BaseModel

from .models import ProgressEvent, ResourceEvent

"""MIME type used for structured Runwatch resource events."""
RESOURCE_MIME_TYPE = "application/vnd.runwatch.resource+json"
"""MIME type used for structured Runwatch progress events."""
EVENT_MIME_TYPE = "application/vnd.runwatch.event+json"
FALLBACK_PREFIX = "__RUNWATCH_EVENT_JSON__="


class _Display(Protocol):
    def __call__(self, value: object, *, raw: bool = False) -> object: ...


def _display_payload(mime_type: str, payload: dict[str, Any], text: str) -> None:
    try:
        display_module = import_module("IPython.display")
        display_payload = cast(_Display, getattr(display_module, "display"))
        display_payload({mime_type: payload, "text/plain": text}, raw=True)
    except Exception:
        print(
            FALLBACK_PREFIX + json.dumps({"mime_type": mime_type, "payload": payload})
        )


def emit_event(
    event: BaseModel | dict[str, Any], *, text: str | None = None
) -> dict[str, Any]:
    payload = event.model_dump(mode="json") if isinstance(event, BaseModel) else event
    mime_type = (
        RESOURCE_MIME_TYPE
        if payload.get("event") == "resource_created"
        else EVENT_MIME_TYPE
    )
    _display_payload(
        mime_type, payload, text or str(payload.get("event", "runwatch event"))
    )
    return payload


def emit_resource(event: ResourceEvent, *, text: str) -> dict[str, Any]:
    from .resources import validate_resource_event

    validate_resource_event(event)
    return emit_event(event, text=text)


def emit_progress(
    completed: float,
    *,
    total: float | None = None,
    unit: str | None = None,
    message: str | None = None,
    metrics: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Emit structured progress from a running notebook cell.

    Parameters
    ----------
    completed:
        Work completed so far.
    total:
        Optional total amount of work.
    unit:
        Optional unit label displayed by the dashboard.
    message:
        Optional human-readable progress message.
    metrics:
        Optional scalar metrics to attach to the event.

    Returns
    -------
    dict[str, Any]
        The JSON-compatible event payload written to notebook output.
    """
    event = ProgressEvent(
        completed=completed,
        total=total,
        unit=unit,
        message=message,
        metrics=metrics or {},
    )
    value = f"{completed:g}" if total is None else f"{completed:g}/{total:g}"
    return emit_event(event, text=f"Progress: {value}{f' {unit}' if unit else ''}")
