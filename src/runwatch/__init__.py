"""Runwatch public API with kernel-safe lazy provider emitters."""

from importlib import import_module
from importlib.metadata import PackageNotFoundError, version
from types import ModuleType

from .emit import (
    EVENT_MIME_TYPE,
    RESOURCE_MIME_TYPE,
    emit_event,
    emit_progress,
    emit_resource,
)
from .models import ResourceEvent, ResourceLifecycle, ResourceSpec

__all__ = [
    "EVENT_MIME_TYPE",
    "RESOURCE_MIME_TYPE",
    "ResourceEvent",
    "ResourceLifecycle",
    "ResourceSpec",
    "aws",
    "emit_event",
    "emit_progress",
    "emit_resource",
    "local",
]

"""Dependency-light AWS resource emitters loaded on first access."""
aws: ModuleType
"""Dependency-light local resource emitters loaded on first access."""
local: ModuleType

try:
    __version__ = version("runwatch-notebook")
except PackageNotFoundError:  # pragma: no cover - source-only fallback
    __version__ = "0+unknown"


def __getattr__(name: str) -> ModuleType:
    if name not in {"aws", "local"}:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module = import_module(f".{name}", __name__)
    globals()[name] = module
    return module


def __dir__() -> list[str]:
    return sorted({*globals(), "aws", "local"})
