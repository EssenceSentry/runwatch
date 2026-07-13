"""Runwatch public API."""

from importlib.metadata import PackageNotFoundError, version

from . import aws, local
from .emit import EVENT_MIME_TYPE, RESOURCE_MIME_TYPE, emit_progress
from .models import ResourceEvent, ResourceLifecycle, ResourceSpec

aws.__doc__ = "AWS resource emitters for Runwatch-managed notebook cells."
local.__doc__ = "Local resource emitters for Runwatch-managed notebook cells."

__all__ = [
    "EVENT_MIME_TYPE",
    "RESOURCE_MIME_TYPE",
    "ResourceEvent",
    "ResourceLifecycle",
    "ResourceSpec",
    "aws",
    "emit_progress",
    "local",
]

try:
    __version__ = version("runwatch-notebook")
except PackageNotFoundError:  # pragma: no cover - source-only fallback
    __version__ = "0+unknown"
