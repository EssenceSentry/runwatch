"""Supervisor-side resource adapter contracts."""

from ..models import ResourceEvent
from .base import (
    AdapterContext,
    AwsClientProvider,
    AwsResourceAdapter,
    ResourceAdapter,
    ResourceConfigurationError,
    ResourceOperationError,
    StopNotSupported,
)


def validate_resource_event(event: ResourceEvent) -> None:
    """Validate a registration using the installed lazy adapter registry.

    Parameters
    ----------
    event:
        Resource registration to validate.
    """

    from ..adapters import default_adapter_registry

    default_adapter_registry().validate(event)


__all__ = [
    "AdapterContext",
    "AwsClientProvider",
    "AwsResourceAdapter",
    "ResourceAdapter",
    "ResourceConfigurationError",
    "ResourceOperationError",
    "StopNotSupported",
    "validate_resource_event",
]
