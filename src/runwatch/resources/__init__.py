from ..models import ResourceEvent
from .base import (
    AwsClientProvider,
    ResourceAdapter,
    ResourceConfigurationError,
    ResourceOperationError,
    StopNotSupported,
)
from .cloudwatch import CloudWatchMetricAdapter
from .cloudwatch_logs import CloudWatchLogsAdapter
from .dashboard import DashboardAdapter
from .local import FileCountAdapter, LineCountAdapter, SystemMetricsAdapter
from .s3 import S3PrefixAdapter
from .s3_manifest import S3ManifestAdapter
from .sagemaker import SageMakerProcessingAdapter

BUILTIN_ADAPTERS = (
    SageMakerProcessingAdapter,
    S3PrefixAdapter,
    S3ManifestAdapter,
    CloudWatchMetricAdapter,
    CloudWatchLogsAdapter,
    DashboardAdapter,
    SystemMetricsAdapter,
    FileCountAdapter,
    LineCountAdapter,
)

BUILTIN_ADAPTER_TYPES = {
    (adapter.provider, adapter.resource_type): adapter for adapter in BUILTIN_ADAPTERS
}


def validate_resource_event(event: ResourceEvent) -> None:
    adapter = BUILTIN_ADAPTER_TYPES.get((event.resource.provider, event.resource.type))
    if adapter is None:
        raise ResourceConfigurationError(
            f"No adapter for {event.resource.provider}.{event.resource.type}"
        )
    adapter.validate_registration(event)


__all__ = [
    "BUILTIN_ADAPTERS",
    "BUILTIN_ADAPTER_TYPES",
    "AwsClientProvider",
    "ResourceAdapter",
    "ResourceConfigurationError",
    "ResourceOperationError",
    "StopNotSupported",
    "validate_resource_event",
]
