"""Lazy discovery and validation of Runwatch resource adapters."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from functools import lru_cache
from importlib import import_module
from importlib.metadata import entry_points

from .models import ResourceEvent
from .resources.base import ResourceAdapter, ResourceConfigurationError

ADAPTER_ENTRY_POINT_GROUP = "runwatch.adapters"

_BUILTIN_ADAPTER_TARGETS = {
    "aws.sagemaker_processing_job": (
        "runwatch.resources.sagemaker:SageMakerProcessingAdapter"
    ),
    "aws.s3_prefix": "runwatch.resources.s3:S3PrefixAdapter",
    "aws.s3_manifest": "runwatch.resources.s3_manifest:S3ManifestAdapter",
    "aws.cloudwatch_metric": "runwatch.resources.cloudwatch:CloudWatchMetricAdapter",
    "aws.cloudwatch_logs": "runwatch.resources.cloudwatch_logs:CloudWatchLogsAdapter",
    "local.dashboard": "runwatch.resources.dashboard:DashboardAdapter",
    "local.system_metrics": "runwatch.resources.local:SystemMetricsAdapter",
    "local.file_count": "runwatch.resources.local:FileCountAdapter",
    "local.line_count": "runwatch.resources.local:LineCountAdapter",
}


@dataclass(frozen=True)
class _AdapterRegistration:
    target: str
    loader: Callable[[], object]


def _load_target(target: str) -> object:
    module_name, separator, attribute = target.partition(":")
    if not separator or not module_name or not attribute:
        raise ResourceConfigurationError(f"Invalid adapter target {target!r}")
    return getattr(import_module(module_name), attribute)


class AdapterRegistry:
    """Lazily resolve built-in and entry-point resource adapters."""

    def __init__(self, *, discover_plugins: bool = True) -> None:
        self._registrations: dict[str, _AdapterRegistration] = {}
        self._loaded: dict[str, type[ResourceAdapter]] = {}
        for key, target in _BUILTIN_ADAPTER_TARGETS.items():
            self._add(key, target, lambda target=target: _load_target(target))
        if discover_plugins:
            for entry_point in entry_points(group=ADAPTER_ENTRY_POINT_GROUP):
                self._add(
                    entry_point.name,
                    entry_point.value,
                    lambda entry_point=entry_point: entry_point.load(),
                )

    def _add(self, key: str, target: str, loader: Callable[[], object]) -> None:
        existing = self._registrations.get(key)
        if existing is not None:
            if existing.target == target:
                return
            raise ResourceConfigurationError(
                f"Duplicate adapter registration for {key}: "
                f"{existing.target!r} and {target!r}"
            )
        self._registrations[key] = _AdapterRegistration(target, loader)

    def copy(self) -> AdapterRegistry:
        """Return an independently mutable registry with the same lazy loaders.

        Returns
        -------
        AdapterRegistry
            A registry whose explicit test/application registrations do not affect
            the process-wide default.
        """

        duplicate = AdapterRegistry(discover_plugins=False)
        duplicate._registrations = dict(self._registrations)
        duplicate._loaded = dict(self._loaded)
        return duplicate

    def register(self, adapter_type: type[ResourceAdapter]) -> None:
        """Register one explicitly supplied adapter class.

        Parameters
        ----------
        adapter_type:
            Concrete adapter used by this registry instance.
        """

        key = f"{adapter_type.provider}.{adapter_type.resource_type}"
        if key in self._registrations or key in self._loaded:
            raise ResourceConfigurationError(f"Adapter {key} is already registered")
        self._loaded[key] = adapter_type

    def resolve(
        self, provider: str, resource_type: str
    ) -> type[ResourceAdapter] | None:
        """Resolve an adapter only when its resource kind is referenced.

        Parameters
        ----------
        provider:
            Resource provider namespace.
        resource_type:
            Provider-specific resource type.

        Returns
        -------
        type[ResourceAdapter] | None
            Concrete adapter class, or ``None`` when no registration exists.
        """

        key = f"{provider}.{resource_type}"
        cached = self._loaded.get(key)
        if cached is not None:
            return cached
        registration = self._registrations.get(key)
        if registration is None:
            return None
        value = registration.loader()
        if not isinstance(value, type) or not issubclass(value, ResourceAdapter):
            raise ResourceConfigurationError(
                f"Adapter entry point {key} did not load a ResourceAdapter class"
            )
        adapter_type = value
        actual_key = f"{adapter_type.provider}.{adapter_type.resource_type}"
        if actual_key != key:
            raise ResourceConfigurationError(
                f"Adapter entry point {key} loaded mismatched adapter {actual_key}"
            )
        self._loaded[key] = adapter_type
        return adapter_type

    def validate(self, event: ResourceEvent) -> type[ResourceAdapter]:
        """Validate a resource event with its installed adapter definition.

        Parameters
        ----------
        event:
            Resource registration received from configuration or notebook output.

        Returns
        -------
        type[ResourceAdapter]
            Adapter class that accepted the registration.

        Raises
        ------
        ResourceConfigurationError
            If no adapter is installed or its registration policy rejects the event.
        """

        provider = event.resource.provider
        resource_type = event.resource.type
        adapter = self.resolve(provider, resource_type)
        if adapter is None:
            raise ResourceConfigurationError(
                f"No adapter for {provider}.{resource_type}"
            )
        adapter.validate_registration(event)
        return adapter


@lru_cache(maxsize=1)
def default_adapter_registry() -> AdapterRegistry:
    """Return the process-wide registry of installed adapter definitions.

    Returns
    -------
    AdapterRegistry
        Lazily populated registry shared by configuration and execution validation.
    """

    return AdapterRegistry()
