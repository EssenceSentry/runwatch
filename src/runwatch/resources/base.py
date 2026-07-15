from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, TypeVar, cast

from ..models import AwsSettings, Ownership, ResourceEvent, ResourceObservation


class ResourceOperationError(RuntimeError):
    pass


class StopNotSupported(ResourceOperationError):
    pass


class ResourceConfigurationError(ResourceOperationError):
    pass


class AwsClientProvider:
    def __init__(self, settings: AwsSettings) -> None:
        import boto3

        self.settings = settings
        self._session = boto3.Session(
            profile_name=settings.profile_name,
            region_name=settings.region_name,
        )
        self._clients: dict[tuple[str, str | None], Any] = {}

    def client(self, service: str, region: str | None = None) -> Any:
        key = (service, region or self.settings.region_name)
        if key not in self._clients:
            self._clients[key] = self._session.client(service, region_name=key[1])
        return self._clients[key]


_ServiceT = TypeVar("_ServiceT")


def _new_service_map() -> dict[str, object]:
    return {}


@dataclass
class AdapterContext:
    """Runtime dependencies supplied to resource adapters.

    Attributes
    ----------
    working_dir:
        Directory against which adapters resolve relative local paths.
    settings:
        Namespaced settings made available by the Runwatch supervisor.
    """

    working_dir: Path
    settings: Mapping[str, object]
    _services: dict[str, object] = field(default_factory=_new_service_map, repr=False)

    def service(self, name: str, factory: Callable[[], _ServiceT]) -> _ServiceT:
        """Return one lazily constructed process-local adapter service.

        Parameters
        ----------
        name:
            Stable service key shared by adapters using the same dependency.
        factory:
            Constructor called only when the service has not been created.

        Returns
        -------
        object
            The cached or newly constructed service.
        """

        if name not in self._services:
            self._services[name] = factory()
        return cast(_ServiceT, self._services[name])

    def register_service(self, name: str, value: object) -> None:
        """Register an explicitly supplied runtime service.

        Parameters
        ----------
        name:
            Stable service key.
        value:
            Service instance used by subsequently created adapters.
        """

        self._services[name] = value


class ResourceAdapter(ABC):
    provider: str
    resource_type: str
    supports_stop: bool = False
    supports_blocking: bool = False

    def __init__(
        self,
        context: AdapterContext | None = None,
        *,
        aws: AwsClientProvider | None = None,
        aws_settings: AwsSettings | None = None,
        working_dir: Path | None = None,
    ) -> None:
        if context is None:
            if working_dir is None:
                raise TypeError("ResourceAdapter requires an AdapterContext")
            context = AdapterContext(
                working_dir=working_dir,
                settings={"aws": aws_settings or AwsSettings()},
            )
            if aws is not None:
                context.register_service("aws.client_provider", aws)
        self.context = context
        self.working_dir = context.working_dir

    @abstractmethod
    async def inspect(
        self, resource: dict[str, Any], cursor: dict[str, Any]
    ) -> ResourceObservation:
        raise NotImplementedError

    async def stop(self, resource: dict[str, Any]) -> None:
        raise StopNotSupported(
            f"{self.provider}.{self.resource_type} resources cannot be stopped by Runwatch"
        )

    async def close(self) -> None:
        """Release adapter-owned clients or other runtime state."""

    @classmethod
    def has_terminal_condition(cls, event: ResourceEvent) -> bool:
        """Return whether this concrete registration can safely block a run."""

        return cls.supports_blocking

    @classmethod
    def validate_registration(cls, event: ResourceEvent) -> None:
        lifecycle = event.lifecycle
        resource = event.resource
        qualified_type = f"{resource.provider}.{resource.type}"
        if lifecycle.blocking and not cls.has_terminal_condition(event):
            raise ResourceConfigurationError(
                f"{qualified_type} cannot be blocking without a valid terminal condition"
            )
        if lifecycle.stop_on_cancel and not cls.supports_stop:
            raise ResourceConfigurationError(
                f"{qualified_type} does not support stop_on_cancel"
            )
        if lifecycle.stop_on_cancel and resource.ownership is not Ownership.EXCLUSIVE:
            raise ResourceConfigurationError(
                f"{qualified_type} requires exclusive ownership when stop_on_cancel is enabled"
            )


class AwsResourceAdapter(ResourceAdapter):
    """Resource adapter with lazily shared AWS settings and clients."""

    def __init__(
        self,
        context: AdapterContext | None = None,
        *,
        aws: AwsClientProvider | None = None,
        aws_settings: AwsSettings | None = None,
        working_dir: Path | None = None,
    ) -> None:
        super().__init__(
            context,
            aws=aws,
            aws_settings=aws_settings,
            working_dir=working_dir,
        )
        settings = self.context.settings.get("aws")
        if not isinstance(settings, AwsSettings):
            raise ResourceConfigurationError("AWS adapter settings are unavailable")
        self.aws_settings = settings
        self.aws = self.context.service(
            "aws.client_provider", lambda: AwsClientProvider(settings)
        )
