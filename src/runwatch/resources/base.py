from __future__ import annotations

import inspect
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable, Mapping
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
        self._closed = False

    def client(self, service: str, region: str | None = None) -> Any:
        if self._closed:
            raise RuntimeError("AWS client provider is closed")
        key = (service, region or self.settings.region_name)
        if key not in self._clients:
            self._clients[key] = self._session.client(service, region_name=key[1])
        return self._clients[key]

    def close(self) -> None:
        """Close every cached botocore client exactly once."""

        if self._closed:
            return
        self._closed = True
        clients = list(self._clients.values())
        self._clients.clear()
        failures: list[Exception] = []
        for client in clients:
            close = getattr(client, "close", None)
            if not callable(close):
                continue
            try:
                close()
            except Exception as error:
                failures.append(error)
        if failures:
            raise RuntimeError(
                "AWS client cleanup failed: "
                + "; ".join(str(error) for error in failures)
            ) from failures[0]


_ServiceT = TypeVar("_ServiceT")
_ServiceCloser = Callable[[object], Awaitable[None] | None]


def _new_service_map() -> dict[str, object]:
    return {}


def _new_service_closer_map() -> dict[str, tuple[object, _ServiceCloser]]:
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
    _service_closers: dict[str, tuple[object, _ServiceCloser]] = field(
        default_factory=_new_service_closer_map, repr=False
    )
    _closed: bool = field(default=False, init=False, repr=False)

    def service(
        self,
        name: str,
        factory: Callable[[], _ServiceT],
        *,
        close: Callable[[_ServiceT], Awaitable[None] | None] | None = None,
    ) -> _ServiceT:
        """Return one lazily constructed process-local adapter service.

        Parameters
        ----------
        name:
            Stable service key shared by adapters using the same dependency.
        factory:
            Constructor called only when the service has not been created.
        close:
            Optional synchronous or asynchronous callback that makes a newly
            constructed service context-owned. Services previously supplied with
            :meth:`register_service` remain borrowed.

        Returns
        -------
        object
            The cached or newly constructed service.
        """

        if self._closed:
            raise RuntimeError("Adapter context is closed")
        if name not in self._services:
            value = factory()
            self._services[name] = value
            if close is not None:
                self._service_closers[name] = (
                    value,
                    cast(_ServiceCloser, close),
                )
        return cast(_ServiceT, self._services[name])

    def register_service(
        self,
        name: str,
        value: _ServiceT,
        *,
        close: Callable[[_ServiceT], Awaitable[None] | None] | None = None,
    ) -> None:
        """Register an explicitly supplied runtime service.

        Parameters
        ----------
        name:
            Stable service key.
        value:
            Service instance used by subsequently created adapters.
        close:
            Optional synchronous or asynchronous callback transferring ownership
            to this context. Without it, the registered value remains borrowed.
        """

        if self._closed:
            raise RuntimeError("Adapter context is closed")
        if name in self._services and self._services[name] is not value:
            raise ValueError(f"Adapter service {name!r} is already registered")
        self._services[name] = value
        if close is not None:
            self._service_closers[name] = (value, cast(_ServiceCloser, close))

    async def aclose(self) -> None:
        """Close every context-owned service once, including after failures."""

        if self._closed:
            return
        self._closed = True
        owned = list(reversed(self._service_closers.values()))
        self._service_closers.clear()
        failures: list[Exception] = []
        for value, close in owned:
            try:
                result = close(value)
                if inspect.isawaitable(result):
                    await result
            except Exception as error:
                failures.append(error)
        self._services.clear()
        if failures:
            raise RuntimeError(
                "Adapter service cleanup failed: "
                + "; ".join(str(error) for error in failures)
            ) from failures[0]


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
        self._owns_context = context is None
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
        """Release adapter-owned clients or other runtime state.

        Overrides must await ``super().close()`` so directly constructed adapters
        release the context and services they own.
        """

        if self._owns_context:
            await self.context.aclose()

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
            "aws.client_provider",
            lambda: AwsClientProvider(settings),
            close=AwsClientProvider.close,
        )
