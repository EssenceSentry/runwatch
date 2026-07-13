from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

import boto3

from ..models import AwsSettings, Ownership, ResourceEvent, ResourceObservation


class ResourceOperationError(RuntimeError):
    pass


class StopNotSupported(ResourceOperationError):
    pass


class ResourceConfigurationError(ResourceOperationError):
    pass


class AwsClientProvider:
    def __init__(self, settings: AwsSettings) -> None:
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


class ResourceAdapter(ABC):
    provider: str
    resource_type: str
    supports_stop: bool = False
    supports_blocking: bool = False

    def __init__(
        self,
        *,
        aws: AwsClientProvider,
        aws_settings: AwsSettings,
        working_dir: Path,
    ) -> None:
        self.aws = aws
        self.aws_settings = aws_settings
        self.working_dir = working_dir

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
