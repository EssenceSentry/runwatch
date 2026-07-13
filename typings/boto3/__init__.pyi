from __future__ import annotations

from typing import Any


class _Client:
    meta: Any

    def __getattr__(self, name: str) -> Any: ...


class Session:
    region_name: str | None

    def __init__(self, **kwargs: object) -> None: ...
    def client(self, service_name: str, **kwargs: object) -> _Client: ...


def client(service_name: str, **kwargs: object) -> _Client: ...
def resource(service_name: str, **kwargs: object) -> Any: ...
