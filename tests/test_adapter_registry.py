from __future__ import annotations

from typing import Any

import pytest

import runwatch.adapters as adapter_module
from runwatch.adapters import AdapterRegistry
from runwatch.models import (
    ResourceEvent,
    ResourceLifecycle,
    ResourceObservation,
    ResourceSpec,
)
from runwatch.resources.base import ResourceAdapter, ResourceConfigurationError


class ExampleAdapter(ResourceAdapter):
    provider = "example"
    resource_type = "job"

    async def inspect(
        self, resource: dict[str, Any], cursor: dict[str, Any]
    ) -> ResourceObservation:
        raise NotImplementedError


def example_event() -> ResourceEvent:
    return ResourceEvent(
        resource=ResourceSpec(provider="example", type="job", id="job-1"),
        lifecycle=ResourceLifecycle(),
    )


def test_builtin_adapter_import_is_deferred_until_resolution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    imported: list[str] = []
    real_import = adapter_module.import_module

    def capture_import(name: str):
        imported.append(name)
        return real_import(name)

    monkeypatch.setattr(adapter_module, "import_module", capture_import)
    registry = AdapterRegistry(discover_plugins=False)

    assert imported == []
    assert registry.resolve("local", "dashboard") is not None
    assert imported == ["runwatch.resources.dashboard"]


def test_explicit_adapter_registration_and_validation() -> None:
    registry = AdapterRegistry(discover_plugins=False)
    registry.register(ExampleAdapter)

    assert registry.resolve("example", "job") is ExampleAdapter
    assert registry.validate(example_event()) is ExampleAdapter
    with pytest.raises(ResourceConfigurationError, match="already registered"):
        registry.register(ExampleAdapter)


def test_entry_point_adapter_is_loaded_lazily(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loads = 0

    class EntryPoint:
        name = "example.job"
        value = "example_plugin:ExampleAdapter"

        def load(self) -> object:
            nonlocal loads
            loads += 1
            return ExampleAdapter

    def fake_entry_points(**kwargs: object) -> list[EntryPoint]:
        return [EntryPoint()]

    monkeypatch.setattr(
        adapter_module,
        "entry_points",
        fake_entry_points,
    )

    registry = AdapterRegistry()
    assert loads == 0
    assert registry.resolve("example", "job") is ExampleAdapter
    assert loads == 1
    assert registry.resolve("example", "job") is ExampleAdapter
    assert loads == 1


def test_entry_point_must_match_registered_resource_kind(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class EntryPoint:
        name = "example.other"
        value = "example_plugin:ExampleAdapter"

        @staticmethod
        def load() -> object:
            return ExampleAdapter

    def fake_entry_points(**kwargs: object) -> list[EntryPoint]:
        return [EntryPoint()]

    monkeypatch.setattr(
        adapter_module,
        "entry_points",
        fake_entry_points,
    )

    registry = AdapterRegistry()
    with pytest.raises(ResourceConfigurationError, match="mismatched adapter"):
        registry.resolve("example", "other")
