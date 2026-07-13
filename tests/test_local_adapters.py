# pyright: reportMissingParameterType=false, reportMissingTypeArgument=false, reportPrivateUsage=false, reportUnknownMemberType=false, reportUnknownParameterType=false, reportUnknownVariableType=false
from __future__ import annotations

import os
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

import runwatch.resources.local as local_module
from runwatch.models import AwsSettings, ResourceStatus
from runwatch.resources.local import (
    FileCountAdapter,
    LineCountAdapter,
    SystemMetricsAdapter,
)


def adapter(adapter_type: type, root: Path):
    return adapter_type(aws=None, aws_settings=AwsSettings(), working_dir=root)


@pytest.mark.asyncio
async def test_line_count_is_incremental_honors_tail_and_completes(
    tmp_path: Path,
) -> None:
    path = tmp_path / "worker.log"
    path.write_text("one\ntwo\n", encoding="utf-8")
    cursor: dict = {}
    resource = {
        "external_id": "worker.log",
        "metadata": {"expected_lines": 3, "tail_lines": 1},
    }
    monitor = adapter(LineCountAdapter, tmp_path)
    first = await monitor.inspect(resource, cursor)
    assert first.metrics["line_count"] == 2
    assert first.log_lines == ["two"]
    with path.open("a", encoding="utf-8") as handle:
        handle.write("three\n")
    second = await monitor.inspect(resource, cursor)
    assert second.status is ResourceStatus.COMPLETED
    assert second.metrics["line_count"] == 3


@pytest.mark.asyncio
async def test_file_count_expected_completion(tmp_path: Path) -> None:
    root = tmp_path / "parts"
    root.mkdir()
    (root / "a.parquet").write_bytes(b"a")
    resource = {
        "external_id": "parts",
        "metadata": {
            "pattern": "*.parquet",
            "recursive": False,
            "expected_count": 2,
            "completion_marker": None,
            "settled_seconds": None,
        },
    }
    monitor = adapter(FileCountAdapter, tmp_path)
    cursor: dict = {}
    assert (await monitor.inspect(resource, cursor)).status is ResourceStatus.RUNNING
    (root / "b.parquet").write_bytes(b"b")
    complete = await monitor.inspect(resource, cursor)
    assert complete.status is ResourceStatus.COMPLETED
    assert complete.metrics["file_count"] == 2
    assert "settled_for_seconds" not in complete.metrics


@pytest.mark.asyncio
async def test_file_count_reports_settlement_only_when_configured(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "parts"
    root.mkdir()
    (root / "part.json").write_text("{}", encoding="utf-8")
    resource = {
        "external_id": "parts",
        "metadata": {
            "pattern": "*.json",
            "recursive": False,
            "expected_count": None,
            "completion_marker": None,
            "settled_seconds": 2,
        },
    }
    clock = iter([100.0, 103.0])
    monkeypatch.setattr(local_module.time, "time", lambda: next(clock))
    monitor = adapter(FileCountAdapter, tmp_path)
    cursor: dict = {}

    first = await monitor.inspect(resource, cursor)
    second = await monitor.inspect(resource, cursor)

    assert first.metrics["settled_for_seconds"] == 0
    assert second.metrics["settled_for_seconds"] == 3
    assert second.status is ResourceStatus.COMPLETED


@pytest.mark.asyncio
async def test_line_count_rate_remains_stable_between_growth_polls(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "worker.log"
    path.write_text("one\n", encoding="utf-8")
    resource = {"external_id": "worker.log", "metadata": {"tail_lines": 1}}
    clock = iter([100.0, 100.5, 101.0, 101.5, 112.0])
    monkeypatch.setattr(local_module.time, "time", lambda: next(clock))
    monitor = adapter(LineCountAdapter, tmp_path)
    cursor: dict = {}

    assert (await monitor.inspect(resource, cursor)).metrics["lines_per_second"] == 0
    assert (await monitor.inspect(resource, cursor)).metrics["lines_per_second"] == 0
    with path.open("a", encoding="utf-8") as handle:
        handle.write("two\nthree\n")
    growth = await monitor.inspect(resource, cursor)
    unchanged = await monitor.inspect(resource, cursor)
    stale = await monitor.inspect(resource, cursor)

    assert growth.metrics["lines_per_second"] == 2
    assert unchanged.metrics["lines_per_second"] == 2
    assert stale.metrics["lines_per_second"] == 0


@pytest.mark.asyncio
async def test_system_metrics_degrade_without_nvml(tmp_path: Path, monkeypatch) -> None:
    monitor = adapter(SystemMetricsAdapter, tmp_path)
    resource = {
        "metadata": {
            "include_host": True,
            "include_kernel": False,
            "gpu": "none",
        }
    }
    observation = await monitor.inspect(resource, {})
    assert observation.status is ResourceStatus.RUNNING
    assert "host_cpu_percent" in observation.metrics


@pytest.mark.asyncio
async def test_line_count_preserves_partial_crlf_across_polls(tmp_path: Path) -> None:
    path = tmp_path / "worker.log"
    path.write_bytes(b"one\r")
    cursor: dict = {}
    resource = {"external_id": "worker.log", "metadata": {"tail_lines": 10}}
    monitor = adapter(LineCountAdapter, tmp_path)

    first = await monitor.inspect(resource, cursor)
    assert first.metrics["line_count"] == 0
    assert first.metrics["partial_line_pending"] is True
    with path.open("ab") as handle:
        handle.write(b"\ntwo")
    second = await monitor.inspect(resource, cursor)
    assert second.metrics["line_count"] == 1
    assert second.log_lines == ["one"]
    with path.open("ab") as handle:
        handle.write(b"\n")
    third = await monitor.inspect(resource, cursor)
    assert third.metrics["line_count"] == 2
    assert third.log_lines == ["two"]


@pytest.mark.asyncio
async def test_line_count_detects_truncation_rewrite_and_rotation(
    tmp_path: Path,
) -> None:
    path = tmp_path / "worker.log"
    path.write_text("one\ntwo\n", encoding="utf-8")
    cursor: dict = {}
    resource = {"external_id": "worker.log", "metadata": {"tail_lines": 10}}
    monitor = adapter(LineCountAdapter, tmp_path)
    assert (await monitor.inspect(resource, cursor)).metrics["line_count"] == 2

    path.write_text("new\n", encoding="utf-8")
    truncated = await monitor.inspect(resource, cursor)
    assert truncated.metrics["line_count"] == 1
    assert truncated.metrics["reset_reason"] == "truncation"

    path.write_text("alt\n", encoding="utf-8")
    rewritten = await monitor.inspect(resource, cursor)
    assert rewritten.metrics["line_count"] == 1
    assert rewritten.metrics["reset_reason"] == "rewrite"

    rotated = tmp_path / "worker.log.1"
    os.replace(path, rotated)
    path.write_text("replacement\n", encoding="utf-8")
    replacement = await monitor.inspect(resource, cursor)
    assert replacement.metrics["line_count"] == 1
    assert replacement.metrics["reset_reason"] == "rotation"


@pytest.mark.asyncio
async def test_line_count_bounds_a_growing_unterminated_line(tmp_path: Path) -> None:
    path = tmp_path / "worker.log"
    path.write_bytes(b"x" * (2 * 1_048_576))
    cursor: dict = {}
    resource = {"external_id": "worker.log", "metadata": {"tail_lines": 1}}
    monitor = adapter(LineCountAdapter, tmp_path)

    first = await monitor.inspect(resource, cursor)
    second = await monitor.inspect(resource, cursor)
    assert first.metrics["partial_line_buffered_bytes"] == 65_536
    assert second.metrics["partial_line_truncated"] is True
    assert second.metrics["line_count"] == 0
    with path.open("ab") as handle:
        handle.write(b"\n")
    completed = await monitor.inspect(resource, cursor)
    assert completed.metrics["line_count"] == 1
    assert len(completed.log_lines[0].encode()) <= 16_410


def test_nvml_keeps_healthy_gpu_data_when_one_device_fails(monkeypatch) -> None:
    shutdown_calls: list[bool] = []

    class FakeNvml:
        NVML_TEMPERATURE_GPU = 0

        @staticmethod
        def nvmlInit() -> None:
            pass

        @staticmethod
        def nvmlShutdown() -> None:
            shutdown_calls.append(True)

        @staticmethod
        def nvmlDeviceGetCount() -> int:
            return 2

        @staticmethod
        def nvmlDeviceGetHandleByIndex(index: int) -> int:
            return index

        @staticmethod
        def nvmlDeviceGetUtilizationRates(handle: int):
            if handle == 1:
                raise RuntimeError("device unavailable")
            return SimpleNamespace(gpu=50)

        @staticmethod
        def nvmlDeviceGetMemoryInfo(handle: int):
            return SimpleNamespace(used=0, total=0)

        @staticmethod
        def nvmlDeviceGetTemperature(handle: int, sensor: int) -> int:
            return 42

    monkeypatch.setitem(sys.modules, "pynvml", FakeNvml)

    metrics = SystemMetricsAdapter._nvidia_metrics()

    assert metrics["nvidia_available"] is True
    assert metrics["gpu_0_memory_percent"] is None
    assert metrics["gpu_1_error"] == "device unavailable"
    assert shutdown_calls == [True]
