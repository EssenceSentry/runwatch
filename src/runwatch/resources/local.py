from __future__ import annotations

import asyncio
import base64
import contextlib
import math
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, BinaryIO, cast

import psutil

from ..models import ResourceEvent, ResourceObservation, ResourceStatus
from .base import ResourceAdapter, ResourceOperationError

_LINE_CHUNK_BYTES = 1_048_576
_MAX_PARTIAL_TAIL_BYTES = 65_536
_MAX_LOG_LINE_BYTES = 16_384
_LINE_RATE_WINDOW_SECONDS = 10.0
_MAX_LINE_RATE_SAMPLES = 64


def _resolve(working_dir: Path, value: str) -> Path:
    path = Path(value).expanduser()
    return path if path.is_absolute() else working_dir / path


@dataclass(frozen=True)
class _LineFileRead:
    stat: os.stat_result
    chunk: bytes
    reset_reason: str | None


@dataclass(frozen=True)
class _LineProgress:
    complete: list[bytes]
    partial: bytes
    line_count: int
    rate: float


def _pending_line_observation(path: Path) -> ResourceObservation:
    return ResourceObservation(
        status=ResourceStatus.PENDING,
        message="File does not exist yet",
        metrics={"path": str(path), "exists": False, "line_count": 0},
    )


def _line_reset_reason(
    handle: BinaryIO,
    stat: os.stat_result,
    cursor: dict[str, Any],
    offset: int,
) -> str | None:
    inode = getattr(stat, "st_ino", None)
    if cursor and cursor.get("inode") != inode:
        return "rotation"
    if stat.st_size < offset:
        return "truncation"
    if not offset or not cursor.get("fingerprint_b64"):
        return None
    start = max(0, offset - 64)
    handle.seek(start)
    fingerprint = base64.b64encode(handle.read(offset - start)).decode("ascii")
    return "rewrite" if fingerprint != cursor.get("fingerprint_b64") else None


def _reset_line_cursor(cursor: dict[str, Any], inode: int | None) -> None:
    cursor.clear()
    cursor.update(
        {
            "inode": inode,
            "offset": 0,
            "line_count": 0,
            "partial_b64": "",
            "partial_truncated": False,
        }
    )


def _read_line_chunk(path: Path, cursor: dict[str, Any]) -> _LineFileRead:
    with path.open("rb") as handle:
        stat = os.fstat(handle.fileno())
        inode = getattr(stat, "st_ino", None)
        offset = int(cursor.get("offset", 0))
        reset_reason = _line_reset_reason(handle, stat, cursor, offset)
        if not cursor or reset_reason:
            _reset_line_cursor(cursor, inode)
            offset = 0
        handle.seek(offset)
        chunk = handle.read(_LINE_CHUNK_BYTES)
        cursor["offset"] = handle.tell()
        fingerprint_start = max(0, cursor["offset"] - 64)
        handle.seek(fingerprint_start)
        cursor["fingerprint_b64"] = base64.b64encode(
            handle.read(cursor["offset"] - fingerprint_start)
        ).decode("ascii")
    return _LineFileRead(stat=stat, chunk=chunk, reset_reason=reset_reason)


def _cursor_partial(cursor: dict[str, Any]) -> bytes:
    try:
        return base64.b64decode(cursor.get("partial_b64", "") or "", validate=True)
    except (ValueError, TypeError):
        cursor["partial_truncated"] = True
        return b""


def _split_complete_lines(value: bytes) -> tuple[list[bytes], bytes]:
    complete: list[bytes] = []
    start = 0
    index = 0
    while index < len(value):
        current = value[index]
        if current == 10:  # LF
            complete.append(value[start : index + 1])
            start = index + 1
        elif current == 13:  # CR or CRLF
            if index + 1 == len(value):
                break
            if value[index + 1] == 10:
                complete.append(value[start : index + 2])
                start = index + 2
                index += 1
            else:
                complete.append(value[start : index + 1])
                start = index + 1
        index += 1
    return complete, value[start:]


def _decode_log_line(value: bytes) -> str:
    stripped = value.rstrip(b"\r\n")
    if len(stripped) > _MAX_LOG_LINE_BYTES:
        stripped = b"[...truncated...] " + stripped[-_MAX_LOG_LINE_BYTES:]
    return stripped.decode("utf-8", errors="replace")


def _advance_line_cursor(cursor: dict[str, Any], chunk: bytes) -> _LineProgress:
    previous = _cursor_partial(cursor)
    complete, partial = _split_complete_lines(previous + chunk)
    partial_was_truncated = bool(cursor.get("partial_truncated")) and not complete
    if len(partial) > _MAX_PARTIAL_TAIL_BYTES:
        partial = partial[-_MAX_PARTIAL_TAIL_BYTES:]
        partial_was_truncated = True
    cursor["partial_b64"] = base64.b64encode(partial).decode("ascii")
    cursor["partial_truncated"] = partial_was_truncated if partial else False
    line_count = int(cursor.get("line_count", 0)) + len(complete)
    now = time.time()
    rate = _rolling_line_rate(cursor, now, line_count, changed=bool(complete))
    cursor["line_count"] = line_count
    return _LineProgress(
        complete=complete,
        partial=partial,
        line_count=line_count,
        rate=rate,
    )


def _rolling_line_rate(
    cursor: dict[str, Any], now: float, line_count: int, *, changed: bool
) -> float:
    samples = _line_rate_samples(cursor.get("rate_samples", []))
    if changed or not samples:
        samples.append([now, line_count])
    cutoff = now - _LINE_RATE_WINDOW_SECONDS
    while len(samples) > 1 and float(samples[1][0]) <= cutoff:
        samples.pop(0)
    samples = samples[-_MAX_LINE_RATE_SAMPLES:]
    cursor["rate_samples"] = samples
    if len(samples) < 2:
        return 0.0
    elapsed = float(samples[-1][0]) - float(samples[0][0])
    if elapsed <= 0:
        return 0.0
    return (int(samples[-1][1]) - int(samples[0][1])) / elapsed


def _line_rate_samples(value: object) -> list[list[float | int]]:
    if not isinstance(value, list):
        return []
    parsed = (
        _parse_line_rate_sample(item)
        for item in cast(list[object], value)[-_MAX_LINE_RATE_SAMPLES:]
    )
    return [sample for sample in parsed if sample is not None]


def _parse_line_rate_sample(value: object) -> list[float | int] | None:
    if not isinstance(value, list):
        return None
    sample = cast(list[object], value)
    if len(sample) != 2:
        return None
    raw_time, raw_count = sample
    if isinstance(raw_time, bool) or not isinstance(raw_time, (int, float)):
        return None
    if isinstance(raw_count, bool) or not isinstance(raw_count, int):
        return None
    sample_time = float(raw_time)
    if not math.isfinite(sample_time) or raw_count < 0:
        return None
    return [sample_time, raw_count]


def _line_observation(
    path: Path,
    resource: dict[str, Any],
    cursor: dict[str, Any],
    read: _LineFileRead,
    progress: _LineProgress,
) -> ResourceObservation:
    expected = resource.get("metadata", {}).get("expected_lines")
    completed = expected is not None and progress.line_count >= int(expected)
    tail_lines = int(resource.get("metadata", {}).get("tail_lines", 100))
    decoded = [_decode_log_line(part) for part in progress.complete]
    return ResourceObservation(
        status=ResourceStatus.COMPLETED if completed else ResourceStatus.RUNNING,
        terminal=completed,
        message="Expected line count reached" if completed else None,
        metrics={
            "path": str(path),
            "exists": True,
            "bytes": read.stat.st_size,
            "line_count": progress.line_count,
            "expected_lines": expected,
            "lines_per_second": round(progress.rate, 3),
            "modified_at": read.stat.st_mtime,
            "catching_up": cursor["offset"] < read.stat.st_size,
            "reset_reason": read.reset_reason,
            "partial_line_pending": bool(progress.partial),
            "partial_line_buffered_bytes": len(progress.partial),
            "partial_line_truncated": bool(cursor["partial_truncated"]),
        },
        log_lines=decoded[-tail_lines:] if tail_lines else [],
    )


def _settlement_metric(
    configured_seconds: object, settled_for: float
) -> dict[str, float]:
    if configured_seconds is None:
        return {}
    return {"settled_for_seconds": round(settled_for, 2)}


class SystemMetricsAdapter(ResourceAdapter):
    provider = "local"
    resource_type = "system_metrics"

    @classmethod
    def validate_registration(cls, event: ResourceEvent) -> None:
        super().validate_registration(event)
        metadata = event.resource.metadata
        if metadata.get("gpu", "all") not in {"all", "none"}:
            raise ResourceOperationError(
                "local.system_metrics gpu must be 'all' or 'none'"
            )
        if (
            not metadata.get("include_host", True)
            and not metadata.get("include_kernel", True)
            and metadata.get("gpu", "all") == "none"
        ):
            raise ResourceOperationError(
                "local.system_metrics requires at least one enabled scope"
            )

    async def inspect(
        self, resource: dict[str, Any], cursor: dict[str, Any]
    ) -> ResourceObservation:
        return await asyncio.to_thread(self._inspect_sync, resource)

    def _inspect_sync(self, resource: dict[str, Any]) -> ResourceObservation:
        metadata = resource.get("metadata", {})
        metrics: dict[str, Any] = {}
        if metadata.get("include_host", True):
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage(str(self.working_dir))
            metrics.update(
                {
                    "host_cpu_percent": psutil.cpu_percent(interval=None),
                    "host_memory_percent": memory.percent,
                    "host_memory_used_bytes": memory.used,
                    "host_memory_available_bytes": memory.available,
                    "disk_percent": disk.percent,
                    "disk_free_bytes": disk.free,
                }
            )
        if metadata.get("include_kernel", True):
            pid = metadata.get("kernel_pid")
            if pid:
                try:
                    process = psutil.Process(int(pid))
                    with process.oneshot():
                        memory = process.memory_info()
                        metrics.update(
                            {
                                "kernel_pid": int(pid),
                                "kernel_cpu_percent": process.cpu_percent(
                                    interval=None
                                ),
                                "kernel_memory_rss_bytes": memory.rss,
                                "kernel_memory_vms_bytes": memory.vms,
                                "kernel_threads": process.num_threads(),
                            }
                        )
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    metrics.update({"kernel_pid": int(pid), "kernel_available": False})
        if metadata.get("gpu", "all") == "all":
            metrics.update(self._nvidia_metrics())
        return ResourceObservation(status=ResourceStatus.RUNNING, metrics=metrics)

    @staticmethod
    def _nvidia_metrics() -> dict[str, Any]:
        try:
            import pynvml  # type: ignore[import-not-found]
        except ImportError:
            return {"nvidia_available": False}
        try:
            pynvml.nvmlInit()
            count = pynvml.nvmlDeviceGetCount()
            values: dict[str, Any] = {"nvidia_available": True, "gpu_count": count}
            for index in range(count):
                try:
                    handle = pynvml.nvmlDeviceGetHandleByIndex(index)
                    utilization = pynvml.nvmlDeviceGetUtilizationRates(handle)
                    memory = pynvml.nvmlDeviceGetMemoryInfo(handle)
                    memory_percent = (
                        round(100 * memory.used / memory.total, 2)
                        if memory.total
                        else None
                    )
                    values.update(
                        {
                            f"gpu_{index}_utilization_percent": utilization.gpu,
                            f"gpu_{index}_memory_percent": memory_percent,
                            f"gpu_{index}_memory_used_bytes": memory.used,
                            f"gpu_{index}_memory_total_bytes": memory.total,
                            f"gpu_{index}_temperature_c": pynvml.nvmlDeviceGetTemperature(
                                handle, pynvml.NVML_TEMPERATURE_GPU
                            ),
                        }
                    )
                except Exception as error:
                    values[f"gpu_{index}_error"] = str(error)
            return values
        except Exception as error:
            return {"nvidia_available": False, "nvidia_error": str(error)}
        finally:
            with contextlib.suppress(Exception):
                pynvml.nvmlShutdown()


class FileCountAdapter(ResourceAdapter):
    provider = "local"
    resource_type = "file_count"

    @classmethod
    def has_terminal_condition(cls, event: ResourceEvent) -> bool:
        metadata = event.resource.metadata
        return (
            metadata.get("expected_count") is not None
            or bool(metadata.get("completion_marker"))
            or metadata.get("settled_seconds") is not None
        )

    @classmethod
    def validate_registration(cls, event: ResourceEvent) -> None:
        super().validate_registration(event)
        metadata = event.resource.metadata
        expected = metadata.get("expected_count")
        if expected is not None and (
            isinstance(expected, bool) or not isinstance(expected, int) or expected < 0
        ):
            raise ResourceOperationError(
                "local.file_count expected_count must be nonnegative"
            )
        settled = metadata.get("settled_seconds")
        if settled is not None and (
            isinstance(settled, bool)
            or not isinstance(settled, (int, float))
            or not math.isfinite(float(settled))
            or settled <= 0
        ):
            raise ResourceOperationError(
                "local.file_count settled_seconds must be a positive finite number"
            )
        pattern = metadata.get("pattern", "*")
        if not isinstance(pattern, str) or not pattern:
            raise ResourceOperationError("local.file_count pattern must not be empty")
        cls._validate_relative_value(pattern, "pattern")
        marker = metadata.get("completion_marker")
        if marker:
            if not isinstance(marker, str):
                raise ResourceOperationError(
                    "local.file_count completion_marker must be a string"
                )
            cls._validate_relative_value(marker, "completion_marker")

    @staticmethod
    def _validate_relative_value(value: str, field: str) -> None:
        path = Path(value)
        if path.is_absolute() or ".." in path.parts:
            raise ResourceOperationError(
                f"local.file_count {field} must stay within the monitored directory"
            )

    async def inspect(
        self, resource: dict[str, Any], cursor: dict[str, Any]
    ) -> ResourceObservation:
        return await asyncio.to_thread(self._inspect_sync, resource, cursor)

    def _inspect_sync(
        self, resource: dict[str, Any], cursor: dict[str, Any]
    ) -> ResourceObservation:
        root = _resolve(self.working_dir, resource["external_id"])
        metadata = resource.get("metadata", {})
        pattern = str(metadata.get("pattern", "*"))
        recursive = bool(metadata.get("recursive", False))
        if not root.exists():
            cursor.pop("signature", None)
            cursor.pop("changed_at", None)
            return ResourceObservation(
                status=ResourceStatus.PENDING,
                message="Directory does not exist yet",
                metrics={"path": str(root), "exists": False, "file_count": 0},
            )
        if not root.is_dir():
            raise ResourceOperationError(f"File-count path is not a directory: {root}")
        iterator = root.rglob(pattern) if recursive else root.glob(pattern)
        stats: list[os.stat_result] = []
        for path in iterator:
            try:
                if path.is_file():
                    stats.append(path.stat())
            except OSError:
                # Files may be atomically renamed or removed while a glob is read.
                continue
        count = len(stats)
        total_bytes = sum(stat.st_size for stat in stats)
        latest = max((stat.st_mtime for stat in stats), default=None)
        marker = metadata.get("completion_marker")
        marker_found = bool(marker and (root / str(marker)).exists())
        expected = metadata.get("expected_count")
        expected_met = expected is not None and count >= int(expected)
        settled_seconds = metadata.get("settled_seconds")
        signature = f"{count}:{total_bytes}:{latest}"
        now = time.time()
        if cursor.get("signature") != signature:
            cursor.update({"signature": signature, "changed_at": now})
        settled_for = max(0.0, now - float(cursor.get("changed_at", now)))
        settled = settled_seconds is not None and settled_for >= float(settled_seconds)
        completed = marker_found or expected_met or settled
        metrics: dict[str, Any] = {
            "path": str(root),
            "exists": True,
            "pattern": pattern,
            "file_count": count,
            "total_bytes": total_bytes,
            "latest_modified_at": latest,
            "expected_count": expected,
            "completion_marker_found": marker_found,
            **_settlement_metric(settled_seconds, settled_for),
        }
        return ResourceObservation(
            status=ResourceStatus.COMPLETED if completed else ResourceStatus.RUNNING,
            terminal=completed,
            message="Completion condition satisfied" if completed else None,
            metrics=metrics,
        )


class LineCountAdapter(ResourceAdapter):
    provider = "local"
    resource_type = "line_count"

    @classmethod
    def has_terminal_condition(cls, event: ResourceEvent) -> bool:
        return event.resource.metadata.get("expected_lines") is not None

    @classmethod
    def validate_registration(cls, event: ResourceEvent) -> None:
        super().validate_registration(event)
        metadata = event.resource.metadata
        expected = metadata.get("expected_lines")
        if expected is not None and (
            isinstance(expected, bool) or not isinstance(expected, int) or expected < 0
        ):
            raise ResourceOperationError(
                "local.line_count expected_lines must be nonnegative"
            )
        tail_lines = metadata.get("tail_lines", 100)
        if (
            isinstance(tail_lines, bool)
            or not isinstance(tail_lines, int)
            or tail_lines < 0
        ):
            raise ResourceOperationError(
                "local.line_count tail_lines must be nonnegative"
            )

    async def inspect(
        self, resource: dict[str, Any], cursor: dict[str, Any]
    ) -> ResourceObservation:
        path = _resolve(self.working_dir, resource["external_id"])
        return await asyncio.to_thread(self._inspect_sync, path, resource, cursor)

    @staticmethod
    def _inspect_sync(
        path: Path, resource: dict[str, Any], cursor: dict[str, Any]
    ) -> ResourceObservation:
        if not path.exists():
            cursor.clear()
            return _pending_line_observation(path)
        read = _read_line_chunk(path, cursor)
        progress = _advance_line_cursor(cursor, read.chunk)
        return _line_observation(path, resource, cursor, read, progress)

    @staticmethod
    def _split_complete_lines(value: bytes) -> tuple[list[bytes], bytes]:
        return _split_complete_lines(value)

    @staticmethod
    def _decode_log_line(value: bytes) -> str:
        return _decode_log_line(value)
