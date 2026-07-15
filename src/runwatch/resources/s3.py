from __future__ import annotations

import asyncio
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

from botocore.exceptions import ClientError

from ..models import ResourceEvent, ResourceObservation, ResourceStatus
from .base import AwsResourceAdapter, ResourceOperationError


def parse_s3_uri(uri: str) -> tuple[str, str]:
    parsed = urlparse(uri)
    if (
        parsed.scheme != "s3"
        or not parsed.netloc
        or parsed.params
        or parsed.query
        or parsed.fragment
    ):
        raise ResourceOperationError(f"Invalid S3 URI: {uri!r}")
    return parsed.netloc, parsed.path.lstrip("/")


def _nonnegative_int(value: Any, name: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ResourceOperationError(f"aws.s3_prefix {name} must be nonnegative")
    return value


def _finite_number(value: Any, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ResourceOperationError(f"aws.s3_prefix {name} must be numeric")
    result = float(value)
    if not math.isfinite(result):
        raise ResourceOperationError(f"aws.s3_prefix {name} must be finite")
    return result


@dataclass
class _PrefixScan:
    count: int = 0
    total_size: int = 0
    latest: datetime | None = None
    latest_key: str | None = None
    last_key: str | None = None
    marker_found: bool = False

    def consume(self, item: dict[str, Any], marker_key: str | None) -> None:
        self.count += 1
        self.total_size += int(item.get("Size", 0))
        modified = item.get("LastModified")
        if isinstance(modified, datetime) and (
            self.latest is None or modified > self.latest
        ):
            self.latest = modified
            self.latest_key = item.get("Key")
        if item.get("Key"):
            self.last_key = str(item["Key"])
        self.marker_found = self.marker_found or bool(
            marker_key and item.get("Key") == marker_key
        )


def _marker_key(prefix: str, completion_marker: str | None) -> str | None:
    if not completion_marker:
        return None
    if completion_marker.startswith(prefix):
        return completion_marker
    return f"{prefix.rstrip('/')}/{completion_marker.lstrip('/')}"


def _timestamp_iso(value: Any) -> str | None:
    if value is None:
        return None
    return datetime.fromtimestamp(float(value), timezone.utc).isoformat()


class S3PrefixAdapter(AwsResourceAdapter):
    provider = "aws"
    resource_type = "s3_prefix"

    @classmethod
    def has_terminal_condition(cls, event: ResourceEvent) -> bool:
        metadata = event.resource.metadata
        return metadata.get("expected_count") is not None or bool(
            metadata.get("completion_marker")
        )

    @classmethod
    def validate_registration(cls, event: ResourceEvent) -> None:
        super().validate_registration(event)
        parse_s3_uri(event.resource.id)
        metadata = event.resource.metadata
        _nonnegative_int(metadata.get("expected_count"), "expected_count")
        max_pages = metadata.get("max_pages", 100)
        if (
            isinstance(max_pages, bool)
            or not isinstance(max_pages, int)
            or max_pages <= 0
        ):
            raise ResourceOperationError("aws.s3_prefix max_pages must be positive")
        full_rescan_seconds = _finite_number(
            metadata.get("full_rescan_seconds", 300.0), "full_rescan_seconds"
        )
        if full_rescan_seconds < 0:
            raise ResourceOperationError(
                "aws.s3_prefix full_rescan_seconds must be nonnegative"
            )

    async def inspect(
        self, resource: dict[str, Any], cursor: dict[str, Any]
    ) -> ResourceObservation:
        bucket, prefix = parse_s3_uri(resource["external_id"])
        metadata = resource.get("metadata", {})
        expected_count = _nonnegative_int(
            metadata.get("expected_count"), "expected_count"
        )
        max_pages = int(metadata.get("max_pages", 100))
        full_rescan_seconds = _finite_number(
            metadata.get("full_rescan_seconds", 300.0), "full_rescan_seconds"
        )
        client = self.aws.client("s3", resource.get("region"))
        scan_state = cursor.setdefault("prefix_scan", {})
        candidate_state = dict(scan_state)
        now = datetime.now(timezone.utc).timestamp()
        phase = self._prepare_scan(candidate_state, now, full_rescan_seconds)
        start_after = candidate_state.get("last_key")
        result = await asyncio.to_thread(
            self._list_prefix,
            client,
            bucket,
            prefix,
            max_pages,
            metadata.get("completion_marker"),
            start_after,
        )
        self._merge_scan(candidate_state, result, phase, now)
        if metadata.get("completion_marker") and not candidate_state["marker_found"]:
            candidate_state["marker_found"] = await asyncio.to_thread(
                self._completion_marker_exists,
                client,
                bucket,
                prefix,
                str(metadata["completion_marker"]),
            )
        scan_state.clear()
        scan_state.update(candidate_state)
        marker_found = bool(scan_state["marker_found"])
        object_count = int(scan_state["object_count"])
        completed = marker_found or (
            expected_count is not None and object_count >= int(expected_count)
        )
        metrics = self._scan_metrics(
            bucket,
            prefix,
            scan_state,
            result,
            phase,
            full_rescan_seconds,
            expected_count,
        )
        status = ResourceStatus.COMPLETED if completed else ResourceStatus.RUNNING
        return ResourceObservation(
            status=status,
            terminal=completed,
            metrics=metrics,
            message=self._scan_message(completed, bool(result["truncated"]), phase),
        )

    @staticmethod
    def _prepare_scan(
        scan_state: dict[str, Any], now: float, full_rescan_seconds: float
    ) -> str:
        existing = scan_state.get("phase")
        if existing is not None:
            return str(existing)
        last_full_scan = float(scan_state.get("last_full_scan", 0.0))
        full_scan_due = (
            not scan_state.get("initialized")
            or full_rescan_seconds == 0
            or now - last_full_scan >= full_rescan_seconds
        )
        phase = "full" if full_scan_due else "incremental"
        if phase == "full":
            scan_state.update(
                object_count=0,
                total_bytes=0,
                last_key=None,
                latest_object_key=None,
                latest_object_timestamp=None,
                marker_found=False,
            )
        return phase

    @staticmethod
    def _merge_scan(
        scan_state: dict[str, Any], result: dict[str, Any], phase: str, now: float
    ) -> None:
        scan_state["object_count"] = int(scan_state.get("object_count", 0)) + int(
            result["object_count"]
        )
        scan_state["total_bytes"] = int(scan_state.get("total_bytes", 0)) + int(
            result["total_bytes"]
        )
        if result.get("last_key"):
            scan_state["last_key"] = result["last_key"]
        latest = result.get("latest_object_timestamp")
        previous = scan_state.get("latest_object_timestamp")
        if latest is not None and (
            previous is None or float(latest) >= float(previous)
        ):
            scan_state["latest_object_timestamp"] = latest
            scan_state["latest_object_key"] = result.get("latest_object_key")
        scan_state["marker_found"] = bool(
            scan_state.get("marker_found") or result["marker_found"]
        )
        scan_state["phase"] = phase if result["truncated"] else None
        if not result["truncated"]:
            scan_state["initialized"] = True
            if phase == "full":
                scan_state["last_full_scan"] = now

    @staticmethod
    def _scan_metrics(
        bucket: str,
        prefix: str,
        scan_state: dict[str, Any],
        result: dict[str, Any],
        phase: str,
        full_rescan_seconds: float,
        expected_count: Any,
    ) -> dict[str, Any]:
        latest = scan_state.get("latest_object_timestamp")
        reconciled = scan_state.get("last_full_scan")
        return {
            "bucket": bucket,
            "prefix": prefix,
            "object_count": int(scan_state["object_count"]),
            "total_bytes": int(scan_state["total_bytes"]),
            "latest_object_key": scan_state.get("latest_object_key"),
            "latest_object_time": _timestamp_iso(latest),
            "pages_scanned": result["pages_scanned"],
            "objects_scanned_this_poll": result["object_count"],
            "scan_mode": phase,
            "scan_in_progress": bool(result["truncated"]),
            "truncated": bool(result["truncated"]),
            "counts_reconciled_at": _timestamp_iso(reconciled),
            "full_rescan_seconds": full_rescan_seconds,
            "expected_count": expected_count,
            "completion_marker_found": bool(scan_state["marker_found"]),
        }

    @staticmethod
    def _scan_message(completed: bool, truncated: bool, phase: str) -> str | None:
        if completed:
            return "Completion condition satisfied"
        if truncated:
            return "Prefix scan is catching up; counts are currently lower bounds"
        if phase == "incremental":
            return "Incremental counts assume append-only keys until the next full reconciliation"
        return None

    @staticmethod
    def _list_prefix(
        client: Any,
        bucket: str,
        prefix: str,
        max_pages: int,
        completion_marker: str | None,
        start_after: str | None,
    ) -> dict[str, Any]:
        scan = _PrefixScan()
        pages = 0
        truncated = False
        marker_key = _marker_key(prefix, completion_marker)
        paginator = client.get_paginator("list_objects_v2")
        request: dict[str, Any] = {"Bucket": bucket, "Prefix": prefix}
        if start_after:
            request["StartAfter"] = start_after
        for page in paginator.paginate(**request):
            pages += 1
            for item in page.get("Contents", []):
                scan.consume(item, marker_key)
            if pages >= max_pages:
                truncated = bool(page.get("IsTruncated"))
                break
        return {
            "object_count": scan.count,
            "total_bytes": scan.total_size,
            "latest_object_key": scan.latest_key,
            "latest_object_timestamp": scan.latest.timestamp() if scan.latest else None,
            "last_key": scan.last_key,
            "pages_scanned": pages,
            "truncated": truncated,
            "marker_found": scan.marker_found,
        }

    @staticmethod
    def _completion_marker_exists(
        client: Any,
        bucket: str,
        prefix: str,
        completion_marker: str,
    ) -> bool:
        marker_key = (
            completion_marker
            if completion_marker.startswith(prefix)
            else f"{prefix.rstrip('/')}/{completion_marker.lstrip('/')}"
        )
        try:
            client.head_object(Bucket=bucket, Key=marker_key)
            return True
        except ClientError as error:
            if error.response.get("Error", {}).get("Code") in {
                "404",
                "NoSuchKey",
                "NotFound",
            }:
                return False
            raise
