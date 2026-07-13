from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

from botocore.exceptions import ClientError

from ..models import ResourceEvent, ResourceObservation, ResourceStatus
from .base import ResourceAdapter, ResourceOperationError


@dataclass(frozen=True)
class LogStreamDiscovery:
    """One bounded slice of CloudWatch log-stream discovery."""

    names: list[str]
    next_token: str | None
    truncated: bool


async def describe_log_stream_names(
    logs: Any,
    *,
    log_group: str,
    stream_prefix: str,
    max_streams: int,
    start_token: str | None = None,
) -> LogStreamDiscovery:
    """Discover up to ``max_streams`` across paginated CloudWatch responses."""

    names: list[str] = []
    seen: set[str] = set()
    next_token = start_token
    while len(names) < max_streams:
        request: dict[str, Any] = {
            "logGroupName": log_group,
            "limit": min(50, max_streams - len(names)),
        }
        if stream_prefix:
            request["logStreamNamePrefix"] = stream_prefix
        if next_token:
            request["nextToken"] = next_token
        response = await asyncio.to_thread(logs.describe_log_streams, **request)
        for item in response.get("logStreams", []):
            name = item.get("logStreamName")
            if name and str(name) not in seen:
                seen.add(str(name))
                names.append(str(name))
        new_token = response.get("nextToken")
        if not new_token or new_token == next_token:
            return LogStreamDiscovery(names[:max_streams], None, False)
        next_token = str(new_token)
    return LogStreamDiscovery(names[:max_streams], next_token, bool(next_token))


def validate_cloudwatch_logs_configuration(metadata: dict[str, Any]) -> int | None:
    log_group = metadata.get("log_group")
    if not isinstance(log_group, str) or not log_group.strip():
        raise ResourceOperationError("aws.cloudwatch_logs log_group must not be empty")
    configured = metadata.get("max_streams")
    if configured is None:
        return None
    if isinstance(configured, bool) or not isinstance(configured, int):
        raise ResourceOperationError(
            "aws.cloudwatch_logs max_streams must be an integer"
        )
    if not 1 <= configured <= 100:
        raise ResourceOperationError(
            "aws.cloudwatch_logs max_streams must be between 1 and 100"
        )
    return configured


async def discover_log_streams_with_cursor(
    logs: Any,
    *,
    log_group: str,
    stream_prefix: str,
    max_streams: int,
    cursor: dict[str, Any],
    cursor_key: str = "stream_discovery_token",
) -> tuple[LogStreamDiscovery, bool]:
    """Rotate bounded discovery across polls and recover expired AWS tokens."""

    start_token_value = cursor.get(cursor_key)
    start_token = str(start_token_value) if start_token_value else None
    try:
        result = await describe_log_stream_names(
            logs,
            log_group=log_group,
            stream_prefix=stream_prefix,
            max_streams=max_streams,
            start_token=start_token,
        )
    except ClientError as error:
        code = error.response.get("Error", {}).get("Code")
        if not start_token or code != "InvalidParameterException":
            raise
        cursor.pop(cursor_key, None)
        start_token = None
        result = await describe_log_stream_names(
            logs,
            log_group=log_group,
            stream_prefix=stream_prefix,
            max_streams=max_streams,
        )
    if result.next_token:
        cursor[cursor_key] = result.next_token
    else:
        cursor.pop(cursor_key, None)
    return result, bool(start_token)


def _is_missing_log_resource(error: ClientError) -> bool:
    return error.response.get("Error", {}).get("Code") == "ResourceNotFoundException"


async def _discover_for_inspection(
    logs: Any,
    *,
    log_group: str,
    stream_prefix: str,
    max_streams: int,
    cursor: dict[str, Any],
) -> tuple[LogStreamDiscovery, bool] | None:
    try:
        return await discover_log_streams_with_cursor(
            logs,
            log_group=log_group,
            stream_prefix=stream_prefix,
            max_streams=max_streams,
            cursor=cursor,
        )
    except ClientError as error:
        if _is_missing_log_resource(error):
            return None
        raise


async def _read_log_page(
    logs: Any,
    *,
    log_group: str,
    stream: str,
    limit: int,
    previous_token: str | None,
) -> dict[str, Any] | None:
    request: dict[str, Any] = {
        "logGroupName": log_group,
        "logStreamName": stream,
        "startFromHead": True,
        "limit": limit,
    }
    if previous_token:
        request["nextToken"] = previous_token
    try:
        return await asyncio.to_thread(logs.get_log_events, **request)
    except ClientError as error:
        if _is_missing_log_resource(error):
            return None
        raise


def _format_log_lines(stream: str, page: dict[str, Any]) -> list[str]:
    return [
        f"[{stream} @ {event.get('timestamp')}] {str(event.get('message', '')).rstrip()}"
        for event in page.get("events", [])
    ]


async def _read_discovered_streams(
    logs: Any,
    *,
    log_group: str,
    streams: list[str],
    cursor: dict[str, Any],
    line_limit: int,
) -> list[str]:
    tokens: dict[str, str] = cursor.setdefault("log_tokens", {})
    remaining = line_limit
    lines: list[str] = []
    for stream in streams:
        if remaining <= 0:
            break
        previous = tokens.get(stream)
        page = await _read_log_page(
            logs,
            log_group=log_group,
            stream=stream,
            limit=remaining,
            previous_token=previous,
        )
        if page is None:
            tokens.pop(stream, None)
            continue
        next_token = page.get("nextForwardToken")
        if next_token:
            tokens[stream] = next_token
        if previous and previous == next_token:
            continue
        new_lines = _format_log_lines(stream, page)
        accepted = new_lines[:remaining]
        lines.extend(accepted)
        remaining -= len(accepted)
    return lines


class CloudWatchLogsAdapter(ResourceAdapter):
    provider = "aws"
    resource_type = "cloudwatch_logs"

    @classmethod
    def validate_registration(cls, event: ResourceEvent) -> None:
        super().validate_registration(event)
        validate_cloudwatch_logs_configuration(event.resource.metadata)

    async def inspect(
        self, resource: dict[str, Any], cursor: dict[str, Any]
    ) -> ResourceObservation:
        metadata = resource.get("metadata", {})
        group = str(metadata["log_group"])
        prefix = str(metadata.get("stream_prefix", ""))
        configured_max_streams = validate_cloudwatch_logs_configuration(metadata)
        max_streams = configured_max_streams or self.aws_settings.max_log_streams
        logs = self.aws.client("logs", resource.get("region"))
        discovered = await _discover_for_inspection(
            logs,
            log_group=group,
            stream_prefix=prefix,
            max_streams=max_streams,
            cursor=cursor,
        )
        if discovered is None:
            return ResourceObservation(
                status=ResourceStatus.PENDING,
                message="CloudWatch log group does not exist yet",
                metrics={"log_group": group, "stream_count": 0},
            )
        discovery, started_mid_cycle = discovered
        lines = await _read_discovered_streams(
            logs,
            log_group=group,
            streams=discovery.names,
            cursor=cursor,
            line_limit=self.aws_settings.max_log_lines_per_poll,
        )
        rotating = discovery.truncated or started_mid_cycle
        return ResourceObservation(
            status=ResourceStatus.RUNNING,
            metrics={
                "log_group": group,
                "stream_prefix": prefix,
                "stream_count": len(discovery.names),
                "stream_discovery_truncated": rotating,
                "stream_rotation_active": rotating,
            },
            log_lines=lines,
        )
