from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime
from typing import Any, ClassVar, cast

from botocore.exceptions import ClientError

from ..models import ResourceEvent, ResourceObservation, ResourceStatus
from .base import ResourceAdapter, ResourceOperationError
from .cloudwatch_logs import LogStreamDiscovery, discover_log_streams_with_cursor


def _iso(value: Any) -> str | None:
    return value.isoformat() if isinstance(value, datetime) else None


def _processing_cluster_metrics(description: dict[str, Any]) -> dict[str, Any]:
    processing_resources = description.get("ProcessingResources")
    if not isinstance(processing_resources, dict):
        return {}
    cluster_config = cast(dict[str, Any], processing_resources).get("ClusterConfig")
    if not isinstance(cluster_config, dict):
        return {}
    typed_cluster_config = cast(dict[str, Any], cluster_config)
    field_names = {
        "InstanceCount": "instance_count",
        "InstanceType": "instance_type",
        "VolumeSizeInGB": "volume_size_gb",
    }
    return {
        metric_name: typed_cluster_config[field_name]
        for field_name, metric_name in field_names.items()
        if typed_cluster_config.get(field_name) is not None
    }


@dataclass(frozen=True)
class _LogReadResult:
    lines: list[str]
    selected_streams: list[str]
    discovery_truncated: bool = False
    drain_truncated: bool = False
    pages_read: int = 0


@dataclass
class _LogDrainState:
    values: list[str]
    line_budget: int
    page_budget: int
    pages_read: int = 0
    drain_truncated: bool = False


class SageMakerProcessingAdapter(ResourceAdapter):
    provider = "aws"
    resource_type = "sagemaker_processing_job"
    supports_stop = True
    supports_blocking = True

    _STATUS_MAP: ClassVar[dict[str, ResourceStatus]] = {
        "InProgress": ResourceStatus.RUNNING,
        "Stopping": ResourceStatus.STOPPING,
        "Completed": ResourceStatus.COMPLETED,
        "Failed": ResourceStatus.FAILED,
        "Stopped": ResourceStatus.STOPPED,
    }
    _TERMINAL: ClassVar[set[str]] = {"Completed", "Failed", "Stopped"}

    @classmethod
    def validate_registration(cls, event: ResourceEvent) -> None:
        super().validate_registration(event)
        log_group = event.resource.metadata.get(
            "log_group", "/aws/sagemaker/ProcessingJobs"
        )
        if not isinstance(log_group, str) or not log_group.strip():
            raise ResourceOperationError(
                "aws.sagemaker_processing_job log_group must not be empty"
            )

    async def inspect(
        self, resource: dict[str, Any], cursor: dict[str, Any]
    ) -> ResourceObservation:
        client = self.aws.client("sagemaker", resource.get("region"))
        job_name = resource["external_id"]
        description = await asyncio.to_thread(
            client.describe_processing_job, ProcessingJobName=job_name
        )
        aws_status = str(description.get("ProcessingJobStatus", "Unknown"))
        status = self._STATUS_MAP.get(aws_status, ResourceStatus.UNKNOWN)
        terminal = aws_status in self._TERMINAL
        logs = await self._read_cloudwatch_logs(resource, cursor, drain=terminal)
        metrics = {
            "aws_status": aws_status,
            "processing_job_arn": description.get("ProcessingJobArn"),
            "creation_time": _iso(description.get("CreationTime")),
            "start_time": _iso(description.get("ProcessingStartTime")),
            "end_time": _iso(description.get("ProcessingEndTime")),
            "failure_reason": description.get("FailureReason"),
            "exit_message": description.get("ExitMessage"),
            "monitoring_log_streams": logs.selected_streams,
            "log_stream_discovery_truncated": logs.discovery_truncated,
            "log_drain_truncated": logs.drain_truncated,
            "log_pages_read": logs.pages_read,
            **_processing_cluster_metrics(description),
        }
        message = description.get("FailureReason") or description.get("ExitMessage")
        return ResourceObservation(
            status=status,
            terminal=terminal,
            message=message,
            metrics=metrics,
            log_lines=logs.lines,
            raw={"processing_job_status": aws_status},
        )

    async def stop(self, resource: dict[str, Any]) -> None:
        client = self.aws.client("sagemaker", resource.get("region"))
        job_name = resource["external_id"]
        try:
            description = await asyncio.to_thread(
                client.describe_processing_job, ProcessingJobName=job_name
            )
            status = description.get("ProcessingJobStatus")
            if status in self._TERMINAL or status == "Stopping":
                return
            await asyncio.to_thread(
                client.stop_processing_job, ProcessingJobName=job_name
            )
        except ClientError as error:
            code = error.response.get("Error", {}).get("Code")
            if code in {"ResourceNotFound", "ValidationException"}:
                # A second stop after terminal transition can race with the first.
                latest = await asyncio.to_thread(
                    client.describe_processing_job, ProcessingJobName=job_name
                )
                if latest.get("ProcessingJobStatus") in self._TERMINAL | {"Stopping"}:
                    return
            raise

    async def _read_cloudwatch_logs(
        self,
        resource: dict[str, Any],
        cursor: dict[str, Any],
        *,
        drain: bool,
    ) -> _LogReadResult:
        metadata = resource.get("metadata", {})
        if not resource.get("lifecycle", {}).get("retain_logs", True):
            return _LogReadResult([], [])
        log_group = metadata.get("log_group", "/aws/sagemaker/ProcessingJobs")
        job_name = resource["external_id"]
        logs = self.aws.client("logs", resource.get("region"))
        discovered = await self._discover_cloudwatch_logs(
            logs,
            log_group=log_group,
            job_name=job_name,
            cursor=cursor,
        )
        if discovered is None:
            return _LogReadResult([], [])
        discovery, started_mid_cycle = discovered
        discovery_truncated = discovery.truncated or started_mid_cycle
        return await self._read_discovered_logs(
            logs,
            log_group=log_group,
            streams=discovery.names,
            cursor=cursor,
            drain=drain,
            discovery_truncated=discovery_truncated,
        )

    async def _discover_cloudwatch_logs(
        self,
        logs: Any,
        *,
        log_group: str,
        job_name: str,
        cursor: dict[str, Any],
    ) -> tuple[LogStreamDiscovery, bool] | None:
        try:
            return await discover_log_streams_with_cursor(
                logs,
                log_group=log_group,
                stream_prefix=job_name,
                max_streams=self.aws_settings.max_log_streams,
                cursor=cursor,
                cursor_key="sagemaker_stream_discovery_token",
            )
        except ClientError as error:
            if (
                error.response.get("Error", {}).get("Code")
                == "ResourceNotFoundException"
            ):
                return None
            raise

    async def _read_discovered_logs(
        self,
        logs: Any,
        *,
        log_group: str,
        streams: list[str],
        cursor: dict[str, Any],
        drain: bool,
        discovery_truncated: bool,
    ) -> _LogReadResult:
        tokens: dict[str, str] = cursor.setdefault("log_tokens", {})
        state = _LogDrainState(
            values=[],
            line_budget=self.aws_settings.max_log_lines_per_poll,
            page_budget=(
                self.aws_settings.final_log_drain_max_pages if drain else len(streams)
            ),
        )
        for index, stream in enumerate(streams):
            if state.line_budget <= 0 or state.page_budget <= 0:
                state.drain_truncated = drain
                break
            streams_left = max(1, len(streams) - index)
            stream_line_budget = max(1, state.line_budget // streams_left)
            stable = await self._read_stream_pages(
                logs,
                log_group=log_group,
                stream=stream,
                stream_line_budget=stream_line_budget,
                tokens=tokens,
                state=state,
                drain=drain,
            )
            if drain and not stable:
                state.drain_truncated = True
        if drain and discovery_truncated:
            state.drain_truncated = True
        return _LogReadResult(
            state.values,
            streams,
            discovery_truncated=discovery_truncated,
            drain_truncated=state.drain_truncated,
            pages_read=state.pages_read,
        )

    async def _read_stream_pages(
        self,
        logs: Any,
        *,
        log_group: str,
        stream: str,
        stream_line_budget: int,
        tokens: dict[str, str],
        state: _LogDrainState,
        drain: bool,
    ) -> bool:
        stable = False
        while state.page_budget > 0 and stream_line_budget > 0:
            previous_token = tokens.get(stream)
            next_token, lines, stable = await self._read_log_stream(
                logs,
                log_group,
                stream,
                stream_line_budget,
                previous_token,
            )
            state.pages_read += 1
            state.page_budget -= 1
            if next_token:
                tokens[stream] = next_token
            accepted = lines[:stream_line_budget]
            state.values.extend(accepted)
            consumed = len(accepted)
            stream_line_budget -= consumed
            state.line_budget -= consumed
            if stable or not drain or state.line_budget <= 0:
                break
        return stable

    @staticmethod
    async def _read_log_stream(
        logs: Any,
        log_group: str,
        stream: str,
        limit: int,
        previous_token: str | None,
    ) -> tuple[str | None, list[str], bool]:
        kwargs: dict[str, Any] = {
            "logGroupName": log_group,
            "logStreamName": stream,
            "startFromHead": True,
            "limit": limit,
        }
        if previous_token:
            kwargs["nextToken"] = previous_token
        try:
            page = await asyncio.to_thread(logs.get_log_events, **kwargs)
        except ClientError as error:
            if (
                error.response.get("Error", {}).get("Code")
                == "ResourceNotFoundException"
            ):
                return previous_token, [], True
            raise
        next_token = page.get("nextForwardToken")
        if previous_token and next_token == previous_token:
            return next_token, [], True
        lines = [
            f"[{stream} @ {event.get('timestamp')}] {str(event.get('message', '')).rstrip()}"
            for event in page.get("events", [])
        ]
        return next_token, lines, not next_token
