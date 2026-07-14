from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from runwatch.models import AwsSettings, ResourceStatus
from runwatch.resources.sagemaker import SageMakerProcessingAdapter


class FakeSageMaker:
    def __init__(self) -> None:
        self.status = "InProgress"
        self.stop_calls = 0
        self.processing_resources: Any = None

    def describe_processing_job(self, *, ProcessingJobName: str) -> dict[str, Any]:
        description = {
            "ProcessingJobStatus": self.status,
            "ProcessingJobArn": f"arn:aws:sagemaker:::processing-job/{ProcessingJobName}",
        }
        if self.processing_resources is not None:
            description["ProcessingResources"] = self.processing_resources
        return description

    def stop_processing_job(self, *, ProcessingJobName: str) -> dict[str, Any]:
        self.stop_calls += 1
        self.status = "Stopped"
        return {}


class FakeLogs:
    def describe_log_streams(self, **kwargs: Any) -> dict[str, Any]:
        return {"logStreams": [{"logStreamName": "job-1/algo-1"}]}

    def get_log_events(self, **kwargs: Any) -> dict[str, Any]:
        return {
            "events": [{"timestamp": 1, "message": "hello"}],
            "nextForwardToken": "token-1",
        }


class FakeAws:
    def __init__(self) -> None:
        self.sagemaker = FakeSageMaker()
        self.logs = FakeLogs()

    def client(self, service: str, region: str | None = None) -> Any:
        return self.sagemaker if service == "sagemaker" else self.logs


@pytest.mark.asyncio
async def test_sagemaker_inspect_logs_and_idempotent_stop(tmp_path: Path) -> None:
    fake = FakeAws()
    adapter = SageMakerProcessingAdapter(
        aws=fake,  # type: ignore[arg-type]
        aws_settings=AwsSettings(final_log_drain_seconds=0),
        working_dir=tmp_path,
    )
    resource = {
        "external_id": "job-1",
        "region": "us-east-1",
        "metadata": {"log_group": "/aws/sagemaker/ProcessingJobs"},
        "lifecycle": {"retain_logs": True},
    }
    cursor: dict[str, Any] = {}
    observation = await adapter.inspect(resource, cursor)
    assert observation.status is ResourceStatus.RUNNING
    assert observation.log_lines == ["[job-1/algo-1 @ 1] hello"]
    await adapter.stop(resource)
    await adapter.stop(resource)
    terminal = await adapter.inspect(resource, cursor)
    assert terminal.status is ResourceStatus.STOPPED
    assert fake.sagemaker.stop_calls == 1


@pytest.mark.asyncio
async def test_sagemaker_inspect_reports_processing_cluster_scale(
    tmp_path: Path,
) -> None:
    fake = FakeAws()
    fake.sagemaker.processing_resources = {
        "ClusterConfig": {
            "InstanceCount": 4,
            "InstanceType": "ml.m5.4xlarge",
            "VolumeSizeInGB": 200,
        }
    }
    adapter = SageMakerProcessingAdapter(
        aws=fake,  # type: ignore[arg-type]
        aws_settings=AwsSettings(),
        working_dir=tmp_path,
    )

    observation = await adapter.inspect(
        {
            "external_id": "job-1",
            "region": "us-east-1",
            "metadata": {},
            "lifecycle": {"retain_logs": False},
        },
        {},
    )

    assert observation.metrics["instance_count"] == 4
    assert observation.metrics["instance_type"] == "ml.m5.4xlarge"
    assert observation.metrics["volume_size_gb"] == 200


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("processing_resources", "expected_metrics"),
    [
        (None, {}),
        ({}, {}),
        ({"ClusterConfig": None}, {}),
        (
            {"ClusterConfig": {"InstanceType": "ml.c5.xlarge"}},
            {"instance_type": "ml.c5.xlarge"},
        ),
    ],
)
async def test_sagemaker_inspect_tolerates_missing_or_partial_cluster_config(
    tmp_path: Path,
    processing_resources: Any,
    expected_metrics: dict[str, Any],
) -> None:
    fake = FakeAws()
    fake.sagemaker.processing_resources = processing_resources
    adapter = SageMakerProcessingAdapter(
        aws=fake,  # type: ignore[arg-type]
        aws_settings=AwsSettings(),
        working_dir=tmp_path,
    )

    observation = await adapter.inspect(
        {
            "external_id": "job-1",
            "metadata": {},
            "lifecycle": {"retain_logs": False},
        },
        {},
    )

    scale_names = {"instance_count", "instance_type", "volume_size_gb"}
    actual_metrics = {
        name: observation.metrics[name]
        for name in scale_names
        if name in observation.metrics
    }
    assert actual_metrics == expected_metrics


@pytest.mark.asyncio
async def test_sagemaker_stop_is_noop_while_already_stopping(tmp_path: Path) -> None:
    fake = FakeAws()
    fake.sagemaker.status = "Stopping"
    adapter = SageMakerProcessingAdapter(
        aws=fake,  # type: ignore[arg-type]
        aws_settings=AwsSettings(),
        working_dir=tmp_path,
    )
    await adapter.stop({"external_id": "job-1", "region": "us-east-1"})
    assert fake.sagemaker.stop_calls == 0


class PaginatedLogs(FakeLogs):
    def __init__(self) -> None:
        self.requests: list[dict[str, Any]] = []

    def get_log_events(self, **kwargs: Any) -> dict[str, Any]:
        self.requests.append(kwargs)
        token = kwargs.get("nextToken")
        if token is None:
            return {
                "events": [{"timestamp": 1, "message": "first"}],
                "nextForwardToken": "page-2",
            }
        if token == "page-2":
            return {
                "events": [{"timestamp": 2, "message": "second"}],
                "nextForwardToken": "caught-up",
            }
        return {"events": [], "nextForwardToken": "caught-up"}


@pytest.mark.asyncio
async def test_terminal_sagemaker_logs_are_drained_until_token_stabilizes(
    tmp_path: Path,
) -> None:
    fake = FakeAws()
    fake.sagemaker.status = "Completed"
    fake.logs = PaginatedLogs()
    adapter = SageMakerProcessingAdapter(
        aws=fake,  # type: ignore[arg-type]
        aws_settings=AwsSettings(
            max_log_lines_per_poll=10, final_log_drain_max_pages=10
        ),
        working_dir=tmp_path,
    )

    observation = await adapter.inspect(
        {
            "external_id": "job-1",
            "metadata": {},
            "lifecycle": {"retain_logs": True},
        },
        {},
    )

    assert observation.log_lines == [
        "[job-1/algo-1 @ 1] first",
        "[job-1/algo-1 @ 2] second",
    ]
    assert observation.metrics["log_pages_read"] == 3
    assert observation.metrics["log_drain_truncated"] is False


@pytest.mark.asyncio
async def test_terminal_sagemaker_log_drain_reports_page_bound(tmp_path: Path) -> None:
    fake = FakeAws()
    fake.sagemaker.status = "Failed"
    fake.logs = PaginatedLogs()
    adapter = SageMakerProcessingAdapter(
        aws=fake,  # type: ignore[arg-type]
        aws_settings=AwsSettings(
            max_log_lines_per_poll=10, final_log_drain_max_pages=2
        ),
        working_dir=tmp_path,
    )

    observation = await adapter.inspect(
        {
            "external_id": "job-1",
            "metadata": {},
            "lifecycle": {"retain_logs": True},
        },
        {},
    )

    assert observation.metrics["log_pages_read"] == 2
    assert observation.metrics["log_drain_truncated"] is True
