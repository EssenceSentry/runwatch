# pyright: reportMissingParameterType=false, reportMissingTypeArgument=false, reportUnknownArgumentType=false, reportUnknownLambdaType=false, reportUnknownMemberType=false, reportUnknownParameterType=false, reportUnknownVariableType=false
from __future__ import annotations

import pytest

from runwatch import aws, local
from runwatch.models import (
    ProgressEvent,
    ResourceEvent,
    ResourceLifecycle,
    ResourceSpec,
)
from runwatch.resources import (
    ResourceConfigurationError,
    validate_resource_event,
)


def test_namespaced_sagemaker_event_is_borrowed_and_safe_by_default(
    monkeypatch,
) -> None:
    captured: list[dict] = []
    monkeypatch.setattr(
        "runwatch.emit._display_payload",
        lambda mime, payload, text: captured.append(payload),
    )
    payload = aws.emit_sagemaker_processing_job("job-123", logical_key="build")
    event = ResourceEvent.model_validate(payload)
    assert event.schema_version == 2
    assert event.resource.type == "sagemaker_processing_job"
    assert event.resource.logical_key == "build"
    assert event.resource.ownership.value == "borrowed"
    assert event.lifecycle.blocking is True
    assert event.lifecycle.stop_on_cancel is False
    assert captured == [payload]


def test_owned_sagemaker_helper_opts_in_to_provider_stop(monkeypatch) -> None:
    monkeypatch.setattr("runwatch.emit._display_payload", lambda *args: None)

    owned = ResourceEvent.model_validate(
        aws.emit_owned_sagemaker_processing_job("job-owned")
    )
    explicit_legacy = ResourceEvent.model_validate(
        aws.emit_sagemaker_processing_job("job-explicit", stop_on_cancel=True)
    )

    assert owned.resource.ownership.value == "exclusive"
    assert owned.lifecycle.stop_on_cancel is True
    assert explicit_legacy.resource.ownership.value == "exclusive"
    assert explicit_legacy.lifecycle.stop_on_cancel is True
    with pytest.raises(ValueError, match="requires ownership='exclusive'"):
        aws.emit_sagemaker_processing_job(
            "job-borrowed",
            ownership="borrowed",
            stop_on_cancel=True,
        )


def test_impossible_blocking_monitors_are_rejected(monkeypatch) -> None:
    monkeypatch.setattr("runwatch.emit._display_payload", lambda *args, **kwargs: None)
    with pytest.raises(ValueError, match="blocking S3"):
        aws.emit_s3_prefix("s3://bucket/prefix", blocking=True)
    with pytest.raises(ValueError, match="blocking line-count"):
        local.emit_line_count("worker.log", blocking=True)
    with pytest.raises(ValueError, match="blocking file-count"):
        local.emit_file_count("parts", blocking=True)


def test_metrics_and_logs_are_nonblocking(monkeypatch) -> None:
    monkeypatch.setattr("runwatch.emit._display_payload", lambda *args, **kwargs: None)
    metric = ResourceEvent.model_validate(
        aws.emit_cloudwatch_metric(namespace="Pipeline", metric_name="Rows")
    )
    logs = ResourceEvent.model_validate(
        aws.emit_cloudwatch_logs(log_group="/aws/example")
    )
    system = ResourceEvent.model_validate(local.emit_system_metrics(gpu="none"))
    dashboard = ResourceEvent.model_validate(
        local.emit_dashboard(
            "http://127.0.0.1:8501",
            name="Training",
            health_path="/_stcore/health",
        )
    )
    assert not metric.lifecycle.blocking
    assert not logs.lifecycle.blocking
    assert not system.lifecycle.blocking
    assert not dashboard.lifecycle.blocking
    assert dashboard.resource.type == "dashboard"
    assert dashboard.resource.ownership.value == "external"
    assert dashboard.resource.metadata["name"] == "Training"


@pytest.mark.parametrize(
    ("provider", "resource_type", "metadata"),
    [
        ("aws", "cloudwatch_metric", {}),
        ("aws", "cloudwatch_logs", {}),
        ("local", "system_metrics", {}),
        ("aws", "s3_prefix", {}),
        ("local", "file_count", {}),
        ("local", "line_count", {}),
    ],
)
def test_central_validation_rejects_impossible_blocking_resources(
    provider: str, resource_type: str, metadata: dict
) -> None:
    event = ResourceEvent(
        resource=ResourceSpec(
            provider=provider, type=resource_type, id="example", metadata=metadata
        ),
        lifecycle=ResourceLifecycle(blocking=True),
    )
    with pytest.raises(ResourceConfigurationError, match="cannot be blocking"):
        validate_resource_event(event)


def test_central_validation_accepts_conditional_blocking_resource() -> None:
    event = ResourceEvent(
        resource=ResourceSpec(
            provider="local",
            type="line_count",
            id="worker.log",
            metadata={"expected_lines": 10},
        ),
        lifecycle=ResourceLifecycle(blocking=True),
    )
    validate_resource_event(event)


def test_emitter_argument_validation_and_metadata(monkeypatch) -> None:
    monkeypatch.setattr("runwatch.emit._display_payload", lambda *args, **kwargs: None)
    with pytest.raises(ValueError, match="expected_count"):
        aws.emit_s3_prefix("s3://bucket/prefix", expected_count=-1)
    with pytest.raises(ValueError, match="max_pages"):
        aws.emit_s3_prefix("s3://bucket/prefix", max_pages=0)
    with pytest.raises(ValueError, match="full_rescan_seconds"):
        aws.emit_s3_prefix("s3://bucket/prefix", full_rescan_seconds=-1)
    with pytest.raises(ValueError, match="period_seconds"):
        aws.emit_cloudwatch_metric(
            namespace="Pipeline", metric_name="Rows", period_seconds=0
        )
    with pytest.raises(ValueError, match="multiple of 60"):
        aws.emit_cloudwatch_metric(
            namespace="Pipeline", metric_name="Rows", period_seconds=2
        )
    with pytest.raises(ValueError, match="1440"):
        aws.emit_cloudwatch_metric(
            namespace="Pipeline",
            metric_name="Rows",
            period_seconds=1,
            lookback_seconds=1_441,
        )
    with pytest.raises(ValueError, match="max_streams"):
        aws.emit_cloudwatch_logs(log_group="/aws/example", max_streams=0)
    with pytest.raises(ValueError, match="cannot override"):
        aws.emit_s3_prefix(
            "s3://bucket/prefix",
            metadata={"expected_count": 10},
        )
    with pytest.raises(ValueError, match="scope"):
        local.emit_system_metrics(include_host=False, include_kernel=False, gpu="none")
    with pytest.raises(ValueError, match="expected_count"):
        local.emit_file_count("parts", expected_count=-1)
    with pytest.raises(ValueError, match="settled_seconds"):
        local.emit_file_count("parts", settled_seconds=0)
    with pytest.raises(ValueError, match="expected_lines"):
        local.emit_line_count("worker.log", expected_lines=-1)
    with pytest.raises(ValueError, match="tail_lines"):
        local.emit_line_count("worker.log", tail_lines=-1)
    with pytest.raises(ValueError, match="localhost or a loopback"):
        local.emit_dashboard("https://example.com/private")
    with pytest.raises(ValueError, match="health_path"):
        local.emit_dashboard("http://localhost:8501", health_path="relative")

    prefix = ResourceEvent.model_validate(
        aws.emit_s3_prefix(
            "s3://bucket/prefix",
            completion_marker="_SUCCESS",
            blocking=True,
            metadata={"custom": "value"},
        )
    )
    manifest = ResourceEvent.model_validate(
        aws.emit_s3_manifest("s3://bucket/run.json")
    )
    files = ResourceEvent.model_validate(
        local.emit_file_count("parts", settled_seconds=1, blocking=True)
    )
    lines = ResourceEvent.model_validate(
        local.emit_line_count(
            "worker.log", expected_lines=0, tail_lines=0, blocking=True
        )
    )
    assert prefix.resource.metadata["custom"] == "value"
    assert manifest.lifecycle.blocking
    assert files.lifecycle.blocking
    assert lines.lifecycle.retain_logs is False


def test_structured_payloads_reject_nonfinite_or_non_json_values() -> None:
    with pytest.raises(ValueError, match="finite"):
        ProgressEvent(completed=1, metrics={"loss": float("nan")})
    with pytest.raises(ValueError, match="unsupported"):
        ResourceSpec(provider="local", type="metric", id="x", metadata={"bad": {1}})
