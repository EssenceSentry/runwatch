# pyright: reportMissingParameterType=false, reportUnknownArgumentType=false, reportUnknownLambdaType=false, reportUnknownMemberType=false, reportUnknownParameterType=false
from __future__ import annotations

from pathlib import Path

import nbformat
import pytest

from runwatch.config import dump_default_config, load_config
from runwatch.models import (
    NotificationSettings,
    ResourceObservation,
    ResourceStatus,
    RunwatchConfig,
)
from runwatch.resources import ResourceConfigurationError
from runwatch.validation import validate_execution


def test_init_config_is_commented_and_round_trips(tmp_path: Path) -> None:
    path = tmp_path / "runwatch.yaml"
    dump_default_config(path)
    text = path.read_text(encoding="utf-8")
    assert "# Per-cell execution timeout" in text
    assert "# Automatically mirror tqdm progress" in text
    assert "# Generic endpoints receive" in text
    config = load_config(path)
    assert config == RunwatchConfig()
    assert config.notebook.capture_tqdm is True
    assert config.notebook.tqdm_min_interval_seconds == 0.5
    assert config.host.prevent_system_sleep is False
    assert config.notifications.ntfy_on_section_start is False
    assert config.server.linger_seconds == 90


def test_config_expands_environment_variables(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("RUNWATCH_TEST_TOPIC", "nightly$$run")
    path = tmp_path / "runwatch.yaml"
    path.write_text(
        """
schema_version: 2
notifications:
  ntfy_base_url: https://ntfy.example
  ntfy_topic: ${RUNWATCH_TEST_TOPIC}
""",
        encoding="utf-8",
    )
    config = load_config(path)
    assert config.notifications.ntfy_topic == "nightly$$run"


def test_config_rejects_unset_environment_variables(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.delenv("RUNWATCH_MISSING_BASE_URL", raising=False)
    monkeypatch.delenv("RUNWATCH_MISSING_TOPIC", raising=False)
    path = tmp_path / "runwatch.yaml"
    path.write_text(
        """
notifications:
  ntfy_base_url: ${RUNWATCH_MISSING_BASE_URL}
  ntfy_topic: $RUNWATCH_MISSING_TOPIC
""",
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError,
        match=(
            "unset environment variable.*RUNWATCH_MISSING_BASE_URL, "
            "RUNWATCH_MISSING_TOPIC"
        ),
    ):
        load_config(path)


def test_config_ignores_comment_placeholders_and_supports_literal_dollars(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("RUNWATCH_COMMENT_ONLY", raising=False)
    monkeypatch.delenv("RUNWATCH_LITERAL_TOPIC", raising=False)
    path = tmp_path / "runwatch.yaml"
    path.write_text(
        """
# Optional example: ${RUNWATCH_COMMENT_ONLY}
notifications:
  ntfy_base_url: https://ntfy.example
  ntfy_topic: $$RUNWATCH_LITERAL_TOPIC
""",
        encoding="utf-8",
    )

    config = load_config(path)

    assert config.notifications.ntfy_topic == "$RUNWATCH_LITERAL_TOPIC"


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("ntfy_base_url", "ntfy.example"),
        ("ntfy_base_url", "file:///tmp/ntfy"),
        ("webhook_urls", ["ftp://example.test/hook"]),
    ],
)
def test_config_rejects_non_http_notification_destinations(
    field: str, value: object
) -> None:
    with pytest.raises(ValueError, match=r"absolute HTTP\(S\) URLs"):
        RunwatchConfig.model_validate({"notifications": {field: value}})


@pytest.mark.parametrize(
    "url",
    ["http://localhost:8080/hook", "http://127.0.0.1/hook", "http://[::1]/hook"],
)
def test_notification_config_accepts_plain_http_only_for_loopback_by_default(
    url: str,
) -> None:
    config = RunwatchConfig.model_validate({"notifications": {"webhook_urls": [url]}})
    assert config.notifications.webhook_urls == [url]


def test_notification_config_requires_explicit_plain_http_for_network_hosts() -> None:
    with pytest.raises(ValueError, match="allow_insecure_http"):
        RunwatchConfig.model_validate(
            {"notifications": {"webhook_urls": ["http://192.168.1.4/hook"]}}
        )
    config = RunwatchConfig.model_validate(
        {
            "notifications": {
                "webhook_urls": ["http://192.168.1.4/hook"],
                "allow_insecure_http": True,
            }
        }
    )
    assert config.notifications.allow_insecure_http


@pytest.mark.parametrize("attempts", [0, 21])
def test_notification_config_bounds_routing_attempts(attempts: int) -> None:
    with pytest.raises(ValueError):
        NotificationSettings(max_routing_attempts=attempts)


@pytest.mark.parametrize(
    "status",
    [ResourceStatus.COMPLETED, ResourceStatus.FAILED, ResourceStatus.STOPPED],
)
def test_resource_observation_normalizes_terminal_statuses(
    status: ResourceStatus,
) -> None:
    observation = ResourceObservation(status=status)

    assert observation.terminal is True


def test_resource_observation_rejects_nonterminal_status_marked_terminal() -> None:
    with pytest.raises(ValueError, match="cannot be terminal"):
        ResourceObservation(status=ResourceStatus.RUNNING, terminal=True)


def test_resource_observation_allows_terminal_unknown_status() -> None:
    observation = ResourceObservation(status=ResourceStatus.UNKNOWN, terminal=True)

    assert observation.terminal is True


def test_notification_config_bounds_periodic_interval_and_rejects_fragments() -> None:
    with pytest.raises(ValueError):
        RunwatchConfig.model_validate({"notifications": {"periodic_seconds": 59}})
    with pytest.raises(ValueError, match="fragments"):
        RunwatchConfig.model_validate(
            {"notifications": {"webhook_urls": ["https://hooks.example/hook#secret"]}}
        )


def test_section_start_notifications_require_ntfy() -> None:
    with pytest.raises(ValueError, match="ntfy_on_section_start requires"):
        RunwatchConfig.model_validate(
            {"notifications": {"ntfy_on_section_start": True}}
        )

    config = RunwatchConfig.model_validate(
        {
            "notifications": {
                "ntfy_base_url": "https://ntfy.example",
                "ntfy_topic": "runs",
                "ntfy_on_section_start": True,
            }
        }
    )
    assert config.notifications.ntfy_on_section_start is True


def test_config_rejects_nonterminal_blocking_resource(tmp_path: Path) -> None:
    path = tmp_path / "runwatch.yaml"
    path.write_text(
        """
schema_version: 2
resources:
  - resource:
      provider: aws
      type: cloudwatch_metric
      id: Pipeline/Rows
      metadata:
        namespace: Pipeline
        metric_name: Rows
    lifecycle:
      blocking: true
""",
        encoding="utf-8",
    )
    with pytest.raises(ResourceConfigurationError, match="cannot be blocking"):
        load_config(path)


def test_preflight_reports_notebook_and_missing_cloudflared(
    tmp_path: Path, monkeypatch
) -> None:
    notebook_path = tmp_path / "demo.ipynb"
    notebook = nbformat.v4.new_notebook(
        cells=[nbformat.v4.new_code_cell("print('ok')")]
    )
    nbformat.write(notebook, notebook_path)
    monkeypatch.setattr(
        "runwatch.validation.KernelSpecManager.get_kernel_spec",
        lambda self, name: object(),
    )
    monkeypatch.setattr("runwatch.validation.shutil.which", lambda binary: None)
    config = RunwatchConfig.model_validate(
        {
            "server": {
                "share": "cloudflared",
                "cloudflared_binary": "missing-cloudflared",
            }
        }
    )

    report = validate_execution(notebook_path, config, working_dir=tmp_path)

    assert report["valid"] is False
    assert report["cell_count"] == 1
    assert "missing-cloudflared" in report["errors"][0]


def test_preflight_reports_unavailable_requested_sleep_inhibitor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    notebook_path = tmp_path / "demo.ipynb"
    nbformat.write(nbformat.v4.new_notebook(), notebook_path)

    def unavailable() -> None:
        raise RuntimeError("logind is unavailable")

    monkeypatch.setattr("runwatch.validation.create_sleep_inhibitor", unavailable)
    config = RunwatchConfig.model_validate({"host": {"prevent_system_sleep": True}})

    report = validate_execution(notebook_path, config, working_dir=tmp_path)

    assert report["valid"] is False
    assert any("logind is unavailable" in error for error in report["errors"])


def test_config_defaults_and_rejects_non_mapping(tmp_path: Path) -> None:
    assert load_config(None) == RunwatchConfig()
    path = tmp_path / "runwatch.yaml"
    path.write_text("- not\n- a\n- mapping\n", encoding="utf-8")
    with pytest.raises(ValueError, match="must be a mapping"):
        load_config(path)


def test_config_rejects_nonpositive_tqdm_interval() -> None:
    with pytest.raises(ValueError, match="tqdm_min_interval_seconds"):
        RunwatchConfig.model_validate({"notebook": {"tqdm_min_interval_seconds": 0}})


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("max_event_payload_bytes", 1_023),
        ("max_resource_payload_bytes", 1_023),
        ("max_notification_record_bytes", 1_023),
        ("max_delivery_error_bytes", 255),
    ],
)
def test_config_rejects_unsafe_per_record_storage_caps(field: str, value: int) -> None:
    with pytest.raises(ValueError, match=field):
        RunwatchConfig.model_validate({"storage": {field: value}})


def test_preflight_collects_notebook_workdir_kernel_resource_and_lan_issues(
    tmp_path: Path, monkeypatch
) -> None:
    notebook_path = tmp_path / "broken.ipynb"
    notebook_path.write_text("not json", encoding="utf-8")
    working_file = tmp_path / "not-a-directory"
    working_file.write_text("x", encoding="utf-8")

    def missing_kernel(self, name):
        from jupyter_client.kernelspec import NoSuchKernel

        raise NoSuchKernel(name)

    monkeypatch.setattr(
        "runwatch.validation.KernelSpecManager.get_kernel_spec",
        missing_kernel,
    )
    config = RunwatchConfig.model_validate(
        {
            "server": {"share": "lan", "host": "192.0.2.10"},
            "resources": [
                {
                    "resource": {
                        "provider": "unknown",
                        "type": "job",
                        "id": "job-1",
                    }
                }
            ],
        }
    )
    report = validate_execution(notebook_path, config, working_dir=working_file)
    assert report["valid"] is False
    assert report["cell_count"] == 0
    assert any("nbformat" in error for error in report["errors"])
    assert any("not a directory" in error for error in report["errors"])
    assert any("not installed" in error for error in report["errors"])
    assert any("No adapter" in error for error in report["errors"])
    assert any("LAN sharing" in warning for warning in report["warnings"])


def test_preflight_warns_when_blocking_resource_has_no_overall_timeout(
    tmp_path: Path, monkeypatch
) -> None:
    notebook_path = tmp_path / "demo.ipynb"
    nbformat.write(nbformat.v4.new_notebook(), notebook_path)
    monkeypatch.setattr(
        "runwatch.validation.KernelSpecManager.get_kernel_spec",
        lambda self, name: object(),
    )
    config = RunwatchConfig.model_validate(
        {
            "resources": [
                {
                    "resource": {
                        "provider": "aws",
                        "type": "s3_manifest",
                        "id": "s3://bucket/progress.json",
                    },
                    "lifecycle": {"blocking": True},
                }
            ]
        }
    )
    report = validate_execution(notebook_path, config, working_dir=tmp_path)
    assert report["valid"] is True
    assert report["configured_resources"][0]["blocking"] is True
    assert any("no overall completion timeout" in item for item in report["warnings"])
