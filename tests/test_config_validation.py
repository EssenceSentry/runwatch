# pyright: reportMissingParameterType=false, reportUnknownArgumentType=false, reportUnknownLambdaType=false, reportUnknownMemberType=false, reportUnknownParameterType=false
from __future__ import annotations

from pathlib import Path

import nbformat
import pytest

from runwatch.config import dump_default_config, load_config
from runwatch.models import RunwatchConfig
from runwatch.resources import ResourceConfigurationError
from runwatch.validation import validate_execution


def test_init_config_is_commented_and_round_trips(tmp_path: Path) -> None:
    path = tmp_path / "runwatch.yaml"
    dump_default_config(path)
    text = path.read_text(encoding="utf-8")
    assert "# Per-cell execution timeout" in text
    assert "# Generic endpoints receive" in text
    assert load_config(path) == RunwatchConfig()


def test_config_expands_environment_variables(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("RUNWATCH_TEST_TOPIC", "nightly-run")
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
    assert config.notifications.ntfy_topic == "nightly-run"


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


def test_config_defaults_and_rejects_non_mapping(tmp_path: Path) -> None:
    assert load_config(None) == RunwatchConfig()
    path = tmp_path / "runwatch.yaml"
    path.write_text("- not\n- a\n- mapping\n", encoding="utf-8")
    with pytest.raises(ValueError, match="must be a mapping"):
        load_config(path)


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
