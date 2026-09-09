from __future__ import annotations

import subprocess
from importlib.metadata import version as installed_version
from pathlib import Path

import pytest

from scripts import check_github_workflows
from scripts.check_release import project_metadata, validate_release_tag

ROOT = Path(__file__).resolve().parents[1]


def test_project_metadata_matches_commit_and_installed_distribution() -> None:
    name, version = project_metadata()
    timestamp, _commit = (
        subprocess.check_output(
            ["git", "-C", str(ROOT), "show", "-s", "--format=%ct:%H", "HEAD"], text=True
        )
        .strip()
        .split(":")
    )
    assert name == "runwatch-notebook"
    assert version == f"0.0.dev{timestamp}"
    assert installed_version(name) == version
    validate_release_tag(f"v{version}", version)


@pytest.mark.parametrize("tag", ["0.2.0", "v0.2.0", "release-0.2.0", ""])
def test_release_tag_must_match_snapshot_version(tag: str) -> None:
    with pytest.raises(ValueError, match="Release tag must be"):
        validate_release_tag(tag, "0.0.dev1300000000")


@pytest.mark.parametrize(
    ("project", "message"),
    [
        ('name = "not-runwatch"', "Expected project name"),
        ('name = "runwatch-notebook"\nversion = "0.2.0"', "must be dynamic"),
        (
            'name = "runwatch-notebook"\ndynamic = ["version"]\nversion = "0.2.0"',
            "must be dynamic",
        ),
        ('name = "runwatch-notebook"\ndynamic = ["version"]', "snapshot backend"),
    ],
)
def test_project_metadata_rejects_invalid_contract(
    tmp_path: Path, project: str, message: str
) -> None:
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text(f"[project]\n{project}\n", encoding="utf-8")
    with pytest.raises(ValueError, match=message):
        project_metadata(pyproject)


def test_snapshot_release_workflow_is_valid() -> None:
    assert (
        check_github_workflows.check_workflow(ROOT / ".github/workflows/release.yml")
        == []
    )


@pytest.mark.parametrize(
    ("before", "after"),
    [
        ("id-token: write", "contents: write"),
        ("name: pypi", "name: unprotected"),
        ("pypa/gh-action-pypi-publish", "untrusted/publish"),
        ("actions/download-artifact", "actions/checkout"),
    ],
)
def test_release_workflow_rejects_unverified_publication(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, before: str, after: str
) -> None:
    workflow = tmp_path / ".github/workflows/release.yml"
    workflow.parent.mkdir(parents=True)
    workflow.write_text(
        (ROOT / ".github/workflows/release.yml").read_text().replace(before, after),
        encoding="utf-8",
    )
    monkeypatch.setattr(check_github_workflows, "ROOT", tmp_path)
    assert check_github_workflows.check_workflow(workflow)
