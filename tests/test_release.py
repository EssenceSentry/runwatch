# pyright: reportMissingParameterType=false
from __future__ import annotations

from pathlib import Path

import pytest

from scripts.check_release import project_metadata, validate_release_tag


def test_project_metadata_is_the_release_source_of_truth() -> None:
    name, version = project_metadata()

    assert name == "runwatch-notebook"
    assert version == "0.2.0"
    validate_release_tag("v0.2.0", version)


@pytest.mark.parametrize("tag", ["0.2.0", "v0.2.1", "release-0.2.0", ""])
def test_release_tag_must_match_project_version(tag: str) -> None:
    with pytest.raises(ValueError, match="Release tag must be"):
        validate_release_tag(tag, "0.2.0")


def test_project_metadata_rejects_the_wrong_distribution_name(tmp_path: Path) -> None:
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text(
        '[project]\nname = "not-runwatch"\nversion = "0.2.0"\n',
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Expected project name"):
        project_metadata(pyproject)
