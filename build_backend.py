"""Setuptools backend with deterministic commit-derived snapshot versions."""

from __future__ import annotations

import os
import re
import subprocess
from email.parser import Parser
from pathlib import Path

ROOT = Path(__file__).resolve().parent
ARCHIVE_IDENTITY = ROOT / ".git_archival.txt"
PRETEND_VERSION_ENV = "SETUPTOOLS_SCM_PRETEND_VERSION_FOR_RUNWATCH_NOTEBOOK"
_COMMIT_RE = re.compile(r"[0-9a-f]{40}")
_SNAPSHOT_VERSION_RE = re.compile(r"0\.0\.dev[0-9]+")


def snapshot_version(*, root: Path = ROOT) -> str:
    """Return the deterministic package version for a committed snapshot."""

    identity = _git_identity(root) or _archive_identity(root) or _sdist_version(root)
    if identity is None:
        raise RuntimeError(
            "Cannot derive the Runwatch snapshot version: expected a Git "
            "checkout, an expanded .git_archival.txt, or sdist PKG-INFO."
        )
    if isinstance(identity, str):
        return identity
    timestamp, _commit = identity
    return f"0.0.dev{timestamp}"


def _git_identity(root: Path) -> tuple[int, str] | None:
    top_level = _run_git(root, "rev-parse", "--show-toplevel")
    if top_level is None:
        return None
    try:
        git_root = Path(top_level).resolve(strict=True)
        package_root = root.resolve(strict=True)
    except OSError:
        return None
    if git_root != package_root:
        return None

    identity = _run_git(root, "show", "-s", "--format=%ct:%H", "HEAD")
    if identity is None:
        return None
    return _parse_identity(identity)


def _run_git(root: Path, *args: str) -> str | None:
    try:
        result = subprocess.run(
            ["git", "-C", root.as_posix(), *args],
            check=False,
            capture_output=True,
            text=True,
        )
    except FileNotFoundError:
        return None
    if result.returncode != 0:
        return None
    return result.stdout.strip()


def _archive_identity(root: Path) -> tuple[int, str] | None:
    path = root / ARCHIVE_IDENTITY.name
    if not path.is_file():
        return None
    values: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        key, separator, value = line.partition("=")
        if separator:
            values[key.strip()] = value.strip()
    if any("$Format" in value for value in values.values()):
        return None
    return _parse_identity(
        f"{values.get('commit_timestamp', '')}:{values.get('commit_hash', '')}"
    )


def _sdist_version(root: Path) -> str | None:
    candidates = [root / "PKG-INFO", *root.glob("*.egg-info/PKG-INFO")]
    for path in candidates:
        if not path.is_file():
            continue
        metadata = Parser().parsestr(path.read_text(encoding="utf-8"))
        version = metadata.get("Version", "").strip()
        if _SNAPSHOT_VERSION_RE.fullmatch(version):
            return version
    return None


def _parse_identity(raw: str) -> tuple[int, str] | None:
    timestamp_text, separator, commit = raw.partition(":")
    if (
        not separator
        or not timestamp_text.isdecimal()
        or not _COMMIT_RE.fullmatch(commit)
    ):
        return None
    return int(timestamp_text), commit


os.environ[PRETEND_VERSION_ENV] = snapshot_version()

from setuptools import build_meta as _setuptools_backend  # noqa: E402

build_wheel = _setuptools_backend.build_wheel
build_sdist = _setuptools_backend.build_sdist
prepare_metadata_for_build_wheel = _setuptools_backend.prepare_metadata_for_build_wheel
get_requires_for_build_wheel = _setuptools_backend.get_requires_for_build_wheel
get_requires_for_build_sdist = _setuptools_backend.get_requires_for_build_sdist
build_editable = _setuptools_backend.build_editable
prepare_metadata_for_build_editable = (
    _setuptools_backend.prepare_metadata_for_build_editable
)
get_requires_for_build_editable = _setuptools_backend.get_requires_for_build_editable
