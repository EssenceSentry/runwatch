from __future__ import annotations

import os
import subprocess
import sys
import tarfile
from pathlib import Path

import pytest

import build_backend as backend

COMMIT = "0123456789abcdef0123456789abcdef01234567"
SNAPSHOT = "0.0.dev1300000000"


@pytest.fixture(autouse=True)
def isolated_git_fixtures(monkeypatch: pytest.MonkeyPatch) -> None:
    # Commit hooks export repository-specific variables. Temporary repositories
    # and linked worktrees must select their own indexes and object databases.
    for name in (
        "GIT_DIR",
        "GIT_WORK_TREE",
        "GIT_COMMON_DIR",
        "GIT_INDEX_FILE",
        "GIT_OBJECT_DIRECTORY",
        "GIT_ALTERNATE_OBJECT_DIRECTORIES",
    ):
        monkeypatch.delenv(name, raising=False)


def _git(root: Path, *args: str) -> str:
    return subprocess.check_output(
        ["git", "-C", str(root), *args], text=True, stderr=subprocess.DEVNULL
    ).strip()


def _commit(root: Path, timestamp: int) -> str:
    subprocess.run(
        [
            "git",
            "-C",
            str(root),
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.invalid",
            "-c",
            "core.hooksPath=/dev/null",
            "commit",
            "--allow-empty",
            "-m",
            "snapshot fixture",
        ],
        env={
            **os.environ,
            "GIT_AUTHOR_DATE": f"{timestamp} +0000",
            "GIT_COMMITTER_DATE": f"{timestamp} +0000",
        },
        check=True,
        capture_output=True,
    )
    return _git(root, "rev-parse", "HEAD")


def _archive(root: Path) -> None:
    (root / ".git_archival.txt").write_text(
        f"commit_hash = {COMMIT}\ncommit_timestamp = 1300000000\n", encoding="utf-8"
    )


def test_git_identity_changes_only_with_the_commit(tmp_path: Path) -> None:
    _git(tmp_path, "init")
    _commit(tmp_path, 1300000000)
    expected = "0.0.dev1300000000"
    assert backend.snapshot_version(root=tmp_path) == expected
    _git(tmp_path, "tag", "v999.0")
    (tmp_path / "uncommitted.py").write_text("print(42)\n", encoding="utf-8")
    _archive(tmp_path)
    assert backend.snapshot_version(root=tmp_path) == expected
    _commit(tmp_path, 1400000000)
    assert backend.snapshot_version(root=tmp_path) == "0.0.dev1400000000"


def test_linked_worktree_has_its_own_identity(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init")
    _commit(repo, 1300000000)
    worktree = tmp_path / "worktree"
    _git(repo, "worktree", "add", "-b", "snapshot-test", str(worktree))
    _commit(worktree, 1400000000)
    assert backend.snapshot_version(root=worktree) == "0.0.dev1400000000"
    assert backend.snapshot_version(root=repo) != backend.snapshot_version(
        root=worktree
    )


def test_archive_ignores_an_enclosing_repository(tmp_path: Path) -> None:
    _git(tmp_path, "init")
    _commit(tmp_path, 1400000000)
    source = tmp_path / "unpacked-source"
    source.mkdir()
    _archive(source)
    assert backend.snapshot_version(root=source) == SNAPSHOT


def test_git_archive_expands_the_distributed_identity(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init")
    for name in (".gitattributes", ".git_archival.txt"):
        (repo / name).write_bytes((backend.ROOT / name).read_bytes())
    _git(repo, "add", ".gitattributes", ".git_archival.txt")
    _commit(repo, 1300000000)
    archive = tmp_path / "source.tar"
    _git(repo, "archive", "--format=tar", f"--output={archive}", "HEAD")
    extracted = tmp_path / "source"
    extracted.mkdir()
    with tarfile.open(archive) as contents:
        identity = contents.extractfile(".git_archival.txt")
        assert identity is not None
        (extracted / ".git_archival.txt").write_bytes(identity.read())
    assert backend.snapshot_version(root=extracted) == "0.0.dev1300000000"


def test_archive_works_without_git(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _archive(tmp_path)
    monkeypatch.setenv("PATH", str(tmp_path))
    assert backend.snapshot_version(root=tmp_path) == SNAPSHOT


@pytest.mark.parametrize(
    "relative", ["PKG-INFO", "runwatch_notebook.egg-info/PKG-INFO"]
)
def test_sdist_fallback(tmp_path: Path, relative: str) -> None:
    (tmp_path / ".git_archival.txt").write_text(
        "commit_hash = $Format:%H$\ncommit_timestamp = $Format:%ct$\n", encoding="utf-8"
    )
    metadata = tmp_path / relative
    metadata.parent.mkdir(exist_ok=True)
    metadata.write_text(
        f"Name: runwatch-notebook\nVersion: {SNAPSHOT}\n", encoding="utf-8"
    )
    assert backend.snapshot_version(root=tmp_path) == SNAPSHOT
    _archive(tmp_path)
    metadata.write_text("Version: 0.0.dev1\n", encoding="utf-8")
    assert backend.snapshot_version(root=tmp_path) == SNAPSHOT


@pytest.mark.parametrize(
    "identity",
    [
        "",
        "commit_hash = $Format:%H$\ncommit_timestamp = $Format:%ct$",
        f"commit_hash = {COMMIT}\ncommit_timestamp = -1",
        "commit_hash = invalid\ncommit_timestamp = 1300000000",
        "unrecognized line\ncommit_timestamp = 1300000000",
    ],
)
def test_missing_or_invalid_identity_fails(tmp_path: Path, identity: str) -> None:
    (tmp_path / ".git_archival.txt").write_text(identity, encoding="utf-8")
    (tmp_path / "PKG-INFO").write_text("Version: 0.2.0\n", encoding="utf-8")
    with pytest.raises(RuntimeError, match="Cannot derive the Runwatch snapshot"):
        backend.snapshot_version(root=tmp_path)


def test_snapshot_cannot_be_selected_through_environment() -> None:
    actual = subprocess.check_output(
        [
            sys.executable,
            "-c",
            "import os, build_backend; print(os.environ[build_backend.PRETEND_VERSION_ENV])",
        ],
        cwd=backend.ROOT,
        env={
            **os.environ,
            backend.PRETEND_VERSION_ENV: "99.0",
            "SETUPTOOLS_SCM_PRETEND_VERSION": "88.0",
        },
        text=True,
    ).strip()
    assert actual == backend.snapshot_version()
