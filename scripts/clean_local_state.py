"""Remove local generated state from a working tree.

The default mode is a dry run. Pass ``--execute`` to delete the reported files
and directories. The script intentionally does not remove ``.venv``, ``.env``,
or ``private`` unless explicit opt-in flags are provided.
"""

from __future__ import annotations

import argparse
import shutil
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class CleanTarget:
    path: Path
    reason: str


DEFAULT_ROOT_TARGETS: tuple[tuple[str, str], ...] = (
    (".DS_Store", "macOS metadata"),
    (".coverage", "coverage data"),
    (".grimp_cache", "import graph cache"),
    (".import_linter_cache", "import-linter cache"),
    (".mypy_cache", "mypy cache"),
    (".pytest_cache", "pytest cache"),
    (".ruff_cache", "ruff cache"),
    (".build", "local build output"),
    ("build", "setuptools build output"),
    ("src/runwatch_notebook.egg-info", "setuptools egg-info"),
    ("site", "legacy MkDocs build output"),
    ("tmp", "temporary workspace"),
    ("tmp-progress.log", "temporary progress log"),
    ("web_artifacts_fake_sessions/runwatch/.runtime", "fake-session runtime"),
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--execute",
        action="store_true",
        help="delete targets instead of printing a dry run",
    )
    parser.add_argument(
        "--include-venv",
        action="store_true",
        help="also remove .venv",
    )
    parser.add_argument(
        "--include-env",
        action="store_true",
        help="also remove .env",
    )
    parser.add_argument(
        "--include-private",
        action="store_true",
        help="also remove private",
    )
    return parser.parse_args()


def iter_targets(args: argparse.Namespace) -> list[CleanTarget]:
    targets = [
        CleanTarget(REPO_ROOT / rel_path, reason)
        for rel_path, reason in DEFAULT_ROOT_TARGETS
    ]
    targets.extend(
        CleanTarget(path, "Python bytecode cache")
        for path in sorted(REPO_ROOT.rglob("__pycache__"))
        if ".venv" not in path.parts and ".git" not in path.parts
    )
    targets.extend(
        CleanTarget(path, "macOS metadata")
        for path in sorted(REPO_ROOT.rglob(".DS_Store"))
        if ".venv" not in path.parts and ".git" not in path.parts
    )
    if args.include_venv:
        targets.append(CleanTarget(REPO_ROOT / ".venv", "virtual environment"))
    if args.include_env:
        targets.append(CleanTarget(REPO_ROOT / ".env", "environment file"))
    if args.include_private:
        targets.append(CleanTarget(REPO_ROOT / "private", "private generated files"))
    return _dedupe_existing(targets)


def _dedupe_existing(targets: list[CleanTarget]) -> list[CleanTarget]:
    seen: set[Path] = set()
    existing: list[CleanTarget] = []
    for target in targets:
        path = target.path.resolve()
        if path in seen or not path.exists():
            continue
        _validate_target(path)
        seen.add(path)
        existing.append(CleanTarget(path, target.reason))
    return existing


def _validate_target(path: Path) -> None:
    if path == REPO_ROOT:
        raise ValueError("Refusing to remove repository root")
    if REPO_ROOT not in path.parents:
        raise ValueError(f"Refusing to remove path outside repo: {path}")
    if ".git" in path.parts:
        raise ValueError(f"Refusing to remove git internals: {path}")


def remove_target(path: Path) -> None:
    if path.is_dir() and not path.is_symlink():
        shutil.rmtree(path)
    else:
        path.unlink()


def main() -> int:
    args = parse_args()
    targets = iter_targets(args)
    action = "DELETE" if args.execute else "would delete"
    for target in targets:
        relative = target.path.relative_to(REPO_ROOT)
        print(f"{action}: {relative} ({target.reason})")
    if args.execute:
        for target in targets:
            remove_target(target.path)
    if not targets:
        print("No local generated state found.")
    elif not args.execute:
        print("Dry run only. Re-run with --execute to delete these targets.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
