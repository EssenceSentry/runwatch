#!/usr/bin/env python3
"""Install repo-local agent skills into Codex, VS Code Copilot, or Claude Code."""

from __future__ import annotations

import argparse
import os
import shutil
import sys
from dataclasses import dataclass
from pathlib import Path

IGNORED_DIRS = {"__pycache__", "__MACOSX"}
IGNORED_FILES = {".DS_Store"}


@dataclass(frozen=True)
class Skill:
    """A repository skill available for installation."""

    name: str
    source: Path


def discover_skills(source_root: Path) -> dict[str, Skill]:
    """Discover child directories containing a ``SKILL.md`` file."""
    skills: dict[str, Skill] = {}
    for path in sorted(source_root.iterdir()):
        if path.is_dir() and (path / "SKILL.md").is_file():
            skills[path.name] = Skill(name=path.name, source=path)
    return skills


def default_codex_home() -> Path:
    """Return ``$CODEX_HOME`` or the default personal Codex directory."""
    configured = os.environ.get("CODEX_HOME")
    return Path(configured).expanduser() if configured else Path.home() / ".codex"


def default_copilot_skills_dir() -> Path:
    """Return the VS Code Copilot personal skills directory."""
    return Path.home() / ".copilot" / "skills"


def default_claude_skills_dir() -> Path:
    """Return the Claude Code personal skills directory."""
    return Path.home() / ".claude" / "skills"


def parse_args(argv: list[str]) -> argparse.Namespace:
    """Parse installer command-line arguments."""
    parser = argparse.ArgumentParser(
        description=(
            "Install skills from this repository's agent_skills directory into "
            "Codex, VS Code Copilot, or Claude Code."
        )
    )
    parser.add_argument(
        "skills",
        nargs="*",
        help=(
            "Skill names to install. Use 'update' to install or replace every "
            "current repository skill."
        ),
    )
    parser.add_argument(
        "--target",
        choices=["codex", "copilot", "claude", "both", "all"],
        default="both",
        help=(
            "Destination: 'codex', 'copilot', 'claude', 'both' (Codex and "
            "Copilot, the default), or 'all'."
        ),
    )
    parser.add_argument(
        "--update",
        action="store_true",
        help="Install or replace every current repository skill.",
    )
    parser.add_argument(
        "--replace",
        action="store_true",
        help="Replace existing destinations for explicitly named skills.",
    )
    parser.add_argument(
        "--codex-home",
        type=Path,
        default=None,
        help="Codex home directory. Defaults to $CODEX_HOME or ~/.codex.",
    )
    parser.add_argument(
        "--skills-dir",
        type=Path,
        default=None,
        help="Codex skills directory. Defaults to <codex-home>/skills.",
    )
    parser.add_argument(
        "--copilot-skills-dir",
        type=Path,
        default=None,
        help="Copilot skills directory. Defaults to ~/.copilot/skills/.",
    )
    parser.add_argument(
        "--claude-skills-dir",
        type=Path,
        default=None,
        help="Claude Code skills directory. Defaults to ~/.claude/skills/.",
    )
    return parser.parse_args(argv)


def should_ignore(_directory: str, names: list[str]) -> set[str]:
    """Return generated or platform-specific files excluded from installations."""
    return {
        name
        for name in names
        if name in IGNORED_DIRS
        or name in IGNORED_FILES
        or name.endswith(".pyc")
        or name.startswith("._")
    }


def remove_existing(path: Path) -> None:
    """Remove an existing skill destination without following directory symlinks."""
    if path.is_symlink() or path.is_file():
        path.unlink()
    elif path.exists():
        shutil.rmtree(path)


def install_skill(skill: Skill, destination_root: Path, *, replace: bool) -> str:
    """Copy one skill into a destination root."""
    destination = destination_root / skill.name
    if destination.exists() or destination.is_symlink():
        if not replace:
            return f"skip {skill.name}: already installed"
        remove_existing(destination)

    destination_root.mkdir(parents=True, exist_ok=True)
    shutil.copytree(skill.source, destination, ignore=should_ignore)
    if not (destination / "SKILL.md").is_file():
        raise RuntimeError(f"Installed skill is missing SKILL.md: {destination}")
    return f"installed {skill.name}: {destination}"


def prompt_yes_no(question: str) -> bool:
    """Prompt once for an interactive full installation."""
    answer = input(f"{question} [y/N] ").strip().lower()
    return answer in {"y", "yes"}


def validate_destination(source_root: Path, destination_root: Path) -> None:
    """Reject destinations that overlap the repository skill source tree."""
    source = source_root.resolve()
    destination = destination_root.expanduser().resolve()
    if (
        destination == source
        or source in destination.parents
        or destination in source.parents
    ):
        raise ValueError(
            "Destination skills directory cannot overlap the repository "
            "agent_skills directory."
        )


def resolve_destinations(args: argparse.Namespace) -> list[tuple[str, Path]]:
    """Return the selected destination labels and paths."""
    destinations: list[tuple[str, Path]] = []
    target: str = args.target

    if target in ("codex", "both", "all"):
        codex_home = (args.codex_home or default_codex_home()).expanduser()
        codex_dir = (args.skills_dir or (codex_home / "skills")).expanduser()
        destinations.append(("codex", codex_dir))

    if target in ("copilot", "both", "all"):
        copilot_dir = (
            args.copilot_skills_dir.expanduser()
            if args.copilot_skills_dir
            else default_copilot_skills_dir()
        )
        destinations.append(("copilot", copilot_dir))

    if target in ("claude", "all"):
        claude_dir = (
            args.claude_skills_dir.expanduser()
            if args.claude_skills_dir
            else default_claude_skills_dir()
        )
        destinations.append(("claude", claude_dir))

    return destinations


def confirm_full_install(
    destinations: list[tuple[str, Path]], skills: dict[str, Skill]
) -> bool:
    """Show and confirm one plan covering every selected destination."""
    print(
        "No skill names passed; the installer will install or replace all current "
        "repository skills."
    )
    for label, destination_root in destinations:
        install_count = 0
        replace_count = 0
        for skill in skills.values():
            destination = destination_root / skill.name
            if destination.exists() or destination.is_symlink():
                replace_count += 1
            else:
                install_count += 1
        print(
            f"[{label}] {destination_root}: "
            f"install {install_count}, replace {replace_count}"
        )
    return prompt_yes_no("Proceed with this install plan?")


def install_to_destination(
    label: str,
    destination_root: Path,
    skills: dict[str, Skill],
    *,
    requested: list[str],
    update_mode: bool,
    replace: bool,
) -> None:
    """Install the selected skills into one validated destination."""
    if update_mode:
        selected = [(skill, True) for skill in skills.values()]
    elif requested:
        selected = [(skills[name], replace) for name in requested]
    else:
        selected = [(skill, True) for skill in skills.values()]

    for skill, do_replace in selected:
        message = install_skill(skill, destination_root, replace=do_replace)
        print(f"[{label}] {message}")


def main(argv: list[str] | None = None) -> int:
    """Run the repository skill installer."""
    args = parse_args(sys.argv[1:] if argv is None else argv)
    source_root = Path(__file__).resolve().parent
    skills = discover_skills(source_root)
    if not skills:
        print(f"No skills found under {source_root}.", file=sys.stderr)
        return 1

    requested = list(args.skills)
    update_mode = bool(args.update)
    if requested and requested[0] == "update":
        update_mode = True
        requested = requested[1:]

    if update_mode and requested:
        print(
            "Do not pass skill names with update; update installs every current "
            "repository skill.",
            file=sys.stderr,
        )
        return 2

    missing = sorted(set(requested) - set(skills))
    if missing:
        print(
            f"Unknown skill(s): {', '.join(missing)}. "
            f"Available: {', '.join(skills)}",
            file=sys.stderr,
        )
        return 2

    destinations = resolve_destinations(args)
    try:
        for _label, destination_root in destinations:
            validate_destination(source_root, destination_root)
    except ValueError as error:
        print(str(error), file=sys.stderr)
        return 2

    if not update_mode and not requested:
        if not confirm_full_install(destinations, skills):
            print("No skills installed.")
            return 0

    for label, destination_root in destinations:
        install_to_destination(
            label,
            destination_root,
            skills,
            requested=requested,
            update_mode=update_mode,
            replace=bool(args.replace),
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
