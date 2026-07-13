from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
INSTALLER = ROOT / "scripts" / "install_skills.py"


def run_installer(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(INSTALLER), *args],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )


def test_installs_runwatch_for_codex_copilot_and_claude(tmp_path: Path) -> None:
    codex_home = tmp_path / "codex"
    copilot_skills = tmp_path / "copilot"
    claude_skills = tmp_path / "claude"

    completed = run_installer(
        "runwatch",
        "--target",
        "all",
        "--codex-home",
        str(codex_home),
        "--copilot-skills-dir",
        str(copilot_skills),
        "--claude-skills-dir",
        str(claude_skills),
    )

    assert completed.returncode == 0, completed.stderr
    installed = [
        codex_home / "skills" / "runwatch",
        copilot_skills / "runwatch",
        claude_skills / "runwatch",
    ]
    assert all((path / "SKILL.md").is_file() for path in installed)
    assert all(not (path / "__pycache__").exists() for path in installed)


def test_update_replaces_existing_skill_and_unknown_name_fails(
    tmp_path: Path,
) -> None:
    skills_dir = tmp_path / "skills"
    destination = skills_dir / "runwatch"
    destination.mkdir(parents=True)
    (destination / "stale.txt").write_text("stale", encoding="utf-8")

    updated = run_installer(
        "update", "--target", "codex", "--skills-dir", str(skills_dir)
    )
    assert updated.returncode == 0, updated.stderr
    assert (destination / "SKILL.md").is_file()
    assert not (destination / "stale.txt").exists()

    unknown = run_installer(
        "missing", "--target", "codex", "--skills-dir", str(skills_dir)
    )
    assert unknown.returncode == 2
    assert "Unknown skill(s): missing" in unknown.stderr
