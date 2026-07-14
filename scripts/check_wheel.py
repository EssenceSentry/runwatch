#!/usr/bin/env python3
"""Build, inspect, install, and smoke-test the Runwatch wheel."""

from __future__ import annotations

import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from zipfile import ZipFile

ROOT = Path(__file__).resolve().parents[1]
REQUIRED_PACKAGE_FILES = {
    "runwatch/__init__.py",
    "runwatch/cli.py",
    "runwatch/default_config.yaml",
    "runwatch/py.typed",
}
REQUIRED_DATA_SUFFIXES = {
    "share/runwatch/web_artifacts/common/neumorphic-gloss-components.css",
    "share/runwatch/web_artifacts/runwatch/app.js",
    "share/runwatch/web_artifacts/runwatch/index.html",
    "share/runwatch/web_artifacts/runwatch/mascot/alert.png",
    "share/runwatch/web_artifacts/runwatch/mascot/confused.png",
    "share/runwatch/web_artifacts/runwatch/mascot/inspecting.png",
    "share/runwatch/web_artifacts/runwatch/mascot/phrases.json",
    "share/runwatch/web_artifacts/runwatch/mascot/ready.png",
    "share/runwatch/web_artifacts/runwatch/mascot/running.png",
    "share/runwatch/web_artifacts/runwatch/mascot/sleeping.png",
    "share/runwatch/web_artifacts/runwatch/mascot/success.png",
    "share/runwatch/web_artifacts/runwatch/mascot/waiting.png",
    "share/runwatch/web_artifacts/runwatch/styles.css",
}


def _run(command: list[str], *, cwd: Path) -> None:
    subprocess.run(command, cwd=cwd, check=True)


def _venv_python(venv: Path) -> Path:
    return venv / ("Scripts/python.exe" if sys.platform == "win32" else "bin/python")


def _venv_runwatch(venv: Path) -> Path:
    return venv / (
        "Scripts/runwatch.exe" if sys.platform == "win32" else "bin/runwatch"
    )


def _inspect_wheel(wheel: Path) -> None:
    with ZipFile(wheel) as archive:
        names = set(archive.namelist())
    missing_package = sorted(REQUIRED_PACKAGE_FILES - names)
    missing_data = sorted(
        suffix
        for suffix in REQUIRED_DATA_SUFFIXES
        if not any(name.endswith(suffix) for name in names)
    )
    forbidden = sorted(name for name in names if "auto_classifier" in name)
    if missing_package or missing_data or forbidden:
        details = []
        if missing_package:
            details.append("missing package files: " + ", ".join(missing_package))
        if missing_data:
            details.append("missing data files: " + ", ".join(missing_data))
        if forbidden:
            details.append("forbidden namespace files: " + ", ".join(forbidden))
        raise RuntimeError("; ".join(details))


def main() -> int:
    uv = shutil.which("uv")
    if uv is None:
        print("uv is required for the wheel check", file=sys.stderr)
        return 1
    try:
        with tempfile.TemporaryDirectory(prefix="runwatch-wheel-") as directory:
            root = Path(directory)
            source = root / "source"
            shutil.copytree(
                ROOT,
                source,
                ignore=shutil.ignore_patterns(
                    ".git",
                    ".venv",
                    ".build",
                    ".coverage",
                    ".pytest_cache",
                    ".ruff_cache",
                    "__pycache__",
                    "*.egg-info",
                    "build",
                    "dist",
                ),
            )
            wheel_dir = root / "dist"
            _run([uv, "build", "--wheel", "--out-dir", str(wheel_dir)], cwd=source)
            wheels = list(wheel_dir.glob("*.whl"))
            if len(wheels) != 1:
                raise RuntimeError(f"expected one wheel, found {len(wheels)}")
            wheel = wheels[0]
            _inspect_wheel(wheel)

            venv = root / ".venv"
            _run([uv, "venv", "--python", sys.executable, str(venv)], cwd=root)
            python = _venv_python(venv)
            _run([uv, "pip", "install", "--python", str(python), str(wheel)], cwd=root)
            _run(
                [
                    str(python),
                    "-c",
                    (
                        "import runwatch; "
                        "from runwatch.web import _web_artifacts_root; "
                        "assert runwatch.__version__ == '0.2.0'; "
                        "assert _web_artifacts_root().is_dir()"
                    ),
                ],
                cwd=root,
            )
            _run([str(_venv_runwatch(venv)), "version"], cwd=root)
    except (OSError, RuntimeError, subprocess.CalledProcessError) as exc:
        print(f"wheel check failed: {exc}", file=sys.stderr)
        return 1
    print("Runwatch wheel contents and isolated install passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
