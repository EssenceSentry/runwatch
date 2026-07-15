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
    "runwatch/_cli_entrypoint.py",
    "runwatch/_notebook_snapshot.py",
    "runwatch/adapters.py",
    "runwatch/cli.py",
    "runwatch/default_config.yaml",
    "runwatch/py.typed",
}
REQUIRED_DATA_SUFFIXES = {
    "share/runwatch/web_artifacts/common/neumorphic-gloss-components.css",
    "share/runwatch/web_artifacts/runwatch/app.js",
    "share/runwatch/web_artifacts/runwatch/index.html",
    "share/runwatch/web_artifacts/runwatch/notebook.html",
    "share/runwatch/web_artifacts/runwatch/notebook.js",
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


def _assert_base_cli_requires_supervisor(command: list[str], *, cwd: Path) -> None:
    result = subprocess.run(command, cwd=cwd, capture_output=True, text=True)
    output = f"{result.stdout}\n{result.stderr}"
    if result.returncode == 0:
        raise RuntimeError("base-only runwatch CLI unexpectedly succeeded")
    if "runwatch-notebook[supervisor]" not in output:
        raise RuntimeError(
            "base-only runwatch CLI did not explain how to install extras"
        )
    if "Traceback (most recent call last)" in output:
        raise RuntimeError("base-only runwatch CLI emitted a Python traceback")


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
                        "import importlib.metadata, sys; "
                        "import runwatch; import runwatch.aws; import runwatch.local; "
                        "from runwatch import (ResourceEvent, ResourceLifecycle, "
                        "ResourceSpec, emit_resource); "
                        "assert runwatch.__version__ == '0.2.0'; "
                        "event = ResourceEvent(resource=ResourceSpec("
                        "provider='example', type='job', id='job-1'), "
                        "lifecycle=ResourceLifecycle()); "
                        "assert emit_resource(event, text='example')['resource']"
                        "['provider'] == 'example'; "
                        "names = {d.metadata['Name'].lower() "
                        "for d in importlib.metadata.distributions()}; "
                        "assert not names.intersection({'boto3', 'fastapi', "
                        "'httpx', 'nbclient', 'nbconvert', 'beautifulsoup4', "
                        "'uvicorn'}); "
                        "assert not {'boto3', 'fastapi', 'httpx', 'nbclient', "
                        "'nbconvert', 'bs4'} "
                        ".intersection(sys.modules)"
                    ),
                ],
                cwd=root,
            )
            _assert_base_cli_requires_supervisor(
                [str(_venv_runwatch(venv)), "version"], cwd=root
            )
            _assert_base_cli_requires_supervisor(
                [str(python), "-m", "runwatch", "version"], cwd=root
            )
            requirement = f"runwatch-notebook[supervisor] @ {wheel.as_uri()}"
            _run(
                [uv, "pip", "install", "--python", str(python), requirement],
                cwd=root,
            )
            _run(
                [
                    str(python),
                    "-c",
                    (
                        "from nbconvert import HTMLExporter; import bs4; "
                        "from runwatch.web import _web_artifacts_root; "
                        "root = _web_artifacts_root(); "
                        "assert (root / 'runwatch/notebook.html').is_file(); "
                        "assert (root / 'runwatch/notebook.js').is_file(); "
                        "assert HTMLExporter and bs4"
                    ),
                ],
                cwd=root,
            )
            _run([str(_venv_runwatch(venv)), "version"], cwd=root)
    except (OSError, RuntimeError, subprocess.CalledProcessError) as exc:
        print(f"wheel check failed: {exc}", file=sys.stderr)
        return 1
    print("Runwatch base and supervisor-extra wheel installs passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
