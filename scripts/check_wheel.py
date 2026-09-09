#!/usr/bin/env python3
"""Build, inspect, install, and smoke-test the Runwatch wheel."""

from __future__ import annotations

import shutil
import subprocess
import sys
import tempfile
from email.parser import Parser
from pathlib import Path
from zipfile import ZipFile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from build_backend import _git_identity, snapshot_version  # noqa: E402

BUILD_FILES = {
    "pyproject.toml",
    "MANIFEST.in",
    "LICENSE",
    "README.md",
    "RELEASE.md",
    "build_backend.py",
    ".git_archival.txt",
    ".gitattributes",
}

REQUIRED_PACKAGE_FILES = {
    "runwatch/__init__.py",
    "runwatch/_cli_entrypoint.py",
    "runwatch/_notebook_snapshot.py",
    "runwatch/_marimo_worker.py",
    "runwatch/_marimo_runner.py",
    "runwatch/_runner_core.py",
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


def _inspect_wheel(wheel: Path, expected_version: str) -> None:
    with ZipFile(wheel) as archive:
        names = set(archive.namelist())
        metadata_name = next(
            name for name in names if name.endswith(".dist-info/METADATA")
        )
        metadata = Parser().parsestr(archive.read(metadata_name).decode("utf-8"))
        if metadata["Version"] != expected_version:
            raise RuntimeError("wheel metadata does not match the snapshot version")
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


def _stamp_build_identity(source: Path) -> str:
    """Keep HEAD identity while testing current source outside the checkout."""
    version = snapshot_version()
    identity = _git_identity(ROOT)
    if identity is not None:
        timestamp, commit = identity
        (source / ".git_archival.txt").write_text(
            f"commit_hash = {commit}\ncommit_timestamp = {timestamp}\n",
            encoding="utf-8",
        )
    if snapshot_version(root=source) != version:
        raise RuntimeError("copied source lost its snapshot identity")
    return version


def _copy_build_source(source: Path) -> None:
    source.mkdir()
    for name in BUILD_FILES:
        shutil.copy2(ROOT / name, source / name)
    for name in ("src/runwatch", "web_artifacts"):
        shutil.copytree(
            ROOT / name,
            source / name,
            ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
        )
    # Model an unrelated local file in the archive. It must not enter the sdist.
    (source / "excluded-local-file.txt").write_text(
        "packaging sentinel\n", encoding="utf-8"
    )


def _check_sdist(uv: str, distributions: Path, expected_version: str) -> None:
    sdists = list(distributions.glob("*.tar.gz"))
    if len(sdists) != 1:
        raise RuntimeError(f"expected one sdist, found {len(sdists)}")
    unpacked = distributions.parent / "unpacked"
    shutil.unpack_archive(sdists[0], unpacked)
    source = next(unpacked.iterdir())
    allowed = BUILD_FILES | {"src", "web_artifacts", "PKG-INFO", "setup.cfg"}
    unexpected = sorted(
        path.name for path in source.iterdir() if path.name not in allowed
    )
    if unexpected:
        raise RuntimeError(f"sdist contains unrelated files: {unexpected}")
    for name in ("build_backend.py", "RELEASE.md", ".git_archival.txt", "PKG-INFO"):
        if not (source / name).is_file():
            raise RuntimeError(f"sdist is missing {name}")
    # Ordinary Git builds carry unexpanded archive placeholders in their sdists.
    # Reproduce that case so this rebuild can only use the sdist's PKG-INFO.
    (source / ".git_archival.txt").write_text(
        "commit_hash = $Format:%H$\ncommit_timestamp = $Format:%ct$\n",
        encoding="utf-8",
    )
    rebuilt = distributions.parent / "rebuilt"
    _run([uv, "build", "--wheel", "--out-dir", str(rebuilt)], cwd=source)
    wheels = list(rebuilt.glob("*.whl"))
    if len(wheels) != 1:
        raise RuntimeError(f"expected one rebuilt wheel, found {len(wheels)}")
    _inspect_wheel(wheels[0], expected_version)


def _check_marimo_wheel(uv: str, python: Path, wheel: Path, root: Path) -> None:
    _run(
        [
            str(python),
            "-c",
            "import importlib.util; assert importlib.util.find_spec('marimo') is None",
        ],
        cwd=root,
    )
    requirement = f"runwatch-notebook[marimo] @ {wheel.as_uri()}"
    _run([uv, "pip", "install", "--python", str(python), requirement], cwd=root)
    notebook = root / "native.py"
    notebook.write_text(
        "import marimo\napp=marimo.App()\n@app.cell\ndef _():\n    print('wheel-marimo-result')\n    return\n"
    )
    _run([str(python), "-m", "runwatch", "validate", str(notebook), "--json"], cwd=root)
    _run(
        [
            str(python),
            "-c",
            "\n".join(
                [
                    "import asyncio, json",
                    "from pathlib import Path",
                    "from runwatch.supervisor import RunSupervisor",
                    "from runwatch.models import RunwatchConfig",
                    "async def run():",
                    "    supervisor=RunSupervisor(notebook_path=Path('native.py'), output_path=Path('native.executed.html'), working_dir=Path.cwd(), run_dir=Path('native-run'), config=RunwatchConfig())",
                    "    try:",
                    "        await supervisor.start()",
                    "        assert (await asyncio.wait_for(supervisor.wait(), 45)).value == 'succeeded'",
                    "        assert 'wheel-marimo-result' in Path('native.executed.html').read_text()",
                    "        assert json.loads(Path('native.executed.session.json').read_text())['backend'] == 'marimo'",
                    "    finally:",
                    "        await supervisor.close()",
                    "asyncio.run(run())",
                ]
            ),
        ],
        cwd=root,
    )


def main() -> int:
    uv = shutil.which("uv")
    if uv is None:
        print("uv is required for the wheel check", file=sys.stderr)
        return 1
    try:
        with tempfile.TemporaryDirectory(prefix="runwatch-wheel-") as directory:
            root = Path(directory)
            source = root / "source"
            _copy_build_source(source)
            expected_version = _stamp_build_identity(source)
            wheel_dir = root / "dist"
            _run([uv, "build", "--out-dir", str(wheel_dir)], cwd=source)
            wheels = list(wheel_dir.glob("*.whl"))
            if len(wheels) != 1:
                raise RuntimeError(f"expected one wheel, found {len(wheels)}")
            wheel = wheels[0]
            _inspect_wheel(wheel, expected_version)
            _check_sdist(uv, wheel_dir, expected_version)

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
                        f"assert runwatch.__version__ == {expected_version!r}; "
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
            _check_marimo_wheel(uv, python, wheel, root)
    except (OSError, RuntimeError, subprocess.CalledProcessError) as exc:
        print(f"wheel check failed: {exc}", file=sys.stderr)
        return 1
    print(
        "Runwatch snapshot sdist, base, supervisor, and Marimo wheel installs passed."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
