"""Dependency-light console-script entry point for the optional Runwatch CLI."""

from __future__ import annotations

import sys
from importlib.util import find_spec

_SUPERVISOR_MODULES = (
    "async_timeout",
    "boto3",
    "fastapi",
    "httpx",
    "jinja2",
    "jupyter_client",
    "nbclient",
    "nbformat",
    "psutil",
    "qrcode",
    "typer",
    "uvicorn",
    "websockets",
    "yaml",
)


def _missing_supervisor_modules() -> tuple[str, ...]:
    return tuple(name for name in _SUPERVISOR_MODULES if find_spec(name) is None)


def main() -> int:
    """Run the Typer CLI or explain how to install its optional dependencies."""

    if _missing_supervisor_modules():
        print(
            "The Runwatch CLI requires the 'supervisor' extra.\n"
            "Install it with:\n"
            "  python -m pip install 'runwatch-notebook[supervisor]'",
            file=sys.stderr,
        )
        return 2

    from .cli import app

    app()
    return 0
