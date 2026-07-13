#!/usr/bin/env python3
"""Run cheap repository file hygiene checks."""

from __future__ import annotations

import argparse
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

FORBIDDEN_TRACKED_PREFIXES = (
    ".build/",
    ".import_linter_cache/",
    ".pytest_cache/",
    ".ruff_cache/",
    "htmlcov/",
    "site/",
)
FORBIDDEN_TRACKED_PARTS = ("/cdk.out/",)
MERGE_MARKERS = ("<<<<<<<", ">>>>>>>")
SECRET_PATTERNS = (
    ("aws access key id", re.compile(r"\b(?:A3T[A-Z0-9]|AKIA|ASIA)[A-Z0-9]{16}\b")),
    ("openai api key", re.compile(r"\bsk-(?:proj-)?[A-Za-z0-9_-]{20,}\b")),
    ("github token", re.compile(r"\bgh[pousr]_[A-Za-z0-9_]{36,}\b")),
    ("slack token", re.compile(r"\bxox[baprs]-[A-Za-z0-9-]{10,}\b")),
    (
        "private key block",
        re.compile(r"-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----"),
    ),
)


@dataclass(frozen=True)
class Finding:
    path: str
    message: str

    def format(self) -> str:
        return f"{self.path}: {self.message}"


def _tracked_files() -> list[str]:
    result = subprocess.run(
        ["git", "ls-files"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return [line for line in result.stdout.splitlines() if line]


def _relative_path(path: Path) -> str:
    try:
        return path.resolve().relative_to(ROOT).as_posix()
    except ValueError:
        return path.as_posix()


def _is_binary(data: bytes) -> bool:
    return b"\0" in data


def _line_ending_finding(relpath: str, data: bytes) -> Finding | None:
    crlf = data.count(b"\r\n")
    bare_lf = data.count(b"\n") - crlf
    bare_cr = data.count(b"\r") - crlf
    if bare_cr:
        return Finding(relpath, "contains bare CR line endings")
    if crlf and bare_lf:
        return Finding(relpath, "contains mixed CRLF and LF line endings")
    if crlf:
        return Finding(relpath, "contains CRLF line endings; use LF")
    return None


def _text_findings(relpath: str, data: bytes) -> list[Finding]:
    findings: list[Finding] = []
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError as exc:
        return [Finding(relpath, f"is not valid UTF-8 text: {exc}")]

    for line_number, line in enumerate(text.splitlines(keepends=True), start=1):
        content = line.removesuffix("\n").removesuffix("\r")
        if content.endswith((" ", "\t")):
            findings.append(
                Finding(relpath, f"line {line_number} has trailing whitespace")
            )
        if any(content.startswith(marker) for marker in MERGE_MARKERS):
            findings.append(
                Finding(relpath, f"line {line_number} looks like a merge marker")
            )
        if "allowlist secret" not in content:
            for name, pattern in SECRET_PATTERNS:
                if pattern.search(content):
                    findings.append(
                        Finding(
                            relpath,
                            f"line {line_number} contains a possible {name}",
                        )
                    )

    if data and not data.endswith(b"\n"):
        findings.append(Finding(relpath, "does not end with a newline"))

    line_ending = _line_ending_finding(relpath, data)
    if line_ending is not None:
        findings.append(line_ending)

    return findings


def check_path(path: Path, *, max_bytes: int) -> list[Finding]:
    if not path.exists() or path.is_dir():
        return []

    relpath = _relative_path(path)
    findings: list[Finding] = []

    if relpath.startswith(FORBIDDEN_TRACKED_PREFIXES) or any(
        part in f"/{relpath}" for part in FORBIDDEN_TRACKED_PARTS
    ):
        findings.append(
            Finding(relpath, "should not be committed; generated/cache output")
        )

    size = path.stat().st_size
    if size > max_bytes:
        findings.append(
            Finding(relpath, f"is too large for source control ({size} bytes)")
        )

    data = path.read_bytes()
    if _is_binary(data):
        return findings
    return findings + _text_findings(relpath, data)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "paths", nargs="*", help="Files to check. Defaults to git ls-files."
    )
    parser.add_argument(
        "--max-bytes",
        type=int,
        default=5_000_000,
        help="Maximum allowed file size. Default: 5 MB.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    paths = args.paths or _tracked_files()
    findings: list[Finding] = []
    for raw_path in paths:
        findings.extend(check_path(ROOT / raw_path, max_bytes=args.max_bytes))

    if findings:
        for finding in findings:
            print(finding.format())
        return 1

    print(f"File hygiene checks passed for {len(paths)} file(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
