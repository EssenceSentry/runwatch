#!/usr/bin/env python3
"""Run Flake8 CCR with a small audited exception allowlist."""

from __future__ import annotations

import argparse
import ast
import json
import pathlib
import re
import subprocess
import sys
from dataclasses import dataclass
from typing import Any, Iterable

CCR_RE = re.compile(
    r"^(?P<path>.*?):(?P<line>\d+):(?P<col>\d+): "
    r"CCR001 Cognitive complexity is too high "
    r"\((?P<score>\d+) > (?P<max_score>\d+)\)$"
)


@dataclass(frozen=True)
class PackageTarget:
    name: str
    path: pathlib.Path


@dataclass(frozen=True)
class ComplexityFinding:
    package: str
    path: str
    line: int
    col: int
    symbol: str
    score: int
    max_score: int
    raw: str


@dataclass(frozen=True)
class ComplexityException:
    package: str
    path: str
    symbol: str
    score: int
    max_score: int
    reason: str
    follow_up: str

    def matches(self, finding: ComplexityFinding) -> bool:
        return (
            self.package == finding.package
            and self.path == finding.path
            and self.symbol == finding.symbol
            and finding.score <= self.score
            and finding.max_score == self.max_score
        )


def _default_flake8() -> str:
    local = pathlib.Path(".venv/bin/flake8")
    if local.exists():
        return str(local)
    return "flake8"


def parse_package(value: str) -> PackageTarget:
    if ":" not in value:
        raise argparse.ArgumentTypeError("--package values must use the form name:path")
    name, path = value.split(":", 1)
    name = name.strip()
    if not name:
        raise argparse.ArgumentTypeError("package name must be non-empty")
    package_path = pathlib.Path(path)
    if not package_path.exists():
        raise argparse.ArgumentTypeError(f"package path does not exist: {path}")
    return PackageTarget(name=name, path=package_path)


def parse_package_exception_limit(value: str) -> tuple[str, int]:
    if "=" not in value:
        raise argparse.ArgumentTypeError(
            "--max-exceptions-for-package values must use the form package=count"
        )
    name, count_text = value.split("=", 1)
    name = name.strip()
    if not name:
        raise argparse.ArgumentTypeError("package name must be non-empty")
    try:
        count = int(count_text)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("exception count must be an integer") from exc
    if count < 0:
        raise argparse.ArgumentTypeError("exception count must be non-negative")
    return name, count


def load_allowlist(path: pathlib.Path) -> list[ComplexityException]:
    if not path.exists():
        return []
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("complexity allowlist must be a JSON object")
    values = payload.get("exceptions", [])
    if not isinstance(values, list):
        raise ValueError("complexity allowlist 'exceptions' must be a list")
    return [_exception_from_json(item) for item in values]


def _exception_from_json(item: object) -> ComplexityException:
    if not isinstance(item, dict):
        raise ValueError("each complexity exception must be a JSON object")
    required = {
        "package",
        "path",
        "symbol",
        "score",
        "max_score",
        "reason",
        "follow_up",
    }
    missing = sorted(required - set(item))
    if missing:
        raise ValueError(f"complexity exception is missing fields: {missing}")
    return ComplexityException(
        package=_string_field(item, "package"),
        path=_string_field(item, "path"),
        symbol=_string_field(item, "symbol"),
        score=_int_field(item, "score"),
        max_score=_int_field(item, "max_score"),
        reason=_string_field(item, "reason"),
        follow_up=_string_field(item, "follow_up"),
    )


def _string_field(item: dict[str, Any], name: str) -> str:
    value = item[name]
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"complexity exception {name!r} must be a non-empty string")
    return value


def _int_field(item: dict[str, Any], name: str) -> int:
    value = item[name]
    if not isinstance(value, int):
        raise ValueError(f"complexity exception {name!r} must be an integer")
    return value


def run_flake8(
    packages: Iterable[PackageTarget],
    *,
    flake8: str,
    max_complexity: int,
) -> str:
    paths = [str(package.path) for package in packages]
    result = subprocess.run(
        [
            flake8,
            *paths,
            "--select=CCR",
            f"--max-cognitive-complexity={max_complexity}",
        ],
        capture_output=True,
        check=False,
        text=True,
    )
    output = "\n".join(part for part in (result.stdout, result.stderr) if part)
    if result.returncode not in {0, 1}:
        raise RuntimeError(
            output or f"flake8 failed with exit code {result.returncode}"
        )
    return output


def parse_findings(
    output: str,
    packages: Iterable[PackageTarget],
) -> list[ComplexityFinding]:
    package_list = list(packages)
    findings: list[ComplexityFinding] = []
    unparsed: list[str] = []
    for raw in output.splitlines():
        if not raw.strip():
            continue
        match = CCR_RE.match(raw)
        if match is None:
            unparsed.append(raw)
            continue
        path = pathlib.Path(match.group("path"))
        package = package_for_path(path, package_list)
        line = int(match.group("line"))
        findings.append(
            ComplexityFinding(
                package=package,
                path=path.as_posix(),
                line=line,
                col=int(match.group("col")),
                symbol=symbol_at_line(path, line),
                score=int(match.group("score")),
                max_score=int(match.group("max_score")),
                raw=raw,
            )
        )
    if unparsed:
        raise ValueError(
            "Could not parse flake8 complexity output:\n" + "\n".join(unparsed)
        )
    return findings


def package_for_path(path: pathlib.Path, packages: list[PackageTarget]) -> str:
    resolved = path.resolve()
    for package in packages:
        try:
            resolved.relative_to(package.path.resolve())
        except ValueError:
            continue
        return package.name
    return "<unknown>"


def symbol_at_line(path: pathlib.Path, line: int) -> str:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    best: list[tuple[int, int, str]] = []

    def visit(node: ast.AST, parents: tuple[str, ...] = ()) -> None:
        if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            start = node.lineno
            end = getattr(node, "end_lineno", start)
            if start <= line <= end:
                name = ".".join((*parents, node.name))
                best.append((start, end, name))
                visit_children(node, (*parents, node.name))
                return
        visit_children(node, parents)

    def visit_children(node: ast.AST, parents: tuple[str, ...]) -> None:
        for child in ast.iter_child_nodes(node):
            visit(child, parents)

    visit(tree)
    if not best:
        return "<module>"
    return max(best, key=lambda item: item[0])[2]


def validate_exceptions(
    findings: list[ComplexityFinding],
    exceptions: list[ComplexityException],
    *,
    max_exceptions_per_package: int,
    package_exception_limits: dict[str, int],
) -> tuple[list[ComplexityFinding], list[ComplexityException]]:
    package_counts: dict[str, int] = {}
    for exception in exceptions:
        package_counts[exception.package] = package_counts.get(exception.package, 0) + 1
    over_limit = {
        package: count
        for package, count in package_counts.items()
        if count > package_exception_limits.get(package, max_exceptions_per_package)
    }
    if over_limit:
        details = ", ".join(
            (
                f"{package}={count}/"
                f"{package_exception_limits.get(package, max_exceptions_per_package)}"
            )
            for package, count in sorted(over_limit.items())
        )
        raise ValueError(
            "complexity allowlist exceeds max exceptions per package "
            f"(default {max_exceptions_per_package}): {details}"
        )

    unapproved: list[ComplexityFinding] = []
    used: set[int] = set()
    for finding in findings:
        match_index = next(
            (
                index
                for index, exception in enumerate(exceptions)
                if exception.matches(finding)
            ),
            None,
        )
        if match_index is None:
            unapproved.append(finding)
        else:
            used.add(match_index)

    unused = [
        exception for index, exception in enumerate(exceptions) if index not in used
    ]
    return unapproved, unused


def print_report(
    findings: list[ComplexityFinding],
    exceptions: list[ComplexityException],
) -> None:
    if not findings:
        print("No cognitive complexity violations.")
        return
    print("Cognitive complexity violations:")
    for finding in findings:
        status = "approved" if any(e.matches(finding) for e in exceptions) else "new"
        print(
            f"- [{status}] {finding.path}:{finding.line} "
            f"{finding.symbol} ({finding.score} > {finding.max_score})"
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run Flake8 cognitive complexity with an audited allowlist."
    )
    parser.add_argument(
        "--package",
        action="append",
        type=parse_package,
        required=True,
        help="Package target in the form name:path. Repeatable.",
    )
    parser.add_argument(
        "--allowlist",
        type=pathlib.Path,
        default=pathlib.Path("scripts/complexity_allowlist.json"),
        help="JSON allowlist path.",
    )
    parser.add_argument(
        "--max-complexity",
        type=int,
        default=15,
        help="Maximum cognitive complexity before CCR001 is reported.",
    )
    parser.add_argument(
        "--max-exceptions-per-package",
        type=int,
        default=2,
        help="Maximum approved CCR exceptions per package.",
    )
    parser.add_argument(
        "--max-exceptions-for-package",
        action="append",
        default=[],
        type=parse_package_exception_limit,
        metavar="PACKAGE=COUNT",
        help=(
            "Package-specific approved CCR exception limit. Repeatable. "
            "Packages without an override use --max-exceptions-per-package."
        ),
    )
    parser.add_argument(
        "--flake8",
        default=_default_flake8(),
        help="Flake8 executable.",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    packages: list[PackageTarget] = args.package
    package_exception_limits: dict[str, int] = {}
    for package, limit in args.max_exceptions_for_package:
        if package in package_exception_limits:
            parser.error(f"duplicate exception limit for package: {package}")
        package_exception_limits[package] = limit
    try:
        exceptions = load_allowlist(args.allowlist)
        output = run_flake8(
            packages,
            flake8=args.flake8,
            max_complexity=args.max_complexity,
        )
        findings = parse_findings(output, packages)
        unapproved, unused = validate_exceptions(
            findings,
            exceptions,
            max_exceptions_per_package=args.max_exceptions_per_package,
            package_exception_limits=package_exception_limits,
        )
    except Exception as exc:
        print(f"cognitive complexity check failed: {exc}", file=sys.stderr)
        return 2

    print_report(findings, exceptions)
    if unapproved:
        print("\nUnapproved complexity violations:", file=sys.stderr)
        for finding in unapproved:
            print(f"- {finding.raw} [{finding.symbol}]", file=sys.stderr)
    if unused:
        print("\nUnused complexity allowlist entries:", file=sys.stderr)
        for exception in unused:
            print(
                f"- {exception.package} {exception.path} {exception.symbol}",
                file=sys.stderr,
            )
    return 1 if unapproved or unused else 0


if __name__ == "__main__":
    raise SystemExit(main())
