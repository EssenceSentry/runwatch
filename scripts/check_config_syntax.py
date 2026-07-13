#!/usr/bin/env python3
"""Validate repository JSON, TOML, and YAML syntax."""

from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path
from typing import Any

import tomllib
import yaml
from yaml.constructor import ConstructorError

ROOT = Path(__file__).resolve().parents[1]
CONFIG_SUFFIXES = {".json", ".toml", ".yaml", ".yml"}


class UniqueKeyLoader(yaml.BaseLoader):
    """YAML loader that rejects duplicate mapping keys."""


def _construct_mapping(
    loader: UniqueKeyLoader,
    node: yaml.nodes.MappingNode,
    deep: bool = False,
) -> dict[Any, Any]:
    mapping: dict[Any, Any] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if key in mapping:
            raise ConstructorError(
                "while constructing a mapping",
                node.start_mark,
                f"found duplicate key {key!r}",
                key_node.start_mark,
            )
        mapping[key] = loader.construct_object(value_node, deep=deep)
    return mapping


UniqueKeyLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    _construct_mapping,
)


def _tracked_config_files() -> list[str]:
    result = subprocess.run(
        ["git", "ls-files"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return [
        line
        for line in result.stdout.splitlines()
        if Path(line).suffix.lower() in CONFIG_SUFFIXES
    ]


def _json_object_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    values: dict[str, Any] = {}
    for key, value in pairs:
        if key in values:
            raise ValueError(f"duplicate JSON key {key!r}")
        values[key] = value
    return values


def _check_json(path: Path) -> None:
    json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=_json_object_pairs)


def _check_toml(path: Path) -> None:
    tomllib.loads(path.read_text(encoding="utf-8"))


def _check_yaml(path: Path) -> None:
    text = path.read_text(encoding="utf-8")
    list(yaml.load_all(text, Loader=UniqueKeyLoader))


def check_path(path: Path) -> str | None:
    if not path.exists() or path.is_dir():
        return None
    suffix = path.suffix.lower()
    try:
        if suffix == ".json":
            _check_json(path)
        elif suffix == ".toml":
            _check_toml(path)
        elif suffix in {".yaml", ".yml"}:
            _check_yaml(path)
    except Exception as exc:
        return f"{path.relative_to(ROOT).as_posix()}: {exc}"
    return None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "paths",
        nargs="*",
        help="Config files to check. Defaults to tracked JSON/TOML/YAML files.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    paths = args.paths or _tracked_config_files()
    failures = [
        failure
        for raw_path in paths
        if (failure := check_path(ROOT / raw_path)) is not None
    ]
    if failures:
        for failure in failures:
            print(failure)
        return 1

    print(f"Config syntax checks passed for {len(paths)} file(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
