#!/usr/bin/env python3
"""Generate public API documentation coverage diagnostics for Runwatch.

This script discovers public symbols by reading ``__all__`` declarations under a
package source root and resolves them to canonical definitions.

Output records canonical symbols alongside the public aliases that expose them.
"""

from __future__ import annotations

import argparse
import ast
import json
import pathlib
import textwrap
from dataclasses import dataclass
from typing import Iterable, Mapping

SECTION_HEADERS = {
    "Attributes",
    "Parameters",
    "Returns",
    "Raises",
    "Examples",
    "Methods",
    "See Also",
    "Notes",
    "References",
}

DEFAULT_PACKAGE_NAME = "runwatch"


@dataclass(frozen=True)
class SymbolInfo:
    canonical_module: str
    canonical_name: str
    canonical_fqn: str
    public_symbols: tuple[str, ...]
    public_modules: tuple[str, ...]
    file: str
    kind: str
    stability_tier: str
    has_docstring: bool
    has_sections: bool
    sections: tuple[str, ...]


@dataclass(frozen=True)
class ModuleData:
    name: str
    path: pathlib.Path
    all_names: tuple[str, ...]
    defs: Mapping[str, ast.AST]
    imports: Mapping[str, tuple[str, str]]
    docstrings: Mapping[str, str | None]


def infer_package_name(source_root: pathlib.Path) -> str:
    parts = list(source_root.resolve().parts)
    if "src" in parts:
        src_index = len(parts) - 1 - list(reversed(parts)).index("src")
        parts = parts[src_index + 1 :]
    return ".".join(parts)


def validate_source_root(
    source_root: pathlib.Path,
    *,
    expected_package_name: str,
) -> pathlib.Path:
    resolved = source_root.resolve()
    package_name = infer_package_name(resolved)
    if package_name != expected_package_name:
        raise ValueError(
            "--source-root must point at the requested package root "
            f"{expected_package_name!r}; "
            f"got package root {package_name!r} from {source_root}."
        )
    if not (resolved / "__init__.py").is_file():
        raise ValueError(
            f"--source-root must contain a package __init__.py: {resolved}"
        )
    return resolved


def path_to_module(source_root: pathlib.Path, path: pathlib.Path) -> str:
    package_name = infer_package_name(source_root)
    relative = path.relative_to(source_root).with_suffix("")
    rel_parts = list(relative.parts)
    if not rel_parts:
        return package_name
    if rel_parts[-1] == "__init__":
        rel_parts = rel_parts[:-1]
    if not rel_parts:
        return package_name
    return package_name + "." + ".".join(rel_parts)


def _extract_all(
    node: ast.Assign | ast.AnnAssign,
    *,
    lazy_exports: Mapping[str, tuple[str, str]],
) -> list[str]:
    target_names: list[str] = []
    if isinstance(node, ast.Assign):
        target_names = [t.id for t in node.targets if isinstance(t, ast.Name)]
    elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
        target_names = [node.target.id]

    if "__all__" not in target_names:
        return []

    value = node.value
    out: list[str] = []
    if isinstance(value, (ast.List, ast.Tuple)):
        for item in value.elts:
            if isinstance(item, ast.Constant) and isinstance(item.value, str):
                out.append(item.value)
    elif (
        isinstance(value, ast.Call)
        and isinstance(value.func, ast.Name)
        and value.func.id == "list"
        and len(value.args) == 1
        and isinstance(value.args[0], ast.Name)
        and value.args[0].id == "_EXPORT_MODULES"
    ):
        out.extend(lazy_exports)
    return out


def _extract_module_or_expr_doc(node: ast.stmt, *, is_module_doc: bool) -> str | None:
    if is_module_doc or not isinstance(node, ast.Expr):
        return None
    value = node.value
    if isinstance(value, ast.Constant) and isinstance(value.value, str):
        return textwrap.dedent(value.value)
    return None


def parse_module(source_root: pathlib.Path, path: pathlib.Path) -> ModuleData:
    text = path.read_text(encoding="utf-8")
    tree = ast.parse(text)
    module_name = path_to_module(source_root, path)

    all_names: list[str] = []
    defs: dict[str, ast.AST] = {}
    imports: dict[str, tuple[str, str]] = {}
    lazy_exports: dict[str, tuple[str, str]] = {}
    docstrings: dict[str, str | None] = {}

    pending_doc: str | None = None

    for index, node in enumerate(tree.body):
        is_module_doc = index == 0
        node_doc = _extract_module_or_expr_doc(node, is_module_doc=is_module_doc)
        if node_doc is not None:
            pending_doc = node_doc
            continue

        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            defs[node.name] = node
            docstrings[node.name] = ast.get_docstring(node)
            pending_doc = None
            continue

        if isinstance(node, ast.Expr):
            pending_doc = None
            continue

        if isinstance(node, (ast.Assign, ast.AnnAssign)):
            all_names.extend(_extract_all(node, lazy_exports=lazy_exports))

            targets: list[ast.expr] = []
            if isinstance(node, ast.Assign):
                targets = list(node.targets)
            else:
                targets = [node.target]

            if len(targets) == 1 and isinstance(targets[0], ast.Attribute):
                target = targets[0]
                if (
                    isinstance(target.value, ast.Name)
                    and target.attr == "__doc__"
                    and isinstance(node.value, ast.Constant)
                    and isinstance(node.value.value, str)
                ):
                    docstrings[target.value.id] = textwrap.dedent(node.value.value)

            target_names: list[str] = [t.id for t in targets if isinstance(t, ast.Name)]
            if pending_doc is not None and target_names:
                for target_name in target_names:
                    docstrings[target_name] = pending_doc

            if (
                len(target_names) == 1
                and target_names[0] == "_EXPORT_MODULES"
                and isinstance(node.value, ast.Dict)
            ):
                extracted = _extract_lazy_export_imports(node.value)
                lazy_exports.update(extracted)
                imports.update(extracted)

            pending_doc = None
            continue

        if isinstance(node, ast.ImportFrom):
            if node.module == "__future__":
                pending_doc = None
                continue
            imported_module = resolve_from_module(
                module_name,
                node.module,
                node.level,
                is_package=path.name == "__init__.py",
            )
            if imported_module is None:
                pending_doc = None
                continue
            for alias in node.names:
                if alias.name == "*":
                    continue
                local_name = alias.asname or alias.name
                imports[local_name] = (imported_module, alias.name)
            pending_doc = None
            continue

        if isinstance(node, ast.Import):
            for alias in node.names:
                local_name = alias.asname or alias.name.split(".")[0]
                imports[local_name] = (alias.name, alias.name.split(".")[-1])
            pending_doc = None
            continue

        pending_doc = None

    docstrings.setdefault("__module__", None)

    return ModuleData(
        name=module_name,
        path=path,
        all_names=tuple(all_names),
        defs=defs,
        imports=imports,
        docstrings=docstrings,
    )


def _extract_lazy_export_imports(value: ast.Dict) -> dict[str, tuple[str, str]]:
    imports: dict[str, tuple[str, str]] = {}
    for key_node, value_node in zip(value.keys, value.values, strict=True):
        if (
            isinstance(key_node, ast.Constant)
            and isinstance(key_node.value, str)
            and isinstance(value_node, ast.Constant)
            and isinstance(value_node.value, str)
        ):
            imports[key_node.value] = (value_node.value, key_node.value)
    return imports


def resolve_from_module(
    current_module: str, module: str | None, level: int, *, is_package: bool
) -> str | None:
    if level == 0:
        return module

    base_parts = current_module.split(".")
    if not is_package:
        base_parts = base_parts[:-1]

    parent_hops = level - 1
    if parent_hops > len(base_parts):
        return None
    if parent_hops:
        base_parts = base_parts[:-parent_hops]
    if module:
        base_parts.extend(module.split("."))
    return ".".join(filter(None, base_parts)) or None


def collect_modules(source_root: pathlib.Path) -> dict[str, ModuleData]:
    modules: dict[str, ModuleData] = {}
    for path in source_root.rglob("*.py"):
        if "__pycache__" in path.parts:
            continue
        module = parse_module(source_root, path)
        modules[module.name] = module
    return modules


def extract_sections(doc: str | None) -> tuple[str, ...]:
    if not doc:
        return ()
    lines = textwrap.dedent(doc).splitlines()
    found: list[str] = []
    for line in lines:
        header = line.strip()
        if header in SECTION_HEADERS:
            found.append(header)
    return tuple(found)


def has_common_sections(kind: str, doc: str | None) -> tuple[bool, tuple[str, ...]]:
    sections = extract_sections(doc)
    section_set = set(sections)
    if kind == "function":
        required = {"Parameters", "Returns"}
        return required.issubset(section_set), sections
    elif kind == "class":
        return bool(section_set & {"Parameters", "Attributes", "Methods"}), sections
    else:
        return True, sections


def resolve_owner(
    modules: Mapping[str, ModuleData],
    module_name: str,
    symbol: str,
    *,
    seen: set[tuple[str, str]] | None = None,
) -> tuple[str, str] | None:
    if seen is None:
        seen = set()
    if (module_name, symbol) in seen:
        return None
    seen.add((module_name, symbol))

    module = modules.get(module_name)
    if module is None:
        return None

    if symbol in module.defs:
        return module.name, symbol

    imported = module.imports.get(symbol)
    if imported is None:
        return module.name, symbol

    upstream_module, upstream_name = imported
    if upstream_module == symbol:
        return module.name, symbol

    if upstream_module in modules:
        resolved = resolve_owner(modules, upstream_module, upstream_name, seen=seen)
        if resolved is not None:
            return resolved

    return upstream_module, upstream_name


def allowed_public_modules(
    modules: Mapping[str, ModuleData],
    *,
    package_name: str,
    public_suffixes: Iterable[str],
) -> tuple[str, ...]:
    allowed = {
        package_name if not suffix else f"{package_name}.{suffix}"
        for suffix in public_suffixes
    }
    return tuple(sorted(module for module in modules if module in allowed))


def stability_tier(
    public_module: str,
    *,
    package_name: str,
    public_tiers: Mapping[str, str],
) -> str:
    suffix = public_module.removeprefix(package_name).lstrip(".")
    return public_tiers.get(suffix, "internal-exported")


def _symbol_kind(
    modules: Mapping[str, ModuleData], module_name: str, symbol: str
) -> str:
    module = modules.get(module_name)
    if module is None:
        return "symbol"
    node = module.defs.get(symbol)
    if isinstance(node, ast.ClassDef):
        return "class"
    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
        return "function"
    return "symbol"


def _display_path(path: pathlib.Path) -> str:
    resolved = path.resolve()
    try:
        return resolved.relative_to(pathlib.Path.cwd().resolve()).as_posix()
    except ValueError:
        return resolved.as_posix()


def scan_public_symbols(
    modules: Mapping[str, ModuleData],
    *,
    dedupe: bool,
    public_suffixes: Iterable[str],
    public_tiers: Mapping[str, str],
) -> list[SymbolInfo]:
    entries: list[tuple[str, str, str, str, str, str, bool, bool, tuple[str, ...]]] = []
    if not modules:
        return []
    package_name = min(modules, key=lambda name: name.count("."))
    public_module_names = set(
        allowed_public_modules(
            modules,
            package_name=package_name,
            public_suffixes=public_suffixes,
        )
    )
    for module in modules.values():
        if module.name not in public_module_names:
            continue
        for symbol in module.all_names:
            if symbol.startswith("_"):
                continue
            canonical_module, canonical_name = resolve_owner(
                modules, module.name, symbol
            )
            if canonical_module is None:
                continue
            if canonical_name.startswith("_"):
                continue

            canonical = modules.get(canonical_module)
            doc = (
                canonical.docstrings.get(canonical_name)
                if canonical is not None
                else None
            )
            kind = _symbol_kind(modules, canonical_module, canonical_name)
            has_sections, sections = has_common_sections(kind, doc)
            canonical_fqn = f"{canonical_module}.{canonical_name}"
            entries.append(
                (
                    f"{module.name}.{symbol}",
                    module.name,
                    stability_tier(
                        module.name,
                        package_name=package_name,
                        public_tiers=public_tiers,
                    ),
                    canonical_fqn,
                    canonical_module,
                    canonical_name,
                    canonical is not None and bool(doc and doc.strip()),
                    bool(has_sections and doc and doc.strip()),
                    sections,
                )
            )

    if not dedupe:
        out: list[SymbolInfo] = []
        for (
            public_fqn,
            public_module,
            public_tier,
            canonical_fqn,
            canonical_module,
            canonical_name,
            has_docstring,
            has_sections,
            sections,
        ) in entries:
            canonical = modules.get(canonical_module)
            file = _display_path(canonical.path) if canonical else ""
            kind = _symbol_kind(modules, canonical_module, canonical_name)
            out.append(
                SymbolInfo(
                    canonical_module=canonical_module,
                    canonical_name=canonical_name,
                    canonical_fqn=canonical_fqn,
                    public_symbols=(public_fqn,),
                    public_modules=(public_module,),
                    file=file,
                    kind=kind,
                    stability_tier=public_tier,
                    has_docstring=has_docstring,
                    has_sections=has_sections,
                    sections=sections,
                )
            )
        return sorted(
            out, key=lambda item: (item.canonical_fqn, item.public_symbols[0])
        )

    grouped: dict[str, list[tuple[str, str, str, bool, tuple[str, ...], bool]]] = {}
    for (
        public_fqn,
        public_module,
        public_tier,
        canonical_fqn,
        canonical_module,
        canonical_name,
        has_docstring,
        has_sections,
        sections,
    ) in entries:
        grouped.setdefault(canonical_fqn, []).append(
            (
                public_fqn,
                public_module,
                public_tier,
                has_docstring,
                sections,
                has_sections,
            )
        )

    out: list[SymbolInfo] = []
    for canonical_fqn, aliases in grouped.items():
        canonical_module, canonical_name = canonical_fqn.rsplit(".", 1)
        canonical = modules.get(canonical_module)
        file = _display_path(canonical.path) if canonical is not None else ""
        public_symbols = tuple(sorted(entry[0] for entry in aliases))
        public_modules = tuple(sorted({entry[1] for entry in aliases}))
        public_tiers = {entry[2] for entry in aliases}
        out.append(
            SymbolInfo(
                canonical_module=canonical_module,
                canonical_name=canonical_name,
                canonical_fqn=canonical_fqn,
                public_symbols=public_symbols,
                public_modules=public_modules,
                file=file,
                kind=_symbol_kind(modules, canonical_module, canonical_name),
                stability_tier=_merged_stability_tier(public_tiers),
                has_docstring=all(entry[3] for entry in aliases),
                has_sections=all(entry[5] for entry in aliases if entry[3]),
                sections=next((entry[4] for entry in aliases), ()),
            )
        )
    out.sort(key=lambda item: (item.canonical_fqn, item.public_symbols[0]))
    return out


def _merged_stability_tier(tiers: set[str]) -> str:
    if "stable" in tiers:
        return "stable"
    if "experimental" in tiers:
        return "experimental"
    return "internal-exported"


def to_markdown(entries: Iterable[SymbolInfo], *, package_name: str) -> str:
    title = f"{package_name} Public API Inventory"
    lines = [
        f"# {title}",
        "",
        "Generated by `scripts/check_api_docs.py` from configured "
        "public `__all__` boundaries.",
        "",
        "Stability tiers are release-review labels: `stable` APIs are intended "
        "to keep compatibility, `experimental` APIs may change with release "
        "notes, and `internal-exported` names require explicit review before "
        "promotion.",
        "",
        "| Canonical symbol | Public symbols | Public modules | Stability | File | Kind | Docstring | NumPy sections |",
        "| --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for item in entries:
        lines.append(
            "| `{}` | `{}` | `{}` | {} | `{}` | {} | {} | {} |".format(
                item.canonical_fqn,
                ", ".join(item.public_symbols),
                ", ".join(item.public_modules),
                item.stability_tier,
                item.file,
                item.kind,
                "yes" if item.has_docstring else "no",
                "yes" if item.has_sections else "no",
            )
        )
    return "\n".join(lines)


def to_json(entries: Iterable[SymbolInfo], *, dedupe: bool) -> str:
    payload = []
    for item in entries:
        item_payload = {
            "canonical_symbol": item.canonical_fqn,
            "public_symbols": list(item.public_symbols),
            "public_modules": list(item.public_modules),
            "stability_tier": item.stability_tier,
            "file": item.file,
            "kind": item.kind,
            "has_docstring": item.has_docstring,
            "has_numpy_sections": item.has_sections,
            "has_scipy_sections": item.has_sections,
            "sections": list(item.sections),
            "deduplicated": dedupe,
        }
        if dedupe:
            item_payload["alias_count"] = len(item.public_symbols)
        payload.append(item_payload)
    return json.dumps(payload, indent=2, sort_keys=True)


def default_public_suffixes(package_name: str) -> tuple[str, ...]:
    del package_name
    return ("",)


def default_public_tiers(
    package_name: str,
    public_suffixes: Iterable[str],
) -> dict[str, str]:
    del package_name
    return {suffix: "stable" for suffix in public_suffixes}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Detect public API doc coverage for Runwatch"
    )
    parser.add_argument(
        "--source-root",
        default="src/runwatch",
        help="Package source root to scan",
    )
    parser.add_argument(
        "--package-name",
        default=DEFAULT_PACKAGE_NAME,
        help="Expected import package name for --source-root",
    )
    parser.add_argument(
        "--public-module-suffix",
        action="append",
        default=None,
        help=(
            "Public module suffix to scan, relative to --package-name. "
            "Use an empty string for the package root. Repeatable. "
            "Defaults to the package root."
        ),
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit JSON output instead of markdown",
    )
    parser.add_argument(
        "--emit-index",
        default=None,
        help="Optional path to write markdown index",
    )
    parser.add_argument(
        "--dedupe",
        default=True,
        action="store_true",
        help="Collapse aliases to their canonical symbols (default).",
    )
    parser.add_argument(
        "--no-dedupe",
        dest="dedupe",
        action="store_false",
        help="Emit one row per public alias instead of canonical groups.",
    )
    parser.add_argument(
        "--require-sections",
        action="store_true",
        help=(
            "Exit non-zero if any callable/public class/function has documentation "
            "but misses Parameters/Returns sections."
        ),
    )
    parser.add_argument(
        "--require-docstring",
        action="store_true",
        help=("Exit non-zero if any canonical symbol lacks a docstring."),
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    try:
        source_root = validate_source_root(
            pathlib.Path(args.source_root),
            expected_package_name=args.package_name,
        )
    except ValueError as exc:
        parser.error(str(exc))
    public_suffixes = tuple(
        args.public_module_suffix
        if args.public_module_suffix is not None
        else default_public_suffixes(args.package_name)
    )
    public_tiers = default_public_tiers(args.package_name, public_suffixes)
    modules = collect_modules(source_root)
    entries = scan_public_symbols(
        modules,
        dedupe=args.dedupe,
        public_suffixes=public_suffixes,
        public_tiers=public_tiers,
    )

    payload = (
        to_json(entries, dedupe=args.dedupe)
        if args.json
        else to_markdown(entries, package_name=args.package_name)
    )

    if args.emit_index:
        pathlib.Path(args.emit_index).write_text(payload + "\n", encoding="utf-8")

    print(payload)

    if args.require_docstring:
        missing_docstrings = [e for e in entries if not e.has_docstring]
        if missing_docstrings:
            return 1

    if args.require_sections:
        missing_sections = [
            e
            for e in entries
            if e.kind in {"class", "function"}
            and e.has_docstring
            and not e.has_sections
        ]
        if missing_sections:
            return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
