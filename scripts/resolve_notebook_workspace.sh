#!/usr/bin/env bash
# Resolve the workspace root (nearest ancestor with pyproject.toml) for a given
# notebook file path, then print it. Used by VS Code tasks that cannot rely on
# ${fileWorkspaceFolder} for notebook editors.
set -euo pipefail

if [ "$#" -ne 1 ]; then
    echo "Usage: resolve_notebook_workspace.sh <notebook-path>" >&2
    exit 1
fi

notebook_dir="$(dirname -- "$1")"
if [ ! -d "$notebook_dir" ]; then
    echo "Error: notebook directory does not exist: $notebook_dir" >&2
    exit 1
fi

d="$(cd -- "$notebook_dir" && pwd)"
while true; do
    if [ -f "$d/pyproject.toml" ]; then
        printf '%s\n' "$d"
        exit 0
    fi
    if [ "$d" = "/" ]; then
        break
    fi
    d="$(dirname -- "$d")"
done

echo "Error: no pyproject.toml found above $notebook_dir" >&2
exit 1
