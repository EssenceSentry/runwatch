#!/usr/bin/env bash
# Launch the active notebook with its workspace environment and Runwatch.
set -euo pipefail

if [ "$#" -ne 1 ]; then
    echo "Usage: run_active_notebook.sh <notebook-path>" >&2
    exit 1
fi

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "$script_dir/.." && pwd)"
notebook_path="$1"
workspace_root="$("$script_dir/resolve_notebook_workspace.sh" "$notebook_path")"

workspace_python="$workspace_root/.venv/bin/python"
workspace_dotenv="$workspace_root/.venv/bin/dotenv"
runwatch="$repo_root/.venv/bin/runwatch"

for executable in "$workspace_python" "$workspace_dotenv" "$runwatch"; do
    if [ ! -x "$executable" ]; then
        echo "Error: required executable not found: $executable" >&2
        exit 1
    fi
done

"$workspace_python" -m ipykernel install \
    --prefix "$workspace_root/.venv" \
    --name runwatch-workspace \
    --display-name "Runwatch workspace (.venv)"

export PATH="$workspace_root/.venv/bin:$PATH"
export JUPYTER_PATH="$workspace_root/.venv/share/jupyter${JUPYTER_PATH:+:$JUPYTER_PATH}"

exec "$workspace_dotenv" \
    --file "$workspace_root/.env" \
    run \
    -- \
    "$runwatch" execute "$notebook_path" \
    --config "$repo_root/.vscode/runwatch.yaml" \
    --share cloudflared
