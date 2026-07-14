# Runwatch CLI and emitters

## CLI

```text
runwatch execute NOTEBOOK [--config PATH] [--run-dir DIR] [--share none|lan|cloudflared] [--host HOST] [--port PORT]
runwatch validate NOTEBOOK [--config PATH] [--json]
runwatch status RUN_DIR [--json]
runwatch context RUN_DIR [--json | --format markdown|json]
runwatch events RUN_DIR [--follow] [--json]
runwatch resume RUN_DIR
runwatch restart RUN_DIR [--from-cell N]
runwatch resource stop RUN_DIR RESOURCE_ID
runwatch open RUN_DIR
runwatch init-config [PATH]
runwatch version
```

## Dashboard handoff

For monitoring from a phone or another machine on the same network, launch with LAN
sharing enabled:

```text
runwatch execute notebook.ipynb --share lan
```

Wait for the authoritative line printed by Runwatch:

```text
Dashboard: http://192.168.1.20:8765/?token=...
```

Give that exact URL to the user immediately after launch. Include both forms:

```text
[Open Runwatch dashboard](http://192.168.1.20:8765/?token=...)
Raw URL: `http://192.168.1.20:8765/?token=...`
```

Do not substitute `localhost`, omit the token, or reconstruct the URL from separate
host, port, or token values. The pairing URL is a bearer credential: send it only to
the user in the private conversation, and replace it if Runwatch prints a new one.

`resume` reloads `source.ipynb` and uses the live kernel only when prior executed cells
are unchanged. `restart` creates a new kernel epoch. If the old process is gone,
`resume` journals a durable action and reconstructs the run from cell zero. Offline
resource stops are journaled the same way. `--from-cell N` is zero-based and must
identify a cell in the current source notebook.

## Notebook editing

```python
from pathlib import Path

import nbformat

path = Path("RUN_DIR/source.ipynb")
notebook = nbformat.read(path, as_version=4)
notebook.cells[INDEX].source = "replacement source"
nbformat.write(notebook, path)
```

Cell insertion, deletion, or reordering requires restart.

## Emitters

```python
from runwatch import aws, emit_progress, local

# Safe default for an existing job: borrowed and observation-only.
aws.emit_sagemaker_processing_job(existing_job_name, logical_key="observed-build")
# Only for a job this run created and is allowed to stop.
aws.emit_owned_sagemaker_processing_job(created_job_name, logical_key="owned-build")
aws.emit_s3_prefix(uri, expected_count=100, completion_marker="_SUCCESS", blocking=True)
aws.emit_s3_manifest(uri, blocking=True)
aws.emit_cloudwatch_metric(namespace="Pipeline", metric_name="Rows")
aws.emit_cloudwatch_logs(log_group="/aws/example", stream_prefix="run/")

local.emit_system_metrics(include_host=True, include_kernel=True, gpu="all")
local.emit_file_count(path, pattern="*.parquet", expected_count=100, blocking=True)
local.emit_line_count(path, expected_lines=1000, tail_lines=100, blocking=True)
local.emit_dashboard(
    "http://127.0.0.1:8501",
    name="Training dashboard",
    health_path="/_stcore/health",
    logical_key="training-dashboard",
)

emit_progress(50, total=100, unit="parts", message="Building")
```

Metrics and logs are nonblocking. A blocking S3 prefix or local file monitor must define
a completion condition. The generic SageMaker emitter is borrowed and does not stop the
job during cancellation unless ownership is explicitly opted in; prefer the owned helper
for a job created by the current run.

`local.emit_dashboard` registers an existing loopback HTTP application. It is external,
nonblocking, and cannot be stopped by Runwatch. With LAN or Cloudflare sharing, the
Runwatch resource card opens an authenticated reverse proxy using the current pairing
session. Keep the local URL deterministic across replay; Runwatch recreates the
ephemeral share after process recovery.

S3 prefix polling is incremental between periodic full reconciliations. Pass
`full_rescan_seconds=0` for an exact full listing on every poll.

## S3 manifest

```json
{
  "schema_version": 1,
  "status": "running",
  "completed": 50,
  "total": 100,
  "message": "Building",
  "metrics": {"rows": 1000000}
}
```

Status is `running`, `completed`, or `failed`; metrics are scalar values.
