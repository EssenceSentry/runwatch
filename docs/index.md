---
title: "Runwatch"
subject: "Runwatch Documentation"
short_title: "Runwatch"
description: "Durable notebook execution and typed resource monitoring with runwatch."
---

(docs-runwatch-index)=

# Runwatch

`runwatch` provides durable `nbclient` execution, failure pause and
recovery, typed AWS and local resource monitors, and a mobile dashboard. Runwatch is
agent-agnostic: edit the run-owned `source.ipynb` with ordinary `nbformat`, then use the
`runwatch` CLI to resume or restart.

Install the package:

```bash
python -m pip install "runwatch-notebook"
runwatch init-config runwatch.yaml
runwatch validate notebook.ipynb --config runwatch.yaml
runwatch execute notebook.ipynb --config runwatch.yaml
```

Notebook cells emit resources through the namespaced API:

```python
from runwatch import aws, emit_progress, local

aws.emit_sagemaker_processing_job(job_name, logical_key="build")
local.emit_system_metrics()
local.emit_dashboard("http://127.0.0.1:8501", name="Training UI")
emit_progress(1, total=3, unit="stages")
```

Python notebook runs also capture existing `tqdm`, `tqdm.auto`, and `tqdm.notebook`
progress bars automatically; no Runwatch-specific loop wrapper is required.

See the repository `README.md` for the operational workflow and
complete emitter examples.

```{toctree}
:maxdepth: 2
:hidden:

api
architecture
resource-events
security
quality-gates
```
