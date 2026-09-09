# Marimo notebooks

Runwatch executes native Marimo Python notebooks in a dedicated, browserless session.
Install a chosen snapshot with the optional backend into a prepared environment
before launching a run. Replace the example commit timestamp, or use a pinned source
installation as described in {doc}`releasing`:

```bash
python -m pip install 'runwatch-notebook[supervisor,marimo]==0.0.dev<commit_timestamp>'
runwatch validate analysis.py --config runwatch.yaml
runwatch execute analysis.py --config runwatch.yaml --keep-run
```

`notebook.backend` defaults to `auto`: `.ipynb` selects Jupyter and `.py` must pass
Marimo's static notebook and graph validation. An ordinary Python script is rejected.
Validation compiles source without executing notebook cells. Inline PEP 723 Python
and dependency requirements must already be satisfied; Runwatch never provisions an
environment or installs notebook dependencies.

```yaml
schema_version: 2
notebook:
  backend: marimo
  marimo:
    python_executable: /path/to/prepared/venv/bin/python
    capture_native_progress: true
  capture_tqdm: true
  timeout_seconds: null
```

Omit `python_executable` to use Runwatch's interpreter. `kernel_name` belongs to
Jupyter and is rejected for Marimo. The notebook interpreter needs Marimo and any
packages imported by the notebook, including Runwatch when using its emitters.

## Execution and repair

Startup execution, reactive autorun, module autoreloading, and automatic dependency
installation are disabled. Source saves never request execution. Marimo orders each
explicit batch by dependency relationships; independent branches may finish after
another branch fails. The run then pauses with its kernel and resource monitors live.

Use `runwatch context RUN_DIR --json` to inspect the backend, graph parents, active
cells, failed cells, source path, attempts, and epoch. Edit only `RUN_DIR/source.py`.
`input.py` preserves the original bytes, while `marimo-runtime/runtime.py` is owned by
the runner and must never be edited directly. Checkpoints include only source that
has been applied to the runtime, so pending edits do not masquerade as executed code.

`runwatch resume RUN_DIR` accepts body changes with an unambiguous cell mapping and
runs failed, interrupted, stale, and failure-blocked work. Successful unaffected
branches retain their attempts and outputs. Changes to cell structure/order, setup,
imported Python modules, or the prepared environment require `runwatch restart`.
Adjacent changed unnamed cells are treated conservatively as an ambiguous mapping
and require restart. Never reinterpret an ambiguous mapping as a live repair.

An unqualified restart creates a new epoch and runs the whole graph. For Marimo,
`runwatch restart RUN_DIR --from-cell N` selects document cell N, its descendants,
and every required ancestor. Unrelated branches are `not_replayed`. Indexes are
zero-based; this differs from Jupyter, where the selector starts sequential replay
at N and deliberately skips earlier cells.

Disabled cells and intentional `mo.stop` branches are skipped with reasons. Native
`idle` notifications are provisional until execution status and errors are reconciled.
Cell IDs are scoped to the runtime epoch and applied source generation. Cell timeouts
interrupt execution and pause for repair; cancellation has bounded escalation through
interrupt and process termination. After ambiguous transport loss, Runwatch stops the
owned runtime and requires an explicit recovery action, without resubmitting the batch.
After controller loss, PID/start-time identities are checked and surviving owned
processes are stopped before reconstruction.

## Outputs and monitoring

The same Runwatch resource emitters, ownership rules, blocking waits, metrics, linked
dashboards, webhook/ntfy delivery, host sleep inhibition, and sharing modes apply.
Both tqdm and `mo.status.progress_bar` feed the progress and Bayesian ETA model.
Literal Markdown headings in `mo.md` cells can trigger section notifications, once
per section and execution batch.

Results are `<stem>.executed.html` and `<stem>.executed.session.json` beside the original
notebook. `--output PATH.html` relocates both (`PATH.session.json` is the companion).
The original `.py` receives conflict-guarded repaired source; outputs live in the
sidecar. Partial and final snapshots include source, cell identities, errors, outputs,
and execution metadata. Runtime image assets are copied into data URLs before shutdown.
Snapshots remain readable without Marimo running. Native tables get static previews of up to 1,000 available rows;
interactive widgets use static content or an explicit read-only placeholder.

Artifacts are published on pause and terminal transitions. Failed publication prevents
automatic run-directory cleanup. Dashboard notebook views and result HTML retain the
existing scriptless rendering boundary and never execute cells. Share the Runwatch
dashboard; the authenticated Marimo control server binds only to loopback and is private.

## Compatibility boundary

The adapter uses the private code-mode API behind
[Marimo Pair](https://marimo.io/blog/marimo-pair), rather than installing Pair's agent
skill. The supported range is `marimo>=0.24.0,<0.25`; `uv.lock` records the tested
version. A runtime capability probe checks the code-mode and configuration contract
before user cells run. Upgrading this range requires native integration validation.
See [Marimo runtime configuration](https://docs.marimo.io/guides/configuration/runtime_configuration/).

Manifest v4 records the backend; database v4, snapshot v3, and CLI/dashboard
presentation v2 carry native identity and graph information. Manifest v2/v3 runs
retain Jupyter semantics. Runwatch's configuration and emitter schemas remain v2.
Jupyter remains usable without installing the Marimo extra.

Attaching existing sessions, interactive editing through Runwatch, notebook conversion,
environment provisioning, and automatic Jupyter export are outside this backend.
