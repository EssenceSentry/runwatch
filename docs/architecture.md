# Architecture

Runwatch is a single local supervisor process with four authority domains:

```text
source.ipynb
  → NotebookRunner / nbclient / kernel epoch
  → RunSupervisor + durable SQLite actions
  → ResourceManager + typed adapters
  → authenticated mobile dashboard, CLI, notifications
```

The kernel is a child process and may be replaced. SQLite and the run artifacts are the
durable authority for run state; AWS remains authoritative for AWS resource state.

## Notebook lifecycle

```text
created → starting → running
                       ├─ failure → paused → resume in live kernel
                       │                    └→ restart/replay in a new kernel epoch
                       ├─ cells done → waiting_external
                       └─ remote resource stop → cancelling → cancelled
        → succeeded | failed | cancelled
```

`input.ipynb` is immutable. `source.ipynb` is a normal, output-free nbformat v4 file
that humans and agents may edit. The runner owns partial and final executed notebooks.

A cell execution timeout follows the same paused recovery path after Runwatch
interrupts and synchronizes the kernel. If synchronization cannot be proven, the
action requires a new kernel instead of offering an unsafe live resume.

A live resume accepts edits only to the failed and future cells. Structural changes or
edits to an already executed cell require restart. Restart reads the complete source
notebook and starts a new kernel epoch. Replay begins at zero unless the local operator
explicitly selects a later start cell.

## Durable actions

Local CLI recovery and resource stops insert SQLite actions before side effects,
including when the original controller process is gone. Each action is
bound to expected kernel, attempt, source, or resource versions and moves through:

```text
requested → executing → completed | rejected | failed
```

Only one supervisor owns a run lock. The lock and run record bind a PID to its process
start time and controller token so PID reuse cannot impersonate the old owner. Pending
actions and resource-monitor cursors survive process restart.

Notification intents and per-destination delivery attempts also live in SQLite. A slow
or failing webhook cannot block another destination, failed deliveries use bounded
exponential backoff, and an attempt interrupted by process shutdown is retried when the
run is reopened.

## Resource protocol

Notebook cells emit `application/vnd.runwatch.resource+json`. The event carries a
provider/type identity, logical reconciliation key, ownership, lifecycle, and typed
metadata. The runner supplies the cell attempt and kernel epoch.

Adapters implement inspection and may optionally implement stop. Observations and
cursors are persisted after each poll. Observation history is bounded per resource and
evenly downsampled across the full retained history for mobile charts. The general event
journal is also bounded; high-volume cell output uses coalesced transient refresh events
instead of growing SQLite without limit. SSE reconnects replay retained events from the
browser's last event ID.

Each adapter also declares whether it can safely block and validates conditional
terminal metadata. The same validation runs for static configuration, public emitters,
and raw notebook events, preventing nonterminal metrics or logs from gating a run.

Only SageMaker Processing initially supports stop. Confirming a remote stop requests
normal run cancellation, interrupts the notebook, and stops all other eligible owned
resources. Eligibility is checked centrally before cancellation. Observation-only
metric/log changes do not invalidate a stop confirmation, while provider-state,
ownership, disposition, and terminal-state changes do.

`local.dashboard` is a special external observation resource. Its adapter health-checks
an explicit loopback URL. The share runtime creates one root-mounted authenticated
reverse proxy per active registration: it binds to the LAN for LAN mode, or binds to
loopback and receives its own quick tunnel for Cloudflare mode. The durable resource is
restored from SQLite, but proxy ports and tunnel URLs are process-owned ephemeral state.
Runwatch snapshots expose only a same-origin Open route, which performs a fresh resource
and link-state check before redirecting the paired browser.

Terminal SageMaker log collection follows CloudWatch pagination until tokens stabilize
or the configured page/line bound is reached. A bounded drain is explicitly reported
as truncated instead of silently presented as complete.

## Process recovery

`runwatch resume RUN_DIR` queues a live resume when the original process owns the run.
If it is gone, the command reacquires the lock, restores adapters from SQLite, starts a
new kernel epoch, and replays from cell zero. Arbitrary Python memory is reconstructed
through replay; Runwatch does not attempt whole-process serialization.

## Agent boundary

Runwatch is agent-agnostic. Codex and Claude use their native filesystem, shell, and
remote applications. A repository skill explains the stable Runwatch CLI and nbformat
workflow; no agent subprocess, transcript, or repair proposal is stored by Runwatch.
