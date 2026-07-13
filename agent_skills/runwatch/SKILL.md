---
name: runwatch
description: Execute, monitor, inspect, repair, resume, restart, and verify Runwatch-managed Jupyter notebook runs. Use when launching a notebook with Runwatch, providing its LAN or mobile dashboard URL, monitoring a run, repairing a paused or failed run, editing source.ipynb or imported code, recovering a dead kernel or Runwatch process, inspecting resources, or choosing between live resume and replay.
---

# Runwatch execution and recovery

Treat Runwatch as the execution authority. Use native filesystem and editing tools for
source changes; use the Runwatch CLI for run transitions and resource control.

## Launch and hand off a run

1. Run the notebook with `runwatch execute`. Use `--share lan` whenever the user needs
   to monitor it from another device on the same network.
2. Stay attached to the command output until Runwatch prints its `Dashboard:` line.
   If the command yields a background session, keep polling that session. Do not infer
   or construct the URL yourself.
3. After every launch, immediately give the user the exact dashboard pairing URL,
   including its token. Present it both as a clickable Markdown link and as raw text so
   a mobile client cannot silently rewrite it.
4. For a LAN handoff, verify that the URL uses the machine's LAN address rather than
   `localhost` or `127.0.0.1`. Do not claim the run is handed off until the user has the
   usable URL.
5. Treat the pairing URL as a bearer credential. Share it only in the private response
   to the user; never place it in notifications, commits, pull requests, issues, or
   durable documentation.
6. If Runwatch restarts or prints a replacement pairing URL, give the user the new URL
   and do not reuse the stale one.

## Recover a run

1. Locate the run directory supplied by the user, notification, or `.runwatch/runs/`.
2. Run `runwatch context RUN_DIR --json` and inspect the failed cell, kernel epoch,
   source path, resources, and recent events.
3. Read `RUN_DIR/source.ipynb` with `nbformat.read(..., as_version=4)`. It is the
   canonical editable notebook; never edit `input.ipynb` or executed checkpoints.
4. Make the smallest required change:
   - For a failed/future cell, update the normal `NotebookNode` and write it with
     `nbformat.write`.
   - For imported code, edit the source file with normal repository tools.
5. Choose the transition:
   - Run `runwatch resume RUN_DIR` when no already-executed cell or imported module
     changed. It reuses the live kernel when possible and reconstructs from cell zero
     when the kernel or Runwatch process is gone.
   - Run `runwatch restart RUN_DIR` after imported-code or earlier-cell changes, or when
     an explicit new kernel epoch is required.
   - Use `--from-cell N` only when the selected cell reconstructs all required state;
     this deliberately skips earlier cells in a new kernel. `N` is zero-based and must
     identify a cell in the current `source.ipynb`.
6. Wait for the CLI action to complete or reject, then verify with
   `runwatch status RUN_DIR --json` and, when needed,
   `runwatch events RUN_DIR --follow --json`.

## Safety

- Do not invoke AWS stop/delete commands when a typed Runwatch resource command exists.
- Treat a SageMaker stop as whole-run cancellation; inspect the cascading resources
  before running `runwatch resource stop`.
- Do not assume a new kernel retains Python variables, imports, connections, or threads.
- Do not reuse a stale run, cell attempt, resource version, or source hash after Runwatch
  rejects an action; reload context first.
- Preserve deterministic provider identifiers and logical resource keys during replay.
- When a notebook starts a localhost web UI, register it with
  `local.emit_dashboard(...)` instead of constructing a public URL. Runwatch will expose
  the resource through the current LAN or Cloudflare sharing mode and recreate that
  ephemeral link after recovery.

Read [references/cli-and-emitters.md](references/cli-and-emitters.md) when exact command
syntax, emitter parameters, or manifest structure is needed.
