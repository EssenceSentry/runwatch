---
title: "Quality Gates"
subject: "Runwatch Documentation"
short_title: "Quality Gates"
description: "The local and GitHub quality gates required for every Runwatch change."
---

# Quality gates

Install all development dependencies and hooks once:

```bash
uv sync --extra supervisor --extra test --extra dev --extra docs
uv run pre-commit install
```

Run the complete local gate with:

```bash
uv run pre-commit run --all-files
```

The gate checks repository hygiene, configuration and workflow syntax, lockfile
freshness, Ruff, Black, strict Pyright over source and tests, cognitive complexity,
public API docs, the
repository-local agent skill, a clean built-wheel smoke test, strict Sphinx docs, and
the full package suite with branch coverage. Coverage is enforced across `runwatch` at
80%; add focused tests instead of lowering the threshold.

Test modules keep strict semantic diagnostics enabled while narrowly disabling
diagnostics that are inherent to dynamic test doubles and intentional checks of
private implementation details. This keeps Pylance and the command-line Pyright gate
aligned without weakening strict analysis of package source.

The root `pyrightconfig.json` is the canonical configuration for both Pylance and the
local/GitHub Pyright gate. VS Code is pinned to `.venv/bin/python` so third-party types
resolve from the same environment used by the gate.

`.github/workflows/quality-gate.yml` mirrors the non-test local hooks as named jobs.
`.github/workflows/tests.yml` runs the same pytest coverage command on Ubuntu for every
supported Python version and on macOS for Python 3.12. These jobs cover the supported
local POSIX execution platforms; they do not imply Windows or shared-filesystem support.
`scripts/check_quality_gate_parity.py` prevents the Ruff and Black target lists from
drifting between local and remote gates.

The main jobs use the committed lockfile for reproducibility. A separate compatibility
matrix deliberately resolves without that lockfile: Python 3.10 uses uv's
`lowest-direct` strategy to exercise declared dependency floors, while Python 3.13 uses
the latest available versions within Runwatch's declared bounds. Both lanes execute the
private `nbclient` hook signature probes, cleanup serialization, and a real fallback
event notebook run. This catches dependency drift that a single locked environment
cannot expose. `nbclient` is bounded to the minor versions whose private hooks Runwatch
tests directly; widening that bound requires updating the probes first.

The cognitive-complexity ceiling is 15. Exceptions must be explicit in
`scripts/complexity_allowlist.json`, include a reason and follow-up, and fail once
unused. Runwatch currently carries no exceptions.

The wheel gate builds the actual distribution in a temporary directory and verifies the
package, default config, typing marker, dashboard assets, and console command. It first
installs the base wheel and proves that the root API, AWS/local emitter modules, and a
generic resource event work without supervisor dependencies. It then installs the
`supervisor` extra in the same isolated environment and checks the dashboard assets and
CLI. The wheel must not contain an `auto_classifier` namespace.
