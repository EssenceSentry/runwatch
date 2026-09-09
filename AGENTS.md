# Runwatch repository guidance

Runwatch is a standalone, agent-agnostic package. Its public import namespace is
`runwatch`; do not add `auto_classifier` compatibility imports or dependencies.

Read [RELEASE.md](RELEASE.md) before changing versioning, distribution, or snapshot
policy. Versions come from Git commit timestamps; do not add manual version bumps
or local-version suffixes that prevent PyPI publication.

Use `src/runwatch/` for implementation, `tests/` for package tests,
`web_artifacts/runwatch/` for the dashboard, and
`web_artifacts_fake_sessions/runwatch/` for the replayable demonstration. Shared
dashboard styles live in `web_artifacts/common/`.

When launching a notebook for a user, use `runwatch execute ... --share lan` when
phone access is requested. Stay attached until the CLI prints `Dashboard:` and give
the user that exact tokenized LAN URL immediately, as both a clickable link and raw
text. Never reconstruct the URL or publish it in commits, notifications, issues, or
documentation; it is a bearer credential. The one product-level exception is an
explicitly configured ntfy topic, which receives a rotated Cloudflare pairing URL as a
clickable target.

Run `uv sync --extra supervisor --extra marimo --extra test --extra dev --extra docs` before development. The required
local gate is `uv run pre-commit run --all-files`. Keep the GitHub quality workflow and
pre-commit lint target lists in parity, keep branch coverage at or above 80%, and do
not lower a gate to accommodate a change.

Runwatch supports Jupyter `.ipynb` and native Marimo `.py` notebooks. Agents edit only
the run-owned `source.ipynb` or `source.py`; immutable inputs, checkpoints, and runtime
copies belong to the runner. Jupyter resume accepts failed/future-cell edits. Marimo
resume accepts unambiguous cell-body repairs and preserves unaffected branches. Use
restart after structural, imported-source, or environment changes. For Marimo, an
explicit from-cell selector includes descendants and required ancestors; Jupyter
retains sequential replay semantics. Validate private Marimo API changes against the
locked runtime before changing the supported version range. See
`agent_skills/runwatch/SKILL.md` for the operational workflow.
