# Runwatch repository guidance

Runwatch is a standalone, agent-agnostic package. Its public import namespace is
`runwatch`; do not add `auto_classifier` compatibility imports or dependencies.

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

Run `uv sync --extra test --extra dev --extra docs` before development. The required
local gate is `uv run pre-commit run --all-files`. Keep the GitHub quality workflow and
pre-commit lint target lists in parity, keep branch coverage at or above 80%, and do
not lower a gate to accommodate a change.

Runwatch-managed notebooks are ordinary nbformat documents. Agents edit only a
run-owned `source.ipynb`, choose `resume` only for failed/future-cell edits, and use
`restart` after changing earlier cells or imported source. See
`agent_skills/runwatch/SKILL.md` for the operational workflow.
