---
title: "Releasing Runwatch"
subject: "Runwatch Documentation"
short_title: "Releasing"
description: "Maintainer workflow for publishing verified Runwatch distributions to PyPI."
---

# Releasing Runwatch

Runwatch publishes the `runwatch-notebook` distribution from GitHub Releases through
PyPI Trusted Publishing. The release workflow stores no PyPI API token: only its
`pypi-publish` job can request a short-lived OIDC credential, after an unprivileged job
has built and verified the distributions.

## One-time trust configuration

The repository needs a protected GitHub environment named `pypi`. PyPI's trusted
publisher must match these values exactly:

```text
Project:      runwatch-notebook
Owner:        EssenceSentry
Repository:   runwatch
Workflow:     release.yml
Environment:  pypi
```

For the first release, register a pending publisher from the PyPI account publishing
page. After the project exists, manage the publisher from the project's **Publishing**
page. Keep manual approval enabled on the GitHub environment and restrict deployments
to version tags.

## Prepare a release

1. Update `project.version` in `pyproject.toml`. It is the canonical package version;
   documentation and wheel checks derive the version from package metadata.
2. Update user-facing documentation and release notes for the version.
3. Refresh `uv.lock`, then run `uv run pre-commit run --all-files`.
4. Commit and push the exact validated tree to `main`.
5. Create a GitHub Release whose tag is exactly `v<project.version>`, such as
   `v0.2.0`, and target the release commit on `main`.

Publishing the GitHub Release starts `.github/workflows/release.yml`. It rechecks tag
and version equality, proves the tagged commit belongs to `main`, reruns the complete
quality gate, builds both the wheel and source distribution, validates their metadata,
and uploads them as one immutable workflow artifact. The separate `pypi-publish` job
downloads only those verified files and waits for approval on the `pypi` environment
before publishing them with provenance attestations.

PyPI distributions and version numbers are immutable. If publication fails before any
file is accepted, fix the release workflow and rerun it. If any file was published,
increment the package version; do not enable duplicate-skipping or replace an existing
release.
