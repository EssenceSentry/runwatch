```{include} ../RELEASE.md
```

## Install a chosen source snapshot

Replace `<full-commit-sha>` with the commit you have selected and validated:

```bash
python -m pip install 'runwatch-notebook[supervisor] @ git+https://github.com/EssenceSentry/runwatch.git@<full-commit-sha>'
```

Use `supervisor,marimo` for native Marimo notebooks. A local checkout can be installed
with `python -m pip install '.[supervisor,marimo]'` from its repository root.

## Prepare a snapshot release

1. Update the code and release notes, refresh `uv.lock` when dependencies change, and
   run `uv run pre-commit run --all-files`.
2. Commit the tested changes and push the selected commit to `main`. There is no
   manual package version to bump.
3. Run `uv sync --locked --extra dev`, then `uv run python scripts/check_release.py`
   to display the commit-derived version. The dynamic root package has no fixed
   version in `uv.lock`, so committing does not require another lockfile update.
4. Create a GitHub Release targeting that commit with the exact tag `v<snapshot
   version>`, for example `v0.0.dev1788274865`. Record the full source commit SHA
   in the release notes and mark the GitHub Release as a prerelease.

Publishing the GitHub Release starts `.github/workflows/release.yml`. Its build job
checks tag/version equality and main-branch ancestry, runs the complete quality gate,
builds wheel and source distributions, and validates them with `twine check --strict`.
The gate also rebuilds an sdist without Git identity and checks the resulting wheel's
version, contents, isolated installations, and notebook execution.

A separate publishing job downloads only those verified distributions and uploads
them to PyPI with attestations through Trusted Publishing. The build job has
read-only repository access. Only the publishing job receives `id-token: write`,
and it uses the protected `pypi` environment. Keep its required reviewers and `v*`
tag restriction configured. No long-lived PyPI API token is stored in the workflow.

The PyPI trusted publisher must match owner `EssenceSentry`, repository `runwatch`,
workflow `release.yml`, and environment `pypi`. Existing publisher configuration for
`runwatch-notebook` continues to apply.

PyPI versions and files are immutable. If publication fails before any file is
accepted, fix the workflow and rerun it. After a partial upload, inspect the accepted
files and publish a new commit/version; do not enable duplicate-skipping or replace
existing files. Verify the exact version on PyPI and install its wheel in an isolated
environment before reporting publication complete.
