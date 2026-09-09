# Release and snapshot policy

Runwatch is developed as a sequence of commit snapshots. Every commit may contain
breaking changes; `HEAD` is active development and is never a compatibility or
production promise. A release makes an immutable build of a chosen commit available
for consumers to validate and adopt explicitly.

Package versions are generated as
`0.0.dev<commit_timestamp>`, using the commit's Unix timestamp. These development
versions do not express semantic compatibility. Git commits and release metadata
record source identity; artifact digests identify the exact built bytes. Consumers
pin a chosen commit or distribution and validate it against their own requirements.
Runwatch's persisted and wire schema versions remain independently managed contracts.

## Build a local distribution

Run `uv build` from the repository root to create local wheel and source
distributions. This does not publish them. Build distributable snapshots from clean,
committed source: the backend derives identity from `HEAD` and deliberately does not
reject uncommitted modifications. Local test builds can therefore share a version
with different source bytes. Tags, branch names, and dirty-worktree markers do not
change the package version.

`build_backend.py` uses Git only when the package directory is the repository root
(including linked worktrees). Otherwise it reads an expanded `.git_archival.txt`
from `git archive`, then snapshot-version metadata from an sdist's `PKG-INFO`.
An enclosing consumer repository cannot supply the package's identity. Missing or
unusable identity fails the build instead of inventing a version.

The backend sets `SETUPTOOLS_SCM_PRETEND_VERSION_FOR_RUNWATCH_NOTEBOOK` from that
identity. A caller-supplied value is not a separate version-selection interface.
Setuptools and setuptools-scm are build dependencies, not base runtime dependencies.
The installed distribution metadata supplies `runwatch.__version__`, the CLI, and
the documentation version. Run `uv sync` after switching commits to refresh an
editable installation; its cache includes Git commit identity and backend inputs.

## Distribution channel

Snapshot wheels and sdists are published to PyPI through GitHub Trusted Publishing.
The public version omits the Git hash suffix because PyPI does not accept local
version identifiers. The GitHub release targets the exact source commit and records
its full SHA in the release notes. Release only one chosen commit per timestamp;
PyPI rejects attempts to replace an already published version. If two commits share
a timestamp, choose a new commit for the second release rather than reusing the
version or overriding it through an environment variable.

These development versions sort below the historical `0.2.0` release. Install an
explicit snapshot version, for example:

```bash
python -m pip install 'runwatch-notebook[supervisor,marimo]==0.0.dev1788274865'
```

Replace the example with the version from the release you selected. An unqualified
upgrade, even with `--pre`, may select the historical stable version instead. Source
checkouts, Git archives, sdists, and wheels use the same public snapshot version.
