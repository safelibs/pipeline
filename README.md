# SafeLibs Pipeline

This repository contains the original phase-based SafeLibs pipeline extracted from
the larger `safelibs` repo.

## Repository Layout

- `safelibs.py`: runner that executes the phase scripts in `pipeline/`
- `pipeline/`: Juvenal-backed phase scripts (`01-recon.py` through `07-document.py`)
- `tests/test_safelibs_cli.py`: regression tests for resume and tagging behavior
- `ports/`: ignored local checkouts for `safelibs/port-*` repositories

## Requirements

- Python 3.9+
- `git`
- `gh` for `--create-github` and GitHub-backed `status`
- `pytest` to run the test suite
- `rich` for the live multi-job `port --jobs N` dashboard
- `uv` and `juvenal` to execute the scripts under `pipeline/`

## Common Commands

Run the sequential runner:

```bash
python3 safelibs.py port libyaml
python3 safelibs.py port libyaml -L
python3 safelibs.py port libyaml -L /tmp/libyaml.log
python3 safelibs.py port libyaml --from 02-setup
python3 safelibs.py port libyaml --from upgrade
python3 safelibs.py port libyaml --from-last
python3 safelibs.py port libyaml --dry-run
python3 safelibs.py port libyaml --filter-upgradeable
python3 safelibs.py port libyaml --filter-tag 04-test
python3 safelibs.py port libyaml --create-github --github-repo OWNER/libyaml-safe
python3 safelibs.py port libyaml --push-github
python3 safelibs.py port libyaml --github-repo OWNER/libyaml-safe --push-github
python3 safelibs.py port
python3 safelibs.py port --jobs 4
python3 safelibs.py port --jobs 4 --filter-upgradeable
python3 safelibs.py port --filter-upgradeable
python3 safelibs.py status libyaml
python3 safelibs.py status
```

The runner always uses `PORTS_DIR/port-LIBNAME` as the workdir. By default
`PORTS_DIR` is `./ports` next to `safelibs.py`; use `--ports-dir PATH` to
override it. The workdir is treated as a managed checkout for
`https://github.com/safelibs/port-LIBNAME.git`. Managed checkouts are cloned
when the GitHub repo exists, fetched before use, and fast-forward pulled when
the worktree is clean. If the GitHub repo does not exist yet, the runner falls
back to cloning `safelibs/port-template` as the scaffold and rewrites `origin`
to the eventual `port-LIBNAME` URL; if the template clone also fails, it
initializes a fresh local git repo. Use `--no-auto-pull` to skip
clone/fetch/pull, and `--github-owner` or `--github-prefix` for non-default port
repo namespaces.

The runner owns git repository setup. It initializes the workdir when needed,
creates an initial empty commit for fresh repos, tags each completed phase, and
can create or push to a GitHub remote. `--create-github` uses the GitHub CLI to
create `--github-repo`; `--push-github` pushes the current branch and the
library's phase tags to `--github-remote` (`origin` by default). If a managed
checkout has no remote, the runner adds the expected `safelibs/port-LIBNAME`
remote. If `--github-repo` is provided with `--push-github` and the remote is
missing, the runner adds `https://github.com/OWNER/REPO.git`.

Use `--dry-run` with `port` to print the planned checkout sync, workdir setup,
phase execution, tagging, and GitHub create/push actions without running phase
scripts or making git, GitHub, or workdir changes.

`status LIBNAME` reports the local checkout, branch, dirty state, upstream
ahead/behind counts, completed phase tags, next phase, and best-effort versions
from `original/debian/changelog` and `safe/Cargo.toml`. `status` without a
library checks all local `PORTS_DIR/port-*` directories plus repositories
returned by `gh repo list safelibs` whose names start with `port-`. `port`
without a library round-robins the same known ports by running one pending phase
for each port. Add `--jobs N` to run up to `N` known ports at the same time; on
a real TTY the runner shows a `rich`-powered live table with each job's port
name, current state, phase, progress, detail, and latest phase output. In
captured or non-TTY output, concurrent jobs fall back to plain per-library
prefixed log lines instead of the live dashboard. Add `--filter-upgradeable` to
`port` to first compare the upstream portion of
`original/debian/changelog` against the latest available Ubuntu package
metadata and skip ports without a newer upstream version. The check prefers
`apt-cache showsrc` metadata and falls back to binary package metadata when
source metadata is unavailable.

Run an individual phase script directly:

```bash
uv run --isolated --upgrade --script pipeline/01-recon.py libyaml /tmp/safelibs-libyaml
uv run --isolated --upgrade --script pipeline/06-upgrade.py libyaml /tmp/safelibs-libyaml
uv run --isolated --upgrade --script pipeline/07-document.py libyaml /tmp/safelibs-libyaml
```

Direct phase execution assumes the workdir is already a git repo. Prefer
`safelibs.py` for normal runs so initialization, phase tagging, resume, and
GitHub publishing behavior stay consistent.

Run the tests:

```bash
python3 -m pytest
```
