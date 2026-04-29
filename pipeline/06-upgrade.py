#!/usr/bin/env -S uv run --isolated --upgrade --script
# /// script
# requires-python = ">=3.9"
# dependencies = [
#   "juvenal",
# ]
# ///
"""Upgrade a completed Rust port to the latest Ubuntu source package."""

from __future__ import annotations

from pathlib import Path
import os
import tempfile
import juvenal.api as juvenal
import sys

LIBNAME = "libzstd" if len(sys.argv) == 1 else sys.argv[1]
DISTRO = "ubuntu 24.04"
WORKDIR = tempfile.mkdtemp() if len(sys.argv) < 3 else sys.argv[2]
BACKEND_KW = {"backend": os.environ["JUVENAL_BACKEND"]} if os.environ.get("JUVENAL_BACKEND") else {}
REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import safelibs


def _maybe_skip_upgrade() -> bool:
    status = safelibs._upgradeability_status(WORKDIR, LIBNAME)
    if status["reason"] != "new-upstream-version" or status["upgradeable"]:
        return False

    current = safelibs._version_description(
        status["current_upstream"],
        status["current_source"],
    )
    latest = safelibs._version_description(
        status["latest_upstream"],
        status["latest_source"],
    )
    print(
        f"Skipping upgrade for {LIBNAME}; current upstream version is already latest "
        f"available Ubuntu package version (current {current}, latest {latest}).",
        flush=True,
    )
    raise SystemExit(safelibs.PHASE_SKIPPED_EXIT_CODE)


if not _maybe_skip_upgrade():
    with juvenal.goal(f"upgrade the {LIBNAME} Rust port to the latest Ubuntu version", working_dir=WORKDIR, **BACKEND_KW):
        juvenal.plan_and_do(
            f"""
            {LIBNAME}, originally implemented as {WORKDIR}/original, has been translated to a memory-safe Rust
            implementation in {WORKDIR}/safe. Upgrade this completed port to track the latest {DISTRO}
            source package for {LIBNAME}.

            Overwrite {WORKDIR}/original with the latest available {DISTRO} source package for {LIBNAME} and
            commit it. Identify and document every meaningful difference in the new version in
            {WORKDIR}/upgrade-report.md, and commit the report.

            The report must include:

            - baseline and latest Ubuntu package versions.
            - findings from debian/changelog, debian/patches, upstream changelogs, NEWS files, release notes,
              package metadata, and other relevant Debian or Ubuntu source-package documentation.
            - public C API, header, symbol, ABI, pkg-config, build-system, and packaging changes.
            - behavior changes, bug fixes, security fixes, and CVE-relevant changes.
            - added, removed, or changed upstream tests and test data.
            - compatibility risks for software listed in {WORKDIR}/dependents.json and harnessed by
              {WORKDIR}/test-original.sh.

            Then do the following:

            1. Port all new or changed upstream tests from the upgraded {WORKDIR}/original tree into the Rust-port
               test suite, adapting only where necessary to use public APIs.
            2. Update {WORKDIR}/safe so it implements the latest source package behavior while preserving source,
               link, runtime, and package-upgrade compatibility as appropriate and adapting to the changes in the
               newer version.
            3. Add regression tests for behavior, ABI, packaging, and dependent-application changes discovered during
               the diff.
            4. Update package metadata and any install/upgrade harnesses so an installed older {LIBNAME}-safe
               package can upgrade cleanly to the new one. The version of the replacement packages should be the same
               as the upgraded original version.

            Validate the result thoroughly:

            - run the existing tests against {WORKDIR}/safe.
            - run the newly ported latest-version tests against {WORKDIR}/safe.
            - verify source compatibility by compiling latest-version public-API consumers against {LIBNAME}-safe.
            - verify link compatibility by checking exported symbols and linking representative objects.
            - verify runtime compatibility with the dependent applications in {WORKDIR}/dependents.json.
            - verify package install and upgrade behavior on {DISTRO}.

            Each implementation and test-porting phase should commit to git so that succeeding checkers can reason
            about what changed. End with a final commit that contains any remaining fixes, the updated report, and a
            concise summary of the validated upgrade path.
            """
        )
