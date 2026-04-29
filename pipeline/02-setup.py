#!/usr/bin/env -S uv run --isolated --upgrade --script
# /// script
# requires-python = ">=3.9"
# dependencies = [
#   "juvenal",
# ]
# ///
"""Port a C library to memory-safe Rust."""

from __future__ import annotations

import os
import tempfile
import juvenal.api as juvenal
import sys

LIBNAME = "libzstd" if len(sys.argv) == 1 else sys.argv[1]
DISTRO = "ubuntu 24.04"
WORKDIR = tempfile.mkdtemp() if len(sys.argv) < 3 else sys.argv[2]
BACKEND_KW = {"backend": os.environ["JUVENAL_BACKEND"]} if os.environ.get("JUVENAL_BACKEND") else {}

with juvenal.goal(f"port the {LIBNAME} library from C to Rust", working_dir=WORKDIR, **BACKEND_KW):
    juvenal.do(
        [
            (
                f"analyze the test cases of {LIBNAME} in {WORKDIR}/original, "
                "identify test cases that use private/non-imported API, and "
                "rewrite these test cases to use the public APIs instead, even "
                "if test coverage decreases in doing so. Test coverage must only "
                "decrease when necessary to avoid using private APIs, not "
                "needlessly out of laziness or corner-cutting. Make sure all "
                "tests pass and commit to git."
            ),
            (
                f"analyze the test cases of {LIBNAME} in {WORKDIR}/original, "
                "identify functionality lacking test coverage, and write test "
                "cases covering such functionality using the public APIs. Tests "
                "must not use any non-exported APIs, even if test coverage is "
                "less than optimal as a result, though a best effort must be "
                "made for good test coverage. Make sure all tests pass and "
                "commit to git."
            ),
            (
                f"identify a diverse set of software in the {DISTRO} "
                f"repositories that depends on {LIBNAME} for either compile-time "
                "or runtime use. Document the names and what runtime "
                f"functionality (if any) depends on {LIBNAME} in {WORKDIR}/dependents.json "
                "and commit to git"
            ),
            (
                f"write {WORKDIR}/test-original.sh that uses docker to "
                f"build/install/test, as appropriate, the {LIBNAME}-dependent "
                f"software described in {WORKDIR}/dependents.json. Make sure all "
                "these tests pass and commit to git."
            ),
        ],
        checkers=["tester", "senior-tester"],
    )
