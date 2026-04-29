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
        (
            f"retrieve and unpack the source package of the {DISTRO} {LIBNAME} package "
            f"into {WORKDIR}/original and commit to git"
        ),
        checker="pm",
    )
    juvenal.do(
        [
            (
                f"retrieve historical and current CVEs affecting {LIBNAME}, "
                f"including full text information, into {WORKDIR}/all_cves.json "
                "and commit that to git"
            ),
            (
                f"analyze CVEs in {WORKDIR}/all_cves.json and identify "
                "non-memory-corruption CVEs that may affect a Rust "
                f"reimplementation. Store the result into {WORKDIR}/relevant_cves.json "
                "and commit that to git"
            ),
        ],
        checkers=["security-engineer"],
    )
