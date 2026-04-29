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
    juvenal.plan_and_do(
        f"""
        Thoroughly analyze the {LIBNAME} code in {WORKDIR}/original to
        develop a plan to port the {LIBNAME} library from C to Rust into
        {WORKDIR}/safe. The library should be:

        - **source-compatible**, so a C program that uses {LIBNAME} should
          be able to compile against {LIBNAME}-safe,
          meaning that all public APIs should remain exported and compatible. All test cases in {WORKDIR}/original
          should continue to pass. Programs in {WORKDIR}/dependents.json (as harnessed in {WORKDIR}/test-original.sh)
          should continue to compile.
        - **link-compatible**, so an object file previously compiled
          against the original {LIBNAME} should be able to
          link against {LIBNAME}-safe, meaning all symbols should be identically exported. Test file objects from
          {WORKDIR}/original should continue to link against {LIBNAME}-safe and run properly.
        - **runtime-compatible**, so a program that relies on the original
          {LIBNAME} should run perfectly when the library is replaced with
          {LIBNAME}-safe. Programs in {WORKDIR}/dependents.json (as harnessed in {WORKDIR}/test-original.sh)
          should continue to function with {LIBNAME}-safe just as they did with the original {LIBNAME}.
        - **reasonably safe**: unsafe Rust is okay as an intermediate step,
          but all code in the final result should be safe
          unless it MUST be unsafe (e.g., to interface with C application code or the OS).
        - **drop-in replaceable**: {LIBNAME}-safe should ship as a package
          for {DISTRO}. {WORKDIR}/test-original.sh and related files should
          be modified to install the {LIBNAME}-safe package and ensure continued functionality of all
          software described in {WORKDIR}/dependents.json.

        Priorities, from most to least (but still) important, are:

        1. perfect compile and runtime interoperability. This is a must-have.
        2. security, both memory safety and resilience against
           previously-identified non-memory vulnerabilities such as
           all those in {WORKDIR}/relevant_cves.json, which must be mitigated in {LIBNAME}-safe.
        3. performance. Good to have, but not at the expense of the other two.

        The library should be contained in {WORKDIR}/safe as a standard Rust package.
        For testing, the test cases in {WORKDIR}/original must be ported over.

        Each implementation phase should commit to git so that the
        succeeding checkers can reason about what was changed.
        Ensure that this workflow is linear: checkers must only bounce
        back to the previous implementor. This means that each major
        testing step (e.g., each class of test cases) will probably
        require its own implementation phase followed by checking phase,
        and will likely require a general "fix everything remaining" sort
        of catch-all implementation phase toward the end. Make sure all
        the test cases for all the above properties are thoroughly
        checked at the end.
        """
    )
