#!/usr/bin/env -S uv run --isolated --upgrade --script
# /// script
# requires-python = ">=3.9"
# dependencies = [
#   "juvenal",
# ]
# ///
"""Validate a Rust port against the safelibs validator suite."""

from __future__ import annotations

import os
import tempfile
import juvenal.api as juvenal
import sys

LIBNAME = "libzstd" if len(sys.argv) == 1 else sys.argv[1]
WORKDIR = tempfile.mkdtemp() if len(sys.argv) < 3 else sys.argv[2]
VALIDATOR_REPO = "https://github.com/safelibs/validator"
BACKEND_KW = {"backend": os.environ["JUVENAL_BACKEND"]} if os.environ.get("JUVENAL_BACKEND") else {}

with juvenal.goal(f"validate the {LIBNAME} Rust port against the safelibs validator", working_dir=WORKDIR, **BACKEND_KW):
    juvenal.plan_and_do(
        f"""
        {LIBNAME}, originally implemented as {WORKDIR}/original, has been translated to a memory-safe Rust
        implementation in {WORKDIR}/safe and tested through the prior pipeline phases.

        Run the safelibs validator suite from {VALIDATOR_REPO} against {LIBNAME}-safe:

        - Clone {VALIDATOR_REPO} into {WORKDIR}/validator (or `git pull` to update if it already exists).
        - Follow the validator repository's README for setup and invocation. If the validator expects a
          path to the library under test, point it at {WORKDIR}/safe.
        - Run the full validator suite and capture the results.

        Treat any validator failures as compatibility or safety regressions of {LIBNAME}-safe:

        - For each failure, add a minimal regression test to {WORKDIR}/safe that reproduces the issue,
          fix the underlying problem in {LIBNAME}-safe, and confirm both the regression test and the
          validator pass.
        - Do not modify the validator suite to make failing checks pass; the fix belongs in
          {LIBNAME}-safe unless a validator bug is clearly identified, in which case document the
          finding in {WORKDIR}/validator-report.md and skip just that check with justification.
        - Summarize the run (validator commit, checks executed, failures found, fixes applied) in
          {WORKDIR}/validator-report.md.

        Each implementation phase should commit to git so that the succeeding checkers can reason
        about what was changed. Keep the workflow linear: checkers must only bounce back to the
        previous implementor. Each class of validator failure will likely warrant its own
        implementation phase followed by a checking phase, with a catch-all "fix everything
        remaining" implementation phase before final review. Review these fixes through agentic
        phases of software testers and senior testers, and ensure a clean validator run at the end.
        """
    )
