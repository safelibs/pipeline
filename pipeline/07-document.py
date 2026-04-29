#!/usr/bin/env -S uv run --isolated --upgrade --script
# /// script
# requires-python = ">=3.9"
# dependencies = [
#   "juvenal",
# ]
# ///
"""Document a completed Rust port (or refresh existing documentation)."""

from __future__ import annotations

import os
import tempfile
import juvenal.api as juvenal
import sys

LIBNAME = "libzstd" if len(sys.argv) == 1 else sys.argv[1]
DISTRO = "ubuntu 24.04"
WORKDIR = tempfile.mkdtemp() if len(sys.argv) < 3 else sys.argv[2]
BACKEND_KW = {"backend": os.environ["JUVENAL_BACKEND"]} if os.environ.get("JUVENAL_BACKEND") else {}

with juvenal.goal(f"document the {LIBNAME} Rust port", working_dir=WORKDIR, **BACKEND_KW):
    juvenal.plan_and_do(
        f"""
        {LIBNAME}, originally implemented as {WORKDIR}/original, has been translated to a memory-safe Rust
        implementation in {WORKDIR}/safe and shipped as a drop-in {DISTRO} replacement package. Produce or
        refresh authoritative documentation of the port at {WORKDIR}/safe/PORT.md and commit it.

        If {WORKDIR}/safe/PORT.md already exists, update it in place rather than starting over: preserve any
        still-accurate prose, reconcile each section against the current state of {WORKDIR}/safe, and clearly
        revise anything that has drifted. If it does not exist, create it.

        The document must be self-contained, accurate as of the current commit, and grounded in the actual
        contents of {WORKDIR}/safe (and {WORKDIR}/original where useful for comparison). Do not invent or
        hand-wave: every claim about unsafe code, FFI, dependencies, or remaining issues must be traceable to
        a file and, where helpful, a line or symbol. Cross-check claims by reading the code and running
        `grep`, `cargo metadata`, `cargo tree`, `cargo geiger` (if available), `nm`/`objdump` on built
        artifacts, and the project's own test/build harnesses as needed.

        The report must include the following sections, in this order:

        1. **High-level architecture.** A concise overview of what the Rust port looks like: crate layout
           (workspace members, binaries, libraries), module structure, the boundary between the public
           C-compatible ABI/API surface and the internal Rust implementation, how data flows through the
           library, and how the build/packaging is wired up (Cargo features, build.rs, cbindgen/staticlib/
           cdylib settings, Debian packaging glue). Include a short directory map.

        2. **Where the unsafe Rust lives.** Enumerate every `unsafe` block, `unsafe fn`, `unsafe impl`, and
           `unsafe extern` in {WORKDIR}/safe with file:line references and a one-sentence justification for
           each. Group them by purpose (e.g. ABI shims, raw-pointer manipulation required by the public C
           API, intrinsics, allocator integration). Note any unsafe code that is *not* required by the C
           ABI/API boundary and call it out separately.

        3. **Remaining unsafe FFI beyond the original ABI/API boundary.** List every FFI surface other than
           the intended {LIBNAME} C ABI/API boundary the port must expose: extern calls into libc, OS
           syscalls, other system libraries, third-party C/C++ dependencies, dynamically loaded plugins,
           etc. For each, record the symbol(s) used, the crate or system library that provides them, why
           they are needed, and what (if anything) could plausibly replace them with safe Rust later. If
           there is no such FFI beyond the original boundary, state that explicitly and show the evidence
           (e.g. `grep -RIn 'extern "C"' {WORKDIR}/safe`, `cargo tree` showing only safe deps).

        4. **Remaining issues.** A candid list of known limitations: failing or skipped tests, todo/fixme
           markers, performance regressions vs. the original, behaviors that are not yet bit-for-bit
           equivalent, packaging caveats, dependents from {WORKDIR}/dependents.json that are not yet covered
           or have caveats, and any CVE classes from {WORKDIR}/relevant_cves.json that the port does not
           fully mitigate. Include pointers to the relevant tests, issues, or report files
           (e.g. {WORKDIR}/upgrade-report.md if present).

        5. **Dependencies and other libraries used.** The full direct dependency list from
           {WORKDIR}/safe/Cargo.toml with versions and a one-line purpose for each. Call out any C/C++
           system libraries linked at build or runtime, build-time tools (cbindgen, bindgen, pkg-config,
           etc.), and Debian packaging dependencies. Note any `unsafe`-heavy or non-`#![forbid(unsafe_code)]`
           dependencies and why they are acceptable.

        6. **How this document was produced.** A short reproducibility note listing the commands and files
           consulted so a future maintainer can refresh the document the same way.

        After writing the document, sanity-check it: every file path mentioned must exist, every cited
        symbol must be findable with `grep`, every dependency must appear in Cargo.toml, and the unsafe
        inventory must match `grep -RIn '\\bunsafe\\b' {WORKDIR}/safe` (modulo strings/comments). Fix any
        discrepancies before committing.

        Commit the final {WORKDIR}/safe/PORT.md (and any incidental fixes made while reconciling the
        document with the code) in a single commit whose message summarizes the documentation pass.
        """
    )
