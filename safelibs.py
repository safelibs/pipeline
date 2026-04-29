#!/usr/bin/env python3
"""Run the safelibs pipeline scripts in order on a shared temp directory.

Usage:
    safelibs.py status [libname ...]
    safelibs.py sync [libname ...]
    safelibs.py port [libname ...]
    safelibs.py port --jobs 4
    safelibs.py port [libname ...] --filter-upgradeable
    safelibs.py port [libname ...] -L [logfile]
    safelibs.py port [libname ...] --from 01-recon
    safelibs.py port [libname ...] --from 02-setup
    safelibs.py port [libname ...] --from-last
    safelibs.py port [libname ...] --dry-run
    safelibs.py port libname --create-github --github-repo OWNER/REPO
    safelibs.py port [libname ...] --push-github
    safelibs.py port [libname ...] --claude
    safelibs.py port [libname ...] --backend claude

Every action that touches a port checkout will fetch, fast-forward pull,
and push local commits/tags to the GitHub remote so the local working
copies stay in lockstep with upstream. Use --no-auto-pull to opt out.
"""

import argparse
import codecs
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from contextlib import contextmanager
import glob
import json
import os
import re
import selectors
import shlex
import subprocess
import sys
import threading
import time
import urllib.parse
import urllib.request

try:
    from rich.console import Console
    from rich.live import Live
    from rich.table import Table
    from rich.text import Text
except ImportError:  # pragma: no cover - the CLI falls back to plain output.
    Console = None
    Live = None
    Table = None
    Text = None

REPO_ROOT = os.path.dirname(os.path.abspath(__file__))
PIPELINE_DIR = os.path.join(REPO_ROOT, "pipeline")
DEFAULT_PORTS_DIR = os.path.join(REPO_ROOT, "ports")
DEFAULT_GITHUB_OWNER = "safelibs"
DEFAULT_PORT_REPO_PREFIX = "port-"
DEFAULT_LOG_DIR = "/tmp"
_USE_DEFAULT_LOG_PATH = object()
DEFAULT_GIT_USER_NAME = "SafeLibs Pipeline"
DEFAULT_GIT_USER_EMAIL = "safelibs@example.invalid"
PHASE_SKIPPED_EXIT_CODE = 80
JUVENAL_BACKEND_ENV = "JUVENAL_BACKEND"
JUVENAL_BACKEND_CHOICES = ("codex", "claude")
_REPORTER_STATE = threading.local()


def _port_repo_name(libname, prefix=DEFAULT_PORT_REPO_PREFIX):
    if libname.startswith(prefix):
        return libname
    return f"{prefix}{libname}"


def _libname_from_port_repo(repo_name, prefix=DEFAULT_PORT_REPO_PREFIX):
    if repo_name.startswith(prefix):
        return repo_name[len(prefix):]
    return repo_name


def _default_port_workdir(libname, ports_dir, prefix=DEFAULT_PORT_REPO_PREFIX):
    return os.path.abspath(os.path.join(ports_dir, _port_repo_name(libname, prefix)))


def _github_repo_slug(libname, owner, prefix, github_repo=None):
    if github_repo:
        return github_repo
    return f"{owner}/{_port_repo_name(libname, prefix)}"


def _github_remote_url(repo):
    if (
        repo.startswith(("http://", "https://", "ssh://", "git@", "file://"))
        or os.path.isabs(repo)
    ):
        return repo
    return f"https://github.com/{repo}.git"


def _current_reporter():
    return getattr(_REPORTER_STATE, "current", None)


@contextmanager
def _reporter_context(reporter):
    previous = _current_reporter()
    _REPORTER_STATE.current = reporter
    try:
        yield
    finally:
        _REPORTER_STATE.current = previous


def _trim_status_text(text, limit=80):
    if text is None:
        return ""
    compact = " ".join(str(text).split())
    if len(compact) <= limit:
        return compact
    return f"{compact[:limit - 3]}..."


def _format_elapsed(seconds):
    total = max(0, int(seconds))
    minutes, seconds = divmod(total, 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours}:{minutes:02d}:{seconds:02d}"
    return f"{minutes}:{seconds:02d}"


def _status_style(status):
    return {
        "queued": "dim",
        "starting": "cyan",
        "syncing": "cyan",
        "filtering": "magenta",
        "preparing": "blue",
        "planning": "blue",
        "running": "yellow",
        "tagging": "yellow",
        "pushing": "yellow",
        "complete": "green",
        "planned": "green",
        "skipped": "bright_black",
        "failed": "bold red",
    }.get(status, "white")


class _ConcurrentPortRunController:
    def __init__(self, *, jobs, log_handle=None, enable_tui=False):
        self.jobs = jobs
        self.log_handle = log_handle
        self.enable_tui = (
            enable_tui
            and Console is not None
            and Live is not None
            and Table is not None
            and Text is not None
            and hasattr(sys.stdout, "isatty")
            and sys.stdout.isatty()
        )
        self.console = Console() if self.enable_tui else None
        self.output_lock = threading.Lock()
        self.state_lock = threading.Lock()
        self.states = {}

    def register_job(self, libname, repo_name):
        now = time.time()
        with self.state_lock:
            self.states[libname] = {
                "libname": libname,
                "repo_name": repo_name,
                "status": "queued",
                "phase": "-",
                "progress": "-",
                "detail": "Queued",
                "last_output": "",
                "started_at": now,
                "updated_at": now,
            }

    def state_for(self, libname):
        with self.state_lock:
            state = self.states.get(libname)
            if state is None:
                return None
            return dict(state)

    def set_status(self, libname, status=None, *, phase=None, progress=None, detail=None):
        with self.state_lock:
            state = self.states[libname]
            if status is not None:
                state["status"] = status
            if phase is not None:
                state["phase"] = phase or "-"
            if progress is not None:
                state["progress"] = progress or "-"
            if detail is not None:
                state["detail"] = _trim_status_text(detail)
            state["updated_at"] = time.time()

    def note_output(self, libname, text):
        last_line = ""
        for line in str(text).splitlines():
            stripped = line.strip()
            if stripped:
                last_line = stripped
        if not last_line and str(text).strip():
            last_line = str(text).strip()
        if not last_line:
            return
        with self.state_lock:
            state = self.states[libname]
            state["last_output"] = _trim_status_text(last_line, limit=96)
            state["updated_at"] = time.time()

    def write_prefixed_text(self, libname, text, *, stream, terminal=True, log_handle=None):
        prefix = f"[{libname}] "
        target_log = self.log_handle if log_handle is None else log_handle
        with self.output_lock:
            if terminal and not self.enable_tui:
                stream.write(prefix + text)
                stream.flush()
            if target_log is not None:
                target_log.write(prefix + text)
                target_log.flush()

    def render(self):
        table = Table(
            title=f"SafeLibs Port Jobs ({self.jobs} parallel)",
            expand=True,
        )
        table.add_column("Port", no_wrap=True)
        table.add_column("State", no_wrap=True)
        table.add_column("Phase", no_wrap=True)
        table.add_column("Progress", no_wrap=True, justify="right")
        table.add_column("Detail", overflow="fold")
        table.add_column("Last Output", overflow="fold")
        table.add_column("Elapsed", no_wrap=True, justify="right")

        with self.state_lock:
            rows = [dict(self.states[key]) for key in sorted(self.states)]

        now = time.time()
        for row in rows:
            state_text = Text(row["status"], style=_status_style(row["status"]))
            table.add_row(
                row["repo_name"],
                state_text,
                row["phase"] or "-",
                row["progress"] or "-",
                row["detail"],
                row["last_output"],
                _format_elapsed(now - row["started_at"]),
            )
        return table

    def refresh(self, live):
        if self.enable_tui:
            live.update(self.render())


class _ConcurrentPortReporter:
    def __init__(self, controller, libname):
        self.controller = controller
        self.libname = libname
        self.buffers = {
            "stdout": "",
            "stderr": "",
        }

    def set_status(self, status=None, *, phase=None, progress=None, detail=None):
        self.controller.set_status(
            self.libname,
            status,
            phase=phase,
            progress=progress,
            detail=detail,
        )

    def emit(self, message, *, stream, log_handle=None):
        self.controller.set_status(self.libname, detail=message)
        for part in f"{message}\n".splitlines(keepends=True):
            self.controller.write_prefixed_text(
                self.libname,
                part,
                stream=stream,
                terminal=not self.controller.enable_tui,
                log_handle=log_handle,
            )

    def write_stream_text(self, text, *, stream, log_handle=None, final=False):
        channel = "stderr" if stream is sys.stderr else "stdout"
        if text:
            self.controller.note_output(self.libname, text)
        buffer = self.buffers[channel] + text
        self.buffers[channel] = ""
        for part in buffer.splitlines(keepends=True):
            if part.endswith(("\n", "\r")):
                self.controller.write_prefixed_text(
                    self.libname,
                    part,
                    stream=stream,
                    terminal=not self.controller.enable_tui,
                    log_handle=log_handle,
                )
                continue
            self.buffers[channel] = part

        if final and self.buffers[channel]:
            self.controller.write_prefixed_text(
                self.libname,
                self.buffers[channel],
                stream=stream,
                terminal=not self.controller.enable_tui,
                log_handle=log_handle,
            )
            self.buffers[channel] = ""


def _is_git_worktree(workdir):
    if not os.path.isdir(workdir):
        return False
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            cwd=workdir,
            capture_output=True,
            text=True,
        )
    except FileNotFoundError:
        return False
    if result.returncode != 0:
        return False

    git_root = result.stdout.strip()
    if not git_root:
        return False
    return os.path.realpath(git_root) == os.path.realpath(workdir)


def _run_optional_command(command, *, cwd, log_handle=None):
    try:
        return subprocess.run(
            command,
            cwd=cwd,
            capture_output=True,
            text=True,
        )
    except FileNotFoundError as exc:
        _emit(
            f"Failed to run {' '.join(command)}: {exc}",
            log_handle=log_handle,
            stream=sys.stderr,
        )
        return None


def _git_output(workdir, *args):
    try:
        result = subprocess.run(
            ["git", *args],
            cwd=workdir,
            capture_output=True,
            text=True,
        )
    except FileNotFoundError:
        return None
    if result.returncode != 0:
        return None
    return result.stdout.strip()


def _workdir_is_dirty(workdir):
    status = _git_output(workdir, "status", "--porcelain")
    return status is None or bool(status)


def _ensure_remote_if_missing(workdir, remote, remote_url, log_handle=None):
    if _remote_url(workdir, remote, log_handle=log_handle) is not None:
        return

    _emit(f"Adding git remote {remote}: {remote_url}", log_handle=log_handle)
    _run_checked_command(
        ["git", "remote", "add", remote, remote_url],
        cwd=workdir,
        error_message=f"Failed to add git remote {remote}",
        log_handle=log_handle,
    )


def _auto_pull_workdir(workdir, remote, log_handle=None):
    if not _workdir_has_git_history(workdir, log_handle=log_handle):
        return "no-history"
    if _workdir_is_dirty(workdir):
        _emit(
            f"Skipping auto-pull in {workdir}; the worktree has local changes.",
            log_handle=log_handle,
        )
        return "dirty"

    _emit(f"Fetching {remote} in {workdir}", log_handle=log_handle)
    fetch_result = _run_optional_command(
        ["git", "fetch", "--prune", remote],
        cwd=workdir,
        log_handle=log_handle,
    )
    if fetch_result is None:
        return "fetch-failed"
    if fetch_result.returncode != 0:
        detail = fetch_result.stderr.strip() or fetch_result.stdout.strip()
        suffix = f": {detail}" if detail else ""
        _emit(
            f"Skipping auto-pull in {workdir}; fetch from {remote} failed{suffix}",
            log_handle=log_handle,
            stream=sys.stderr,
        )
        return "fetch-failed"

    branch = _git_output(workdir, "branch", "--show-current")
    if not branch:
        _emit(
            f"Skipping auto-pull in {workdir}; HEAD is detached.",
            log_handle=log_handle,
        )
        return "detached"

    remote_ref = f"refs/remotes/{remote}/{branch}"
    remote_branch = _git_output(workdir, "rev-parse", "--verify", "--quiet", remote_ref)
    if remote_branch is None:
        _emit(
            f"Skipping auto-pull in {workdir}; {remote}/{branch} does not exist.",
            log_handle=log_handle,
        )
        return "no-remote-branch"

    _emit(f"Auto-pulling {remote}/{branch} in {workdir}", log_handle=log_handle)
    _run_checked_command(
        ["git", "pull", "--ff-only", remote, branch],
        cwd=workdir,
        error_message=f"Failed to fast-forward {workdir} from {remote}/{branch}",
        log_handle=log_handle,
        echo_output=True,
    )
    return "pulled"


def _auto_push_workdir(workdir, remote, libname=None, log_handle=None):
    """Push the current branch and local tags to ``remote`` if anything is ahead.

    Soft-fails (logs to stderr) on failure rather than aborting the run, so
    routine syncs do not turn transient network or auth issues into pipeline
    failures.
    """
    if not _workdir_has_git_history(workdir, log_handle=log_handle):
        return "no-history"

    if _remote_url(workdir, remote, log_handle=log_handle) is None:
        _emit(
            f"Skipping auto-push in {workdir}; remote {remote} is not configured.",
            log_handle=log_handle,
        )
        return "no-remote"

    branch = _git_output(workdir, "branch", "--show-current")
    if not branch:
        _emit(
            f"Skipping auto-push in {workdir}; HEAD is detached.",
            log_handle=log_handle,
        )
        return "detached"

    upstream, ahead, behind = _git_ahead_behind(workdir)
    if upstream is not None and ahead and behind and ahead > 0 and behind > 0:
        _emit(
            f"Skipping auto-push in {workdir}; branch {branch} has diverged "
            f"from {upstream} (ahead {ahead}, behind {behind}). "
            "Reconcile manually before pushing.",
            log_handle=log_handle,
            stream=sys.stderr,
        )
        return "diverged"

    pushed_anything = False

    needs_branch_push = upstream is None or (ahead and ahead > 0)
    if needs_branch_push:
        if upstream is None:
            _emit(
                f"Auto-pushing {branch} in {workdir} (setting upstream {remote}/{branch})",
                log_handle=log_handle,
            )
            push_command = ["git", "push", "-u", remote, f"{branch}:{branch}"]
        else:
            _emit(
                f"Auto-pushing {ahead} commit(s) on {branch} in {workdir}",
                log_handle=log_handle,
            )
            push_command = ["git", "push", remote, f"{branch}:{branch}"]
        push_result = _run_optional_command(
            push_command,
            cwd=workdir,
            log_handle=log_handle,
        )
        if push_result is None or push_result.returncode != 0:
            detail = ""
            if push_result is not None:
                detail = push_result.stderr.strip() or push_result.stdout.strip()
            suffix = f": {detail}" if detail else ""
            _emit(
                f"Auto-push of {branch} failed in {workdir}{suffix}",
                log_handle=log_handle,
                stream=sys.stderr,
            )
            return "push-failed"
        pushed_anything = True

    tag_filter = f"{libname}/*" if libname else None
    tag_text = (
        _git_output(workdir, "tag", "--list", tag_filter)
        if tag_filter
        else _git_output(workdir, "tag", "--list")
    )
    local_tags = [t for t in (tag_text or "").splitlines() if t]
    if local_tags:
        tag_result = _run_optional_command(
            [
                "git",
                "push",
                remote,
                *[f"refs/tags/{t}:refs/tags/{t}" for t in local_tags],
            ],
            cwd=workdir,
            log_handle=log_handle,
        )
        if tag_result is None or tag_result.returncode != 0:
            detail = ""
            if tag_result is not None:
                detail = tag_result.stderr.strip() or tag_result.stdout.strip()
            suffix = f": {detail}" if detail else ""
            _emit(
                f"Auto-push of tags failed in {workdir}{suffix}",
                log_handle=log_handle,
                stream=sys.stderr,
            )
        else:
            stdout = (tag_result.stdout + tag_result.stderr).strip()
            if stdout and ("new tag" in stdout or "->" in stdout):
                pushed_anything = True

    return "pushed" if pushed_anything else "up-to-date"


def _clone_port_repo(remote_url, workdir, log_handle=None):
    parent_dir = os.path.dirname(workdir)
    os.makedirs(parent_dir, exist_ok=True)
    _emit(f"Cloning {remote_url} into {workdir}", log_handle=log_handle)
    result = _run_optional_command(
        ["git", "clone", remote_url, workdir],
        cwd=parent_dir,
        log_handle=log_handle,
    )
    if result is not None and result.returncode == 0:
        return True

    detail = ""
    if result is not None:
        detail = result.stderr.strip() or result.stdout.strip()
    suffix = f": {detail}" if detail else ""
    _emit(
        f"No usable GitHub checkout found at {remote_url}{suffix}",
        log_handle=log_handle,
        stream=sys.stderr,
    )
    return False


def _sync_port_workdir(
    workdir,
    repo_slug,
    remote,
    libname=None,
    log_handle=None,
    dry_run=False,
):
    remote_url = _github_remote_url(repo_slug)
    if _is_git_worktree(workdir):
        if dry_run:
            _emit_dry_run(
                f"would ensure git remote {remote}: {remote_url}",
                log_handle=log_handle,
            )
            _emit_dry_run(
                f"would fetch and fast-forward pull {remote} in {workdir} when clean",
                log_handle=log_handle,
            )
            tag_scope = f"{libname}/*" if libname else "all"
            _emit_dry_run(
                f"would push local commits and {tag_scope} tags to {remote} in {workdir}",
                log_handle=log_handle,
            )
            return "dry-run"
        _ensure_remote_if_missing(workdir, remote, remote_url, log_handle=log_handle)
        pull_status = _auto_pull_workdir(workdir, remote, log_handle=log_handle)
        _auto_push_workdir(
            workdir,
            remote,
            libname=libname,
            log_handle=log_handle,
        )
        return pull_status

    if os.path.exists(workdir) and os.listdir(workdir):
        _emit(
            f"Skipping GitHub checkout for non-git directory {workdir}",
            log_handle=log_handle,
        )
        return "non-git"

    if dry_run:
        _emit_dry_run(
            f"would clone {remote_url} into {workdir} if available",
            log_handle=log_handle,
        )
        return "dry-run"

    if _clone_port_repo(remote_url, workdir, log_handle=log_handle):
        return "cloned"
    return "missing"


def _safe_read_text(path):
    try:
        with open(path, encoding="utf-8") as handle:
            return handle.read()
    except OSError:
        return None


def _debian_changelog_version(workdir):
    text = _safe_read_text(os.path.join(workdir, "original", "debian", "changelog"))
    if text is None:
        return None
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        match = re.match(r"^[^(]+\(([^)]+)\)", line)
        if match:
            return match.group(1)
        return None
    return None


def _debian_changelog_source_metadata(path):
    text = _safe_read_text(path)
    if text is None:
        return None

    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        match = re.match(r"^([^(]+?)\s*\(([^)]+)\)", line)
        if match:
            return match.group(1).strip(), match.group(2).strip()
        return None
    return None


def _dsc_source_metadata(path):
    text = _safe_read_text(path)
    if text is None:
        return None

    source = None
    version = None
    for line in text.splitlines():
        if source is None:
            match = re.match(r"^Source:\s*(\S+)\s*$", line)
            if match:
                source = match.group(1)
                if version is not None:
                    return source, version
                continue

        if version is None:
            match = re.match(r"^Version:\s*(\S+)\s*$", line)
            if match:
                version = match.group(1)
                if source is not None:
                    return source, version

    return None


def _latest_dsc_source_metadata(workdir):
    latest = None
    pattern = os.path.join(workdir, "original", "*.dsc")
    for path in glob.glob(pattern):
        metadata = _dsc_source_metadata(path)
        if metadata is None:
            continue
        if latest is None or _debian_version_greater(metadata[1], latest[1]):
            latest = metadata
    return latest


def _current_source_metadata(workdir):
    metadata = _debian_changelog_source_metadata(
        os.path.join(workdir, "original", "debian", "changelog")
    )
    if metadata is not None:
        return metadata

    metadata = _latest_dsc_source_metadata(workdir)
    if metadata is not None:
        return metadata

    return _debian_changelog_source_metadata(
        os.path.join(workdir, "safe", "debian", "changelog")
    )


def _cargo_package_version(workdir):
    text = _safe_read_text(os.path.join(workdir, "safe", "Cargo.toml"))
    if text is None:
        return None

    in_package = False
    for raw_line in text.splitlines():
        line = raw_line.split("#", 1)[0].strip()
        if not line:
            continue
        if line == "[package]":
            in_package = True
            continue
        if line.startswith("[") and line.endswith("]"):
            in_package = False
            continue
        if not in_package:
            continue
        match = re.match(r'version\s*=\s*["\']([^"\']+)["\']', line)
        if match:
            return match.group(1)
    return None


def _source_upstream_version(version):
    if version is None:
        return None

    upstream = version.strip()
    if not upstream:
        return None
    if ":" in upstream:
        upstream = upstream.split(":", 1)[1]
    if "-" in upstream:
        upstream = upstream.rsplit("-", 1)[0]
    upstream = re.sub(r"\+safelibs\d+$", "", upstream)
    upstream = re.sub(r"\+safe\d+$", "", upstream)
    return upstream or None


def _fallback_version_key(version):
    parts = []
    for part in re.split(r"(\d+)", version):
        if not part:
            continue
        if part.isdigit():
            parts.append((0, int(part)))
        else:
            parts.append((1, part))
    return parts


def _debian_version_greater(candidate, baseline):
    try:
        result = subprocess.run(
            ["dpkg", "--compare-versions", candidate, "gt", baseline],
            capture_output=True,
            text=True,
        )
    except FileNotFoundError:
        result = None

    if result is not None and result.returncode in (0, 1):
        return result.returncode == 0

    return _fallback_version_key(candidate) > _fallback_version_key(baseline)


def _latest_version(versions):
    latest = None
    for version in versions:
        if latest is None or _debian_version_greater(version, latest):
            latest = version
    return latest


def _apt_cache_version_fields(output):
    versions = []
    for line in output.splitlines():
        match = re.match(r"^Version:\s*(\S+)\s*$", line)
        if match:
            versions.append(match.group(1))
    return versions


def _apt_cache_policy_versions(output):
    versions = []
    for line in output.splitlines():
        candidate = re.match(r"^\s*Candidate:\s*(\S+)\s*$", line)
        if candidate and candidate.group(1) != "(none)":
            versions.append(candidate.group(1))
            continue

        version_row = re.match(r"^\s*(?:\*\*\*\s*)?([0-9][^\s]*)\s+\d+", line)
        if version_row:
            versions.append(version_row.group(1))
    return versions


def _apt_cache_madison_versions(output, package_name):
    versions = []
    for line in output.splitlines():
        fields = [field.strip() for field in line.split("|")]
        if len(fields) < 2:
            continue
        if fields[0] != package_name:
            continue
        if fields[1]:
            versions.append(fields[1])
    return versions


def _ubuntu_series_codename(log_handle=None):
    text = _safe_read_text("/etc/os-release")
    if text is not None:
        values = {}
        for line in text.splitlines():
            if "=" not in line:
                continue
            key, value = line.split("=", 1)
            values[key] = value.strip().strip('"')
        for key in ("VERSION_CODENAME", "UBUNTU_CODENAME"):
            if values.get(key):
                return values[key]

    result = _run_optional_command(["lsb_release", "-sc"], cwd=REPO_ROOT, log_handle=log_handle)
    if result is not None and result.returncode == 0:
        codename = result.stdout.strip()
        if codename:
            return codename
    return None


def _launchpad_source_versions(source_package, log_handle=None):
    series = _ubuntu_series_codename(log_handle=log_handle)
    if series is None:
        return []

    query = urllib.parse.urlencode(
        {
            "ws.op": "getPublishedSources",
            "source_name": source_package,
            "exact_match": "true",
            "distro_series": f"/ubuntu/{series}",
        }
    )
    url = f"https://api.launchpad.net/1.0/ubuntu/+archive/primary?{query}"

    try:
        with urllib.request.urlopen(url, timeout=15) as response:
            payload = json.load(response)
    except Exception:
        return []

    versions = []
    for entry in payload.get("entries", []):
        if entry.get("status") != "Published":
            continue
        version = entry.get("source_package_version")
        if isinstance(version, str) and version:
            versions.append(version)
    return versions


def _latest_ubuntu_package_version(libname, log_handle=None, source_package=None):
    package_names = []
    for package_name in (source_package, libname):
        if package_name and package_name not in package_names:
            package_names.append(package_name)

    for package_name in package_names:
        commands = [
            (
                ["apt-cache", "showsrc", package_name],
                lambda output, current_name=package_name: _apt_cache_version_fields(output),
            ),
            (
                ["apt-cache", "madison", package_name],
                lambda output, current_name=package_name: _apt_cache_madison_versions(
                    output,
                    current_name,
                ),
            ),
            (
                ["apt-cache", "show", package_name],
                lambda output, current_name=package_name: _apt_cache_version_fields(output),
            ),
            (
                ["apt-cache", "policy", package_name],
                lambda output, current_name=package_name: _apt_cache_policy_versions(output),
            ),
        ]

        for command, parser in commands:
            result = _run_optional_command(command, cwd=REPO_ROOT, log_handle=log_handle)
            if result is None or result.returncode != 0:
                continue

            latest = _latest_version(parser(result.stdout))
            if latest is not None:
                return latest

        latest = _latest_version(_launchpad_source_versions(package_name, log_handle=log_handle))
        if latest is not None:
            return latest

    return None


def _resolve_latest_ubuntu_package_version(libname, log_handle=None, source_package=None):
    if source_package is None:
        return _latest_ubuntu_package_version(libname, log_handle=log_handle)

    try:
        return _latest_ubuntu_package_version(
            libname,
            log_handle=log_handle,
            source_package=source_package,
        )
    except TypeError as exc:
        if "source_package" not in str(exc):
            raise
        return _latest_ubuntu_package_version(libname, log_handle=log_handle)


def _upgradeability_status(workdir, libname, log_handle=None):
    current_metadata = _current_source_metadata(workdir)
    current_source_package = None
    current_source = None
    if current_metadata is not None:
        current_source_package, current_source = current_metadata
    current_upstream = _source_upstream_version(current_source)
    latest_source = _resolve_latest_ubuntu_package_version(
        libname,
        log_handle=log_handle,
        source_package=current_source_package,
    )
    latest_upstream = _source_upstream_version(latest_source)

    if current_source is None:
        return {
            "upgradeable": False,
            "reason": "missing-current-version",
            "current_source_package": current_source_package,
            "current_source": None,
            "current_upstream": None,
            "latest_source": latest_source,
            "latest_upstream": latest_upstream,
        }

    if latest_source is None:
        return {
            "upgradeable": False,
            "reason": "missing-latest-version",
            "current_source_package": current_source_package,
            "current_source": current_source,
            "current_upstream": current_upstream,
            "latest_source": None,
            "latest_upstream": None,
        }

    if current_upstream is None or latest_upstream is None:
        return {
            "upgradeable": False,
            "reason": "invalid-version",
            "current_source_package": current_source_package,
            "current_source": current_source,
            "current_upstream": current_upstream,
            "latest_source": latest_source,
            "latest_upstream": latest_upstream,
        }

    return {
        "upgradeable": _debian_version_greater(latest_upstream, current_upstream),
        "reason": "new-upstream-version",
        "current_source_package": current_source_package,
        "current_source": current_source,
        "current_upstream": current_upstream,
        "latest_source": latest_source,
        "latest_upstream": latest_upstream,
    }


def _version_description(upstream, source):
    if upstream is None:
        return "unknown"
    if source and source != upstream:
        return f"{upstream} (source package {source})"
    return upstream


def _filter_upgradeable_allows_port(
    workdir,
    libname,
    log_handle=None,
    *,
    quiet_unchanged=False,
):
    status = _upgradeability_status(workdir, libname, log_handle=log_handle)
    if status["upgradeable"]:
        current = _version_description(
            status["current_upstream"],
            status["current_source"],
        )
        latest = _version_description(
            status["latest_upstream"],
            status["latest_source"],
        )
        _emit(
            f"{libname}: new upstream version available: {current} -> {latest}",
            log_handle=log_handle,
        )
        return True

    reason = status["reason"]
    if reason == "missing-current-version":
        _emit(
            f"Skipping {libname}; no current version found in original/debian/changelog.",
            log_handle=log_handle,
        )
        return False
    if reason == "missing-latest-version":
        _emit(
            f"Skipping {libname}; could not determine the latest available Ubuntu package version.",
            log_handle=log_handle,
            stream=sys.stderr,
        )
        return False
    if reason == "invalid-version":
        _emit(
            f"Skipping {libname}; could not compare current and latest upstream versions.",
            log_handle=log_handle,
            stream=sys.stderr,
        )
        return False

    if not quiet_unchanged:
        current = _version_description(
            status["current_upstream"],
            status["current_source"],
        )
        latest = _version_description(
            status["latest_upstream"],
            status["latest_source"],
        )
        _emit(
            f"Skipping {libname}; current upstream version is already latest "
            f"available Ubuntu package version (current {current}, latest {latest}).",
            log_handle=log_handle,
        )
    return False


def _git_ahead_behind(workdir):
    upstream = _git_output(workdir, "rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{u}")
    if upstream is None:
        return None, None, None

    counts = _git_output(workdir, "rev-list", "--left-right", "--count", f"HEAD...{upstream}")
    if counts is None:
        return upstream, None, None

    parts = counts.split()
    if len(parts) != 2:
        return upstream, None, None
    return upstream, int(parts[0]), int(parts[1])


def _port_status(workdir, libname, phases, repo_slug=None):
    status = {
        "libname": libname,
        "repo_name": _port_repo_name(libname),
        "repo_slug": repo_slug,
        "workdir": os.path.abspath(workdir),
        "checkout": "missing",
        "remote_url": None,
        "branch": None,
        "head": None,
        "dirty": None,
        "upstream": None,
        "ahead": None,
        "behind": None,
        "completed_phases": [],
        "last_phase": None,
        "next_phase": phases[0] if phases else None,
        "stage": "not-started",
        "original_version": None,
        "safe_version": None,
    }

    if not os.path.exists(workdir):
        return status

    if not _is_git_worktree(workdir):
        status["checkout"] = "non-git"
        return status

    status["checkout"] = "present"
    status["remote_url"] = _git_output(workdir, "remote", "get-url", "origin")
    status["branch"] = _git_output(workdir, "branch", "--show-current")
    status["head"] = _git_output(workdir, "rev-parse", "--short", "HEAD")
    dirty_status = _git_output(workdir, "status", "--porcelain")
    status["dirty"] = dirty_status is None or bool(dirty_status)
    upstream, ahead, behind = _git_ahead_behind(workdir)
    status["upstream"] = upstream
    status["ahead"] = ahead
    status["behind"] = behind
    current_metadata = _current_source_metadata(workdir)
    status["original_version"] = None if current_metadata is None else current_metadata[1]
    status["safe_version"] = _cargo_package_version(workdir)

    tags = _git_output(workdir, "tag", "--merged", "HEAD", "--list", f"{libname}/*")
    tagged_phases = set()
    if tags:
        prefix = f"{libname}/"
        tagged_phases = {
            tag[len(prefix):]
            for tag in tags.splitlines()
            if tag.startswith(prefix)
        }
    completed = [phase for phase in phases if phase in tagged_phases]
    status["completed_phases"] = completed
    if completed:
        last_phase = completed[-1]
        status["last_phase"] = last_phase
        last_index = phases.index(last_phase)
        if last_index == len(phases) - 1:
            status["stage"] = "complete"
            status["next_phase"] = None
        else:
            status["stage"] = f"after {last_phase}"
            status["next_phase"] = phases[last_index + 1]
    return status


def _format_port_status(status):
    lines = [
        f"{status['repo_name']} ({status['libname']})",
        f"  Workdir: {status['workdir']}",
        f"  Checkout: {status['checkout']}",
    ]
    if status.get("repo_slug"):
        lines.append(f"  GitHub: {status['repo_slug']}")
    if status["checkout"] != "present":
        return lines

    branch = status["branch"] or "detached"
    head = status["head"] or "unknown"
    dirty = "dirty" if status["dirty"] else "clean"
    lines.append(f"  Git: {branch} {head} ({dirty})")
    if status["remote_url"]:
        lines.append(f"  Remote: {status['remote_url']}")
    if status["upstream"]:
        ahead = status["ahead"]
        behind = status["behind"]
        if ahead is None or behind is None:
            lines.append(f"  Upstream: {status['upstream']}")
        else:
            lines.append(f"  Upstream: {status['upstream']} (ahead {ahead}, behind {behind})")

    completed = status["completed_phases"]
    if completed:
        phase_text = f"completed through {status['last_phase']}"
    else:
        phase_text = "no completed phases"
    if status["next_phase"]:
        phase_text = f"{phase_text}; next {status['next_phase']}"
    lines.append(f"  Stage: {phase_text}")

    versions = []
    if status["original_version"]:
        versions.append(f"original {status['original_version']}")
    if status["safe_version"]:
        versions.append(f"safe {status['safe_version']}")
    if versions:
        lines.append(f"  Versions: {', '.join(versions)}")
    return lines


def _list_github_port_repos(owner, prefix, log_handle=None):
    result = _run_optional_command(
        [
            "gh",
            "repo",
            "list",
            owner,
            "--limit",
            "1000",
            "--json",
            "name,nameWithOwner,url",
        ],
        cwd=REPO_ROOT,
        log_handle=log_handle,
    )
    if result is None:
        return []
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip()
        suffix = f": {detail}" if detail else ""
        _emit(
            f"Could not list GitHub repositories for {owner}{suffix}",
            log_handle=log_handle,
            stream=sys.stderr,
        )
        return []

    try:
        repos = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        _emit(
            f"Could not parse GitHub repository list for {owner}: {exc}",
            log_handle=log_handle,
            stream=sys.stderr,
        )
        return []

    return [
        repo
        for repo in repos
        if isinstance(repo, dict) and repo.get("name", "").startswith(prefix)
    ]


def _local_port_repos(ports_dir, prefix):
    if not os.path.isdir(ports_dir):
        return []
    repos = []
    for entry in os.scandir(ports_dir):
        if (
            entry.is_dir()
            and entry.name.startswith(prefix)
            and entry.name != _port_repo_name("template", prefix)
        ):
            repos.append(
                {
                    "name": entry.name,
                    "libname": _libname_from_port_repo(entry.name, prefix),
                    "workdir": entry.path,
                    "repo_slug": None,
                }
            )
    return sorted(repos, key=lambda repo: repo["name"])


def _known_port_repos(ports_dir, owner, prefix, log_handle=None):
    by_name = {repo["name"]: repo for repo in _local_port_repos(ports_dir, prefix)}
    for remote in _list_github_port_repos(owner, prefix, log_handle=log_handle):
        name = remote["name"]
        if name == _port_repo_name("template", prefix):
            continue
        repo = by_name.setdefault(
            name,
            {
                "name": name,
                "libname": _libname_from_port_repo(name, prefix),
                "workdir": os.path.join(ports_dir, name),
                "repo_slug": remote.get("nameWithOwner") or f"{owner}/{name}",
            },
        )
        repo["repo_slug"] = remote.get("nameWithOwner") or repo.get("repo_slug") or f"{owner}/{name}"
    return [by_name[name] for name in sorted(by_name)]


def _emit_status(statuses, log_handle=None):
    for index, status in enumerate(statuses):
        if index:
            _emit("", log_handle=log_handle)
        for line in _format_port_status(status):
            _emit(line, log_handle=log_handle)


def _find_phase_index(phases, phase_name):
    matches = [
        i for i, phase in enumerate(phases)
        if phase == phase_name or phase.endswith(f"-{phase_name}")
    ]
    if not matches:
        return None
    return matches[0]


def _find_last_tagged_phase_index(workdir, libname, phases, log_handle=None):
    result = subprocess.run(
        ["git", "tag", "--merged", "HEAD", "--list", f"{libname}/*"],
        cwd=workdir,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        _emit(
            f"Failed to list reachable tags in {workdir}: {result.stderr.strip()}",
            log_handle=log_handle,
            stream=sys.stderr,
        )
        sys.exit(1)

    prefix = f"{libname}/"
    tagged_phases = {
        tag[len(prefix):]
        for tag in result.stdout.splitlines()
        if tag.startswith(prefix)
    }

    matches = [i for i, phase in enumerate(phases) if phase in tagged_phases]
    if not matches:
        return None
    return matches[-1]


def _workdir_has_git_history(workdir, log_handle=None):
    if not os.path.exists(os.path.join(workdir, ".git")):
        return False

    result = subprocess.run(
        ["git", "rev-parse", "--quiet", "--verify", "HEAD^{commit}"],
        cwd=workdir,
        capture_output=True,
        text=True,
    )
    if result.returncode == 0:
        return True
    if result.returncode == 1:
        return False

    _emit(
        f"Failed to inspect git history in {workdir}: {result.stderr.strip()}",
        log_handle=log_handle,
        stream=sys.stderr,
    )
    sys.exit(1)


def _run_checked_command(
    command,
    *,
    cwd,
    error_message,
    log_handle=None,
    echo_output=False,
):
    try:
        result = subprocess.run(
            command,
            cwd=cwd,
            capture_output=True,
            text=True,
        )
    except FileNotFoundError as exc:
        _emit(
            f"{error_message}: {exc}",
            log_handle=log_handle,
            stream=sys.stderr,
        )
        sys.exit(1)

    if echo_output:
        _emit_subprocess_output(result, log_handle=log_handle)
    if result.returncode == 0:
        return result

    if not echo_output:
        _emit_subprocess_output(result, log_handle=log_handle)
    detail = result.stderr.strip() or result.stdout.strip()
    suffix = f": {detail}" if detail else ""
    _emit(
        f"{error_message}{suffix}",
        log_handle=log_handle,
        stream=sys.stderr,
    )
    sys.exit(1)


def _git_config_value(workdir, key, log_handle=None):
    try:
        result = subprocess.run(
            ["git", "config", "--get", key],
            cwd=workdir,
            capture_output=True,
            text=True,
        )
    except FileNotFoundError as exc:
        _emit(
            f"Failed to inspect git config {key}: {exc}",
            log_handle=log_handle,
            stream=sys.stderr,
        )
        sys.exit(1)

    if result.returncode == 0:
        return result.stdout.strip()
    if result.returncode == 1:
        return None

    _emit(
        f"Failed to inspect git config {key}: {result.stderr.strip()}",
        log_handle=log_handle,
        stream=sys.stderr,
    )
    sys.exit(1)


def _ensure_git_identity(workdir, log_handle=None):
    if _git_config_value(workdir, "user.name", log_handle=log_handle) is None:
        _run_checked_command(
            ["git", "config", "user.name", DEFAULT_GIT_USER_NAME],
            cwd=workdir,
            error_message="Failed to configure git user.name",
            log_handle=log_handle,
        )
    if _git_config_value(workdir, "user.email", log_handle=log_handle) is None:
        _run_checked_command(
            ["git", "config", "user.email", DEFAULT_GIT_USER_EMAIL],
            cwd=workdir,
            error_message="Failed to configure git user.email",
            log_handle=log_handle,
        )


def _ensure_git_repo(workdir, log_handle=None, dry_run=False):
    if not os.path.exists(os.path.join(workdir, ".git")):
        if dry_run:
            _emit_dry_run(
                f"would initialize git repository in {workdir}",
                log_handle=log_handle,
            )
            _emit_dry_run(
                f"would create initial git commit in {workdir}",
                log_handle=log_handle,
            )
            return
        _emit(f"Initializing git repository in {workdir}", log_handle=log_handle)
        _run_checked_command(
            ["git", "init"],
            cwd=workdir,
            error_message=f"Failed to initialize git repository in {workdir}",
            log_handle=log_handle,
        )

    if _workdir_has_git_history(workdir, log_handle=log_handle):
        return

    if dry_run:
        _emit_dry_run(
            f"would create initial git commit in {workdir}",
            log_handle=log_handle,
        )
        return

    _emit(f"Creating initial git commit in {workdir}", log_handle=log_handle)
    _ensure_git_identity(workdir, log_handle=log_handle)
    _run_checked_command(
        ["git", "commit", "--allow-empty", "-m", "init"],
        cwd=workdir,
        error_message=f"Failed to create initial git commit in {workdir}",
        log_handle=log_handle,
    )


def _remote_url(workdir, remote, log_handle=None):
    try:
        result = subprocess.run(
            ["git", "remote", "get-url", remote],
            cwd=workdir,
            capture_output=True,
            text=True,
        )
    except FileNotFoundError as exc:
        _emit(
            f"Failed to inspect git remote {remote}: {exc}",
            log_handle=log_handle,
            stream=sys.stderr,
        )
        sys.exit(1)

    if result.returncode == 0:
        return result.stdout.strip()
    if result.returncode == 2:
        return None

    _emit(
        f"Failed to inspect git remote {remote}: {result.stderr.strip()}",
        log_handle=log_handle,
        stream=sys.stderr,
    )
    sys.exit(1)


def _ensure_github_remote(workdir, repo, remote, log_handle=None, dry_run=False):
    if dry_run:
        remote_url = f"https://github.com/{repo}.git"
        _emit_dry_run(
            f"would ensure git remote {remote}: {remote_url}",
            log_handle=log_handle,
        )
        return

    if _remote_url(workdir, remote, log_handle=log_handle) is not None:
        return

    remote_url = f"https://github.com/{repo}.git"
    _emit(
        f"Adding git remote {remote}: {remote_url}",
        log_handle=log_handle,
    )
    _run_checked_command(
        ["git", "remote", "add", remote, remote_url],
        cwd=workdir,
        error_message=f"Failed to add git remote {remote}",
        log_handle=log_handle,
    )


def _create_github_repo(workdir, repo, visibility, remote, log_handle=None, dry_run=False):
    command = [
        "gh",
        "repo",
        "create",
        repo,
        f"--{visibility}",
        "--source",
        workdir,
        "--remote",
        remote,
    ]
    if dry_run:
        _emit_dry_run_command(command, cwd=workdir, log_handle=log_handle)
        return

    _emit(
        f"Creating GitHub repository {repo}",
        log_handle=log_handle,
    )
    _run_checked_command(
        command,
        cwd=workdir,
        error_message=f"Failed to create GitHub repository {repo}",
        log_handle=log_handle,
        echo_output=True,
    )


def _library_tags(workdir, libname, log_handle=None):
    try:
        result = subprocess.run(
            ["git", "tag", "--list", f"{libname}/*"],
            cwd=workdir,
            capture_output=True,
            text=True,
        )
    except FileNotFoundError as exc:
        _emit(
            f"Failed to list tags for {libname}: {exc}",
            log_handle=log_handle,
            stream=sys.stderr,
        )
        sys.exit(1)

    if result.returncode != 0:
        _emit(
            f"Failed to list tags for {libname}: {result.stderr.strip()}",
            log_handle=log_handle,
            stream=sys.stderr,
        )
        sys.exit(1)
    return result.stdout.splitlines()


def _push_github(workdir, libname, remote, log_handle=None, dry_run=False):
    if dry_run:
        _emit_dry_run_command(
            ["git", "push", "-u", remote, "HEAD"],
            cwd=workdir,
            log_handle=log_handle,
        )
        _emit_dry_run(
            f"would push {libname} phase tags to GitHub remote {remote}",
            log_handle=log_handle,
        )
        return

    _emit(
        f"Pushing current branch to GitHub remote {remote}",
        log_handle=log_handle,
    )
    _run_checked_command(
        ["git", "push", "-u", remote, "HEAD"],
        cwd=workdir,
        error_message=f"Failed to push current branch to {remote}",
        log_handle=log_handle,
        echo_output=True,
    )

    tags = _library_tags(workdir, libname, log_handle=log_handle)
    if not tags:
        _emit(f"No {libname} phase tags to push.", log_handle=log_handle)
        return

    _emit(
        f"Pushing {len(tags)} {libname} phase tag(s) to GitHub remote {remote}",
        log_handle=log_handle,
    )
    _run_checked_command(
        [
            "git",
            "push",
            remote,
            *[f"refs/tags/{tag}:refs/tags/{tag}" for tag in tags],
        ],
        cwd=workdir,
        error_message=f"Failed to push {libname} phase tags to {remote}",
        log_handle=log_handle,
        echo_output=True,
    )


def _reset_workdir_to_tag(workdir, tag, log_handle=None, dry_run=False):
    if dry_run:
        _emit_dry_run(
            f"would reset workdir to tag {tag}",
            log_handle=log_handle,
        )
        return

    _emit(f"Resetting workdir to tag {tag}", log_handle=log_handle)
    result = subprocess.run(
        ["git", "checkout", tag, "--", "."],
        cwd=workdir,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        _emit(
            f"Failed to checkout tag {tag}: {result.stderr.strip()}",
            log_handle=log_handle,
            stream=sys.stderr,
        )
        _emit(
            "Make sure a previous run completed that phase.",
            log_handle=log_handle,
            stream=sys.stderr,
        )
        sys.exit(1)
    reset_result = subprocess.run(
        ["git", "reset", tag],
        cwd=workdir,
        capture_output=True,
        text=True,
    )
    _emit_subprocess_output(reset_result, log_handle=log_handle)
    if reset_result.returncode != 0:
        _emit(
            f"Failed to reset workdir to tag {tag}: {reset_result.stderr.strip()}",
            log_handle=log_handle,
            stream=sys.stderr,
        )
        sys.exit(1)


def _default_log_path(libname):
    return os.path.join(
        DEFAULT_LOG_DIR,
        f"safelibs-{libname}-{int(time.time())}.log",
    )


def _emit(message, *, log_handle=None, stream=None):
    if stream is None:
        stream = sys.stdout
    reporter = _current_reporter()
    if reporter is not None:
        reporter.emit(message, stream=stream, log_handle=log_handle)
        return
    print(message, file=stream, flush=True)
    if log_handle is not None:
        print(message, file=log_handle, flush=True)


def _format_command(command):
    return " ".join(shlex.quote(str(part)) for part in command)


def _emit_dry_run(message, *, log_handle=None):
    _emit(f"Dry run: {message}", log_handle=log_handle)


def _emit_dry_run_command(command, *, cwd, log_handle=None):
    _emit_dry_run(
        f"would run {_format_command(command)} in {cwd}",
        log_handle=log_handle,
    )


def _write_stream_text(text, *, stream, log_handle=None, final=False):
    if not text:
        if not final:
            return
    reporter = _current_reporter()
    if reporter is not None:
        reporter.write_stream_text(
            text,
            stream=stream,
            log_handle=log_handle,
            final=final,
        )
        return
    stream.write(text)
    stream.flush()
    if log_handle is not None:
        log_handle.write(text)
        log_handle.flush()


def _run_phase(script, libname, workdir, log_handle=None):
    reporter = _current_reporter()
    if reporter is None and log_handle is None:
        result = subprocess.run(
            [script, libname, workdir],
            cwd=workdir,
        )
        return result.returncode

    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"

    with subprocess.Popen(
        [script, libname, workdir],
        cwd=workdir,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ) as process:
        assert process.stdout is not None
        assert process.stderr is not None
        stdout_decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
        stderr_decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
        with selectors.DefaultSelector() as selector:
            selector.register(
                process.stdout,
                selectors.EVENT_READ,
                (sys.stdout, stdout_decoder),
            )
            selector.register(
                process.stderr,
                selectors.EVENT_READ,
                (sys.stderr, stderr_decoder),
            )

            while selector.get_map():
                for key, _ in selector.select():
                    stream = key.fileobj
                    target, decoder = key.data
                    chunk = os.read(stream.fileno(), 8192)
                    if chunk:
                        _write_stream_text(
                            decoder.decode(chunk),
                            stream=target,
                            log_handle=log_handle,
                            final=False,
                        )
                        continue

                    _write_stream_text(
                        decoder.decode(b"", final=True),
                        stream=target,
                        log_handle=log_handle,
                        final=True,
                    )
                    selector.unregister(stream)
                    stream.close()
        return process.wait()


def _emit_subprocess_output(result, *, log_handle=None):
    for line in result.stdout.splitlines():
        _emit(line, log_handle=log_handle)
    for line in result.stderr.splitlines():
        _emit(line, log_handle=log_handle, stream=sys.stderr)


def _libname_arg(value):
    if not value or "/" in value or value.startswith("-") or value.startswith("."):
        raise argparse.ArgumentTypeError(f"invalid libname: {value!r}")
    if value == "template":
        raise argparse.ArgumentTypeError(
            "'template' is the port scaffold, not a real port; refusing"
        )
    return value


def _build_parser():
    parser = argparse.ArgumentParser(description="Run the safelibs pipeline.")
    parser.add_argument(
        "action",
        choices=["status", "sync", "port"],
        help=(
            "Action to run. 'status' reports port status; 'sync' fetches, "
            "fast-forward pulls, and pushes local commits/tags for each "
            "managed port; 'port' runs porting phases."
        ),
    )
    parser.add_argument(
        "libname",
        nargs="*",
        type=_libname_arg,
        help=(
            "Library or libraries to operate on. If omitted, 'status' reports "
            "every known port and 'port' advances every known port by one "
            "pending phase. If multiple are given, each is processed in turn."
        ),
    )
    parser.add_argument(
        "--ports-dir",
        metavar="PORTS_DIR",
        default=DEFAULT_PORTS_DIR,
        help=(
            "Directory for port checkouts. The runner uses "
            "PORTS_DIR/port-LIBNAME. Defaults to ./ports next to safelibs.py."
        ),
    )
    parser.add_argument(
        "--no-auto-pull",
        action="store_true",
        help=(
            "Do not clone, fetch, or fast-forward managed port repositories "
            "before running or reporting status."
        ),
    )
    parser.add_argument(
        "--filter-upgradeable",
        action="store_true",
        help=(
            "For 'port', first check whether the checked-out source has a "
            "newer upstream Ubuntu package version available; skip the port "
            "when it does not."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Print the porting plan without cloning, pulling, initializing "
            "workdirs, running phases, tagging, or creating/pushing GitHub repos."
        ),
    )
    parser.add_argument(
        "--jobs",
        type=int,
        default=None,
        metavar="N",
        help=(
            "Run up to N round-robin port jobs at once when no LIBNAME is given. "
            "Defaults to 1."
        ),
    )
    parser.add_argument(
        "-L",
        "--log-file",
        nargs="?",
        const=_USE_DEFAULT_LOG_PATH,
        default=None,
        metavar="PATH",
        help=(
            "Append runner and phase output to PATH. If PATH is omitted, "
            "defaults to /tmp/safelibs-TARGET-EPOCH.log."
        ),
    )
    resume_group = parser.add_mutually_exclusive_group()
    resume_group.add_argument(
        "--from", dest="from_phase",
        help="Phase to resume from (e.g. '02-setup'). "
             "Resets workdir to the tag of the preceding phase before running.",
    )
    resume_group.add_argument(
        "--from-last",
        action="store_true",
        help="Resume from the phase after the most recent existing phase tag.",
    )
    parser.add_argument(
        "--github-repo",
        metavar="OWNER/REPO",
        help=(
            "GitHub repository slug used for sync/status, --create-github, "
            "or to add a missing remote before --push-github."
        ),
    )
    parser.add_argument(
        "--github-owner",
        default=DEFAULT_GITHUB_OWNER,
        help="GitHub owner used for managed port repos. Defaults to safelibs.",
    )
    parser.add_argument(
        "--github-prefix",
        default=DEFAULT_PORT_REPO_PREFIX,
        help="GitHub/local repo name prefix for managed ports. Defaults to port-.",
    )
    parser.add_argument(
        "--create-github",
        action="store_true",
        help="Create --github-repo with the gh CLI and add it as a git remote.",
    )
    parser.add_argument(
        "--github-visibility",
        choices=["private", "public", "internal"],
        default="private",
        help="Visibility for --create-github. Defaults to private.",
    )
    parser.add_argument(
        "--push-github",
        action="store_true",
        help=(
            "After a successful port run, push the current branch and phase "
            "tags to the GitHub remote."
        ),
    )
    parser.add_argument(
        "--github-remote",
        default="origin",
        help="Git remote name for GitHub create/push operations. Defaults to origin.",
    )
    backend_group = parser.add_mutually_exclusive_group()
    backend_group.add_argument(
        "--backend",
        choices=JUVENAL_BACKEND_CHOICES,
        default=None,
        help=(
            "Juvenal backend to use for phase scripts. Defaults to juvenal's "
            "own default (codex) when unset."
        ),
    )
    backend_group.add_argument(
        "--claude",
        dest="backend",
        action="store_const",
        const="claude",
        help="Shortcut for --backend claude.",
    )
    return parser


def _pipeline_scripts(log_handle=None):
    scripts = sorted(glob.glob(os.path.join(PIPELINE_DIR, "*.py")))
    if not scripts:
        _emit(
            f"No pipeline scripts found in {PIPELINE_DIR}",
            log_handle=log_handle,
            stream=sys.stderr,
        )
        sys.exit(1)
    phases = [os.path.splitext(os.path.basename(s))[0] for s in scripts]
    return scripts, phases


def _sync_one(args, log_handle=None):
    libname = args.libname
    ports_dir = os.path.abspath(getattr(args, "ports_dir", DEFAULT_PORTS_DIR))
    github_owner = getattr(args, "github_owner", DEFAULT_GITHUB_OWNER)
    github_prefix = getattr(args, "github_prefix", DEFAULT_PORT_REPO_PREFIX)
    github_remote = getattr(args, "github_remote", "origin")
    github_repo = getattr(args, "github_repo", None)
    dry_run = getattr(args, "dry_run", False)

    workdir = _default_port_workdir(libname, ports_dir, github_prefix)
    repo_slug = _github_repo_slug(
        libname,
        github_owner,
        github_prefix,
        github_repo=github_repo,
    )
    _emit(f"Syncing {libname} ({repo_slug}) at {workdir}", log_handle=log_handle)
    _sync_port_workdir(
        workdir,
        repo_slug,
        github_remote,
        libname=libname,
        log_handle=log_handle,
        dry_run=dry_run,
    )


def _sync_all(args, log_handle=None):
    ports_dir = os.path.abspath(getattr(args, "ports_dir", DEFAULT_PORTS_DIR))
    github_owner = getattr(args, "github_owner", DEFAULT_GITHUB_OWNER)
    github_prefix = getattr(args, "github_prefix", DEFAULT_PORT_REPO_PREFIX)
    github_remote = getattr(args, "github_remote", "origin")
    dry_run = getattr(args, "dry_run", False)

    repos = _known_port_repos(
        ports_dir, github_owner, github_prefix, log_handle=log_handle
    )
    if not repos:
        _emit(
            f"No {github_prefix} repositories found in {ports_dir} or "
            f"github.com/{github_owner}.",
            log_handle=log_handle,
        )
        return
    for repo in repos:
        repo_slug = repo.get("repo_slug") or f"{github_owner}/{repo['name']}"
        _emit(
            f"Syncing {repo['libname']} ({repo_slug}) at {repo['workdir']}",
            log_handle=log_handle,
        )
        _sync_port_workdir(
            repo["workdir"],
            repo_slug,
            github_remote,
            libname=repo["libname"],
            log_handle=log_handle,
            dry_run=dry_run,
        )


def _status_all(args, phases, log_handle=None):
    ports_dir = os.path.abspath(getattr(args, "ports_dir", DEFAULT_PORTS_DIR))
    github_owner = getattr(args, "github_owner", DEFAULT_GITHUB_OWNER)
    github_prefix = getattr(args, "github_prefix", DEFAULT_PORT_REPO_PREFIX)
    github_remote = getattr(args, "github_remote", "origin")
    dry_run = getattr(args, "dry_run", False)

    repos = _known_port_repos(ports_dir, github_owner, github_prefix, log_handle=log_handle)
    statuses = []
    for repo in repos:
        repo_slug_for_status = repo.get("repo_slug") or f"{github_owner}/{repo['name']}"
        if not getattr(args, "no_auto_pull", False):
            _sync_port_workdir(
                repo["workdir"],
                repo_slug_for_status,
                github_remote,
                libname=repo["libname"],
                log_handle=log_handle,
                dry_run=dry_run,
            )
        statuses.append(
            _port_status(
                repo["workdir"],
                repo["libname"],
                phases,
                repo_slug=repo_slug_for_status,
            )
        )
    if not statuses:
        _emit(
            f"No {github_prefix} repositories found in {ports_dir} or github.com/{github_owner}.",
            log_handle=log_handle,
        )
    else:
        _emit_status(statuses, log_handle=log_handle)


def _status_one(args, phases, log_handle=None):
    libname = args.libname
    ports_dir = os.path.abspath(getattr(args, "ports_dir", DEFAULT_PORTS_DIR))
    workdir = _default_port_workdir(
        libname,
        ports_dir,
        getattr(args, "github_prefix", DEFAULT_PORT_REPO_PREFIX),
    )
    github_repo = getattr(args, "github_repo", None)
    github_remote = getattr(args, "github_remote", "origin")
    github_owner = getattr(args, "github_owner", DEFAULT_GITHUB_OWNER)
    github_prefix = getattr(args, "github_prefix", DEFAULT_PORT_REPO_PREFIX)
    dry_run = getattr(args, "dry_run", False)
    repo_slug = _github_repo_slug(
        libname,
        github_owner,
        github_prefix,
        github_repo=github_repo,
    )

    if not getattr(args, "no_auto_pull", False):
        _sync_port_workdir(
            workdir,
            repo_slug,
            github_remote,
            libname=libname,
            log_handle=log_handle,
            dry_run=dry_run,
        )

    _emit_status(
        [
            _port_status(
                workdir,
                libname,
                phases,
                repo_slug=repo_slug,
            )
        ],
        log_handle=log_handle,
    )


def _run_port_one(
    args,
    scripts,
    phases,
    *,
    log_handle=None,
    log_path=None,
    max_phases=None,
    skip_complete_reset=False,
    job_reporter=None,
):
    libname = args.libname
    ports_dir = os.path.abspath(getattr(args, "ports_dir", DEFAULT_PORTS_DIR))
    workdir = _default_port_workdir(
        libname,
        ports_dir,
        getattr(args, "github_prefix", DEFAULT_PORT_REPO_PREFIX),
    )
    create_github = getattr(args, "create_github", False)
    github_repo = getattr(args, "github_repo", None)
    github_remote = getattr(args, "github_remote", "origin")
    github_visibility = getattr(args, "github_visibility", "private")
    push_github = getattr(args, "push_github", False)
    github_owner = getattr(args, "github_owner", DEFAULT_GITHUB_OWNER)
    github_prefix = getattr(args, "github_prefix", DEFAULT_PORT_REPO_PREFIX)
    dry_run = getattr(args, "dry_run", False)
    repo_slug = _github_repo_slug(
        libname,
        github_owner,
        github_prefix,
        github_repo=github_repo,
    )

    if not getattr(args, "no_auto_pull", False):
        if job_reporter is not None:
            job_reporter.set_status("syncing", detail="Syncing managed checkout")
        _sync_port_workdir(
            workdir,
            repo_slug,
            github_remote,
            libname=libname,
            log_handle=log_handle,
            dry_run=dry_run,
        )

    if getattr(args, "filter_upgradeable", False):
        if job_reporter is not None:
            job_reporter.set_status("filtering", detail="Checking Ubuntu package versions")
        if not _filter_upgradeable_allows_port(
            workdir,
            libname,
            log_handle=log_handle,
            quiet_unchanged=getattr(args, "quiet_unchanged_filter", False),
        ):
            if job_reporter is not None:
                job_reporter.set_status("skipped")
            return False

    if job_reporter is not None:
        job_reporter.set_status("preparing", detail="Preparing workdir")
    if dry_run:
        if not os.path.isdir(workdir):
            _emit_dry_run(f"would create workdir {workdir}", log_handle=log_handle)
    else:
        os.makedirs(workdir, exist_ok=True)

    _ensure_git_repo(workdir, log_handle=log_handle, dry_run=dry_run)

    # Determine which scripts to run
    start_index = 0
    from_phase = args.from_phase
    reset_tag = None
    filter_upgradeable = getattr(args, "filter_upgradeable", False)
    if job_reporter is not None:
        job_reporter.set_status("planning", detail="Determining resume point")
    if args.from_last:
        if _workdir_has_git_history(workdir, log_handle=log_handle):
            last_tagged_index = _find_last_tagged_phase_index(
                workdir,
                libname,
                phases,
                log_handle=log_handle,
            )
        else:
            last_tagged_index = None

        if last_tagged_index is None:
            _emit(
                f"No existing phase tags found for '{libname}'; starting from beginning.",
                log_handle=log_handle,
            )
        else:
            completed_tag = f"{libname}/{phases[last_tagged_index]}"
            upgrade_index = _find_phase_index(phases, "upgrade") if filter_upgradeable else None
            if upgrade_index is not None and last_tagged_index >= upgrade_index:
                start_index = upgrade_index
                _emit(
                    f"New upstream version found; re-running from {phases[start_index]}.",
                    log_handle=log_handle,
                )
            elif last_tagged_index == len(phases) - 1:
                start_index = len(phases)
                if not skip_complete_reset:
                    reset_tag = completed_tag
                _emit(
                    f"Last completed phase tag is {completed_tag}; "
                    "no remaining phases to run.",
                    log_handle=log_handle,
                )
            else:
                reset_tag = completed_tag
                from_phase = phases[last_tagged_index + 1]

    if from_phase:
        start_index = _find_phase_index(phases, from_phase)

        if start_index > 0:
            # Reset to the tag of the phase just before the restart point
            reset_tag = f"{libname}/{phases[start_index - 1]}"

    if reset_tag:
        _reset_workdir_to_tag(
            workdir,
            reset_tag,
            log_handle=log_handle,
            dry_run=dry_run,
        )

    if create_github:
        if job_reporter is not None:
            job_reporter.set_status("preparing", detail=f"Creating {github_repo}")
        _create_github_repo(
            workdir,
            github_repo,
            github_visibility,
            github_remote,
            log_handle=log_handle,
            dry_run=dry_run,
        )
    if github_repo and (create_github or push_github):
        if job_reporter is not None:
            job_reporter.set_status("preparing", detail=f"Ensuring git remote {github_remote}")
        _ensure_github_remote(
            workdir,
            github_repo,
            github_remote,
            log_handle=log_handle,
            dry_run=dry_run,
        )

    _emit(f"Library: {libname}", log_handle=log_handle)
    _emit(f"Workdir: {workdir}", log_handle=log_handle)
    if log_path is not None:
        _emit(f"Log file: {log_path}", log_handle=log_handle)
    if 0 < start_index < len(phases):
        _emit(f"Resuming from: {phases[start_index]}", log_handle=log_handle)
    scripts_to_run = scripts[start_index:]
    if dry_run and max_phases is not None:
        scripts_to_run = scripts_to_run[:max_phases]
    progress_total = (
        len(scripts_to_run)
        if max_phases is None or dry_run
        else min(max_phases, len(scripts_to_run))
    )
    if job_reporter is not None:
        next_phase = phases[start_index] if start_index < len(phases) else None
        job_reporter.set_status(
            "planning",
            phase=next_phase or "-",
            progress=f"0/{progress_total}" if progress_total else "0/0",
            detail="Prepared execution plan",
        )
    _emit(
        f"Scripts: {[os.path.basename(s) for s in scripts_to_run]}",
        log_handle=log_handle,
    )

    if not scripts_to_run and job_reporter is not None:
        job_reporter.set_status(
            "complete" if not dry_run else "planned",
            phase="-",
            progress="0/0",
            detail="No remaining phases to run",
        )

    counted_phases = 0
    last_phase = None
    for index, script in enumerate(scripts_to_run, start=1):
        if max_phases is not None and not dry_run and counted_phases >= max_phases:
            break
        name = os.path.basename(script)
        phase = os.path.splitext(name)[0]
        last_phase = phase
        phase_progress = (
            index
            if max_phases is None or dry_run
            else min(counted_phases + 1, progress_total)
        )
        progress = f"{phase_progress}/{progress_total}" if progress_total else "0/0"
        if job_reporter is not None:
            job_reporter.set_status(
                "running" if not dry_run else "planned",
                phase=phase,
                progress=progress,
                detail=f"{'Would run' if dry_run else 'Running'} {name}",
            )
        _emit(f"\n{'='*60}", log_handle=log_handle)
        action = "Would run" if dry_run else "Running"
        _emit(f"{action} {name}", log_handle=log_handle)
        _emit(f"{'='*60}", log_handle=log_handle)
        if dry_run:
            _emit_dry_run_command(
                [script, libname, workdir],
                cwd=workdir,
                log_handle=log_handle,
            )
            _emit_dry_run(
                f"would tag {libname}/{phase}",
                log_handle=log_handle,
            )
            continue

        returncode = _run_phase(script, libname, workdir, log_handle=log_handle)
        phase_skipped = returncode == PHASE_SKIPPED_EXIT_CODE
        if returncode not in {0, PHASE_SKIPPED_EXIT_CODE}:
            if job_reporter is not None:
                job_reporter.set_status(
                    "failed",
                    phase=phase,
                    progress=progress,
                    detail=f"{name} failed with exit code {returncode}",
                )
            _emit(
                f"{name} failed with exit code {returncode}",
                log_handle=log_handle,
                stream=sys.stderr,
            )
            sys.exit(returncode)

        tag = f"{libname}/{phase}"
        if job_reporter is not None:
            job_reporter.set_status(
                "tagging",
                phase=phase,
                progress=progress,
                detail=f"Tagging {tag}",
            )
        tag_result = subprocess.run(
            ["git", "tag", "-f", tag],
            cwd=workdir,
            capture_output=True,
            text=True,
        )
        _emit_subprocess_output(tag_result, log_handle=log_handle)
        if tag_result.returncode != 0:
            if job_reporter is not None:
                job_reporter.set_status(
                    "failed",
                    phase=phase,
                    progress=progress,
                    detail=f"Failed to tag {tag}",
                )
            _emit(
                f"Failed to tag {tag}: {tag_result.stderr.strip()}",
                log_handle=log_handle,
                stream=sys.stderr,
            )
            sys.exit(1)
        _emit(f"Tagged {tag}", log_handle=log_handle)
        if phase_skipped and max_phases is not None:
            _emit(
                f"{name} skipped itself; continuing because skipped phases do not count "
                "against the phase limit.",
                log_handle=log_handle,
            )
        if not phase_skipped:
            counted_phases += 1
        if job_reporter is not None:
            job_reporter.set_status(
                "running"
                if (
                    index < len(scripts_to_run)
                    and (max_phases is None or dry_run or counted_phases < max_phases)
                ) else "preparing",
                phase=phase,
                progress=progress,
                detail=f"Tagged {tag}{' (skipped)' if phase_skipped else ''}",
            )

    if push_github:
        if job_reporter is not None:
            job_reporter.set_status("pushing", detail=f"Pushing to {github_remote}")
        _push_github(
            workdir,
            libname,
            github_remote,
            log_handle=log_handle,
            dry_run=dry_run,
        )
    elif (
        not dry_run
        and not getattr(args, "no_auto_pull", False)
        and _is_git_worktree(workdir)
    ):
        if job_reporter is not None:
            job_reporter.set_status(
                "pushing", detail=f"Auto-pushing to {github_remote}"
            )
        _auto_push_workdir(
            workdir,
            github_remote,
            libname=libname,
            log_handle=log_handle,
        )

    if dry_run:
        if job_reporter is not None:
            final_phase = last_phase or "-"
            job_reporter.set_status(
                "planned",
                phase=final_phase,
                progress=f"{progress_total}/{progress_total}" if progress_total else "0/0",
                detail="Dry run plan complete",
            )
        _emit(f"\nDry run complete. Output would be in {workdir}", log_handle=log_handle)
    else:
        if job_reporter is not None:
            final_phase = last_phase or "-"
            job_reporter.set_status(
                "complete",
                phase=final_phase,
                progress=f"{progress_total}/{progress_total}" if progress_total else "0/0",
                detail="Pipeline complete",
            )
        _emit(f"\nPipeline complete. Output in {workdir}", log_handle=log_handle)
    return True


def _run_specified_lib_job(
    libname,
    per_args,
    scripts,
    phases,
    *,
    controller,
    log_handle=None,
    log_path=None,
):
    reporter = _ConcurrentPortReporter(controller, libname)
    controller.set_status(libname, "starting", detail="Starting job")
    with _reporter_context(reporter):
        try:
            _run_port_one(
                per_args,
                scripts,
                phases,
                log_handle=log_handle,
                log_path=log_path,
                job_reporter=reporter,
            )
        except SystemExit as exc:
            code = exc.code if isinstance(exc.code, int) else 1
            reporter.set_status("failed", detail=f"Exited with code {code}")
            return {"libname": libname, "exit_code": code}
        except Exception as exc:  # pragma: no cover - defensive guard for worker threads.
            reporter.set_status("failed", detail=f"Unhandled exception: {exc}")
            return {"libname": libname, "exit_code": 1}
    return {"libname": libname, "exit_code": 0}


def _run_specified_libs(args, libnames, scripts, phases, *, log_handle=None, log_path=None):
    jobs = getattr(args, "jobs", None) or 1
    max_workers = min(jobs, len(libnames)) if libnames else 0

    if max_workers <= 1:
        for libname in libnames:
            per_args = argparse.Namespace(**vars(args))
            per_args.libname = libname
            if len(libnames) > 1:
                _emit("", log_handle=log_handle)
                _emit(f"=== Target: {libname} ===", log_handle=log_handle)
            _run_port_one(
                per_args,
                scripts,
                phases,
                log_handle=log_handle,
                log_path=log_path,
            )
        return

    github_prefix = getattr(args, "github_prefix", DEFAULT_PORT_REPO_PREFIX)
    controller = _ConcurrentPortRunController(
        jobs=max_workers,
        log_handle=log_handle,
        enable_tui=True,
    )
    for libname in libnames:
        controller.register_job(libname, _port_repo_name(libname, github_prefix))

    _emit(
        f"Porting {len(libnames)} library(ies) with up to {max_workers} concurrent job(s).",
        log_handle=log_handle,
    )

    failures = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        pending = set()
        next_index = 0
        stop_submitting = False

        def submit_next():
            nonlocal next_index
            libname = libnames[next_index]
            per_args = argparse.Namespace(**vars(args))
            per_args.libname = libname
            future = executor.submit(
                _run_specified_lib_job,
                libname,
                per_args,
                scripts,
                phases,
                controller=controller,
                log_handle=log_handle,
                log_path=log_path,
            )
            pending.add(future)
            next_index += 1

        while next_index < max_workers and next_index < len(libnames):
            submit_next()

        def consume_pending(live=None):
            nonlocal next_index, stop_submitting
            while pending:
                done, still_pending = wait(
                    pending,
                    timeout=0.1,
                    return_when=FIRST_COMPLETED,
                )
                pending.clear()
                pending.update(still_pending)
                if live is not None:
                    controller.refresh(live)
                if not done:
                    continue

                saw_failure = False
                for future in done:
                    result = future.result()
                    if result["exit_code"] != 0:
                        failures.append(result)
                        saw_failure = True

                if saw_failure:
                    stop_submitting = True
                    while next_index < len(libnames):
                        controller.set_status(
                            libnames[next_index],
                            "skipped",
                            detail="Not started after earlier failure",
                        )
                        next_index += 1
                    continue

                while (
                    not stop_submitting
                    and next_index < len(libnames)
                    and len(pending) < max_workers
                ):
                    submit_next()

            if live is not None:
                controller.refresh(live)

        if controller.enable_tui:
            with Live(
                controller.render(),
                console=controller.console,
                refresh_per_second=8,
                transient=False,
            ) as live:
                consume_pending(live=live)
        else:
            consume_pending()

    if failures:
        _emit("", log_handle=log_handle)
        for failure in failures:
            _emit(
                f"Port target failed: {failure['libname']} exited with code {failure['exit_code']}.",
                log_handle=log_handle,
                stream=sys.stderr,
            )
        sys.exit(failures[0]["exit_code"])


def _round_robin_port_args(args, repo, *, filter_upgradeable, dry_run):
    per_args = argparse.Namespace(**vars(args))
    per_args.libname = repo["libname"]
    if repo.get("repo_slug"):
        per_args.github_repo = repo["repo_slug"]
    if not per_args.from_phase:
        per_args.from_last = True
    per_args.quiet_unchanged_filter = filter_upgradeable and not dry_run
    return per_args


def _run_round_robin_port_job(
    repo,
    per_args,
    scripts,
    phases,
    *,
    controller,
    log_handle=None,
    log_path=None,
):
    libname = repo["libname"]
    reporter = _ConcurrentPortReporter(controller, libname)
    controller.set_status(libname, "starting", detail="Starting job")

    with _reporter_context(reporter):
        try:
            selected = _run_port_one(
                per_args,
                scripts,
                phases,
                log_handle=log_handle,
                log_path=log_path,
                max_phases=1,
                skip_complete_reset=True,
                job_reporter=reporter,
            )
        except SystemExit as exc:
            code = exc.code if isinstance(exc.code, int) else 1
            reporter.set_status("failed", detail=f"Exited with code {code}")
            return {
                "libname": libname,
                "repo_name": repo["name"],
                "selected": False,
                "exit_code": code,
            }
        except Exception as exc:  # pragma: no cover - defensive guard for worker threads.
            reporter.set_status("failed", detail=f"Unhandled exception: {exc}")
            return {
                "libname": libname,
                "repo_name": repo["name"],
                "selected": False,
                "exit_code": 1,
            }

    if not selected:
        current_state = controller.state_for(libname)
        if current_state is not None and current_state["status"] not in {"skipped", "failed"}:
            reporter.set_status("skipped", detail="Skipped")
    return {
        "libname": libname,
        "repo_name": repo["name"],
        "selected": selected,
        "exit_code": 0,
    }


def _round_robin_ports(args, scripts, phases, log_handle=None, log_path=None):
    ports_dir = os.path.abspath(getattr(args, "ports_dir", DEFAULT_PORTS_DIR))
    github_owner = getattr(args, "github_owner", DEFAULT_GITHUB_OWNER)
    github_prefix = getattr(args, "github_prefix", DEFAULT_PORT_REPO_PREFIX)
    filter_upgradeable = getattr(args, "filter_upgradeable", False)
    dry_run = getattr(args, "dry_run", False)
    jobs = getattr(args, "jobs", None)
    if jobs is None:
        jobs = 1
    repos = _known_port_repos(ports_dir, github_owner, github_prefix, log_handle=log_handle)
    if not repos:
        _emit(
            f"No {github_prefix} repositories found in {ports_dir} or github.com/{github_owner}.",
            log_handle=log_handle,
            stream=sys.stderr,
        )
        sys.exit(1)

    if jobs <= 1:
        _emit(
            f"Round-robin porting {len(repos)} known port(s).",
            log_handle=log_handle,
        )
    else:
        _emit(
            f"Round-robin porting {len(repos)} known port(s) with up to {min(jobs, len(repos))} concurrent job(s).",
            log_handle=log_handle,
        )
    selected_ports = 0
    failures = []
    if jobs <= 1:
        for repo in repos:
            per_args = _round_robin_port_args(
                args,
                repo,
                filter_upgradeable=filter_upgradeable,
                dry_run=dry_run,
            )

            _emit("", log_handle=log_handle)
            _emit(
                f"Round-robin target: {repo['name']} ({repo['libname']})",
                log_handle=log_handle,
            )
            if _run_port_one(
                per_args,
                scripts,
                phases,
                log_handle=log_handle,
                log_path=log_path,
                max_phases=1,
                skip_complete_reset=True,
            ):
                selected_ports += 1
    else:
        max_workers = min(jobs, len(repos))
        controller = _ConcurrentPortRunController(
            jobs=max_workers,
            log_handle=log_handle,
            enable_tui=True,
        )
        for repo in repos:
            controller.register_job(repo["libname"], repo["name"])

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            pending = set()
            next_repo_index = 0
            stop_submitting = False

            def submit_next_repo():
                nonlocal next_repo_index
                repo = repos[next_repo_index]
                per_args = _round_robin_port_args(
                    args,
                    repo,
                    filter_upgradeable=filter_upgradeable,
                    dry_run=dry_run,
                )
                future = executor.submit(
                    _run_round_robin_port_job,
                    repo,
                    per_args,
                    scripts,
                    phases,
                    controller=controller,
                    log_handle=log_handle,
                    log_path=log_path,
                )
                pending.add(future)
                next_repo_index += 1

            while next_repo_index < max_workers:
                submit_next_repo()

            def consume_pending(live=None):
                nonlocal next_repo_index, stop_submitting, selected_ports
                while pending:
                    done, still_pending = wait(
                        pending,
                        timeout=0.1,
                        return_when=FIRST_COMPLETED,
                    )
                    pending.clear()
                    pending.update(still_pending)
                    if live is not None:
                        controller.refresh(live)
                    if not done:
                        continue

                    saw_failure = False
                    for future in done:
                        result = future.result()
                        if result["selected"]:
                            selected_ports += 1
                        if result["exit_code"] != 0:
                            failures.append(result)
                            saw_failure = True

                    if saw_failure:
                        stop_submitting = True
                        while next_repo_index < len(repos):
                            queued_repo = repos[next_repo_index]
                            controller.set_status(
                                queued_repo["libname"],
                                "skipped",
                                detail="Not started after earlier failure",
                            )
                            next_repo_index += 1
                        continue

                    while (
                        not stop_submitting
                        and next_repo_index < len(repos)
                        and len(pending) < max_workers
                    ):
                        submit_next_repo()

                if live is not None:
                    controller.refresh(live)

            if controller.enable_tui:
                with Live(
                    controller.render(),
                    console=controller.console,
                    refresh_per_second=8,
                    transient=False,
                ) as live:
                    consume_pending(live=live)
            else:
                consume_pending()

    if failures:
        _emit("", log_handle=log_handle)
        for failure in failures:
            _emit(
                f"Round-robin target failed: {failure['repo_name']} ({failure['libname']}) exited with code {failure['exit_code']}.",
                log_handle=log_handle,
                stream=sys.stderr,
            )
        sys.exit(failures[0]["exit_code"])

    if filter_upgradeable:
        skipped_ports = len(repos) - selected_ports
        _emit("", log_handle=log_handle)
        if selected_ports == 0:
            _emit(
                f"No upgradeable ports found among {len(repos)} known port(s).",
                log_handle=log_handle,
            )
        else:
            _emit(
                f"Upgradeable filter selected {selected_ports} of {len(repos)} "
                f"known port(s); skipped {skipped_ports}.",
                log_handle=log_handle,
            )


def _run_pipeline(args, log_handle=None, log_path=None):
    scripts, phases = _pipeline_scripts(log_handle=log_handle)
    if getattr(args, "dry_run", False):
        _emit(
            "Dry run: planned actions only; no phase scripts, git writes, or GitHub writes will run.",
            log_handle=log_handle,
        )

    libnames = list(args.libname or [])

    if args.action == "status":
        if not libnames:
            _status_all(args, phases, log_handle=log_handle)
            return
        for libname in libnames:
            per_args = argparse.Namespace(**vars(args))
            per_args.libname = libname
            _status_one(per_args, phases, log_handle=log_handle)
        return

    if args.action == "sync":
        if not libnames:
            _sync_all(args, log_handle=log_handle)
            return
        for libname in libnames:
            per_args = argparse.Namespace(**vars(args))
            per_args.libname = libname
            _sync_one(per_args, log_handle=log_handle)
        return

    if args.from_phase and _find_phase_index(phases, args.from_phase) is None:
        _emit(
            f"Unknown phase '{args.from_phase}'. Available: {phases}",
            log_handle=log_handle,
            stream=sys.stderr,
        )
        sys.exit(1)

    if not libnames:
        _round_robin_ports(args, scripts, phases, log_handle=log_handle, log_path=log_path)
        return

    _run_specified_libs(
        args,
        libnames,
        scripts,
        phases,
        log_handle=log_handle,
        log_path=log_path,
    )


def main():
    parser = _build_parser()
    args = parser.parse_args()
    jobs_was_supplied = args.jobs is not None
    if jobs_was_supplied and args.jobs < 1:
        parser.error("--jobs must be a positive integer")
    libnames = list(args.libname or [])
    if args.action in {"status", "sync"}:
        if args.from_phase or args.from_last:
            parser.error("--from and --from-last can only be used with action 'port'")
        if args.filter_upgradeable:
            parser.error("--filter-upgradeable can only be used with action 'port'")
        if args.create_github:
            parser.error("--create-github can only be used with action 'port'")
        if args.push_github:
            parser.error("--push-github can only be used with action 'port'")
        if jobs_was_supplied:
            parser.error("--jobs can only be used with action 'port'")
    if args.github_repo and len(libnames) != 1:
        parser.error("--github-repo requires exactly one LIBNAME")
    if args.create_github and not args.github_repo:
        parser.error("--create-github requires --github-repo OWNER/REPO")
    if args.create_github and len(libnames) != 1:
        parser.error("--create-github requires exactly one LIBNAME")
    if jobs_was_supplied and len(libnames) == 1:
        parser.error("--jobs only applies when zero or multiple LIBNAMEs are given")
    if args.jobs is None:
        args.jobs = 1

    if args.backend is not None:
        os.environ[JUVENAL_BACKEND_ENV] = args.backend

    log_path = None
    if args.log_file is not None:
        if len(libnames) == 1:
            log_target = libnames[0]
        elif libnames:
            log_target = f"{args.action}-multi"
        else:
            log_target = f"{args.action}-all"
        log_path = (
            _default_log_path(log_target)
            if args.log_file is _USE_DEFAULT_LOG_PATH
            else os.path.abspath(args.log_file)
        )

    if log_path is None:
        _run_pipeline(args, log_handle=None, log_path=None)
        return

    try:
        log_dir = os.path.dirname(log_path)
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)
        log_handle = open(log_path, "a", encoding="utf-8", buffering=1)
    except OSError as exc:
        print(f"Failed to open log file {log_path}: {exc}", file=sys.stderr)
        sys.exit(1)

    with log_handle:
        _run_pipeline(args, log_handle=log_handle, log_path=log_path)


if __name__ == "__main__":
    main()
