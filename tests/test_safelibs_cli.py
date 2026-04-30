"""CLI tests for the legacy safelibs.py runner."""

from __future__ import annotations

import argparse
import io
import importlib.util
import json
import os
import runpy
import stat
import subprocess
import sys
import time
import types
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
SAFELIBS_PATH = REPO_ROOT / "safelibs.py"

spec = importlib.util.spec_from_file_location("safelibs_cli_under_test", SAFELIBS_PATH)
safelibs = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(safelibs)


def _git(repo: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *args],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    )


def _write_pipeline_script(path: Path, body: str) -> None:
    path.write_text(
        "\n".join(
            [
                "#!/usr/bin/env python3",
                "import pathlib",
                "import sys",
                "",
                "workdir = pathlib.Path(sys.argv[2])",
                body,
                "",
            ]
        )
    )
    path.chmod(path.stat().st_mode | stat.S_IXUSR)


def _init_workdir_repo(workdir: Path) -> None:
    workdir.mkdir(parents=True)
    _git(workdir, "init")
    _git(workdir, "config", "user.name", "Test User")
    _git(workdir, "config", "user.email", "test@example.com")


def _commit_state(workdir: Path, state: str, message: str) -> None:
    (workdir / "state.txt").write_text(f"{state}\n")
    _git(workdir, "add", "state.txt")
    _git(workdir, "commit", "-m", message)


def _managed_workdir(tmp_path: Path, ports_name: str = "ports") -> Path:
    return tmp_path / ports_name / "port-libfoo"


def _write_changelog(workdir: Path, version: str, package: str = "libfoo") -> None:
    changelog = workdir / "original" / "debian" / "changelog"
    changelog.parent.mkdir(parents=True, exist_ok=True)
    changelog.write_text(
        f"{package} ({version}) noble; urgency=medium\n",
        encoding="utf-8",
    )


def _write_safe_changelog(workdir: Path, version: str, package: str = "libfoo") -> None:
    changelog = workdir / "safe" / "debian" / "changelog"
    changelog.parent.mkdir(parents=True, exist_ok=True)
    changelog.write_text(
        f"{package} ({version}) noble; urgency=medium\n",
        encoding="utf-8",
    )


def _write_dsc(workdir: Path, filename: str, source: str, version: str) -> None:
    dsc = workdir / "original" / filename
    dsc.parent.mkdir(parents=True, exist_ok=True)
    dsc.write_text(
        "\n".join(
            [
                "Format: 3.0 (quilt)",
                f"Source: {source}",
                f"Version: {version}",
                "",
            ]
        ),
        encoding="utf-8",
    )


def _safelibs_argv(workdir: Path, *cli_args: str) -> list[str]:
    return [
        "safelibs.py",
        "port",
        "libfoo",
        "--ports-dir",
        os.fspath(workdir.parent),
        "--no-auto-pull",
        *cli_args,
    ]


def _install_fake_upgradeability_module(
    monkeypatch: pytest.MonkeyPatch,
    *,
    status: dict[str, object],
) -> None:
    fake_safelibs = types.ModuleType("safelibs")
    fake_safelibs.PHASE_SKIPPED_EXIT_CODE = safelibs.PHASE_SKIPPED_EXIT_CODE
    fake_safelibs._upgradeability_status = lambda workdir, libname: status
    fake_safelibs._version_description = safelibs._version_description
    monkeypatch.setitem(sys.modules, "safelibs", fake_safelibs)


def test_builtin_pipeline_includes_upgrade_after_test_phase() -> None:
    scripts = sorted(Path(safelibs.PIPELINE_DIR).glob("*.py"))
    phases = [script.stem for script in scripts]

    assert "04-test" in phases
    assert "05-validate" in phases
    assert "06-upgrade" in phases
    assert phases.index("05-validate") == phases.index("04-test") + 1
    assert phases.index("06-upgrade") == phases.index("05-validate") + 1
    assert os.access(REPO_ROOT / "pipeline" / "05-validate.py", os.X_OK)
    assert os.access(REPO_ROOT / "pipeline" / "06-upgrade.py", os.X_OK)


def test_builtin_pipeline_ends_with_document_phase() -> None:
    scripts = sorted(Path(safelibs.PIPELINE_DIR).glob("*.py"))
    phases = [script.stem for script in scripts]

    assert "07-document" in phases
    assert phases[-1] == "07-document"
    assert phases.index("07-document") == phases.index("06-upgrade") + 1
    assert os.access(REPO_ROOT / "pipeline" / "07-document.py", os.X_OK)


def test_document_phase_prompt_covers_required_sections(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    phase_path = REPO_ROOT / "pipeline" / "07-document.py"
    workdir = _managed_workdir(tmp_path)
    (workdir / ".git").mkdir(parents=True)
    observed: dict[str, object] = {}

    class FakeGoal:
        def __enter__(self) -> None:
            return None

        def __exit__(self, *exc_info: object) -> bool:
            return False

    def goal(description: str, *, working_dir: str) -> FakeGoal:
        observed["goal_description"] = description
        observed["working_dir"] = working_dir
        return FakeGoal()

    def plan_and_do(prompt: str) -> None:
        observed["prompt"] = " ".join(prompt.split())

    def fail_subprocess_run(*args: object, **kwargs: object) -> None:
        raise AssertionError("existing git workdir should not be reinitialized")

    juvenal_module = types.ModuleType("juvenal")
    juvenal_module.__path__ = []
    juvenal_api_module = types.ModuleType("juvenal.api")
    juvenal_api_module.goal = goal
    juvenal_api_module.plan_and_do = plan_and_do
    juvenal_module.api = juvenal_api_module

    monkeypatch.setitem(sys.modules, "juvenal", juvenal_module)
    monkeypatch.setitem(sys.modules, "juvenal.api", juvenal_api_module)
    monkeypatch.setattr(subprocess, "run", fail_subprocess_run)
    monkeypatch.setattr(sys, "argv", [os.fspath(phase_path), "libfoo", os.fspath(workdir)])

    runpy.run_path(os.fspath(phase_path), run_name="__main__")

    prompt = observed["prompt"]
    assert observed["goal_description"] == "document the libfoo Rust port"
    assert observed["working_dir"] == os.fspath(workdir)
    assert isinstance(prompt, str)

    required_contracts = [
        f"{workdir}/safe/PORT.md",
        "High-level architecture",
        "Where the unsafe Rust lives",
        "Remaining unsafe FFI beyond the original ABI/API boundary",
        "Remaining issues",
        "Dependencies and other libraries used",
        "How this document was produced",
        f"{workdir}/dependents.json",
        f"{workdir}/relevant_cves.json",
        f"{workdir}/safe/Cargo.toml",
    ]
    for contract in required_contracts:
        assert contract in prompt


def test_upgrade_phase_prompt_covers_required_upgrade_work(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    phase_path = REPO_ROOT / "pipeline" / "06-upgrade.py"
    workdir = _managed_workdir(tmp_path)
    (workdir / ".git").mkdir(parents=True)
    observed: dict[str, object] = {}

    class FakeGoal:
        def __enter__(self) -> None:
            return None

        def __exit__(self, *exc_info: object) -> bool:
            return False

    def goal(description: str, *, working_dir: str) -> FakeGoal:
        observed["goal_description"] = description
        observed["working_dir"] = working_dir
        return FakeGoal()

    def plan_and_do(prompt: str) -> None:
        observed["prompt"] = " ".join(prompt.split())

    def fail_subprocess_run(*args: object, **kwargs: object) -> None:
        raise AssertionError("existing git workdir should not be reinitialized")

    juvenal_module = types.ModuleType("juvenal")
    juvenal_module.__path__ = []
    juvenal_api_module = types.ModuleType("juvenal.api")
    juvenal_api_module.goal = goal
    juvenal_api_module.plan_and_do = plan_and_do
    juvenal_module.api = juvenal_api_module

    monkeypatch.setitem(sys.modules, "juvenal", juvenal_module)
    monkeypatch.setitem(sys.modules, "juvenal.api", juvenal_api_module)
    _install_fake_upgradeability_module(
        monkeypatch,
        status={
            "upgradeable": True,
            "reason": "new-upstream-version",
            "current_source": "1.2.3-1ubuntu1",
            "current_upstream": "1.2.3",
            "latest_source": "1.2.4-1",
            "latest_upstream": "1.2.4",
        },
    )
    monkeypatch.setattr(subprocess, "run", fail_subprocess_run)
    monkeypatch.setattr(sys, "argv", [os.fspath(phase_path), "libfoo", os.fspath(workdir)])

    runpy.run_path(os.fspath(phase_path), run_name="__main__")

    prompt = observed["prompt"]
    assert observed["goal_description"] == "upgrade the libfoo Rust port to the latest Ubuntu version"
    assert observed["working_dir"] == os.fspath(workdir)
    assert isinstance(prompt, str)

    required_contracts = [
        f"libfoo, originally implemented as {workdir}/original, has been translated to a memory-safe Rust",
        f"implementation in {workdir}/safe",
        "Upgrade this completed port to track the latest ubuntu 24.04 source package for libfoo",
        f"Overwrite {workdir}/original with the latest available ubuntu 24.04 source package for libfoo and commit it",
        f"Identify and document every meaningful difference in the new version in {workdir}/upgrade-report.md",
        "baseline and latest Ubuntu package versions",
        "findings from debian/changelog, debian/patches, upstream changelogs, NEWS files, release notes",
        "public C API, header, symbol, ABI, pkg-config, build-system, and packaging changes",
        "behavior changes, bug fixes, security fixes, and CVE-relevant changes",
        "added, removed, or changed upstream tests and test data",
        f"compatibility risks for software listed in {workdir}/dependents.json",
        f"Update {workdir}/safe so it implements the latest source package behavior",
        f"Port all new or changed upstream tests from the upgraded {workdir}/original tree into the Rust-port test suite",
        "Add regression tests for behavior, ABI, packaging, and dependent-application changes",
        "Update package metadata and any install/upgrade harnesses",
        "run the existing tests",
        "run the newly ported latest-version tests",
        "verify source compatibility",
        "verify link compatibility",
        "verify runtime compatibility",
        "verify package install and upgrade behavior",
        "Each implementation and test-porting phase should commit to git",
    ]
    for contract in required_contracts:
        assert contract in prompt

    assert "latest-original" not in prompt


def test_upgrade_phase_skips_when_no_newer_upstream_version(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    phase_path = REPO_ROOT / "pipeline" / "06-upgrade.py"
    workdir = _managed_workdir(tmp_path)
    workdir.mkdir(parents=True)

    def fail_goal(*args: object, **kwargs: object) -> None:
        raise AssertionError("upgrade phase should skip before invoking juvenal")

    juvenal_module = types.ModuleType("juvenal")
    juvenal_module.__path__ = []
    juvenal_api_module = types.ModuleType("juvenal.api")
    juvenal_api_module.goal = fail_goal
    juvenal_api_module.plan_and_do = fail_goal
    juvenal_module.api = juvenal_api_module

    monkeypatch.setitem(sys.modules, "juvenal", juvenal_module)
    monkeypatch.setitem(sys.modules, "juvenal.api", juvenal_api_module)
    _install_fake_upgradeability_module(
        monkeypatch,
        status={
            "upgradeable": False,
            "reason": "new-upstream-version",
            "current_source": "1.2.3-1ubuntu1",
            "current_upstream": "1.2.3",
            "latest_source": "1.2.3-1ubuntu2",
            "latest_upstream": "1.2.3",
        },
    )
    monkeypatch.setattr(sys, "argv", [os.fspath(phase_path), "libfoo", os.fspath(workdir)])

    with pytest.raises(SystemExit) as exc_info:
        runpy.run_path(os.fspath(phase_path), run_name="__main__")

    out = capsys.readouterr().out
    assert exc_info.value.code == safelibs.PHASE_SKIPPED_EXIT_CODE
    assert "Skipping upgrade for libfoo; current upstream version is already latest" in out
    assert "current 1.2.3 (source package 1.2.3-1ubuntu1)" in out
    assert "latest 1.2.3 (source package 1.2.3-1ubuntu2)" in out


def _run_safelibs_subprocess(
    pipeline_dir: Path,
    workdir: Path,
    *cli_args: str,
    timeout: int = 5,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        _safelibs_subprocess_command(pipeline_dir, workdir, *cli_args),
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )


def _safelibs_subprocess_command(
    pipeline_dir: Path,
    workdir: Path,
    *cli_args: str,
) -> list[str]:
    runner = "\n".join(
        [
            "import importlib.util",
            "import sys",
            "",
            "spec = importlib.util.spec_from_file_location('safelibs_cli_under_test', sys.argv[1])",
            "module = importlib.util.module_from_spec(spec)",
            "assert spec.loader is not None",
            "spec.loader.exec_module(module)",
            "module.PIPELINE_DIR = sys.argv[2]",
            "sys.argv = ['safelibs.py', *sys.argv[3:]]",
            "module.main()",
        ]
    )
    return [
        sys.executable,
        "-c",
        runner,
        os.fspath(SAFELIBS_PATH),
        os.fspath(pipeline_dir),
        "port",
        "libfoo",
        "--ports-dir",
        os.fspath(workdir.parent),
        "--no-auto-pull",
        *cli_args,
    ]


def test_runner_rejects_positional_workdir(capsys: pytest.CaptureFixture[str]) -> None:
    parser = safelibs._build_parser()

    with pytest.raises(SystemExit) as exc_info:
        parser.parse_args(["port", "libfoo", "/tmp/workdir"])

    assert exc_info.value.code == 2
    assert "invalid libname: '/tmp/workdir'" in capsys.readouterr().err


def test_status_rejects_filter_upgradeable(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(sys, "argv", ["safelibs.py", "status", "libfoo", "--filter-upgradeable"])

    with pytest.raises(SystemExit) as exc_info:
        safelibs.main()

    assert exc_info.value.code == 2
    assert "--filter-upgradeable can only be used with action 'port'" in capsys.readouterr().err


@pytest.mark.parametrize("jobs", ["1", "2"])
def test_status_rejects_jobs(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    jobs: str,
) -> None:
    monkeypatch.setattr(sys, "argv", ["safelibs.py", "status", "--jobs", jobs])

    with pytest.raises(SystemExit) as exc_info:
        safelibs.main()

    assert exc_info.value.code == 2
    assert "--jobs can only be used with action 'port'" in capsys.readouterr().err


@pytest.mark.parametrize("jobs", ["1", "2"])
def test_port_rejects_jobs_with_explicit_library(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    jobs: str,
) -> None:
    monkeypatch.setattr(sys, "argv", ["safelibs.py", "port", "libfoo", "--jobs", jobs])

    with pytest.raises(SystemExit) as exc_info:
        safelibs.main()

    assert exc_info.value.code == 2
    assert "--jobs only applies when zero or multiple LIBNAMEs are given" in capsys.readouterr().err


def test_run_phase_uses_inherited_streams_without_log_file(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: dict[str, object] = {}

    def fake_run(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        observed["command"] = command
        observed["kwargs"] = kwargs
        return subprocess.CompletedProcess(command, 17)

    def fail_popen(*args: object, **kwargs: object) -> None:
        raise AssertionError("subprocess.Popen should not be used without -L")

    monkeypatch.setattr(safelibs.subprocess, "run", fake_run)
    monkeypatch.setattr(safelibs.subprocess, "Popen", fail_popen)

    returncode = safelibs._run_phase("/tmp/phase.py", "libfoo", "/tmp/workdir")

    assert returncode == 17
    assert observed["command"] == ["/tmp/phase.py", "libfoo", "/tmp/workdir"]
    assert observed["kwargs"] == {"cwd": "/tmp/workdir"}


def test_runner_initializes_fresh_workdir_before_phase(tmp_path: Path) -> None:
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()
    _write_pipeline_script(
        pipeline_dir / "01-recon.py",
        "\n".join(
            [
                "import subprocess",
                "assert (workdir / '.git').exists()",
                "subprocess.run(['git', 'rev-parse', '--verify', 'HEAD^{commit}'], cwd=workdir, check=True)",
                "(workdir / 'phase.txt').write_text('ran\\n', encoding='utf-8')",
            ]
        ),
    )

    workdir = _managed_workdir(tmp_path)

    result = _run_safelibs_subprocess(pipeline_dir, workdir)

    assert result.returncode == 0, (result.stdout, result.stderr)
    assert "Initializing git repository" in result.stdout
    _git(workdir, "rev-parse", "--verify", "HEAD^{commit}")
    tags = _git(workdir, "tag", "--list", "libfoo/*").stdout.splitlines()
    assert tags == ["libfoo/01-recon"]


def test_dry_run_prints_plan_without_running_or_mutating_workdir(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()
    for name in ["01-recon.py", "02-setup.py"]:
        _write_pipeline_script(
            pipeline_dir / name,
            "raise AssertionError('dry run should not execute phases')",
        )

    workdir = _managed_workdir(tmp_path)
    log_path = tmp_path / "pipeline.log"

    monkeypatch.setattr(safelibs, "PIPELINE_DIR", os.fspath(pipeline_dir))
    monkeypatch.setattr(
        sys,
        "argv",
        _safelibs_argv(workdir, "--dry-run", "-L", os.fspath(log_path)),
    )

    safelibs.main()

    out = capsys.readouterr().out
    assert "Dry run: planned actions only" in out
    assert f"Dry run: would create workdir {workdir}" in out
    assert f"Dry run: would initialize git repository in {workdir}" in out
    assert "Scripts: ['01-recon.py', '02-setup.py']" in out
    assert "Dry run: would run" in out
    assert "01-recon.py libfoo" in out
    assert "02-setup.py libfoo" in out
    assert "Dry run: would tag libfoo/01-recon" in out
    assert "Dry run: would tag libfoo/02-setup" in out
    assert f"Dry run complete. Output would be in {workdir}" in out
    assert not workdir.exists()

    log_text = log_path.read_text(encoding="utf-8")
    assert "Dry run: planned actions only" in log_text
    assert "Dry run: would run" in log_text
    assert "Dry run complete" in log_text


def test_dry_run_prints_auto_sync_and_github_actions_without_executing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()
    _write_pipeline_script(
        pipeline_dir / "01-recon.py",
        "raise AssertionError('dry run should not execute phases')",
    )

    ports_dir = tmp_path / "ports"
    workdir = ports_dir / "port-libfoo"

    monkeypatch.setattr(safelibs, "PIPELINE_DIR", os.fspath(pipeline_dir))
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "safelibs.py",
            "port",
            "libfoo",
            "--ports-dir",
            os.fspath(ports_dir),
            "--dry-run",
            "--create-github",
            "--github-repo",
            "acme/libfoo-safe",
            "--push-github",
        ],
    )

    safelibs.main()

    out = capsys.readouterr().out
    assert (
        "Dry run: would clone https://github.com/acme/libfoo-safe.git "
        f"into {workdir} if available"
    ) in out
    assert (
        "Dry run: would run gh repo create acme/libfoo-safe --private "
        f"--source {workdir} --remote origin in {workdir}"
    ) in out
    assert (
        "Dry run: would ensure git remote origin: "
        "https://github.com/acme/libfoo-safe.git"
    ) in out
    assert f"Dry run: would run git push -u origin HEAD in {workdir}" in out
    assert "Dry run: would push libfoo phase tags to GitHub remote origin" in out
    assert not workdir.exists()


def test_dry_run_filter_upgradeable_fetches_latest_version_without_workdir(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()
    _write_pipeline_script(
        pipeline_dir / "01-recon.py",
        "raise AssertionError('dry run should not execute phases')",
    )

    ports_dir = tmp_path / "ports"
    workdir = ports_dir / "port-libfoo"
    latest_calls: list[str] = []

    def latest_version(libname: str, log_handle: object = None) -> str:
        latest_calls.append(libname)
        return "1.2.4-1"

    monkeypatch.setattr(safelibs, "PIPELINE_DIR", os.fspath(pipeline_dir))
    monkeypatch.setattr(safelibs, "_latest_ubuntu_package_version", latest_version)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "safelibs.py",
            "port",
            "libfoo",
            "--ports-dir",
            os.fspath(ports_dir),
            "--dry-run",
            "--filter-upgradeable",
        ],
    )

    safelibs.main()

    out = capsys.readouterr().out
    assert latest_calls == ["libfoo"]
    assert (
        "Dry run: would clone https://github.com/safelibs/port-libfoo.git "
        f"into {workdir} if available"
    ) in out
    assert "Skipping libfoo; no current version found in original/debian/changelog." in out
    assert not workdir.exists()


def test_builtin_phase_scripts_do_not_bootstrap_git() -> None:
    for script in sorted(Path(safelibs.PIPELINE_DIR).glob("*.py")):
        source = script.read_text(encoding="utf-8")
        assert "import subprocess" not in source
        assert "subprocess.run" not in source


def test_create_github_repo_uses_gh_and_adds_missing_remote(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()
    _write_pipeline_script(pipeline_dir / "01-recon.py", "print('phase stdout')")

    real_run = safelibs.subprocess.run
    gh_commands: list[list[str]] = []

    def fake_run(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        if command[:3] == ["gh", "repo", "create"]:
            gh_commands.append(command)
            return subprocess.CompletedProcess(command, 0, "created\n", "")
        return real_run(command, **kwargs)

    workdir = _managed_workdir(tmp_path)

    monkeypatch.setattr(safelibs, "PIPELINE_DIR", os.fspath(pipeline_dir))
    monkeypatch.setattr(safelibs.subprocess, "run", fake_run)
    monkeypatch.setattr(
        sys,
        "argv",
        _safelibs_argv(
            workdir,
            "--create-github",
            "--github-repo",
            "acme/libfoo-safe",
            "--github-visibility",
            "public",
        ),
    )

    safelibs.main()

    assert gh_commands == [
        [
            "gh",
            "repo",
            "create",
            "acme/libfoo-safe",
            "--public",
            "--source",
            os.fspath(workdir),
            "--remote",
            "origin",
        ]
    ]
    assert _git(workdir, "remote", "get-url", "origin").stdout.strip() == (
        "https://github.com/acme/libfoo-safe.git"
    )


def test_push_github_pushes_current_branch_and_library_tags(tmp_path: Path) -> None:
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()
    _write_pipeline_script(
        pipeline_dir / "01-recon.py",
        "\n".join(
            [
                "import subprocess",
                "(workdir / 'state.txt').write_text('phase1\\n', encoding='utf-8')",
                "subprocess.run(['git', 'add', 'state.txt'], cwd=workdir, check=True)",
                "subprocess.run(['git', 'commit', '-m', 'phase1'], cwd=workdir, check=True)",
            ]
        ),
    )

    remote = tmp_path / "remote.git"
    subprocess.run(["git", "init", "--bare", os.fspath(remote)], check=True)
    workdir = _managed_workdir(tmp_path)
    _init_workdir_repo(workdir)
    _git(workdir, "remote", "add", "origin", os.fspath(remote))

    result = _run_safelibs_subprocess(
        pipeline_dir,
        workdir,
        "--push-github",
    )

    assert result.returncode == 0, (result.stdout, result.stderr)
    branch = _git(workdir, "branch", "--show-current").stdout.strip()
    subprocess.run(
        ["git", "--git-dir", os.fspath(remote), "rev-parse", f"refs/heads/{branch}"],
        check=True,
        capture_output=True,
        text=True,
    )
    subprocess.run(
        ["git", "--git-dir", os.fspath(remote), "rev-parse", "refs/tags/libfoo/01-recon"],
        check=True,
        capture_output=True,
        text=True,
    )


def test_default_workdir_uses_managed_ports_checkout(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()
    _write_pipeline_script(
        pipeline_dir / "01-recon.py",
        "(workdir / 'phase.txt').write_text('ran\\n', encoding='utf-8')",
    )

    ports_dir = tmp_path / "ports"

    monkeypatch.setattr(safelibs, "PIPELINE_DIR", os.fspath(pipeline_dir))
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "safelibs.py",
            "port",
            "libfoo",
            "--ports-dir",
            os.fspath(ports_dir),
            "--no-auto-pull",
        ],
    )

    safelibs.main()

    workdir = ports_dir / "port-libfoo"
    out = capsys.readouterr().out
    assert f"Workdir: {workdir}" in out
    assert (workdir / "phase.txt").read_text(encoding="utf-8") == "ran\n"


def test_managed_ports_checkout_auto_pulls_existing_repo(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()
    _write_pipeline_script(
        pipeline_dir / "01-recon.py",
        "(workdir / 'observed.txt').write_text((workdir / 'state.txt').read_text(encoding='utf-8'), encoding='utf-8')",
    )

    remote = tmp_path / "remote.git"
    seed = tmp_path / "seed"
    ports_dir = tmp_path / "ports"
    workdir = ports_dir / "port-libfoo"
    ports_dir.mkdir()
    subprocess.run(["git", "init", "--bare", os.fspath(remote)], check=True)
    _init_workdir_repo(seed)
    _commit_state(seed, "old", "old")
    _git(seed, "remote", "add", "origin", os.fspath(remote))
    _git(seed, "push", "-u", "origin", "HEAD")
    subprocess.run(["git", "clone", os.fspath(remote), os.fspath(workdir)], check=True)
    _commit_state(seed, "new", "new")
    _git(seed, "push")

    monkeypatch.setattr(safelibs, "PIPELINE_DIR", os.fspath(pipeline_dir))
    monkeypatch.setattr(
        sys,
        "argv",
        ["safelibs.py", "port", "libfoo", "--ports-dir", os.fspath(ports_dir)],
    )

    safelibs.main()

    assert (workdir / "observed.txt").read_text(encoding="utf-8") == "new\n"


def test_status_reports_phase_and_versions(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()
    for name in ["01-recon.py", "02-setup.py", "03-port.py"]:
        _write_pipeline_script(pipeline_dir / name, "raise AssertionError('status should not run phases')")

    ports_dir = tmp_path / "ports"
    workdir = ports_dir / "port-libfoo"
    ports_dir.mkdir()
    _init_workdir_repo(workdir)
    (workdir / "original" / "debian").mkdir(parents=True)
    (workdir / "safe").mkdir()
    (workdir / "original" / "debian" / "changelog").write_text(
        "libfoo (1.2.3-1ubuntu1) noble; urgency=medium\n",
        encoding="utf-8",
    )
    (workdir / "safe" / "Cargo.toml").write_text(
        "\n".join(["[package]", 'name = "libfoo-safe"', 'version = "1.2.3"']),
        encoding="utf-8",
    )
    _git(workdir, "add", "original", "safe")
    _git(workdir, "commit", "-m", "versions")
    _git(workdir, "tag", "libfoo/01-recon")
    _git(workdir, "tag", "libfoo/02-setup")

    monkeypatch.setattr(safelibs, "PIPELINE_DIR", os.fspath(pipeline_dir))
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "safelibs.py",
            "status",
            "libfoo",
            "--ports-dir",
            os.fspath(ports_dir),
            "--no-auto-pull",
        ],
    )

    safelibs.main()

    out = capsys.readouterr().out
    assert "port-libfoo (libfoo)" in out
    assert "Checkout: present" in out
    assert "Stage: completed through 02-setup; next 03-port" in out
    assert "Versions: original 1.2.3-1ubuntu1, safe 1.2.3" in out


def test_status_treats_plain_port_directory_inside_parent_repo_as_non_git(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()
    _write_pipeline_script(pipeline_dir / "01-recon.py", "raise AssertionError('status should not run phases')")

    parent_repo = tmp_path / "parent"
    _init_workdir_repo(parent_repo)
    ports_dir = parent_repo / "ports"
    workdir = ports_dir / "port-libfoo"
    workdir.mkdir(parents=True)
    (workdir / "README.txt").write_text("not a checkout\n", encoding="utf-8")

    monkeypatch.setattr(safelibs, "PIPELINE_DIR", os.fspath(pipeline_dir))
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "safelibs.py",
            "status",
            "libfoo",
            "--ports-dir",
            os.fspath(ports_dir),
            "--no-auto-pull",
        ],
    )

    safelibs.main()

    out = capsys.readouterr().out
    assert "port-libfoo (libfoo)" in out
    assert "Checkout: non-git" in out
    assert "Git:" not in out


def test_status_all_includes_github_port_repos_not_checked_out(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()
    _write_pipeline_script(pipeline_dir / "01-recon.py", "raise AssertionError('status should not run phases')")

    def fake_list_github_port_repos(owner: str, prefix: str, log_handle: object = None) -> list[dict[str, str]]:
        assert owner == "safelibs"
        assert prefix == "port-"
        return [
            {
                "name": "port-libbar",
                "nameWithOwner": "safelibs/port-libbar",
                "url": "https://github.com/safelibs/port-libbar",
            }
        ]

    monkeypatch.setattr(safelibs, "PIPELINE_DIR", os.fspath(pipeline_dir))
    monkeypatch.setattr(safelibs, "_list_github_port_repos", fake_list_github_port_repos)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "safelibs.py",
            "status",
            "--ports-dir",
            os.fspath(tmp_path / "ports"),
            "--no-auto-pull",
        ],
    )

    safelibs.main()

    out = capsys.readouterr().out
    assert "port-libbar (libbar)" in out
    assert "GitHub: safelibs/port-libbar" in out
    assert "Checkout: missing" in out


def test_port_without_library_round_robins_known_ports(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()
    for name in ["01-recon.py", "02-setup.py", "03-port.py"]:
        phase = name.removesuffix(".py")
        _write_pipeline_script(
            pipeline_dir / name,
            "\n".join(
                [
                    "with (workdir / 'runs.log').open('a', encoding='utf-8') as handle:",
                    f"    handle.write('{phase}\\n')",
                ]
            ),
        )

    ports_dir = tmp_path / "ports"
    foo_workdir = ports_dir / "port-libfoo"
    bar_workdir = ports_dir / "port-libbar"
    _init_workdir_repo(foo_workdir)
    _commit_state(foo_workdir, "phase1", "phase1")
    _git(foo_workdir, "tag", "libfoo/01-recon")
    _init_workdir_repo(bar_workdir)

    monkeypatch.setattr(safelibs, "PIPELINE_DIR", os.fspath(pipeline_dir))
    monkeypatch.setattr(safelibs, "_list_github_port_repos", lambda *args, **kwargs: [])
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "safelibs.py",
            "port",
            "--ports-dir",
            os.fspath(ports_dir),
            "--no-auto-pull",
        ],
    )

    safelibs.main()

    out = capsys.readouterr().out
    assert "Round-robin porting 2 known port(s)." in out
    assert (foo_workdir / "runs.log").read_text(encoding="utf-8") == "02-setup\n"
    assert (bar_workdir / "runs.log").read_text(encoding="utf-8") == "01-recon\n"
    assert _git(foo_workdir, "tag", "--list", "libfoo/*").stdout.splitlines() == [
        "libfoo/01-recon",
        "libfoo/02-setup",
    ]
    assert _git(bar_workdir, "tag", "--list", "libbar/*").stdout.splitlines() == [
        "libbar/01-recon",
    ]


def test_port_without_library_jobs_runs_round_robin_targets_in_parallel(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()
    _write_pipeline_script(
        pipeline_dir / "01-recon.py",
        "\n".join(
            [
                "import time",
                "libname = sys.argv[1]",
                "marker = workdir.parent / f'{libname}.started'",
                "marker.write_text('started\\n', encoding='utf-8')",
                "deadline = time.time() + 2",
                "while time.time() < deadline:",
                "    if all((workdir.parent / f'{name}.started').exists() for name in ('libfoo', 'libbar')):",
                "        break",
                "    time.sleep(0.05)",
                "else:",
                "    raise SystemExit(9)",
                "print(f'phase {libname} ready')",
                "with (workdir / 'runs.log').open('a', encoding='utf-8') as handle:",
                "    handle.write('01-recon\\n')",
            ]
        ),
    )

    ports_dir = tmp_path / "ports"
    foo_workdir = ports_dir / "port-libfoo"
    bar_workdir = ports_dir / "port-libbar"
    _init_workdir_repo(foo_workdir)
    _init_workdir_repo(bar_workdir)

    monkeypatch.setattr(safelibs, "PIPELINE_DIR", os.fspath(pipeline_dir))
    monkeypatch.setattr(safelibs, "_list_github_port_repos", lambda *args, **kwargs: [])
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "safelibs.py",
            "port",
            "--ports-dir",
            os.fspath(ports_dir),
            "--no-auto-pull",
            "--jobs",
            "2",
        ],
    )

    safelibs.main()

    out = capsys.readouterr().out
    assert "Round-robin porting 2 known port(s) with up to 2 concurrent job(s)." in out
    assert "[libfoo] Running 01-recon.py" in out
    assert "[libbar] Running 01-recon.py" in out
    assert "phase libfoo ready" in out
    assert "phase libbar ready" in out
    assert (foo_workdir / "runs.log").read_text(encoding="utf-8") == "01-recon\n"
    assert (bar_workdir / "runs.log").read_text(encoding="utf-8") == "01-recon\n"
    assert _git(foo_workdir, "tag", "--list", "libfoo/*").stdout.splitlines() == [
        "libfoo/01-recon",
    ]
    assert _git(bar_workdir, "tag", "--list", "libbar/*").stdout.splitlines() == [
        "libbar/01-recon",
    ]


def test_port_without_library_jobs_updates_rich_live_status(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    rich_console = pytest.importorskip("rich.console")
    render_console = rich_console.Console
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()
    _write_pipeline_script(
        pipeline_dir / "01-recon.py",
        "\n".join(
            [
                "import time",
                "libname = sys.argv[1]",
                "print(f'phase {libname} start')",
                "time.sleep(0.3)",
                "print(f'phase {libname} done')",
            ]
        ),
    )

    ports_dir = tmp_path / "ports"
    _init_workdir_repo(ports_dir / "port-libfoo")
    _init_workdir_repo(ports_dir / "port-libbar")

    class FakeLive:
        instances: list["FakeLive"] = []

        def __init__(self, renderable: object, **_: object) -> None:
            self.updates: list[str] = []
            self.update(renderable)
            FakeLive.instances.append(self)

        def __enter__(self) -> "FakeLive":
            return self

        def __exit__(self, *exc_info: object) -> bool:
            return False

        def update(self, renderable: object) -> None:
            buffer = io.StringIO()
            console = render_console(file=buffer, force_terminal=False, width=160)
            console.print(renderable)
            self.updates.append(buffer.getvalue())

    monkeypatch.setattr(safelibs, "PIPELINE_DIR", os.fspath(pipeline_dir))
    monkeypatch.setattr(safelibs, "_list_github_port_repos", lambda *args, **kwargs: [])
    monkeypatch.setattr(safelibs, "Console", lambda: object())
    monkeypatch.setattr(safelibs, "Live", FakeLive)
    monkeypatch.setattr(sys.stdout, "isatty", lambda: True)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "safelibs.py",
            "port",
            "--ports-dir",
            os.fspath(ports_dir),
            "--no-auto-pull",
            "--jobs",
            "2",
        ],
    )

    safelibs.main()

    assert len(FakeLive.instances) == 1
    rendered = "\n".join(FakeLive.instances[0].updates)
    out = capsys.readouterr().out
    assert "Round-robin porting 2 known port(s) with up to 2 concurrent job(s)." in out
    assert "[libfoo]" not in out
    assert "[libbar]" not in out
    assert "port-libfoo" in rendered
    assert "port-libbar" in rendered
    assert "running" in rendered
    assert "complete" in rendered
    assert "phase libfoo start" in rendered or "phase libbar start" in rendered
    assert "phase libfoo done" in rendered or "phase libbar done" in rendered


def test_port_without_library_jobs_stops_submitting_after_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()
    _write_pipeline_script(
        pipeline_dir / "01-recon.py",
        "\n".join(
            [
                "import time",
                "libname = sys.argv[1]",
                "marker = workdir.parent / f'{libname}.started'",
                "marker.write_text('started\\n', encoding='utf-8')",
                "if libname == 'libbar':",
                "    raise SystemExit(7)",
                "if libname == 'libbaz':",
                "    time.sleep(0.5)",
                "print(f'phase {libname} finished')",
                "(workdir / 'runs.log').write_text('01-recon\\n', encoding='utf-8')",
            ]
        ),
    )

    ports_dir = tmp_path / "ports"
    bar_workdir = ports_dir / "port-libbar"
    baz_workdir = ports_dir / "port-libbaz"
    foo_workdir = ports_dir / "port-libfoo"
    _init_workdir_repo(bar_workdir)
    _init_workdir_repo(baz_workdir)
    _init_workdir_repo(foo_workdir)

    monkeypatch.setattr(safelibs, "PIPELINE_DIR", os.fspath(pipeline_dir))
    monkeypatch.setattr(safelibs, "_list_github_port_repos", lambda *args, **kwargs: [])
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "safelibs.py",
            "port",
            "--ports-dir",
            os.fspath(ports_dir),
            "--no-auto-pull",
            "--jobs",
            "2",
        ],
    )

    with pytest.raises(SystemExit) as exc_info:
        safelibs.main()

    assert exc_info.value.code == 7
    captured = capsys.readouterr()
    assert "Round-robin target failed: port-libbar (libbar) exited with code 7." in captured.err
    assert (ports_dir / "libbar.started").exists()
    assert (ports_dir / "libbaz.started").exists()
    assert not (ports_dir / "libfoo.started").exists()
    assert not (foo_workdir / "runs.log").exists()
    assert "[libfoo] Running 01-recon.py" not in captured.out
    assert (baz_workdir / "runs.log").read_text(encoding="utf-8") == "01-recon\n"


def test_filter_upgradeable_skips_same_upstream_version(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()
    _write_pipeline_script(
        pipeline_dir / "01-recon.py",
        "(workdir / 'runs.log').write_text('ran\\n', encoding='utf-8')",
    )

    workdir = _managed_workdir(tmp_path)
    _write_changelog(workdir, "1.2.3-1ubuntu1")

    monkeypatch.setattr(safelibs, "PIPELINE_DIR", os.fspath(pipeline_dir))
    monkeypatch.setattr(safelibs, "_latest_ubuntu_package_version", lambda *args, **kwargs: "1.2.3-1ubuntu2")
    monkeypatch.setattr(sys, "argv", _safelibs_argv(workdir, "--filter-upgradeable"))

    safelibs.main()

    out = capsys.readouterr().out
    assert "Skipping libfoo; current upstream version is already latest" in out
    assert "current 1.2.3 (source package 1.2.3-1ubuntu1)" in out
    assert "latest 1.2.3 (source package 1.2.3-1ubuntu2)" in out
    assert not (workdir / "runs.log").exists()


def test_filter_upgradeable_runs_newer_upstream_version(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()
    _write_pipeline_script(
        pipeline_dir / "01-recon.py",
        "(workdir / 'runs.log').write_text('ran\\n', encoding='utf-8')",
    )

    workdir = _managed_workdir(tmp_path)
    _write_changelog(workdir, "1.2.3-1ubuntu1")

    monkeypatch.setattr(safelibs, "PIPELINE_DIR", os.fspath(pipeline_dir))
    monkeypatch.setattr(safelibs, "_latest_ubuntu_package_version", lambda *args, **kwargs: "1.2.4-1")
    monkeypatch.setattr(sys, "argv", _safelibs_argv(workdir, "--filter-upgradeable"))

    safelibs.main()

    out = capsys.readouterr().out
    assert "libfoo: new upstream version available" in out
    assert "1.2.3 (source package 1.2.3-1ubuntu1) -> 1.2.4 (source package 1.2.4-1)" in out
    assert "Running 01-recon.py" in out
    assert (workdir / "runs.log").read_text(encoding="utf-8") == "ran\n"


def test_upgradeability_status_uses_source_package_name_from_changelog(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workdir = _managed_workdir(tmp_path)
    _write_changelog(workdir, "1.2.3-1ubuntu1", package="glib2.0")
    observed: list[tuple[str, str | None]] = []

    def latest_version(
        libname: str,
        log_handle: object = None,
        source_package: str | None = None,
    ) -> str:
        observed.append((libname, source_package))
        return "1.2.4-1"

    monkeypatch.setattr(safelibs, "_latest_ubuntu_package_version", latest_version)

    status = safelibs._upgradeability_status(os.fspath(workdir), "glib")

    assert observed == [("glib", "glib2.0")]
    assert status["upgradeable"] is True
    assert status["current_source"] == "1.2.3-1ubuntu1"
    assert status["current_upstream"] == "1.2.3"
    assert status["latest_source"] == "1.2.4-1"
    assert status["latest_upstream"] == "1.2.4"


def test_upgradeability_status_prefers_original_dsc_over_safe_changelog(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workdir = _managed_workdir(tmp_path)
    _write_dsc(workdir, "libgcrypt20_1.10.3-2build1.dsc", "libgcrypt20", "1.10.3-2build1")
    _write_safe_changelog(workdir, "1.10.3+safe1", package="libgcrypt20")
    observed: list[tuple[str, str | None]] = []

    def latest_version(
        libname: str,
        log_handle: object = None,
        source_package: str | None = None,
    ) -> str:
        observed.append((libname, source_package))
        return "1.10.4-1"

    monkeypatch.setattr(safelibs, "_latest_ubuntu_package_version", latest_version)

    status = safelibs._upgradeability_status(os.fspath(workdir), "libgcrypt")

    assert observed == [("libgcrypt", "libgcrypt20")]
    assert status["upgradeable"] is True
    assert status["current_source"] == "1.10.3-2build1"
    assert status["current_upstream"] == "1.10.3"


def test_latest_ubuntu_package_version_falls_back_to_launchpad(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    requested_urls: list[str] = []

    def fake_run_optional_command(
        command: list[str],
        *,
        cwd: str,
        log_handle: object = None,
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(command, 0, "", "")

    class FakeResponse(io.BytesIO):
        def __enter__(self) -> "FakeResponse":
            return self

        def __exit__(self, *exc_info: object) -> bool:
            return False

    def fake_urlopen(url: str, timeout: int = 15) -> FakeResponse:
        requested_urls.append(url)
        payload = {
            "entries": [
                {"status": "Superseded", "source_package_version": "2.0.0-1"},
                {"status": "Published", "source_package_version": "1.2.4-1"},
                {"status": "Published", "source_package_version": "1.2.3-1"},
            ]
        }
        return FakeResponse(json.dumps(payload).encode("utf-8"))

    monkeypatch.setattr(safelibs, "_run_optional_command", fake_run_optional_command)
    monkeypatch.setattr(safelibs, "_ubuntu_series_codename", lambda log_handle=None: "noble")
    monkeypatch.setattr(safelibs.urllib.request, "urlopen", fake_urlopen)

    latest = safelibs._latest_ubuntu_package_version("libfoo", source_package="glib2.0")

    assert latest == "1.2.4-1"
    assert requested_urls
    assert "source_name=glib2.0" in requested_urls[0]


def test_filter_upgradeable_round_robin_only_runs_upgradeable_ports(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()
    _write_pipeline_script(
        pipeline_dir / "01-recon.py",
        "with (workdir / 'runs.log').open('a', encoding='utf-8') as handle:\n"
        "    handle.write('01-recon\\n')",
    )

    ports_dir = tmp_path / "ports"
    foo_workdir = ports_dir / "port-libfoo"
    bar_workdir = ports_dir / "port-libbar"
    _write_changelog(foo_workdir, "1.0.0-1", package="libfoo")
    _write_changelog(bar_workdir, "2.0.0-1", package="libbar")

    def latest_version(libname: str, log_handle: object = None) -> str:
        return {"libfoo": "1.1.0-1", "libbar": "2.0.0-2"}[libname]

    monkeypatch.setattr(safelibs, "PIPELINE_DIR", os.fspath(pipeline_dir))
    monkeypatch.setattr(safelibs, "_list_github_port_repos", lambda *args, **kwargs: [])
    monkeypatch.setattr(safelibs, "_latest_ubuntu_package_version", latest_version)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "safelibs.py",
            "port",
            "--ports-dir",
            os.fspath(ports_dir),
            "--no-auto-pull",
            "--filter-upgradeable",
        ],
    )

    safelibs.main()

    out = capsys.readouterr().out
    assert "Round-robin porting 2 known port(s)." in out
    assert "libfoo: new upstream version available" in out
    assert "Skipping libbar;" not in out
    assert "no new upstream version found" not in out
    assert "Upgradeable filter selected 1 of 2 known port(s); skipped 1." in out
    assert (foo_workdir / "runs.log").read_text(encoding="utf-8") == "01-recon\n"
    assert not (bar_workdir / "runs.log").exists()


def test_dry_run_filter_upgradeable_round_robin_reports_filter_decisions(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()
    _write_pipeline_script(
        pipeline_dir / "01-recon.py",
        "raise AssertionError('dry run should not execute phases')",
    )

    ports_dir = tmp_path / "ports"
    foo_workdir = ports_dir / "port-libfoo"
    bar_workdir = ports_dir / "port-libbar"
    _init_workdir_repo(foo_workdir)
    _init_workdir_repo(bar_workdir)
    _write_changelog(foo_workdir, "1.0.0-1", package="libfoo")
    _write_changelog(bar_workdir, "2.0.0-1", package="libbar")

    def latest_version(libname: str, log_handle: object = None) -> str:
        return {"libfoo": "1.1.0-1", "libbar": "2.0.0-2"}[libname]

    monkeypatch.setattr(safelibs, "PIPELINE_DIR", os.fspath(pipeline_dir))
    monkeypatch.setattr(safelibs, "_list_github_port_repos", lambda *args, **kwargs: [])
    monkeypatch.setattr(safelibs, "_latest_ubuntu_package_version", latest_version)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "safelibs.py",
            "port",
            "--ports-dir",
            os.fspath(ports_dir),
            "--dry-run",
            "--filter-upgradeable",
        ],
    )

    safelibs.main()

    out = capsys.readouterr().out
    assert "Round-robin porting 2 known port(s)." in out
    assert "libfoo: new upstream version available" in out
    assert "Skipping libbar; current upstream version is already latest" in out
    assert "Upgradeable filter selected 1 of 2 known port(s); skipped 1." in out
    assert "01-recon.py libfoo" in out
    assert "01-recon.py libbar" not in out


def test_filter_upgradeable_round_robin_reports_when_no_ports_match(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()
    _write_pipeline_script(
        pipeline_dir / "01-recon.py",
        "raise AssertionError('no upgradeable ports should run')",
    )

    ports_dir = tmp_path / "ports"
    foo_workdir = ports_dir / "port-libfoo"
    bar_workdir = ports_dir / "port-libbar"
    _write_changelog(foo_workdir, "1.0.0-1", package="libfoo")
    _write_changelog(bar_workdir, "2.0.0-1", package="libbar")

    def latest_version(libname: str, log_handle: object = None) -> str:
        return {"libfoo": "1.0.0-2", "libbar": "2.0.0-3"}[libname]

    monkeypatch.setattr(safelibs, "PIPELINE_DIR", os.fspath(pipeline_dir))
    monkeypatch.setattr(safelibs, "_list_github_port_repos", lambda *args, **kwargs: [])
    monkeypatch.setattr(safelibs, "_latest_ubuntu_package_version", latest_version)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "safelibs.py",
            "port",
            "--ports-dir",
            os.fspath(ports_dir),
            "--no-auto-pull",
            "--filter-upgradeable",
        ],
    )

    safelibs.main()

    out = capsys.readouterr().out
    assert "Round-robin porting 2 known port(s)." in out
    assert "No upgradeable ports found among 2 known port(s)." in out


def test_known_port_repos_excludes_template_repo(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ports_dir = tmp_path / "ports"
    (ports_dir / "port-libfoo").mkdir(parents=True)
    (ports_dir / "port-template").mkdir(parents=True)

    monkeypatch.setattr(
        safelibs,
        "_list_github_port_repos",
        lambda *args, **kwargs: [
            {"name": "port-libbar", "nameWithOwner": "safelibs/port-libbar"},
            {"name": "port-template", "nameWithOwner": "safelibs/port-template"},
        ],
    )

    repos = safelibs._known_port_repos(
        os.fspath(ports_dir),
        "safelibs",
        "port-",
    )

    assert [repo["name"] for repo in repos] == ["port-libbar", "port-libfoo"]


def test_filter_upgradeable_round_robin_reruns_upgrade_phase_for_completed_port(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()
    _write_pipeline_script(
        pipeline_dir / "04-test.py",
        "raise AssertionError('completed upgrade should restart at upgrade phase')",
    )
    _write_pipeline_script(
        pipeline_dir / "06-upgrade.py",
        "\n".join(
            [
                "observed = (workdir / 'state.txt').read_text(encoding='utf-8')",
                "(workdir / 'observed.txt').write_text(observed, encoding='utf-8')",
                "with (workdir / 'runs.log').open('a', encoding='utf-8') as handle:",
                "    handle.write('06-upgrade\\n')",
            ]
        ),
    )

    ports_dir = tmp_path / "ports"
    workdir = ports_dir / "port-libfoo"
    _init_workdir_repo(workdir)
    _write_changelog(workdir, "1.0.0-1")
    (workdir / "state.txt").write_text("initial\n", encoding="utf-8")
    _git(workdir, "add", "original", "state.txt")
    _git(workdir, "commit", "-m", "tested")
    _git(workdir, "tag", "libfoo/04-test")
    _commit_state(workdir, "upgraded", "upgraded")
    _git(workdir, "tag", "libfoo/06-upgrade")

    monkeypatch.setattr(safelibs, "PIPELINE_DIR", os.fspath(pipeline_dir))
    monkeypatch.setattr(safelibs, "_list_github_port_repos", lambda *args, **kwargs: [])
    monkeypatch.setattr(safelibs, "_latest_ubuntu_package_version", lambda *args, **kwargs: "1.1.0-1")
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "safelibs.py",
            "port",
            "--ports-dir",
            os.fspath(ports_dir),
            "--no-auto-pull",
            "--filter-upgradeable",
        ],
    )

    safelibs.main()

    out = capsys.readouterr().out
    assert "New upstream version found; re-running from 06-upgrade." in out
    assert "Scripts: ['06-upgrade.py']" in out
    assert (workdir / "runs.log").read_text(encoding="utf-8") == "06-upgrade\n"
    assert (workdir / "observed.txt").read_text(encoding="utf-8") == "upgraded\n"


def test_max_phases_continues_after_skipped_phase(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()
    _write_pipeline_script(
        pipeline_dir / "04-test.py",
        "raise AssertionError('resume should start after 04-test')",
    )
    _write_pipeline_script(
        pipeline_dir / "06-upgrade.py",
        "\n".join(
            [
                "with (workdir / 'runs.log').open('a', encoding='utf-8') as handle:",
                "    handle.write('06-upgrade\\n')",
                f"raise SystemExit({safelibs.PHASE_SKIPPED_EXIT_CODE})",
            ]
        ),
    )
    _write_pipeline_script(
        pipeline_dir / "07-document.py",
        "\n".join(
            [
                "with (workdir / 'runs.log').open('a', encoding='utf-8') as handle:",
                "    handle.write('07-document\\n')",
                "observed = (workdir / 'state.txt').read_text(encoding='utf-8')",
                "(workdir / 'observed.txt').write_text(observed, encoding='utf-8')",
            ]
        ),
    )

    ports_dir = tmp_path / "ports"
    workdir = ports_dir / "port-libfoo"
    _init_workdir_repo(workdir)
    _commit_state(workdir, "tested", "tested")
    _git(workdir, "tag", "libfoo/04-test")
    _commit_state(workdir, "head", "head")

    monkeypatch.setattr(safelibs, "PIPELINE_DIR", os.fspath(pipeline_dir))
    scripts, phases = safelibs._pipeline_scripts()
    args = argparse.Namespace(
        action="port",
        libname="libfoo",
        ports_dir=os.fspath(ports_dir),
        no_auto_pull=True,
        filter_upgradeable=False,
        dry_run=False,
        jobs=None,
        from_phase=None,
        from_last=True,
        github_repo=None,
        github_owner="safelibs",
        github_prefix="port-",
        create_github=False,
        github_visibility="private",
        push_github=False,
        github_remote="origin",
    )

    assert safelibs._run_port_one(
        args,
        scripts,
        phases,
        max_phases=1,
        skip_complete_reset=True,
    )

    out = capsys.readouterr().out
    assert "Resuming from: 06-upgrade" in out
    assert "Scripts: ['06-upgrade.py', '07-document.py']" in out
    assert "06-upgrade.py skipped itself; continuing because skipped phases do not count against the phase limit." in out
    assert (workdir / "runs.log").read_text(encoding="utf-8") == "06-upgrade\n07-document\n"
    assert (workdir / "observed.txt").read_text(encoding="utf-8") == "tested\n"

    tags = _git(workdir, "tag", "--list", "libfoo/*").stdout.splitlines()
    assert "libfoo/06-upgrade" in tags
    assert "libfoo/07-document" in tags


def test_find_last_tagged_phase_index_git_error_is_logged(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def fake_run(*args: object, **kwargs: object) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(args[0], 2, "", "tag failure\n")

    monkeypatch.setattr(safelibs.subprocess, "run", fake_run)
    log_handle = io.StringIO()

    with pytest.raises(SystemExit) as exc_info:
        safelibs._find_last_tagged_phase_index(
            "/tmp/workdir",
            "libfoo",
            ["01-recon"],
            log_handle=log_handle,
        )

    assert exc_info.value.code == 1
    captured = capsys.readouterr()
    assert "Failed to list reachable tags in /tmp/workdir: tag failure" in captured.err
    assert "Failed to list reachable tags in /tmp/workdir: tag failure" in log_handle.getvalue()


def test_workdir_has_git_history_git_error_is_logged(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(safelibs.os.path, "exists", lambda path: True)

    def fake_run(*args: object, **kwargs: object) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(args[0], 2, "", "history failure\n")

    monkeypatch.setattr(safelibs.subprocess, "run", fake_run)
    log_handle = io.StringIO()

    with pytest.raises(SystemExit) as exc_info:
        safelibs._workdir_has_git_history("/tmp/workdir", log_handle=log_handle)

    assert exc_info.value.code == 1
    captured = capsys.readouterr()
    assert "Failed to inspect git history in /tmp/workdir: history failure" in captured.err
    assert "Failed to inspect git history in /tmp/workdir: history failure" in log_handle.getvalue()


def test_reset_workdir_to_tag_checkout_failure_is_logged(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def fake_run(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        if command[:2] == ["git", "checkout"]:
            return subprocess.CompletedProcess(command, 1, "", "checkout failure\n")
        raise AssertionError(f"unexpected command: {command}")

    monkeypatch.setattr(safelibs.subprocess, "run", fake_run)
    log_handle = io.StringIO()

    with pytest.raises(SystemExit) as exc_info:
        safelibs._reset_workdir_to_tag("/tmp/workdir", "libfoo/01-recon", log_handle=log_handle)

    assert exc_info.value.code == 1
    captured = capsys.readouterr()
    assert "Failed to checkout tag libfoo/01-recon: checkout failure" in captured.err
    assert "Make sure a previous run completed that phase." in captured.err
    assert "Failed to checkout tag libfoo/01-recon: checkout failure" in log_handle.getvalue()


def test_reset_workdir_to_tag_reset_failure_is_logged(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def fake_run(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        if command[:2] == ["git", "checkout"]:
            return subprocess.CompletedProcess(command, 0, "", "")
        if command[:2] == ["git", "reset"]:
            return subprocess.CompletedProcess(command, 1, "", "reset failure\n")
        raise AssertionError(f"unexpected command: {command}")

    monkeypatch.setattr(safelibs.subprocess, "run", fake_run)
    log_handle = io.StringIO()

    with pytest.raises(SystemExit) as exc_info:
        safelibs._reset_workdir_to_tag("/tmp/workdir", "libfoo/01-recon", log_handle=log_handle)

    assert exc_info.value.code == 1
    captured = capsys.readouterr()
    assert "Failed to reset workdir to tag libfoo/01-recon: reset failure" in captured.err
    assert "Failed to reset workdir to tag libfoo/01-recon: reset failure" in log_handle.getvalue()


def test_from_last_resumes_after_latest_tag(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()
    _write_pipeline_script(
        pipeline_dir / "01-recon.py",
        "\n".join(
            [
                "with (workdir / 'runs.log').open('a', encoding='utf-8') as handle:",
                "    handle.write('01-recon\\n')",
            ]
        ),
    )
    _write_pipeline_script(
        pipeline_dir / "02-setup.py",
        "\n".join(
            [
                "with (workdir / 'runs.log').open('a', encoding='utf-8') as handle:",
                "    handle.write('02-setup\\n')",
            ]
        ),
    )
    _write_pipeline_script(
        pipeline_dir / "03-port.py",
        "\n".join(
            [
                "with (workdir / 'runs.log').open('a', encoding='utf-8') as handle:",
                "    handle.write('03-port\\n')",
                "observed = (workdir / 'state.txt').read_text(encoding='utf-8')",
                "(workdir / 'observed.txt').write_text(observed, encoding='utf-8')",
            ]
        ),
    )

    workdir = _managed_workdir(tmp_path)
    _init_workdir_repo(workdir)
    _commit_state(workdir, "phase1", "phase1")
    _git(workdir, "tag", "libfoo/01-recon")
    _commit_state(workdir, "phase2", "phase2")
    _git(workdir, "tag", "libfoo/02-setup")
    _commit_state(workdir, "head", "head")

    monkeypatch.setattr(safelibs, "PIPELINE_DIR", os.fspath(pipeline_dir))
    monkeypatch.setattr(sys, "argv", _safelibs_argv(workdir, "--from-last"))

    safelibs.main()

    out = capsys.readouterr().out
    assert "Resuming from: 03-port" in out
    assert (workdir / "runs.log").read_text(encoding="utf-8") == "03-port\n"
    assert (workdir / "observed.txt").read_text(encoding="utf-8") == "phase2\n"

    tags = _git(workdir, "tag", "--list", "libfoo/*").stdout.splitlines()
    assert "libfoo/03-port" in tags


def test_from_last_ignores_stale_later_tags_off_head(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()
    _write_pipeline_script(
        pipeline_dir / "01-recon.py",
        "\n".join(
            [
                "with (workdir / 'runs.log').open('a', encoding='utf-8') as handle:",
                "    handle.write('01-recon\\n')",
            ]
        ),
    )
    _write_pipeline_script(
        pipeline_dir / "02-setup.py",
        "\n".join(
            [
                "with (workdir / 'runs.log').open('a', encoding='utf-8') as handle:",
                "    handle.write('02-setup\\n')",
            ]
        ),
    )
    _write_pipeline_script(
        pipeline_dir / "03-port.py",
        "\n".join(
            [
                "with (workdir / 'runs.log').open('a', encoding='utf-8') as handle:",
                "    handle.write('03-port\\n')",
                "observed = (workdir / 'state.txt').read_text(encoding='utf-8')",
                "(workdir / 'observed.txt').write_text(observed, encoding='utf-8')",
            ]
        ),
    )

    workdir = _managed_workdir(tmp_path)
    _init_workdir_repo(workdir)
    _commit_state(workdir, "phase1", "phase1")
    _git(workdir, "tag", "libfoo/01-recon")
    _commit_state(workdir, "phase2", "phase2")
    _git(workdir, "tag", "libfoo/02-setup")
    _commit_state(workdir, "phase3", "phase3")
    _git(workdir, "tag", "libfoo/03-port")
    _git(workdir, "reset", "--hard", "libfoo/02-setup")

    monkeypatch.setattr(safelibs, "PIPELINE_DIR", os.fspath(pipeline_dir))
    monkeypatch.setattr(sys, "argv", _safelibs_argv(workdir, "--from-last"))

    safelibs.main()

    out = capsys.readouterr().out
    assert "Resuming from: 03-port" in out
    assert "no remaining phases to run." not in out
    assert (workdir / "runs.log").read_text(encoding="utf-8") == "03-port\n"
    assert (workdir / "observed.txt").read_text(encoding="utf-8") == "phase2\n"


def test_from_last_starts_from_beginning_without_matching_tags(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()
    _write_pipeline_script(
        pipeline_dir / "01-recon.py",
        "\n".join(
            [
                "with (workdir / 'runs.log').open('a', encoding='utf-8') as handle:",
                "    handle.write('01-recon\\n')",
            ]
        ),
    )

    workdir = _managed_workdir(tmp_path)
    _init_workdir_repo(workdir)
    _commit_state(workdir, "head", "head")

    monkeypatch.setattr(safelibs, "PIPELINE_DIR", os.fspath(pipeline_dir))
    monkeypatch.setattr(sys, "argv", _safelibs_argv(workdir, "--from-last"))

    safelibs.main()

    out = capsys.readouterr().out
    assert "No existing phase tags found for 'libfoo'; starting from beginning." in out
    assert (workdir / "runs.log").read_text(encoding="utf-8") == "01-recon\n"

    tags = _git(workdir, "tag", "--list", "libfoo/*").stdout.splitlines()
    assert "libfoo/01-recon" in tags


@pytest.mark.parametrize("precreate_workdir", [False, True], ids=["missing", "empty"])
def test_from_last_starts_from_beginning_with_fresh_workdir(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    precreate_workdir: bool,
) -> None:
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()
    _write_pipeline_script(
        pipeline_dir / "01-recon.py",
        "\n".join(
            [
                "import subprocess",
                "with (workdir / 'runs.log').open('a', encoding='utf-8') as handle:",
                "    handle.write('01-recon\\n')",
                "subprocess.run(['git', 'add', 'runs.log'], cwd=workdir, check=True)",
                "subprocess.run(['git', 'commit', '-m', '01-recon'], cwd=workdir, check=True)",
            ]
        ),
    )
    _write_pipeline_script(
        pipeline_dir / "02-setup.py",
        "\n".join(
            [
                "import subprocess",
                "with (workdir / 'runs.log').open('a', encoding='utf-8') as handle:",
                "    handle.write('02-setup\\n')",
                "subprocess.run(['git', 'add', 'runs.log'], cwd=workdir, check=True)",
                "subprocess.run(['git', 'commit', '-m', '02-setup'], cwd=workdir, check=True)",
            ]
        ),
    )

    workdir = _managed_workdir(tmp_path)
    if precreate_workdir:
        workdir.mkdir(parents=True)

    monkeypatch.setattr(safelibs, "PIPELINE_DIR", os.fspath(pipeline_dir))
    monkeypatch.setattr(sys, "argv", _safelibs_argv(workdir, "--from-last"))

    safelibs.main()

    out = capsys.readouterr().out
    assert "No existing phase tags found for 'libfoo'; starting from beginning." in out
    assert "Scripts: ['01-recon.py', '02-setup.py']" in out
    assert (workdir / "runs.log").read_text(encoding="utf-8") == "01-recon\n02-setup\n"

    tags = _git(workdir, "tag", "--list", "libfoo/*").stdout.splitlines()
    assert "libfoo/01-recon" in tags
    assert "libfoo/02-setup" in tags


def test_unknown_phase_is_reported_and_logged(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()
    _write_pipeline_script(
        pipeline_dir / "01-recon.py",
        "print('phase stdout')",
    )

    workdir = _managed_workdir(tmp_path)
    log_path = tmp_path / "pipeline.log"

    monkeypatch.setattr(safelibs, "PIPELINE_DIR", os.fspath(pipeline_dir))
    monkeypatch.setattr(
        sys,
        "argv",
        _safelibs_argv(workdir, "-L", os.fspath(log_path), "--from", "99-missing"),
    )

    with pytest.raises(SystemExit) as exc_info:
        safelibs.main()

    assert exc_info.value.code == 1
    captured = capsys.readouterr()
    assert "Unknown phase '99-missing'. Available: ['01-recon']" in captured.err
    assert "Unknown phase '99-missing'. Available: ['01-recon']" in log_path.read_text(encoding="utf-8")


def test_no_pipeline_scripts_is_reported_and_logged(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()
    log_path = tmp_path / "pipeline.log"

    monkeypatch.setattr(safelibs, "PIPELINE_DIR", os.fspath(pipeline_dir))
    monkeypatch.setattr(
        sys,
        "argv",
        _safelibs_argv(_managed_workdir(tmp_path), "-L", os.fspath(log_path)),
    )

    with pytest.raises(SystemExit) as exc_info:
        safelibs.main()

    assert exc_info.value.code == 1
    captured = capsys.readouterr()
    assert f"No pipeline scripts found in {pipeline_dir}" in captured.err
    assert f"No pipeline scripts found in {pipeline_dir}" in log_path.read_text(encoding="utf-8")


def test_log_file_defaults_to_tmp_path_with_epoch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()
    _write_pipeline_script(
        pipeline_dir / "01-recon.py",
        "\n".join(
            [
                "print('phase stdout')",
                "print('phase stderr', file=sys.stderr)",
            ]
        ),
    )

    workdir = _managed_workdir(tmp_path)
    log_path = tmp_path / "safelibs-libfoo-1700000000.log"

    monkeypatch.setattr(safelibs, "PIPELINE_DIR", os.fspath(pipeline_dir))
    monkeypatch.setattr(safelibs, "DEFAULT_LOG_DIR", os.fspath(tmp_path))
    monkeypatch.setattr(safelibs.time, "time", lambda: 1700000000)
    monkeypatch.setattr(sys, "argv", _safelibs_argv(workdir, "-L"))

    safelibs.main()

    assert log_path.read_text(encoding="utf-8").count(f"Log file: {log_path}") == 1
    log_text = log_path.read_text(encoding="utf-8")
    assert "Library: libfoo" in log_text
    assert "phase stdout\n" in log_text
    assert "phase stderr\n" in log_text
    assert "Tagged libfoo/01-recon" in log_text


def test_log_file_appends_existing_contents(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()
    _write_pipeline_script(
        pipeline_dir / "01-recon.py",
        "\n".join(
            [
                "print('phase stdout')",
                "print('phase stderr', file=sys.stderr)",
            ]
        ),
    )

    log_path = tmp_path / "pipeline.log"
    log_path.write_text("existing line\n", encoding="utf-8")

    monkeypatch.setattr(safelibs, "PIPELINE_DIR", os.fspath(pipeline_dir))

    first_workdir = _managed_workdir(tmp_path, "ports-1")
    monkeypatch.setattr(
        sys,
        "argv",
        _safelibs_argv(first_workdir, "-L", os.fspath(log_path)),
    )
    safelibs.main()

    second_workdir = _managed_workdir(tmp_path, "ports-2")
    monkeypatch.setattr(
        sys,
        "argv",
        _safelibs_argv(second_workdir, "-L", os.fspath(log_path)),
    )
    safelibs.main()

    log_text = log_path.read_text(encoding="utf-8")
    assert log_text.startswith("existing line\n")
    assert log_text.count("Library: libfoo") == 2
    assert log_text.count("Running 01-recon.py") == 2
    assert log_text.count("phase stdout\n") == 2
    assert log_text.count("phase stderr\n") == 2


def test_log_file_preserves_phase_stderr_stream(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()
    _write_pipeline_script(
        pipeline_dir / "01-recon.py",
        "\n".join(
            [
                "print('phase stdout')",
                "print('phase stderr', file=sys.stderr)",
            ]
        ),
    )

    workdir = _managed_workdir(tmp_path)
    log_path = tmp_path / "pipeline.log"

    monkeypatch.setattr(safelibs, "PIPELINE_DIR", os.fspath(pipeline_dir))
    monkeypatch.setattr(
        sys,
        "argv",
        _safelibs_argv(workdir, "-L", os.fspath(log_path)),
    )

    safelibs.main()

    captured = capsys.readouterr()
    assert "phase stdout\n" in captured.out
    assert "phase stderr\n" not in captured.out
    assert "phase stderr\n" in captured.err

    log_text = log_path.read_text(encoding="utf-8")
    assert "phase stdout\n" in log_text
    assert "phase stderr\n" in log_text


@pytest.mark.parametrize("use_log_file", [False, True], ids=["without-log-file", "with-log-file"])
def test_phase_launch_oserror_is_not_reported_as_log_open_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    use_log_file: bool,
) -> None:
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()
    script_path = pipeline_dir / "01-recon.py"
    script_path.write_text("#!/usr/bin/env python3\n", encoding="utf-8")
    script_path.chmod(0o644)

    workdir = _managed_workdir(tmp_path)

    monkeypatch.setattr(safelibs, "PIPELINE_DIR", os.fspath(pipeline_dir))
    argv = _safelibs_argv(workdir)
    log_path = tmp_path / "pipeline.log"
    if use_log_file:
        argv.extend(["-L", os.fspath(log_path)])
    monkeypatch.setattr(sys, "argv", argv)

    with pytest.raises(PermissionError):
        safelibs.main()

    captured = capsys.readouterr()
    assert "Failed to open log file" not in captured.err
    if use_log_file:
        assert log_path.exists()


def test_log_file_parent_setup_failure_is_reported_cleanly(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    blocking_parent = tmp_path / "existing-file"
    blocking_parent.write_text("not a directory\n", encoding="utf-8")
    log_path = blocking_parent / "sub.log"

    monkeypatch.setattr(sys, "argv", ["safelibs.py", "port", "libfoo", "-L", os.fspath(log_path)])

    with pytest.raises(SystemExit) as exc_info:
        safelibs.main()

    assert exc_info.value.code == 1
    captured = capsys.readouterr()
    assert f"Failed to open log file {log_path}" in captured.err


def test_phase_failure_exit_code_is_reported_and_logged(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()
    _write_pipeline_script(
        pipeline_dir / "01-recon.py",
        "\n".join(
            [
                "print('phase stdout')",
                "print('phase stderr', file=sys.stderr)",
                "raise SystemExit(7)",
            ]
        ),
    )

    workdir = _managed_workdir(tmp_path)
    log_path = tmp_path / "pipeline.log"

    monkeypatch.setattr(safelibs, "PIPELINE_DIR", os.fspath(pipeline_dir))
    monkeypatch.setattr(
        sys,
        "argv",
        _safelibs_argv(workdir, "-L", os.fspath(log_path)),
    )

    with pytest.raises(SystemExit) as exc_info:
        safelibs.main()

    assert exc_info.value.code == 7
    captured = capsys.readouterr()
    assert "01-recon.py failed with exit code 7" in captured.err
    log_text = log_path.read_text(encoding="utf-8")
    assert "phase stdout\n" in log_text
    assert "phase stderr\n" in log_text
    assert "01-recon.py failed with exit code 7" in log_text


def test_log_file_preserves_phase_stderr_stream_in_standalone_process(
    tmp_path: Path,
) -> None:
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()
    _write_pipeline_script(
        pipeline_dir / "01-recon.py",
        "\n".join(
            [
                "print('phase stdout')",
                "print('phase stderr', file=sys.stderr)",
            ]
        ),
    )

    workdir = _managed_workdir(tmp_path)
    log_path = tmp_path / "pipeline.log"
    result = _run_safelibs_subprocess(
        pipeline_dir,
        workdir,
        "-L",
        os.fspath(log_path),
    )

    assert result.returncode == 0
    assert "phase stdout\n" in result.stdout
    assert "phase stderr\n" not in result.stdout
    assert "phase stderr\n" in result.stderr

    log_text = log_path.read_text(encoding="utf-8")
    assert "phase stdout\n" in log_text
    assert "phase stderr\n" in log_text


def test_log_file_handles_partial_line_mixed_stream_output_without_hanging(
    tmp_path: Path,
) -> None:
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()
    _write_pipeline_script(
        pipeline_dir / "01-recon.py",
        "\n".join(
            [
                "sys.stdout.write('partial stdout')",
                "sys.stdout.flush()",
                "sys.stderr.write('stderr-start:' + ('x' * 131072))",
                "sys.stderr.flush()",
                "sys.stdout.write(' done\\n')",
                "sys.stdout.flush()",
            ]
        ),
    )

    workdir = _managed_workdir(tmp_path)
    log_path = tmp_path / "pipeline.log"
    result = _run_safelibs_subprocess(
        pipeline_dir,
        workdir,
        "-L",
        os.fspath(log_path),
    )

    assert result.returncode == 0
    assert "partial stdout done\n" in result.stdout
    assert "stderr-start:" in result.stderr

    log_text = log_path.read_text(encoding="utf-8")
    assert "partial stdout" in log_text
    assert " done\n" in log_text
    assert "stderr-start:" in log_text


def test_log_file_streams_phase_output_before_phase_exit(
    tmp_path: Path,
) -> None:
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()
    _write_pipeline_script(
        pipeline_dir / "01-recon.py",
        "\n".join(
            [
                "import time",
                "print('early output')",
                "time.sleep(1.5)",
                "print('late output')",
            ]
        ),
    )

    workdir = _managed_workdir(tmp_path)
    log_path = tmp_path / "pipeline.log"
    command = _safelibs_subprocess_command(
        pipeline_dir,
        workdir,
        "-L",
        os.fspath(log_path),
    )

    with subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    ) as process:
        saw_early_output = False
        deadline = time.time() + 3
        while time.time() < deadline:
            if log_path.exists():
                log_text = log_path.read_text(encoding="utf-8")
                if "early output\n" in log_text:
                    saw_early_output = True
                    assert process.poll() is None
                    break
            if process.poll() is not None:
                break
            time.sleep(0.1)

        stdout, stderr = process.communicate(timeout=5)

    assert process.returncode == 0, (stdout, stderr)
    assert saw_early_output, (stdout, stderr, log_path.read_text(encoding="utf-8"))
    final_log_text = log_path.read_text(encoding="utf-8")
    assert "late output\n" in final_log_text


def test_tag_failure_is_reported_and_logged(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()
    _write_pipeline_script(
        pipeline_dir / "01-recon.py",
        "\n".join(
            [
                "print('phase stdout')",
            ]
        ),
    )

    real_run = safelibs.subprocess.run

    def fake_run(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        if command[:2] == ["git", "tag"]:
            return subprocess.CompletedProcess(command, 1, "", "tag failure\n")
        return real_run(command, **kwargs)

    workdir = _managed_workdir(tmp_path)
    log_path = tmp_path / "pipeline.log"

    monkeypatch.setattr(safelibs, "PIPELINE_DIR", os.fspath(pipeline_dir))
    monkeypatch.setattr(safelibs.subprocess, "run", fake_run)
    monkeypatch.setattr(
        sys,
        "argv",
        _safelibs_argv(workdir, "-L", os.fspath(log_path)),
    )

    with pytest.raises(SystemExit) as exc_info:
        safelibs.main()

    assert exc_info.value.code == 1
    captured = capsys.readouterr()
    assert "Failed to tag libfoo/01-recon: tag failure" in captured.err
    assert "Failed to tag libfoo/01-recon: tag failure" in log_path.read_text(encoding="utf-8")


def test_from_last_noops_when_latest_tag_is_final_phase(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()
    _write_pipeline_script(
        pipeline_dir / "01-recon.py",
        "\n".join(
            [
                "with (workdir / 'runs.log').open('a', encoding='utf-8') as handle:",
                "    handle.write('01-recon\\n')",
            ]
        ),
    )
    _write_pipeline_script(
        pipeline_dir / "02-setup.py",
        "\n".join(
            [
                "with (workdir / 'runs.log').open('a', encoding='utf-8') as handle:",
                "    handle.write('02-setup\\n')",
            ]
        ),
    )
    _write_pipeline_script(
        pipeline_dir / "03-port.py",
        "\n".join(
            [
                "with (workdir / 'runs.log').open('a', encoding='utf-8') as handle:",
                "    handle.write('03-port\\n')",
            ]
        ),
    )

    workdir = _managed_workdir(tmp_path)
    _init_workdir_repo(workdir)
    _commit_state(workdir, "phase1", "phase1")
    _git(workdir, "tag", "libfoo/01-recon")
    _commit_state(workdir, "phase2", "phase2")
    _git(workdir, "tag", "libfoo/02-setup")
    _commit_state(workdir, "phase3", "phase3")
    _git(workdir, "tag", "libfoo/03-port")
    _commit_state(workdir, "head", "head")

    monkeypatch.setattr(safelibs, "PIPELINE_DIR", os.fspath(pipeline_dir))
    monkeypatch.setattr(sys, "argv", _safelibs_argv(workdir, "--from-last"))

    safelibs.main()

    out = capsys.readouterr().out
    assert "no remaining phases to run." in out
    assert "Scripts: []" in out
    assert (workdir / "state.txt").read_text(encoding="utf-8") == "phase3\n"
    assert not (workdir / "runs.log").exists()


def _write_phase_script_records_state(pipeline_dir: Path, phase: str) -> None:
    _write_pipeline_script(
        pipeline_dir / f"{phase}.py",
        "\n".join(
            [
                "with (workdir / 'runs.log').open('a', encoding='utf-8') as handle:",
                f"    handle.write('{phase}\\n')",
                "observed = (workdir / 'state.txt').read_text(encoding='utf-8')",
                f"(workdir / '{phase}-observed.txt').write_text(observed, encoding='utf-8')",
            ]
        ),
    )


def test_continue_resumes_from_head_without_resetting(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()
    for phase in ("01-recon", "02-setup", "03-port", "04-test"):
        _write_phase_script_records_state(pipeline_dir, phase)

    workdir = _managed_workdir(tmp_path)
    _init_workdir_repo(workdir)
    _commit_state(workdir, "phase1", "phase1")
    _git(workdir, "tag", "libfoo/01-recon")
    _commit_state(workdir, "phase2", "phase2")
    _git(workdir, "tag", "libfoo/02-setup")
    _commit_state(workdir, "phase3", "phase3")
    _git(workdir, "tag", "libfoo/03-port")
    # Local edit on top of 03-port that --from-last would discard.
    _commit_state(workdir, "local-tweak", "local-tweak")
    head_before = _git(workdir, "rev-parse", "HEAD").stdout.strip()

    monkeypatch.setattr(safelibs, "PIPELINE_DIR", os.fspath(pipeline_dir))
    monkeypatch.setattr(sys, "argv", _safelibs_argv(workdir, "--continue"))

    safelibs.main()

    out = capsys.readouterr().out
    assert "continuing from HEAD with 04-test" in out
    assert (workdir / "runs.log").read_text(encoding="utf-8") == "04-test\n"
    # Phase saw the local-tweak state, NOT phase3 -- proves no reset happened.
    assert (workdir / "04-test-observed.txt").read_text(encoding="utf-8") == "local-tweak\n"

    head_after = _git(workdir, "rev-parse", "HEAD").stdout.strip()
    # The 04-test tag should land on HEAD (which is at-or-after head_before).
    tag_commit = _git(workdir, "rev-parse", "libfoo/04-test").stdout.strip()
    assert tag_commit == head_after
    # The local-tweak commit must still be reachable from HEAD.
    _git(workdir, "merge-base", "--is-ancestor", head_before, head_after)


def test_continue_starts_from_beginning_without_matching_tags(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()
    _write_phase_script_records_state(pipeline_dir, "01-recon")

    workdir = _managed_workdir(tmp_path)
    _init_workdir_repo(workdir)
    _commit_state(workdir, "head", "head")

    monkeypatch.setattr(safelibs, "PIPELINE_DIR", os.fspath(pipeline_dir))
    monkeypatch.setattr(sys, "argv", _safelibs_argv(workdir, "--continue"))

    safelibs.main()

    out = capsys.readouterr().out
    assert "No phase tags reachable from HEAD for 'libfoo'" in out
    assert (workdir / "runs.log").read_text(encoding="utf-8") == "01-recon\n"


def test_continue_noops_when_latest_tag_is_final_phase(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()
    for phase in ("01-recon", "02-setup"):
        _write_phase_script_records_state(pipeline_dir, phase)

    workdir = _managed_workdir(tmp_path)
    _init_workdir_repo(workdir)
    _commit_state(workdir, "phase1", "phase1")
    _git(workdir, "tag", "libfoo/01-recon")
    _commit_state(workdir, "phase2", "phase2")
    _git(workdir, "tag", "libfoo/02-setup")

    monkeypatch.setattr(safelibs, "PIPELINE_DIR", os.fspath(pipeline_dir))
    monkeypatch.setattr(sys, "argv", _safelibs_argv(workdir, "--continue"))

    safelibs.main()

    out = capsys.readouterr().out
    assert "no remaining phases to run." in out
    assert "Scripts: []" in out
    assert not (workdir / "runs.log").exists()


def test_do_phase_reruns_specified_phase_on_head(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()
    for phase in ("01-recon", "02-setup", "03-port", "04-test"):
        _write_phase_script_records_state(pipeline_dir, phase)

    workdir = _managed_workdir(tmp_path)
    _init_workdir_repo(workdir)
    _commit_state(workdir, "phase1", "phase1")
    _git(workdir, "tag", "libfoo/01-recon")
    _commit_state(workdir, "phase2", "phase2")
    _git(workdir, "tag", "libfoo/02-setup")
    _commit_state(workdir, "phase3", "phase3")
    _git(workdir, "tag", "libfoo/03-port")
    _commit_state(workdir, "phase4", "phase4")
    _git(workdir, "tag", "libfoo/04-test")
    _commit_state(workdir, "head", "head")
    head_before = _git(workdir, "rev-parse", "HEAD").stdout.strip()

    monkeypatch.setattr(safelibs, "PIPELINE_DIR", os.fspath(pipeline_dir))
    monkeypatch.setattr(
        sys, "argv", _safelibs_argv(workdir, "--do-phase", "03-port")
    )

    safelibs.main()

    out = capsys.readouterr().out
    assert "--do-phase: re-running 03-port on HEAD without reset." in out
    # Only 03-port ran, exactly once -- max_phases caps it at one phase.
    assert (workdir / "runs.log").read_text(encoding="utf-8") == "03-port\n"
    # Phase saw HEAD state (not phase2 or phase3 from the tags) -- no reset.
    assert (workdir / "03-port-observed.txt").read_text(encoding="utf-8") == "head\n"

    head_after = _git(workdir, "rev-parse", "HEAD").stdout.strip()
    assert head_after == head_before
    # Tag now points at HEAD.
    assert _git(workdir, "rev-parse", "libfoo/03-port").stdout.strip() == head_after
    # 04-test tag is unchanged and still exists (we never reset away from HEAD).
    _git(workdir, "rev-parse", "libfoo/04-test")


def test_do_phase_rejects_unknown_phase(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()
    _write_phase_script_records_state(pipeline_dir, "01-recon")

    workdir = _managed_workdir(tmp_path)
    _init_workdir_repo(workdir)
    _commit_state(workdir, "head", "head")

    monkeypatch.setattr(safelibs, "PIPELINE_DIR", os.fspath(pipeline_dir))
    monkeypatch.setattr(
        sys, "argv", _safelibs_argv(workdir, "--do-phase", "99-bogus")
    )

    with pytest.raises(SystemExit) as exc_info:
        safelibs.main()
    assert exc_info.value.code == 1
    err = capsys.readouterr().err
    assert "Unknown phase '99-bogus'" in err


def test_continue_and_do_phase_are_mutually_exclusive(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    workdir = _managed_workdir(tmp_path)

    monkeypatch.setattr(
        sys,
        "argv",
        _safelibs_argv(workdir, "--continue", "--do-phase", "02-setup"),
    )

    with pytest.raises(SystemExit):
        safelibs.main()
    err = capsys.readouterr().err
    assert "not allowed with argument" in err


def test_continue_works_in_round_robin_with_jobs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()
    for phase in ("01-recon", "02-setup", "03-port"):
        _write_phase_script_records_state(pipeline_dir, phase)

    ports_dir = tmp_path / "ports"
    foo_workdir = ports_dir / "port-libfoo"
    bar_workdir = ports_dir / "port-libbar"

    _init_workdir_repo(foo_workdir)
    _commit_state(foo_workdir, "phase1", "phase1")
    _git(foo_workdir, "tag", "libfoo/01-recon")
    _commit_state(foo_workdir, "foo-tweak", "foo-tweak")

    _init_workdir_repo(bar_workdir)
    _commit_state(bar_workdir, "phase1", "phase1")
    _git(bar_workdir, "tag", "libbar/01-recon")
    _commit_state(bar_workdir, "phase2", "phase2")
    _git(bar_workdir, "tag", "libbar/02-setup")
    _commit_state(bar_workdir, "bar-tweak", "bar-tweak")

    monkeypatch.setattr(safelibs, "PIPELINE_DIR", os.fspath(pipeline_dir))
    monkeypatch.setattr(safelibs, "_list_github_port_repos", lambda *a, **k: [])
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "safelibs.py",
            "port",
            "--ports-dir",
            os.fspath(ports_dir),
            "--no-auto-pull",
            "--continue",
            "--jobs",
            "2",
        ],
    )

    safelibs.main()

    # Each port advances by exactly one phase (round-robin semantics) and sees
    # its local tweak as state, proving HEAD was not reset.
    assert (foo_workdir / "runs.log").read_text(encoding="utf-8") == "02-setup\n"
    assert (foo_workdir / "02-setup-observed.txt").read_text(encoding="utf-8") == "foo-tweak\n"
    assert (bar_workdir / "runs.log").read_text(encoding="utf-8") == "03-port\n"
    assert (bar_workdir / "03-port-observed.txt").read_text(encoding="utf-8") == "bar-tweak\n"


def test_do_phase_works_in_round_robin(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()
    for phase in ("01-recon", "02-setup", "03-port"):
        _write_phase_script_records_state(pipeline_dir, phase)

    ports_dir = tmp_path / "ports"
    foo_workdir = ports_dir / "port-libfoo"
    bar_workdir = ports_dir / "port-libbar"

    for repo, libname in ((foo_workdir, "libfoo"), (bar_workdir, "libbar")):
        _init_workdir_repo(repo)
        _commit_state(repo, "phase1", "phase1")
        _git(repo, "tag", f"{libname}/01-recon")
        _commit_state(repo, "phase2", "phase2")
        _git(repo, "tag", f"{libname}/02-setup")
        _commit_state(repo, "head", "head")

    monkeypatch.setattr(safelibs, "PIPELINE_DIR", os.fspath(pipeline_dir))
    monkeypatch.setattr(safelibs, "_list_github_port_repos", lambda *a, **k: [])
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "safelibs.py",
            "port",
            "--ports-dir",
            os.fspath(ports_dir),
            "--no-auto-pull",
            "--do-phase",
            "02-setup",
        ],
    )

    safelibs.main()

    # Both ports re-ran 02-setup on HEAD even though they were already past it.
    for repo in (foo_workdir, bar_workdir):
        assert (repo / "runs.log").read_text(encoding="utf-8") == "02-setup\n"
        assert (repo / "02-setup-observed.txt").read_text(encoding="utf-8") == "head\n"
