import os
import subprocess
import sys
from pathlib import Path

import pytest


def _script_path() -> Path:
    return (
        Path(__file__).resolve().parents[6]
        / "infra"
        / "scripts"
        / "chronon"
        / "start-local-chronon-service.sh"
    )


def test_start_script_documents_chronon_repo_override():
    script = _script_path().read_text()

    assert "CHRONON_REPO" in script
    assert "CHRONON_PREFLIGHT_ONLY" in script


def test_start_script_can_fail_before_docker_when_repo_missing(tmp_path: Path):
    missing_repo = tmp_path / "missing-chronon"
    result = subprocess.run(
        ["bash", str(_script_path())],
        env={
            **os.environ,
            "CHRONON_PREFLIGHT_ONLY": "1",
            "CHRONON_REPO": str(missing_repo),
        },
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 1
    assert str(missing_repo) in result.stderr
    assert "CHRONON_REPO" in result.stderr
    assert "git clone" in result.stderr


@pytest.mark.parametrize("loader_fails", [False, True])
def test_start_script_waits_for_completed_data_load(
    tmp_path: Path, loader_fails: bool
) -> None:
    chronon_repo = tmp_path / "chronon"
    jar = (
        chronon_repo
        / "quickstart/mongo-online-impl/target/scala-2.12/mongo-online-impl-assembly-0.1.0-SNAPSHOT.jar"
    )
    jar.parent.mkdir(parents=True)
    jar.touch()
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    docker = bin_dir / "docker"
    # Model a fresh container: Spark is initialized before the loader has
    # completed. Uploading during that interval must not be allowed.
    docker.write_text(
        f"#!{sys.executable}\n"
        """
import os
import sys
from pathlib import Path

args = sys.argv[1:]
state = Path(os.environ['TEST_DOCKER_STATE'])
failed = os.environ['TEST_LOADER_FAILS'] == '1'
if args[0] == 'run' and 'chronon-main' in args:
    state.write_text('0')
elif args[:2] == ['logs', 'chronon-main']:
    print("Spark session available as 'spark'.")
elif args[:3] == ['exec', 'chronon-main', 'test']:
    polls = int(state.read_text()) + 1
    state.write_text(str(polls))
    sys.exit(0 if polls >= 2 and not failed else 1)
elif args[0] == 'inspect':
    print('false' if failed else 'true')
elif args[:3] == ['exec', 'chronon-main', 'bash']:
    if failed or int(state.read_text()) < 2:
        print('Upload started before successful data load', file=sys.stderr)
        sys.exit(17)
    state.with_suffix('.uploaded').touch()
"""
    )
    docker.chmod(0o755)
    for command in ["sleep", "curl"]:
        executable = bin_dir / command
        executable.write_text("#!/bin/sh\nexit 0\n")
        executable.chmod(0o755)
    state = tmp_path / "docker-state"
    result = subprocess.run(
        ["bash", str(_script_path())],
        env={
            **os.environ,
            "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
            "CHRONON_REPO": str(chronon_repo),
            "CHRONON_SERVICE_JAR": str(jar),
            "CHRONON_SERVICE_PID_FILE": str(tmp_path / "service.pid"),
            "CHRONON_PREFLIGHT_ONLY": "0",
            "PYTHON_BIN": "/usr/bin/true",
            "TEST_DOCKER_STATE": str(state),
            "TEST_LOADER_FAILS": "1" if loader_fails else "0",
        },
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )
    if loader_fails:
        assert result.returncode == 1
        assert not state.with_suffix(".uploaded").exists()
        assert "data loader exited" in result.stderr
    else:
        assert result.returncode == 0, result.stderr
        assert state.with_suffix(".uploaded").exists()
        assert "CHRONON_SERVICE_URL=" in result.stdout
