import os
import subprocess
from pathlib import Path


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
