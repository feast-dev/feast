import tempfile
from datetime import timedelta
from pathlib import Path

from feast.utils import _utc_now
from tests.utils.cli_repo_creator import CliRunner


def test_cli_chdir() -> None:
    """
    This test simply makes sure that you can run 'feast --chdir COMMAND'
    to switch to a feature repository before running a COMMAND.
    """
    runner = CliRunner()
    with tempfile.TemporaryDirectory() as temp_dir:
        # Make sure the path is absolute by resolving any symlinks
        temp_path = Path(temp_dir).resolve()
        result = runner.run(["init", "my_project"], cwd=temp_path)
        repo_path = str(temp_path / "my_project" / "feature_repo")
        assert result.returncode == 0

        result = runner.run(["--chdir", repo_path, "apply"], cwd=temp_path)
        assert result.returncode == 0

        result = runner.run(["--chdir", repo_path, "entities", "list"], cwd=temp_path)
        assert result.returncode == 0

        result = runner.run(
            ["--chdir", repo_path, "feature-views", "list"], cwd=temp_path
        )
        assert result.returncode == 0

        end_date = _utc_now()
        start_date = end_date - timedelta(days=100)
        result = runner.run(
            [
                "--chdir",
                repo_path,
                "materialize",
                start_date.isoformat(),
                end_date.isoformat(),
            ],
            cwd=temp_path,
        )
        assert result.returncode == 0

        result = runner.run(
            [
                "--chdir",
                repo_path,
                "materialize-incremental",
                end_date.isoformat(),
            ],
            cwd=temp_path,
        )
        assert result.returncode == 0

        result = runner.run(["--chdir", repo_path, "registry-dump"], cwd=temp_path)
        assert result.returncode == 0

        result = runner.run(["--chdir", repo_path, "teardown"], cwd=temp_path)
        assert result.returncode == 0
