import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from textwrap import dedent

from tests.utils.cli_utils import CliRunner


def test_repo_init() -> None:
    """
    This test simply makes sure that you can run `feast apply && feast materialize` on
    the repo created by "feast init" without errors.
    """
    runner = CliRunner()
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        result = runner.run(["init", "my_project"], cwd=temp_path)
        repo_path = temp_path / "my_project"
        assert result.returncode == 0
        result = runner.run(["apply"], cwd=repo_path)
        assert result.returncode == 0

        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=100)
        result = runner.run(
            ["materialize", start_date.isoformat(), end_date.isoformat()], cwd=repo_path
        )
        assert result.returncode == 0


def test_repo_init_with_underscore_in_project_name() -> None:
    """
    Test `feast init` with underscore in the project name
    """
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        runner = CliRunner()

        # `feast init` should fail with repo names start with underscore
        invalid_repo_names = ["_test", "_test_1"]
        for repo_name in invalid_repo_names:
            result = runner.run(["init", repo_name], cwd=temp_path)
            assert result.returncode != 0

        # `feast init` should succeed with underscore in repo name
        valid_repo_names = ["test_1"]
        for repo_name in valid_repo_names:
            result = runner.run(["init", repo_name], cwd=temp_path)
            assert result.returncode == 0

        # `feast apply` should fail with underscore in project name
        project_name = "test_1"
        repo_dir = temp_path / project_name
        data_dir = repo_dir / "data"
        repo_config = repo_dir / "feature_store.yaml"
        repo_config.write_text(
            dedent(
                f"""
        project: __foo
        registry: {data_dir / "registry.db"}
        provider: local
        online_store:
            path: {data_dir / "online_store.db"}
        """
            )
        )
        result = runner.run(["apply"], cwd=repo_dir)
        assert result.returncode != 0
