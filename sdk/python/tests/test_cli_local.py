import tempfile
from pathlib import Path
from textwrap import dedent

from feast.feature_store import FeatureStore
from tests.cli_utils import CliRunner
from tests.online_read_write_test import basic_rw_test


def test_workflow() -> None:
    """
    Test running apply on a sample repo, and make sure the infra gets created.
    """
    runner = CliRunner()
    with tempfile.TemporaryDirectory() as repo_dir_name, tempfile.TemporaryDirectory() as data_dir_name:

        # Construct an example repo in a temporary dir
        repo_path = Path(repo_dir_name)
        data_path = Path(data_dir_name)

        repo_config = repo_path / "feature_store.yaml"

        repo_config.write_text(
            dedent(
                f"""
        project: foo
        registry: {data_path / "registry.db"}
        provider: local
        online_store:
            path: {data_path / "online_store.db"}
        """
            )
        )

        repo_example = repo_path / "example.py"
        repo_example.write_text(
            (Path(__file__).parent / "example_feature_repo_1.py").read_text()
        )

        result = runner.run(["apply"], cwd=repo_path)
        assert result.returncode == 0

        # entity & feature view list commands should succeed
        result = runner.run(["entities", "list"], cwd=repo_path)
        assert result.returncode == 0
        result = runner.run(["feature-views", "list"], cwd=repo_path)
        assert result.returncode == 0

        # entity & feature view describe commands should succeed when objects exist
        result = runner.run(["entities", "describe", "driver"], cwd=repo_path)
        assert result.returncode == 0
        result = runner.run(
            ["feature-views", "describe", "driver_locations"], cwd=repo_path
        )
        assert result.returncode == 0

        # entity & feature view describe commands should fail when objects don't exist
        result = runner.run(["entities", "describe", "foo"], cwd=repo_path)
        assert result.returncode == 1
        result = runner.run(["feature-views", "describe", "foo"], cwd=repo_path)
        assert result.returncode == 1

        # Doing another apply should be a no op, and should not cause errors
        result = runner.run(["apply"], cwd=repo_path)
        assert result.returncode == 0

        basic_rw_test(
            FeatureStore(repo_path=str(repo_path), config=None),
            view_name="driver_locations",
        )

        result = runner.run(["teardown"], cwd=repo_path)
        assert result.returncode == 0


def test_non_local_feature_repo() -> None:
    """
    Test running apply on a sample repo, and make sure the infra gets created.
    """
    runner = CliRunner()
    with tempfile.TemporaryDirectory() as repo_dir_name:

        # Construct an example repo in a temporary dir
        repo_path = Path(repo_dir_name)

        repo_config = repo_path / "feature_store.yaml"

        repo_config.write_text(
            dedent(
                """
        project: foo
        registry: data/registry.db
        provider: local
        online_store:
            path: data/online_store.db
        """
            )
        )

        repo_example = repo_path / "example.py"
        repo_example.write_text(
            (Path(__file__).parent / "example_feature_repo_1.py").read_text()
        )

        result = runner.run(["apply"], cwd=repo_path)
        assert result.returncode == 0

        fs = FeatureStore(repo_path=str(repo_path))
        assert len(fs.list_feature_views()) == 3

        result = runner.run(["teardown"], cwd=repo_path)
        assert result.returncode == 0
