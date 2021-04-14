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

        # Doing another apply should be a no op, and should not cause errors
        result = runner.run(["apply"], cwd=repo_path)
        assert result.returncode == 0

        basic_rw_test(
            FeatureStore(repo_path=str(repo_path), config=None),
            view_name="driver_locations",
        )

        result = runner.run(["teardown"], cwd=repo_path)
        assert result.returncode == 0
