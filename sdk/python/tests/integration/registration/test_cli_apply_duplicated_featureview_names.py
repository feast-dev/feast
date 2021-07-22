import tempfile
from pathlib import Path
from textwrap import dedent

from tests.utils.cli_utils import CliRunner, get_example_repo


def test_cli_apply_duplicated_featureview_names() -> None:
    """
    Test apply feature views with duplicated names in a feature repo using CLI
    """

    with tempfile.TemporaryDirectory() as repo_dir_name, tempfile.TemporaryDirectory() as data_dir_name:
        runner = CliRunner()
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
            get_example_repo(
                "example_feature_repo_with_duplicated_featureview_names.py"
            )
        )
        rc, output = runner.run_with_output(["apply"], cwd=repo_path)

        assert (
            rc != 0
            and b"Please ensure that all feature view names are unique" in output
        )
