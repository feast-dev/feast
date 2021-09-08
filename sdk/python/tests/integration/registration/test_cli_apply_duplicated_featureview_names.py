import tempfile
from pathlib import Path
from textwrap import dedent

from tests.utils.cli_utils import CliRunner, get_example_repo


def test_cli_apply_duplicated_featureview_names() -> None:
    """
    Test apply feature views with duplicated names and single py file in a feature repo using CLI
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
            and b"Please ensure that all feature view names are case-insensitively unique"
            in output
        )


def test_cli_apply_duplicated_featureview_names_multiple_py_files() -> None:
    """
    Test apply feature views with duplicated names from multiple py files in a feature repo using CLI
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
        # Create multiple py files containing the same feature view name
        for i in range(3):
            repo_example = repo_path / f"example{i}.py"
            repo_example.write_text(get_example_repo("example_feature_repo_2.py"))
        rc, output = runner.run_with_output(["apply"], cwd=repo_path)

        assert (
            rc != 0
            and b"Please ensure that all feature view names are case-insensitively unique"
            in output
        )
