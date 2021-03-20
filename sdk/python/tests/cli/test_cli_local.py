import tempfile
from pathlib import Path
from textwrap import dedent

from tests.cli.online_read_write_test import basic_rw_test
from tests.cli.utils import CliRunner


class TestCliLocal:
    def test_basic(self) -> None:
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
            metadata_store: {data_path / "metadata.db"}
            provider: local
            online_store:
                local:
                    path: {data_path / "online_store.db"}
            """
                )
            )

            repo_example = repo_path / "example.py"
            repo_example.write_text(
                (Path(__file__).parent / "example_feature_repo_1.py").read_text()
            )

            result = runner.run(["apply", str(repo_path)], cwd=repo_path)
            assert result.returncode == 0

            # Doing another apply should be a no op, and should not cause errors
            result = runner.run(["apply", str(repo_path)], cwd=repo_path)
            assert result.returncode == 0

            basic_rw_test(repo_path, "foo")

            result = runner.run(["teardown", str(repo_path)], cwd=repo_path)
            assert result.returncode == 0
