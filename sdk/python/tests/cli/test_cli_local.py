import subprocess
import sys
import tempfile
from pathlib import Path
from textwrap import dedent
from typing import List

from feast import cli


class CliRunner:
    """
    NB. We can't use test runner helper from click here, since it doesn't start a new Python
    interpreter. And we need a new interpreter for each test since we dynamically import
    modules from the feature repo, and it is hard to clean up that state otherwise.
    """

    def run(self, args: List[str], cwd: Path) -> subprocess.CompletedProcess:
        return subprocess.run([sys.executable, cli.__file__] + args, cwd=cwd)


class TestCliLocal:

    def test_basic(self) -> None:
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as repo_dir_name, tempfile.TemporaryDirectory() as data_dir_name:

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

            result = runner.run(["teardown", str(repo_path)], cwd=repo_path)
            assert result.returncode == 0
