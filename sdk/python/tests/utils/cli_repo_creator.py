import random
import string
import subprocess
import sys
import tempfile
from contextlib import contextmanager
from pathlib import Path
from textwrap import dedent
from typing import List, Tuple

from feast.cli import cli
from feast.feature_store import FeatureStore


def get_example_repo(example_repo_py) -> str:
    parent = Path(__file__).parent
    traversal_limit = 5
    while traversal_limit > 0 and parent.parts[-1] != "tests":
        traversal_limit -= 1
        parent = parent.parent

    if parent.parts[-1] != "tests":
        raise ValueError(f"Unable to find where repo {example_repo_py} is located")

    return (parent / "example_repos" / example_repo_py).read_text()


class CliRunner:
    """
    NB. We can't use test runner helper from click here, since it doesn't start a new Python
    interpreter. And we need a new interpreter for each test since we dynamically import
    modules from the feature repo, and it is hard to clean up that state otherwise.
    """

    def run(self, args: List[str], cwd: Path) -> subprocess.CompletedProcess:
        return subprocess.run(
            [sys.executable, cli.__file__] + args, cwd=cwd, capture_output=True
        )

    def run_with_output(self, args: List[str], cwd: Path) -> Tuple[int, bytes]:
        try:
            return (
                0,
                subprocess.check_output(
                    [sys.executable, cli.__file__] + args,
                    cwd=cwd,
                    stderr=subprocess.STDOUT,
                ),
            )
        except subprocess.CalledProcessError as e:
            return e.returncode, e.output

    @contextmanager
    def local_repo(
        self,
        example_repo_py: str,
        offline_store: str,
        online_store: str = "sqlite",
        apply=True,
        teardown=True,
    ):
        """
        Convenience method to set up all the boilerplate for a local feature repo.
        """
        project_id = "test" + "".join(
            random.choice(string.ascii_lowercase + string.digits) for _ in range(10)
        )

        with (
            tempfile.TemporaryDirectory() as repo_dir_name,
            tempfile.TemporaryDirectory() as data_dir_name,
        ):
            repo_path = Path(repo_dir_name)
            data_path = Path(data_dir_name)

            repo_config = repo_path / "feature_store.yaml"
            if online_store == "sqlite":
                yaml_config = dedent(
                    f"""
                project: {project_id}
                registry: {data_path / "registry.db"}
                provider: local
                online_store:
                    path: {data_path / "online_store.db"}
                offline_store:
                    type: {offline_store}
                entity_key_serialization_version: 2
                """
                )
            elif online_store == "milvus":
                yaml_config = dedent(
                    f"""
                project: {project_id}
                registry: {data_path / "registry.db"}
                provider: local
                online_store:
                    path: {data_path / "online_store.db"}
                    type: milvus
                    vector_enabled: true
                    embedding_dim: 10
                offline_store:
                    type: {offline_store}
                entity_key_serialization_version: 3
                """
                )
            else:
                pass

            repo_config.write_text(yaml_config)

            repo_example = repo_path / "example.py"
            repo_example.write_text(example_repo_py)

            if apply:
                result = self.run(["apply"], cwd=repo_path)
                stdout = result.stdout.decode("utf-8")
                stderr = result.stderr.decode("utf-8")
                print(f"Apply stdout:\n{stdout}")
                print(f"Apply stderr:\n{stderr}")
                assert result.returncode == 0, (
                    f"stdout: {result.stdout}\nstderr: {result.stderr}"
                )

            yield FeatureStore(repo_path=str(repo_path), config=None)

            if teardown:
                result = self.run(["teardown"], cwd=repo_path)
                stdout = result.stdout.decode("utf-8")
                stderr = result.stderr.decode("utf-8")
                print(f"Apply stdout:\n{stdout}")
                print(f"Apply stderr:\n{stderr}")
                assert result.returncode == 0, (
                    f"stdout: {result.stdout}\nstderr: {result.stderr}"
                )
