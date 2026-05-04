"""
CLI test utilities for Feast testing.

Note: This module contains a workaround for a known subprocess hang issue:

Dask atexit handler: when a Feast materialization fails mid-execution, Dask's
global ThreadPoolExecutor registers an atexit.register(default_pool.shutdown)
handler. If the pool has active or recently-used threads, shutdown(wait=True)
can block for an extended period, preventing the subprocess from exiting. This
causes the parent's subprocess.check_output / communicate() to block forever.

The fix is to use subprocess.Popen with communicate(timeout=...) so we can kill
the subprocess if it hangs and still recover any partial output (which contains
the error message printed before the hang).

Teardown is intentionally performed in-process (store.teardown()) rather than
via a 'feast teardown' subprocess. This eliminates per-repo subprocess startup
overhead and avoids atexit-handler (Dask thread pool, PySpark JVM) hang risks
that can push the cumulative test time past the pytest global timeout budget.
"""

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
        # Apply a conservative timeout to prevent CI hangs from Dask atexit-handler
        # stalls or other subprocess blockages.
        timeout = 120

        try:
            return subprocess.run(
                [sys.executable, cli.__file__] + args,
                cwd=cwd,
                capture_output=True,
                timeout=timeout,
            )
        except subprocess.TimeoutExpired:
            return subprocess.CompletedProcess(
                args=[sys.executable, cli.__file__] + args,
                returncode=-1,
                stdout=b"",
                stderr=f"Command timed out after {timeout}s: {args}".encode(),
            )

    def run_with_output(self, args: List[str], cwd: Path) -> Tuple[int, bytes]:
        is_teardown = "teardown" in args
        # Use subprocess.Popen + communicate(timeout=...) so that on a hang we can
        # kill the process and still recover any output already buffered in the pipe.
        # This matters when feast prints an error and then hangs in the Dask atexit
        # handler — the error text is already in the pipe buffer and can be read after
        # the process is killed.
        timeout = 120 if is_teardown else 60

        proc = subprocess.Popen(
            [sys.executable, cli.__file__] + args,
            cwd=cwd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        try:
            stdout, _ = proc.communicate(timeout=timeout)
            returncode = proc.returncode
            if returncode != 0:
                return returncode, stdout
            return 0, stdout
        except subprocess.TimeoutExpired:
            proc.kill()
            stdout, _ = proc.communicate()
            if is_teardown:
                return (
                    -1,
                    b"Teardown timed out (known PySpark JVM cleanup issue on macOS)",
                )
            else:
                # Return partial output (likely contains the error printed before hang)
                # with a non-zero returncode so callers can inspect it.
                return -1, stdout or b""

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

            registry_path_yaml = str(data_path / "registry.db")
            online_store_path_yaml = str(data_path / "online_store.db")

            repo_config = repo_path / "feature_store.yaml"
            if online_store == "sqlite":
                yaml_config = dedent(
                    f"""
                project: {project_id}
                registry: {registry_path_yaml}
                provider: local
                online_store:
                    path: {online_store_path_yaml}
                offline_store:
                    type: {offline_store}
                entity_key_serialization_version: 3
                """
                )
            elif online_store == "milvus":
                yaml_config = dedent(
                    f"""
                project: {project_id}
                registry: {registry_path_yaml}
                provider: local
                online_store:
                    path: {online_store_path_yaml}
                    type: milvus
                    vector_enabled: true
                    embedding_dim: 10
                offline_store:
                    type: {offline_store}
                entity_key_serialization_version: 3
                """
                )
            elif online_store:  # Added for mongodb, but very general
                yaml_config = dedent(
                    f"""
                project: {project_id}
                registry: {registry_path_yaml}
                provider: local
                online_store:
                    type: {online_store}
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

            store_instance = FeatureStore(repo_path=str(repo_path), config=None)
            yield store_instance

            if teardown:
                # Use in-process teardown instead of a 'feast teardown' subprocess.
                # Subprocess teardown adds per-repo startup overhead and risks
                # blocking indefinitely in Dask/PySpark atexit handlers, which
                # can push the cumulative test time past the pytest timeout budget.
                # store.teardown() performs the same SQLite/registry cleanup directly.
                store_instance.teardown()
