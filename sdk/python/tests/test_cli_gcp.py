import random
import string
import tempfile
from pathlib import Path
from textwrap import dedent

import pytest

from feast.feature_store import FeatureStore
from tests.cli_utils import CliRunner
from tests.online_read_write_test import basic_rw_test


@pytest.mark.integration
def test_basic() -> None:
    project_id = "".join(
        random.choice(string.ascii_lowercase + string.digits) for _ in range(10)
    )
    runner = CliRunner()
    with tempfile.TemporaryDirectory() as repo_dir_name, tempfile.TemporaryDirectory() as data_dir_name:

        repo_path = Path(repo_dir_name)
        data_path = Path(data_dir_name)

        repo_config = repo_path / "feature_store.yaml"

        repo_config.write_text(
            dedent(
                f"""
        project: {project_id}
        metadata_store: {data_path / "metadata.db"}
        provider: gcp
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

        basic_rw_test(
            FeatureStore(repo_path=str(repo_path), config=None),
            view_name="driver_locations",
        )

        result = runner.run(["teardown", str(repo_path)], cwd=repo_path)
        assert result.returncode == 0
