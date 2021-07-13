import random
import string
import tempfile
from pathlib import Path
from textwrap import dedent

import pytest
import redis

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
        registry: {data_path / "registry.db"}
        provider: local
        offline_store:
            type: bigquery
        online_store:
            type: redis
            connection_string: localhost:6379,db=0
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


@pytest.mark.integration
def test_connection_error() -> None:
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
        registry: {data_path / "registry.db"}
        provider: local
        offline_store:
            type: file
        online_store:
            type: redis
            connection_string: localhost:6379,db=0=
        """
            )
        )

        repo_example = repo_path / "example.py"
        repo_example.write_text(
            (Path(__file__).parent / "example_feature_repo_2.py").read_text()
        )

        result = runner.run(["apply"], cwd=repo_path)
        assert result.returncode == 0

        # Redis does not support names for its databases.
        with pytest.raises(redis.exceptions.ResponseError):
            basic_rw_test(
                FeatureStore(repo_path=str(repo_path), config=None),
                view_name="driver_hourly_stats",
            )
