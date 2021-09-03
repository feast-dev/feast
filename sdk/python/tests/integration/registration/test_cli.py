import tempfile
import uuid
from pathlib import Path, PosixPath
from textwrap import dedent

import pytest
import yaml

from feast import FeatureStore, RepoConfig
from tests.integration.feature_repos.repo_configuration import FULL_REPO_CONFIGS
from tests.integration.feature_repos.universal.data_source_creator import (
    DataSourceCreator,
)
from tests.utils.cli_utils import CliRunner, get_example_repo
from tests.utils.online_read_write_test import basic_rw_test


@pytest.mark.integration
@pytest.mark.parametrize("test_repo_config", FULL_REPO_CONFIGS)
def test_universal_cli(test_repo_config) -> None:
    project = f"test_universal_cli_{str(uuid.uuid4()).replace('-', '')[:8]}"

    runner = CliRunner()

    with tempfile.TemporaryDirectory() as repo_dir_name:
        feature_store_yaml = make_feature_store_yaml(
            project, test_repo_config, repo_dir_name
        )
        repo_path = Path(repo_dir_name)

        repo_config = repo_path / "feature_store.yaml"

        repo_config.write_text(dedent(feature_store_yaml))

        repo_example = repo_path / "example.py"
        repo_example.write_text(get_example_repo("example_feature_repo_1.py"))
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


def make_feature_store_yaml(project, test_repo_config, repo_dir_name: PosixPath):
    offline_creator: DataSourceCreator = test_repo_config.offline_store_creator(project)

    offline_store_config = offline_creator.create_offline_store_config()
    online_store = test_repo_config.online_store

    config = RepoConfig(
        registry=str(Path(repo_dir_name) / "registry.db"),
        project=project,
        provider=test_repo_config.provider,
        offline_store=offline_store_config,
        online_store=online_store,
        repo_path=str(Path(repo_dir_name)),
    )
    config_dict = config.dict()
    if (
        isinstance(config_dict["online_store"], dict)
        and "redis_type" in config_dict["online_store"]
    ):
        del config_dict["online_store"]["redis_type"]
    config_dict["repo_path"] = str(config_dict["repo_path"])

    return yaml.safe_dump(config_dict)
