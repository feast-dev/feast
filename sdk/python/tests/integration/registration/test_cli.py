import os
import tempfile
import uuid
from contextlib import contextmanager
from pathlib import Path
from textwrap import dedent
from typing import List

import pytest
import yaml
from assertpy import assertpy

from feast import FeatureStore, RepoConfig
from tests.integration.feature_repos.integration_test_repo_config import (
    IntegrationTestRepoConfig,
)
from tests.integration.feature_repos.repo_configuration import Environment
from tests.integration.feature_repos.universal.data_source_creator import (
    DataSourceCreator,
)
from tests.integration.feature_repos.universal.data_sources.bigquery import (
    BigQueryDataSourceCreator,
)
from tests.integration.feature_repos.universal.data_sources.file import (
    FileDataSourceCreator,
)
from tests.integration.feature_repos.universal.data_sources.redshift import (
    RedshiftDataSourceCreator,
)
from tests.utils.cli_utils import CliRunner, get_example_repo
from tests.utils.online_read_write_test import basic_rw_test


@pytest.mark.integration
@pytest.mark.universal
def test_universal_cli(environment: Environment):
    project = f"test_universal_cli_{str(uuid.uuid4()).replace('-', '')[:8]}"
    runner = CliRunner()

    with tempfile.TemporaryDirectory() as repo_dir_name:
        try:
            repo_path = Path(repo_dir_name)
            feature_store_yaml = make_feature_store_yaml(
                project, environment.test_repo_config, repo_path
            )

            repo_config = repo_path / "feature_store.yaml"

            repo_config.write_text(dedent(feature_store_yaml))

            repo_example = repo_path / "example.py"
            repo_example.write_text(get_example_repo("example_feature_repo_1.py"))
            result = runner.run(["apply"], cwd=repo_path)
            assertpy.assert_that(result.returncode).is_equal_to(0)

            # Store registry contents, to be compared later.
            fs = FeatureStore(repo_path=str(repo_path))
            registry_dict = fs.registry.to_dict(project=project)
            # Save only the specs, not the metadata.
            registry_specs = {
                key: [fco["spec"] if "spec" in fco else fco for fco in value]
                for key, value in registry_dict.items()
            }

            # entity & feature view list commands should succeed
            result = runner.run(["entities", "list"], cwd=repo_path)
            assertpy.assert_that(result.returncode).is_equal_to(0)
            result = runner.run(["feature-views", "list"], cwd=repo_path)
            assertpy.assert_that(result.returncode).is_equal_to(0)
            result = runner.run(["feature-services", "list"], cwd=repo_path)
            assertpy.assert_that(result.returncode).is_equal_to(0)

            # entity & feature view describe commands should succeed when objects exist
            result = runner.run(["entities", "describe", "driver"], cwd=repo_path)
            assertpy.assert_that(result.returncode).is_equal_to(0)
            result = runner.run(
                ["feature-views", "describe", "driver_locations"], cwd=repo_path
            )
            assertpy.assert_that(result.returncode).is_equal_to(0)
            result = runner.run(
                ["feature-services", "describe", "driver_locations_service"],
                cwd=repo_path,
            )
            assertpy.assert_that(result.returncode).is_equal_to(0)
            assertpy.assert_that(fs.list_feature_views()).is_length(3)

            # entity & feature view describe commands should fail when objects don't exist
            result = runner.run(["entities", "describe", "foo"], cwd=repo_path)
            assertpy.assert_that(result.returncode).is_equal_to(1)
            result = runner.run(["feature-views", "describe", "foo"], cwd=repo_path)
            assertpy.assert_that(result.returncode).is_equal_to(1)
            result = runner.run(["feature-services", "describe", "foo"], cwd=repo_path)
            assertpy.assert_that(result.returncode).is_equal_to(1)

            # Doing another apply should be a no op, and should not cause errors
            result = runner.run(["apply"], cwd=repo_path)
            assertpy.assert_that(result.returncode).is_equal_to(0)
            basic_rw_test(
                FeatureStore(repo_path=str(repo_path), config=None),
                view_name="driver_locations",
            )

            # Confirm that registry contents have not changed.
            registry_dict = fs.registry.to_dict(project=project)
            assertpy.assert_that(registry_specs).is_equal_to(
                {
                    key: [fco["spec"] if "spec" in fco else fco for fco in value]
                    for key, value in registry_dict.items()
                }
            )

            result = runner.run(["teardown"], cwd=repo_path)
            assertpy.assert_that(result.returncode).is_equal_to(0)
        finally:
            runner.run(["teardown"], cwd=repo_path)


def make_feature_store_yaml(project, test_repo_config, repo_dir_name: Path):
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


NULLABLE_ONLINE_STORE_CONFIGS: List[IntegrationTestRepoConfig] = [
    IntegrationTestRepoConfig(
        provider="local",
        offline_store_creator=FileDataSourceCreator,
        online_store=None,
    ),
]

if os.getenv("FEAST_IS_LOCAL_TEST", "False") == "True":
    NULLABLE_ONLINE_STORE_CONFIGS.extend(
        [
            IntegrationTestRepoConfig(
                provider="gcp",
                offline_store_creator=BigQueryDataSourceCreator,
                online_store=None,
            ),
            IntegrationTestRepoConfig(
                provider="aws",
                offline_store_creator=RedshiftDataSourceCreator,
                online_store=None,
            ),
        ]
    )


@pytest.mark.integration
@pytest.mark.parametrize("test_nullable_online_store", NULLABLE_ONLINE_STORE_CONFIGS)
def test_nullable_online_store(test_nullable_online_store) -> None:
    project = f"test_nullable_online_store{str(uuid.uuid4()).replace('-', '')[:8]}"
    runner = CliRunner()

    with tempfile.TemporaryDirectory() as repo_dir_name:
        try:
            repo_path = Path(repo_dir_name)
            feature_store_yaml = make_feature_store_yaml(
                project, test_nullable_online_store, repo_path
            )

            repo_config = repo_path / "feature_store.yaml"

            repo_config.write_text(dedent(feature_store_yaml))

            repo_example = repo_path / "example.py"
            repo_example.write_text(get_example_repo("example_feature_repo_1.py"))
            result = runner.run(["apply"], cwd=repo_path)
            assertpy.assert_that(result.returncode).is_equal_to(0)
        finally:
            runner.run(["teardown"], cwd=repo_path)


@contextmanager
def setup_third_party_provider_repo(provider_name: str):
    with tempfile.TemporaryDirectory() as repo_dir_name:

        # Construct an example repo in a temporary dir
        repo_path = Path(repo_dir_name)

        repo_config = repo_path / "feature_store.yaml"

        repo_config.write_text(
            dedent(
                f"""
        project: foo
        registry: data/registry.db
        provider: {provider_name}
        online_store:
            path: data/online_store.db
            type: sqlite
        offline_store:
            type: file
        """
            )
        )

        (repo_path / "foo").mkdir()
        repo_example = repo_path / "foo/provider.py"
        repo_example.write_text(
            (Path(__file__).parents[2] / "foo_provider.py").read_text()
        )

        yield repo_path


@contextmanager
def setup_third_party_registry_store_repo(registry_store: str):
    with tempfile.TemporaryDirectory() as repo_dir_name:

        # Construct an example repo in a temporary dir
        repo_path = Path(repo_dir_name)

        repo_config = repo_path / "feature_store.yaml"

        repo_config.write_text(
            dedent(
                f"""
        project: foo
        registry:
            registry_store_type: {registry_store}
            path: foobar://foo.bar
        provider: local
        online_store:
            path: data/online_store.db
            type: sqlite
        offline_store:
            type: file
        """
            )
        )

        (repo_path / "foo").mkdir()
        repo_example = repo_path / "foo/registry_store.py"
        repo_example.write_text(
            (Path(__file__).parents[2] / "foo_registry_store.py").read_text()
        )

        yield repo_path


def test_3rd_party_providers() -> None:
    """
    Test running apply on third party providers
    """
    runner = CliRunner()
    # Check with incorrect built-in provider name (no dots)
    with setup_third_party_provider_repo("feast123") as repo_path:
        return_code, output = runner.run_with_output(["apply"], cwd=repo_path)
        assertpy.assert_that(return_code).is_equal_to(1)
        assertpy.assert_that(output).contains(b"Provider 'feast123' is not implemented")
    # Check with incorrect third-party provider name (with dots)
    with setup_third_party_provider_repo("feast_foo.Provider") as repo_path:
        return_code, output = runner.run_with_output(["apply"], cwd=repo_path)
        assertpy.assert_that(return_code).is_equal_to(1)
        assertpy.assert_that(output).contains(
            b"Could not import module 'feast_foo' while attempting to load class 'Provider'"
        )
    # Check with incorrect third-party provider name (with dots)
    with setup_third_party_provider_repo("foo.FooProvider") as repo_path:
        return_code, output = runner.run_with_output(["apply"], cwd=repo_path)
        assertpy.assert_that(return_code).is_equal_to(1)
        assertpy.assert_that(output).contains(
            b"Could not import class 'FooProvider' from module 'foo'"
        )
    # Check with correct third-party provider name
    with setup_third_party_provider_repo("foo.provider.FooProvider") as repo_path:
        return_code, output = runner.run_with_output(["apply"], cwd=repo_path)
        assertpy.assert_that(return_code).is_equal_to(0)


def test_3rd_party_registry_store() -> None:
    """
    Test running apply on third party registry stores
    """
    runner = CliRunner()
    # Check with incorrect built-in provider name (no dots)
    with setup_third_party_registry_store_repo("feast123") as repo_path:
        return_code, output = runner.run_with_output(["apply"], cwd=repo_path)
        assertpy.assert_that(return_code).is_equal_to(1)
        assertpy.assert_that(output).contains(
            b'Registry store class name should end with "RegistryStore"'
        )
    # Check with incorrect third-party registry store name (with dots)
    with setup_third_party_registry_store_repo("feast_foo.RegistryStore") as repo_path:
        return_code, output = runner.run_with_output(["apply"], cwd=repo_path)
        assertpy.assert_that(return_code).is_equal_to(1)
        assertpy.assert_that(output).contains(
            b"Could not import module 'feast_foo' while attempting to load class 'RegistryStore'"
        )
    # Check with incorrect third-party registry store name (with dots)
    with setup_third_party_registry_store_repo("foo.FooRegistryStore") as repo_path:
        return_code, output = runner.run_with_output(["apply"], cwd=repo_path)
        assertpy.assert_that(return_code).is_equal_to(1)
        assertpy.assert_that(output).contains(
            b"Could not import class 'FooRegistryStore' from module 'foo'"
        )
    # Check with correct third-party registry store name
    with setup_third_party_registry_store_repo(
        "foo.registry_store.FooRegistryStore"
    ) as repo_path:
        return_code, output = runner.run_with_output(["apply"], cwd=repo_path)
        assertpy.assert_that(return_code).is_equal_to(0)
