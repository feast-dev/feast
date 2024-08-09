import tempfile
import uuid
from pathlib import Path
from textwrap import dedent

import pytest
from assertpy import assertpy

from feast.feature_store import FeatureStore
from tests.integration.feature_repos.universal.data_sources.file import (
    FileDataSourceCreator,
)
from tests.utils.basic_read_write_test import basic_rw_test
from tests.utils.cli_repo_creator import CliRunner, get_example_repo
from tests.utils.e2e_test_validation import (
    NULLABLE_ONLINE_STORE_CONFIGS,
    make_feature_store_yaml,
)


@pytest.mark.integration
def test_universal_cli():
    project = f"test_universal_cli_{str(uuid.uuid4()).replace('-', '')[:8]}"
    runner = CliRunner()

    with tempfile.TemporaryDirectory() as repo_dir_name:
        try:
            repo_path = Path(repo_dir_name)
            feature_store_yaml = make_feature_store_yaml(
                project,
                repo_path,
                FileDataSourceCreator("project"),
                "local",
                {"type": "sqlite"},
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
            result = runner.run(["data-sources", "list"], cwd=repo_path)
            assertpy.assert_that(result.returncode).is_equal_to(0)
            result = runner.run(["permissions", "list"], cwd=repo_path)
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
            assertpy.assert_that(fs.list_feature_views()).is_length(5)
            result = runner.run(
                ["data-sources", "describe", "customer_profile_source"],
                cwd=repo_path,
            )
            assertpy.assert_that(result.returncode).is_equal_to(0)
            assertpy.assert_that(fs.list_data_sources()).is_length(5)

            # entity & feature view describe commands should fail when objects don't exist
            result = runner.run(["entities", "describe", "foo"], cwd=repo_path)
            assertpy.assert_that(result.returncode).is_equal_to(1)
            result = runner.run(["feature-views", "describe", "foo"], cwd=repo_path)
            assertpy.assert_that(result.returncode).is_equal_to(1)
            result = runner.run(["feature-services", "describe", "foo"], cwd=repo_path)
            assertpy.assert_that(result.returncode).is_equal_to(1)
            result = runner.run(["data-sources", "describe", "foo"], cwd=repo_path)
            assertpy.assert_that(result.returncode).is_equal_to(1)
            result = runner.run(["permissions", "describe", "foo"], cwd=repo_path)
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


@pytest.mark.integration
def test_odfv_apply() -> None:
    project = f"test_odfv_apply{str(uuid.uuid4()).replace('-', '')[:8]}"
    runner = CliRunner()

    with tempfile.TemporaryDirectory() as repo_dir_name:
        try:
            repo_path = Path(repo_dir_name)
            feature_store_yaml = make_feature_store_yaml(
                project,
                repo_path,
                FileDataSourceCreator("project"),
                "local",
                {"type": "sqlite"},
            )

            repo_config = repo_path / "feature_store.yaml"

            repo_config.write_text(dedent(feature_store_yaml))

            repo_example = repo_path / "example.py"
            repo_example.write_text(get_example_repo("on_demand_feature_view_repo.py"))
            result = runner.run(["apply"], cwd=repo_path)
            assertpy.assert_that(result.returncode).is_equal_to(0)

            # entity & feature view list commands should succeed
            result = runner.run(["entities", "list"], cwd=repo_path)
            assertpy.assert_that(result.returncode).is_equal_to(0)
            result = runner.run(["on-demand-feature-views", "list"], cwd=repo_path)
            assertpy.assert_that(result.returncode).is_equal_to(0)
        finally:
            runner.run(["teardown"], cwd=repo_path)


@pytest.mark.integration
@pytest.mark.parametrize("test_nullable_online_store", NULLABLE_ONLINE_STORE_CONFIGS)
def test_nullable_online_store(test_nullable_online_store) -> None:
    project = f"test_nullable_online_store{str(uuid.uuid4()).replace('-', '')[:8]}"
    runner = CliRunner()

    with tempfile.TemporaryDirectory() as repo_dir_name:
        try:
            repo_path = Path(repo_dir_name)
            feature_store_yaml = make_feature_store_yaml(
                project,
                repo_path,
                test_nullable_online_store.offline_store_creator(project),
                test_nullable_online_store.provider,
                test_nullable_online_store.online_store,
            )

            repo_config = repo_path / "feature_store.yaml"

            repo_config.write_text(dedent(feature_store_yaml))

            repo_example = repo_path / "example.py"
            repo_example.write_text(get_example_repo("empty_feature_repo.py"))
            result = runner.run(["apply"], cwd=repo_path)
            assertpy.assert_that(result.returncode).is_equal_to(0)
        finally:
            runner.run(["teardown"], cwd=repo_path)
