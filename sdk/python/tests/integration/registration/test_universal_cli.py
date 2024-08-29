import tempfile
import uuid
from pathlib import Path
from textwrap import dedent

import pytest
from assertpy import assertpy

from feast import utils
from feast.feature_store import FeatureStore
from feast.value_type import tag_key_regex
from tests.integration.feature_repos.universal.data_sources.file import (
    FileDataSourceCreator,
)
from tests.integration.feature_repos.universal.feature_views import TAGS
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

            # project, entity & feature view list commands should succeed
            result = runner.run(["projects", "list"], cwd=repo_path)
            assertpy.assert_that(result.returncode).is_equal_to(0)
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
            result = runner.run(["validation-references", "list"], cwd=repo_path)
            assertpy.assert_that(result.returncode).is_equal_to(0)
            result = runner.run(["stream-feature-views", "list"], cwd=repo_path)
            assertpy.assert_that(result.returncode).is_equal_to(0)
            result = runner.run(["saved-datasets", "list"], cwd=repo_path)
            assertpy.assert_that(result.returncode).is_equal_to(0)

            # entity & feature view describe commands should succeed when objects exist
            result = runner.run(["projects", "describe", project], cwd=repo_path)
            assertpy.assert_that(result.returncode).is_equal_to(0)
            result = runner.run(["projects", "current_project"], cwd=repo_path)
            assertpy.assert_that(result.returncode).is_equal_to(0)
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
            assertpy.assert_that(fs.list_projects()).is_length(1)

            # entity & feature view describe commands should fail when objects don't exist
            result = runner.run(["projects", "describe", "foo"], cwd=repo_path)
            assertpy.assert_that(result.returncode).is_equal_to(1)
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
def test_universal_cli_with_project():
    project = "test_universal_cli_with_project_4567"
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
            repo_example.write_text(
                get_example_repo("example_feature_repo_with_project_1.py")
            )
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
            result = runner.run(["projects", "list"], cwd=repo_path)
            assertpy.assert_that(result.returncode).is_equal_to(0)
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
            result = runner.run(["projects", "describe", project], cwd=repo_path)
            assertpy.assert_that(result.returncode).is_equal_to(0)
            result = runner.run(["projects", "current_project"], cwd=repo_path)
            print(result.returncode)
            print("result: ", result)
            print("result.stdout: ", result.stdout)
            assertpy.assert_that(result.returncode).is_equal_to(0)
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

            projects_list = fs.list_projects()
            assertpy.assert_that(projects_list).is_length(1)
            assertpy.assert_that(projects_list[0].name).is_equal_to(project)
            assertpy.assert_that(projects_list[0].description).is_equal_to(
                "test_universal_cli_with_project_4567 description"
            )

            # tag commands should fail
            tags = "release:test"

            result = runner.run(["tag", "project", "foo", tags], cwd=repo_path)
            assertpy.assert_that(result.returncode).is_equal_to(1)
            assertpy.assert_that(
                str(result.stdout, encoding="utf-8").strip()
            ).is_equal_to("Project foo does not exist")

            result = runner.run(["tag", "entity", "foo", tags], cwd=repo_path)
            assertpy.assert_that(result.returncode).is_equal_to(1)
            assertpy.assert_that(
                str(result.stdout, encoding="utf-8").strip()
            ).is_equal_to(f"Entity foo does not exist in project {project}")
            result = runner.run(["tag", "entity", "driver", tags], cwd=repo_path)
            assertpy.assert_that(result.returncode).is_equal_to(1)
            assertpy.assert_that(
                str(result.stdout, encoding="utf-8").strip()
            ).is_equal_to("Tag 'release' already exists")
            result = runner.run(
                ["tag", "entity", "driver", "new:tag, new-"],
                cwd=repo_path,
            )
            assertpy.assert_that(result.returncode).is_equal_to(1)
            assertpy.assert_that(
                str(result.stdout, encoding="utf-8").strip()
            ).is_equal_to(
                "Can not both modify and remove tag 'new' in the same command"
            )
            result = runner.run(
                ["tag", "entity", "driver", "new-"],
                cwd=repo_path,
            )
            assertpy.assert_that(result.returncode).is_equal_to(1)
            assertpy.assert_that(
                str(result.stdout, encoding="utf-8").strip()
            ).is_equal_to("Tag 'new' not found")
            result = runner.run(
                ["tag", "entity", "driver", "new"],
                cwd=repo_path,
            )
            assertpy.assert_that(result.returncode).is_equal_to(1)
            assertpy.assert_that(
                str(result.stdout, encoding="utf-8").strip()
            ).is_equal_to("At least one tag update is required")
            result = runner.run(
                ["tag", "entity", "driver", "new.:fds"],
                cwd=repo_path,
            )
            assertpy.assert_that(result.returncode).is_equal_to(1)
            assertpy.assert_that(
                str(result.stdout, encoding="utf-8").strip()
            ).is_equal_to(
                f"Invalid tag key: 'new.': name part must consist of alphanumeric characters, '-', '_' or '.', and must start and end with an alphanumeric character (e.g. 'MyName',  or 'my.name',  or '123-abc', regex used for validation is '{tag_key_regex}'"
            )

            # tag commands should succeed
            tagsDict = utils.tags_str_to_dict(tags)
            assertpy.assert_that(fs.list_projects(tags=tagsDict)).is_length(0)
            assertpy.assert_that(fs.list_projects()).is_length(1)
            result = runner.run(
                ["tag", "project", project, tags],
                cwd=repo_path,
            )
            assertpy.assert_that(result.returncode).is_equal_to(0)
            assertpy.assert_that(
                str(result.stdout, encoding="utf-8").strip()
            ).is_equal_to(f"project '{project}' tagged")
            assertpy.assert_that(fs.list_projects(tags=tagsDict)).is_length(1)
            result = runner.run(
                ["tag", "project", project, "new:tag"],
                cwd=repo_path,
            )
            assertpy.assert_that(result.returncode).is_equal_to(0)
            tagsDict["new"] = "tag"
            assertpy.assert_that(fs.list_projects(tags=tagsDict)).is_length(1)
            result = runner.run(
                ["tag", "project", project, "new-"],
                cwd=repo_path,
            )
            assertpy.assert_that(result.returncode).is_equal_to(0)
            assertpy.assert_that(fs.list_projects(tags=tagsDict)).is_length(0)
            assertpy.assert_that(
                fs.list_projects(tags=utils.tags_str_to_dict(tags))
            ).is_length(1)

            tagsDict = utils.tags_str_to_dict(tags)
            assertpy.assert_that(fs.list_entities(tags=tagsDict)).is_length(0)
            assertpy.assert_that(fs.list_entities()).is_length(3)
            assertpy.assert_that(fs.list_entities(tags=TAGS)).is_length(2)
            result = runner.run(
                ["tag", "entity", "driver", tags, "--overwrite"],
                cwd=repo_path,
            )
            assertpy.assert_that(result.returncode).is_equal_to(0)
            assertpy.assert_that(
                str(result.stdout, encoding="utf-8").strip()
            ).is_equal_to("entity 'driver' tagged")
            assertpy.assert_that(fs.list_entities(tags=tagsDict)).is_length(1)
            result = runner.run(
                ["tag", "entity", "driver", "new:tag"],
                cwd=repo_path,
            )
            assertpy.assert_that(result.returncode).is_equal_to(0)
            tagsDict["new"] = "tag"
            assertpy.assert_that(fs.list_entities(tags=tagsDict)).is_length(1)
            assertpy.assert_that(fs.list_entities(tags=TAGS)).is_length(1)
            result = runner.run(
                ["tag", "entity", "driver", "new-"],
                cwd=repo_path,
            )
            assertpy.assert_that(result.returncode).is_equal_to(0)
            assertpy.assert_that(fs.list_entities(tags=tagsDict)).is_length(0)
            assertpy.assert_that(
                fs.list_entities(tags=utils.tags_str_to_dict(tags))
            ).is_length(1)

            tagsDict = utils.tags_str_to_dict(tags)
            assertpy.assert_that(fs.list_feature_views(tags=tagsDict)).is_length(0)
            assertpy.assert_that(fs.list_feature_views()).is_length(5)
            result = runner.run(
                ["tag", "feature-view", "driver_locations", tags],
                cwd=repo_path,
            )
            assertpy.assert_that(result.returncode).is_equal_to(0)
            assertpy.assert_that(
                str(result.stdout, encoding="utf-8").strip()
            ).is_equal_to("feature-view 'driver_locations' tagged")
            assertpy.assert_that(fs.list_feature_views(tags=tagsDict)).is_length(1)
            result = runner.run(
                ["tag", "feature-view", "driver_locations", "new:tag"],
                cwd=repo_path,
            )
            assertpy.assert_that(result.returncode).is_equal_to(0)
            tagsDict["new"] = "tag"
            assertpy.assert_that(fs.list_feature_views(tags=tagsDict)).is_length(1)
            assertpy.assert_that(fs.list_feature_views(tags=TAGS)).is_length(0)
            result = runner.run(
                ["tag", "feature-view", "driver_locations", "new-"],
                cwd=repo_path,
            )
            assertpy.assert_that(result.returncode).is_equal_to(0)
            assertpy.assert_that(fs.list_feature_views(tags=tagsDict)).is_length(0)
            assertpy.assert_that(
                fs.list_feature_views(tags=utils.tags_str_to_dict(tags))
            ).is_length(1)

            # entity & feature view describe commands should fail when objects don't exist
            result = runner.run(["projects", "describe", "foo"], cwd=repo_path)
            assertpy.assert_that(result.returncode).is_equal_to(1)
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
            result = runner.run(
                ["validation-references", "describe", "foo"], cwd=repo_path
            )
            assertpy.assert_that(result.returncode).is_equal_to(1)
            result = runner.run(
                ["stream-feature-views", "describe", "foo"], cwd=repo_path
            )
            assertpy.assert_that(result.returncode).is_equal_to(1)
            result = runner.run(["saved-datasets", "describe", "foo"], cwd=repo_path)
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
            result = runner.run(["projects", "describe", project], cwd=repo_path)
            assertpy.assert_that(result.returncode).is_equal_to(0)
            result = runner.run(["projects", "current_project"], cwd=repo_path)
            assertpy.assert_that(result.returncode).is_equal_to(0)
            result = runner.run(["projects", "list"], cwd=repo_path)
            assertpy.assert_that(result.returncode).is_equal_to(0)
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
            result = runner.run(["projects", "describe", project], cwd=repo_path)
            assertpy.assert_that(result.returncode).is_equal_to(0)
            result = runner.run(["projects", "current_project"], cwd=repo_path)
            assertpy.assert_that(result.returncode).is_equal_to(0)
            result = runner.run(["projects", "list"], cwd=repo_path)
            assertpy.assert_that(result.returncode).is_equal_to(0)
        finally:
            runner.run(["teardown"], cwd=repo_path)
