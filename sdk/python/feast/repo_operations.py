import importlib
import os
import random
import string
import sys
from datetime import datetime, timedelta
from pathlib import Path
from textwrap import dedent
from typing import List, NamedTuple, Union

from feast import Entity, FeatureTable
from feast.driver_test_data import create_driver_hourly_stats_df
from feast.feature_view import FeatureView
from feast.infra.provider import get_provider
from feast.registry import Registry
from feast.repo_config import RepoConfig


def py_path_to_module(path: Path, repo_root: Path) -> str:
    return (
        str(path.relative_to(repo_root))[: -len(".py")]
        .replace("./", "")
        .replace("/", ".")
    )


class ParsedRepo(NamedTuple):
    feature_tables: List[FeatureTable]
    feature_views: List[FeatureView]
    entities: List[Entity]


def parse_repo(repo_root: Path) -> ParsedRepo:
    """ Collect feature table definitions from feature repo """
    res = ParsedRepo(feature_tables=[], entities=[], feature_views=[])

    # FIXME: process subdirs but exclude hidden ones
    repo_files = [p.resolve() for p in repo_root.glob("*.py")]

    for repo_file in repo_files:

        module_path = py_path_to_module(repo_file, repo_root)

        print(f"Processing {repo_file} as {module_path}")
        module = importlib.import_module(module_path)

        for attr_name in dir(module):
            obj = getattr(module, attr_name)
            if isinstance(obj, FeatureTable):
                res.feature_tables.append(obj)
            if isinstance(obj, FeatureView):
                res.feature_views.append(obj)
            elif isinstance(obj, Entity):
                res.entities.append(obj)
    return res


def apply_total(repo_config: RepoConfig, repo_path: Path):
    os.chdir(repo_path)
    sys.path.append("")

    project = repo_config.project
    registry = Registry(repo_config.metadata_store)
    repo = parse_repo(repo_path)

    for entity in repo.entities:
        registry.apply_entity(entity, project=project)

    repo_table_names = set(t.name for t in repo.feature_tables)

    for t in repo.feature_views:
        repo_table_names.add(t.name)

    tables_to_delete = []
    for registry_table in registry.list_feature_tables(project=project):
        if registry_table.name not in repo_table_names:
            tables_to_delete.append(registry_table)

    views_to_delete = []
    for registry_view in registry.list_feature_views(project=project):
        if registry_view.name not in repo_table_names:
            views_to_delete.append(registry_view)

    # Delete tables that should not exist
    for registry_table in tables_to_delete:
        registry.delete_feature_table(registry_table.name, project=project)

    # Create tables that should
    for table in repo.feature_tables:
        registry.apply_feature_table(table, project)

    # Delete views that should not exist
    for registry_view in views_to_delete:
        registry.delete_feature_view(registry_view.name, project=project)

    # Create views that should
    for view in repo.feature_views:
        registry.apply_feature_view(view, project)

    infra_provider = get_provider(repo_config)

    all_to_delete: List[Union[FeatureTable, FeatureView]] = []
    all_to_delete.extend(tables_to_delete)
    all_to_delete.extend(views_to_delete)

    all_to_keep: List[Union[FeatureTable, FeatureView]] = []
    all_to_keep.extend(repo.feature_tables)
    all_to_keep.extend(repo.feature_views)

    infra_provider.update_infra(
        project,
        tables_to_delete=all_to_delete,
        tables_to_keep=all_to_keep,
        partial=False,
    )

    print("Done!")


def teardown(repo_config: RepoConfig, repo_path: Path):
    registry = Registry(repo_config.metadata_store)
    project = repo_config.project
    registry_tables: List[Union[FeatureTable, FeatureView]] = []
    registry_tables.extend(registry.list_feature_tables(project=project))
    registry_tables.extend(registry.list_feature_views(project=project))
    infra_provider = get_provider(repo_config)
    infra_provider.teardown_infra(project, tables=registry_tables)


def registry_dump(repo_config: RepoConfig):
    """ For debugging only: output contents of the metadata registry """

    project = repo_config.project
    registry = Registry(repo_config.metadata_store)

    for entity in registry.list_entities(project=project):
        print(entity)
    for table in registry.list_feature_tables(project=project):
        print(table)


def cli_check_repo(repo_path: Path):
    config_path = repo_path / "feature_store.yaml"
    if not config_path.exists():
        print(
            f"Can't find feature_store.yaml at {repo_path}. Make sure you're running this command in an initialized feast repository."
        )
        sys.exit(1)


def init_repo(repo_path: Path, minimal: bool):

    repo_config = repo_path / "feature_store.yaml"

    if repo_config.exists():
        print("Feature repository is already initalized, nothing to do.")
        sys.exit(1)

    project_id = "proj" + "".join(
        random.choice(string.ascii_lowercase + string.digits) for _ in range(5)
    )

    if minimal:
        repo_config.write_text(
            dedent(
                f"""
        project: {project_id}
        metadata_store: /path/to/metadata.db
        provider: local
        online_store:
            local:
                path: /path/to/online_store.db
        """
            )
        )
        print(
            "Generated example feature_store.yaml. Please edit metadata_store"
            "location before running apply"
        )

    else:
        example_py = (Path(__file__).parent / "example_repo.py").read_text()

        data_path = repo_path / "data"
        data_path.mkdir(exist_ok=True)

        end_date = datetime.now().replace(microsecond=0, second=0, minute=0)
        start_date = end_date - timedelta(days=15)

        driver_entities = [1001, 1002, 1003, 1004, 1005]
        driver_df = create_driver_hourly_stats_df(driver_entities, start_date, end_date)

        driver_stats_path = data_path / "driver_stats.parquet"
        driver_df.to_parquet(
            path=str(driver_stats_path), allow_truncated_timestamps=True
        )

        with open(repo_path / "example.py", "wt") as f:
            f.write(example_py.replace("%PARQUET_PATH%", str(driver_stats_path)))

        # Generate config
        repo_config.write_text(
            dedent(
                f"""
        project: {project_id}
        metadata_store: {data_path / "metadata.db"}
        provider: local
        online_store:
            local:
                path: {data_path / "online_store.db"}
        """
            )
        )

        print("Generated feature_store.yaml and example features in example_repo.py")
        print(
            "Now try runing `feast apply` to apply, or `feast materialize` to sync data to the online store"
        )
