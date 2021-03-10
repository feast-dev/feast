import importlib
import os
import sys
from pathlib import Path
from typing import List, NamedTuple

from feast import Entity, FeatureTable
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
    entities: List[Entity]


def parse_repo(repo_root: Path) -> ParsedRepo:
    """ Collect feature table definitions from feature repo """
    res = ParsedRepo(feature_tables=[], entities=[])

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
    tables_to_delete = []
    for registry_table in registry.list_feature_tables(project=project):
        if registry_table.name not in repo_table_names:
            tables_to_delete.append(registry_table)

    # Delete tables that should not exist
    for registry_table in tables_to_delete:
        registry.delete_feature_table(registry_table.name, project=project)

    for table in repo.feature_tables:
        registry.apply_feature_table(table, project)

    infra_provider = get_provider(repo_config)
    infra_provider.update_infra(
        project, tables_to_delete=tables_to_delete, tables_to_keep=repo.feature_tables
    )

    print("Done!")


def teardown(repo_config: RepoConfig, repo_path: Path):
    registry = Registry(repo_config.metadata_store)
    project = repo_config.project
    registry_tables = registry.list_feature_tables(project=project)
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
