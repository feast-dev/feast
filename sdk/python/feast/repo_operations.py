import importlib
import os
import random
import sys
from datetime import timedelta
from importlib.abc import Loader
from pathlib import Path
from typing import List, NamedTuple, Union

from feast import Entity, FeatureTable
from feast.feature_view import FeatureView
from feast.infra.provider import get_provider
from feast.names import adjectives, animals
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
    registry_config = repo_config.get_registry_config()
    project = repo_config.project
    registry = Registry(
        registry_path=registry_config.path,
        cache_ttl=timedelta(seconds=registry_config.cache_ttl_seconds),
    )
    sys.dont_write_bytecode = True
    repo = parse_repo(repo_path)
    sys.dont_write_bytecode = False

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
    registry_config = repo_config.get_registry_config()
    registry = Registry(
        registry_path=registry_config.path,
        cache_ttl=timedelta(seconds=registry_config.cache_ttl_seconds),
    )
    project = repo_config.project
    registry_tables: List[Union[FeatureTable, FeatureView]] = []
    registry_tables.extend(registry.list_feature_tables(project=project))
    registry_tables.extend(registry.list_feature_views(project=project))
    infra_provider = get_provider(repo_config)
    infra_provider.teardown_infra(project, tables=registry_tables)


def registry_dump(repo_config: RepoConfig):
    """ For debugging only: output contents of the metadata registry """
    registry_config = repo_config.get_registry_config()
    project = repo_config.project
    registry = Registry(
        registry_path=registry_config.path,
        cache_ttl=timedelta(seconds=registry_config.cache_ttl_seconds),
    )

    for entity in registry.list_entities(project=project):
        print(entity)
    for feature_view in registry.list_feature_views(project=project):
        print(feature_view)


def cli_check_repo(repo_path: Path):
    config_path = repo_path / "feature_store.yaml"
    if not config_path.exists():
        print(
            f"Can't find feature_store.yaml at {repo_path}. Make sure you're running feast from an initialized "
            f"feast repository. "
        )
        sys.exit(1)


def init_repo(repo_name: str, template: str):
    import os
    from distutils.dir_util import copy_tree
    from pathlib import Path

    from colorama import Fore, Style

    repo_path = Path(os.path.join(Path.cwd(), repo_name))
    repo_path.mkdir(exist_ok=True)
    repo_config_path = repo_path / "feature_store.yaml"

    if repo_config_path.exists():
        new_directory = os.path.relpath(repo_path, os.getcwd())

        print(
            f"The directory {Style.BRIGHT + Fore.GREEN}{new_directory}{Style.RESET_ALL} contains an existing feature "
            f"store repository that may cause a conflict"
        )
        print()
        sys.exit(1)

    # Copy template directory
    template_path = str(Path(Path(__file__).parent / "templates" / template).absolute())
    if not os.path.exists(template_path):
        raise IOError(f"Could not find template {template}")
    copy_tree(template_path, str(repo_path))

    # Seed the repository
    bootstrap_path = repo_path / "bootstrap.py"
    if os.path.exists(bootstrap_path):
        import importlib.util

        spec = importlib.util.spec_from_file_location("bootstrap", str(bootstrap_path))
        bootstrap = importlib.util.module_from_spec(spec)
        assert isinstance(spec.loader, Loader)
        spec.loader.exec_module(bootstrap)
        bootstrap.bootstrap()  # type: ignore
        os.remove(bootstrap_path)

    # Template the feature_store.yaml file
    feature_store_yaml_path = repo_path / "feature_store.yaml"
    replace_str_in_file(
        feature_store_yaml_path, "project: my_project", f"project: {repo_name}"
    )

    # Remove the __pycache__ folder if it exists
    import shutil

    shutil.rmtree(repo_path / "__pycache__", ignore_errors=True)

    import click

    click.echo()
    click.echo(
        f"Creating a new Feast repository in {Style.BRIGHT + Fore.GREEN}{repo_path}{Style.RESET_ALL}."
    )
    click.echo()


def replace_str_in_file(file_path, match_str, sub_str):
    with open(file_path, "r") as f:
        contents = f.read()
    contents = contents.replace(match_str, sub_str)
    with open(file_path, "wt") as f:
        f.write(contents)


def generate_project_name() -> str:
    """Generates a unique project name"""
    return f"{random.choice(adjectives)}_{random.choice(animals)}"
