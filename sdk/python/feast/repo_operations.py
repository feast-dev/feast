import importlib
import os
import random
import re
import sys
from importlib.abc import Loader
from pathlib import Path
from typing import List, NamedTuple, Set, Tuple, Union

import click
from click.exceptions import BadParameter

from feast import Entity, FeatureTable
from feast.feature_service import FeatureService
from feast.feature_store import FeatureStore, _validate_feature_views
from feast.feature_view import FeatureView
from feast.infra.provider import get_provider
from feast.names import adjectives, animals
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.registry import Registry
from feast.repo_config import RepoConfig
from feast.usage import log_exceptions_and_usage


def py_path_to_module(path: Path, repo_root: Path) -> str:
    return (
        str(path.relative_to(repo_root))[: -len(".py")]
        .replace("./", "")
        .replace("/", ".")
    )


class ParsedRepo(NamedTuple):
    feature_tables: List[FeatureTable]
    feature_views: List[FeatureView]
    on_demand_feature_views: List[OnDemandFeatureView]
    entities: List[Entity]
    feature_services: List[FeatureService]


def read_feastignore(repo_root: Path) -> List[str]:
    """Read .feastignore in the repo root directory (if exists) and return the list of user-defined ignore paths"""
    feast_ignore = repo_root / ".feastignore"
    if not feast_ignore.is_file():
        return []
    lines = feast_ignore.read_text().strip().split("\n")
    ignore_paths = []
    for line in lines:
        # Remove everything after the first occurance of "#" symbol (comments)
        if line.find("#") >= 0:
            line = line[: line.find("#")]
        # Strip leading or ending whitespaces
        line = line.strip()
        # Add this processed line to ignore_paths if it's not empty
        if len(line) > 0:
            ignore_paths.append(line)
    return ignore_paths


def get_ignore_files(repo_root: Path, ignore_paths: List[str]) -> Set[Path]:
    """Get all ignore files that match any of the user-defined ignore paths"""
    ignore_files = set()
    for ignore_path in ignore_paths:
        # ignore_path may contains matchers (* or **). Use glob() to match user-defined path to actual paths
        for matched_path in repo_root.glob(ignore_path):
            if matched_path.is_file():
                # If the matched path is a file, add that to ignore_files set
                ignore_files.add(matched_path.resolve())
            else:
                # Otherwise, list all Python files in that directory and add all of them to ignore_files set
                ignore_files |= {
                    sub_path.resolve()
                    for sub_path in matched_path.glob("**/*.py")
                    if sub_path.is_file()
                }
    return ignore_files


def get_repo_files(repo_root: Path) -> List[Path]:
    """Get the list of all repo files, ignoring undesired files & directories specified in .feastignore"""
    # Read ignore paths from .feastignore and create a set of all files that match any of these paths
    ignore_paths = read_feastignore(repo_root)
    ignore_files = get_ignore_files(repo_root, ignore_paths)

    # List all Python files in the root directory (recursively)
    repo_files = {p.resolve() for p in repo_root.glob("**/*.py") if p.is_file()}
    # Ignore all files that match any of the ignore paths in .feastignore
    repo_files -= ignore_files

    # Sort repo_files to read them in the same order every time
    return sorted(repo_files)


def parse_repo(repo_root: Path) -> ParsedRepo:
    """ Collect feature table definitions from feature repo """
    res = ParsedRepo(
        feature_tables=[],
        entities=[],
        feature_views=[],
        feature_services=[],
        on_demand_feature_views=[],
    )

    for repo_file in get_repo_files(repo_root):
        module_path = py_path_to_module(repo_file, repo_root)
        module = importlib.import_module(module_path)
        for attr_name in dir(module):
            obj = getattr(module, attr_name)
            if isinstance(obj, FeatureTable):
                res.feature_tables.append(obj)
            if isinstance(obj, FeatureView):
                res.feature_views.append(obj)
            elif isinstance(obj, Entity):
                res.entities.append(obj)
            elif isinstance(obj, FeatureService):
                res.feature_services.append(obj)
            elif isinstance(obj, OnDemandFeatureView):
                res.on_demand_feature_views.append(obj)
    return res


@log_exceptions_and_usage
def apply_total(repo_config: RepoConfig, repo_path: Path, skip_source_validation: bool):
    from colorama import Fore, Style

    os.chdir(repo_path)
    store = FeatureStore(repo_path=str(repo_path))
    project = store.project
    if not is_valid_name(project):
        print(
            f"{project} is not valid. Project name should only have "
            f"alphanumerical values and underscores but not start with an underscore."
        )
        sys.exit(1)
    registry = store.registry
    registry._initialize_registry()
    sys.dont_write_bytecode = True
    repo = parse_repo(repo_path)
    _validate_feature_views(repo.feature_views)

    if not skip_source_validation:
        data_sources = [t.batch_source for t in repo.feature_views]
        # Make sure the data source used by this feature view is supported by Feast
        for data_source in data_sources:
            data_source.validate(store.config)

    entities_to_keep, entities_to_delete = _tag_registry_entities_for_keep_delete(
        project, registry, repo
    )
    views_to_keep, views_to_delete = _tag_registry_views_for_keep_delete(
        project, registry, repo
    )
    (
        odfvs_to_keep,
        odfvs_to_delete,
    ) = _tag_registry_on_demand_feature_views_for_keep_delete(project, registry, repo)
    tables_to_keep, tables_to_delete = _tag_registry_tables_for_keep_delete(
        project, registry, repo
    )
    (services_to_keep, services_to_delete,) = _tag_registry_services_for_keep_delete(
        project, registry, repo
    )

    sys.dont_write_bytecode = False

    # Delete views that should not exist
    for registry_view in views_to_delete:
        registry.delete_feature_view(registry_view.name, project=project, commit=False)
        click.echo(
            f"Deleted feature view {Style.BRIGHT + Fore.GREEN}{registry_view.name}{Style.RESET_ALL} from registry"
        )

    # Delete feature services that should not exist
    for feature_service_to_delete in services_to_delete:
        registry.delete_feature_service(
            feature_service_to_delete.name, project=project, commit=False
        )
        click.echo(
            f"Deleted feature service {Style.BRIGHT + Fore.GREEN}{feature_service_to_delete.name}{Style.RESET_ALL} "
            f"from registry"
        )

    # Delete tables that should not exist
    for registry_table in tables_to_delete:
        registry.delete_feature_table(
            registry_table.name, project=project, commit=False
        )
        click.echo(
            f"Deleted feature table {Style.BRIGHT + Fore.GREEN}{registry_table.name}{Style.RESET_ALL} from registry"
        )

    # TODO: delete entities from the registry too

    # Add / update views + entities + services
    all_to_apply: List[
        Union[Entity, FeatureView, FeatureService, OnDemandFeatureView]
    ] = []
    all_to_apply.extend(entities_to_keep)
    all_to_apply.extend(views_to_keep)
    all_to_apply.extend(services_to_keep)
    all_to_apply.extend(odfvs_to_keep)
    # TODO: delete odfvs
    store.apply(all_to_apply, commit=False)
    for entity in entities_to_keep:
        click.echo(
            f"Registered entity {Style.BRIGHT + Fore.GREEN}{entity.name}{Style.RESET_ALL}"
        )
    for view in views_to_keep:
        click.echo(
            f"Registered feature view {Style.BRIGHT + Fore.GREEN}{view.name}{Style.RESET_ALL}"
        )
    for odfv in odfvs_to_keep:
        click.echo(
            f"Registered on demand feature view {Style.BRIGHT + Fore.GREEN}{odfv.name}{Style.RESET_ALL}"
        )
    for feature_service in services_to_keep:
        click.echo(
            f"Registered feature service {Style.BRIGHT + Fore.GREEN}{feature_service.name}{Style.RESET_ALL}"
        )
    # Create tables that should exist
    for table in tables_to_keep:
        registry.apply_feature_table(table, project, commit=False)
        click.echo(
            f"Registered feature table {Style.BRIGHT + Fore.GREEN}{table.name}{Style.RESET_ALL}"
        )

    infra_provider = get_provider(repo_config, repo_path)
    for name in [view.name for view in repo.feature_tables] + [
        table.name for table in repo.feature_views
    ]:
        click.echo(
            f"Deploying infrastructure for {Style.BRIGHT + Fore.GREEN}{name}{Style.RESET_ALL}"
        )
    for name in [view.name for view in views_to_delete] + [
        table.name for table in tables_to_delete
    ]:
        click.echo(
            f"Removing infrastructure for {Style.BRIGHT + Fore.GREEN}{name}{Style.RESET_ALL}"
        )
    # TODO: consider echoing also entities being deployed/removed

    all_to_delete: List[Union[FeatureTable, FeatureView]] = []
    all_to_delete.extend(tables_to_delete)
    all_to_delete.extend(views_to_delete)
    all_to_keep: List[Union[FeatureTable, FeatureView]] = []
    all_to_keep.extend(tables_to_keep)
    all_to_keep.extend(views_to_delete)
    infra_provider.update_infra(
        project,
        tables_to_delete=all_to_delete,
        tables_to_keep=all_to_keep,
        entities_to_delete=entities_to_delete,
        entities_to_keep=entities_to_keep,
        partial=False,
    )

    # Commit the update to the registry only after successful infra update
    registry.commit()


def _tag_registry_entities_for_keep_delete(
    project: str, registry: Registry, repo: ParsedRepo
) -> Tuple[List[Entity], List[Entity]]:
    entities_to_keep: List[Entity] = repo.entities
    entities_to_delete: List[Entity] = []
    repo_entities_names = set([e.name for e in repo.entities])
    for registry_entity in registry.list_entities(project=project):
        if registry_entity.name not in repo_entities_names:
            entities_to_delete.append(registry_entity)
    return entities_to_keep, entities_to_delete


def _tag_registry_views_for_keep_delete(
    project: str, registry: Registry, repo: ParsedRepo
) -> Tuple[List[FeatureView], List[FeatureView]]:
    views_to_keep: List[FeatureView] = repo.feature_views
    views_to_delete: List[FeatureView] = []
    repo_feature_view_names = set(t.name for t in repo.feature_views)
    for registry_view in registry.list_feature_views(project=project):
        if registry_view.name not in repo_feature_view_names:
            views_to_delete.append(registry_view)
    return views_to_keep, views_to_delete


def _tag_registry_on_demand_feature_views_for_keep_delete(
    project: str, registry: Registry, repo: ParsedRepo
) -> Tuple[List[OnDemandFeatureView], List[OnDemandFeatureView]]:
    odfvs_to_keep: List[OnDemandFeatureView] = repo.on_demand_feature_views
    odfvs_to_delete: List[OnDemandFeatureView] = []
    repo_on_demand_feature_view_names = set(
        t.name for t in repo.on_demand_feature_views
    )
    for registry_odfv in registry.list_on_demand_feature_views(project=project):
        if registry_odfv.name not in repo_on_demand_feature_view_names:
            odfvs_to_delete.append(registry_odfv)
    return odfvs_to_keep, odfvs_to_delete


def _tag_registry_tables_for_keep_delete(
    project: str, registry: Registry, repo: ParsedRepo
) -> Tuple[List[FeatureTable], List[FeatureTable]]:
    tables_to_keep: List[FeatureTable] = repo.feature_tables
    tables_to_delete: List[FeatureTable] = []
    repo_table_names = set(t.name for t in repo.feature_tables)
    for registry_table in registry.list_feature_tables(project=project):
        if registry_table.name not in repo_table_names:
            tables_to_delete.append(registry_table)
    return tables_to_keep, tables_to_delete


def _tag_registry_services_for_keep_delete(
    project: str, registry: Registry, repo: ParsedRepo
) -> Tuple[List[FeatureService], List[FeatureService]]:
    services_to_keep: List[FeatureService] = repo.feature_services
    services_to_delete: List[FeatureService] = []
    repo_feature_service_names = set(t.name for t in repo.feature_services)
    for registry_service in registry.list_feature_services(project=project):
        if registry_service.name not in repo_feature_service_names:
            services_to_delete.append(registry_service)
    return services_to_keep, services_to_delete


@log_exceptions_and_usage
def teardown(repo_config: RepoConfig, repo_path: Path):
    # Cannot pass in both repo_path and repo_config to FeatureStore.
    feature_store = FeatureStore(repo_path=repo_path, config=None)
    feature_store.teardown()


@log_exceptions_and_usage
def registry_dump(repo_config: RepoConfig, repo_path: Path):
    """ For debugging only: output contents of the metadata registry """
    registry_config = repo_config.get_registry_config()
    project = repo_config.project
    registry = Registry(registry_config=registry_config, repo_path=repo_path)

    for entity in registry.list_entities(project=project):
        print(entity)
    for feature_view in registry.list_feature_views(project=project):
        print(feature_view)


def cli_check_repo(repo_path: Path):
    sys.path.append(str(repo_path))
    config_path = repo_path / "feature_store.yaml"
    if not config_path.exists():
        print(
            f"Can't find feature_store.yaml at {repo_path}. Make sure you're running feast from an initialized "
            f"feast repository. "
        )
        sys.exit(1)


@log_exceptions_and_usage
def init_repo(repo_name: str, template: str):
    import os
    from distutils.dir_util import copy_tree
    from pathlib import Path

    from colorama import Fore, Style

    if not is_valid_name(repo_name):
        raise BadParameter(
            message="Name should be alphanumeric values and underscores but not start with an underscore",
            param_hint="PROJECT_DIRECTORY",
        )
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


def is_valid_name(name: str) -> bool:
    """A name should be alphanumeric values and underscores but not start with an underscore"""
    return not name.startswith("_") and re.compile(r"\W+").search(name) is None


def replace_str_in_file(file_path, match_str, sub_str):
    with open(file_path, "r") as f:
        contents = f.read()
    contents = contents.replace(match_str, sub_str)
    with open(file_path, "wt") as f:
        f.write(contents)


def generate_project_name() -> str:
    """Generates a unique project name"""
    return f"{random.choice(adjectives)}_{random.choice(animals)}"
