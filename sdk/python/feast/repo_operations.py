import importlib
import json
import os
import random
import re
import sys
from importlib.abc import Loader
from importlib.machinery import ModuleSpec
from pathlib import Path
from typing import List, Set, Union

import click
from click.exceptions import BadParameter

from feast.diff.registry_diff import extract_objects_for_keep_delete_update_add
from feast.entity import Entity
from feast.feature_service import FeatureService
from feast.feature_store import FeatureStore
from feast.feature_view import DUMMY_ENTITY, FeatureView
from feast.names import adjectives, animals
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.registry import FEAST_OBJECT_TYPES, FeastObjectType, Registry
from feast.repo_config import RepoConfig
from feast.repo_contents import RepoContents
from feast.request_feature_view import RequestFeatureView
from feast.usage import log_exceptions_and_usage


def py_path_to_module(path: Path, repo_root: Path) -> str:
    return (
        str(path.relative_to(repo_root))[: -len(".py")]
        .replace("./", "")
        .replace("/", ".")
    )


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
    repo_files = {
        p.resolve()
        for p in repo_root.glob("**/*.py")
        if p.is_file() and "__init__.py" != p.name
    }
    # Ignore all files that match any of the ignore paths in .feastignore
    repo_files -= ignore_files

    # Sort repo_files to read them in the same order every time
    return sorted(repo_files)


def parse_repo(repo_root: Path) -> RepoContents:
    """ Collect feature table definitions from feature repo """
    res = RepoContents(
        entities=set(),
        feature_views=set(),
        feature_services=set(),
        on_demand_feature_views=set(),
        request_feature_views=set(),
    )

    for repo_file in get_repo_files(repo_root):
        module_path = py_path_to_module(repo_file, repo_root)
        module = importlib.import_module(module_path)
        for attr_name in dir(module):
            obj = getattr(module, attr_name)
            if isinstance(obj, FeatureView):
                res.feature_views.add(obj)
            elif isinstance(obj, Entity):
                res.entities.add(obj)
            elif isinstance(obj, FeatureService):
                res.feature_services.add(obj)
            elif isinstance(obj, OnDemandFeatureView):
                res.on_demand_feature_views.add(obj)
            elif isinstance(obj, RequestFeatureView):
                res.request_feature_views.add(obj)
    res.entities.add(DUMMY_ENTITY)
    return res


@log_exceptions_and_usage
def plan(repo_config: RepoConfig, repo_path: Path, skip_source_validation: bool):

    os.chdir(repo_path)
    project, registry, repo, store = _prepare_registry_and_repo(repo_config, repo_path)

    if not skip_source_validation:
        data_sources = [t.batch_source for t in repo.feature_views]
        # Make sure the data source used by this feature view is supported by Feast
        for data_source in data_sources:
            data_source.validate(store.config)

    registry_diff, infra_diff, _ = store._plan(repo)
    click.echo(registry_diff.to_string())
    click.echo(infra_diff.to_string())


def _prepare_registry_and_repo(repo_config, repo_path):
    store = FeatureStore(config=repo_config)
    project = store.project
    if not is_valid_name(project):
        print(
            f"{project} is not valid. Project name should only have "
            f"alphanumerical values and underscores but not start with an underscore."
        )
        sys.exit(1)
    registry = store.registry
    sys.dont_write_bytecode = True
    repo = parse_repo(repo_path)
    return project, registry, repo, store


def extract_objects_for_apply_delete(project, registry, repo):
    # TODO(achals): This code path should be refactored to handle added & kept entities separately.
    (
        _,
        objs_to_delete,
        objs_to_update,
        objs_to_add,
    ) = extract_objects_for_keep_delete_update_add(registry, project, repo)

    all_to_apply: List[
        Union[
            Entity, FeatureView, RequestFeatureView, OnDemandFeatureView, FeatureService
        ]
    ] = []
    for object_type in FEAST_OBJECT_TYPES:
        to_apply = set(objs_to_add[object_type]).union(objs_to_update[object_type])
        all_to_apply.extend(to_apply)

    all_to_delete: List[
        Union[
            Entity, FeatureView, RequestFeatureView, OnDemandFeatureView, FeatureService
        ]
    ] = []
    for object_type in FEAST_OBJECT_TYPES:
        all_to_delete.extend(objs_to_delete[object_type])

    return (
        all_to_apply,
        all_to_delete,
        set(
            objs_to_add[FeastObjectType.FEATURE_VIEW].union(
                objs_to_update[FeastObjectType.FEATURE_VIEW]
            )
        ),
        objs_to_delete[FeastObjectType.FEATURE_VIEW],
    )


def apply_total_with_repo_instance(
    store: FeatureStore,
    project: str,
    registry: Registry,
    repo: RepoContents,
    skip_source_validation: bool,
):
    if not skip_source_validation:
        data_sources = [t.batch_source for t in repo.feature_views]
        # Make sure the data source used by this feature view is supported by Feast
        for data_source in data_sources:
            data_source.validate(store.config)

    registry_diff, infra_diff, new_infra = store._plan(repo)

    # For each object in the registry, determine whether it should be kept or deleted.
    (
        all_to_apply,
        all_to_delete,
        views_to_delete,
        views_to_keep,
    ) = extract_objects_for_apply_delete(project, registry, repo)

    click.echo(registry_diff.to_string())

    if store._should_use_plan():
        store._apply_diffs(registry_diff, infra_diff, new_infra)
        click.echo(infra_diff.to_string())
    else:
        store.apply(all_to_apply, objects_to_delete=all_to_delete, partial=False)
        log_infra_changes(views_to_keep, views_to_delete)


def log_infra_changes(
    views_to_keep: List[FeatureView], views_to_delete: List[FeatureView]
):
    from colorama import Fore, Style

    for view in views_to_keep:
        click.echo(
            f"Deploying infrastructure for {Style.BRIGHT + Fore.GREEN}{view.name}{Style.RESET_ALL}"
        )
    for view in views_to_delete:
        click.echo(
            f"Removing infrastructure for {Style.BRIGHT + Fore.RED}{view.name}{Style.RESET_ALL}"
        )


@log_exceptions_and_usage
def apply_total(repo_config: RepoConfig, repo_path: Path, skip_source_validation: bool):

    os.chdir(repo_path)
    project, registry, repo, store = _prepare_registry_and_repo(repo_config, repo_path)
    apply_total_with_repo_instance(
        store, project, registry, repo, skip_source_validation
    )


@log_exceptions_and_usage
def teardown(repo_config: RepoConfig, repo_path: Path):
    # Cannot pass in both repo_path and repo_config to FeatureStore.
    feature_store = FeatureStore(repo_path=repo_path, config=None)
    feature_store.teardown()


@log_exceptions_and_usage
def registry_dump(repo_config: RepoConfig, repo_path: Path):
    """ For debugging only: output contents of the metadata registry """
    from colorama import Fore, Style

    registry_config = repo_config.get_registry_config()
    project = repo_config.project
    registry = Registry(registry_config=registry_config, repo_path=repo_path)
    registry_dict = registry.to_dict(project=project)

    warning = (
        "Warning: The registry-dump command is for debugging only and may contain "
        "breaking changes in the future. No guarantees are made on this interface."
    )
    click.echo(f"{Style.BRIGHT}{Fore.YELLOW}{warning}{Style.RESET_ALL}")
    click.echo(json.dumps(registry_dict, indent=2))


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
        assert isinstance(spec, ModuleSpec)
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
