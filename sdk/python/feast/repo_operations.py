import importlib
import json
import os
import random
import re
import sys
from importlib.abc import Loader
from pathlib import Path
from typing import List, Set, Union, cast

import click
from click.exceptions import BadParameter

from feast.base_feature_view import BaseFeatureView
from feast.diff.FcoDiff import TransitionType, tag_objects_for_keep_delete_add
from feast.entity import Entity
from feast.feature_service import FeatureService
from feast.feature_store import FeatureStore, RepoContents
from feast.feature_view import DUMMY_ENTITY, DUMMY_ENTITY_NAME, FeatureView
from feast.names import adjectives, animals
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.registry import Registry
from feast.repo_config import RepoConfig
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
    repo_files = {p.resolve() for p in repo_root.glob("**/*.py") if p.is_file()}
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

    registry_diff, _ = store.plan(repo)
    views_to_delete = [
        v
        for v in registry_diff.fco_diffs
        if v.fco_type == "feature view" and v.transition_type == TransitionType.DELETE
    ]
    views_to_keep = [
        v
        for v in registry_diff.fco_diffs
        if v.fco_type == "feature view"
        and v.transition_type in {TransitionType.CREATE, TransitionType.UNCHANGED}
    ]

    log_cli_output(registry_diff, views_to_delete, views_to_keep)


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
    registry._initialize_registry()
    sys.dont_write_bytecode = True
    repo = parse_repo(repo_path)
    return project, registry, repo, store


def extract_objects_for_apply_delete(project, registry, repo):
    (
        entities_to_keep,
        entities_to_delete,
        entities_to_add,
    ) = tag_objects_for_keep_delete_add(
        set(registry.list_entities(project=project)), repo.entities
    )
    # TODO(achals): This code path should be refactored to handle added & kept entities separately.
    entities_to_keep = set(entities_to_keep).union(entities_to_add)
    views = tag_objects_for_keep_delete_add(
        set(registry.list_feature_views(project=project)), repo.feature_views
    )
    views_to_keep, views_to_delete, views_to_add = (
        cast(Set[FeatureView], views[0]),
        cast(Set[FeatureView], views[1]),
        cast(Set[FeatureView], views[2]),
    )
    request_views = tag_objects_for_keep_delete_add(
        set(registry.list_request_feature_views(project=project)),
        repo.request_feature_views,
    )
    request_views_to_keep: Set[RequestFeatureView]
    request_views_to_delete: Set[RequestFeatureView]
    request_views_to_add: Set[RequestFeatureView]
    request_views_to_keep, request_views_to_delete, request_views_to_add = (
        cast(Set[RequestFeatureView], request_views[0]),
        cast(Set[RequestFeatureView], request_views[1]),
        cast(Set[RequestFeatureView], request_views[2]),
    )
    base_views_to_keep: Set[Union[RequestFeatureView, FeatureView]] = {
        *views_to_keep,
        *views_to_add,
        *request_views_to_keep,
        *request_views_to_add,
    }
    base_views_to_delete: Set[Union[RequestFeatureView, FeatureView]] = {
        *views_to_delete,
        *request_views_to_delete,
    }
    odfvs = tag_objects_for_keep_delete_add(
        set(registry.list_on_demand_feature_views(project=project)),
        repo.on_demand_feature_views,
    )
    odfvs_to_keep, odfvs_to_delete, odfvs_to_add = (
        cast(Set[OnDemandFeatureView], odfvs[0]),
        cast(Set[OnDemandFeatureView], odfvs[1]),
        cast(Set[OnDemandFeatureView], odfvs[2]),
    )
    odfvs_to_keep = odfvs_to_keep.union(odfvs_to_add)
    (
        services_to_keep,
        services_to_delete,
        services_to_add,
    ) = tag_objects_for_keep_delete_add(
        set(registry.list_feature_services(project=project)), repo.feature_services
    )
    services_to_keep = services_to_keep.union(services_to_add)
    sys.dont_write_bytecode = False
    # Apply all changes to the registry and infrastructure.
    all_to_apply: List[
        Union[Entity, BaseFeatureView, FeatureService, OnDemandFeatureView]
    ] = []
    all_to_apply.extend(entities_to_keep)
    all_to_apply.extend(base_views_to_keep)
    all_to_apply.extend(services_to_keep)
    all_to_apply.extend(odfvs_to_keep)
    all_to_delete: List[
        Union[Entity, BaseFeatureView, FeatureService, OnDemandFeatureView]
    ] = []
    all_to_delete.extend(entities_to_delete)
    all_to_delete.extend(base_views_to_delete)
    all_to_delete.extend(services_to_delete)
    all_to_delete.extend(odfvs_to_delete)

    return all_to_apply, all_to_delete, views_to_delete, views_to_keep


@log_exceptions_and_usage
def apply_total(repo_config: RepoConfig, repo_path: Path, skip_source_validation: bool):

    os.chdir(repo_path)
    project, registry, repo, store = _prepare_registry_and_repo(repo_config, repo_path)

    if not skip_source_validation:
        data_sources = [t.batch_source for t in repo.feature_views]
        # Make sure the data source used by this feature view is supported by Feast
        for data_source in data_sources:
            data_source.validate(store.config)

    # For each object in the registry, determine whether it should be kept or deleted.
    (
        all_to_apply,
        all_to_delete,
        views_to_delete,
        views_to_keep,
    ) = extract_objects_for_apply_delete(project, registry, repo)

    diff = store.apply(all_to_apply, objects_to_delete=all_to_delete, partial=False)

    log_cli_output(diff, views_to_delete, views_to_keep)


def log_cli_output(diff, views_to_delete, views_to_keep):
    from colorama import Fore, Style

    message_action_map = {
        TransitionType.CREATE: ("Created", Fore.GREEN),
        TransitionType.DELETE: ("Deleted", Fore.RED),
        TransitionType.UNCHANGED: ("Unchanged", Fore.LIGHTBLUE_EX),
        TransitionType.UPDATE: ("Updated", Fore.YELLOW),
    }
    for fco_diff in diff.fco_diffs:
        if fco_diff.name == DUMMY_ENTITY_NAME:
            continue
        action, color = message_action_map[fco_diff.transition_type]
        click.echo(
            f"{action} {fco_diff.fco_type} {Style.BRIGHT + color}{fco_diff.name}{Style.RESET_ALL}"
        )
        if fco_diff.transition_type == TransitionType.UPDATE:
            for _p in fco_diff.fco_property_diffs:
                click.echo(
                    f"\t{_p.property_name}: {Style.BRIGHT + color}{_p.val_existing}{Style.RESET_ALL} -> {Style.BRIGHT + Fore.LIGHTGREEN_EX}{_p.val_declared}{Style.RESET_ALL}"
                )

    views_to_keep_in_infra = [
        view for view in views_to_keep if isinstance(view, FeatureView)
    ]
    for name in [view.name for view in views_to_keep_in_infra]:
        click.echo(
            f"Deploying infrastructure for {Style.BRIGHT + Fore.GREEN}{name}{Style.RESET_ALL}"
        )
    views_to_delete_from_infra = [
        view for view in views_to_delete if isinstance(view, FeatureView)
    ]
    for name in [view.name for view in views_to_delete_from_infra]:
        click.echo(
            f"Removing infrastructure for {Style.BRIGHT + Fore.RED}{name}{Style.RESET_ALL}"
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
