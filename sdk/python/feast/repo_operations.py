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

from feast import PushSource
from feast.batch_feature_view import BatchFeatureView
from feast.data_source import DataSource, KafkaSource, KinesisSource
from feast.diff.registry_diff import extract_objects_for_keep_delete_update_add
from feast.entity import Entity
from feast.feature_service import FeatureService
from feast.feature_store import FeatureStore
from feast.feature_view import DUMMY_ENTITY, FeatureView
from feast.file_utils import replace_str_in_file
from feast.infra.registry.registry import FEAST_OBJECT_TYPES, FeastObjectType, Registry
from feast.names import adjectives, animals
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.repo_config import RepoConfig
from feast.repo_contents import RepoContents
from feast.request_feature_view import RequestFeatureView
from feast.stream_feature_view import StreamFeatureView
from feast.usage import log_exceptions_and_usage


def py_path_to_module(path: Path) -> str:
    return (
        str(path.relative_to(os.getcwd()))[: -len(".py")]
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
    """
    Collects unique Feast object definitions from the given feature repo.

    Specifically, if an object foo has already been added, bar will still be added if
    (bar == foo), but not if (bar is foo). This ensures that import statements will
    not result in duplicates, but defining two equal objects will.
    """
    res = RepoContents(
        data_sources=[],
        entities=[],
        feature_views=[],
        feature_services=[],
        on_demand_feature_views=[],
        stream_feature_views=[],
        request_feature_views=[],
    )

    for repo_file in get_repo_files(repo_root):
        module_path = py_path_to_module(repo_file)
        module = importlib.import_module(module_path)

        for attr_name in dir(module):
            obj = getattr(module, attr_name)

            if isinstance(obj, DataSource) and not any(
                (obj is ds) for ds in res.data_sources
            ):
                res.data_sources.append(obj)

                # Handle batch sources defined within stream sources.
                if (
                    isinstance(obj, PushSource)
                    or isinstance(obj, KafkaSource)
                    or isinstance(obj, KinesisSource)
                ):
                    batch_source = obj.batch_source

                    if batch_source and not any(
                        (batch_source is ds) for ds in res.data_sources
                    ):
                        res.data_sources.append(batch_source)
            if (
                isinstance(obj, FeatureView)
                and not any((obj is fv) for fv in res.feature_views)
                and not isinstance(obj, StreamFeatureView)
                and not isinstance(obj, BatchFeatureView)
            ):
                res.feature_views.append(obj)

                # Handle batch sources defined with feature views.
                batch_source = obj.batch_source
                assert batch_source
                if not any((batch_source is ds) for ds in res.data_sources):
                    res.data_sources.append(batch_source)

                # Handle stream sources defined with feature views.
                if obj.stream_source:
                    stream_source = obj.stream_source
                    if not any((stream_source is ds) for ds in res.data_sources):
                        res.data_sources.append(stream_source)
            elif isinstance(obj, StreamFeatureView) and not any(
                (obj is sfv) for sfv in res.stream_feature_views
            ):
                res.stream_feature_views.append(obj)

                # Handle batch sources defined with feature views.
                batch_source = obj.batch_source
                if not any((batch_source is ds) for ds in res.data_sources):
                    res.data_sources.append(batch_source)

                # Handle stream sources defined with feature views.
                stream_source = obj.stream_source
                assert stream_source
                if not any((stream_source is ds) for ds in res.data_sources):
                    res.data_sources.append(stream_source)
            elif isinstance(obj, BatchFeatureView) and not any(
                (obj is bfv) for bfv in res.feature_views
            ):
                res.feature_views.append(obj)

                # Handle batch sources defined with feature views.
                batch_source = obj.batch_source
                if not any((batch_source is ds) for ds in res.data_sources):
                    res.data_sources.append(batch_source)
            elif isinstance(obj, Entity) and not any(
                (obj is entity) for entity in res.entities
            ):
                res.entities.append(obj)
            elif isinstance(obj, FeatureService) and not any(
                (obj is fs) for fs in res.feature_services
            ):
                res.feature_services.append(obj)
            elif isinstance(obj, OnDemandFeatureView) and not any(
                (obj is odfv) for odfv in res.on_demand_feature_views
            ):
                res.on_demand_feature_views.append(obj)
            elif isinstance(obj, RequestFeatureView) and not any(
                (obj is rfv) for rfv in res.request_feature_views
            ):
                res.request_feature_views.append(obj)

    res.entities.append(DUMMY_ENTITY)
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

    registry_diff, infra_diff, _ = store.plan(repo)
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
            Entity,
            FeatureView,
            RequestFeatureView,
            OnDemandFeatureView,
            StreamFeatureView,
            FeatureService,
        ]
    ] = []
    for object_type in FEAST_OBJECT_TYPES:
        to_apply = set(objs_to_add[object_type]).union(objs_to_update[object_type])
        all_to_apply.extend(to_apply)

    all_to_delete: List[
        Union[
            Entity,
            FeatureView,
            RequestFeatureView,
            OnDemandFeatureView,
            StreamFeatureView,
            FeatureService,
        ]
    ] = []
    for object_type in FEAST_OBJECT_TYPES:
        all_to_delete.extend(objs_to_delete[object_type])

    return (
        all_to_apply,
        all_to_delete,
        set(objs_to_add[FeastObjectType.FEATURE_VIEW]).union(
            set(objs_to_update[FeastObjectType.FEATURE_VIEW])
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

    registry_diff, infra_diff, new_infra = store.plan(repo)

    # For each object in the registry, determine whether it should be kept or deleted.
    (
        all_to_apply,
        all_to_delete,
        views_to_keep,
        views_to_delete,
    ) = extract_objects_for_apply_delete(project, registry, repo)

    click.echo(registry_diff.to_string())

    if store._should_use_plan():
        store._apply_diffs(registry_diff, infra_diff, new_infra)
        click.echo(infra_diff.to_string())
    else:
        store.apply(all_to_apply, objects_to_delete=all_to_delete, partial=False)
        log_infra_changes(views_to_keep, views_to_delete)


def log_infra_changes(
    views_to_keep: Set[FeatureView], views_to_delete: Set[FeatureView]
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
def registry_dump(repo_config: RepoConfig, repo_path: Path) -> str:
    """For debugging only: output contents of the metadata registry"""
    registry_config = repo_config.get_registry_config()
    project = repo_config.project
    registry = Registry(registry_config=registry_config, repo_path=repo_path)
    registry_dict = registry.to_dict(project=project)
    return json.dumps(registry_dict, indent=2, sort_keys=True)


def cli_check_repo(repo_path: Path, fs_yaml_file: Path):
    sys.path.append(str(repo_path))
    if not fs_yaml_file.exists():
        print(
            f"Can't find feature repo configuration file at {fs_yaml_file}. "
            "Make sure you're running feast from an initialized feast repository."
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
    feature_store_yaml_path = repo_path / "feature_repo" / "feature_store.yaml"
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


def generate_project_name() -> str:
    """Generates a unique project name"""
    return f"{random.choice(adjectives)}_{random.choice(animals)}"
