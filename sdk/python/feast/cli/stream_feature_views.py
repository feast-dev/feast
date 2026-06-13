import sys

import click
import yaml

from feast import utils
from feast.cli.cli_options import tagsOption
from feast.errors import FeastObjectNotFoundException
from feast.feature_view import _VALID_STATE_TRANSITIONS, FeatureViewState
from feast.repo_operations import create_feature_store


@click.group(name="stream-feature-views")
def stream_feature_views_cmd():
    """
    [Experimental] Access stream feature views
    """
    pass


@stream_feature_views_cmd.command("describe")
@click.argument("name", type=click.STRING)
@click.pass_context
def stream_feature_views_describe(ctx: click.Context, name: str):
    """
    [Experimental] Describe a stream feature view
    """
    store = create_feature_store(ctx)

    try:
        stream_feature_view = store.get_stream_feature_view(name)
    except FeastObjectNotFoundException as e:
        print(e)
        exit(1)

    data = yaml.safe_load(str(stream_feature_view))
    if hasattr(stream_feature_view, "enabled"):
        data["enabled"] = stream_feature_view.enabled
    if hasattr(stream_feature_view, "state"):
        data["state"] = stream_feature_view.state.name
    print(yaml.dump(data, default_flow_style=False, sort_keys=False))


@stream_feature_views_cmd.command(name="list")
@tagsOption
@click.pass_context
def stream_feature_views_list(ctx: click.Context, tags: list[str]):
    """
    [Experimental] List all stream feature views
    """
    store = create_feature_store(ctx)
    table = []
    tags_filter = utils.tags_list_to_dict(tags)
    for stream_feature_view in store.list_stream_feature_views(tags=tags_filter):
        table.append([stream_feature_view.name])

    from tabulate import tabulate

    print(tabulate(table, headers=["NAME"], tablefmt="plain"))


@stream_feature_views_cmd.command("enable")
@click.argument("name", type=click.STRING)
@click.pass_context
def stream_feature_view_enable(ctx: click.Context, name: str):
    """
    Enable a stream feature view.
    """
    store = create_feature_store(ctx)
    try:
        fv = store.get_stream_feature_view(name)
    except FeastObjectNotFoundException as e:
        print(e)
        sys.exit(1)

    if fv.enabled:
        print(f"Stream feature view '{name}' is already enabled.")
        return

    fv.enabled = True
    store.registry.apply_feature_view(fv, store.project)
    print(f"Stream feature view '{name}' has been enabled.")


@stream_feature_views_cmd.command("disable")
@click.argument("name", type=click.STRING)
@click.pass_context
def stream_feature_view_disable(ctx: click.Context, name: str):
    """
    Disable a stream feature view.
    """
    store = create_feature_store(ctx)
    try:
        fv = store.get_stream_feature_view(name)
    except FeastObjectNotFoundException as e:
        print(e)
        sys.exit(1)

    if not fv.enabled:
        print(f"Stream feature view '{name}' is already disabled.")
        return

    fv.enabled = False
    store.registry.apply_feature_view(fv, store.project)
    print(f"Stream feature view '{name}' has been disabled.")


@stream_feature_views_cmd.command("set-state")
@click.argument("name", type=click.STRING)
@click.argument(
    "state",
    type=click.Choice(
        ["CREATED", "GENERATED", "MATERIALIZING", "AVAILABLE_ONLINE"],
        case_sensitive=False,
    ),
)
@click.pass_context
def stream_feature_view_set_state(ctx: click.Context, name: str, state: str):
    """
    Set the lifecycle state of a stream feature view.
    """
    store = create_feature_store(ctx)
    try:
        fv = store.get_stream_feature_view(name)
    except FeastObjectNotFoundException as e:
        print(e)
        sys.exit(1)

    new_state = FeatureViewState[state.upper()]
    if fv.state == new_state:
        print(f"Stream feature view '{name}' is already in state {new_state.name}.")
        return

    if not fv.state.can_transition_to(new_state):
        current = fv.state.name
        allowed = _VALID_STATE_TRANSITIONS.get(fv.state, set())
        allowed_names = ", ".join(sorted(s.name for s in allowed)) or "none"
        print(
            f"Invalid state transition: {current} -> {new_state.name}. "
            f"Allowed transitions from {current}: {allowed_names}."
        )
        return

    fv.state = new_state
    store.registry.apply_feature_view(fv, store.project)
    print(f"Stream feature view '{name}' state set to {new_state.name}.")
