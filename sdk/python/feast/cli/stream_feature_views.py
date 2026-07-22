import click
import yaml

from feast import utils
from feast.cli.cli_options import tagsOption
from feast.errors import FeastObjectNotFoundException
from feast.feature_view import _VALID_STATE_TRANSITIONS, FeatureViewState
from feast.repo_operations import create_feature_store
from feast.stream_feature_view import StreamFeatureView


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

    print(
        yaml.dump(
            yaml.safe_load(str(stream_feature_view)),
            default_flow_style=False,
            sort_keys=False,
        )
    )


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
def stream_feature_views_enable(ctx: click.Context, name: str):
    """
    [Experimental] Enable a stream feature view for serving and materialization.
    """
    store = create_feature_store(ctx)

    try:
        fv = store.registry.get_any_feature_view(name, store.project)
    except FeastObjectNotFoundException as e:
        print(e)
        exit(1)

    if not isinstance(fv, StreamFeatureView):
        print(f"Feature view '{name}' is not a stream feature view.")
        return

    if fv.enabled:
        print(f"Stream feature view '{name}' is already enabled.")
        return

    fv.enabled = True
    store.registry.apply_feature_view(fv, store.project)
    print(f"Stream feature view '{name}' has been enabled.")


@stream_feature_views_cmd.command("disable")
@click.argument("name", type=click.STRING)
@click.pass_context
def stream_feature_views_disable(ctx: click.Context, name: str):
    """
    [Experimental] Disable a stream feature view for serving and materialization.
    """
    store = create_feature_store(ctx)

    try:
        fv = store.registry.get_any_feature_view(name, store.project)
    except FeastObjectNotFoundException as e:
        print(e)
        exit(1)

    if not isinstance(fv, StreamFeatureView):
        print(f"Feature view '{name}' is not a stream feature view.")
        return

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
def stream_feature_views_set_state(ctx: click.Context, name: str, state: str):
    """
    [Experimental] Set the lifecycle state of a stream feature view.
    """
    store = create_feature_store(ctx)

    try:
        fv = store.registry.get_any_feature_view(name, store.project)
    except FeastObjectNotFoundException as e:
        print(e)
        exit(1)

    if not isinstance(fv, StreamFeatureView):
        print(f"Feature view '{name}' is not a stream feature view.")
        return

    new_state = FeatureViewState[state.upper()]
    if fv.state == new_state:
        print(f"Stream feature view '{name}' is already in state '{new_state.name}'.")
        return

    if not fv.state.can_transition_to(new_state):
        current = fv.state.name
        allowed = _VALID_STATE_TRANSITIONS.get(fv.state, set())
        allowed_names = ", ".join(sorted(s.name for s in allowed)) or "none"
        print(
            f"Invalid state transition: {current} -> {new_state.name} (allowed: {allowed_names})"
            f"Allowed transitions from {current}: {allowed_names}."
        )
        return

    fv.state = new_state
    store.registry.apply_feature_view(fv, store.project)
    print(f"Stream feature view '{name}' state has been set to '{new_state.name}'.")
