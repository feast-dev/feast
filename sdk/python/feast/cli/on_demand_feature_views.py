import sys

import click
import yaml

from feast import utils
from feast.cli.cli_options import tagsOption
from feast.errors import FeastObjectNotFoundException
from feast.feature_view import _VALID_STATE_TRANSITIONS, FeatureViewState
from feast.repo_operations import create_feature_store


@click.group(name="on-demand-feature-views")
def on_demand_feature_views_cmd():
    """
    [Experimental] Access on demand feature views
    """
    pass


@on_demand_feature_views_cmd.command("describe")
@click.argument("name", type=click.STRING)
@click.pass_context
def on_demand_feature_view_describe(ctx: click.Context, name: str):
    """
    [Experimental] Describe an on demand feature view
    """
    store = create_feature_store(ctx)

    try:
        on_demand_feature_view = store.get_on_demand_feature_view(name)
    except FeastObjectNotFoundException as e:
        print(e)
        exit(1)

    data = yaml.safe_load(str(on_demand_feature_view))
    if hasattr(on_demand_feature_view, "enabled"):
        data["enabled"] = on_demand_feature_view.enabled
    if hasattr(on_demand_feature_view, "state"):
        data["state"] = on_demand_feature_view.state.name
    print(yaml.dump(data, default_flow_style=False, sort_keys=False))


@on_demand_feature_views_cmd.command(name="list")
@tagsOption
@click.pass_context
def on_demand_feature_view_list(ctx: click.Context, tags: list[str]):
    """
    [Experimental] List all on demand feature views
    """
    store = create_feature_store(ctx)
    table = []
    tags_filter = utils.tags_list_to_dict(tags)
    for on_demand_feature_view in store.list_on_demand_feature_views(tags=tags_filter):
        table.append([on_demand_feature_view.name])

    from tabulate import tabulate

    print(tabulate(table, headers=["NAME"], tablefmt="plain"))


@on_demand_feature_views_cmd.command("enable")
@click.argument("name", type=click.STRING)
@click.pass_context
def on_demand_feature_view_enable(ctx: click.Context, name: str):
    """
    Enable an on demand feature view.
    """
    store = create_feature_store(ctx)
    try:
        fv = store.get_on_demand_feature_view(name)
    except FeastObjectNotFoundException as e:
        print(e)
        sys.exit(1)

    if fv.enabled:
        print(f"On demand feature view '{name}' is already enabled.")
        return

    fv.enabled = True
    store.registry.apply_feature_view(fv, store.project)
    print(f"On demand feature view '{name}' has been enabled.")


@on_demand_feature_views_cmd.command("disable")
@click.argument("name", type=click.STRING)
@click.pass_context
def on_demand_feature_view_disable(ctx: click.Context, name: str):
    """
    Disable an on demand feature view.
    """
    store = create_feature_store(ctx)
    try:
        fv = store.get_on_demand_feature_view(name)
    except FeastObjectNotFoundException as e:
        print(e)
        sys.exit(1)

    if not fv.enabled:
        print(f"On demand feature view '{name}' is already disabled.")
        return

    fv.enabled = False
    store.registry.apply_feature_view(fv, store.project)
    print(f"On demand feature view '{name}' has been disabled.")


@on_demand_feature_views_cmd.command("set-state")
@click.argument("name", type=click.STRING)
@click.argument(
    "state",
    type=click.Choice(
        ["CREATED", "GENERATED", "MATERIALIZING", "AVAILABLE_ONLINE"],
        case_sensitive=False,
    ),
)
@click.pass_context
def on_demand_feature_view_set_state(ctx: click.Context, name: str, state: str):
    """
    Set the lifecycle state of an on demand feature view.
    """
    store = create_feature_store(ctx)
    try:
        fv = store.get_on_demand_feature_view(name)
    except FeastObjectNotFoundException as e:
        print(e)
        sys.exit(1)

    new_state = FeatureViewState[state.upper()]
    if fv.state == new_state:
        print(f"On demand feature view '{name}' is already in state {new_state.name}.")
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
    print(f"On demand feature view '{name}' state set to {new_state.name}.")
