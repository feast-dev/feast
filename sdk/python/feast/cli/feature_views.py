import sys

import click
import yaml

from feast import utils
from feast.cli.cli_options import tagsOption
from feast.errors import FeastObjectNotFoundException
from feast.feature_view import (
    _VALID_STATE_TRANSITIONS,
    FeatureView,
    FeatureViewState,
)
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.repo_operations import create_feature_store


@click.group(name="feature-views")
def feature_views_cmd():
    """
    Access feature views
    """
    pass


@feature_views_cmd.command("describe")
@click.argument("name", type=click.STRING)
@click.pass_context
def feature_view_describe(ctx: click.Context, name: str):
    """
    Describe a feature view
    """
    store = create_feature_store(ctx)

    try:
        feature_view = store.get_feature_view(name)
    except FeastObjectNotFoundException as e:
        print(e)
        exit(1)

    data = yaml.safe_load(str(feature_view))
    # Always show enabled and state even when they are at default values.
    if hasattr(feature_view, "enabled"):
        data["enabled"] = feature_view.enabled
    if hasattr(feature_view, "state"):
        data["state"] = feature_view.state.name
    print(yaml.dump(data, default_flow_style=False, sort_keys=False))


@feature_views_cmd.command(name="list")
@tagsOption
@click.pass_context
def feature_view_list(ctx: click.Context, tags: list[str]):
    """
    List all feature views
    """
    store = create_feature_store(ctx)
    table = []
    tags_filter = utils.tags_list_to_dict(tags)
    for feature_view in [
        *store.list_batch_feature_views(tags=tags_filter),
        *store.list_on_demand_feature_views(tags=tags_filter),
    ]:
        entities = set()
        if isinstance(feature_view, FeatureView):
            entities.update(feature_view.entities)
        elif isinstance(feature_view, OnDemandFeatureView):
            for backing_fv in feature_view.source_feature_view_projections.values():
                entities.update(store.get_feature_view(backing_fv.name).entities)
        enabled = getattr(feature_view, "enabled", True)
        state = getattr(feature_view, "state", FeatureViewState.STATE_UNSPECIFIED)
        state_display = (
            state.name if isinstance(state, FeatureViewState) else str(state)
        )
        table.append(
            [
                feature_view.name,
                entities if len(entities) > 0 else "n/a",
                type(feature_view).__name__,
                "Yes" if enabled else "No",
                state_display,
            ]
        )

    from tabulate import tabulate

    print(
        tabulate(
            table,
            headers=["NAME", "ENTITIES", "TYPE", "ENABLED", "STATE"],
            tablefmt="plain",
        )
    )


@feature_views_cmd.command("enable")
@click.argument("name", type=click.STRING)
@click.pass_context
def feature_view_enable(ctx: click.Context, name: str):
    """
    Enable a feature view for serving and materialization.
    """
    store = create_feature_store(ctx)
    try:
        fv = store.registry.get_any_feature_view(name, store.project)
    except FeastObjectNotFoundException as e:
        print(e)
        sys.exit(1)

    if not isinstance(fv, (FeatureView, OnDemandFeatureView)):
        print(f"Feature view '{name}' does not support enable/disable.")
        return

    if fv.enabled:
        print(f"Feature view '{name}' is already enabled.")
        return

    fv.enabled = True
    store.registry.apply_feature_view(fv, store.project)
    print(f"Feature view '{name}' has been enabled.")


@feature_views_cmd.command("disable")
@click.argument("name", type=click.STRING)
@click.pass_context
def feature_view_disable(ctx: click.Context, name: str):
    """
    Disable a feature view to prevent serving and materialization.
    """
    store = create_feature_store(ctx)
    try:
        fv = store.registry.get_any_feature_view(name, store.project)
    except FeastObjectNotFoundException as e:
        print(e)
        sys.exit(1)

    if not isinstance(fv, (FeatureView, OnDemandFeatureView)):
        print(f"Feature view '{name}' does not support enable/disable.")
        return

    if not fv.enabled:
        print(f"Feature view '{name}' is already disabled.")
        return

    fv.enabled = False
    store.registry.apply_feature_view(fv, store.project)
    print(f"Feature view '{name}' has been disabled.")


@feature_views_cmd.command("set-state")
@click.argument("name", type=click.STRING)
@click.argument(
    "state",
    type=click.Choice(
        ["CREATED", "GENERATED", "MATERIALIZING", "AVAILABLE_ONLINE"],
        case_sensitive=False,
    ),
)
@click.pass_context
def feature_view_set_state(ctx: click.Context, name: str, state: str):
    """
    Set the lifecycle state of a feature view.
    """
    store = create_feature_store(ctx)
    try:
        fv = store.registry.get_any_feature_view(name, store.project)
    except FeastObjectNotFoundException as e:
        print(e)
        sys.exit(1)

    if not isinstance(fv, (FeatureView, OnDemandFeatureView)):
        print(f"Feature view '{name}' does not support state management.")
        return

    new_state = FeatureViewState[state.upper()]
    if fv.state == new_state:
        print(f"Feature view '{name}' is already in state {new_state.name}.")
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
    print(f"Feature view '{name}' state set to {new_state.name}.")


@feature_views_cmd.command("list-versions")
@click.argument("name", type=click.STRING)
@click.pass_context
def feature_view_versions(ctx: click.Context, name: str):
    """
    List version history for a feature view
    """
    store = create_feature_store(ctx)

    try:
        versions = store.list_feature_view_versions(name)
    except NotImplementedError:
        print("Version history is not supported by this registry backend.")
        exit(1)
    except Exception as e:
        print(e)
        exit(1)

    if not versions:
        print(f"No version history found for feature view '{name}'.")
        return

    table = []
    for v in versions:
        table.append(
            [
                v["version"],
                v["feature_view_type"],
                str(v["created_timestamp"]),
                v["version_id"],
            ]
        )

    from tabulate import tabulate

    print(
        tabulate(
            table,
            headers=["VERSION", "TYPE", "CREATED", "VERSION_ID"],
            tablefmt="plain",
        )
    )
