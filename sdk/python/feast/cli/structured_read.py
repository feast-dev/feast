"""Safe, explicit projections for the first structured read commands.

Provider options, serialized UDFs and configuration are deliberately not exposed.
"""

from __future__ import annotations

from typing import Any

import click

from feast import utils
from feast.cli.output import StructuredError
from feast.errors import FeastObjectNotFoundException, FeatureViewNotFoundException
from feast.feature_view import DUMMY_ENTITY_NAME, FeatureView
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.repo_operations import create_feature_store
from feast.stream_feature_view import StreamFeatureView


def describe_object(obj: Any) -> dict[str, Any]:
    result: dict[str, Any] = {
        "name": obj.name,
        "type": type(obj).__name__,
        "description": getattr(obj, "description", "") or "",
    }
    if hasattr(obj, "join_key"):
        result["join_keys"] = [obj.join_key]
        result["value_type"] = str(obj.value_type)
    if hasattr(obj, "features"):
        result["features"] = sorted(
            [{"name": f.name, "dtype": str(f.dtype)} for f in obj.features],
            key=lambda f: f["name"],
        )
    if hasattr(obj, "entities"):
        result["entities"] = sorted(obj.entities)
    for key in ("enabled", "online"):
        if hasattr(obj, key):
            result[key] = bool(getattr(obj, key))
    if hasattr(obj, "state"):
        result["state"] = obj.state.name
    if hasattr(obj, "ttl"):
        result["ttl_seconds"] = obj.ttl.total_seconds() if obj.ttl is not None else None
    if hasattr(obj, "batch_source"):
        source = obj.batch_source
        result["source"] = (
            {"name": source.name, "type": type(source).__name__} if source else None
        )
    if hasattr(obj, "feature_view_projections"):
        result["feature_references"] = sorted(
            f"{view.name_to_use()}:{feature.name}"
            for view in obj.feature_view_projections
            for feature in view.features
        )
    return result


def read_command(path: str, ctx: click.Context, **params: Any) -> Any:
    resource, action = path.split()
    try:
        tags = utils.tags_list_to_dict(params.get("tags"), strict=True)
    except ValueError as error:
        raise StructuredError(
            "INVALID_ARGUMENT",
            "Invalid tag filter.",
            "Use --tags key:value for each filter, with a non-empty key.",
            exit_code=2,
        ) from error
    store = create_feature_store(ctx)
    if resource == "features":
        views = [
            *store.list_batch_feature_views(),
            *store.list_on_demand_feature_views(),
            *store.list_stream_feature_views(),
        ]
        items = [
            {
                "feature_name": feature.name,
                "feature_view": view.name,
                "dtype": str(feature.dtype),
            }
            for view in views
            for feature in view.features
            if action == "list" or feature.name == params["feature_name"]
        ]
        if action == "describe" and not items:
            raise FeastObjectNotFoundException()
        return {
            "items": sorted(
                items, key=lambda item: (item["feature_view"], item["feature_name"])
            )
        }
    singular = {
        "entities": "entity",
        "feature-views": "feature_view",
        "feature-services": "feature_service",
        "data-sources": "data_source",
    }[resource]
    if action == "describe":
        name = params["name"]
        if resource == "feature-views":
            view = store.registry.get_any_feature_view(name, store.project)
            if isinstance(view, StreamFeatureView) or not isinstance(
                view, (FeatureView, OnDemandFeatureView)
            ):
                raise FeatureViewNotFoundException(name, store.project)
            description = describe_object(view)
            if isinstance(view, FeatureView) and view.entities[:1] == [
                DUMMY_ENTITY_NAME
            ]:
                description["entities"] = []
            return description
        return describe_object(getattr(store, f"get_{singular}")(name))
    if resource == "feature-views":
        objects = [
            *store.list_batch_feature_views(tags=tags),
            *store.list_on_demand_feature_views(tags=tags),
        ]
    else:
        plural = "entities" if resource == "entities" else singular + "s"
        objects = getattr(store, f"list_{plural}")(tags=tags)
    return {
        "items": sorted(
            [describe_object(obj) for obj in objects],
            key=lambda obj: (obj["name"], obj["type"]),
        )
    }
