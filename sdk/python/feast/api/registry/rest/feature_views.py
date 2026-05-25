import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, Query
from fastapi.responses import JSONResponse
from google.protobuf.duration_pb2 import Duration
from pydantic import BaseModel

from feast.api.registry.rest.codegen_utils import render_feature_view_code
from feast.api.registry.rest.rest_utils import (
    create_grpc_pagination_params,
    create_grpc_sorting_params,
    get_object_relationships,
    get_pagination_params,
    get_relationships_for_objects,
    get_sorting_params,
    grpc_call,
    paginate_and_sort,
    parse_tags,
)
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.protos.feast.core.Feature_pb2 import FeatureSpecV2
from feast.protos.feast.core.FeatureView_pb2 import FeatureView as FeatureViewProto
from feast.protos.feast.core.FeatureView_pb2 import FeatureViewSpec
from feast.registry_server import RegistryServer_pb2
from feast.type_map import _convert_value_type_str_to_value_type
from feast.types import from_value_type

logger = logging.getLogger(__name__)


class FeatureModel(BaseModel):
    name: str
    value_type: int = 2


class ApplyFeatureViewRequestBody(BaseModel):
    name: str
    project: str
    entities: Optional[List[str]] = []
    features: Optional[List[FeatureModel]] = []
    batch_source: Optional[str] = ""
    ttl_seconds: Optional[int] = None
    online: Optional[bool] = True
    description: Optional[str] = ""
    tags: Optional[Dict[str, str]] = {}
    owner: Optional[str] = ""


def _extract_feature_view_from_any(any_feature_view: dict) -> dict:
    """Extract the specific feature view type and data from an AnyFeatureView object.

    Args:
        any_feature_view: Dictionary containing the AnyFeatureView data

    Returns:
        Dictionary with 'type' and feature view data, or empty dict if no valid type found
    """
    for key, value in any_feature_view.items():
        if value:
            return {"type": key, **value}

    return {}


def extract_feast_types_from_fields(fields):
    types = set()
    for field in fields:
        value_type_enum = _convert_value_type_str_to_value_type(
            field.get("valueType", "").upper()
        )
        feast_type = from_value_type(value_type_enum)
        dtype = (
            feast_type.__name__ if hasattr(feast_type, "__name__") else str(feast_type)
        )
        types.add(dtype)
    return list(types)


def get_feature_view_router(grpc_handler) -> APIRouter:
    router = APIRouter()

    @router.get("/feature_views/all")
    def list_feature_views_all(
        allow_cache: bool = Query(default=True),
        page: int = Query(1, ge=1),
        limit: int = Query(50, ge=1, le=100),
        sort_by: str = Query(None),
        sort_order: str = Query("asc"),
        include_relationships: bool = Query(
            False, description="Include relationships for each feature view"
        ),
    ):
        projects_resp = grpc_call(
            grpc_handler.ListProjects,
            RegistryServer_pb2.ListProjectsRequest(allow_cache=allow_cache),
        )
        projects = projects_resp.get("projects", [])
        all_feature_views = []
        for project in projects:
            project_name = project["spec"]["name"]
            req = RegistryServer_pb2.ListAllFeatureViewsRequest(
                project=project_name,
                allow_cache=allow_cache,
            )
            response = grpc_call(grpc_handler.ListAllFeatureViews, req)
            any_feature_views = response.get("featureViews", [])
            for any_feature_view in any_feature_views:
                feature_view = _extract_feature_view_from_any(any_feature_view)
                if feature_view:
                    feature_view["project"] = project_name
                    all_feature_views.append(feature_view)
        paged_feature_views, pagination = paginate_and_sort(
            all_feature_views, page, limit, sort_by, sort_order
        )
        result = {
            "featureViews": paged_feature_views,
            "pagination": pagination,
        }
        if include_relationships:
            relationships_map = {}
            for project in projects:
                project_name = project["spec"]["name"]
                # Filter feature views for this project
                project_feature_views = [
                    fv for fv in all_feature_views if fv["project"] == project_name
                ]
                rels = get_relationships_for_objects(
                    grpc_handler,
                    project_feature_views,
                    "featureView",
                    project_name,
                    allow_cache,
                )
                relationships_map.update(rels)
            result["relationships"] = relationships_map
        return result

    @router.get("/feature_views/{name}")
    def get_any_feature_view(
        name: str,
        project: str = Query(...),
        include_relationships: bool = Query(
            False, description="Include relationships for this feature view"
        ),
        allow_cache: bool = Query(True),
    ):
        req = RegistryServer_pb2.GetAnyFeatureViewRequest(
            name=name,
            project=project,
            allow_cache=allow_cache,
        )
        response = grpc_call(grpc_handler.GetAnyFeatureView, req)
        any_feature_view = response.get("anyFeatureView", {})

        result = _extract_feature_view_from_any(any_feature_view)

        if include_relationships:
            relationships = get_object_relationships(
                grpc_handler, "featureView", name, project, allow_cache
            )
            result["relationships"] = relationships

        if result and "spec" in result:
            spec = result["spec"]
            fv_type = result.get("type", "featureView")
            features = spec.get("features", [])
            if not isinstance(features, list):
                features = []
            if fv_type == "onDemandFeatureView":
                class_name = "OnDemandFeatureView"
                sources = spec.get("sources", {})
                if sources:
                    source_exprs = []
                    for k, v in sources.items():
                        var_name = v.get("name", k) if isinstance(v, dict) else str(v)
                        source_exprs.append(f'"{k}": {var_name}')
                    source_name = "{" + ", ".join(source_exprs) + "}"
                else:
                    source_name = "source_feature_view"
            elif fv_type == "streamFeatureView":
                class_name = "StreamFeatureView"
                stream_source = spec.get("streamSource", {})
                source_name = stream_source.get("name", "stream_source")
            else:
                class_name = "FeatureView"
                source_views = spec.get("source_views") or spec.get("sourceViews")
                if source_views and isinstance(source_views, list):
                    source_vars = [sv.get("name", "source_view") for sv in source_views]
                    source_name = "[" + ", ".join(source_vars) + "]"
                else:
                    source = spec.get("source")
                    if isinstance(source, dict) and source.get("name"):
                        source_name = source["name"]
                    else:
                        batch_source = spec.get("batchSource", {})
                        source_name = batch_source.get("name", "driver_stats_source")

            # Entities
            entities = spec.get("entities") or []
            entities_str = ", ".join(entities)

            # Feature schema
            schema_lines = []
            for field in features:
                value_type_enum = _convert_value_type_str_to_value_type(
                    field.get("valueType", "").upper()
                )
                feast_type = from_value_type(value_type_enum)
                dtype = getattr(feast_type, "__name__", str(feast_type))
                desc = field.get("description")
                desc_str = f', description="{desc}"' if desc else ""
                schema_lines.append(
                    f'        Field(name="{field["name"]}", dtype={dtype}{desc_str}),'
                )

            # Feast types
            feast_types = extract_feast_types_from_fields(features)

            # Tags
            tags = spec.get("tags", {})
            tags_str = f"tags={tags}," if tags else ""

            # TTL
            ttl = spec.get("ttl")
            ttl_str = "timedelta(days=1)"
            if ttl:
                if isinstance(ttl, int):
                    ttl_str = f"timedelta(seconds={ttl})"
                elif isinstance(ttl, str) and ttl.endswith("s") and ttl[:-1].isdigit():
                    ttl_str = f"timedelta(seconds={int(ttl[:-1])})"

            # Online
            online = spec.get("online", True)

            # Build context
            context = dict(
                class_name=class_name,
                name=spec.get("name", "example"),
                entities_str=entities_str,
                ttl_str=ttl_str,
                schema_lines=schema_lines,
                online=online,
                source_name=source_name,
                tags_str=tags_str,
                feast_types=feast_types,
            )

            result["featureDefinition"] = render_feature_view_code(context)

        return result

    @router.get("/feature_views")
    def list_all_feature_views(
        project: str = Query(...),
        allow_cache: bool = Query(default=True),
        include_relationships: bool = Query(
            False, description="Include relationships for each feature view"
        ),
        entity: str = Query(None, description="Filter feature views by entity name"),
        feature: str = Query(None, description="Filter feature views by feature name"),
        feature_service: str = Query(
            None, description="Filter feature views by feature service name"
        ),
        data_source: str = Query(
            None, description="Filter feature views by data source name"
        ),
        tags: Dict[str, str] = Depends(parse_tags),
        pagination_params: dict = Depends(get_pagination_params),
        sorting_params: dict = Depends(get_sorting_params),
    ):
        req = RegistryServer_pb2.ListAllFeatureViewsRequest(
            project=project,
            allow_cache=allow_cache,
            tags=tags,
            entity=entity,
            feature=feature,
            feature_service=feature_service,
            data_source=data_source,
            pagination=create_grpc_pagination_params(pagination_params),
            sorting=create_grpc_sorting_params(sorting_params),
        )
        response = grpc_call(grpc_handler.ListAllFeatureViews, req)
        any_feature_views = response.get("featureViews", [])

        # Extract the specific type of feature view from each AnyFeatureView
        feature_views = []
        for any_feature_view in any_feature_views:
            feature_view = _extract_feature_view_from_any(any_feature_view)
            if feature_view:
                feature_views.append(feature_view)

        result = {
            "featureViews": feature_views,
            "pagination": response.get("pagination", {}),
        }

        if include_relationships:
            relationships = get_relationships_for_objects(
                grpc_handler, feature_views, "featureView", project, allow_cache
            )
            result["relationships"] = relationships

        return result

    @router.post("/feature_views", status_code=201)
    def apply_feature_view(body: ApplyFeatureViewRequestBody):
        feature_specs = []
        for f in body.features or []:
            feature_specs.append(FeatureSpecV2(name=f.name, value_type=f.value_type))

        batch_source_proto = (
            DataSourceProto(name=body.batch_source) if body.batch_source else None
        )

        ttl = (
            Duration(seconds=body.ttl_seconds) if body.ttl_seconds is not None else None
        )

        spec = FeatureViewSpec(
            name=body.name,
            entities=body.entities or [],
            features=feature_specs,
            tags=body.tags or {},
            online=body.online if body.online is not None else True,
            description=body.description or "",
            owner=body.owner or "",
        )
        if ttl is not None:
            spec.ttl.CopyFrom(ttl)
        if batch_source_proto:
            spec.batch_source.CopyFrom(batch_source_proto)

        fv_proto = FeatureViewProto(spec=spec)

        req = RegistryServer_pb2.ApplyFeatureViewRequest(
            feature_view=fv_proto,
            project=body.project,
            commit=True,
        )
        grpc_call(grpc_handler.ApplyFeatureView, req)

        return JSONResponse(
            status_code=201,
            content={
                "name": body.name,
                "project": body.project,
                "status": "applied",
            },
        )

    @router.put("/feature_views/{name}/enable")
    def enable_feature_view(
        name: str,
        project: str = Query(...),
    ):
        from feast.feature_view import FeatureView
        from feast.on_demand_feature_view import OnDemandFeatureView
        from feast.stream_feature_view import StreamFeatureView

        req = RegistryServer_pb2.GetAnyFeatureViewRequest(
            name=name,
            project=project,
        )
        resp = grpc_call(grpc_handler.GetAnyFeatureView, req)
        any_fv = resp.get("anyFeatureView", {})

        from google.protobuf.json_format import ParseDict

        fv: Any = None
        proto: Any = None
        if "featureView" in any_fv and any_fv["featureView"]:
            proto = FeatureViewProto()
            ParseDict(any_fv["featureView"], proto)
            fv = FeatureView.from_proto(proto)
        elif "onDemandFeatureView" in any_fv and any_fv["onDemandFeatureView"]:
            from feast.protos.feast.core.OnDemandFeatureView_pb2 import (
                OnDemandFeatureView as OnDemandFeatureViewProto,
            )

            proto = OnDemandFeatureViewProto()
            ParseDict(any_fv["onDemandFeatureView"], proto)
            fv = OnDemandFeatureView.from_proto(proto)
        elif "streamFeatureView" in any_fv and any_fv["streamFeatureView"]:
            from feast.protos.feast.core.StreamFeatureView_pb2 import (
                StreamFeatureView as StreamFeatureViewProto,
            )

            proto = StreamFeatureViewProto()
            ParseDict(any_fv["streamFeatureView"], proto)
            fv = StreamFeatureView.from_proto(proto)
        else:
            return JSONResponse(
                status_code=404,
                content={"error": f"Feature view '{name}' not found."},
            )

        fv.enabled = True
        apply_req = RegistryServer_pb2.ApplyFeatureViewRequest(
            project=project, commit=True
        )
        if isinstance(fv, StreamFeatureView):
            apply_req.stream_feature_view.CopyFrom(fv.to_proto())
        elif isinstance(fv, OnDemandFeatureView):
            apply_req.on_demand_feature_view.CopyFrom(fv.to_proto())
        else:
            apply_req.feature_view.CopyFrom(fv.to_proto())
        grpc_call(grpc_handler.ApplyFeatureView, apply_req)

        return {"name": name, "project": project, "enabled": True}

    @router.put("/feature_views/{name}/disable")
    def disable_feature_view(
        name: str,
        project: str = Query(...),
    ):
        from feast.feature_view import FeatureView
        from feast.on_demand_feature_view import OnDemandFeatureView
        from feast.stream_feature_view import StreamFeatureView

        req = RegistryServer_pb2.GetAnyFeatureViewRequest(
            name=name,
            project=project,
        )
        resp = grpc_call(grpc_handler.GetAnyFeatureView, req)
        any_fv = resp.get("anyFeatureView", {})

        from google.protobuf.json_format import ParseDict

        fv: Any = None
        proto: Any = None
        if "featureView" in any_fv and any_fv["featureView"]:
            proto = FeatureViewProto()
            ParseDict(any_fv["featureView"], proto)
            fv = FeatureView.from_proto(proto)
        elif "onDemandFeatureView" in any_fv and any_fv["onDemandFeatureView"]:
            from feast.protos.feast.core.OnDemandFeatureView_pb2 import (
                OnDemandFeatureView as OnDemandFeatureViewProto,
            )

            proto = OnDemandFeatureViewProto()
            ParseDict(any_fv["onDemandFeatureView"], proto)
            fv = OnDemandFeatureView.from_proto(proto)
        elif "streamFeatureView" in any_fv and any_fv["streamFeatureView"]:
            from feast.protos.feast.core.StreamFeatureView_pb2 import (
                StreamFeatureView as StreamFeatureViewProto,
            )

            proto = StreamFeatureViewProto()
            ParseDict(any_fv["streamFeatureView"], proto)
            fv = StreamFeatureView.from_proto(proto)
        else:
            return JSONResponse(
                status_code=404,
                content={"error": f"Feature view '{name}' not found."},
            )

        fv.enabled = False
        apply_req = RegistryServer_pb2.ApplyFeatureViewRequest(
            project=project, commit=True
        )
        if isinstance(fv, StreamFeatureView):
            apply_req.stream_feature_view.CopyFrom(fv.to_proto())
        elif isinstance(fv, OnDemandFeatureView):
            apply_req.on_demand_feature_view.CopyFrom(fv.to_proto())
        else:
            apply_req.feature_view.CopyFrom(fv.to_proto())
        grpc_call(grpc_handler.ApplyFeatureView, apply_req)

        return {"name": name, "project": project, "enabled": False}

    @router.put("/feature_views/{name}/set-state")
    def set_feature_view_state(
        name: str,
        state: str = Query(...),
        project: str = Query(...),
    ):
        from feast.feature_view import (
            _VALID_STATE_TRANSITIONS,
            FeatureView,
            FeatureViewState,
        )
        from feast.on_demand_feature_view import OnDemandFeatureView
        from feast.stream_feature_view import StreamFeatureView

        try:
            new_state = FeatureViewState[state.upper()]
        except KeyError:
            return JSONResponse(
                status_code=400,
                content={
                    "error": f"Invalid state '{state}'. "
                    f"Valid states: CREATED, GENERATED, MATERIALIZING, AVAILABLE_ONLINE."
                },
            )

        req = RegistryServer_pb2.GetAnyFeatureViewRequest(
            name=name,
            project=project,
        )
        resp = grpc_call(grpc_handler.GetAnyFeatureView, req)
        any_fv = resp.get("anyFeatureView", {})

        from google.protobuf.json_format import ParseDict

        fv: Any = None
        proto: Any = None
        if "featureView" in any_fv and any_fv["featureView"]:
            proto = FeatureViewProto()
            ParseDict(any_fv["featureView"], proto)
            fv = FeatureView.from_proto(proto)
        elif "onDemandFeatureView" in any_fv and any_fv["onDemandFeatureView"]:
            from feast.protos.feast.core.OnDemandFeatureView_pb2 import (
                OnDemandFeatureView as OnDemandFeatureViewProto,
            )

            proto = OnDemandFeatureViewProto()
            ParseDict(any_fv["onDemandFeatureView"], proto)
            fv = OnDemandFeatureView.from_proto(proto)
        elif "streamFeatureView" in any_fv and any_fv["streamFeatureView"]:
            from feast.protos.feast.core.StreamFeatureView_pb2 import (
                StreamFeatureView as StreamFeatureViewProto,
            )

            proto = StreamFeatureViewProto()
            ParseDict(any_fv["streamFeatureView"], proto)
            fv = StreamFeatureView.from_proto(proto)
        else:
            return JSONResponse(
                status_code=404,
                content={"error": f"Feature view '{name}' not found."},
            )

        if not fv.state.can_transition_to(new_state):
            current = fv.state.name
            allowed = _VALID_STATE_TRANSITIONS.get(fv.state, set())
            allowed_names = ", ".join(sorted(s.name for s in allowed)) or "none"
            return JSONResponse(
                status_code=400,
                content={
                    "error": f"Invalid state transition: {current} -> {new_state.name}. "
                    f"Allowed transitions from {current}: {allowed_names}."
                },
            )

        fv.state = new_state
        apply_req = RegistryServer_pb2.ApplyFeatureViewRequest(
            project=project, commit=True
        )
        if isinstance(fv, StreamFeatureView):
            apply_req.stream_feature_view.CopyFrom(fv.to_proto())
        elif isinstance(fv, OnDemandFeatureView):
            apply_req.on_demand_feature_view.CopyFrom(fv.to_proto())
        else:
            apply_req.feature_view.CopyFrom(fv.to_proto())
        grpc_call(grpc_handler.ApplyFeatureView, apply_req)

        return {"name": name, "project": project, "state": new_state.name}

    @router.delete("/feature_views/{name}")
    def delete_feature_view(
        name: str,
        project: str = Query(...),
    ):
        req = RegistryServer_pb2.DeleteFeatureViewRequest(
            name=name,
            project=project,
            commit=True,
        )
        grpc_call(grpc_handler.DeleteFeatureView, req)

        return {"name": name, "project": project, "status": "deleted"}

    return router
