from fastapi import APIRouter, Depends, Query

from feast.api.registry.rest.codegen_utils import render_feature_code
from feast.api.registry.rest.rest_utils import (
    aggregate_across_projects,
    create_grpc_pagination_params,
    create_grpc_sorting_params,
    get_object_relationships,
    get_pagination_params,
    get_relationships_for_objects,
    get_sorting_params,
    grpc_call,
)
from feast.registry_server import RegistryServer_pb2
from feast.type_map import _convert_value_type_str_to_value_type
from feast.types import from_value_type


def get_feature_router(grpc_handler) -> APIRouter:
    router = APIRouter()

    @router.get("/features")
    def list_features(
        project: str = Query(...),
        feature_view: str = Query(None),
        name: str = Query(None),
        include_relationships: bool = Query(
            False, description="Include relationships for each feature"
        ),
        allow_cache: bool = Query(
            True, description="Allow using cached registry data (default: true)"
        ),
        pagination_params: dict = Depends(get_pagination_params),
        sorting_params: dict = Depends(get_sorting_params),
    ):
        req = RegistryServer_pb2.ListFeaturesRequest(
            project=project,
            feature_view=feature_view or "",
            name=name or "",
            allow_cache=allow_cache,
            kind="feature",
            pagination=create_grpc_pagination_params(pagination_params),
            sorting=create_grpc_sorting_params(sorting_params),
        )
        response = grpc_call(grpc_handler.ListFeatures, req)
        if "features" not in response:
            response["features"] = []
        if "pagination" not in response:
            response["pagination"] = {}

        if include_relationships:
            features = response.get("features", [])
            relationships = get_relationships_for_objects(
                grpc_handler, features, "feature", project, allow_cache
            )
            response["relationships"] = relationships
        return response

    @router.get("/features/{feature_view}/{name}")
    def get_feature(
        feature_view: str,
        name: str,
        project: str = Query(...),
        include_relationships: bool = Query(
            False, description="Include relationships for this feature"
        ),
        allow_cache: bool = Query(True),
    ):
        req = RegistryServer_pb2.GetFeatureRequest(
            project=project,
            feature_view=feature_view,
            name=name,
            allow_cache=allow_cache,
        )

        response = grpc_call(grpc_handler.GetFeature, req)

        if include_relationships:
            response["relationships"] = get_object_relationships(
                grpc_handler, "feature", name, project, allow_cache
            )

        if response:
            dtype_str = response.get("type") or response.get("dtype")
            value_type_enum = (
                _convert_value_type_str_to_value_type(dtype_str.upper())
                if dtype_str
                else None
            )
            feast_type = from_value_type(value_type_enum) if value_type_enum else None
            dtype = (
                feast_type.__name__
                if feast_type and hasattr(feast_type, "__name__")
                else "String"
            )
            context = dict(
                name=response.get("name", name),
                dtype=dtype,
                description=response.get("description", ""),
                tags=response.get("tags", response.get("labels", {})) or {},
            )
            response["featureDefinition"] = render_feature_code(context)

        return response

    @router.get("/features/all")
    def list_features_all(
        page: int = Query(1, ge=1),
        limit: int = Query(50, ge=1, le=100),
        sort_by: str = Query(None),
        sort_order: str = Query("asc"),
        include_relationships: bool = Query(
            False, description="Include relationships for each feature"
        ),
        allow_cache: bool = Query(
            True, description="Allow using cached registry data (default: true)"
        ),
    ):
        return aggregate_across_projects(
            grpc_handler=grpc_handler,
            list_method=grpc_handler.ListFeatures,
            request_cls=RegistryServer_pb2.ListFeaturesRequest,
            response_key="features",
            object_type="feature",
            include_relationships=include_relationships,
            allow_cache=allow_cache,
            page=page,
            limit=limit,
            sort_by=sort_by,
            sort_order=sort_order,
            extra_request_params={"kind": "feature"},
        )

    # --- Labels endpoints (items from LabelViews only) ---

    @router.get("/labels")
    def list_labels(
        project: str = Query(...),
        feature_view: str = Query(None, description="Filter by label view name"),
        name: str = Query(None, description="Filter by label name"),
        include_relationships: bool = Query(
            False, description="Include relationships for each label"
        ),
        allow_cache: bool = Query(True),
        pagination_params: dict = Depends(get_pagination_params),
        sorting_params: dict = Depends(get_sorting_params),
    ):
        req = RegistryServer_pb2.ListFeaturesRequest(
            project=project,
            feature_view=feature_view or "",
            name=name or "",
            allow_cache=allow_cache,
            kind="label",
            pagination=create_grpc_pagination_params(pagination_params),
            sorting=create_grpc_sorting_params(sorting_params),
        )
        response = grpc_call(grpc_handler.ListFeatures, req)
        if "features" not in response:
            response["features"] = []
        if "pagination" not in response:
            response["pagination"] = {}

        # Rename key for clarity in the response
        response["labels"] = response.pop("features")

        if include_relationships:
            labels = response.get("labels", [])
            relationships = get_relationships_for_objects(
                grpc_handler, labels, "feature", project, allow_cache
            )
            response["relationships"] = relationships
        return response

    @router.get("/labels/all")
    def list_labels_all(
        page: int = Query(1, ge=1),
        limit: int = Query(50, ge=1, le=100),
        sort_by: str = Query(None),
        sort_order: str = Query("asc"),
        include_relationships: bool = Query(
            False, description="Include relationships for each label"
        ),
        allow_cache: bool = Query(True),
    ):
        result = aggregate_across_projects(
            grpc_handler=grpc_handler,
            list_method=grpc_handler.ListFeatures,
            request_cls=RegistryServer_pb2.ListFeaturesRequest,
            response_key="features",
            object_type="feature",
            include_relationships=include_relationships,
            allow_cache=allow_cache,
            page=page,
            limit=limit,
            sort_by=sort_by,
            sort_order=sort_order,
            extra_request_params={"kind": "label"},
        )
        if "features" in result:
            result["labels"] = result.pop("features")
        return result

    return router
