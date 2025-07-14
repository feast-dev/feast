from typing import Dict

from fastapi import APIRouter, Depends, Query

from feast.api.registry.rest.rest_utils import (
    aggregate_across_projects,
    create_grpc_pagination_params,
    create_grpc_sorting_params,
    get_object_relationships,
    get_pagination_params,
    get_relationships_for_objects,
    get_sorting_params,
    grpc_call,
    parse_tags,
)
from feast.protos.feast.registry import RegistryServer_pb2


def get_feature_service_router(grpc_handler) -> APIRouter:
    router = APIRouter()

    @router.get("/feature_services")
    def list_feature_services(
        project: str = Query(...),
        include_relationships: bool = Query(
            False, description="Include relationships for each feature service"
        ),
        allow_cache: bool = Query(default=True),
        tags: Dict[str, str] = Depends(parse_tags),
        pagination_params: dict = Depends(get_pagination_params),
        sorting_params: dict = Depends(get_sorting_params),
    ):
        req = RegistryServer_pb2.ListFeatureServicesRequest(
            project=project,
            allow_cache=allow_cache,
            tags=tags,
            pagination=create_grpc_pagination_params(pagination_params),
            sorting=create_grpc_sorting_params(sorting_params),
        )
        response = grpc_call(grpc_handler.ListFeatureServices, req)
        feature_services = response.get("featureServices", [])

        result = {
            "featureServices": feature_services,
            "pagination": response.get("pagination", {}),
        }

        if include_relationships:
            relationships = get_relationships_for_objects(
                grpc_handler, feature_services, "featureService", project, allow_cache
            )
            result["relationships"] = relationships

        return result

    @router.get("/feature_services/all")
    def list_feature_services_all(
        allow_cache: bool = Query(default=True),
        page: int = Query(1, ge=1),
        limit: int = Query(50, ge=1, le=100),
        sort_by: str = Query(None),
        sort_order: str = Query("asc"),
        include_relationships: bool = Query(
            False, description="Include relationships for each feature service"
        ),
    ):
        return aggregate_across_projects(
            grpc_handler=grpc_handler,
            list_method=grpc_handler.ListFeatureServices,
            request_cls=RegistryServer_pb2.ListFeatureServicesRequest,
            response_key="featureServices",
            object_type="featureService",
            allow_cache=allow_cache,
            page=page,
            limit=limit,
            sort_by=sort_by,
            sort_order=sort_order,
            include_relationships=include_relationships,
        )

    @router.get("/feature_services/{name}")
    def get_feature_service(
        name: str,
        project: str = Query(...),
        include_relationships: bool = Query(
            False, description="Include relationships for this feature service"
        ),
        allow_cache: bool = Query(default=True),
    ):
        req = RegistryServer_pb2.GetFeatureServiceRequest(
            name=name,
            project=project,
            allow_cache=allow_cache,
        )
        feature_service = grpc_call(grpc_handler.GetFeatureService, req)

        result = feature_service

        if include_relationships:
            relationships = get_object_relationships(
                grpc_handler, "featureService", name, project, allow_cache
            )
            result["relationships"] = relationships

        return result

    return router
