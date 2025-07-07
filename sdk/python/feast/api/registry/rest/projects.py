from fastapi import APIRouter, Depends, Query

from feast.api.registry.rest.rest_utils import (
    create_grpc_pagination_params,
    create_grpc_sorting_params,
    get_pagination_params,
    get_sorting_params,
    grpc_call,
)
from feast.protos.feast.registry import RegistryServer_pb2


def get_project_router(grpc_handler) -> APIRouter:
    router = APIRouter()

    @router.get("/projects/{name}")
    def get_project(
        name: str,
        allow_cache: bool = Query(True),
    ):
        req = RegistryServer_pb2.GetProjectRequest(
            name=name,
            allow_cache=allow_cache,
        )
        return grpc_call(grpc_handler.GetProject, req)

    @router.get("/projects")
    def list_projects(
        allow_cache: bool = Query(True),
        pagination_params: dict = Depends(get_pagination_params),
        sorting_params: dict = Depends(get_sorting_params),
    ):
        req = RegistryServer_pb2.ListProjectsRequest(
            allow_cache=allow_cache,
            pagination=create_grpc_pagination_params(pagination_params),
            sorting=create_grpc_sorting_params(sorting_params),
        )

        return grpc_call(grpc_handler.ListProjects, req)

    return router
