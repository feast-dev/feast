from fastapi import APIRouter, Depends, Query

from feast.api.registry.rest.rest_utils import (
    get_pagination_params,
    get_sorting_params,
    grpc_call,
    list_all_projects,
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
        project = grpc_call(grpc_handler.GetProject, req)

        return project

    @router.get("/projects")
    def list_projects(
        allow_cache: bool = Query(True),
        pagination_params: dict = Depends(get_pagination_params),
        sorting_params: dict = Depends(get_sorting_params),
    ):
        try:
            projects, pagination, err_msg = list_all_projects(
                grpc_handler=grpc_handler,
                allow_cache=allow_cache,
                pagination_params=pagination_params,
                sorting_params=sorting_params,
            )
        except Exception as e:
            return {"error": str(e)}

        if err_msg:
            return {"error": err_msg}

        return {
            "projects": projects,
            "pagination": pagination,
        }

    return router
