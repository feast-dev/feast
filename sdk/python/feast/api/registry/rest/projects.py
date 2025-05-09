from fastapi import APIRouter, Query

from feast.api.registry.rest.rest_utils import grpc_call
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
    ):
        req = RegistryServer_pb2.ListProjectsRequest(
            allow_cache=allow_cache,
        )
        response = grpc_call(grpc_handler.ListProjects, req)
        return {"projects": response.get("projects", [])}

    return router
