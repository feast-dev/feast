from fastapi import APIRouter, Depends, Query

from feast.api.registry.rest.rest_utils import grpc_call, parse_tags
from feast.protos.feast.registry import RegistryServer_pb2


def get_saved_dataset_router(grpc_handler) -> APIRouter:
    router = APIRouter()

    @router.get("/saved_datasets/{name}")
    def get_saved_dataset(
        name: str,
        project: str = Query(...),
        allow_cache: bool = Query(True),
    ):
        req = RegistryServer_pb2.GetSavedDatasetRequest(
            name=name,
            project=project,
            allow_cache=allow_cache,
        )
        return grpc_call(grpc_handler.GetSavedDataset, req)

    @router.get("/saved_datasets")
    def list_saved_datasets(
        project: str = Query(...),
        allow_cache: bool = Query(True),
        tags: dict = Depends(parse_tags),
    ):
        req = RegistryServer_pb2.ListSavedDatasetsRequest(
            project=project,
            allow_cache=allow_cache,
            tags=tags,
        )
        response = grpc_call(grpc_handler.ListSavedDatasets, req)
        return {"saved_datasets": response.get("saved_datasets", [])}

    return router
