from typing import Dict

from fastapi import APIRouter, Depends, Query

from feast.api.registry.rest.rest_utils import (
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


def get_saved_dataset_router(grpc_handler) -> APIRouter:
    router = APIRouter()

    @router.get("/saved_datasets/{name}")
    def get_saved_dataset(
        name: str,
        project: str = Query(...),
        include_relationships: bool = Query(
            False, description="Include relationships for this saved dataset"
        ),
        allow_cache: bool = Query(True),
    ):
        req = RegistryServer_pb2.GetSavedDatasetRequest(
            name=name,
            project=project,
            allow_cache=allow_cache,
        )
        saved_dataset = grpc_call(grpc_handler.GetSavedDataset, req)

        result = saved_dataset

        # Note: saved datasets may not have relationships in the traditional sense
        # but we include the functionality for consistency
        if include_relationships:
            relationships = get_object_relationships(
                grpc_handler, "savedDataset", name, project, allow_cache
            )
            result["relationships"] = relationships

        return result

    @router.get("/saved_datasets")
    def list_saved_datasets(
        project: str = Query(...),
        allow_cache: bool = Query(default=True),
        tags: Dict[str, str] = Depends(parse_tags),
        include_relationships: bool = Query(
            False, description="Include relationships for each saved dataset"
        ),
        pagination_params: dict = Depends(get_pagination_params),
        sorting_params: dict = Depends(get_sorting_params),
    ):
        req = RegistryServer_pb2.ListSavedDatasetsRequest(
            project=project,
            allow_cache=allow_cache,
            tags=tags,
            pagination=create_grpc_pagination_params(pagination_params),
            sorting=create_grpc_sorting_params(sorting_params),
        )
        response = grpc_call(grpc_handler.ListSavedDatasets, req)
        saved_datasets = response.get("savedDatasets", [])

        result = {
            "saved_datasets": saved_datasets,
            "pagination": response.get("pagination", {}),
        }

        if include_relationships:
            relationships = get_relationships_for_objects(
                grpc_handler, saved_datasets, "savedDataset", project, allow_cache
            )
            result["relationships"] = relationships

        return result

    return router
