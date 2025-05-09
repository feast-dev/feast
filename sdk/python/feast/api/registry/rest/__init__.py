from fastapi import FastAPI

from feast.api.registry.rest.data_sources import get_data_source_router
from feast.api.registry.rest.entities import get_entity_router
from feast.api.registry.rest.feature_services import get_feature_service_router
from feast.api.registry.rest.feature_views import get_feature_view_router
from feast.api.registry.rest.permissions import get_permission_router
from feast.api.registry.rest.projects import get_project_router
from feast.api.registry.rest.saved_datasets import get_saved_dataset_router


def register_all_routes(app: FastAPI, grpc_handler):
    app.include_router(get_entity_router(grpc_handler))
    app.include_router(get_data_source_router(grpc_handler))
    app.include_router(get_feature_service_router(grpc_handler))
    app.include_router(get_feature_view_router(grpc_handler))
    app.include_router(get_permission_router(grpc_handler))
    app.include_router(get_project_router(grpc_handler))
    app.include_router(get_saved_dataset_router(grpc_handler))
