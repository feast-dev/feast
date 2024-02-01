from concurrent import futures

import grpc
from google.protobuf.empty_pb2 import Empty

from feast import FeatureStore
from feast.protos.feast.registry import RegistryServer_pb2, RegistryServer_pb2_grpc


class RegistryServer(RegistryServer_pb2_grpc.RegistryServerServicer):
    def __init__(self, store: FeatureStore) -> None:
        super().__init__()
        self.proxied_registry = store.registry

    def GetEntity(self, request: RegistryServer_pb2.GetEntityRequest, context):
        return self.proxied_registry.get_entity(
            name=request.name, project=request.project, allow_cache=request.allow_cache
        ).to_proto()

    def ListEntities(self, request, context):
        return RegistryServer_pb2.ListEntitiesResponse(
            entities=[
                entity.to_proto()
                for entity in self.proxied_registry.list_entities(
                    project=request.project, allow_cache=request.allow_cache
                )
            ]
        )

    def GetDataSource(self, request: RegistryServer_pb2.GetDataSourceRequest, context):
        return self.proxied_registry.get_data_source(
            name=request.name, project=request.project, allow_cache=request.allow_cache
        ).to_proto()

    def ListDataSources(self, request, context):
        return RegistryServer_pb2.ListDataSourcesResponse(
            data_sources=[
                data_source.to_proto()
                for data_source in self.proxied_registry.list_data_sources(
                    project=request.project, allow_cache=request.allow_cache
                )
            ]
        )

    def GetFeatureView(
        self, request: RegistryServer_pb2.GetFeatureViewRequest, context
    ):
        return self.proxied_registry.get_feature_view(
            name=request.name, project=request.project, allow_cache=request.allow_cache
        ).to_proto()

    def ListFeatureViews(self, request, context):
        return RegistryServer_pb2.ListFeatureViewsResponse(
            feature_views=[
                feature_view.to_proto()
                for feature_view in self.proxied_registry.list_feature_views(
                    project=request.project, allow_cache=request.allow_cache
                )
            ]
        )

    def GetRequestFeatureView(
        self, request: RegistryServer_pb2.GetRequestFeatureViewRequest, context
    ):
        return self.proxied_registry.get_request_feature_view(
            name=request.name, project=request.project, allow_cache=request.allow_cache
        ).to_proto()

    def ListRequestFeatureViews(self, request, context):
        return RegistryServer_pb2.ListRequestFeatureViewsResponse(
            request_feature_views=[
                request_feature_view.to_proto()
                for request_feature_view in self.proxied_registry.list_request_feature_views(
                    project=request.project, allow_cache=request.allow_cache
                )
            ]
        )

    def GetStreamFeatureView(
        self, request: RegistryServer_pb2.GetStreamFeatureViewRequest, context
    ):
        return self.proxied_registry.get_stream_feature_view(
            name=request.name, project=request.project, allow_cache=request.allow_cache
        ).to_proto()

    def ListStreamFeatureViews(self, request, context):
        return RegistryServer_pb2.ListStreamFeatureViewsResponse(
            stream_feature_views=[
                stream_feature_view.to_proto()
                for stream_feature_view in self.proxied_registry.list_stream_feature_views(
                    project=request.project, allow_cache=request.allow_cache
                )
            ]
        )

    def GetOnDemandFeatureView(
        self, request: RegistryServer_pb2.GetOnDemandFeatureViewRequest, context
    ):
        return self.proxied_registry.get_on_demand_feature_view(
            name=request.name, project=request.project, allow_cache=request.allow_cache
        ).to_proto()

    def ListOnDemandFeatureViews(self, request, context):
        return RegistryServer_pb2.ListOnDemandFeatureViewsResponse(
            on_demand_feature_views=[
                on_demand_feature_view.to_proto()
                for on_demand_feature_view in self.proxied_registry.list_on_demand_feature_views(
                    project=request.project, allow_cache=request.allow_cache
                )
            ]
        )

    def GetFeatureService(
        self, request: RegistryServer_pb2.GetFeatureServiceRequest, context
    ):
        return self.proxied_registry.get_feature_service(
            name=request.name, project=request.project, allow_cache=request.allow_cache
        ).to_proto()

    def ListFeatureServices(
        self, request: RegistryServer_pb2.ListFeatureServicesRequest, context
    ):
        return RegistryServer_pb2.ListFeatureServicesResponse(
            feature_services=[
                feature_service.to_proto()
                for feature_service in self.proxied_registry.list_feature_services(
                    project=request.project, allow_cache=request.allow_cache
                )
            ]
        )

    def GetSavedDataset(
        self, request: RegistryServer_pb2.GetSavedDatasetRequest, context
    ):
        return self.proxied_registry.get_saved_dataset(
            name=request.name, project=request.project, allow_cache=request.allow_cache
        ).to_proto()

    def ListSavedDatasets(
        self, request: RegistryServer_pb2.ListSavedDatasetsRequest, context
    ):
        return RegistryServer_pb2.ListSavedDatasetsResponse(
            saved_datasets=[
                saved_dataset.to_proto()
                for saved_dataset in self.proxied_registry.list_saved_datasets(
                    project=request.project, allow_cache=request.allow_cache
                )
            ]
        )

    def GetValidationReference(
        self, request: RegistryServer_pb2.GetValidationReferenceRequest, context
    ):
        return self.proxied_registry.get_validation_reference(
            name=request.name, project=request.project, allow_cache=request.allow_cache
        ).to_proto()

    def ListValidationReferences(
        self, request: RegistryServer_pb2.ListValidationReferencesRequest, context
    ):
        return RegistryServer_pb2.ListValidationReferencesResponse(
            validation_references=[
                validation_reference.to_proto()
                for validation_reference in self.proxied_registry.list_validation_references(
                    project=request.project, allow_cache=request.allow_cache
                )
            ]
        )

    def ListProjectMetadata(
        self, request: RegistryServer_pb2.ListProjectMetadataRequest, context
    ):
        return RegistryServer_pb2.ListProjectMetadataResponse(
            project_metadata=[
                project_metadata.to_proto()
                for project_metadata in self.proxied_registry.list_project_metadata(
                    project=request.project, allow_cache=request.allow_cache
                )
            ]
        )

    def GetInfra(self, request: RegistryServer_pb2.GetInfraRequest, context):
        return self.proxied_registry.get_infra(
            project=request.project, allow_cache=request.allow_cache
        ).to_proto()

    def Refresh(self, request, context):
        self.proxied_registry.refresh(request.project)
        return Empty()

    def Proto(self, request, context):
        return self.proxied_registry.proto()


def start_server(store: FeatureStore, port: int):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    RegistryServer_pb2_grpc.add_RegistryServerServicer_to_server(
        RegistryServer(store), server
    )
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    server.wait_for_termination()
