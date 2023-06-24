import grpc
from concurrent import futures
from datetime import datetime

from google.protobuf.empty_pb2 import Empty
from feast.protos.feast.registry import RegistryService_pb2_grpc
from feast.protos.feast.registry import RegistryService_pb2
from feast.entity import Entity
from feast.data_source import DataSource
from feast.feature_view import FeatureView
from feast.feature_service import FeatureService
from feast.saved_dataset import SavedDataset, ValidationReference
from feast.infra.infra_object import Infra

## TODO autogenerate methods

class RegistryServer(RegistryService_pb2_grpc.RegistryServiceServicer):
  def __init__(self) -> None:
     super().__init__()

     from feast.infra.registry.sql import SqlRegistry, SqlRegistryConfig

     self.proxied_registry = SqlRegistry(
        registry_config=SqlRegistryConfig(
         registry_type="sql",
         registry_store_type=None,
         path="sqlite:///feast.db",
         cache_ttl_seconds=60
        ),
        project='test_project',
        repo_path=None
     )
  
  def ApplyEntity(self, request: RegistryService_pb2.ApplyEntityRequest, context):
     self.proxied_registry.apply_entity(
        entity= Entity.from_proto(request.entity),
        project=request.project,
        commit=request.commit
     )
     return Empty()

  def GetEntity(self, request: RegistryService_pb2.GetEntityRequest, context):
     return self.proxied_registry.get_entity(
        name=request.name,
        project=request.project,
        allow_cache=request.allow_cache
     ).to_proto()
  
  def DeleteEntity(self, request: RegistryService_pb2.DeleteEntityRequest, context):
     self.proxied_registry.delete_entity(
        name=request.name,
        project=request.project,
        commit=request.commit
     )
     return Empty()
  
  def ListEntities(self, request, context):
     return RegistryService_pb2.ListEntitiesResponse(
        entities=[entity.to_proto() for entity in self.proxied_registry.list_entities(
        project=request.project,
        allow_cache=request.allow_cache
     )])
  
  def ApplyDataSource(self, request: RegistryService_pb2.ApplyDataSourceRequest, context):
     self.proxied_registry.apply_data_source(
        data_source=DataSource.from_proto(request.data_source),
        project=request.project,
        commit=request.commit
     )
     return Empty()

  def GetDataSource(self, request: RegistryService_pb2.GetDataSourceRequest, context):
     return self.proxied_registry.get_data_source(
        name=request.name,
        project=request.project,
        allow_cache=request.allow_cache
     ).to_proto()
  
  def DeleteDataSource(self, request: RegistryService_pb2.DeleteDataSourceRequest, context):
     self.proxied_registry.delete_data_source(
        name=request.name,
        project=request.project,
        commit=request.commit
     )
     return Empty()
  
  def ListDataSources(self, request, context):
     return RegistryService_pb2.ListDataSourcesResponse(
        data_sources=[data_source.to_proto() for data_source in self.proxied_registry.list_data_sources(
        project=request.project,
        allow_cache=request.allow_cache
     )])

  def ApplyFeatureView(self, request: RegistryService_pb2.ApplyFeatureViewRequest, context):
     self.proxied_registry.apply_feature_view(
        feature_view=FeatureView.from_proto(request.feature_view),
        project=request.project,
        commit=request.commit
     )
     return Empty()

  def GetFeatureView(self, request: RegistryService_pb2.GetFeatureViewRequest, context):
     return self.proxied_registry.get_feature_view(
        name=request.name,
        project=request.project,
        allow_cache=request.allow_cache
     ).to_proto()
  
  def DeleteFeatureView(self, request: RegistryService_pb2.DeleteFeatureViewRequest, context):
     self.proxied_registry.delete_feature_view(
        name=request.name,
        project=request.project,
        commit=request.commit
     )
     return Empty()
  
  def ListFeatureViews(self, request, context):
     return RegistryService_pb2.ListFeatureViewsResponse(
        feature_views=[feature_view.to_proto() for feature_view in self.proxied_registry.list_feature_views(
        project=request.project,
        allow_cache=request.allow_cache
     )])
   
  def GetRequestFeatureView(self, request: RegistryService_pb2.GetRequestFeatureViewRequest, context):
     return self.proxied_registry.get_request_feature_view(
        name=request.name,
        project=request.project,
        allow_cache=request.allow_cache
     ).to_proto() 

  def ListRequestFeatureViews(self, request, context):
     return RegistryService_pb2.ListRequestFeatureViewsResponse(
        request_feature_views=[request_feature_view.to_proto() for request_feature_view in self.proxied_registry.list_request_feature_views(
        project=request.project,
        allow_cache=request.allow_cache
     )])
  
  def GetStreamFeatureView(self, request: RegistryService_pb2.GetStreamFeatureViewRequest, context):
     return self.proxied_registry.get_stream_feature_view(
        name=request.name,
        project=request.project,
        allow_cache=request.allow_cache
     ).to_proto() 

  def ListStreamFeatureViews(self, request, context):
     return RegistryService_pb2.ListStreamFeatureViewsResponse(
        stream_feature_views=[stream_feature_view.to_proto() for stream_feature_view in self.proxied_registry.list_stream_feature_views(
        project=request.project,
        allow_cache=request.allow_cache
     )])

  def GetOnDemandFeatureView(self, request: RegistryService_pb2.GetOnDemandFeatureViewRequest, context):
     return self.proxied_registry.get_on_demand_feature_view(
        name=request.name,
        project=request.project,
        allow_cache=request.allow_cache
     ).to_proto() 

  def ListOnDemandFeatureViews(self, request, context):
     return RegistryService_pb2.ListOnDemandFeatureViewsResponse(
        on_demand_feature_views=[on_demand_feature_view.to_proto() for on_demand_feature_view in self.proxied_registry.list_on_demand_feature_views(
        project=request.project,
        allow_cache=request.allow_cache
     )])

  def ApplyFeatureService(self, request: RegistryService_pb2.ApplyFeatureServiceRequest, context):
     self.proxied_registry.apply_feature_service(
        feature_service=FeatureService.from_proto(request.feature_service),
        project=request.project,
        commit=request.commit
     )
     return Empty()

  def GetFeatureService(self, request: RegistryService_pb2.GetFeatureServiceRequest, context):
     return self.proxied_registry.get_feature_service(
        name=request.name,
        project=request.project,
        allow_cache=request.allow_cache
     ).to_proto()  

  def DeleteFeatureService(self, request: RegistryService_pb2.DeleteFeatureServiceRequest, context):
     self.proxied_registry.delete_feature_service(
        name=request.name,
        project=request.project,
        commit=request.commit
     )
     return Empty()
  
  def ListFeatureServices(self, request: RegistryService_pb2.ListFeatureServicesRequest, context):
     return RegistryService_pb2.ListFeatureServicesResponse(
        feature_services=[feature_service.to_proto() for feature_service in self.proxied_registry.list_feature_services(
        project=request.project,
        allow_cache=request.allow_cache
     )])
  
  def ApplySavedDataset(self, request: RegistryService_pb2.ApplySavedDatasetRequest, context):
     self.proxied_registry.apply_saved_dataset(
        saved_dataset=SavedDataset.from_proto(request.saved_dataset),
        project=request.project,
        commit=request.commit
     )
     return Empty()

  def GetSavedDataset(self, request: RegistryService_pb2.GetSavedDatasetRequest, context):
     return self.proxied_registry.get_saved_dataset(
        name=request.name,
        project=request.project,
        allow_cache=request.allow_cache
     ).to_proto()  

  def DeleteSavedDataset(self, request: RegistryService_pb2.DeleteSavedDatasetRequest, context):
     self.proxied_registry.delete_saved_dataset(
        name=request.name,
        project=request.project,
        commit=request.commit
     )
     return Empty()
  
  def ListSavedDatasets(self, request: RegistryService_pb2.ListSavedDatasetsRequest, context):
     return RegistryService_pb2.ListSavedDatasetsResponse(
        saved_datasets=[saved_dataset.to_proto() for saved_dataset in self.proxied_registry.list_saved_datasets(
        project=request.project,
        allow_cache=request.allow_cache
     )])

  def ApplyValidationReference(self, request: RegistryService_pb2.ApplyValidationReferenceRequest, context):
     self.proxied_registry.apply_validation_reference(
        validation_reference=ValidationReference.from_proto(request.validation_reference),
        project=request.project,
        commit=request.commit
     )
     return Empty()

  def GetValidationReference(self, request: RegistryService_pb2.GetValidationReferenceRequest, context):
     return self.proxied_registry.get_validation_reference(
        name=request.name,
        project=request.project,
        allow_cache=request.allow_cache
     ).to_proto()  

  def DeleteValidationReference(self, request: RegistryService_pb2.DeleteValidationReferenceRequest, context):
     self.proxied_registry.delete_validation_reference(
        name=request.name,
        project=request.project,
        commit=request.commit
     )
     return Empty()
  
  def ListValidationReferences(self, request: RegistryService_pb2.ListValidationReferencesRequest, context):
     return RegistryService_pb2.ListValidationReferencesResponse(
        validation_references=[validation_reference.to_proto() for validation_reference in self.proxied_registry.list_validation_references(
        project=request.project,
        allow_cache=request.allow_cache
     )])

  def ApplyMaterialization(self, request: RegistryService_pb2.ApplyMaterializationRequest, context):
     self.proxied_registry.apply_materialization(
        feature_view=FeatureView.from_proto(request.feature_view),
        project=request.project,
        start_date=datetime.fromtimestamp(request.start_date.seconds + request.start_date.nanos/1e9),
        end_date=datetime.fromtimestamp(request.end_date.seconds + request.end_date.nanos/1e9),
        commit=request.commit
     )
     return Empty()
  
  def ListProjectMetadata(self, request: RegistryService_pb2.ListProjectMetadataRequest, context):
     return RegistryService_pb2.ListProjectMetadataResponse(
        project_metadata=[project_metadata.to_proto() for project_metadata in self.proxied_registry.list_project_metadata(
        project=request.project,
        allow_cache=request.allow_cache
     )])

  def UpdateInfra(self, request: RegistryService_pb2.UpdateInfraRequest, context):
     self.proxied_registry.update_infra(
        infra=Infra.from_proto(request.infra),
        project=request.project,
        commit=request.commit
     )
     return Empty()

  def GetInfra(self, request: RegistryService_pb2.GetInfraRequest, context):
     return self.proxied_registry.get_infra(
        project=request.project,
        allow_cache=request.allow_cache
     ).to_proto()

  def Commit(self, request, context):
     self.proxied_registry.commit()   
     return Empty()
   
  def Refresh(self, request, context):
     self.proxied_registry.refresh(request.project)
     return Empty()

  def Proto(self, request, context):
     return self.proxied_registry.proto()


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    RegistryService_pb2_grpc.add_RegistryServiceServicer_to_server(RegistryServer(), server)
    server.add_insecure_port('[::]:50051')
    print('Starting Server')
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    serve()