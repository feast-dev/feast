from datetime import datetime
from pathlib import Path
from typing import List, Optional, Union

import grpc
from google.protobuf.empty_pb2 import Empty
from pydantic import StrictStr

from feast.base_feature_view import BaseFeatureView
from feast.data_source import DataSource
from feast.entity import Entity
from feast.errors import ReadOnlyRegistryException
from feast.feature_service import FeatureService
from feast.feature_view import FeatureView
from feast.infra.infra_object import Infra
from feast.infra.registry.base_registry import BaseRegistry
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.project_metadata import ProjectMetadata
from feast.protos.feast.core.Registry_pb2 import Registry as RegistryProto
from feast.protos.feast.registry import RegistryServer_pb2, RegistryServer_pb2_grpc
from feast.repo_config import RegistryConfig
from feast.request_feature_view import RequestFeatureView
from feast.saved_dataset import SavedDataset, ValidationReference
from feast.stream_feature_view import StreamFeatureView


class RemoteRegistryConfig(RegistryConfig):
    registry_type: StrictStr = "remote"
    """ str: Provider name or a class name that implements Registry."""

    path: StrictStr = ""
    """ str: Path to metadata store.
    If registry_type is 'remote', then this is a URL for registry server """


class RemoteRegistry(BaseRegistry):
    def __init__(
        self,
        registry_config: Union[RegistryConfig, RemoteRegistryConfig],
        project: str,
        repo_path: Optional[Path],
    ):
        self.channel = grpc.insecure_channel(registry_config.path)
        self.stub = RegistryServer_pb2_grpc.RegistryServerStub(self.channel)

    def apply_entity(self, entity: Entity, project: str, commit: bool = True):
        raise ReadOnlyRegistryException()

    def delete_entity(self, name: str, project: str, commit: bool = True):
        raise ReadOnlyRegistryException()

    def get_entity(self, name: str, project: str, allow_cache: bool = False) -> Entity:
        request = RegistryServer_pb2.GetEntityRequest(
            name=name, project=project, allow_cache=allow_cache
        )

        response = self.stub.GetEntity(request)

        return Entity.from_proto(response)

    def list_entities(self, project: str, allow_cache: bool = False) -> List[Entity]:
        request = RegistryServer_pb2.ListEntitiesRequest(
            project=project, allow_cache=allow_cache
        )

        response = self.stub.ListEntities(request)

        return [Entity.from_proto(entity) for entity in response.entities]

    def apply_data_source(
        self, data_source: DataSource, project: str, commit: bool = True
    ):
        raise ReadOnlyRegistryException()

    def delete_data_source(self, name: str, project: str, commit: bool = True):
        raise ReadOnlyRegistryException()

    def get_data_source(
        self, name: str, project: str, allow_cache: bool = False
    ) -> DataSource:
        request = RegistryServer_pb2.GetDataSourceRequest(
            name=name, project=project, allow_cache=allow_cache
        )

        response = self.stub.GetDataSource(request)

        return DataSource.from_proto(response)

    def list_data_sources(
        self, project: str, allow_cache: bool = False
    ) -> List[DataSource]:
        request = RegistryServer_pb2.ListDataSourcesRequest(
            project=project, allow_cache=allow_cache
        )

        response = self.stub.ListDataSources(request)

        return [
            DataSource.from_proto(data_source) for data_source in response.data_sources
        ]

    def apply_feature_service(
        self, feature_service: FeatureService, project: str, commit: bool = True
    ):
        raise ReadOnlyRegistryException()

    def delete_feature_service(self, name: str, project: str, commit: bool = True):
        raise ReadOnlyRegistryException()

    def get_feature_service(
        self, name: str, project: str, allow_cache: bool = False
    ) -> FeatureService:
        request = RegistryServer_pb2.GetFeatureServiceRequest(
            name=name, project=project, allow_cache=allow_cache
        )

        response = self.stub.GetFeatureService(request)

        return FeatureService.from_proto(response)

    def list_feature_services(
        self, project: str, allow_cache: bool = False
    ) -> List[FeatureService]:
        request = RegistryServer_pb2.ListFeatureServicesRequest(
            project=project, allow_cache=allow_cache
        )

        response = self.stub.ListFeatureServices(request)

        return [
            FeatureService.from_proto(feature_service)
            for feature_service in response.feature_services
        ]

    def apply_feature_view(
        self, feature_view: BaseFeatureView, project: str, commit: bool = True
    ):
        raise ReadOnlyRegistryException()

    def delete_feature_view(self, name: str, project: str, commit: bool = True):
        raise ReadOnlyRegistryException()

    def get_stream_feature_view(
        self, name: str, project: str, allow_cache: bool = False
    ) -> StreamFeatureView:
        request = RegistryServer_pb2.GetStreamFeatureViewRequest(
            name=name, project=project, allow_cache=allow_cache
        )

        response = self.stub.GetStreamFeatureView(request)

        return StreamFeatureView.from_proto(response)

    def list_stream_feature_views(
        self, project: str, allow_cache: bool = False
    ) -> List[StreamFeatureView]:
        request = RegistryServer_pb2.ListStreamFeatureViewsRequest(
            project=project, allow_cache=allow_cache
        )

        response = self.stub.ListStreamFeatureViews(request)

        return [
            StreamFeatureView.from_proto(stream_feature_view)
            for stream_feature_view in response.stream_feature_views
        ]

    def get_on_demand_feature_view(
        self, name: str, project: str, allow_cache: bool = False
    ) -> OnDemandFeatureView:
        request = RegistryServer_pb2.GetOnDemandFeatureViewRequest(
            name=name, project=project, allow_cache=allow_cache
        )

        response = self.stub.GetOnDemandFeatureView(request)

        return OnDemandFeatureView.from_proto(response)

    def list_on_demand_feature_views(
        self, project: str, allow_cache: bool = False
    ) -> List[OnDemandFeatureView]:
        request = RegistryServer_pb2.ListOnDemandFeatureViewsRequest(
            project=project, allow_cache=allow_cache
        )

        response = self.stub.ListOnDemandFeatureViews(request)

        return [
            OnDemandFeatureView.from_proto(on_demand_feature_view)
            for on_demand_feature_view in response.on_demand_feature_views
        ]

    def get_feature_view(
        self, name: str, project: str, allow_cache: bool = False
    ) -> FeatureView:
        request = RegistryServer_pb2.GetFeatureViewRequest(
            name=name, project=project, allow_cache=allow_cache
        )

        response = self.stub.GetFeatureView(request)

        return FeatureView.from_proto(response)

    def list_feature_views(
        self, project: str, allow_cache: bool = False
    ) -> List[FeatureView]:
        request = RegistryServer_pb2.ListFeatureViewsRequest(
            project=project, allow_cache=allow_cache
        )

        response = self.stub.ListFeatureViews(request)

        return [
            FeatureView.from_proto(feature_view)
            for feature_view in response.feature_views
        ]

    def get_request_feature_view(
        self, name: str, project: str, allow_cache: bool = False
    ) -> RequestFeatureView:
        request = RegistryServer_pb2.GetRequestFeatureViewRequest(
            name=name, project=project, allow_cache=allow_cache
        )

        response = self.stub.GetRequestFeatureView(request)

        return RequestFeatureView.from_proto(response)

    def list_request_feature_views(
        self, project: str, allow_cache: bool = False
    ) -> List[RequestFeatureView]:
        request = RegistryServer_pb2.ListRequestFeatureViewsRequest(
            project=project, allow_cache=allow_cache
        )

        response = self.stub.ListRequestFeatureViews(request)

        return [
            RequestFeatureView.from_proto(request_feature_view)
            for request_feature_view in response.request_feature_views
        ]

    def apply_materialization(
        self,
        feature_view: FeatureView,
        project: str,
        start_date: datetime,
        end_date: datetime,
        commit: bool = True,
    ):
        raise ReadOnlyRegistryException()

    def apply_saved_dataset(
        self,
        saved_dataset: SavedDataset,
        project: str,
        commit: bool = True,
    ):
        raise ReadOnlyRegistryException()

    def delete_saved_dataset(self, name: str, project: str, allow_cache: bool = False):
        raise ReadOnlyRegistryException()

    def get_saved_dataset(
        self, name: str, project: str, allow_cache: bool = False
    ) -> SavedDataset:
        request = RegistryServer_pb2.GetSavedDatasetRequest(
            name=name, project=project, allow_cache=allow_cache
        )

        response = self.stub.GetSavedDataset(request)

        return SavedDataset.from_proto(response)

    def list_saved_datasets(
        self, project: str, allow_cache: bool = False
    ) -> List[SavedDataset]:
        request = RegistryServer_pb2.ListSavedDatasetsRequest(
            project=project, allow_cache=allow_cache
        )

        response = self.stub.ListSavedDatasets(request)

        return [
            SavedDataset.from_proto(saved_dataset)
            for saved_dataset in response.saved_datasets
        ]

    def apply_validation_reference(
        self,
        validation_reference: ValidationReference,
        project: str,
        commit: bool = True,
    ):
        raise ReadOnlyRegistryException()

    def delete_validation_reference(self, name: str, project: str, commit: bool = True):
        raise ReadOnlyRegistryException()

    def get_validation_reference(
        self, name: str, project: str, allow_cache: bool = False
    ) -> ValidationReference:
        request = RegistryServer_pb2.GetValidationReferenceRequest(
            name=name, project=project, allow_cache=allow_cache
        )

        response = self.stub.GetValidationReference(request)

        return ValidationReference.from_proto(response)

    def list_validation_references(
        self, project: str, allow_cache: bool = False
    ) -> List[ValidationReference]:
        request = RegistryServer_pb2.ListValidationReferencesRequest(
            project=project, allow_cache=allow_cache
        )

        response = self.stub.ListValidationReferences(request)

        return [
            ValidationReference.from_proto(validation_reference)
            for validation_reference in response.validation_references
        ]

    def list_project_metadata(
        self, project: str, allow_cache: bool = False
    ) -> List[ProjectMetadata]:
        request = RegistryServer_pb2.ListProjectMetadataRequest(
            project=project, allow_cache=allow_cache
        )

        response = self.stub.ListProjectMetadata(request)

        return [ProjectMetadata.from_proto(pm) for pm in response.project_metadata]

    def update_infra(self, infra: Infra, project: str, commit: bool = True):
        raise ReadOnlyRegistryException()

    def get_infra(self, project: str, allow_cache: bool = False) -> Infra:
        request = RegistryServer_pb2.GetInfraRequest(
            project=project, allow_cache=allow_cache
        )

        response = self.stub.GetInfra(request)

        return Infra.from_proto(response)

    def apply_user_metadata(
        self,
        project: str,
        feature_view: BaseFeatureView,
        metadata_bytes: Optional[bytes],
    ):
        pass

    def get_user_metadata(
        self, project: str, feature_view: BaseFeatureView
    ) -> Optional[bytes]:
        pass

    def proto(self) -> RegistryProto:
        return self.stub.Proto(Empty())

    def commit(self):
        raise ReadOnlyRegistryException()

    def refresh(self, project: Optional[str] = None):
        request = RegistryServer_pb2.RefreshRequest(project=str(project))

        self.stub.Refresh(request)
