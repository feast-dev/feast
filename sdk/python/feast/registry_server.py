from concurrent import futures
from datetime import datetime
from typing import Union, cast

import grpc
from google.protobuf.empty_pb2 import Empty
from pytz import utc

from feast import FeatureService, FeatureStore
from feast.data_source import DataSource
from feast.entity import Entity
from feast.errors import FeatureViewNotFoundException
from feast.feast_object import FeastObject
from feast.feature_view import FeatureView
from feast.infra.infra_object import Infra
from feast.infra.registry.base_registry import BaseRegistry
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.permissions.action import CRUD, AuthzedAction
from feast.permissions.permission import Permission
from feast.permissions.security_manager import assert_permissions, permitted_resources
from feast.permissions.server.grpc import grpc_interceptors
from feast.permissions.server.utils import (
    ServerType,
    init_auth_manager,
    init_security_manager,
    str_to_auth_manager_type,
)
from feast.protos.feast.registry import RegistryServer_pb2, RegistryServer_pb2_grpc
from feast.saved_dataset import SavedDataset, ValidationReference
from feast.stream_feature_view import StreamFeatureView


class RegistryServer(RegistryServer_pb2_grpc.RegistryServerServicer):
    def __init__(self, registry: BaseRegistry) -> None:
        super().__init__()
        self.proxied_registry = registry

    def ApplyEntity(self, request: RegistryServer_pb2.ApplyEntityRequest, context):
        assert_permissions(
            resource=self.proxied_registry.apply_entity(
                entity=Entity.from_proto(request.entity),
                project=request.project,
                commit=request.commit,
            ),
            actions=CRUD,
        )
        return Empty()

    def GetEntity(self, request: RegistryServer_pb2.GetEntityRequest, context):
        return assert_permissions(
            self.proxied_registry.get_entity(
                name=request.name,
                project=request.project,
                allow_cache=request.allow_cache,
            ),
            actions=[AuthzedAction.READ],
        ).to_proto()

    def ListEntities(self, request: RegistryServer_pb2.ListEntitiesRequest, context):
        return RegistryServer_pb2.ListEntitiesResponse(
            entities=[
                entity.to_proto()
                for entity in permitted_resources(
                    resources=cast(
                        list[FeastObject],
                        self.proxied_registry.list_entities(
                            project=request.project,
                            allow_cache=request.allow_cache,
                            tags=dict(request.tags),
                        ),
                    ),
                    actions=AuthzedAction.READ,
                )
            ]
        )

    def DeleteEntity(self, request: RegistryServer_pb2.DeleteEntityRequest, context):
        assert_permissions(
            resource=self.proxied_registry.get_entity(
                name=request.name, project=request.project
            ),
            actions=AuthzedAction.DELETE,
        )

        self.proxied_registry.delete_entity(
            name=request.name, project=request.project, commit=request.commit
        )
        return Empty()

    def ApplyDataSource(
        self, request: RegistryServer_pb2.ApplyDataSourceRequest, context
    ):
        assert_permissions(
            resource=self.proxied_registry.apply_data_source(
                data_source=DataSource.from_proto(request.data_source),
                project=request.project,
                commit=request.commit,
            ),
            actions=CRUD,
        )
        return Empty()

    def GetDataSource(self, request: RegistryServer_pb2.GetDataSourceRequest, context):
        return assert_permissions(
            resource=self.proxied_registry.get_data_source(
                name=request.name,
                project=request.project,
                allow_cache=request.allow_cache,
            ),
            actions=AuthzedAction.READ,
        ).to_proto()

    def ListDataSources(
        self, request: RegistryServer_pb2.ListDataSourcesRequest, context
    ):
        return RegistryServer_pb2.ListDataSourcesResponse(
            data_sources=[
                data_source.to_proto()
                for data_source in permitted_resources(
                    resources=cast(
                        list[FeastObject],
                        self.proxied_registry.list_data_sources(
                            project=request.project,
                            allow_cache=request.allow_cache,
                            tags=dict(request.tags),
                        ),
                    ),
                    actions=AuthzedAction.READ,
                )
            ]
        )

    def DeleteDataSource(
        self, request: RegistryServer_pb2.DeleteDataSourceRequest, context
    ):
        assert_permissions(
            resource=self.proxied_registry.get_data_source(
                name=request.name,
                project=request.project,
            ),
            actions=AuthzedAction.DELETE,
        )

        self.proxied_registry.delete_data_source(
            name=request.name, project=request.project, commit=request.commit
        )
        return Empty()

    def GetFeatureView(
        self, request: RegistryServer_pb2.GetFeatureViewRequest, context
    ):
        return assert_permissions(
            self.proxied_registry.get_feature_view(
                name=request.name,
                project=request.project,
                allow_cache=request.allow_cache,
            ),
            actions=[AuthzedAction.READ],
        ).to_proto()

    def ApplyFeatureView(
        self, request: RegistryServer_pb2.ApplyFeatureViewRequest, context
    ):
        feature_view_type = request.WhichOneof("base_feature_view")
        if feature_view_type == "feature_view":
            feature_view = FeatureView.from_proto(request.feature_view)
        elif feature_view_type == "on_demand_feature_view":
            feature_view = OnDemandFeatureView.from_proto(
                request.on_demand_feature_view
            )
        elif feature_view_type == "stream_feature_view":
            feature_view = StreamFeatureView.from_proto(request.stream_feature_view)

        assert_permissions(
            resource=self.proxied_registry.apply_feature_view(
                feature_view=feature_view,
                project=request.project,
                commit=request.commit,
            ),
            actions=CRUD,
        )
        return Empty()

    def ListFeatureViews(
        self, request: RegistryServer_pb2.ListFeatureViewsRequest, context
    ):
        return RegistryServer_pb2.ListFeatureViewsResponse(
            feature_views=[
                feature_view.to_proto()
                for feature_view in permitted_resources(
                    resources=cast(
                        list[FeastObject],
                        self.proxied_registry.list_feature_views(
                            project=request.project,
                            allow_cache=request.allow_cache,
                            tags=dict(request.tags),
                        ),
                    ),
                    actions=AuthzedAction.READ,
                )
            ]
        )

    def DeleteFeatureView(
        self, request: RegistryServer_pb2.DeleteFeatureViewRequest, context
    ):
        feature_view: Union[StreamFeatureView, FeatureView]

        try:
            feature_view = self.proxied_registry.get_stream_feature_view(
                name=request.name, project=request.project, allow_cache=False
            )
        except FeatureViewNotFoundException:
            feature_view = self.proxied_registry.get_feature_view(
                name=request.name, project=request.project, allow_cache=False
            )

        assert_permissions(
            resource=feature_view,
            actions=[AuthzedAction.DELETE],
        )
        self.proxied_registry.delete_feature_view(
            name=request.name, project=request.project, commit=request.commit
        )
        return Empty()

    def GetStreamFeatureView(
        self, request: RegistryServer_pb2.GetStreamFeatureViewRequest, context
    ):
        return assert_permissions(
            resource=self.proxied_registry.get_stream_feature_view(
                name=request.name,
                project=request.project,
                allow_cache=request.allow_cache,
            ),
            actions=[AuthzedAction.READ],
        ).to_proto()

    def ListStreamFeatureViews(
        self, request: RegistryServer_pb2.ListStreamFeatureViewsRequest, context
    ):
        return RegistryServer_pb2.ListStreamFeatureViewsResponse(
            stream_feature_views=[
                stream_feature_view.to_proto()
                for stream_feature_view in permitted_resources(
                    resources=cast(
                        list[FeastObject],
                        self.proxied_registry.list_stream_feature_views(
                            project=request.project,
                            allow_cache=request.allow_cache,
                            tags=dict(request.tags),
                        ),
                    ),
                    actions=AuthzedAction.READ,
                )
            ]
        )

    def GetOnDemandFeatureView(
        self, request: RegistryServer_pb2.GetOnDemandFeatureViewRequest, context
    ):
        return assert_permissions(
            resource=self.proxied_registry.get_on_demand_feature_view(
                name=request.name,
                project=request.project,
                allow_cache=request.allow_cache,
            ),
            actions=[AuthzedAction.READ],
        ).to_proto()

    def ListOnDemandFeatureViews(
        self, request: RegistryServer_pb2.ListOnDemandFeatureViewsRequest, context
    ):
        return RegistryServer_pb2.ListOnDemandFeatureViewsResponse(
            on_demand_feature_views=[
                on_demand_feature_view.to_proto()
                for on_demand_feature_view in permitted_resources(
                    resources=cast(
                        list[FeastObject],
                        self.proxied_registry.list_on_demand_feature_views(
                            project=request.project,
                            allow_cache=request.allow_cache,
                            tags=dict(request.tags),
                        ),
                    ),
                    actions=AuthzedAction.READ,
                )
            ]
        )

    def ApplyFeatureService(
        self, request: RegistryServer_pb2.ApplyFeatureServiceRequest, context
    ):
        assert_permissions(
            resource=self.proxied_registry.apply_feature_service(
                feature_service=FeatureService.from_proto(request.feature_service),
                project=request.project,
                commit=request.commit,
            ),
            actions=CRUD,
        )

        return Empty()

    def GetFeatureService(
        self, request: RegistryServer_pb2.GetFeatureServiceRequest, context
    ):
        return assert_permissions(
            resource=self.proxied_registry.get_feature_service(
                name=request.name,
                project=request.project,
                allow_cache=request.allow_cache,
            ),
            actions=[AuthzedAction.READ],
        ).to_proto()

    def ListFeatureServices(
        self, request: RegistryServer_pb2.ListFeatureServicesRequest, context
    ):
        return RegistryServer_pb2.ListFeatureServicesResponse(
            feature_services=[
                feature_service.to_proto()
                for feature_service in permitted_resources(
                    resources=cast(
                        list[FeastObject],
                        self.proxied_registry.list_feature_services(
                            project=request.project,
                            allow_cache=request.allow_cache,
                            tags=dict(request.tags),
                        ),
                    ),
                    actions=AuthzedAction.READ,
                )
            ]
        )

    def DeleteFeatureService(
        self, request: RegistryServer_pb2.DeleteFeatureServiceRequest, context
    ):
        (
            assert_permissions(
                resource=self.proxied_registry.get_feature_service(
                    name=request.name, project=request.project
                ),
                actions=[AuthzedAction.DELETE],
            ),
        )

        self.proxied_registry.delete_feature_service(
            name=request.name, project=request.project, commit=request.commit
        )
        return Empty()

    def ApplySavedDataset(
        self, request: RegistryServer_pb2.ApplySavedDatasetRequest, context
    ):
        assert_permissions(
            resource=self.proxied_registry.apply_saved_dataset(
                saved_dataset=SavedDataset.from_proto(request.saved_dataset),
                project=request.project,
                commit=request.commit,
            ),
            actions=CRUD,
        )

        return Empty()

    def GetSavedDataset(
        self, request: RegistryServer_pb2.GetSavedDatasetRequest, context
    ):
        return assert_permissions(
            self.proxied_registry.get_saved_dataset(
                name=request.name,
                project=request.project,
                allow_cache=request.allow_cache,
            ),
            actions=[AuthzedAction.READ],
        ).to_proto()

    def ListSavedDatasets(
        self, request: RegistryServer_pb2.ListSavedDatasetsRequest, context
    ):
        return RegistryServer_pb2.ListSavedDatasetsResponse(
            saved_datasets=[
                saved_dataset.to_proto()
                for saved_dataset in permitted_resources(
                    resources=cast(
                        list[FeastObject],
                        self.proxied_registry.list_saved_datasets(
                            project=request.project,
                            allow_cache=request.allow_cache,
                            tags=dict(request.tags),
                        ),
                    ),
                    actions=AuthzedAction.READ,
                )
            ]
        )

    def DeleteSavedDataset(
        self, request: RegistryServer_pb2.DeleteSavedDatasetRequest, context
    ):
        assert_permissions(
            resource=self.proxied_registry.get_saved_dataset(
                name=request.name, project=request.project
            ),
            actions=[AuthzedAction.DELETE],
        )

        self.proxied_registry.delete_saved_dataset(
            name=request.name, project=request.project, commit=request.commit
        )
        return Empty()

    def ApplyValidationReference(
        self, request: RegistryServer_pb2.ApplyValidationReferenceRequest, context
    ):
        assert_permissions(
            resource=self.proxied_registry.apply_validation_reference(
                validation_reference=ValidationReference.from_proto(
                    request.validation_reference
                ),
                project=request.project,
                commit=request.commit,
            ),
            actions=CRUD,
        )

        return Empty()

    def GetValidationReference(
        self, request: RegistryServer_pb2.GetValidationReferenceRequest, context
    ):
        return assert_permissions(
            self.proxied_registry.get_validation_reference(
                name=request.name,
                project=request.project,
                allow_cache=request.allow_cache,
            ),
            actions=[AuthzedAction.READ],
        ).to_proto()

    def ListValidationReferences(
        self, request: RegistryServer_pb2.ListValidationReferencesRequest, context
    ):
        return RegistryServer_pb2.ListValidationReferencesResponse(
            validation_references=[
                validation_reference.to_proto()
                for validation_reference in permitted_resources(
                    resources=cast(
                        list[FeastObject],
                        self.proxied_registry.list_validation_references(
                            project=request.project,
                            allow_cache=request.allow_cache,
                            tags=dict(request.tags),
                        ),
                    ),
                    actions=AuthzedAction.READ,
                )
            ]
        )

    def DeleteValidationReference(
        self, request: RegistryServer_pb2.DeleteValidationReferenceRequest, context
    ):
        assert_permissions(
            resource=self.proxied_registry.get_validation_reference(
                name=request.name, project=request.project
            ),
            actions=[AuthzedAction.DELETE],
        )
        self.proxied_registry.delete_validation_reference(
            name=request.name, project=request.project, commit=request.commit
        )
        return Empty()

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

    def ApplyMaterialization(
        self, request: RegistryServer_pb2.ApplyMaterializationRequest, context
    ):
        assert_permissions(
            resource=FeatureView.from_proto(request.feature_view),
            actions=[AuthzedAction.WRITE_ONLINE],
        )

        self.proxied_registry.apply_materialization(
            feature_view=FeatureView.from_proto(request.feature_view),
            project=request.project,
            start_date=datetime.fromtimestamp(
                request.start_date.seconds + request.start_date.nanos / 1e9, tz=utc
            ),
            end_date=datetime.fromtimestamp(
                request.end_date.seconds + request.end_date.nanos / 1e9, tz=utc
            ),
            commit=request.commit,
        )
        return Empty()

    def UpdateInfra(self, request: RegistryServer_pb2.UpdateInfraRequest, context):
        self.proxied_registry.update_infra(
            infra=Infra.from_proto(request.infra),
            project=request.project,
            commit=request.commit,
        )
        return Empty()

    def GetInfra(self, request: RegistryServer_pb2.GetInfraRequest, context):
        return self.proxied_registry.get_infra(
            project=request.project, allow_cache=request.allow_cache
        ).to_proto()

    def ApplyPermission(
        self, request: RegistryServer_pb2.ApplyPermissionRequest, context
    ):
        assert_permissions(
            self.proxied_registry.apply_permission(
                permission=Permission.from_proto(request.permission),
                project=request.project,
                commit=request.commit,
            ),
            actions=CRUD,
        )
        return Empty()

    def GetPermission(self, request: RegistryServer_pb2.GetPermissionRequest, context):
        permission = self.proxied_registry.get_permission(
            name=request.name, project=request.project, allow_cache=request.allow_cache
        )
        assert_permissions(
            resource=permission,
            actions=[AuthzedAction.READ],
        )
        permission.to_proto().project = request.project

        return permission.to_proto()

    def ListPermissions(
        self, request: RegistryServer_pb2.ListPermissionsRequest, context
    ):
        return RegistryServer_pb2.ListPermissionsResponse(
            permissions=[
                permission.to_proto()
                for permission in permitted_resources(
                    resources=cast(
                        list[FeastObject],
                        self.proxied_registry.list_permissions(
                            project=request.project, allow_cache=request.allow_cache
                        ),
                    ),
                    actions=AuthzedAction.READ,
                )
            ]
        )

    def DeletePermission(
        self, request: RegistryServer_pb2.DeletePermissionRequest, context
    ):
        assert_permissions(
            resource=self.proxied_registry.get_permission(
                name=request.name,
                project=request.project,
            ),
            actions=[AuthzedAction.DELETE],
        )

        self.proxied_registry.delete_permission(
            name=request.name, project=request.project, commit=request.commit
        )
        return Empty()

    def Commit(self, request, context):
        self.proxied_registry.commit()
        return Empty()

    def Refresh(self, request, context):
        self.proxied_registry.refresh(request.project)
        return Empty()

    def Proto(self, request, context):
        return self.proxied_registry.proto()


def start_server(store: FeatureStore, port: int, wait_for_termination: bool = True):
    auth_manager_type = str_to_auth_manager_type(store.config.auth_config.type)
    init_security_manager(auth_type=auth_manager_type, fs=store)
    init_auth_manager(
        auth_type=auth_manager_type,
        server_type=ServerType.GRPC,
        auth_config=store.config.auth_config,
    )

    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=10),
        interceptors=grpc_interceptors(auth_manager_type),
    )
    RegistryServer_pb2_grpc.add_RegistryServerServicer_to_server(
        RegistryServer(store.registry), server
    )
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    if wait_for_termination:
        server.wait_for_termination()
    else:
        return server
