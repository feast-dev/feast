from feast.core.CoreService_pb2 import (
    ApplyEntityRequest,
    ApplyEntityResponse,
    ApplyFeatureTableRequest,
    ApplyFeatureTableResponse,
    ArchiveProjectRequest,
    ArchiveProjectResponse,
    CreateProjectRequest,
    CreateProjectResponse,
    DeleteFeatureTableRequest,
    DeleteFeatureTableResponse,
    GetEntityRequest,
    GetEntityResponse,
    GetFeastCoreVersionRequest,
    GetFeastCoreVersionResponse,
    GetFeatureTableRequest,
    GetFeatureTableResponse,
    ListEntitiesRequest,
    ListEntitiesResponse,
    ListFeaturesRequest,
    ListFeaturesResponse,
    ListFeatureTablesRequest,
    ListFeatureTablesResponse,
    ListProjectsRequest,
    ListProjectsResponse,
    ListStoresRequest,
    ListStoresResponse,
    UpdateStoreRequest,
    UpdateStoreResponse,
)
from feast.rest.transport import rest_transport


class CoreServiceRESTStub(object):
    def __init__(self, core_url, service_name="feast.core.CoreService") -> None:
        super().__init__()
        self._url = core_url
        self._service_name = service_name

    @rest_transport
    def GetFeastCoreVersion(
        self, req: GetFeastCoreVersionRequest, *args, **kwargs
    ) -> GetFeastCoreVersionResponse:
        pass

    @rest_transport
    def GetEntity(self, req: GetEntityRequest, *args, **kwargs) -> GetEntityResponse:
        pass

    @rest_transport
    def ListFeatures(
        self, req: ListFeaturesRequest, *args, **kwargs
    ) -> ListFeaturesResponse:
        pass

    @rest_transport
    def ListStores(self, req: ListStoresRequest, *args, **kwargs) -> ListStoresResponse:
        pass

    @rest_transport
    def ApplyEntity(
        self, req: ApplyEntityRequest, *args, **kwargs
    ) -> ApplyEntityResponse:
        pass

    @rest_transport
    def ListEntities(
        self, req: ListEntitiesRequest, *args, **kwargs
    ) -> ListEntitiesResponse:
        pass

    @rest_transport
    def UpdateStore(
        self, req: UpdateStoreRequest, *args, **kwargs
    ) -> UpdateStoreResponse:
        pass

    @rest_transport
    def CreateProject(
        self, req: CreateProjectRequest, *args, **kwargs
    ) -> CreateProjectResponse:
        pass

    @rest_transport
    def ArchiveProject(
        self, req: ArchiveProjectRequest, *args, **kwargs
    ) -> ArchiveProjectResponse:
        pass

    @rest_transport
    def ListProjects(
        self, req: ListProjectsRequest, *args, **kwargs
    ) -> ListProjectsResponse:
        pass

    @rest_transport
    def ApplyFeatureTable(
        self, req: ApplyFeatureTableRequest, *args, **kwargs
    ) -> ApplyFeatureTableResponse:
        pass

    @rest_transport
    def ListFeatureTables(
        self, req: ListFeatureTablesRequest, *args, **kwargs
    ) -> ListFeatureTablesResponse:
        pass

    @rest_transport
    def GetFeatureTable(
        self, req: GetFeatureTableRequest, *args, **kwargs
    ) -> GetFeatureTableResponse:
        pass

    @rest_transport
    def DeleteFeatureTable(
        self, req: DeleteFeatureTableRequest, *args, **kwargs
    ) -> DeleteFeatureTableResponse:
        pass
