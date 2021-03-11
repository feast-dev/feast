from feast.rest.transport import rest_transport
from feast.serving.ServingService_pb2 import (
    GetFeastServingInfoRequest,
    GetFeastServingInfoResponse,
    GetOnlineFeaturesRequestV2,
    GetOnlineFeaturesResponse,
)


class ServingServiceRESTStub(object):
    def __init__(
        self, serving_url, service_name="feast.serving.ServingService"
    ) -> None:
        super().__init__()
        self.url = serving_url
        self.service_name = service_name

    @rest_transport
    def GetFeastServingInfo(
        self, req: GetFeastServingInfoRequest, *args, **kwargs
    ) -> GetFeastServingInfoResponse:
        pass

    @rest_transport
    def GetOnlineFeatures(
        self, req: GetOnlineFeaturesRequestV2, *args, **kwargs
    ) -> GetOnlineFeaturesResponse:
        pass
