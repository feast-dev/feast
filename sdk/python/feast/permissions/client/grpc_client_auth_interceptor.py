import logging

import grpc

from feast.permissions.auth.auth_type import AuthType
from feast.permissions.auth_model import AuthConfig
from feast.permissions.client.auth_client_manager import get_auth_client_manager

logger = logging.getLogger(__name__)


class GrpcClientAuthHeaderInterceptor(
    grpc.UnaryUnaryClientInterceptor,
    grpc.UnaryStreamClientInterceptor,
    grpc.StreamUnaryClientInterceptor,
    grpc.StreamStreamClientInterceptor,
):
    def __init__(self, auth_type: AuthConfig):
        self._auth_type = auth_type

    def intercept_unary_unary(
        self, continuation, client_call_details, request_iterator
    ):
        client_call_details = self._append_auth_header_metadata(client_call_details)
        return continuation(client_call_details, request_iterator)

    def intercept_unary_stream(
        self, continuation, client_call_details, request_iterator
    ):
        client_call_details = self._append_auth_header_metadata(client_call_details)
        return continuation(client_call_details, request_iterator)

    def intercept_stream_unary(
        self, continuation, client_call_details, request_iterator
    ):
        client_call_details = self._append_auth_header_metadata(client_call_details)
        return continuation(client_call_details, request_iterator)

    def intercept_stream_stream(
        self, continuation, client_call_details, request_iterator
    ):
        client_call_details = self._append_auth_header_metadata(client_call_details)
        return continuation(client_call_details, request_iterator)

    def _append_auth_header_metadata(self, client_call_details):
        if self._auth_type.type is not AuthType.NONE.value:
            logger.info(
                f"Intercepted the grpc api method {client_call_details.method} call to inject Authorization header "
                f"token. "
            )
            metadata = client_call_details.metadata or []
            auth_client_manager = get_auth_client_manager(self._auth_type)
            access_token = auth_client_manager.get_token()
            metadata.append(
                (b"authorization", b"Bearer " + access_token.encode("utf-8"))
            )
            client_call_details = client_call_details._replace(metadata=metadata)
        return client_call_details
